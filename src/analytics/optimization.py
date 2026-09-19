"""Query optimization and platform evaluation."""

import os
import sys
import time
import statistics

from pyspark.sql import DataFrame, SparkSession

from src.analytics.constants import (
    INTEGRATED_TRIPS_PATH,
    PRODUCT_BASE_PATH,
    SILVER_TAXI_ZONES_PATH,
)
from src.common.spark_session import get_spark
from src.analytics.runtime import configure_spark_temp_dir



PRODUCT_PATHS = {
    "daily_mobility": f"{PRODUCT_BASE_PATH}/daily_mobility",
    "taxi_zone_statistics": f"{PRODUCT_BASE_PATH}/taxi_zone_statistics",
    "weather_impact": f"{PRODUCT_BASE_PATH}/weather_impact",
    "air_quality_impact": f"{PRODUCT_BASE_PATH}/air_quality_impact",
}


def print_separator(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def bench(spark: SparkSession, sql: str, n: int = 3, label: str = "") -> float:
    times = []

    for _ in range(n):
        start = time.perf_counter()
        spark.sql(sql).collect()
        times.append(time.perf_counter() - start)

    measured = times[1:]
    median = round(statistics.median(measured), 3)

    print(
        f"  {label:<40}"
        f"median={median:.3f}s  "
        f"runs={[round(t, 3) for t in times]}"
    )

    return median


def verify_same(
    df1: DataFrame,
    df2: DataFrame,
    label: str = ""
) -> bool:
    left = df1.collect()
    right = df2.collect()

    identical = sorted(map(str, left)) == sorted(map(str, right))

    print(
        f"  Results [{label}]:",
        "✓ identical" if identical else "✗ differ"
    )

    return identical


def summarise(results: dict, pairs: list[tuple]) -> None:
    print_separator("PERFORMANCE SUMMARY")

    print(
        f"  {'Experiment':<30}"
        f"{'Baseline':>12}"
        f"{'Optimized':>12}"
        f"{'Improvement':>14}"
    )
    print("  " + "-" * 68)

    for name, base_key, opt_key in pairs:
        baseline = results[base_key]
        optimized = results[opt_key]
        improvement = (baseline - optimized) / baseline * 100

        print(
            f"  {name:<30}"
            f"{baseline:>11.3f}s"
            f"{optimized:>11.3f}s"
            f"{improvement:>13.2f}%"
        )


def exp_caching(spark: SparkSession, results: dict) -> None:
    print_separator("EXP 1 – Caching")
    spark.catalog.clearCache()

    query = """
        SELECT year, month, COUNT(*) AS taxi_demand
        FROM trips
        WHERE year = 2024
        GROUP BY year, month
        ORDER BY year, month
    """

    results["caching_baseline"] = bench(
        spark, query, label="no cache"
    )

    cached = (
        spark.sql("""
            SELECT year, month
            FROM trips
            WHERE year = 2024
        """)
        .cache()
    )

    cached.createOrReplaceTempView("trips_cached")
    cached.count()

    cached_query = """
        SELECT year, month, COUNT(*) AS taxi_demand
        FROM trips_cached
        GROUP BY year, month
        ORDER BY year, month
    """

    results["caching_optimized"] = bench(
        spark, cached_query, label="cached"
    )

    verify_same(
        spark.sql(query),
        spark.sql(cached_query),
        "caching"
    )

    print("\n  EXPLAIN FORMATTED (cached):")
    spark.sql(cached_query).explain("formatted")

    spark.catalog.clearCache()


def exp_partition_pruning(
    spark: SparkSession,
    results: dict
) -> None:
    print_separator("EXP 2 – Partition Pruning")
    spark.catalog.clearCache()

    baseline = """
        SELECT year, month, COUNT(*) AS taxi_demand
        FROM trips
        GROUP BY year, month
        ORDER BY year, month
    """

    pruned = """
        SELECT month, COUNT(*) AS taxi_demand
        FROM trips
        WHERE year = 2024
        GROUP BY month
        ORDER BY month
    """

    results["pruning_baseline"] = bench(
        spark, baseline, label="all years"
    )

    results["pruning_optimized"] = bench(
        spark, pruned, label="year=2024 (pruned)"
    )

    baseline_2024 = (
        spark.sql(baseline)
        .filter("year = 2024")
        .drop("year")
    )

    verify_same(
        baseline_2024,
        spark.sql(pruned),
        "partition pruning"
    )

    print("\n  EXPLAIN FORMATTED (pruned):")
    spark.sql(pruned).explain("formatted")


def exp_broadcast_join(
    spark: SparkSession,
    results: dict
) -> None:
    print_separator("EXP 3 – Broadcast Join")
    spark.catalog.clearCache()

    no_hint = """
        SELECT
            z.borough,
            COUNT(*) AS trip_count,
            AVG(t.trip_distance) AS avg_distance
        FROM trips t
        JOIN zone_lookup z
          ON t.pickup_location_id = z.location_id
        GROUP BY z.borough
        ORDER BY trip_count DESC
    """

    with_hint = """
        SELECT /*+ BROADCAST(z) */
            z.borough,
            COUNT(*) AS trip_count,
            AVG(t.trip_distance) AS avg_distance
        FROM trips t
        JOIN zone_lookup z
          ON t.pickup_location_id = z.location_id
        GROUP BY z.borough
        ORDER BY trip_count DESC
    """

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        "-1"
    )

    results["broadcast_baseline"] = bench(
        spark,
        no_hint,
        label="non-broadcast join"
    )

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        str(10 * 1024 * 1024)
    )

    results["broadcast_optimized"] = bench(
        spark,
        with_hint,
        label="broadcast hint"
    )

    verify_same(
        spark.sql(no_hint),
        spark.sql(with_hint),
        "broadcast join"
    )

    print("\n  EXPLAIN FORMATTED (broadcast):")
    spark.sql(with_hint).explain("formatted")


def exp_aqe(
    spark: SparkSession,
    results: dict
) -> None:
    print_separator("EXP 4 – Adaptive Query Execution")
    spark.catalog.clearCache()

    query = """
        SELECT
            z.borough,
            COUNT(*) AS trip_count,
            AVG(t.trip_distance) AS avg_distance
        FROM trips t
        JOIN zone_lookup z
          ON t.pickup_location_id = z.location_id
        GROUP BY z.borough
        ORDER BY trip_count DESC
    """

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        str(10 * 1024 * 1024)
    )

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "false"
    )

    results["aqe_off"] = bench(
        spark,
        query,
        label="AQE disabled"
    )

    aqe_off_result = spark.sql(query)

    print("\n  EXPLAIN FORMATTED (AQE off):")
    aqe_off_result.explain("formatted")

    aqe_off_rows = aqe_off_result.collect()

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    results["aqe_on"] = bench(
        spark,
        query,
        label="AQE enabled"
    )

    aqe_on_result = spark.sql(query)

    print("\n  EXPLAIN FORMATTED (AQE on):")
    aqe_on_result.explain("formatted")

    aqe_on_rows = aqe_on_result.collect()

    identical = sorted(map(str, aqe_off_rows)) == sorted(
        map(str, aqe_on_rows)
    )

    print(
        "  Results [AQE]:",
        "✓ identical" if identical else "✗ differ"
    )


def get_size(path: str) -> int:
    if not os.path.exists(path):
        return 0

    return sum(
        os.path.getsize(os.path.join(root, file))
        for root, _, files in os.walk(path)
        for file in files
    )


def format_size(size: int) -> str:
    if size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"

    if size < 1024 ** 3:
        return f"{size / 1024 ** 2:.2f} MB"

    return f"{size / 1024 ** 3:.2f} GB"


def measure_storage() -> None:
    print_separator("STORAGE OVERHEAD")

    integrated_size = get_size(INTEGRATED_TRIPS_PATH)

    print(
        f"  {'Integrated trips':<25}"
        f"{format_size(integrated_size):>12}"
    )

    product_total = 0

    for name, path in PRODUCT_PATHS.items():
        size = get_size(path)
        product_total += size

        print(
            f"  {name:<25}"
            f"{format_size(size):>12}"
        )

    print("-" * 40)

    print(
        f"  {'Analytical products':<25}"
        f"{format_size(product_total):>12}"
    )

    if integrated_size:
        overhead = product_total / integrated_size * 100

        print(
            f"  {'Product/source ratio':<25}"
            f"{overhead:>11.2f}%"
        )


def main() -> None:
    evaluate = "--evaluate" in sys.argv

    spark = get_spark("week2-optimization")
    configure_spark_temp_dir()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark ready:", spark.version)

    (
        spark.read
        .format("delta")
        .load(INTEGRATED_TRIPS_PATH)
        .createOrReplaceTempView("trips")
    )

    zones = (
        spark.read.format("delta")
        .load(SILVER_TAXI_ZONES_PATH)
        .select("location_id", "borough")
    )

    zones.createOrReplaceTempView("zone_lookup")

    print(f"Integrated trips: {spark.table('trips').count()}")
    print(f"Zone lookup rows: {zones.count()}")

    results = {}

    exp_caching(spark, results)
    exp_partition_pruning(spark, results)
    exp_broadcast_join(spark, results)
    exp_aqe(spark, results)

    summarise(
        results,
        [
            ("Caching", "caching_baseline", "caching_optimized"),
            ("Partition Pruning", "pruning_baseline", "pruning_optimized"),
            ("Broadcast Join", "broadcast_baseline", "broadcast_optimized"),
            ("AQE", "aqe_off", "aqe_on"),
        ]
    )

    if evaluate:
        measure_storage()

    spark.stop()


if __name__ == "__main__":
    main()
