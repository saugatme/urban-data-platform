"""Query optimization experiments for the integrated urban taxi dataset."""

import time
import statistics

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import get_spark


TRIPS_PATH = "data/gold/integrated_taxi_trips"
ZONES_PATH = "data/raw/taxi_zones/taxi_zone_lookup.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bench(
    spark: SparkSession,
    sql: str,
    n: int = 3,
    label: str = ""
) -> float:
    """Run a query several times and report median after warm-up."""

    times = []

    for _ in range(n):
        start = time.perf_counter()

        spark.sql(sql).collect()

        times.append(time.perf_counter() - start)

    # Ignore first run because it may include warm-up / initialization cost.
    measured = times[1:]

    med = round(statistics.median(measured), 3)

    print(
        f"  {label:<40} "
        f"median={med:.3f}s  "
        f"runs={[round(t, 3) for t in times]}"
    )

    return med


def verify_same(
    df1: DataFrame,
    df2: DataFrame,
    label: str = ""
) -> bool:

    diff = (
        df1.subtract(df2).count()
        + df2.subtract(df1).count()
    )

    status = (
        "✓ identical"
        if diff == 0
        else f"✗ differ by {diff} rows"
    )

    print(f"  Results [{label}]: {status}")

    return diff == 0


def print_separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(title)
    print(f"{'=' * 60}")


def summarise(results: dict, pairs: list[tuple]) -> None:

    print_separator("SUMMARY")

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

        pct = (baseline - optimized) / baseline * 100

        print(
            f"  {name:<30}"
            f"{baseline:>11.3f}s"
            f"{optimized:>11.3f}s"
            f"{pct:>13.2f}%"
        )


# ---------------------------------------------------------------------------
# EXPERIMENT 1 – Caching
# ---------------------------------------------------------------------------

def exp_caching(
    spark: SparkSession,
    results: dict
) -> None:

    print_separator("EXP 1 – Caching")

    spark.catalog.clearCache()

    query = """
        SELECT
            year,
            month,
            COUNT(*) AS taxi_demand
        FROM trips
        WHERE year = 2024
        GROUP BY year, month
        ORDER BY year, month
    """

    # -------------------------------------------------------
    # Baseline
    # -------------------------------------------------------

    results["caching_baseline"] = bench(
        spark,
        query,
        label="no cache"
    )

    # -------------------------------------------------------
    # Cache only the required columns and rows.
    #
    # This is important because caching SELECT * from the
    # entire 2024 dataset can consume too much memory.
    # -------------------------------------------------------

    cached = (
        spark.sql("""
            SELECT
                year,
                month
            FROM trips
            WHERE year = 2024
        """)
        .cache()
    )

    cached.createOrReplaceTempView("trips_cached")

    # Materialise the cache.
    cached.count()

    cached_query = """
        SELECT
            year,
            month,
            COUNT(*) AS taxi_demand
        FROM trips_cached
        GROUP BY year, month
        ORDER BY year, month
    """

    results["caching_optimized"] = bench(
        spark,
        cached_query,
        label="cached"
    )

    # -------------------------------------------------------
    # Verify correctness
    # -------------------------------------------------------

    verify_same(
        spark.sql(query),
        spark.sql(cached_query),
        label="caching"
    )

    # -------------------------------------------------------
    # Physical plan
    # -------------------------------------------------------

    print("\n  EXPLAIN (cached):")
    spark.sql(cached_query).explain("formatted")

    # Clear cache before next experiment.
    spark.catalog.clearCache()


# ---------------------------------------------------------------------------
# EXPERIMENT 2 – Partition Pruning
# ---------------------------------------------------------------------------

def exp_partition_pruning(
    spark: SparkSession,
    results: dict
) -> None:

    print_separator("EXP 2 – Partition Pruning")

    spark.catalog.clearCache()

    # Baseline: scans all years.
    baseline = """
        SELECT
            year,
            month,
            COUNT(*) AS taxi_demand
        FROM trips
        GROUP BY year, month
        ORDER BY year, month
    """

    # Optimized: filters on partition column year.
    pruned = """
        SELECT
            month,
            COUNT(*) AS taxi_demand
        FROM trips
        WHERE year = 2024
        GROUP BY month
        ORDER BY month
    """

    results["pruning_baseline"] = bench(
        spark,
        baseline,
        label="all years"
    )

    results["pruning_optimized"] = bench(
        spark,
        pruned,
        label="year=2024 (pruned)"
    )

    # -------------------------------------------------------
    # Verify correctness
    # -------------------------------------------------------

    baseline_2024 = (
        spark.sql(baseline)
        .filter("year = 2024")
        .drop("year")
    )

    verify_same(
        baseline_2024,
        spark.sql(pruned),
        label="partition pruning"
    )

    # -------------------------------------------------------
    # Physical plan
    # -------------------------------------------------------

    print("\n  EXPLAIN (pruned):")
    spark.sql(pruned).explain("formatted")


# ---------------------------------------------------------------------------
# EXPERIMENT 3 – Broadcast Join
# ---------------------------------------------------------------------------

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

    # -------------------------------------------------------
    # Baseline: disable automatic broadcast.
    # This forces a non-broadcast join.
    # -------------------------------------------------------

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        "-1"
    )

    results["broadcast_baseline"] = bench(
        spark,
        no_hint,
        label="non-broadcast join"
    )

    # -------------------------------------------------------
    # Optimized: enable broadcast.
    # -------------------------------------------------------

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        str(10 * 1024 * 1024)
    )

    results["broadcast_optimized"] = bench(
        spark,
        with_hint,
        label="broadcast hint"
    )

    # -------------------------------------------------------
    # Verify correctness
    # -------------------------------------------------------

    verify_same(
        spark.sql(no_hint),
        spark.sql(with_hint),
        label="broadcast join"
    )

    # -------------------------------------------------------
    # Physical plan
    # -------------------------------------------------------

    print("\n  EXPLAIN (broadcast):")
    spark.sql(with_hint).explain("formatted")


# ---------------------------------------------------------------------------
# EXPERIMENT 4 – Adaptive Query Execution
# ---------------------------------------------------------------------------

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

    # Keep the small zone table eligible for broadcast.
    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        str(10 * 1024 * 1024)
    )

    # -------------------------------------------------------
    # AQE OFF
    # -------------------------------------------------------

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "false"
    )

    results["aqe_off"] = bench(
        spark,
        query,
        label="AQE disabled"
    )

    # Collect result while AQE is OFF.
    aqe_off_result = spark.sql(query).collect()

    print("\n  EXPLAIN (AQE off):")
    spark.sql(query).explain("formatted")

    # -------------------------------------------------------
    # AQE ON
    # -------------------------------------------------------

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    results["aqe_on"] = bench(
        spark,
        query,
        label="AQE enabled"
    )

    # Collect result while AQE is ON.
    aqe_on_result = spark.sql(query).collect()

    print("\n  EXPLAIN (AQE on):")
    spark.sql(query).explain("formatted")

    # -------------------------------------------------------
    # Verify correctness
    # -------------------------------------------------------

    identical = aqe_off_result == aqe_on_result

    print(
        "  Results [AQE]:",
        "✓ identical" if identical else "✗ differ"
    )


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:

    spark = get_spark("week2-optimization")

    spark.sparkContext.setLogLevel("WARN")

    print("Spark ready:", spark.version)

    # -------------------------------------------------------
    # Load integrated Trips Delta table
    # -------------------------------------------------------

    (
        spark.read
        .format("delta")
        .load(TRIPS_PATH)
        .createOrReplaceTempView("trips")
    )

    # -------------------------------------------------------
    # Load Taxi Zone Lookup CSV
    # -------------------------------------------------------

    zones = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ZONES_PATH)
        .selectExpr(
            "LocationID AS location_id",
            "Borough AS borough"
        )
    )

    zones.createOrReplaceTempView("zone_lookup")

    print("\nTaxi Zone Lookup:")
    zones.printSchema()

    print(f"Zone lookup rows: {zones.count()}")

    # -------------------------------------------------------
    # Run experiments
    # -------------------------------------------------------

    results = {}

    exp_caching(spark, results)

    exp_partition_pruning(spark, results)

    exp_broadcast_join(spark, results)

    exp_aqe(spark, results)

    # -------------------------------------------------------
    # Summary
    # -------------------------------------------------------

    summarise(
        results,
        [
            (
                "Caching",
                "caching_baseline",
                "caching_optimized"
            ),
            (
                "Partition Pruning",
                "pruning_baseline",
                "pruning_optimized"
            ),
            (
                "Broadcast Join",
                "broadcast_baseline",
                "broadcast_optimized"
            ),
            (
                "AQE",
                "aqe_off",
                "aqe_on"
            ),
        ]
    )

    spark.stop()


if __name__ == "__main__":
    main()
