"""Benchmark all six Week 2 analytical queries on the integrated Delta table."""

from statistics import median
from time import perf_counter

from src.analytics.queries import (
    q1_monthly_demand,
    q2_weather_distance,
    q3_air_quality_demand,
    q4_zone_demand_variance,
    q5_peak_hours,
    q6_monthly_trend,
    register_trips,
)
from src.analytics.runtime import configure_spark_temp_dir
from src.common.spark_session import get_spark


QUERIES = {
    "Q1 monthly demand by zone": q1_monthly_demand,
    "Q2 weather and distance": q2_weather_distance,
    "Q3 air quality and demand": q3_air_quality_demand,
    "Q4 zone demand variance": q4_zone_demand_variance,
    "Q5 weekly peak hours": q5_peak_hours,
    "Q6 monthly demand trend": q6_monthly_trend,
}


def benchmark_query(spark, label, query) -> tuple[list[float], int]:
    """Collect a query three times and return warm-run timings and row count."""
    timings = []
    row_count = 0

    for _ in range(3):
        start = perf_counter()
        rows = query(spark).collect()
        timings.append(perf_counter() - start)
        row_count = len(rows)

    return timings, row_count


def main() -> None:
    configure_spark_temp_dir()
    spark = get_spark("week2-query-benchmark")
    spark.sparkContext.setLogLevel("WARN")
    register_trips(spark, "data")

    print("Week 2 analytical-query benchmark")
    print("Method: three runs; report median of runs 2-3 after warm-up.\n")
    print(f"{'Query':<32} {'Rows':>8} {'Runs (s)':>28} {'Median (s)':>12}")
    print("-" * 86)

    for label, query in QUERIES.items():
        timings, row_count = benchmark_query(spark, label, query)
        warm_median = median(timings[1:])
        shown = ", ".join(f"{value:.3f}" for value in timings)
        print(f"{label:<32} {row_count:>8} [{shown:>22}] {warm_median:>12.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
