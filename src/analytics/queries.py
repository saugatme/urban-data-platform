"""Analytical queries for the integrated urban taxi dataset."""

import statistics
import time

from pyspark.sql import DataFrame, SparkSession


from src.analytics.constants import weather_condition_sql

def register_trips(spark: SparkSession, base: str) -> None:
    trips = spark.read.format("delta").load(f"{base}/gold/integrated_taxi_trips")
    trips.createOrReplaceTempView("trips")


def q1_monthly_demand(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        SELECT year, month, pickup_location_id, pickup_zone, COUNT(*) AS taxi_demand
        FROM trips
        WHERE year > 2023
        GROUP BY year, month, pickup_location_id, pickup_zone
        ORDER BY taxi_demand DESC
    """)


def q2_weather_distance(spark: SparkSession) -> DataFrame:
    return spark.sql(f"""
        SELECT
            {weather_condition_sql()} AS weather_condition,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 3) AS avg_trip_distance
        FROM trips
        WHERE trip_distance > 0
        GROUP BY condition_code
        ORDER BY avg_trip_distance DESC
    """)


def q3_air_quality_demand(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        SELECT
            CASE
                WHEN pm25_hourly_avg < 10 THEN '0-10'
                WHEN pm25_hourly_avg < 20 THEN '10-20'
                WHEN pm25_hourly_avg < 30 THEN '20-30'
                WHEN pm25_hourly_avg < 50 THEN '30-50'
                ELSE '50+'
            END AS pm25_range,
            COUNT(*) AS taxi_demand,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance
        FROM trips
        WHERE pm25_hourly_avg IS NOT NULL
        GROUP BY
            CASE
                WHEN pm25_hourly_avg < 10 THEN '0-10'
                WHEN pm25_hourly_avg < 20 THEN '10-20'
                WHEN pm25_hourly_avg < 30 THEN '20-30'
                WHEN pm25_hourly_avg < 50 THEN '30-50'
                ELSE '50+'
            END
        ORDER BY pm25_range
    """)


def q4_zone_demand_variance(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        WITH weather_demand AS (
            SELECT pickup_zone, condition_code, COUNT(*) AS demand
            FROM trips
            WHERE pickup_zone IS NOT NULL AND condition_code IS NOT NULL
            GROUP BY pickup_zone, condition_code
        )
        SELECT
            pickup_zone,
            ROUND(STDDEV(demand), 2) AS demand_variation,
            MIN(demand)              AS min_demand,
            MAX(demand)              AS max_demand,
            ROUND(AVG(demand), 2)   AS avg_demand
        FROM weather_demand
        GROUP BY pickup_zone
        ORDER BY demand_variation DESC
    """)


def q5_peak_hours(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        WITH hourly AS (
            SELECT
                DAYOFWEEK(pickup_datetime)            AS day_of_week,
                DATE_FORMAT(pickup_datetime, 'EEEE')  AS day_name,
                HOUR(pickup_datetime)                 AS hour_of_day,
                COUNT(*)                              AS trip_count
            FROM trips
            WHERE pickup_datetime IS NOT NULL
            GROUP BY
                DAYOFWEEK(pickup_datetime),
                DATE_FORMAT(pickup_datetime, 'EEEE'),
                HOUR(pickup_datetime)
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY day_of_week ORDER BY trip_count DESC) AS rn
            FROM hourly
        )
        SELECT
            day_name,
            CONCAT(LPAD(CAST(hour_of_day AS STRING), 2, '0'), ':00') AS peak_hour,
            trip_count
        FROM ranked
        WHERE rn = 1
        ORDER BY day_of_week
    """)


def q6_monthly_trend(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        WITH monthly AS (
            SELECT month, COUNT(*) AS taxi_demand
            FROM trips
            WHERE year = 2024
            GROUP BY month
        ),
        with_previous AS (
            SELECT
                month,
                taxi_demand,
                LAG(taxi_demand) OVER (ORDER BY month) AS previous_month_demand
            FROM monthly
        )
        SELECT
            month,
            taxi_demand,
            ROUND(100.0 * (taxi_demand - previous_month_demand) / previous_month_demand, 2) AS mom_change_pct
        FROM with_previous
        ORDER BY month
    """)


def bench(spark: SparkSession, sql: str, n: int = 3, label: str = "") -> float:
    times = []
    for _ in range(n):
        start = time.time()
        spark.sql(sql).collect()
        times.append(round(time.time() - start, 2))
    med = round(statistics.median(times), 2)
    print(f"{label}: runs={times}  median={med}s")
    return med


def verify_same(spark: SparkSession, sql1: str, sql2: str) -> bool:
    df1 = spark.sql(sql1)
    df2 = spark.sql(sql2)
    diff = df1.subtract(df2).count() + df2.subtract(df1).count()
    print("Results identical:", diff == 0)
    return diff == 0