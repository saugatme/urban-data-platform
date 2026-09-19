"""Reusable analytical data products for the integrated taxi dataset."""

from datetime import datetime
from pyspark.sql import functions as F

from src.analytics.constants import (
    INTEGRATED_TRIPS_PATH,
    PRODUCT_BASE_PATH,
    weather_condition_column,
)
from src.common.spark_session import get_spark
from src.analytics.runtime import configure_spark_temp_dir


SCHEMA_VERSION = "1.0"


def add_metadata(df):
    now = datetime.now()

    return (
        df
        .withColumn("data_source", F.lit("integrated_taxi_trips"))
        .withColumn("creation_time", F.lit(now).cast("timestamp"))
        .withColumn("refresh_time", F.lit(now).cast("timestamp"))
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
    )


def write_product(spark, df, name):
    path = f"{PRODUCT_BASE_PATH}/{name}"

    (
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    )

    # Count the written product instead of recomputing its source aggregation.
    row_count = spark.read.format("delta").load(path).count()
    print(f"{name}: {row_count} rows -> {path}")


def daily_mobility(trips):
    df = (
        trips
        .withColumn("trip_date", F.to_date("pickup_datetime"))
        .groupBy(
            "trip_date",
            "year",
            "month",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare")
        )
    )

    return add_metadata(df)


def taxi_zone_statistics(trips):
    df = (
        trips
        .groupBy(
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare")
        )
    )

    return add_metadata(df)


def weather_impact(trips):
    weather_condition = weather_condition_column()

    temp_bucket = (
        F.when(F.col("temp") < 5, "Cold (<5°C)")
        .when(F.col("temp") < 20, "Mild (5–20°C)")
        .otherwise("Hot (>=20°C)")
    )

    df = (
        trips
        .filter(
            F.col("temp").isNotNull()
            & F.col("condition_code").isNotNull()
            & (F.col("trip_distance") > 0)
        )
        .withColumn("temp_bucket", temp_bucket)
        .withColumn("weather_condition", weather_condition)
        .groupBy(
            "temp_bucket",
            "weather_condition"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("temp"), 2).alias("avg_temperature"),
            F.round(F.avg("precipitation"), 3).alias("avg_precipitation"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance_miles"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare")
        )
    )

    return add_metadata(df)


def air_quality_impact(trips):
    aqi_category = (
        F.when(F.col("pm25_hourly_avg") <= 12, "Good")
        .when(F.col("pm25_hourly_avg") <= 35.4, "Moderate")
        .when(
            F.col("pm25_hourly_avg") <= 55.4,
            "Unhealthy for Sensitive Groups"
        )
        .when(
            F.col("pm25_hourly_avg") <= 150.4,
            "Unhealthy"
        )
        .when(
            F.col("pm25_hourly_avg") <= 250.4,
            "Very Unhealthy"
        )
        .otherwise("Hazardous")
    )

    df = (
        trips
        .filter(
            F.col("pm25_hourly_avg").isNotNull()
            & (F.col("trip_distance") > 0)
        )
        .withColumn("aqi_category", aqi_category)
        .groupBy("aqi_category")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("pm25_hourly_avg"), 2).alias("avg_pm25"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare")
        )
    )

    return add_metadata(df)


def main():
    spark = get_spark("data-products")
    configure_spark_temp_dir()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark ready: {spark.version}")

    trips = (
        spark.read
        .format("delta")
        .load(INTEGRATED_TRIPS_PATH)
    )

    print(f"Integrated trips: {trips.count()}")

    products = {
        "daily_mobility": daily_mobility(trips),
        "taxi_zone_statistics": taxi_zone_statistics(trips),
        "weather_impact": weather_impact(trips),
        "air_quality_impact": air_quality_impact(trips),
    }

    for name, df in products.items():
        print(f"\n{name}")
        df.printSchema()
        write_product(spark, df, name)

    print("\nData products created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
