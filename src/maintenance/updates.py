"""Create realistic incremental source-data releases."""

from datetime import timedelta
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from src.common.spark_session import get_spark
from src.ingestion.config import DATASETS, UPDATE_DATASETS


# Reads a CSV file with its header row and detected column types.
def _read_csv(spark: SparkSession, path: Path):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


# Creates new taxi trips after the latest source trip and adds copied duplicates.
def _taxi_update(taxi):
    first_pickup, last_pickup = taxi.agg(
        F.min("tpep_pickup_datetime"), F.max("tpep_pickup_datetime")
    ).first()
    shift_days = (last_pickup.date() - first_pickup.date()).days + 2

    new = (
        taxi.sample(False, 0.055, seed=42)
        .withColumn("tpep_pickup_datetime", F.expr(f"tpep_pickup_datetime + INTERVAL {shift_days} DAYS"))
        .withColumn("tpep_dropoff_datetime", F.expr(f"tpep_dropoff_datetime + INTERVAL {shift_days} DAYS"))
    )
    duplicates = taxi.sample(False, 0.0009, seed=7)
    return new.unionByName(duplicates), new.count(), duplicates.count()


# Creates 168 new hourly weather observations and adds humidity.
def _weather_update(spark: SparkSession, weather):
    templates = weather.orderBy(F.rand(42)).limit(168).withColumn(
        "id", F.row_number().over(Window.orderBy(F.rand(42))) - 1
    )
    last_time = weather.agg(
        F.max(F.expr("make_timestamp(year, month, day, hour, 0, 0)"))
    ).first()[0]
    hours = spark.range(168).select(
        "id", F.expr(f"timestampadd(HOUR, id + 1, TIMESTAMP '{last_time}')").alias("time")
    )

    return (
        templates.join(hours, "id")
        .withColumn("year", F.year("time"))
        .withColumn("month", F.month("time"))
        .withColumn("day", F.dayofmonth("time"))
        .withColumn("hour", F.hour("time"))
        .withColumn("humidity", F.greatest(F.lit(20.0), F.least(F.lit(100.0), F.col("rhum").cast("double"))))
        .drop("id", "time")
    )


# Creates 168 new hourly air-quality observations and adds AQI.
def _air_update(spark: SparkSession, air):
    templates = air.filter(F.col("Sample Measurement").between(0, 250)).orderBy(F.rand(42)).limit(168).withColumn(
        "id", F.row_number().over(Window.orderBy(F.rand(42))) - 1
    )
    first_time = air.agg(F.max("Date Local")).first()[0] + timedelta(days=1)
    hours = spark.range(168).select(
        "id", F.expr(f"timestampadd(HOUR, id, TIMESTAMP '{first_time} 00:00:00')").alias("time")
    )

    return (
        templates.join(hours, "id")
        .withColumn("Date Local", F.to_date("time"))
        .withColumn("Time Local", F.col("time"))
        .withColumn("Date GMT", F.to_date(F.expr("time + INTERVAL 5 HOURS")))
        .withColumn("Time GMT", F.expr("time + INTERVAL 5 HOURS"))
        .withColumn("aqi", F.round(F.least(F.lit(500.0), F.greatest(F.lit(0.0), F.col("Sample Measurement") * 2))))
        .drop("id", "time")
    )


# Loads sources and output locations from configuration, then writes the releases.
def generate_updates(spark: SparkSession) -> None:
    taxi_source = DATASETS["taxi_trips"]
    weather_source = DATASETS["weather"]
    air_source = DATASETS["air_quality"]

    taxi = spark.read.format(taxi_source["format"]).load(taxi_source["path"])
    weather = _read_csv(spark, Path(weather_source["path"]))
    air = _read_csv(spark, Path(air_source["path"]))

    taxi, new_count, duplicate_count = _taxi_update(taxi)
    weather = _weather_update(spark, weather)
    air = _air_update(spark, air)

    taxi.write.mode("overwrite").format(UPDATE_DATASETS["taxi_trips"]["format"]).save(UPDATE_DATASETS["taxi_trips"]["path"])
    weather.write.mode("overwrite").option("header", True).format(UPDATE_DATASETS["weather"]["format"]).save(UPDATE_DATASETS["weather"]["path"])
    air.write.mode("overwrite").option("header", True).format(UPDATE_DATASETS["air_quality"]["format"]).save(UPDATE_DATASETS["air_quality"]["path"])

    print(f"Taxi: {new_count} new, {duplicate_count} duplicates")
    print(f"Weather: {weather.count()} records, added humidity")
    print(f"Air quality: {air.count()} records, added aqi")
    print("Update files created.")


# Starts Spark, runs the generator, then closes Spark.
def main() -> None:
    spark = get_spark("update-generator")
    generate_updates(spark)
    spark.stop()


if __name__ == "__main__":
    main()