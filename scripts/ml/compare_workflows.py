"""Compare raw-source and integrated-platform ML preparation workflows."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.analytics.runtime import configure_spark_temp_dir
from src.common.spark_session import get_spark
from src.ml.training import TARGET, build_feature_pipeline, build_training_dataset, chronological_split


def raw_training_dataset(spark, base: str = "data"):
    """Reimplement the preparation path directly from the original source files."""
    root = Path(base) / "raw"
    taxi = spark.read.parquet(str(root / "taxi_trips"))
    weather = spark.read.option("header", True).option("inferSchema", True).csv(str(root / "weather" / "weather.csv"))
    air = spark.read.option("header", True).option("inferSchema", True).csv(str(root / "air_quality" / "hourly_88101_2024.csv"))
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(root / "taxi_zones" / "taxi_zone_lookup.csv"))

    taxi = (
        taxi.select(
            F.col("tpep_pickup_datetime").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            "passenger_count",
            F.col("PULocationID").alias("pickup_location_id"),
        )
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & (F.col("dropoff_datetime") >= F.col("pickup_datetime"))
            & F.col("passenger_count").isNotNull()
            & (F.col("passenger_count") > 0)
        )
        .withColumn(TARGET, (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
        .filter(F.col(TARGET).between(1, 180))
    )

    weather_timestamp = F.make_timestamp("year", "month", "day", "hour", F.lit(0), F.lit(0))
    weather = (
        weather.withColumn("weather_timestamp", weather_timestamp)
        .select(
            "weather_timestamp", "temp", F.col("rhum").alias("rel_humidity"),
            F.col("prcp").alias("precipitation"), F.col("wspd").alias("wind_speed"),
            F.col("coco").alias("condition_code"),
        )
        .withColumn("row_number", F.row_number().over(Window.partitionBy("weather_timestamp").orderBy("temp")))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    air = (
        air.filter(
            (F.col("State Code") == 36) & (F.col("County Code") == 81)
            & (F.col("Site Num") == 124) & F.col("Sample Measurement").isNotNull()
        )
        .withColumn(
            "aq_timestamp",
            F.to_timestamp(
                F.concat(F.date_format("Date Local", "yyyy-MM-dd"), F.lit(" "), F.date_format("Time Local", "HH:mm")),
                "yyyy-MM-dd HH:mm",
            ),
        )
        .filter(F.col("aq_timestamp").isNotNull())
        .groupBy("aq_timestamp")
        .agg(F.avg("Sample Measurement").alias("pm25_hourly_avg"))
    )
    zones = zones.select(F.col("LocationID").alias("pickup_location_id"), F.col("Borough").alias("pickup_borough"))

    return (
        taxi.withColumn("pickup_hour_timestamp", F.date_trunc("hour", "pickup_datetime"))
        .join(F.broadcast(weather), F.col("pickup_hour_timestamp") == F.col("weather_timestamp"), "left")
        .drop("pickup_hour_timestamp", "weather_timestamp")
        .join(F.broadcast(air), F.date_trunc("hour", "pickup_datetime") == F.col("aq_timestamp"), "left")
        .drop("aq_timestamp")
        .join(F.broadcast(zones), "pickup_location_id", "left")
        .filter(F.col("pickup_borough").isNotNull())
    )


def fit_seconds(dataset, name: str) -> float:
    """Time lazy raw/integrated preparation and the same linear ML pipeline."""
    started = time.perf_counter()
    build_feature_pipeline("linear").fit(chronological_split(dataset).train)
    seconds = round(time.perf_counter() - started, 3)
    print(f"{name}: {seconds:.3f}s")
    return seconds


def main() -> None:
    configure_spark_temp_dir()
    spark = get_spark("week4-raw-vs-platform")
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw_seconds = fit_seconds(raw_training_dataset(spark), "Approach A - raw datasets")
        platform_seconds = fit_seconds(build_training_dataset(spark), "Approach B - integrated platform")
        report = {
            "raw_workflow_seconds": raw_seconds,
            "integrated_platform_seconds": platform_seconds,
            "reduction_percent": round((raw_seconds - platform_seconds) / raw_seconds * 100, 2),
            "note": "The raw source contains the original release; the integrated platform also includes Week 3 incremental records, so this is directional performance evidence.",
        }
        output = Path("data/benchmark/week4_ml/raw_vs_platform.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
