"""Synthesise a second data release for each dataset.

The course supplies only one release, so Week 3 needs a realistic second one.
Rather than inventing values, this script samples existing rows and perturbs
them, which preserves each dataset's real distributions.  Every generated file
is written to ``data/incoming/`` so it is obviously separate from the original
release under ``data/raw/``.

What is produced, matching the Week 3 brief:

* **taxi_trips** — Parquet, ~7% new trips timestamped after the latest existing
  trip, plus ~1.5% rows copied verbatim so duplicate handling can be proven.
* **weather** — CSV of new hourly rows continuing the series, with a new
  ``humidity`` column (20-100).
* **air_quality** — CSV of new hourly rows continuing the series, with a new
  ``aqi`` column (0-500).

Files are written with the **raw** column names, so the incremental pipeline
puts them through exactly the same standardisation as the first release.

Counts are printed and written to ``data/incoming/generation_summary.json`` so
the Week 3 documentation cites measured numbers rather than estimates.
"""

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from src.common.spark_session import get_spark  # noqa: E402

NEW_TRIP_FRACTION = 0.07
DUPLICATE_FRACTION = 0.015
WEATHER_NEW_HOURS = 168          # one week
AIR_QUALITY_NEW_HOURS = 168
SEED = 42

INCOMING = "data/incoming"


def _sequence_column(df: DataFrame) -> DataFrame:
    """Add a deterministic 1-based sequence number."""
    return df.withColumn(
        "_seq",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id())),
    )


def generate_taxi_trips(spark: SparkSession, summary: dict) -> None:
    source = spark.read.parquet("data/raw/taxi_trips/")
    total = source.count()

    max_pickup = source.agg(F.max("tpep_pickup_datetime").alias("m")).collect()[0]["m"]
    print(f"taxi_trips: {total:,} existing rows, latest pickup {max_pickup}")

    # New trips: sampled rows shifted to start after the latest existing trip,
    # spread across the following week, with distance and fare lightly jittered.
    sample = source.sample(withReplacement=False, fraction=NEW_TRIP_FRACTION, seed=SEED)
    sample = _sequence_column(sample)

    duration = F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    offset_seconds = (F.col("_seq") % (7 * 24 * 60)) * 60

    new_trips = (
        sample
        .withColumn("_duration", duration)
        .withColumn(
            "tpep_pickup_datetime",
            (F.lit(max_pickup).cast("timestamp").cast("long") + offset_seconds + F.lit(60)).cast("timestamp"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            (F.col("tpep_pickup_datetime").cast("long") + F.col("_duration")).cast("timestamp"),
        )
        .withColumn(
            "trip_distance",
            F.round(F.col("trip_distance") * (F.lit(0.9) + F.rand(SEED) * F.lit(0.2)), 2),
        )
        .drop("_seq", "_duration")
    )

    # Duplicates: copied verbatim, so the pipeline must recognise them as
    # already present rather than inserting them again.
    duplicates = source.sample(withReplacement=False, fraction=DUPLICATE_FRACTION, seed=SEED + 1)

    update = new_trips.unionByName(duplicates)
    new_count = new_trips.count()
    duplicate_count = duplicates.count()

    update.write.mode("overwrite").parquet(f"{INCOMING}/taxi_trips/")

    summary["taxi_trips"] = {
        "existing_rows": total,
        "new_rows": new_count,
        "duplicate_rows": duplicate_count,
        "total_rows_in_update": new_count + duplicate_count,
        "new_pct_of_existing": round(new_count / total * 100, 2),
        "duplicate_pct_of_existing": round(duplicate_count / total * 100, 2),
        "latest_existing_pickup": str(max_pickup),
        "schema_changes": [],
        "format": "parquet",
        "path": f"{INCOMING}/taxi_trips/",
    }
    print(f"  -> {new_count:,} new + {duplicate_count:,} duplicates")


def generate_weather(spark: SparkSession, summary: dict) -> None:
    source = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "data/raw/weather/weather.csv"
    )
    total = source.count()

    latest = (
        source.orderBy(
            F.col("year").desc(), F.col("month").desc(), F.col("day").desc(), F.col("hour").desc()
        )
        .limit(1)
        .collect()[0]
    )
    print(f"weather: {total:,} existing rows, latest {latest['year']}-{latest['month']}-{latest['day']} {latest['hour']}h")

    base_ts = F.to_timestamp(
        F.concat_ws(
            " ",
            F.concat_ws("-", F.lit(latest["year"]), F.lpad(F.lit(latest["month"]), 2, "0"), F.lpad(F.lit(latest["day"]), 2, "0")),
            F.concat(F.lpad(F.lit(latest["hour"]), 2, "0"), F.lit(":00:00")),
        )
    )

    # Sample real rows for their meteorological values, then re-stamp them onto
    # the hours that follow the end of the existing series.
    sample = _sequence_column(
        source.sample(withReplacement=True, fraction=1.5, seed=SEED).limit(WEATHER_NEW_HOURS)
    )

    new_rows = (
        sample
        .withColumn("_ts", (base_ts.cast("long") + F.col("_seq") * F.lit(3600)).cast("timestamp"))
        .withColumn("year", F.year("_ts"))
        .withColumn("month", F.month("_ts"))
        .withColumn("day", F.dayofmonth("_ts"))
        .withColumn("hour", F.hour("_ts"))
        # Documented schema evolution for release 2.
        .withColumn("humidity", F.round(F.lit(20) + F.rand(SEED) * F.lit(80), 1))
        .drop("_ts", "_seq")
    )

    count = new_rows.count()
    new_rows.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{INCOMING}/weather_csv"
    )
    _collapse_csv(f"{INCOMING}/weather_csv", f"{INCOMING}/weather/weather_update.csv")

    summary["weather"] = {
        "existing_rows": total,
        "new_rows": count,
        "duplicate_rows": 0,
        "total_rows_in_update": count,
        "schema_changes": ["humidity (new, 20-100)"],
        "format": "csv",
        "path": f"{INCOMING}/weather/weather_update.csv",
    }
    print(f"  -> {count:,} new hourly rows, + humidity column")


def generate_air_quality(spark: SparkSession, summary: dict) -> None:
    source = spark.read.option("header", "true").option("inferSchema", "true").csv(
        "data/raw/air_quality/hourly_88101_2024.csv"
    )
    total = source.count()

    max_date = source.agg(F.max("Date Local").alias("m")).collect()[0]["m"]
    print(f"air_quality: {total:,} existing rows, latest date {max_date}")

    sample = _sequence_column(
        source.sample(withReplacement=True, fraction=1.5, seed=SEED).limit(AIR_QUALITY_NEW_HOURS)
    )

    base_ts = F.to_timestamp(F.lit(str(max_date)))

    new_rows = (
        sample
        .withColumn("_ts", (base_ts.cast("long") + F.col("_seq") * F.lit(3600)).cast("timestamp"))
        .withColumn("Date Local", F.date_format("_ts", "yyyy-MM-dd"))
        .withColumn("Time Local", F.date_format("_ts", "HH:mm"))
        # Documented schema evolution for release 2.
        .withColumn("aqi", F.round(F.rand(SEED) * F.lit(500)).cast("int"))
        .drop("_ts", "_seq")
    )

    count = new_rows.count()
    new_rows.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"{INCOMING}/air_quality_csv"
    )
    _collapse_csv(f"{INCOMING}/air_quality_csv", f"{INCOMING}/air_quality/air_quality_update.csv")

    summary["air_quality"] = {
        "existing_rows": total,
        "new_rows": count,
        "duplicate_rows": 0,
        "total_rows_in_update": count,
        "schema_changes": ["aqi (new, 0-500)"],
        "format": "csv",
        "path": f"{INCOMING}/air_quality/air_quality_update.csv",
    }
    print(f"  -> {count:,} new hourly rows, + aqi column")


def _collapse_csv(spark_dir: str, target_file: str) -> None:
    """Move Spark's single part-file to a plain CSV path and drop the directory."""
    import shutil

    source_dir = Path(spark_dir)
    target = Path(target_file)
    target.parent.mkdir(parents=True, exist_ok=True)

    part = next(source_dir.glob("part-*.csv"))
    shutil.move(str(part), str(target))
    shutil.rmtree(source_dir)


def main() -> None:
    spark = get_spark("generate-incremental-updates")
    spark.sparkContext.setLogLevel("WARN")

    summary: dict = {}
    generate_taxi_trips(spark, summary)
    generate_weather(spark, summary)
    generate_air_quality(spark, summary)

    Path(INCOMING).mkdir(parents=True, exist_ok=True)
    summary_path = Path(INCOMING) / "generation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    print(f"\nSummary written to {summary_path}")
    print(json.dumps(summary, indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
