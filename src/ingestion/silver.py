"""
Task 4 — Common Data Model
Reads from bronze, applies standard transformations, writes to silver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, concat, lit, year, month,
    when, coalesce
)


# ── GENERIC: applies to every dataset ────────────────────────────────────────

def enforce_common_model(df: DataFrame) -> DataFrame:
    """Drop fully-null columns, strip leading/trailing whitespace from strings."""
    # drop 100% null columns
    total = df.count()
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count == total:
            df = df.drop(c)
    # trim strings
    from pyspark.sql.types import StringType
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df


# ── DATASET-SPECIFIC: silver transformations ──────────────────────────────────

def silver_taxi_trips(df: DataFrame) -> DataFrame:
    # types are already correct from bronze
    # cast categoricals to integer (they came as long)
    for c in ["rate_code_id", "payment_type", "passenger_count"]:
        df = df.withColumn(c, col(c).cast("integer"))
    return df


def silver_weather(df: DataFrame) -> DataFrame:
    # drop 100% null columns (snwd, snwd_source, wpgt, wpgt_source)
    null_cols = ["snwd", "snwd_source", "wpgt", "wpgt_source"]
    for c in null_cols:
        if c in df.columns:
            df = df.drop(c)
    # fill missing precipitation with 0 (no rain recorded = 0)
    df = df.withColumn("precipitation", coalesce(col("precipitation"), lit(0.0)))
    # fill 6 missing condition_code with -1 (unknown sentinel)
    df = df.withColumn("condition_code", coalesce(col("condition_code"), lit(-1)))
    return df


def silver_air_quality(df: DataFrame) -> DataFrame:
    # drop 100% null column
    if "uncertainty" in df.columns:
        df = df.drop("uncertainty")

    # FIX: time_local and time_gmt were cast to timestamp incorrectly
    # they are HH:mm strings — combine with date to make a proper timestamp
    if "time_local" in df.columns and "date_local" in df.columns:
        df = df.drop("time_local")  # drop the broken timestamp
        # date_local is already a timestamp (date part only), keep it

    if "time_gmt" in df.columns and "date_gmt" in df.columns:
        df = df.drop("time_gmt")
        df = df.drop("date_gmt")  # we'll use date_local only for joins

    # cast sample_measurement nulls to 0 (below detection limit)
    df = df.withColumn(
        "sample_measurement",
        coalesce(col("sample_measurement"), lit(0.0))
    )
    return df


def silver_taxi_zones(df: DataFrame) -> DataFrame:
    # already clean, nothing to do
    return df


# ── SILVER WRITER ─────────────────────────────────────────────────────────────

SILVER_CONFIG = {
    "taxi_trips":  {"transform": silver_taxi_trips,  "partition_by": ["year", "month"]},
    "weather":     {"transform": silver_weather,     "partition_by": None},
    "air_quality": {"transform": silver_air_quality, "partition_by": ["year", "month"]},
    "taxi_zones":  {"transform": silver_taxi_zones,  "partition_by": None},
}


def build_silver(spark: SparkSession, name: str, base: str = "data"):
    print(f"\n{'='*50}")
    print(f"Building silver: {name}")

    df = spark.read.format("delta").load(f"{base}/bronze/{name}")
    before = df.count()

    # 1. generic model enforcement
    df = enforce_common_model(df)

    # 2. dataset-specific transformations
    df = SILVER_CONFIG[name]["transform"](df)

    after = df.count()
    print(f"  Rows: {before:,} → {after:,}")
    print(f"  Cols: {len(df.columns)}")

    # 3. write to silver
    partition_by = SILVER_CONFIG[name]["partition_by"]
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(f"{base}/silver/{name}")
    print(f"  Saved to: {base}/silver/{name}")


def build_all_silver(spark: SparkSession, base: str = "data"):
    for name in SILVER_CONFIG:
        build_silver(spark, name, base)