"""
Task 4 — Common Data Model
Reads from bronze, applies standard transformations, writes to silver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, concat, date_format, lit, coalesce
)



def enforce_common_model(df: DataFrame) -> DataFrame:
    """Drop fully-null columns, strip leading/trailing whitespace from strings."""
    total = df.count()
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count == total:
            df = df.drop(c)
    from pyspark.sql.types import StringType
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df



def silver_taxi_trips(df: DataFrame) -> DataFrame:
    for c in ["rate_code_id", "payment_type", "passenger_count"]:
        df = df.withColumn(c, col(c).cast("integer"))
    return df


def silver_weather(df: DataFrame) -> DataFrame:
    null_cols = ["snwd", "snwd_source", "wpgt", "wpgt_source"]
    for c in null_cols:
        if c in df.columns:
            df = df.drop(c)
    df = df.withColumn("precipitation", coalesce(col("precipitation"), lit(0.0)))
    df = df.withColumn("condition_code", coalesce(col("condition_code"), lit(-1)))
    return df


def silver_air_quality(df: DataFrame) -> DataFrame:
    if "uncertainty" in df.columns:
        df = df.drop("uncertainty")

    if "time_local" in df.columns and "date_local" in df.columns:
        df = df.withColumn(
            "aq_timestamp",
            to_timestamp(
                concat(date_format(col("date_local"), "yyyy-MM-dd"), lit(" "), date_format(col("time_local"), "HH:mm")),
                "yyyy-MM-dd HH:mm"
            )
        )

    if "time_gmt" in df.columns and "date_gmt" in df.columns:
        df = df.drop("time_gmt", "date_gmt")

    df = df.withColumn(
        "sample_measurement",
        coalesce(col("sample_measurement"), lit(0.0))
    )
    return df


def silver_taxi_zones(df: DataFrame) -> DataFrame:
    return df



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

    df = enforce_common_model(df)

    df = SILVER_CONFIG[name]["transform"](df)

    after = df.count()
    print(f"  Rows: {before:,} → {after:,}")
    print(f"  Cols: {len(df.columns)}")

    partition_by = SILVER_CONFIG[name]["partition_by"]
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(f"{base}/silver/{name}")
    print(f"  Saved to: {base}/silver/{name}")


def build_all_silver(spark: SparkSession, base: str = "data"):
    for name in SILVER_CONFIG:
        build_silver(spark, name, base)
