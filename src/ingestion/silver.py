"""Common data model transformations for the silver layer."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, concat, date_format, lit, to_timestamp, trim
from pyspark.sql.types import StringType


def enforce_common_model(df: DataFrame) -> DataFrame:
    total = df.count()
    for column_name in df.columns:
        if df.filter(col(column_name).isNull()).count() == total:
            df = df.drop(column_name)
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


def silver_taxi_trips(df: DataFrame) -> DataFrame:
    for column_name in ["rate_code_id", "payment_type", "passenger_count"]:
        df = df.withColumn(column_name, col(column_name).cast("integer"))
    return df


def silver_weather(df: DataFrame) -> DataFrame:
    for column_name in ["snwd", "snwd_source", "wpgt", "wpgt_source"]:
        if column_name in df.columns:
            df = df.drop(column_name)
    return (
        df.withColumn("precipitation", coalesce(col("precipitation"), lit(0.0)))
        .withColumn("condition_code", coalesce(col("condition_code"), lit(-1)))
    )


def silver_air_quality(df: DataFrame) -> DataFrame:
    if "uncertainty" in df.columns:
        df = df.drop("uncertainty")
    df = df.withColumn(
        "aq_timestamp",
        to_timestamp(
            concat(
                date_format(col("date_local"), "yyyy-MM-dd"), lit(" "),
                date_format(col("time_local"), "HH:mm"),
            ),
            "yyyy-MM-dd HH:mm",
        ),
    )
    return df.drop("time_local", "time_gmt", "date_gmt")


SILVER_CONFIG = {
    "taxi_trips": {"transform": silver_taxi_trips, "partition_by": ["year", "month"]},
    "weather": {"transform": silver_weather, "partition_by": None},
    "air_quality": {"transform": silver_air_quality, "partition_by": ["year", "month"]},
    "taxi_zones": {"transform": lambda df: df, "partition_by": None},
}


def build_silver(spark: SparkSession, name: str, base: str = "data") -> None:
    df = spark.read.format("delta").load(f"{base}/bronze/{name}")
    df = SILVER_CONFIG[name]["transform"](enforce_common_model(df))

    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    partition_by = SILVER_CONFIG[name]["partition_by"]
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(f"{base}/silver/{name}")


def build_all_silver(spark: SparkSession, base: str = "data") -> None:
    for name in SILVER_CONFIG:
        build_silver(spark, name, base)