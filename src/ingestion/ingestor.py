"""Generic ingestion framework for CSV and Parquet urban datasets."""

import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, month, monotonically_increasing_id, row_number, to_timestamp, year
from pyspark.sql.window import Window

from src.ingestion.config import COLUMN_RENAMES, DATASETS


def load(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    raise ValueError(f"Unsupported format: {fmt}")


def standardize_columns(df: DataFrame, name: str) -> DataFrame:
    for source, target in COLUMN_RENAMES.get(name, {}).items():
        if source in df.columns and source != target:
            df = df.withColumnRenamed(source, target)
    return df


def validate_schema(df: DataFrame, required_columns: list[str], name: str) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{name}: missing required columns: {', '.join(missing)}")


def normalize_timestamps(df: DataFrame, timestamp_columns: list[str]) -> DataFrame:
    for column_name in timestamp_columns:
        if column_name in df.columns:
            df = df.withColumn(column_name, to_timestamp(col(column_name)))
    return df


def add_partition_columns(df: DataFrame, name: str) -> DataFrame:
    if name == "taxi_trips":
        return df.withColumn("year", year("pickup_datetime")).withColumn("month", month("pickup_datetime"))
    if name == "air_quality":
        return df.withColumn("year", year("date_local")).withColumn("month", month("date_local"))
    return df


def _empty_rejected(df: DataFrame) -> DataFrame:
    return df.limit(0).withColumn("rejection_reason", lit("").cast("string"))


def _combine_rejections(df: DataFrame, rejected_parts: list[DataFrame]) -> DataFrame:
    if not rejected_parts:
        return _empty_rejected(df)
    rejected = rejected_parts[0]
    for part in rejected_parts[1:]:
        rejected = rejected.unionByName(part, allowMissingColumns=True)
    return rejected


def validate(
    df: DataFrame,
    primary_key: list[str],
    timestamp_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Return valid and rejected rows while preserving one row per duplicate key."""
    rejected_parts = []

    if primary_key:
        null_condition = " OR ".join(f"{key} IS NULL" for key in primary_key)
        null_keys = df.filter(null_condition)
        df = df.filter(f"NOT ({null_condition})")
        rejected_parts.append(null_keys.withColumn("rejection_reason", lit("null_primary_key")))

    present_timestamps = [name for name in timestamp_columns if name in df.columns]
    if present_timestamps:
        invalid_condition = " OR ".join(f"{name} IS NULL" for name in present_timestamps)
        invalid_timestamps = df.filter(invalid_condition)
        df = df.filter(f"NOT ({invalid_condition})")
        rejected_parts.append(invalid_timestamps.withColumn("rejection_reason", lit("invalid_timestamp")))

    if primary_key:
        window = Window.partitionBy(*primary_key).orderBy(monotonically_increasing_id())
        numbered = df.withColumn("_duplicate_rank", row_number().over(window))
        duplicate_rows = numbered.filter(col("_duplicate_rank") > 1).drop("_duplicate_rank")
        df = numbered.filter(col("_duplicate_rank") == 1).drop("_duplicate_rank")
        rejected_parts.append(duplicate_rows.withColumn("rejection_reason", lit("duplicate_primary_key")))

    return df, _combine_rejections(df, rejected_parts)


def save_delta(df: DataFrame, name: str, layer: str, partition_by: list[str] | None, base: str) -> str:
    path = f"{base}/{layer}/{name}"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    return path


def log_metadata(
    spark: SparkSession,
    name: str,
    total: int,
    rejected: int,
    elapsed: float,
    layer: str,
    schema_version: str,
    base: str,
) -> None:
    log_df = spark.createDataFrame([{
        "dataset": name,
        "layer": layer,
        "processed_records": total,
        "accepted_records": total - rejected,
        "rejected_records": rejected,
        "execution_time_sec": round(elapsed, 2),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": schema_version,
    }])
    log_df.write.format("delta").mode("append").save(f"{base}/metadata/ingestion_log")


def ingest(spark: SparkSession, name: str, layer: str = "bronze", base: str = "data") -> DataFrame:
    cfg = DATASETS[name]
    started_at = time.time()

    print(f"\n{'=' * 50}\nIngesting: {name} -> {layer}")
    df = load(spark, cfg["path"], cfg["format"])
    total = df.count()
    print(f"  Loaded: {total:,} rows")

    df = standardize_columns(df, name)
    validate_schema(df, cfg["required_columns"], name)
    df = normalize_timestamps(df, cfg["timestamp_columns"])
    df = add_partition_columns(df, name)

    df, rejected_df = validate(df, cfg["primary_key"] or [], cfg["timestamp_columns"])
    invalid_by_rule = df.filter(~cfg["validity_condition"]())
    df = df.filter(cfg["validity_condition"]())
    invalid_by_rule = invalid_by_rule.withColumn("rejection_reason", lit("dataset_rule_failure"))
    rejected_df = _combine_rejections(df, [rejected_df, invalid_by_rule])

    rejected_count = rejected_df.count()
    if rejected_count:
        rejected_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{base}/rejected/{name}")
    print(f"  Rejected: {rejected_count:,} rows")

    path = save_delta(df, name, layer, cfg["partition_by"], base)
    elapsed = time.time() - started_at
    log_metadata(spark, name, total, rejected_count, elapsed, layer, cfg["schema_version"], base)
    print(f"  Saved to: {path}\n  Time: {elapsed:.1f}s")
    return df


def ingest_all(spark: SparkSession, layer: str = "bronze", base: str = "data") -> None:
    for name in DATASETS:
        ingest(spark, name, layer, base)
