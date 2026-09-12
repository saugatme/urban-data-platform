"""
Generic ingestion framework — Task 3
Loads raw files → validates → standardizes → saves as Delta → logs metadata
"""

import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month

from src.ingestion.config import DATASETS, COLUMN_RENAMES



def load(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "parquet":
        return spark.read.parquet(path)
    elif fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")



def standardize_columns(df: DataFrame, name: str) -> DataFrame:
    renames = COLUMN_RENAMES.get(name, {})
    for old, new in renames.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def validate_schema(df: DataFrame, required_columns: list[str], name: str) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{name}: missing required columns: {', '.join(missing)}")


TIMESTAMP_COLS = {
    "taxi_trips":  ["pickup_datetime", "dropoff_datetime"],
    "air_quality": ["date_local", "date_gmt"],
    "weather":     [],   # year/month/day/hour kept as integers
    "taxi_zones":  [],
}

def normalize_timestamps(df: DataFrame, name: str) -> DataFrame:
    for c in TIMESTAMP_COLS.get(name, []):
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c)))
    return df



def add_partition_columns(df: DataFrame, name: str) -> DataFrame:
    """Add year/month from pickup_datetime for taxi_trips and air_quality."""
    if name == "taxi_trips" and "pickup_datetime" in df.columns:
        df = df.withColumn("year",  year(col("pickup_datetime"))) \
               .withColumn("month", month(col("pickup_datetime")))
    if name == "air_quality" and "date_local" in df.columns:
        df = df.withColumn("year",  year(col("date_local").cast("date"))) \
               .withColumn("month", month(col("date_local").cast("date")))
    return df



def validate(df: DataFrame, name: str, primary_key: list, timestamp_columns: list) -> tuple[DataFrame, DataFrame]:
    """
    Returns (valid_df, rejected_df).
    Checks: null primary keys, invalid timestamps, duplicate primary keys.
    """
    rejected_parts = []

    if primary_key:
        null_filter = " OR ".join([f"{k} IS NULL" for k in primary_key])
        nulls = df.filter(null_filter)
        df    = df.filter(f"NOT ({null_filter})")
        if nulls.count() > 0:
            rejected_parts.append(nulls.withColumn("rejection_reason",
                                  col(primary_key[0]).cast("string") if False
                                  else __import__("pyspark.sql.functions",
                                  fromlist=["lit"]).lit("null_primary_key")))

    timestamp_columns = [c for c in timestamp_columns if c in df.columns]
    if timestamp_columns:
        timestamp_filter = " OR ".join([f"{c} IS NULL" for c in timestamp_columns])
        invalid_timestamps = df.filter(timestamp_filter)
        df = df.filter(f"NOT ({timestamp_filter})")
        if invalid_timestamps.count() > 0:
            from pyspark.sql.functions import lit
            rejected_parts.append(invalid_timestamps.withColumn("rejection_reason", lit("invalid_timestamp")))

    if primary_key:
        from pyspark.sql.functions import count, lit
        dupes = (df.groupBy(primary_key)
                   .agg(count("*").alias("cnt"))
                   .filter(col("cnt") > 1)
                   .drop("cnt"))
        if dupes.count() > 0:
            dupe_rows = df.join(dupes, on=primary_key, how="inner")
            df        = df.join(dupes, on=primary_key, how="left_anti")
            rejected_parts.append(
                dupe_rows.withColumn("rejection_reason", lit("duplicate_primary_key")))

    rejected = rejected_parts[0] if len(rejected_parts) == 1 \
               else rejected_parts[0].unionByName(rejected_parts[1], allowMissingColumns=True) \
               if len(rejected_parts) > 1 else df.limit(0).withColumn(
                   "rejection_reason",
                   __import__("pyspark.sql.functions", fromlist=["lit"]).lit(""))

    return df, rejected



def save_delta(df: DataFrame, name: str, layer: str, partition_by: list, base: str = "data"):
    path = f"{base}/{layer}/{name}"
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    return path



def log_metadata(spark: SparkSession, name: str, total: int, rejected: int,
                 elapsed: float, layer: str, base: str = "data"):
    from pyspark.sql import Row
    log_path = f"{base}/metadata/ingestion_log"
    row = Row(
        dataset=name,
        layer=layer,
        processed_records=total,
        rejected_records=rejected,
        accepted_records=total - rejected,
        execution_time_sec=round(elapsed, 2),
        ingested_at=datetime.utcnow().isoformat(),
        schema_version="1.0",
    )
    log_df = spark.createDataFrame([row])
    log_df.write.format("delta").mode("append").save(log_path)



def ingest(spark: SparkSession, name: str, layer: str = "bronze", base: str = "data"):
    cfg = DATASETS[name]
    t0  = time.time()

    print(f"\n{'='*50}")
    print(f"Ingesting: {name} → {layer}")

    df = load(spark, cfg["path"], cfg["format"])
    total = df.count()
    print(f"  Loaded:      {total:,} rows")

    df = standardize_columns(df, name)
    validate_schema(df, cfg["required_columns"], name)

    df = normalize_timestamps(df, name)

    df = add_partition_columns(df, name)

    df, rejected_df = validate(df, name, cfg["primary_key"] or [], TIMESTAMP_COLS.get(name, []))
    rejected_count = rejected_df.count()
    print(f"  Rejected:    {rejected_count:,} rows")

    if rejected_count > 0:
        rejected_df.write.format("delta").mode("append") \
            .save(f"{base}/rejected/{name}")

    df = cfg["rules"](df)
    after_rules = df.count()
    print(f"  After rules: {after_rules:,} rows")

    path = save_delta(df, name, layer, cfg["partition_by"], base)
    print(f"  Saved to:    {path}")

    elapsed = time.time() - t0
    log_metadata(spark, name, total, rejected_count + (total - rejected_count - after_rules), elapsed, layer, base)
    print(f"  Time:        {elapsed:.1f}s")

    return df


def ingest_all(spark: SparkSession, layer: str = "bronze", base: str = "data"):
    for name in DATASETS:
        ingest(spark, name, layer, base)
