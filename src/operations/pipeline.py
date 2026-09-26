"""Incremental updates, validation, monitoring, and product refresh."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from src.ingestion.config import DATASETS
from src.ingestion.ingestor import (
    add_partition_columns,
    load,
    normalize_timestamps,
    standardize_columns,
    validate_schema,
)

UPDATE_FILES = {
    "taxi_trips": ("data/updates/taxi_trips", "parquet"),
    "weather": ("data/updates/weather", "csv"),
    "air_quality": ("data/updates/air_quality", "csv"),
}
ALLOWED_NEW_COLUMNS = {"weather": {"humidity"}, "air_quality": {"aqi"}}
SCHEMA_VERSIONS = {"weather": "1.1", "air_quality": "1.1"}
MONITOR_PATH = "monitoring/operations_runs"


def _hash(df: DataFrame, columns: list[str]) -> DataFrame:
    """Hash full taxi rows; explicit null text prevents null/empty collisions."""
    values = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in sorted(columns)]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *values), 256))


def _add_rejection(rejected: list[DataFrame], df: DataFrame, condition, reason: str) -> DataFrame:
    bad = df.filter(condition).withColumn("rejection_reason", F.lit(reason))
    rejected.append(bad)
    return df.filter(~condition)


def prepare_update(spark: SparkSession, name: str, base: str = "data") -> tuple[DataFrame, DataFrame, dict]:
    """Standardise one update and quarantine invalid rows without stopping it."""
    path, fmt = UPDATE_FILES[name]
    cfg = DATASETS[name]
    df = load(spark, path, fmt)
    df = standardize_columns(df, name)
    validate_schema(df, cfg["required_columns"], name)
    df = normalize_timestamps(df, cfg["timestamp_columns"])
    df = add_partition_columns(df, name)

    bronze_columns = set(spark.read.format("delta").load(f"{base}/bronze/{name}").columns)
    new_columns = set(df.columns) - bronze_columns
    allowed = ALLOWED_NEW_COLUMNS.get(name, set())
    unexpected = sorted(new_columns - allowed)
    if unexpected:
        df = df.drop(*unexpected)
    schema = {"evolved": sorted(new_columns & allowed), "unexpected": unexpected}

    rejected: list[DataFrame] = []
    required_null = None
    for column in cfg["required_columns"]:
        test = F.col(column).isNull()
        required_null = test if required_null is None else required_null | test
    df = _add_rejection(rejected, df, required_null, "incomplete_record")
    df = _add_rejection(rejected, df, ~cfg["validity_condition"](), "invalid_value")

    if name == "weather" and "humidity" in df.columns:
        df = _add_rejection(rejected, df, ~F.col("humidity").between(20, 100), "humidity_out_of_range")
    if name == "air_quality":
        if "aqi" in df.columns:
            df = _add_rejection(rejected, df, ~F.col("aqi").between(0, 500), "aqi_out_of_range")

    if name == "taxi_trips":
        zone_ids = [r["location_id"] for r in spark.read.format("delta").load(f"{base}/bronze/taxi_zones").select("location_id").collect()]
        missing_zone = ~F.col("pickup_location_id").isin(zone_ids) | ~F.col("dropoff_location_id").isin(zone_ids)
        df = _add_rejection(rejected, df, missing_zone, "missing_taxi_zone")

    empty = df.limit(0).withColumn("rejection_reason", F.lit(""))
    for part in rejected:
        empty = empty.unionByName(part, allowMissingColumns=True)
    return df, empty, schema


def _split_new_rows(spark: SparkSession, name: str, update: DataFrame, base: str) -> tuple[DataFrame, DataFrame]:
    """Separate new rows from duplicates already stored in Bronze."""
    existing = spark.read.format("delta").load(f"{base}/bronze/{name}")
    keys = DATASETS[name]["primary_key"]
    if keys:
        scope = [c for c in ("year", "month") if c in existing.columns and c in update.columns]
        if scope:
            affected = update.select(*scope).distinct().hint("broadcast")
            existing = existing.join(affected, scope, "inner")
        existing_keys = existing.select(*keys)
        return (
            update.join(existing_keys, keys, "left_anti"),
            update.join(existing_keys, keys, "left_semi").withColumn("rejection_reason", F.lit("duplicate_record")),
        )

    common = sorted(set(existing.columns) & set(update.columns))
    hashes = _hash(existing, common).select("_row_hash")
    hashed_update = _hash(update, common)
    return (
        hashed_update.join(hashes, "_row_hash", "left_anti").drop("_row_hash"),
        hashed_update.join(hashes, "_row_hash", "left_semi").drop("_row_hash").withColumn("rejection_reason", F.lit("duplicate_record")),
    )

def record_run(spark: SparkSession, name: str, metrics: dict, stage: str = "incremental_update", base: str = "data") -> None:
    """Append one compact monitoring row to a Delta table."""
    row = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "dataset": name,
        "stage": stage,
        "processed": int(metrics["processed"]),
        "inserted": int(metrics["inserted"]),
        "rejected": int(metrics["rejected"]),
        "seconds": round(float(metrics["seconds"]), 3),
        "validation_seconds": round(float(metrics["validation_seconds"]), 3),
        "schema_version": SCHEMA_VERSIONS.get(name, DATASETS.get(name, {}).get("schema_version", "n/a")),
        "validation_failures": json.dumps(metrics["failures"], sort_keys=True),
        "schema_changes": json.dumps(metrics["schema"], sort_keys=True),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").save(f"{base}/{MONITOR_PATH}")


def apply_update(spark: SparkSession, name: str, base: str = "data") -> dict:
    """Validate, deduplicate, append, isolate rejects, and monitor one release."""
    started = time.time()
    raw_count = load(spark, *UPDATE_FILES[name]).count()
    validation_started = time.time()
    accepted, rejected, schema = prepare_update(spark, name, base)
    validation_seconds = time.time() - validation_started
    new_rows, duplicates = _split_new_rows(spark, name, accepted, base)
    rejected = rejected.unionByName(duplicates, allowMissingColumns=True).cache()
    # Materialise the rejected release before appending new Bronze rows. Without
    # this, Spark can re-evaluate duplicate detection after the append and label
    # every just-inserted row as a duplicate in the monitoring record.
    rejected_count = rejected.count()
    failures = {r["rejection_reason"]: r["count"] for r in rejected.groupBy("rejection_reason").count().collect()}
    if rejected_count:
        rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{base}/rejected/{name}")

    new_rows = new_rows.cache()
    inserted = new_rows.count()
    if inserted:
        writer = new_rows.write.format("delta").mode("append").option("mergeSchema", "true")
        partitions = DATASETS[name]["partition_by"]
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(f"{base}/bronze/{name}")
    new_rows.unpersist()
    rejected.unpersist()
    metrics = {
        "processed": raw_count, "inserted": inserted, "rejected": rejected_count,
        "seconds": time.time() - started, "validation_seconds": validation_seconds,
        "failures": failures, "schema": schema,
    }
    record_run(spark, name, metrics, base=base)
    print(f"{name}: processed={raw_count:,}, inserted={inserted:,}, rejected={rejected_count:,}")
    return {"dataset": name, **metrics}


def refresh_products(spark: SparkSession, changed: set[str], base: str = "data") -> list[str]:
    """Rebuild only products that depend on a changed source dataset."""
    if not changed:
        return []

    from src.analytics import data_products
    from src.analytics.constants import INTEGRATED_TRIPS_PATH, PRODUCT_BASE_PATH
    from src.ingestion.silver import build_silver
    from src.integration.integrate import build_gold

    for name in changed:
        started = time.time()
        build_silver(spark, name, base)
        record_run(spark, name, {"processed": 0, "inserted": 0, "rejected": 0, "seconds": time.time() - started, "validation_seconds": 0, "failures": {}, "schema": {}}, "silver_rebuild", base)

    started = time.time()
    build_gold(spark, base)
    record_run(spark, "integrated_taxi_trips", {"processed": 0, "inserted": 0, "rejected": 0, "seconds": time.time() - started, "validation_seconds": 0, "failures": {}, "schema": {}}, "gold_rebuild", base)

    trips = spark.read.format("delta").load(INTEGRATED_TRIPS_PATH)
    dependencies = {
        "daily_mobility": {"taxi_trips"},
        "taxi_zone_statistics": {"taxi_trips"},
        "weather_impact": {"taxi_trips", "weather"},
        "air_quality_impact": {"taxi_trips", "air_quality"},
    }
    builders = {
        "daily_mobility": data_products.daily_mobility,
        "taxi_zone_statistics": data_products.taxi_zone_statistics,
        "weather_impact": data_products.weather_impact,
        "air_quality_impact": data_products.air_quality_impact,
    }
    refreshed = []
    for product, builder in builders.items():
        if changed & dependencies[product]:
            started = time.time()
            builder(trips).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{PRODUCT_BASE_PATH}/{product}")
            record_run(spark, product, {"processed": 0, "inserted": 0, "rejected": 0, "seconds": time.time() - started, "validation_seconds": 0, "failures": {}, "schema": {}}, "product_refresh", base)
            refreshed.append(product)
    return refreshed

def run_all(spark: SparkSession, base: str = "data") -> tuple[list[dict], list[str]]:
    results = [apply_update(spark, name, base) for name in UPDATE_FILES]
    refreshed = refresh_products(spark, {r["dataset"] for r in results if r["inserted"]}, base)
    return results, refreshed


def monitoring_queries(spark: SparkSession, base: str = "data") -> dict[str, DataFrame]:
    """Return the four operational monitoring summaries."""
    spark.read.format("delta").load(f"{base}/{MONITOR_PATH}").createOrReplaceTempView("operations_runs")
    return {
        "Validation failures": spark.sql("SELECT dataset, COUNT(*) runs, SUM(rejected) rejected FROM operations_runs GROUP BY dataset ORDER BY rejected DESC"),
        "Slowest datasets": spark.sql("SELECT dataset, ROUND(AVG(seconds), 3) avg_seconds FROM operations_runs GROUP BY dataset ORDER BY avg_seconds DESC"),
        "Rejected per execution": spark.sql("SELECT run_at, dataset, processed, inserted, rejected, validation_failures FROM operations_runs ORDER BY run_at DESC"),
        "Processing trend": spark.sql("SELECT dataset, run_at, seconds, seconds - LAG(seconds) OVER (PARTITION BY dataset ORDER BY run_at) change_from_previous FROM operations_runs ORDER BY dataset, run_at"),
    }
