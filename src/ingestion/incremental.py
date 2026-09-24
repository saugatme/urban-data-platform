"""Incremental ingestion of a second data release.

Week 1 ingests a single release with ``mode("overwrite")``.  Week 3 receives a
second release per dataset and must fold it into the existing Bronze tables
without rebuilding them, while ignoring rows already present and accepting the
documented schema additions (``humidity`` on weather, ``aqi`` on air quality).

Two merge strategies are used, chosen by whether the dataset has a primary key:

* **Keyed datasets** (weather, air quality, taxi zones) use Delta ``MERGE`` with
  an insert-only clause.  Matched rows are deliberately left untouched, which
  both preserves unchanged records and makes a repeated run a no-op.
* **Taxi trips** have no reliable key, as documented in the Week 1 data
  catalogue.  A content hash over the columns common to both sides identifies
  rows byte-identical to ones already stored; those are dropped and the
  remainder is appended.

Both strategies are idempotent: running the pipeline twice inserts nothing the
second time.
"""

import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sha2

from src.ingestion.config import DATASETS
from src.ingestion.ingestor import (
    _combine_rejections,
    add_partition_columns,
    load,
    normalize_timestamps,
    standardize_columns,
    validate,
    validate_schema,
)
from src.ingestion.validation import apply_rules, build_context, check_schema_drift
from src.monitoring.monitor import record_run


HASH_COLUMN = "_row_hash"


def _row_hash(df: DataFrame, columns: list[str]) -> DataFrame:
    """Add a deterministic content hash over ``columns``.

    Columns are sorted so the hash does not depend on column order, and nulls
    are given an explicit marker so they cannot collide with an empty string.
    """
    ordered = sorted(columns)
    parts = [
        col(name).cast("string").alias(name) if name in df.columns else lit("").alias(name)
        for name in ordered
    ]
    return df.withColumn(HASH_COLUMN, sha2(concat_ws("||", *parts), 256))


def prepare_update(
    spark: SparkSession,
    name: str,
    base: str = "data",
) -> tuple[DataFrame, DataFrame, dict, dict]:
    """Load and clean one update file.

    Returns the accepted rows, the rejected rows, the schema-drift summary, and
    a per-reason failure count.  Reuses the Week 1 standardisation helpers so
    the update passes through exactly the same cleaning as the first release.
    """
    cfg = DATASETS[name]
    df = load(spark, cfg["update_path"], cfg["format"])

    df = standardize_columns(df, name)
    validate_schema(df, cfg["required_columns"], name)
    df = normalize_timestamps(df, cfg["timestamp_columns"])
    df = add_partition_columns(df, name)

    baseline = spark.read.format("delta").load(f"{base}/bronze/{name}").columns
    drift = check_schema_drift(df, baseline, cfg.get("schema_contract"))

    # Structural checks first (null/duplicate keys, bad timestamps), then the
    # dataset rule, then the pluggable rules.
    df, rejected_df = validate(df, cfg["primary_key"] or [], cfg["timestamp_columns"])

    invalid_by_rule = df.filter(~cfg["validity_condition"]())
    df = df.filter(cfg["validity_condition"]())
    invalid_by_rule = invalid_by_rule.withColumn("rejection_reason", lit("dataset_rule_failure"))

    rules = cfg.get("validation_rules") or []
    requirements = {requirement for rule in rules for requirement in rule.requires}
    context = build_context(spark, requirements, base)
    df, rule_rejections = apply_rules(df, rules, context)

    rejected_df = _combine_rejections(df, [rejected_df, invalid_by_rule, *rule_rejections])

    failures = {
        row["rejection_reason"]: row["count"]
        for row in rejected_df.groupBy("rejection_reason").count().collect()
    }

    return df, rejected_df, drift, failures


def merge_keyed(spark: SparkSession, name: str, updates: DataFrame, base: str) -> int:
    """Insert-only Delta MERGE on the dataset's primary key."""
    path = f"{base}/bronze/{name}"
    target = DeltaTable.forPath(spark, path)
    keys = DATASETS[name]["primary_key"]

    # Delta reports a missing merge key as an unresolved-expression error that
    # does not say which side is at fault, so check both sides up front.
    stored = spark.read.format("delta").load(path).columns
    missing_target = [key for key in keys if key not in stored]
    missing_source = [key for key in keys if key not in updates.columns]
    if missing_target or missing_source:
        raise ValueError(
            f"{name}: primary key column(s) absent - "
            f"missing from bronze table: {missing_target or 'none'}, "
            f"missing from update: {missing_source or 'none'}"
        )

    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    before = spark.read.format("delta").load(path).count()

    (
        target.alias("target")
        .merge(updates.alias("source"), condition)
        # No whenMatched clause: existing rows are preserved untouched, which is
        # what makes a repeated run insert nothing.
        .whenNotMatchedInsertAll()
        .execute()
    )

    return spark.read.format("delta").load(path).count() - before


def merge_hashed(spark: SparkSession, name: str, updates: DataFrame, base: str) -> int:
    """Append only rows whose content hash is absent from the target table."""
    path = f"{base}/bronze/{name}"
    existing = spark.read.format("delta").load(path)

    common = sorted(set(existing.columns) & set(updates.columns))
    if not common:
        raise ValueError(f"{name}: update shares no columns with the bronze table")

    existing_hashes = _row_hash(existing, common).select(HASH_COLUMN).distinct()
    hashed_updates = _row_hash(updates, common)

    new_rows = hashed_updates.join(existing_hashes, HASH_COLUMN, "left_anti").drop(HASH_COLUMN)
    new_rows = new_rows.cache()
    inserted = new_rows.count()

    if inserted:
        writer = new_rows.write.format("delta").mode("append").option("mergeSchema", "true")
        partition_by = DATASETS[name]["partition_by"]
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    new_rows.unpersist()
    return inserted


def update_dataset(spark: SparkSession, name: str, base: str = "data") -> dict:
    """Fold one dataset's second release into its Bronze table."""
    cfg = DATASETS[name]
    started_at = time.time()
    run_started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'=' * 50}\nIncremental update: {name}")

    accepted, rejected_df, drift, failures = prepare_update(spark, name, base)
    accepted = accepted.cache()
    processed = accepted.count()
    rejected_count = rejected_df.count()

    if drift["evolved"]:
        print(f"  Schema evolution (documented): {', '.join(drift['evolved'])}")
    if drift["unexpected"]:
        print(f"  ! Unexpected new columns: {', '.join(drift['unexpected'])}")
    if drift["missing"]:
        print(f"  ! Columns missing from update: {', '.join(drift['missing'])}")

    if rejected_count:
        # Week 1 overwrites this table, so it holds a snapshot of the last
        # ingest.  The incremental pipeline appends instead, because rejections
        # from each release are worth keeping.  That makes it a log, so each
        # row is stamped with its run: without this, re-running the update
        # would add indistinguishable copies of the same rejection.
        (
            rejected_df
            .withColumn("rejected_at", lit(run_started_at))
            .withColumn("source_release", lit("incremental"))
            .write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(f"{base}/rejected/{name}")
        )

    if cfg["primary_key"]:
        inserted = merge_keyed(spark, name, accepted, base)
    else:
        inserted = merge_hashed(spark, name, accepted, base)

    accepted.unpersist()
    elapsed = time.time() - started_at

    duplicates_ignored = processed - inserted
    print(
        f"  Processed: {processed:,} | Inserted: {inserted:,} | "
        f"Duplicates ignored: {duplicates_ignored:,} | Rejected: {rejected_count:,}"
    )
    print(f"  Time: {elapsed:.1f}s")

    record_run(
        spark,
        pipeline="incremental_update",
        dataset=name,
        layer="bronze",
        processed=processed,
        inserted=inserted,
        rejected=rejected_count,
        elapsed=elapsed,
        schema_version=cfg["schema_version"],
        validation_failures=failures,
        schema_changes=drift,
        base=base,
    )

    return {
        "dataset": name,
        "processed": processed,
        "inserted": inserted,
        "duplicates_ignored": duplicates_ignored,
        "rejected": rejected_count,
        "schema_changes": drift,
        "validation_failures": failures,
        "execution_time_sec": round(elapsed, 3),
    }


def update_all(spark: SparkSession, base: str = "data") -> list[dict]:
    """Run the incremental update for every dataset that ships one."""
    results = []
    for name, cfg in DATASETS.items():
        if not cfg.get("update_path"):
            print(f"\nSkipping {name}: no update release configured")
            continue
        results.append(update_dataset(spark, name, base))
    return results
