"""Measure the five Week 3 overheads.

Run this after ``run_incremental_update.py``, because the update and refresh
timings are read back from the logs those pipelines wrote rather than being
produced by re-running expensive merges.  Validation and monitoring overhead
are measured directly here, since neither leaves a timing behind.

Results are written to ``data/benchmark/week3_evaluation.json`` so the
evaluation report cites measured numbers.
"""

import json
import os
import sys
import time
from pathlib import Path

# Must be set before ANY pyspark import
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import functions as F

from src.analytics.data_products import PRODUCT_LOG_PATH
from src.common.spark_session import get_spark
from src.ingestion.config import DATASETS
from src.ingestion.incremental import prepare_update
from src.monitoring.monitor import PIPELINE_LOG_PATH, record_run

OUTPUT = Path("data/benchmark/week3_evaluation.json")
MONITORING_SAMPLES = 5


def directory_size(path: str) -> int:
    root = Path(path)
    if not root.exists():
        return 0
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


def format_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} GB"


def incremental_update_time(spark) -> dict:
    """Per-dataset Bronze merge time, from the monitoring log."""
    log = spark.read.format("delta").load(f"data/{PIPELINE_LOG_PATH}")
    rows = (
        log.filter(F.col("pipeline") == "incremental_update")
        .groupBy("dataset")
        .agg(
            F.max("run_at").alias("last_run"),
            F.round(F.avg("execution_time_sec"), 3).alias("avg_seconds"),
            F.sum("processed_records").alias("processed"),
            F.sum("inserted_records").alias("inserted"),
            F.sum("rejected_records").alias("rejected"),
        )
        .collect()
    )
    return {row["dataset"]: row.asDict() for row in rows}


def analytical_refresh_time(spark) -> dict:
    """Per-product refresh time and action, from the product log."""
    try:
        log = spark.read.format("delta").load(PRODUCT_LOG_PATH)
    except Exception:  # noqa: BLE001 - product refresh has not run yet
        return {}

    rows = (
        log.groupBy("product_name", "refresh_mode", "action")
        .agg(
            F.round(F.avg("execution_time_sec"), 3).alias("avg_seconds"),
            F.max("rows_written").alias("rows_written"),
            F.count("*").alias("runs"),
        )
        .collect()
    )
    return {f"{row['product_name']}::{row['action']}": row.asDict() for row in rows}


def storage_overhead() -> dict:
    layers = {
        "bronze": "data/bronze",
        "silver": "data/silver",
        "gold_integrated": "data/gold/integrated_taxi_trips",
        "data_products": "data/gold/data_products",
        "monitoring_log": f"data/{PIPELINE_LOG_PATH}",
        "ingestion_log": "data/metadata/ingestion_log",
        "rejected": "data/rejected",
        "incoming_updates": "data/incoming",
    }
    sizes = {name: directory_size(path) for name, path in layers.items()}

    source = sizes["gold_integrated"] or 1
    return {
        "bytes": sizes,
        "human": {name: format_size(size) for name, size in sizes.items()},
        "monitoring_pct_of_gold": round(sizes["monitoring_log"] / source * 100, 4),
        "products_pct_of_gold": round(sizes["data_products"] / source * 100, 4),
    }


def validation_overhead(spark) -> dict:
    """Cost of the pluggable rules, by preparing each update with and without them."""
    results = {}

    for name, cfg in DATASETS.items():
        if not cfg.get("update_path"):
            continue

        original = cfg.get("validation_rules") or []
        if not original:
            continue

        # With rules.
        started_at = time.time()
        accepted, rejected, _, _ = prepare_update(spark, name, "data")
        accepted.count()
        rejected.count()
        with_rules = time.time() - started_at

        # Without rules: same code path, rule list temporarily emptied.
        cfg["validation_rules"] = []
        try:
            started_at = time.time()
            accepted, rejected, _, _ = prepare_update(spark, name, "data")
            accepted.count()
            rejected.count()
            without_rules = time.time() - started_at
        finally:
            cfg["validation_rules"] = original

        results[name] = {
            "rules": [rule.reason for rule in original],
            "with_rules_sec": round(with_rules, 3),
            "without_rules_sec": round(without_rules, 3),
            "overhead_sec": round(with_rules - without_rules, 3),
            "overhead_pct": round((with_rules - without_rules) / without_rules * 100, 2)
            if without_rules
            else None,
        }
        print(f"  {name}: {with_rules:.3f}s with rules vs {without_rules:.3f}s without")

    return results


def monitoring_overhead(spark) -> dict:
    """Cost of writing one monitoring row."""
    timings = []
    for index in range(MONITORING_SAMPLES):
        started_at = time.time()
        record_run(
            spark,
            pipeline="evaluation_probe",
            dataset=f"probe_{index}",
            status="success",
            processed=0,
            elapsed=0.0,
            base="data",
        )
        timings.append(time.time() - started_at)

    average = sum(timings) / len(timings)
    print(f"  monitoring write: {average:.3f}s average over {len(timings)} samples")
    return {
        "samples": len(timings),
        "per_write_sec": [round(value, 3) for value in timings],
        "avg_per_write_sec": round(average, 3),
        "note": "Probe rows are tagged pipeline='evaluation_probe' and can be filtered out.",
    }


def main() -> None:
    spark = get_spark("week3-evaluation")
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== 1. Incremental update time ===")
    updates = incremental_update_time(spark)
    print(json.dumps(updates, indent=2, default=str))

    print("\n=== 2. Analytical refresh time ===")
    refresh = analytical_refresh_time(spark)
    print(json.dumps(refresh, indent=2, default=str))

    print("\n=== 3. Storage overhead ===")
    storage = storage_overhead()
    print(json.dumps(storage["human"], indent=2))

    print("\n=== 4. Validation overhead ===")
    validation = validation_overhead(spark)

    print("\n=== 5. Monitoring overhead ===")
    monitoring = monitoring_overhead(spark)

    report = {
        "incremental_update_time": updates,
        "analytical_refresh_time": refresh,
        "storage_overhead": storage,
        "validation_overhead": validation,
        "monitoring_overhead": monitoring,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nWritten to {OUTPUT}")

    spark.stop()


if __name__ == "__main__":
    main()
