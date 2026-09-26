"""Measure update, refresh, storage, validation, and monitoring overhead."""

import json
import time
from pathlib import Path

from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.operations.session import get_spark
from src.operations.pipeline import MONITOR_PATH, record_run


def size(path: str) -> int:
    root = Path(path)
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file()) if root.exists() else 0


def human(value: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.2f} {unit}"
        value /= 1024


def main():
    spark = get_spark("operations-evaluation")
    log = spark.read.format("delta").load(f"data/{MONITOR_PATH}")

    updates = log.filter("stage = 'incremental_update'").groupBy("dataset").agg(
        F.round(F.avg("seconds"), 3).alias("average_update_seconds"),
        F.round(F.avg("validation_seconds"), 3).alias("average_validation_seconds"),
        F.sum("processed").alias("processed"), F.sum("inserted").alias("inserted"), F.sum("rejected").alias("rejected"),
    )
    update_results = {row["dataset"]: row.asDict() for row in updates.collect()}

    refresh = log.filter("stage = 'product_refresh'").select("dataset", "seconds").collect()
    refresh_results = {row["dataset"]: row["seconds"] for row in refresh}
    storage = {name: human(size(path)) for name, path in {
        "bronze": "data/bronze", "silver": "data/silver", "gold": "data/gold",
        "updates": "data/updates", "rejected": "data/rejected", "monitoring": f"data/{MONITOR_PATH}",
    }.items()}

    started = time.time()
    record_run(spark, "evaluation_probe", {"processed": 0, "inserted": 0, "rejected": 0, "seconds": 0, "validation_seconds": 0, "failures": {}, "schema": {}}, "evaluation", "data")
    monitoring_seconds = round(time.time() - started, 3)

    report = {
        "incremental_update_time": update_results,
        "analytical_refresh_time": refresh_results,
        "storage_overhead": storage,
        "validation_overhead": {name: row["average_validation_seconds"] for name, row in update_results.items()},
        "monitoring_overhead_seconds": monitoring_seconds,
    }
    output = Path("data/benchmark/operations_evaluation.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    print(f"Written to {output}")
    spark.stop()


if __name__ == "__main__":
    main()
