"""Pipeline run monitoring.

Every pipeline writes one row per dataset per run to
``data/monitoring/pipeline_log``.  This mirrors ``log_metadata()`` in
:mod:`src.ingestion.ingestor`, which records ingestion specifically; the
monitoring log is the cross-pipeline view that Week 3 queries.

The schema is declared explicitly rather than inferred, so appends from
different pipelines stay compatible.
"""

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


PIPELINE_LOG_PATH = "monitoring/pipeline_log"

PIPELINE_LOG_SCHEMA = StructType([
    StructField("pipeline", StringType(), False),
    StructField("dataset", StringType(), False),
    StructField("layer", StringType(), True),
    StructField("status", StringType(), False),
    StructField("processed_records", LongType(), True),
    StructField("inserted_records", LongType(), True),
    StructField("rejected_records", LongType(), True),
    StructField("execution_time_sec", DoubleType(), True),
    StructField("schema_version", StringType(), True),
    StructField("validation_failures", StringType(), True),
    StructField("validation_failure_count", LongType(), True),
    StructField("schema_changes", StringType(), True),
    StructField("run_at", StringType(), False),
])


def record_run(
    spark: SparkSession,
    pipeline: str,
    dataset: str,
    *,
    layer: str | None = None,
    status: str = "success",
    processed: int = 0,
    inserted: int = 0,
    rejected: int = 0,
    elapsed: float = 0.0,
    schema_version: str | None = None,
    validation_failures: dict | None = None,
    schema_changes: dict | None = None,
    base: str = "data",
) -> None:
    """Append one run record to the monitoring log.

    ``validation_failures`` maps a rejection reason to a count; it is stored as
    JSON so new rule names need no schema change.
    """
    failures = validation_failures or {}

    row = {
        "pipeline": pipeline,
        "dataset": dataset,
        "layer": layer,
        "status": status,
        "processed_records": int(processed),
        "inserted_records": int(inserted),
        "rejected_records": int(rejected),
        "execution_time_sec": round(float(elapsed), 3),
        "schema_version": schema_version,
        "validation_failures": json.dumps(failures, sort_keys=True),
        "validation_failure_count": int(sum(failures.values())),
        "schema_changes": json.dumps(schema_changes or {}, sort_keys=True),
        "run_at": datetime.now(timezone.utc).isoformat(),
    }

    (
        spark.createDataFrame([row], schema=PIPELINE_LOG_SCHEMA)
        .write.format("delta")
        .mode("append")
        .save(f"{base}/{PIPELINE_LOG_PATH}")
    )


@contextmanager
def monitored(spark: SparkSession, pipeline: str, dataset: str, base: str = "data", **fixed):
    """Time a block and record it, including when it raises.

    The caller mutates the yielded dict to report counts; a failure still
    produces a log row with ``status='failed'`` so a crashed run is visible in
    the monitoring queries rather than simply absent.
    """
    stats: dict = {"processed": 0, "inserted": 0, "rejected": 0}
    started_at = time.time()
    status = "success"

    try:
        yield stats
    except Exception as error:  # noqa: BLE001 - recorded, then re-raised
        status = "failed"
        stats.setdefault("validation_failures", {})
        stats["validation_failures"]["pipeline_error"] = 1
        stats["error"] = str(error)
        raise
    finally:
        record_run(
            spark,
            pipeline,
            dataset,
            status=status,
            elapsed=time.time() - started_at,
            processed=stats.get("processed", 0),
            inserted=stats.get("inserted", 0),
            rejected=stats.get("rejected", 0),
            validation_failures=stats.get("validation_failures"),
            schema_changes=stats.get("schema_changes"),
            schema_version=stats.get("schema_version"),
            layer=stats.get("layer"),
            base=base,
            **fixed,
        )
