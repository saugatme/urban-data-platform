"""Spark SQL questions answered against the pipeline monitoring log.

Each function registers the log as a temp view and returns a DataFrame, so the
query text stays visible and reusable the same way ``src/analytics/queries.py``
handles the analytical queries.
"""

from pyspark.sql import DataFrame, SparkSession

from src.monitoring.monitor import PIPELINE_LOG_PATH


VIEW = "pipeline_log"


def register_log(spark: SparkSession, base: str = "data") -> None:
    spark.read.format("delta").load(f"{base}/{PIPELINE_LOG_PATH}").createOrReplaceTempView(VIEW)


VALIDATION_FAILURES_SQL = f"""
SELECT
    dataset,
    COUNT(*)                          AS runs,
    SUM(validation_failure_count)     AS total_validation_failures,
    SUM(rejected_records)             AS total_rejected,
    ROUND(
        SUM(rejected_records) / NULLIF(SUM(processed_records), 0) * 100,
        3
    )                                 AS rejected_pct
FROM {VIEW}
GROUP BY dataset
ORDER BY total_validation_failures DESC, total_rejected DESC
"""


SLOWEST_DATASETS_SQL = f"""
SELECT
    dataset,
    pipeline,
    COUNT(*)                                  AS runs,
    ROUND(AVG(execution_time_sec), 3)         AS avg_seconds,
    ROUND(MAX(execution_time_sec), 3)         AS max_seconds,
    ROUND(SUM(execution_time_sec), 3)         AS total_seconds
FROM {VIEW}
GROUP BY dataset, pipeline
ORDER BY avg_seconds DESC
"""


REJECTED_PER_RUN_SQL = f"""
SELECT
    run_at,
    pipeline,
    dataset,
    processed_records,
    inserted_records,
    rejected_records,
    validation_failures
FROM {VIEW}
WHERE rejected_records > 0
ORDER BY run_at DESC, rejected_records DESC
"""


PROCESSING_TIME_TREND_SQL = f"""
SELECT
    dataset,
    run_at,
    execution_time_sec,
    ROUND(
        execution_time_sec - LAG(execution_time_sec) OVER (
            PARTITION BY dataset ORDER BY run_at
        ),
        3
    ) AS change_vs_previous_run
FROM {VIEW}
ORDER BY dataset, run_at
"""


def validation_failures_by_dataset(spark: SparkSession) -> DataFrame:
    """Which dataset fails validation most often?"""
    return spark.sql(VALIDATION_FAILURES_SQL)


def slowest_datasets(spark: SparkSession) -> DataFrame:
    """Which dataset takes longest to process?"""
    return spark.sql(SLOWEST_DATASETS_SQL)


def rejected_per_run(spark: SparkSession) -> DataFrame:
    """How many records were rejected in each run, and why?"""
    return spark.sql(REJECTED_PER_RUN_SQL)


def processing_time_trend(spark: SparkSession) -> DataFrame:
    """How does processing time change from run to run?"""
    return spark.sql(PROCESSING_TIME_TREND_SQL)


MONITORING_QUESTIONS = {
    "Validation failures by dataset": validation_failures_by_dataset,
    "Slowest datasets": slowest_datasets,
    "Rejected records per run": rejected_per_run,
    "Processing time trend": processing_time_trend,
}


def run_all(spark: SparkSession, base: str = "data", limit: int = 20) -> None:
    """Print every monitoring question, for the run guide and the report."""
    register_log(spark, base)
    for title, query in MONITORING_QUESTIONS.items():
        print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")
        query(spark).show(limit, truncate=False)


if __name__ == "__main__":
    import os
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from src.common.spark_session import get_spark

    spark = get_spark("monitoring")
    spark.sparkContext.setLogLevel("WARN")
    run_all(spark, base="data")
    spark.stop()
