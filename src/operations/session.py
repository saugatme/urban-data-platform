"""Spark settings used only by the operational maintenance scripts."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Importing this preserves the project's existing .env and Hadoop setup.
from src.common.spark_session import PROJECT_ROOT  # noqa: F401


def get_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.maxConcurrentOutputFileWriters", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("parquet.block.size", "33554432")
    return spark
