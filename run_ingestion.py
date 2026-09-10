import os
import sys

# Must be set before ANY pyspark import
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.ingestion.ingestor import ingest_all
from src.ingestion.silver import build_all_silver


def get_spark_ingestion():
    builder = (
        SparkSession.builder
        .appName("ingestion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.default.parallelism", "8")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


if __name__ == "__main__":
    spark = get_spark_ingestion()

    print("\n>>> BRONZE: Ingesting raw data...")
    ingest_all(spark, layer="bronze", base="data")

    print("\n>>> SILVER: Applying common data model...")
    build_all_silver(spark, base="data")

    spark.stop()
    print("\nDone. Bronze + Silver layers ready.")