import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.integration.integrate import build_gold


def get_spark():
    builder = (
        SparkSession.builder
        .appName("integration")
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
        # auto-broadcast threshold: broadcast tables under 100MB
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


if __name__ == "__main__":
    spark = get_spark()
    build_gold(spark, base="data")
    spark.stop()
    print("\nDone. Check data/gold/integrated_taxi_trips")