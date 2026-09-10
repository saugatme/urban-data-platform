import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.benchmark.benchmark import run_benchmark


def get_spark():
    builder = (
        SparkSession.builder
        .appName("benchmark")
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
        # disable AQE for fair comparison
        .config("spark.sql.adaptive.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


if __name__ == "__main__":
    spark = get_spark()
    run_benchmark(spark)
    spark.stop()