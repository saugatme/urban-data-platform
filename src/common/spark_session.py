import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# load .env (manual parser, no dependency)
_env = PROJECT_ROOT / ".env"
if _env.exists():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

_hadoop = os.environ.get("HADOOP_HOME")
if _hadoop:  # Windows-only requirement; ignored on Linux/Mac/WSL
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + str(Path(_hadoop) / "bin")


def get_spark(app_name: str = "urban-data-platform"):
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8"))
    return configure_spark_with_delta_pip(builder).getOrCreate()