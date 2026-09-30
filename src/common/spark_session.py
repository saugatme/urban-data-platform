import os
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_env = PROJECT_ROOT / ".env"
if _env.exists():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

_hadoop = os.environ.get("HADOOP_HOME")
if _hadoop:
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + str(Path(_hadoop) / "bin")


def get_spark(
    app_name: str = "urban-data-platform",
    master: str = "local[*]",
    extra_configs: Mapping[str, str] | None = None,
):
    """Create the shared Delta-enabled Spark session.

    ``master`` and ``extra_configs`` let memory-intensive workflows reduce local
    parallelism or set JVM memory before Spark starts. Existing callers retain
    the original ``local[*]`` configuration.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()
