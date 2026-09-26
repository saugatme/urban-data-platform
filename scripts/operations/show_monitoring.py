"""Print operational monitoring summaries."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.operations.session import get_spark
from src.operations.pipeline import monitoring_queries


if __name__ == "__main__":
    spark = get_spark("operations-monitoring")
    for title, query in monitoring_queries(spark).items():
        print(f"\n=== {title} ===")
        query.show(truncate=False)
    spark.stop()
