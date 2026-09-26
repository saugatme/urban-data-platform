"""Apply an incremental release and refresh affected products."""

import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.operations.session import get_spark
from src.operations.pipeline import UPDATE_FILES, refresh_products, run_all


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Rebuild downstream layers after an interrupted refresh.")
    args = parser.parse_args()

    spark = get_spark("operations")
    spark.sparkContext.setLogLevel("WARN")
    if args.refresh:
        products = refresh_products(spark, set(UPDATE_FILES))
        print("Refreshed products:", products)
    else:
        updates, products = run_all(spark)
        print("\nUpdated datasets:", [r["dataset"] for r in updates if r["inserted"]])
        print("Refreshed products:", products)
    spark.stop()
