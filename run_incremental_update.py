"""Apply the second data release and bring every downstream table back in line.

Order matters.  Bronze is merged incrementally, Silver and Gold are rebuilt
from the updated Bronze, and only then are the analytical products refreshed
against the new Gold table.

Silver and Gold are recomputed rather than merged.  They are derived views over
Bronze with whole-table semantics (``enforce_common_model`` drops all-null
columns by inspecting every row; the Gold join enriches each trip against
hourly weather and air quality), so a partial rebuild could leave them
inconsistent with Bronze.  The incremental saving is taken where it is both
safe and largest: the Bronze merge itself, and the product refresh.
"""

import json
import os
import sys
import time

# Must be set before ANY pyspark import
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.analytics.data_products import refresh_all
from src.common.spark_session import get_spark
from src.ingestion.config import DATASETS
from src.ingestion.incremental import update_all
from src.ingestion.silver import build_silver
from src.integration.integrate import build_gold
from src.monitoring.monitor import record_run


def main() -> None:
    spark = get_spark("incremental-update")
    spark.sparkContext.setLogLevel("WARN")

    # Delta needs this to accept the documented new columns during MERGE.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    print("\n>>> BRONZE: merging the second release...")
    results = update_all(spark, base="data")

    updated = [row["dataset"] for row in results if row["inserted"] > 0]

    print("\n>>> SILVER: rebuilding affected datasets...")
    if updated:
        for name in updated:
            started_at = time.time()
            build_silver(spark, name, base="data")
            elapsed = time.time() - started_at
            print(f"  {name}: rebuilt in {elapsed:.1f}s")
            record_run(
                spark, pipeline="silver_rebuild", dataset=name, layer="silver",
                elapsed=elapsed, schema_version=DATASETS[name]["schema_version"],
                base="data",
            )
    else:
        print("  No datasets changed - skipped")

    print("\n>>> GOLD: rebuilding the integrated table...")
    if updated:
        started_at = time.time()
        build_gold(spark, base="data")
        elapsed = time.time() - started_at
        print(f"  integrated_taxi_trips: rebuilt in {elapsed:.1f}s")
        record_run(
            spark, pipeline="gold_rebuild", dataset="integrated_taxi_trips",
            layer="gold", elapsed=elapsed, base="data",
        )
    else:
        print("  No datasets changed - skipped")

    print("\n>>> PRODUCTS: refreshing analytical products...")
    product_results = refresh_all(spark, base="data")

    print("\n" + "=" * 60)
    print("Incremental update summary")
    print("=" * 60)
    print(json.dumps({"datasets": results, "products": product_results}, indent=2, default=str))

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
