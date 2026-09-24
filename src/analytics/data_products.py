"""Reusable analytical data products for the integrated taxi dataset."""

import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.monitoring.monitor import record_run
from src.analytics.constants import (
    INTEGRATED_TRIPS_PATH,
    PRODUCT_BASE_PATH,
    weather_condition_column,
)
from src.common.spark_session import get_spark
from src.analytics.runtime import configure_spark_temp_dir


SCHEMA_VERSION = "1.0"


def add_metadata(df):
    now = datetime.now()

    return (
        df
        .withColumn("data_source", F.lit("integrated_taxi_trips"))
        .withColumn("creation_time", F.lit(now).cast("timestamp"))
        .withColumn("refresh_time", F.lit(now).cast("timestamp"))
        .withColumn("schema_version", F.lit(SCHEMA_VERSION))
    )


def write_product(spark, df, name):
    path = f"{PRODUCT_BASE_PATH}/{name}"

    (
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    )

    # Count the written product instead of recomputing its source aggregation.
    row_count = spark.read.format("delta").load(path).count()
    print(f"{name}: {row_count} rows -> {path}")


def daily_mobility(trips):
    df = (
        trips
        .withColumn("trip_date", F.to_date("pickup_datetime"))
        .groupBy(
            "trip_date",
            "year",
            "month",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare")
        )
    )

    return add_metadata(df)


def taxi_zone_statistics(trips):
    df = (
        trips
        .groupBy(
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare")
        )
    )

    return add_metadata(df)


def weather_impact(trips):
    weather_condition = weather_condition_column()

    temp_bucket = (
        F.when(F.col("temp") < 5, "Cold (<5°C)")
        .when(F.col("temp") < 20, "Mild (5–20°C)")
        .otherwise("Hot (>=20°C)")
    )

    df = (
        trips
        .filter(
            F.col("temp").isNotNull()
            & F.col("condition_code").isNotNull()
            & (F.col("trip_distance") > 0)
        )
        .withColumn("temp_bucket", temp_bucket)
        .withColumn("weather_condition", weather_condition)
        .groupBy(
            "temp_bucket",
            "weather_condition"
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("temp"), 2).alias("avg_temperature"),
            F.round(F.avg("precipitation"), 3).alias("avg_precipitation"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance_miles"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare")
        )
    )

    return add_metadata(df)


def air_quality_impact(trips):
    aqi_category = (
        F.when(F.col("pm25_hourly_avg") <= 12, "Good")
        .when(F.col("pm25_hourly_avg") <= 35.4, "Moderate")
        .when(
            F.col("pm25_hourly_avg") <= 55.4,
            "Unhealthy for Sensitive Groups"
        )
        .when(
            F.col("pm25_hourly_avg") <= 150.4,
            "Unhealthy"
        )
        .when(
            F.col("pm25_hourly_avg") <= 250.4,
            "Very Unhealthy"
        )
        .otherwise("Hazardous")
    )

    df = (
        trips
        .filter(
            F.col("pm25_hourly_avg").isNotNull()
            & (F.col("trip_distance") > 0)
        )
        .withColumn("aqi_category", aqi_category)
        .groupBy("aqi_category")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("pm25_hourly_avg"), 2).alias("avg_pm25"),
            F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare")
        )
    )

    return add_metadata(df)


# Week 3 refresh policy.  ``full`` products aggregate over the whole history,
# so a new release changes every row and there is nothing to reuse.
# ``incremental`` products are keyed by date, so only the months touched by the
# new data need recomputing; see the refresh section below.
PRODUCTS = {
    "daily_mobility": {
        "build": daily_mobility,
        "refresh_mode": "incremental",
        "partition_by": ["year", "month"],
        "grain": "zone-day",
    },
    "taxi_zone_statistics": {
        "build": taxi_zone_statistics,
        "refresh_mode": "full",
        "partition_by": None,
        "grain": "zone (whole period)",
    },
    "weather_impact": {
        "build": weather_impact,
        "refresh_mode": "full",
        "partition_by": None,
        "grain": "temperature bucket x condition (whole period)",
    },
    "air_quality_impact": {
        "build": air_quality_impact,
        "refresh_mode": "full",
        "partition_by": None,
        "grain": "PM2.5 category (whole period)",
    },
}


# ---------------------------------------------------------------------------
# Week 3: refreshing these products after an incremental update.
#
# ``full`` products aggregate the whole history, so new trips change every row
# and there is nothing to reuse.  ``incremental`` products are keyed by date,
# so only the months the new data touches are recomputed and swapped in with
# Delta ``replaceWhere``.  A per-product watermark in ``_product_log`` skips a
# product whose source has not moved.
# ---------------------------------------------------------------------------

PRODUCT_LOG_PATH = f"{PRODUCT_BASE_PATH}/_product_log"

PRODUCT_LOG_SCHEMA = StructType([
    StructField("product_name", StringType(), False),
    StructField("source_tables", StringType(), False),
    StructField("refresh_mode", StringType(), False),
    StructField("action", StringType(), False),
    StructField("rows_written", LongType(), True),
    StructField("partitions_refreshed", StringType(), True),
    StructField("source_max_timestamp", StringType(), True),
    StructField("last_refreshed_at", StringType(), False),
    StructField("execution_time_sec", DoubleType(), True),
    StructField("schema_version", StringType(), False),
])


def _log_exists(spark: SparkSession) -> bool:
    try:
        spark.read.format("delta").load(PRODUCT_LOG_PATH).limit(1).count()
        return True
    except Exception:  # noqa: BLE001 - absent table on first run
        return False


def read_watermarks(spark: SparkSession) -> dict[str, str]:
    """Return the last recorded source watermark per product."""
    if not _log_exists(spark):
        return {}

    log = spark.read.format("delta").load(PRODUCT_LOG_PATH)
    latest = (
        log.groupBy("product_name")
        .agg(F.max("last_refreshed_at").alias("last_refreshed_at"))
        .join(log, ["product_name", "last_refreshed_at"])
        .select("product_name", "source_max_timestamp")
        .collect()
    )
    return {row["product_name"]: row["source_max_timestamp"] for row in latest}


def write_log_entry(spark: SparkSession, **fields) -> None:
    row = {
        "product_name": fields["product_name"],
        "source_tables": fields.get("source_tables", "integrated_taxi_trips"),
        "refresh_mode": fields["refresh_mode"],
        "action": fields["action"],
        "rows_written": int(fields.get("rows_written") or 0),
        "partitions_refreshed": fields.get("partitions_refreshed"),
        "source_max_timestamp": fields.get("source_max_timestamp"),
        "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
        "execution_time_sec": round(float(fields.get("execution_time_sec") or 0.0), 3),
        "schema_version": SCHEMA_VERSION,
    }
    (
        spark.createDataFrame([row], schema=PRODUCT_LOG_SCHEMA)
        .write.format("delta").mode("append").save(PRODUCT_LOG_PATH)
    )


def _affected_months(trips: DataFrame, watermark: str | None) -> list[tuple[int, int]]:
    """Return the (year, month) pairs containing trips newer than the watermark.

    The watermark is stored as ``"YYYY-MM-DD HH:MM:SS"`` rather than ISO-8601,
    because a cast that silently produced NULL here would select no rows and
    the refresh would quietly do nothing.  A NULL cast is therefore treated as
    an error rather than as "no new data".
    """
    if watermark is None:
        scope = trips
    else:
        parsed = F.lit(watermark).cast("timestamp")
        if trips.select(parsed.isNull().alias("bad")).first()["bad"]:
            raise ValueError(
                f"Unparseable product watermark {watermark!r}; refusing to refresh "
                "against a NULL cutoff. Delete the product log entry to force a full rebuild."
            )
        scope = trips.filter(F.col("pickup_datetime") > parsed)

    rows = scope.select("year", "month").distinct().collect()
    return sorted((row["year"], row["month"]) for row in rows if row["year"] is not None)


def _replace_where(months: list[tuple[int, int]]) -> str:
    return " OR ".join(f"(year = {year} AND month = {month})" for year, month in months)


def _is_partitioned(spark: SparkSession, path: str) -> bool:
    try:
        detail = DeltaTable.forPath(spark, path).detail().collect()[0]
        return bool(detail["partitionColumns"])
    except Exception:  # noqa: BLE001 - absent table
        return False


def refresh_product(
    spark: SparkSession,
    name: str,
    trips: DataFrame,
    source_max: str,
    watermark: str | None,
    force: bool = False,
) -> dict:
    """Refresh one product according to its configured mode."""
    cfg = PRODUCTS[name]
    mode = cfg["refresh_mode"]
    path = f"{PRODUCT_BASE_PATH}/{name}"
    started_at = time.time()

    if not force and watermark == source_max:
        print(f"  {name}: up to date (watermark unchanged) - skipped")
        write_log_entry(
            spark, product_name=name, refresh_mode=mode, action="skipped",
            rows_written=0, source_max_timestamp=source_max,
            execution_time_sec=time.time() - started_at,
        )
        return {"product": name, "action": "skipped", "rows": 0, "seconds": 0.0}

    incremental_possible = (
        mode == "incremental"
        and watermark is not None
        and _is_partitioned(spark, path)
    )

    if incremental_possible:
        months = _affected_months(trips, watermark)
        if not months:
            print(f"  {name}: no new months - skipped")
            action, rows, partitions = "skipped", 0, None
        else:
            condition = _replace_where(months)
            scope = trips.filter(condition)
            product = cfg["build"](scope)
            (
                product.write.format("delta").mode("overwrite")
                .option("replaceWhere", condition)
                .option("mergeSchema", "true")
                .partitionBy(*cfg["partition_by"])
                .save(path)
            )
            rows = spark.read.format("delta").load(path).filter(condition).count()
            action = "incremental"
            partitions = condition
            print(f"  {name}: refreshed {len(months)} month(s) -> {rows:,} rows")
    else:
        # Full rebuild: either the product aggregates the whole history, or it
        # has no usable watermark/partitioning yet (first Week 3 run).
        product = cfg["build"](trips)
        writer = (
            product.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if cfg["partition_by"]:
            writer = writer.partitionBy(*cfg["partition_by"])
        writer.save(path)
        rows = spark.read.format("delta").load(path).count()
        action = "full"
        partitions = None
        print(f"  {name}: full rebuild -> {rows:,} rows")

    elapsed = time.time() - started_at
    write_log_entry(
        spark, product_name=name, refresh_mode=mode, action=action,
        rows_written=rows, partitions_refreshed=partitions,
        source_max_timestamp=source_max, execution_time_sec=elapsed,
    )
    return {"product": name, "action": action, "rows": rows, "seconds": round(elapsed, 3)}


def refresh_all(spark: SparkSession, base: str = "data", force: bool = False) -> list[dict]:
    """Refresh every product against the current integrated table."""
    started_at = time.time()
    trips = spark.read.format("delta").load(INTEGRATED_TRIPS_PATH).cache()

    source_max_row = trips.agg(F.max("pickup_datetime").alias("m")).collect()[0]["m"]
    # "YYYY-MM-DD HH:MM:SS" - str(), not isoformat(): the 'T' separator is what
    # a timestamp cast is least reliable about, and this value is cast back.
    source_max = str(source_max_row) if source_max_row else None
    watermarks = read_watermarks(spark)

    print(f"\nSource watermark: {source_max}")

    results = [
        refresh_product(spark, name, trips, source_max, watermarks.get(name), force)
        for name in PRODUCTS
    ]

    trips.unpersist()

    record_run(
        spark,
        pipeline="product_refresh",
        dataset="data_products",
        layer="gold",
        processed=len(results),
        inserted=sum(r["rows"] for r in results),
        elapsed=time.time() - started_at,
        schema_version=SCHEMA_VERSION,
        base=base,
    )
    return results


def main():
    spark = get_spark("data-products")
    configure_spark_temp_dir()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark ready: {spark.version}")

    trips = (
        spark.read
        .format("delta")
        .load(INTEGRATED_TRIPS_PATH)
    )

    print(f"Integrated trips: {trips.count()}")

    products = {
        "daily_mobility": daily_mobility(trips),
        "taxi_zone_statistics": taxi_zone_statistics(trips),
        "weather_impact": weather_impact(trips),
        "air_quality_impact": air_quality_impact(trips),
    }

    for name, df in products.items():
        print(f"\n{name}")
        df.printSchema()
        write_product(spark, df, name)

    print("\nData products created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
