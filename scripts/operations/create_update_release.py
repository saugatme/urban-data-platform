"""Create a small second-release dataset for incremental processing."""

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.operations.session import get_spark

OUT = Path("data/updates")
HOURS = 168
SEED = 42


def sequence(df):
    return df.withColumn("_n", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))


def taxi_update(spark):
    taxi = spark.read.parquet("data/raw/taxi_trips")
    latest = taxi.agg(F.max("tpep_pickup_datetime").alias("latest")).first()["latest"]
    valid = taxi.filter(
        (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        & (F.col("passenger_count") > 0)
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
    )
    new = sequence(valid.sample(False, 0.06, SEED)).withColumn(
        "_duration", F.expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)")
    )
    new = new.withColumn(
        "tpep_pickup_datetime", F.expr(f"timestampadd(SECOND, _n * 60, TIMESTAMP '{latest}')")
    ).withColumn("tpep_dropoff_datetime", F.expr("timestampadd(SECOND, _duration, tpep_pickup_datetime)")).drop("_n", "_duration")
    duplicates = valid.sample(False, 0.001, SEED + 1)
    update = new.unionByName(duplicates)
    update.write.mode("overwrite").parquet(str(OUT / "taxi_trips"))
    return {"new_records": new.count(), "duplicate_records": duplicates.count(), "schema_change": "none"}


def hourly_update(spark, name, source_path, date_column, time_column, extra_column):
    source = spark.read.option("header", True).option("inferSchema", True).csv(source_path)
    if name == "weather":
        last = source.orderBy(F.desc("year"), F.desc("month"), F.desc("day"), F.desc("hour")).first()
        start = f"{last['year']}-{last['month']:02d}-{last['day']:02d} {last['hour']:02d}:00:00"
    else:
        start = str(source.agg(F.max(date_column).alias("m")).first()["m"])
    templates = sequence(source.orderBy(F.rand(SEED)).limit(HOURS))
    offset = "_n" if name == "weather" else "_n - 1"
    base = f"TIMESTAMP '{start}'" if name == "weather" else f"timestampadd(DAY, 1, TIMESTAMP '{start}')"
    rows = templates.withColumn("_ts", F.expr(f"timestampadd(HOUR, {offset}, {base})"))
    if name == "weather":
        rows = rows.withColumn("year", F.year("_ts")).withColumn("month", F.month("_ts")).withColumn("day", F.dayofmonth("_ts")).withColumn("hour", F.hour("_ts")).withColumn("humidity", F.round(F.lit(20) + F.rand(SEED) * 80, 1))
    else:
        rows = rows.withColumn(date_column, F.to_date("_ts")).withColumn(time_column, F.col("_ts")).withColumn("Date GMT", F.to_date(F.expr("timestampadd(HOUR, 5, _ts)"))).withColumn("Time GMT", F.expr("timestampadd(HOUR, 5, _ts)")).withColumn("aqi", F.round(F.rand(SEED) * 500).cast("int"))
    rows.drop("_n", "_ts").write.mode("overwrite").option("header", True).csv(str(OUT / name))
    return {"new_records": HOURS, "duplicate_records": 0, "schema_change": extra_column}


def main():
    spark = get_spark("update-release-generator")
    spark.sparkContext.setLogLevel("WARN")
    OUT.mkdir(parents=True, exist_ok=True)
    summary = {
        "taxi_trips": taxi_update(spark),
        "weather": hourly_update(spark, "weather", "data/raw/weather/weather.csv", None, None, "humidity"),
        "air_quality": hourly_update(spark, "air_quality", "data/raw/air_quality/hourly_88101_2024.csv", "Date Local", "Time Local", "aqi"),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
