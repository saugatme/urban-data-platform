"""
Task 5 — Integration Pipeline
Builds gold/integrated_taxi_trips by enriching each taxi trip with:
  - weather conditions at pickup time
  - air quality (PM2.5 daily average) from one fixed NYC site
  - pickup zone + borough
  - dropoff zone + borough
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, date_trunc, broadcast, avg, lpad, concat, lit, to_timestamp
)


AQ_STATE  = 36
AQ_COUNTY = 81
AQ_SITE   = 124



def load_silver(spark: SparkSession, name: str, base: str = "data") -> DataFrame:
    return spark.read.format("delta").load(f"{base}/silver/{name}")



def prepare_weather(df: DataFrame) -> DataFrame:
    """Build an hourly timestamp from year/month/day/hour integer columns."""
    df = df.withColumn(
        "weather_ts",
        to_timestamp(
            concat(
                col("year").cast("string"), lit("-"),
                lpad(col("month").cast("string"), 2, "0"), lit("-"),
                lpad(col("day").cast("string"),   2, "0"), lit(" "),
                lpad(col("hour").cast("string"),  2, "0"), lit(":00:00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    df = df.select(
        "weather_ts",
        "temp", "rel_humidity", "precipitation",
        "wind_speed", "wind_direction", "pressure", "condition_code"
    )
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    w = Window.partitionBy("weather_ts").orderBy("temp")
    return df.withColumn("_rn", row_number().over(w)) \
             .filter(col("_rn") == 1).drop("_rn")



def prepare_air_quality(df: DataFrame) -> DataFrame:
    """Filter to one site and keep one PM2.5 value per hour."""
    df = df.filter(
        (col("state_code") == AQ_STATE) &
        (col("county_code") == AQ_COUNTY) &
        (col("site_num") == AQ_SITE)
    )
    return df.groupBy("aq_timestamp").agg(
        avg("sample_measurement").alias("pm25_hourly_avg")
    )


def prepare_trips(df: DataFrame) -> DataFrame:
    """Add the hourly key used by weather and air-quality joins."""
    return df.withColumn("pickup_hour", date_trunc("hour", col("pickup_datetime")))



def build_integrated(spark: SparkSession, base: str = "data") -> DataFrame:
    trips    = load_silver(spark, "taxi_trips",  base)
    weather  = load_silver(spark, "weather",     base)
    air_qual = load_silver(spark, "air_quality", base)
    zones    = load_silver(spark, "taxi_zones",  base)

    print(f"  trips:     {trips.count():,}")
    print(f"  weather:   {weather.count():,}")
    print(f"  air_qual:  {air_qual.count():,}")
    print(f"  zones:     {zones.count():,}")

    trips    = prepare_trips(trips)
    weather  = prepare_weather(weather)
    air_qual = prepare_air_quality(air_qual)

    trips = trips.join(
        broadcast(weather),
        on=trips["pickup_hour"] == weather["weather_ts"],
        how="left"
    ).drop("weather_ts")

    trips = trips.join(
        broadcast(air_qual),
        on=trips["pickup_hour"] == air_qual["aq_timestamp"],
        how="left"
    ).drop("aq_timestamp")

    pickup_zones = zones.select(
        col("location_id").alias("pu_loc"),
        col("zone").alias("pickup_zone"),
        col("borough").alias("pickup_borough"),
        col("service_zone").alias("pickup_service_zone")
    )
    trips = trips.join(
        broadcast(pickup_zones),
        on=trips["pickup_location_id"] == pickup_zones["pu_loc"],
        how="left"
    ).drop("pu_loc")

    dropoff_zones = zones.select(
        col("location_id").alias("do_loc"),
        col("zone").alias("dropoff_zone"),
        col("borough").alias("dropoff_borough"),
        col("service_zone").alias("dropoff_service_zone")
    )
    trips = trips.join(
        broadcast(dropoff_zones),
        on=trips["dropoff_location_id"] == dropoff_zones["do_loc"],
        how="left"
    ).drop("do_loc")

    return trips



def build_gold(spark: SparkSession, base: str = "data"):
    print("\n>>> GOLD: Building integrated_taxi_trips...")
    df = build_integrated(spark, base)

    total = df.count()
    print(f"  Integrated rows: {total:,}")

    weather_hits = df.filter(col("temp").isNotNull()).count()
    aq_hits      = df.filter(col("pm25_hourly_avg").isNotNull()).count()
    print(f"  Weather join:    {weather_hits:,} ({100*weather_hits/total:.1f}% matched)")
    print(f"  AQ join:         {aq_hits:,} ({100*aq_hits/total:.1f}% matched)")

    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(f"{base}/gold/integrated_taxi_trips")

    print(f"  Saved to: {base}/gold/integrated_taxi_trips")
    return df
