"""Build the integrated taxi trip Delta table."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, concat, date_trunc, lit, lpad, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

AQ_STATE = 36
AQ_COUNTY = 81
AQ_SITE = 124


def load_silver(spark: SparkSession, name: str, base: str) -> DataFrame:
    return spark.read.format("delta").load(f"{base}/silver/{name}")


def prepare_weather(df: DataFrame) -> DataFrame:
    weather = df.withColumn(
        "weather_timestamp",
        to_timestamp(
            concat(
                col("year").cast("string"), lit("-"), lpad(col("month").cast("string"), 2, "0"), lit("-"),
                lpad(col("day").cast("string"), 2, "0"), lit(" "), lpad(col("hour").cast("string"), 2, "0"),
                lit(":00:00"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    ).select(
        "weather_timestamp", "temp", "rel_humidity", "precipitation", "wind_speed",
        "wind_direction", "pressure", "condition_code",
    )
    window = Window.partitionBy("weather_timestamp").orderBy("temp")
    return weather.withColumn("_row", row_number().over(window)).filter(col("_row") == 1).drop("_row")


def prepare_air_quality(df: DataFrame) -> DataFrame:
    return (
        df.filter(
            (col("state_code") == AQ_STATE)
            & (col("county_code") == AQ_COUNTY)
            & (col("site_num") == AQ_SITE)
        )
        .groupBy("aq_timestamp")
        .agg(avg("sample_measurement").alias("pm25_hourly_avg"))
    )


def build_integrated(spark: SparkSession, base: str = "data") -> DataFrame:
    trips = load_silver(spark, "taxi_trips", base).withColumn("pickup_hour", date_trunc("hour", "pickup_datetime"))
    weather = prepare_weather(load_silver(spark, "weather", base))
    air_quality = prepare_air_quality(load_silver(spark, "air_quality", base))
    zones = load_silver(spark, "taxi_zones", base)

    trips = trips.join(broadcast(weather), trips.pickup_hour == weather.weather_timestamp, "left").drop("weather_timestamp")
    trips = trips.join(broadcast(air_quality), trips.pickup_hour == air_quality.aq_timestamp, "left").drop("aq_timestamp")

    pickup_zones = zones.select(
        col("location_id").alias("pickup_zone_id"), col("zone").alias("pickup_zone"),
        col("borough").alias("pickup_borough"), col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        col("location_id").alias("dropoff_zone_id"), col("zone").alias("dropoff_zone"),
        col("borough").alias("dropoff_borough"), col("service_zone").alias("dropoff_service_zone"),
    )
    return (
        trips.join(broadcast(pickup_zones), trips.pickup_location_id == pickup_zones.pickup_zone_id, "left")
        .drop("pickup_zone_id")
        .join(broadcast(dropoff_zones), trips.dropoff_location_id == dropoff_zones.dropoff_zone_id, "left")
        .drop("dropoff_zone_id")
    )


def build_gold(spark: SparkSession, base: str = "data") -> DataFrame:
    df = build_integrated(spark, base)
    (
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .partitionBy("year", "month")
        .save(f"{base}/gold/integrated_taxi_trips")
    )
    return df