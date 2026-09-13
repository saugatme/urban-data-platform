from pyspark.sql.column import Column
from pyspark.sql.functions import col


def _valid_taxi_trips() -> Column:
    return (
        (col("trip_distance") > 0)
        & (col("fare_amount") >= 0)
        & col("passenger_count").isNotNull()
        & (col("passenger_count") > 0)
        & col("pickup_datetime").isNotNull()
        & col("dropoff_datetime").isNotNull()
        & (col("dropoff_datetime") >= col("pickup_datetime"))
    )


def _valid_weather() -> Column:
    return (
        col("hour").between(0, 23)
        & col("month").between(1, 12)
        & col("day").between(1, 31)
    )


def _valid_air_quality() -> Column:
    return col("sample_measurement").isNotNull()


def _valid_taxi_zones() -> Column:
    return col("location_id").isNotNull() & (col("location_id") > 0)


COLUMN_RENAMES = {
    "taxi_trips": {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
    },
    "weather": {
        "dwpt": "dew_point", "rhum": "rel_humidity", "prcp": "precipitation",
        "wdir": "wind_direction", "wspd": "wind_speed", "pres": "pressure",
        "coco": "condition_code",
    },
    "air_quality": {
        "State Code": "state_code", "County Code": "county_code", "Site Num": "site_num",
        "Parameter Code": "parameter_code", "POC": "poc", "Latitude": "latitude",
        "Longitude": "longitude", "Datum": "datum", "Parameter Name": "parameter_name",
        "Date Local": "date_local", "Time Local": "time_local", "Date GMT": "date_gmt",
        "Time GMT": "time_gmt", "Sample Measurement": "sample_measurement",
        "Units of Measure": "units_of_measure", "MDL": "mdl", "Uncertainty": "uncertainty",
        "Qualifier": "qualifier", "Method Type": "method_type", "Method Code": "method_code",
        "Method Name": "method_name", "State Name": "state_name", "County Name": "county_name",
        "Date of Last Change": "date_of_last_change",
    },
    "taxi_zones": {"LocationID": "location_id", "Borough": "borough", "Zone": "zone"},
}


DATASETS = {
    "taxi_trips": {
        "path": "data/raw/taxi_trips/",
        "format": "parquet",
        "primary_key": None,
        "required_columns": [
            "pickup_datetime", "dropoff_datetime", "trip_distance", "passenger_count",
            "fare_amount", "pickup_location_id", "dropoff_location_id",
        ],
        "timestamp_columns": ["pickup_datetime", "dropoff_datetime"],
        "partition_by": ["year", "month"],
        "validity_condition": _valid_taxi_trips,
        "schema_version": "1.0",
    },
    "weather": {
        "path": "data/raw/weather/weather.csv",
        "format": "csv",
        "primary_key": ["year", "month", "day", "hour"],
        "required_columns": ["year", "month", "day", "hour", "temp"],
        "timestamp_columns": [],
        "partition_by": None,
        "validity_condition": _valid_weather,
        "schema_version": "1.0",
    },
    "air_quality": {
        "path": "data/raw/air_quality/hourly_88101_2024.csv",
        "format": "csv",
        "primary_key": [
            "state_code", "county_code", "site_num", "parameter_code", "poc",
            "date_local", "time_local",
        ],
        "required_columns": [
            "state_code", "county_code", "site_num", "date_local", "time_local",
            "sample_measurement",
        ],
        "timestamp_columns": ["date_local"],
        "partition_by": ["year", "month"],
        "validity_condition": _valid_air_quality,
        "schema_version": "1.0",
    },
    "taxi_zones": {
        "path": "data/raw/taxi_zones/taxi_zone_lookup.csv",
        "format": "csv",
        "primary_key": ["location_id"],
        "required_columns": ["location_id", "borough", "zone"],
        "timestamp_columns": [],
        "partition_by": None,
        "validity_condition": _valid_taxi_zones,
        "schema_version": "1.0",
    },
}
