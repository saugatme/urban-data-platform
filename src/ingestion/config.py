from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def _rules_taxi_trips(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("trip_distance") > 0) &
        (col("fare_amount") >= 0) &
        (col("passenger_count") > 0)
    )

def _rules_weather(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("hour").between(0, 23)) &
        (col("month").between(1, 12))
    )

def _rules_air_quality(df: DataFrame) -> DataFrame:
    return df.filter(col("sample_measurement").isNotNull())

def _rules_taxi_zones(df: DataFrame) -> DataFrame:
    return df.filter(col("location_id").isNotNull())



COLUMN_RENAMES = {
    "taxi_trips": {
        "VendorID":            "vendor_id",
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count":     "passenger_count",
        "trip_distance":       "trip_distance",
        "RatecodeID":          "rate_code_id",
        "store_and_fwd_flag":  "store_and_fwd_flag",
        "PULocationID":        "pickup_location_id",
        "DOLocationID":        "dropoff_location_id",
        "payment_type":        "payment_type",
        "fare_amount":         "fare_amount",
        "extra":               "extra",
        "mta_tax":             "mta_tax",
        "tip_amount":          "tip_amount",
        "tolls_amount":        "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount":        "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee":         "airport_fee",
    },
    "weather": {
        "year": "year", "month": "month", "day": "day", "hour": "hour",
        "temp": "temp", "dwpt": "dew_point", "rhum": "rel_humidity",
        "prcp": "precipitation", "wdir": "wind_direction",
        "wspd": "wind_speed", "pres": "pressure",
        "coco": "condition_code",
    },
    "air_quality": {
        "State Code":       "state_code",
        "County Code":      "county_code",
        "Site Num":         "site_num",
        "Parameter Code":   "parameter_code",
        "POC":              "poc",
        "Latitude":         "latitude",
        "Longitude":        "longitude",
        "Datum":            "datum",
        "Parameter Name":   "parameter_name",
        "Date Local":       "date_local",
        "Time Local":       "time_local",
        "Date GMT":         "date_gmt",
        "Time GMT":         "time_gmt",
        "Sample Measurement": "sample_measurement",
        "Units of Measure": "units_of_measure",
        "MDL":              "mdl",
        "Uncertainty":      "uncertainty",
        "Qualifier":        "qualifier",
        "Method Type":      "method_type",
        "Method Code":      "method_code",
        "Method Name":      "method_name",
        "State Name":       "state_name",
        "County Name":      "county_name",
        "Date of Last Change": "date_of_last_change",
    },
    "taxi_zones": {
        "LocationID":    "location_id",
        "Borough":       "borough",
        "Zone":          "zone",
        "service_zone":  "service_zone",
    },
}



DATASETS = {
    "taxi_trips": {
        "path":         "data/raw/taxi_trips/",
        "format":       "parquet",
        "primary_key":  None,           # no unique key exists
        "required_columns": ["pickup_datetime", "dropoff_datetime", "trip_distance", "passenger_count",
                             "fare_amount", "pickup_location_id", "dropoff_location_id"],
        "partition_by": ["year", "month"],
        "rules":        _rules_taxi_trips,
    },
    "weather": {
        "path":         "data/raw/weather/weather.csv",
        "format":       "csv",
        "primary_key":  ["year", "month", "day", "hour"],
        "required_columns": ["year", "month", "day", "hour", "temp"],
        "partition_by": None,
        "rules":        _rules_weather,
    },
    "air_quality": {
        "path":         "data/raw/air_quality/hourly_88101_2024.csv",
        "format":       "csv",
        "primary_key":  ["state_code", "county_code", "site_num",
                         "parameter_code", "poc", "date_local", "time_local"],
        "required_columns": ["state_code", "county_code", "site_num", "date_local", "time_local", "sample_measurement"],
        "partition_by": ["year", "month"],
        "rules":        _rules_air_quality,
    },
    "taxi_zones": {
        "path":         "data/raw/taxi_zones/taxi_zone_lookup.csv",
        "format":       "csv",
        "primary_key":  ["location_id"],
        "required_columns": ["location_id", "borough", "zone"],
        "partition_by": None,
        "rules":        _rules_taxi_zones,
    },
}
