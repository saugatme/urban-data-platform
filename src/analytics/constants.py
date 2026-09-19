"""Shared definitions for Week 2 analytical modules."""

from pyspark.sql import Column, functions as F


DATA_ROOT = "data"
INTEGRATED_TRIPS_PATH = f"{DATA_ROOT}/gold/integrated_taxi_trips"
SILVER_TAXI_ZONES_PATH = f"{DATA_ROOT}/silver/taxi_zones"
PRODUCT_BASE_PATH = f"{DATA_ROOT}/gold/data_products"

WEATHER_CONDITIONS = {
    1: "Clear",
    2: "Fair",
    3: "Cloudy",
    4: "Overcast",
    5: "Fog",
    6: "Freezing Fog",
    7: "Light Rain",
    8: "Rain",
    9: "Heavy Rain",
    10: "Freezing Rain",
    11: "Heavy Freezing Rain",
    12: "Sleet",
    13: "Heavy Sleet",
    14: "Light Snowfall",
    15: "Snowfall",
    16: "Heavy Snowfall",
}


def weather_condition_sql(column: str = "condition_code") -> str:
    """Return the SQL CASE expression for a Meteostat condition-code column."""
    cases = "\n".join(
        f"WHEN {code} THEN '{label}'" for code, label in WEATHER_CONDITIONS.items()
    )
    return f"CASE {column}\n{cases}\nELSE 'Unknown' END"


def weather_condition_column(column: str = "condition_code") -> Column:
    """Return the DataFrame equivalent of :func:`weather_condition_sql`."""
    expression = None
    for code, label in WEATHER_CONDITIONS.items():
        if expression is None:
            expression = F.when(F.col(column) == code, label)
        else:
            expression = expression.when(F.col(column) == code, label)
    return expression.otherwise("Unknown")
