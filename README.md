# Urban Data Integration Platform

A Spark and Delta Lake platform for the Week 1 Urban Data Integration Platform assignment. It ingests taxi trips, weather, air quality, and taxi zones; validates and standardizes them; and creates one integrated Delta table.

## Data layout

```text
data/raw/                 source files, unchanged
data/bronze/              validated Delta tables
data/silver/              common-model Delta tables
data/gold/                integrated_taxi_trips
data/rejected/            invalid rows with a reason
data/metadata/            ingestion_log
data/benchmark/           storage benchmark outputs
```

Put the course datasets in these paths:

```text
data/raw/taxi_trips/
data/raw/weather/weather.csv
data/raw/air_quality/hourly_88101_2024.csv
data/raw/taxi_zones/taxi_zone_lookup.csv
```

## Setup

Use Python 3.11 or 3.12 and Java 17.

```bash
uv venv --python 3.12
.venv\Scripts\activate
uv pip install -r requirements.txt
```

On Windows, configure Hadoop helpers if required by your local Spark installation. Set `HADOOP_HOME`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` in `.env`.

## Run

Run the stages in this order.

```bash
python run_ingestion.py
python run_integration.py
python run_benchmark.py
```

`run_ingestion.py` writes bronze and silver tables. `run_integration.py` writes `data/gold/integrated_taxi_trips`. `run_benchmark.py` compares partitioned and flat taxi-trip storage. Use `python inspect_bronze.py` to inspect bronze tables and ingestion metadata.

Latest run: 8,480,836 taxi trips were accepted; weather, air quality, and taxi zones had no rejected rows. The benchmark result is documented below.

## Design

- Names use `snake_case`.
- The Spark session uses the `America/New_York` timezone for all timestamp handling.
- Taxi trips, air quality, and integrated trips are partitioned by `year` and `month`.
- Weather and taxi zones are not partitioned.
- All contextual joins are left joins, so accepted taxi trips remain in the integrated table.
- Dataset-specific details are in `src/ingestion/config.py`; the ingestion flow is shared.

See `docs/architecture_diagram.md` for the architecture diagram, and `docs/t6_benchmark_report.md` for the completed benchmark report.

- Documents relating to the task specifics and architecture design can be found in `docs/`
