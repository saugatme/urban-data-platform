# Urban Data Integration Platform — Weeks 1 and 2

A Spark and Delta Lake platform for urban-data ingestion, integration, analytics, and performance evaluation. Week 1 builds the validated Bronze, Silver, and integrated Gold layers; Week 2 adds analytical queries, reusable Gold products, and Spark optimization experiments.

## Data layout

```text
data/raw/                 source files, unchanged
data/bronze/              validated Delta tables
data/silver/              common-model Delta tables
data/gold/integrated_taxi_trips      integrated trip table
data/gold/data_products/             Week 2 reusable analytical Delta tables
data/rejected/            invalid rows with a reason
data/metadata/            ingestion_log
data/benchmark/           storage benchmark outputs
```

## Repository layout

```text
src/common/       shared Spark-session setup
src/ingestion/    Week 1 ingestion and Silver transformations
src/integration/  Week 1 Gold-table integration
src/benchmark/    Week 1 storage benchmark
src/analytics/    Week 2 queries, data products, and optimization
docs/week1/       Week 1 reports
docs/week2/       Week 2 reports and run guide
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

Java 17 specifically: Spark 3.5 does not run reliably on Java 21. A trivial job such as `spark.range(5).count()` can succeed while real shuffle work fails with a `BlockManagerId` null-pointer error, so a minimal smoke test is not sufficient evidence that the runtime is supported. On a machine whose hostname resolves to a loopback address, also set `SPARK_LOCAL_IP=127.0.0.1`, or Spark may bind to a LAN address and fail to send its JARs.

```bash
uv venv --python 3.12
.venv\Scripts\activate
uv pip install -r requirements.txt
```

On Windows, configure Hadoop helpers if required by your local Spark installation. Set `HADOOP_HOME`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` in `.env`.

## Fresh clone / GitHub setup

Start from the repository root after cloning:

```bash
git clone <repository-url>
cd urban-data-platform
uv venv --python 3.12
.venv\Scripts\activate
uv pip install -r requirements.txt
```

Install Java 17 and confirm it is available:

```powershell
java -version
```

The course data is not included in Git because `data/` is large and generated output must not be versioned. Download the required course datasets and place them in the paths listed above before running the pipeline.

On Windows, copy `.env.example` to `.env` only if your Spark installation requires the optional local settings:

```powershell
Copy-Item .env.example .env
```

The first Spark run requires internet access so Delta can resolve its matching JARs; Spark caches them locally for later runs.

## Run

Run the stages in this order.

```bash
python run_ingestion.py
python run_integration.py
python run_benchmark.py
```

`run_ingestion.py` writes bronze and silver tables. `run_integration.py` writes `data/gold/integrated_taxi_trips`. `run_benchmark.py` compares partitioned and flat taxi-trip storage. Use `python inspect_bronze.py` to inspect bronze tables and ingestion metadata.

Latest run: 8,480,836 taxi trips were accepted; weather, air quality, and taxi zones had no rejected rows. The benchmark result is documented below.

## Week 2 analytics

Week 2 uses the integrated Gold table created by Week 1. Create the analytical products, then run the optimization evaluation:

```bash
python -m src.analytics.data_products
python -m src.analytics.optimization --evaluate
python run_week2_query_benchmark.py
```

The first command writes the reusable Delta products under `data/gold/data_products/`; the second evaluates caching, partition pruning, broadcast joins, AQE, and product-storage overhead. The third runs all six analytical-query functions three times and reports the median of the two warm runs.

- [Week 2 design report](docs/week2/design_report.md) and [LaTeX source](docs/week2/design_report.tex) — analytical design, trade-offs, and measured evidence
See [the Week 2 guide](docs/week2/README.md) for prerequisites, source layout, reports, and notebook usage.

## Week 3 operations

Week 3 takes the platform from a one-shot build to one that can receive a second data release, keep its products correct, and report on itself. It needs Weeks 1 and 2 to have run first.

```bash
python scripts/generate_incremental_updates.py
python run_incremental_update.py
python -m src.monitoring.queries
python run_week3_evaluation.py
```

The first command synthesises a second release into `data/incoming/` by sampling and perturbing the existing data, adding `humidity` to weather and `aqi` to air quality as documented schema evolution. The second merges it into Bronze without a rebuild, then rebuilds Silver and Gold and refreshes the analytical products against their watermarks. The third answers the four monitoring questions from `data/monitoring/pipeline_log`. The fourth measures the five Week 3 overheads into `data/benchmark/week3_evaluation.json`.

Running `run_incremental_update.py` twice is the idempotency check: the second run should insert 0 rows for every dataset.

See the [Week 3 design report](docs/week3/design_report.md) and [evaluation report](docs/week3/evaluation_report.md).

## Design

- Names use `snake_case`.
- The Spark session uses the `America/New_York` timezone for all timestamp handling.
- Taxi trips, air quality, and integrated trips are partitioned by `year` and `month`.
- Weather and taxi zones are not partitioned.
- All contextual joins are left joins, so accepted taxi trips remain in the integrated table.
- Dataset-specific details are in `src/ingestion/config.py`; the ingestion flow is shared.

## Documentation

- [Week 1 documentation](docs/week1/README.md) — architecture, data model, ingestion, integration, and benchmarking
- [Week 2 documentation](docs/week2/README.md) — analytics, data products, optimization, and evaluation
- [Documentation index](docs/README.md) — complete documentation map
