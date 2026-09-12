# Urban Data Integration Platform

A reusable Spark + Delta Lake data engineering platform that ingests heterogeneous
urban datasets (taxi trips, weather, air quality, taxi zones), validates and
standardizes them, and integrates them into a single analytical dataset.

Built for the *Data-intensive Computing* course, Week 1: Build a Generic Urban Data Integration Platform.

---

## Project Structure

```
urban-data-platform/
├── data/
│   ├── raw/                        # Original files, NEVER modified (gitignored)
│   │   ├── taxi_trips/             # yellow_tripdata_2024-01/02/03.parquet
│   │   ├── weather/                # weather.csv
│   │   ├── air_quality/            # hourly_88101_2024.csv
│   │   └── taxi_zones/             # taxi_zone_lookup.csv
│   ├── bronze/                     # Ingested as Delta, minimal changes (gitignored)
│   ├── silver/                     # Cleaned, typed, standardized Delta tables (gitignored)
│   ├── gold/                       # Integrated analytical Delta tables (gitignored)
│   ├── benchmark/                  # Benchmark storage strategy outputs (gitignored)
│   ├── metadata/                   # Ingestion log Delta table (gitignored)
│   └── rejected/                   # Rejected rows with rejection_reason (gitignored)
├── src/
│   ├── common/
│   │   └── spark_session.py        # Shared Spark + Delta session builder (do not modify)
│   ├── ingestion/
│   │   ├── config.py               # Dataset configs: paths, formats, PKs, rules
│   │   ├── ingestor.py             # Generic ingestion pipeline (bronze layer)
│   │   └── silver.py               # Common data model enforcement (silver layer)
│   ├── integration/
│   │   └── integrate.py            # Enrichment joins → gold/integrated_taxi_trips
│   └── benchmark/
│       └── benchmark.py            # Storage strategy comparison + query latency
├── docs/
│   ├── data_catalog.md             # Task 1 deliverable
│   ├── storage_architecture.md     # Task 2 deliverable
│   ├── ingestion_framework.md      # Task 3 deliverable
│   ├── common_data_model.md        # Task 4 deliverable
│   ├── integration_pipeline.md     # Task 5 deliverable
│   ├── benchmark_report.md         # Task 6 deliverable
│   └── learning_guide_week1.md     # Concept explanations for the team
├── run_ingestion.py                # Entry point: bronze + silver pipeline
├── run_integration.py              # Entry point: gold integration pipeline
├── run_benchmark.py                # Entry point: storage strategy benchmark
├── inspect_bronze.py               # Utility: schema, null counts, ingestion log
├── notebooks/                      # Exploration and scratch work
├── requirements.txt
└── .env                            # Local environment variables (gitignored)
```

---

## Setup (new teammate — ~15 min)

### 1. Clone the repo

```bash
git clone https://github.com/saugatme/urban-data-platform.git
cd urban-data-platform
```

### 2. Python environment

Python 3.11 or 3.12 only — **PySpark 3.5 does not support Python 3.13.**

```bash
uv venv --python 3.12
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux / Mac / WSL
uv pip install -r requirements.txt
```

### 3. Java 17

Verify with `java -version`. If missing, install [Eclipse Temurin 17](https://adoptium.net).

### 4. Hadoop helpers (Windows only — skip on Linux/Mac/WSL)

- Download `winutils.exe` and `hadoop.dll` from https://github.com/steveloughran/winutils (hadoop-3.0.0/bin)
- Place both in `C:\hadoop\bin\`
- Create `.env` in the project root:

```
HADOOP_HOME=C:\hadoop
PYSPARK_PYTHON=python
PYSPARK_DRIVER_PYTHON=python
```

### 5. Download datasets

Download from the course page into `data/raw/` following the structure above.

### 6. Verify setup

Run `notebooks/exploration.ipynb` top to bottom. Expected: Spark session starts without errors.

---

## How to Run

Run these in order — each step depends on the previous.

### 1. Bronze + Silver (ingestion + standardization)

```bash
python run_ingestion.py
```

Loads all four datasets, validates, standardizes, and saves as Delta tables in `data/bronze/` and `data/silver/`. Expect ~2 minutes.

### 2. Gold (integration)

```bash
python run_integration.py
```

Enriches each taxi trip with weather, air quality, pickup zone, and dropoff zone. Output: `data/gold/integrated_taxi_trips`. Expect ~5 minutes.

### 3. Benchmark

```bash
python run_benchmark.py
```

Compares two storage strategies (partitioned vs flat) for taxi trips. Measures ingestion time, storage size, file count, and query latency. Expect ~10 minutes.

### Inspect bronze tables

```bash
python inspect_bronze.py
```

Prints schema, row counts, null counts per column, and ingestion log for all bronze tables.

---

## What the Pipeline Does

```
raw file
  → load (CSV / Parquet)
  → standardize columns (snake_case)
  → normalize timestamps
  → validate (null PKs, duplicates → rejected/)
  → apply business rules
  → save as Delta (bronze)
  → apply common data model (silver)
  → join weather + air quality + zones (gold)
```

**Bronze** — raw data in Delta with column renames and timestamp normalization. Invalid rows isolated to `data/rejected/`.

**Silver** — enforces the common data model: drops 100% null columns, fills measurement nulls, fixes incorrectly typed columns, casts categorical IDs to `integer`.

**Gold** — each trip enriched with hourly weather, hourly PM2.5 from a fixed NYC monitoring site (Queens, site 124), pickup zone/borough, and dropoff zone/borough. All joins are left joins — no trips are lost.

**Metadata** — every pipeline run appends to `data/metadata/ingestion_log` recording row counts and execution time.

---

## Dataset Summary

| Dataset | Format | Raw Rows | Accepted Rows | Partitioned |
|---|---|---|---|---|
| Taxi Trips | Parquet | 9,554,778 | 8,480,870 | `year`, `month` |
| Weather | CSV | 8,784 | 8,784 | None |
| Air Quality | CSV | 8,139,551 | 8,139,551 | `year`, `month` |
| Taxi Zones | CSV | 265 | 265 | None |
| Integrated Trips (gold) | Delta | — | 8,480,870 | `year`, `month` |

---

## Team Workflow

- **Never commit to `main`** — one branch per task: `git checkout -b task-N-description`
- `data/` and `.env` are gitignored — every teammate downloads datasets locally
- **Do not modify** `data/raw/` or `src/common/spark_session.py`
- Dataset-specific changes go in `src/ingestion/config.py` only — the pipeline itself stays generic