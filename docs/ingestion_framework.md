# Task 3: Generic Ingestion Framework

## Overview

The ingestion framework loads all four datasets from raw files, validates them, standardizes them, and stores them as Delta tables in the bronze layer. The framework is generic — one shared pipeline handles all datasets. Dataset-specific behaviour is defined in configuration, not in separate scripts.

---

## Architecture

```
Raw File
   │
   ▼
Load (CSV / Parquet)
   │
   ▼
Standardize Columns (rename to snake_case)
   │
   ▼
Normalize Timestamps (cast to timestamp type)
   │
   ▼
Add Partition Columns (year, month from primary timestamp)
   │
   ▼
Validate (null PKs, duplicate PKs → rejected table)
   │
   ▼
Apply Dataset Rules (business-level filters)
   │
   ▼
Save as Delta Table (bronze layer)
   │
   ▼
Log Metadata (ingestion_log Delta table)
```

---

## Components

### Generic Components (reusable across all datasets)

| Component | Description |
|---|---|
| `load()` | Auto-detects format (CSV/Parquet) from config and loads into DataFrame |
| `standardize_columns()` | Renames columns to snake_case using per-dataset rename maps |
| `normalize_timestamps()` | Casts configured columns to `timestamp` type |
| `add_partition_columns()` | Derives `year` and `month` from the primary timestamp column |
| `validate()` | Checks for null primary keys and duplicate primary keys; returns valid and rejected DataFrames |
| `save_delta()` | Writes DataFrame as a partitioned or unpartitioned Delta table |
| `log_metadata()` | Appends one row per run to a Delta-backed ingestion log |
| `ingest()` | Orchestrates all steps above for any dataset |
| `ingest_all()` | Loops over all configured datasets |

### Dataset-Specific Components

| Component | Description |
|---|---|
| Column rename maps (`COLUMN_RENAMES`) | Each dataset has different source column names |
| Business rules (`_rules_*`) | Each dataset has different validity constraints |
| Primary key definition | Differs per dataset; taxi trips has no primary key |
| Partition strategy | Taxi trips and air quality partition by `year/month`; weather and taxi zones are not partitioned |

---

## Configuration Design

All dataset-specific parameters are defined in a single `DATASETS` dictionary in `config.py`. Adding a new dataset requires only a new entry in this dictionary — no changes to the pipeline logic.

```python
DATASETS = {
    "taxi_trips": {
        "path":         "data/raw/taxi_trips/",
        "format":       "parquet",
        "primary_key":  None,
        "partition_by": ["year", "month"],
        "rules":        _rules_taxi_trips,
    },
    ...
}
```

Business rules are plain Python functions that take a DataFrame and return a filtered DataFrame. This keeps rules testable and explicit.

---

## Transformation Rules

| Dataset | Rules Applied |
|---|---|
| Taxi Trips | `trip_distance > 0`, `fare_amount >= 0`, `passenger_count > 0` |
| Weather | `hour` between 0–23, `month` between 1–12 |
| Air Quality | `sample_measurement` is not null |
| Taxi Zones | `location_id` is not null |

---

## Metadata Management

Every pipeline run appends one record to `data/metadata/ingestion_log` (a Delta table):

| Field | Description |
|---|---|
| `dataset` | Dataset name |
| `layer` | Bronze / silver |
| `processed_records` | Total rows loaded from raw |
| `rejected_records` | Rows removed by validation + rules |
| `accepted_records` | Rows written to Delta |
| `execution_time_sec` | Wall-clock time for the run |
| `ingested_at` | UTC timestamp of the run |
| `schema_version` | Version string for schema tracking |

Rejected rows are written to `data/rejected/{dataset}` as a separate Delta table with a `rejection_reason` column, so they can be inspected without interrupting the pipeline.

---

## Ingestion Results

| Dataset | Raw Rows | Accepted | Rejected | Time |
|---|---|---|---|---|
| Taxi Trips | 9,554,778 | 8,480,870 | 1,073,908 | 31s |
| Weather | 8,784 | 8,784 | 0 | 5s |
| Air Quality | 8,139,551 | 8,139,551 | 0 | 58s |
| Taxi Zones | 265 | 265 | 0 | 2s |

Taxi trips lost 11.2% of rows to business rules (zero-distance trips, negative fares, zero passengers) — consistent with known data quality issues in NYC taxi data.

---

## Design Decisions

### Why a config dict instead of separate scripts?
One pipeline function handles all datasets. Adding a new dataset means one new dictionary entry, not a new file. This eliminates code duplication and reduces maintenance surface.

### Why are rules functions and not strings?
String-based rules (e.g. SQL WHERE clauses) would require a parser. Python functions are directly testable, composable, and type-safe.

### Why Delta and not plain Parquet?
Delta provides ACID transactions, schema enforcement, time travel, and append support for the metadata log. These properties are required for Week 3 (incremental updates).

### If 20 new datasets were added
Only `config.py` would change. Each new dataset needs a path, format, primary key, partition strategy, column renames, and a rules function. The pipeline itself requires no modification.