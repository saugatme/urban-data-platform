# Task 3 – Monitoring

Week 1 already logged ingestion runs to `data/metadata/ingestion_log`. Week 3 adds a second, cross-pipeline log at `data/monitoring/pipeline_log` so every pipeline — not just ingestion — reports on itself in one place.

Both are kept. `ingestion_log` remains the Week 1 record of what was ingested; `pipeline_log` is the operational view the monitoring queries run against.

---

## What is recorded

One row per dataset per pipeline run, written by `record_run()` in `src/monitoring/monitor.py`.

| Column | Purpose |
|---|---|
| `pipeline` | Which pipeline wrote the row (`incremental_update`, `silver_rebuild`, `gold_rebuild`, `product_refresh`) |
| `dataset` | Which dataset the row is about |
| `layer` | `bronze` / `silver` / `gold` |
| `status` | `success` or `failed` |
| `processed_records` | Rows read from the update |
| `inserted_records` | Rows actually added |
| `rejected_records` | Rows sent to the rejected table |
| `execution_time_sec` | Wall-clock duration |
| `schema_version` | Dataset schema version at the time of the run |
| `validation_failures` | JSON map of rejection reason → count |
| `validation_failure_count` | Total, so it can be summed without parsing JSON |
| `schema_changes` | JSON summary of `evolved` / `unexpected` / `missing` columns |
| `run_at` | UTC timestamp |

Two design points are worth stating.

**`validation_failures` is JSON, not columns.** A new validation rule invents a new reason. Storing reasons as a map means adding a rule requires no schema migration, which is what keeps the validation framework genuinely pluggable. `validation_failure_count` is stored alongside it so the common aggregation never needs to parse the JSON.

**The schema is declared, not inferred.** `PIPELINE_LOG_SCHEMA` is explicit, so appends from four different pipelines stay compatible instead of drifting on whatever the first writer happened to produce.

### Failures are recorded, not just successes

The `monitored()` context manager records a row with `status = "failed"` when a pipeline raises, then re-raises. Without this, a crashed run would simply be *absent* from the log — and absence is indistinguishable from "never ran". A monitoring system that only records successes cannot answer "what broke".

---

## The four questions

Implemented as named SQL constants in `src/monitoring/queries.py` and runnable with:

```bash
python -m src.monitoring.queries
```

### 1. Which dataset fails validation most?

```sql
SELECT dataset,
       COUNT(*)                      AS runs,
       SUM(validation_failure_count) AS total_validation_failures,
       SUM(rejected_records)         AS total_rejected,
       ROUND(SUM(rejected_records) / NULLIF(SUM(processed_records), 0) * 100, 3) AS rejected_pct
FROM pipeline_log
GROUP BY dataset
ORDER BY total_validation_failures DESC, total_rejected DESC
```

The percentage matters more than the count: a dataset with ten times the volume will naturally have more rejections without being worse.

### 2. Which dataset takes longest to process?

```sql
SELECT dataset, pipeline, COUNT(*) AS runs,
       ROUND(AVG(execution_time_sec), 3) AS avg_seconds,
       ROUND(MAX(execution_time_sec), 3) AS max_seconds,
       ROUND(SUM(execution_time_sec), 3) AS total_seconds
FROM pipeline_log
GROUP BY dataset, pipeline
ORDER BY avg_seconds DESC
```

Grouping by pipeline as well as dataset separates a slow merge from a slow rebuild, which need different fixes.

### 3. How many records were rejected per run, and why?

```sql
SELECT run_at, pipeline, dataset, processed_records, inserted_records,
       rejected_records, validation_failures
FROM pipeline_log
WHERE rejected_records > 0
ORDER BY run_at DESC, rejected_records DESC
```

`validation_failures` is carried through so the answer includes the reason, not just the count.

### 4. How does processing time trend across runs?

```sql
SELECT dataset, run_at, execution_time_sec,
       ROUND(execution_time_sec - LAG(execution_time_sec)
             OVER (PARTITION BY dataset ORDER BY run_at), 3) AS change_vs_previous_run
FROM pipeline_log
ORDER BY dataset, run_at
```

The `LAG` window is partitioned by dataset so a trend never compares one dataset's run against another's.

---

## Sample output

Produced against the synthetic fixture described in [the evaluation report](evaluation_report.md). Numbers from the full dataset will differ; the shape is what matters here.

```text
============================================================
Validation failures by dataset
============================================================
dataset      runs  total_validation_failures  total_rejected  rejected_pct
taxi_trips   2     1                          1               11.111
weather      2     0                          0               0.0
air_quality  2     0                          0               0.0
```

The single `taxi_trips` failure is the deliberately planted trip carrying an unknown zone id.

---

## What is useful, and what is missing

**Useful.** Having `processed`, `inserted` and `rejected` as separate columns is what makes the log diagnostic rather than decorative. `processed > 0` with `inserted = 0` is the signature of a correctly idempotent re-run; the same pattern with a non-zero `rejected` count is a data problem. Neither is visible from a duration alone.

**Missing.** Three gaps are worth naming honestly:

- **No alerting.** The log is queried on demand. A real deployment would need a threshold that fires when `rejected_pct` crosses a bound.
- **No row-level lineage.** The log says 1 row was rejected for `missing_reference_taxi_zone_ids`; finding *which* row means reading `data/rejected/<dataset>`. The two are linked only by dataset and timestamp.
- **No resource metrics.** Wall-clock time is recorded, but not memory, shuffle volume or spill — so the log can show that a run got slower without showing why.

**Cost.** Each run appends a single small row, so the log grows by one row per dataset per pipeline invocation. Its measured size and write cost are in [the evaluation report](evaluation_report.md).
