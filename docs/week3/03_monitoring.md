# Platform Monitoring

Each update, rebuild, and product refresh writes one small Delta row to `data/monitoring/operations_runs/`.

## Recorded fields

| Field | Purpose |
|---|---|
| `run_at`, `dataset`, `stage` | Identifies what ran and when |
| `processed`, `inserted`, `rejected` | Shows release outcome |
| `seconds`, `validation_seconds` | Shows processing cost |
| `schema_version`, `schema_changes` | Records compatible schema evolution |
| `validation_failures` | Stores rejection counts by reason |

## Monitoring questions

The monitoring script uses Spark SQL to show:

1. rejection totals by dataset;
2. average execution time by dataset;
3. each execution with its rejection reason; and
4. execution-time change between consecutive runs.

## Final observations

Taxi trips had the only rejected records: 8,490 rows, all labelled `duplicate_record`. The taxi update took 104.352 seconds, making it the largest maintenance operation. Weather and air-quality updates inserted every row with no rejections.

Some log entries have `processed=0`. These are valid rebuild or product-refresh stages, not failed ingestion attempts.

## Run

```powershell
scripts\operations\show_monitoring.py
```
