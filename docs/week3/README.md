# Operations and Maintenance

This section adds a small second data release to the completed platform. It validates the release, inserts only genuinely new rows, records what happened, refreshes the affected analytical products, and measures the cost.

## Before you start

Complete the existing pipeline first:

```bash
python run_ingestion.py
python run_integration.py
python -m src.analytics.data_products
```

The operations scripts use the established Bronze, Silver, Gold, and product tables. They do not change the Week 1 or Week 2 implementation.

## Run the workflow

```powershell
scripts\operations\create_update_release.py
scripts\operations\run_pipeline.py
scripts\operations\show_monitoring.py
scripts\operations\evaluate.py
```

| Step | Script | Result |
|---|---|---|
| 1 | `create_update_release.py` | Creates the second taxi, weather, and air-quality release. |
| 2 | `run_pipeline.py` | Validates and inserts new rows, keeps rejected rows separate, and refreshes affected products. |
| 3 | `show_monitoring.py` | Answers the four monitoring questions from the run log. |
| 4 | `evaluate.py` | Saves runtime, storage, validation, and monitoring measurements. |

## Documents

| Document | Description |
|---|---|
| [Incremental release](01_update_release.md) | Release contents, counts, duplicates, and schema additions |
| [Pipeline refresh](02_apply_and_refresh.md) | Insert-only processing and affected-product refresh |
| [Monitoring](03_monitoring.md) | Run-log fields and Spark SQL summaries |
| [Validation](04_validation.md) | Rules, rejected rows, and extensibility |
| [Evaluation](05_evaluation.md) | Final timing and storage results |
| [Evaluation report](evaluation_report.md) | Short final results and discussion |

## Final run summary

| Dataset | Processed | Inserted | Rejected |
|---|---:|---:|---:|
| Taxi trips | 517,165 | 508,675 | 8,490 intentional duplicates |
| Weather | 168 | 168 | 0 |
| Air quality | 168 | 168 | 0 |

The refreshed products were `daily_mobility`, `taxi_zone_statistics`, `weather_impact`, and `air_quality_impact`.

## Output locations

```text
data/updates/summary.json                    release counts and schema changes
data/rejected/<dataset>/                     excluded records and rejection reasons
data/monitoring/operations_runs/             Delta run log
data/benchmark/operations_evaluation.json    final measurements
data/gold/data_products/                     refreshed analytical products
```
