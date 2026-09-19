# Week 2 — Analytics and Optimization

Week 2 builds on the integrated Delta table created in Week 1. It adds analytical queries, reusable Gold-layer data products, and Spark optimization experiments. The Week 1 ingestion and integration pipeline must complete before any Week 2 command is run.

## Prerequisites

From the project root, create the Python environment and install dependencies as described in the root README. Then create the Week 1 tables if they do not already exist:

```bash
python run_ingestion.py
python run_integration.py
```

Week 2 reads these canonical Delta tables:

- `data/gold/integrated_taxi_trips`
- `data/silver/taxi_zones`

## Run Week 2

Create the reusable data products:

```bash
python -m src.analytics.data_products
```

Run the optimization experiments and include data-product storage overhead in the output:

```bash
python -m src.analytics.optimization --evaluate
```

Benchmark all six analytical-query functions with three runs each (the command reports the median of runs 2–3):

```bash
python run_week2_query_benchmark.py
```

The analytical-query demonstrations and their recorded output are in `notebooks/week2_queries.ipynb`. The query functions themselves are in `src/analytics/queries.py`; they return Spark DataFrames and do not modify the source table.

## Deliverables

### Analytical queries

Six queries analyse taxi demand by zone and month, weather-related trip distance, PM2.5 demand patterns, demand variance by weather condition, peak travel hours, and month-over-month demand. See the [Tasks 1–2 report](task_1_2_analytical_queries.md) for each question, design, and output schema.

### Reusable data products

The following Delta tables are written beneath `data/gold/data_products/`:

| Product | Purpose |
|---|---|
| `daily_mobility` | Daily trips, distance, revenue, and fare by pickup zone |
| `taxi_zone_statistics` | Whole-period trip, distance, revenue, and fare statistics by pickup zone |
| `weather_impact` | Trip outcomes grouped by temperature bucket and weather condition |
| `air_quality_impact` | Trip outcomes grouped by PM2.5 air-quality category |

Every product includes `data_source`, `creation_time`, `refresh_time`, and `schema_version` metadata. `trip_distance` values originate in miles, so the weather product exposes `avg_distance_miles`; other product distance columns retain the source unit.

### Optimization experiments

`src/analytics/optimization.py` evaluates caching, partition pruning, broadcast joins, and Adaptive Query Execution (AQE). It runs each query three times, excludes the first run as warm-up, and reports the median of runs 2–3. The output also includes `EXPLAIN FORMATTED` plans and a correctness comparison for each experiment.

Benchmark timings depend on the machine, Spark configuration, and cached state. Use the recorded reports as evidence for the assignment, not as universal performance guarantees:

- [Task 3 — Query optimization](task_3_query_optimization.md) — implementation and physical-plan evidence
- [Task 4 — Data products](task_4_data_products.md) — detailed reusable-product documentation
- [Task 5 — Platform evaluation](task_5_platform_evaluation.md) — storage overhead and scaling recommendations

## Submission documents

- [Week 2 design report](design_report.md) and [LaTeX source](design_report.tex) — submission-ready report and editable source

## Source layout

```text
src/analytics/constants.py       shared Week 2 paths and weather labels
src/analytics/runtime.py         Windows-safe temporary-directory setup
src/analytics/queries.py         six analytical query functions
src/analytics/data_products.py   materialised analytical products
src/analytics/optimization.py    optimization experiments and evaluation
notebooks/week2_queries.ipynb    interactive analysis and recorded evidence
docs/week2/task_1_2_analytical_queries.md  Tasks 1–2 report
docs/week2/task_3_query_optimization.md    Task 3 report
docs/week2/task_4_data_products.md         Task 4 report
docs/week2/task_5_platform_evaluation.md   Task 5 report
```

## Notes

- All timestamp handling uses the `America/New_York` Spark session timezone inherited from Week 1.
- The optimization broadcast-join experiment intentionally joins the large trips table to the small canonical silver taxi-zone lookup.
- Week 2 configures a process-local, user-writable Spark temporary directory. Windows can still log a non-fatal JAR-cleanup warning at Spark shutdown if a JAR remains locked; successful query output is unaffected.
- Rerunning the data-products command overwrites only the Week 2 product tables; it does not change the Week 1 bronze, silver, or integrated Gold tables.
