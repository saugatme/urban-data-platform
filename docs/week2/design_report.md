# Urban Data Integration Platform — Week 2 Design Report

**Module:** Data-intensive Computing
**Submission:** Week 2 — Analytics, data products, and optimisation
**Project:** Urban Data Integration Platform

## 1. Purpose and analytical requirements

Week 2 turns the integrated NYC taxi Delta table from Week 1 into an analytical layer. The source is the canonical `data/gold/integrated_taxi_trips` table, containing 8,480,836 cleaned and enriched trip records. The analytics layer answers six stakeholder questions without modifying that source:

1. Which pickup zones have the highest monthly demand?
2. How do trip distances vary with weather conditions?
3. Does PM2.5 air quality correspond with taxi demand?
4. Which zones have the greatest demand variability under different weather conditions?
5. What are the busiest pickup hours for each day of the week?
6. How does taxi demand change month over month?

All six analyses are implemented as reusable Spark SQL DataFrame functions in `src/analytics/queries.py`. This keeps query logic testable and makes it available both to the notebook and to the command-line benchmark. The functions return DataFrames, so they are read-only with respect to the integrated table. Timestamp calculations use the Week 1 `America/New_York` session timezone, preventing an artificial shift in local day or hour.

## 2. Analytical query design

The query design deliberately favours compact, interpretable outputs. Q1 groups by `year`, `month`, and pickup zone; Q2 groups distance statistics by normalised weather condition; Q3 aggregates demand by PM2.5 category; Q4 first aggregates zone/condition counts and then calculates a standard deviation; Q5 derives weekday and hour from pickup time; and Q6 aggregates monthly trip totals before calculating the month-over-month change. Zone names are obtained by joining to the canonical 265-row Silver taxi-zone Delta lookup rather than rereading a raw CSV.

The notebook, `notebooks/week2_queries.ipynb`, provides interactive demonstrations and recorded result samples. The standalone command below runs the same six functions three times each and reports the median of runs 2–3, so a marker can reproduce query timing on the submission machine:

```powershell
python run_week2_query_benchmark.py
```

The first iteration is deliberately excluded because Spark session startup, Delta metadata loading, and file-system caching can make a cold run unrepresentative. The complete analytical design and output schemas are documented in [Tasks 1–2](task_1_2_analytical_queries.md).

## 3. Materialised data products

Four Gold-layer Delta products are generated automatically by `src/analytics/data_products.py`. Each uses the integrated table as its source and records `data_source`, `creation_time`, `refresh_time`, and `schema_version` metadata.

| Product | Primary users | Value |
|---|---|---|
| `daily_mobility` | transport planners | Daily zone-level demand, distance, revenue, and fare trends without a trip-level scan. |
| `taxi_zone_statistics` | operations analysts | Whole-period zone comparison for demand, distance, revenue, and fare. |
| `weather_impact` | mobility and weather analysts | Trip outcomes by temperature bucket and normalised weather condition. |
| `air_quality_impact` | policy analysts | Trip outcomes by PM2.5 category. |

The products are persisted as Delta tables under `data/gold/data_products/`. Rerunning the generator overwrites only those derived tables; it does not alter the Week 1 Bronze, Silver, or integrated Gold layers. The product set occupies 1.25 MB against a 1.19 GB integrated source (0.10%), so the storage trade-off is very small relative to the benefit of pre-computed aggregates. Product definitions, metadata, and refresh approach are detailed in [Task 4](task_4_data_products.md).

## 4. Optimisation strategy and results

The project evaluates four complementary Spark optimisation techniques in `src/analytics/optimization.py`: persistence for repeated workloads, partition filtering, a broadcast join for a small dimension table, and Adaptive Query Execution (AQE). Every experiment uses the same result before and after optimisation; the script checks equality and prints `EXPLAIN FORMATTED` output, which provides plan-level evidence rather than assuming that a configuration change was applied.

The measured results below were obtained locally with Spark 3.5.9 and Delta Lake 3.2.1. Each figure is the median of runs 2–3; run 1 was discarded as warm-up.

| Technique | Baseline | Optimised | Change | Interpretation |
|---|---:|---:|---:|---|
| Cache repeated aggregation | 1.070 s | 0.536 s | 49.91% faster | Persisted input avoids repeat read and recomputation. |
| `year = 2024` partition filter | 0.861 s | 0.778 s | 9.64% faster | Plan shows `PartitionFilters`; gain is limited by mostly single-year data. |
| Broadcast taxi-zone lookup | 3.368 s | 1.244 s | 63.06% faster | Avoids shuffling 8.48M trips for a 265-row lookup. |
| AQE | 1.187 s | 1.232 s | 3.79% slower | The join was already broadcastable, leaving little for runtime adaptation. |

Broadcast join is the strongest result because the lookup/source size asymmetry is extreme. AQE is not presented as universally harmful: this small, already well-planned local workload simply has too little skew or repartitioning opportunity to offset AQE's planning overhead. At larger multi-city scale, AQE should be re-evaluated with production partition sizes and skewed keys. The reproducible experiment and plan discussion are in [Task 3](task_3_query_optimization.md).

## 5. Engineering decisions and trade-offs

Shared Week 2 paths, weather labels, and weather SQL mappings live in `src/analytics/constants.py`, avoiding disagreement between query, product, and optimisation modules. `src/analytics/runtime.py` sets a user-writable, process-local Spark temporary directory before Spark starts. On Windows this avoids the non-fatal JAR cleanup warning that can occur when Spark inherits `C:\\Windows\\Temp`; it does not change data locations or the result of any query.

The main performance trade-off is between refresh cost and query latency. The project does not cache every table permanently: caching is applied only in the repeated-workload experiment because storage pressure and stale cached data are real operational costs. Similarly, data products use batch overwrite because Week 2 is based on a fixed integrated dataset. For a continuous multi-city feed, incremental Delta `MERGE` refreshes and a `city` partition column would be preferable.

Q1 and Q4 remain relatively expensive because they scan the full trip table and perform multi-level aggregations. Q4 adds a second shuffle to calculate standard deviation across weather-condition groups. Scaling to ten cities therefore requires partitioning by `city`, reviewing `spark.sql.shuffle.partitions`, clustering/Z-ordering frequently filtered columns, incremental product refresh, and execution on a distributed cluster rather than local mode. These conclusions are expanded in [Task 5](task_5_platform_evaluation.md).

## 6. Reproducibility and submission contents

From a fresh checkout, install the requirements, provide the course datasets locally, then run the Week 1 ingestion and integration commands before any Week 2 command. Create the analytical products with `python -m src.analytics.data_products`, execute optimisation evidence with `python -m src.analytics.optimization --evaluate`, and run six-query timing with `python run_week2_query_benchmark.py`. The root [README](../../README.md) contains the complete setup and execution order.

The Week 2 submission includes the source code, the analytical notebook, four task reports, this 3–5 page design report, the benchmark methodology and results, and executable instructions. No Week 1 implementation has been changed as part of the Week 2 analytics work.
