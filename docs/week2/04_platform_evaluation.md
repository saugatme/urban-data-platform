# Task 5 – Platform Evaluation

## Benchmark Summary

| Optimization      | Baseline | Optimized | Improvement | Correctness |
|-------------------|----------|-----------|-------------|-------------|
| Caching           | 1.046 s  | 0.738 s   | 29.45%      | ✓ Identical |
| Partition Pruning | 0.731 s  | 0.729 s   | 0.27%       | ✓ Identical |
| Broadcast Join    | 2.555 s  | 0.907 s   | 64.50%      | ✓ Identical |
| AQE               | 0.797 s  | 0.797 s   | 0.00%       | ✓ Identical |

Methodology: median of runs 2–3 per experiment (run 1 excluded to avoid cold-start effects).

---

## Which optimization produced the largest improvement?

Broadcast join produced the largest improvement at **64.50%**, reducing execution time from 2.555 s to 0.907 s. The taxi-zone lookup contains only 265 rows against 8.4M trip records. Without broadcasting, Spark shuffles the Trips dataset to co-locate matching keys — an expensive network operation. Broadcasting the lookup eliminates that shuffle entirely. The physical plan confirmed `BroadcastExchange` and `BroadcastHashJoin` were used.

---

## Which optimization had little or no effect?

**AQE showed 0.00% improvement.** This is explained by the experimental setup: `autoBroadcastJoinThreshold` was already enabled before the AQE experiment, so Spark was already using a broadcast hash join in both the AQE-off and AQE-on runs. AQE's primary benefit — adapting join strategy at runtime — was not triggered because the plan was already optimal. On a query where no broadcast threshold is set and Spark must choose between shuffle and broadcast at runtime, AQE would show a more meaningful effect.

**Partition pruning showed only 0.27% improvement.** The physical plan confirmed `PartitionFilters: year = 2024` was applied correctly. The negligible timing gain reflects that the current dataset covers primarily 2024 — pruning eliminates very few partitions. At larger scale spanning multiple years, the improvement would be proportionally larger.

---

## Which queries remain computationally expensive?

Q1 (monthly demand by zone) and Q4 (zone demand variance) remain the most expensive. Both require a full scan of 8.4M trip records followed by multi-level aggregation. Q4 uses a two-stage aggregation — first grouping by `(zone, condition_code)`, then computing `STDDEV` across groups — introducing a second shuffle. Caching helps on repeated runs but the first execution still pays the full I/O cost.

---

## What characteristics of the data explain these results?

- **Table size asymmetry** explains broadcast join's effectiveness — 265-row lookup against 8.4M trips is an ideal broadcast candidate
- **Single-year dataset** limits partition pruning gains — the dataset is primarily 2024, so `WHERE year = 2024` eliminates almost nothing
- **Already-optimal join plan** explains AQE's 0% gain — broadcast threshold was pre-configured, leaving AQE nothing to adapt
- **Low PM2.5 variance** in NYC 2024 data explains why `air_quality_impact` produced only 3 AQI categories — values stayed within Good/Moderate/Unhealthy bands

---

## Storage Overhead of Analytical Data Products

| Product               | Size       |
|-----------------------|------------|
| Integrated trips      | 1.19 GB    |
| daily_mobility        | 606.16 KB  |
| taxi_zone_statistics  | 19.80 KB   |
| weather_impact        | 7.80 KB    |
| air_quality_impact    | 5.32 KB    |
| **Total products**    | **639.08 KB** |
| **Product/source ratio** | **0.05%** |

All four products together consume 0.05% of the source table size. The aggregation from 8.4M rows to tens or hundreds of summary rows results in negligible storage overhead while enabling sub-second query performance on pre-computed results.

---

## Scaling to Ten Cities

| Recommendation | Reason |
|---|---|
| Add `city` as a partition column | Enables pruning across cities; without it every city query scans all data |
| Increase `spark.sql.shuffle.partitions` | Default of 8 under-parallelises a 10x larger dataset |
| Apply Delta Z-ordering on `(city, pickup_location_id, year, month)` | Co-locates frequently filtered columns on disk |
| Switch to incremental Delta `MERGE` for product refresh | Full overwrite becomes expensive at scale |
| Deploy on a real cluster (YARN/Kubernetes) | `local[*]` is limited to one machine |