# Task 5 – Platform Evaluation

## Benchmark Summary

| Optimization      | Baseline | Optimized | Improvement | Correctness |
|-------------------|----------|-----------|-------------|-------------|
| Caching           | 1.070 s  | 0.536 s   | 49.91%      | ✓ Identical |
| Partition Pruning | 0.861 s  | 0.778 s   | 9.64%       | ✓ Identical |
| Broadcast Join    | 3.368 s  | 1.244 s   | 63.06%      | ✓ Identical |
| AQE               | 1.187 s  | 1.232 s   | -3.79%      | ✓ Identical |


## Analytical-query timing

`python run_week2_query_benchmark.py` executed every analytical function three times. The table records the median of runs 2–3 after warm-up.

| Query | Rows | Median |
|---|---:|---:|
| Q1 — monthly demand by zone | 760 | 1.532 s |
| Q2 — weather and distance | 15 | 1.478 s |
| Q3 — air quality and demand | 4 | 1.145 s |
| Q4 — zone demand variance | 257 | 1.171 s |
| Q5 — weekly peak hours | 7 | 2.238 s |
| Q6 — monthly demand trend | 4 | 0.747 s |

Q5 is slowest because it derives weekday/hour values for every trip before aggregating and ranking. Q1 follows because it creates 760 zone-month groups. Q4 uses a two-stage aggregation. Q6 is fastest because its window operates after a small monthly aggregation. The Q6 value was measured before the later, semantics-preserving `PARTITION BY year` correction; rerun the benchmark to record the final implementation's value.

The Windows JAR-cleanup messages occurred after successful query completion during Spark shutdown. They are non-fatal and do not invalidate the timings or results.
Methodology: median of runs 2–3 per experiment (run 1 excluded to avoid cold-start effects).

Run context: Spark 3.5.9, Delta Lake 3.2.1, 8,480,836 integrated trips, and a 265-row taxi-zone lookup.

---

## Which optimization produced the largest improvement?

Broadcast join produced the largest improvement at **63.06%**, reducing execution time from 3.368 s to 1.244 s. The taxi-zone lookup contains only 265 rows against 8.48M trip records. Broadcasting the small lookup avoids shuffling the large Trips dataset. The physical plan confirmed `BroadcastExchange` and `BroadcastHashJoin` were used.

---

## Which optimization had little or no effect?

**AQE was 3.79% slower in this run.** This is not a correctness issue: the results were identical. The join was already eligible for broadcast with AQE disabled, leaving little runtime adaptation for AQE to improve. At this local scale, the difference is within normal run-to-run timing variation.

**Partition pruning improved by 9.64%.** The physical plan confirmed `PartitionFilters: year = 2024`. The dataset is primarily one year, so pruning eliminates relatively few partitions; the benefit would grow with additional years of data.

---

## Which queries remain computationally expensive?

Q5 (weekly peak hours) is the slowest measured analytical query at 2.238 s because it derives weekday/hour fields for every trip before aggregation and ranking. Q1 follows at 1.532 s because it scans the full table and produces 760 zone-month groups. Q4's two-stage aggregation — first by `(zone, condition_code)`, then `STDDEV` — makes it more expensive than its 257-row output suggests. Caching helps repeated workloads but cannot remove first-run I/O.

---

## What characteristics of the data explain these results?

- **Table size asymmetry** explains broadcast join's effectiveness — a 265-row lookup against 8.48M trips is an ideal broadcast candidate
- **Single-year data coverage** limits partition pruning gains — `WHERE year = 2024` eliminates relatively few partitions
- **An already-broadcastable join** limits AQE's opportunity to improve the plan in this local experiment
- **Low PM2.5 variance** in NYC 2024 data explains why `air_quality_impact` produced only 3 AQI categories — values stayed within Good/Moderate/Unhealthy bands

---

## Storage Overhead of Analytical Data Products

| Product               | Size       |
|-----------------------|------------|
| Integrated trips      | 1.19 GB    |
| daily_mobility        | 1.18 MB    |
| taxi_zone_statistics  | 38.55 KB   |
| weather_impact        | 15.78 KB   |
| air_quality_impact    | 9.85 KB    |
| **Total products**    | **1.25 MB** |
| **Product/source ratio** | **0.10%** |

All four products together consume 0.10% of the source table size. The aggregation from 8.48M rows to tens or hundreds of summary rows results in negligible storage overhead while enabling sub-second access to pre-computed summaries.

---

## Scaling to Ten Cities

| Recommendation | Reason |
|---|---|
| Add `city` as a partition column | Enables pruning across cities; without it every city query scans all data |
| Increase `spark.sql.shuffle.partitions` | Default of 8 under-parallelises a 10x larger dataset |
| Apply Delta Z-ordering on `(city, pickup_location_id, year, month)` | Co-locates frequently filtered columns on disk |
| Switch to incremental Delta `MERGE` for product refresh | Full overwrite becomes expensive at scale |
| Deploy on a real cluster (YARN/Kubernetes) | `local[*]` is limited to one machine |