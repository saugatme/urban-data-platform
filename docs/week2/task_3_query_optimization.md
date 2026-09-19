# Task 3 – Query Performance Optimization

## Objective

Four Spark optimization techniques were evaluated against the integrated urban taxi dataset:

1. Caching frequently accessed data
2. Partition pruning
3. Broadcast joins
4. Adaptive Query Execution (AQE)

Each optimization was compared against a baseline using the median of runs 2–3; run 1 was excluded to avoid cold-start effects. Results were verified by comparing the small aggregated result sets after sorting.

---

## Methodology

- Three runs per query; median of runs 2–3 used to reduce cold-start noise
- `EXPLAIN FORMATTED` used to confirm physical plan changes
- Sorted result rows used to verify result correctness

---

## Results Summary

| Optimization      | Baseline | Optimized | Improvement | Correctness |
|-------------------|----------|-----------|-------------|-------------|
| Caching           | 1.070 s  | 0.536 s   | 49.91%      | ✓ Identical |
| Partition Pruning | 0.861 s  | 0.778 s   | 9.64%       | ✓ Identical |
| Broadcast Join    | 3.368 s  | 1.244 s   | 63.06%      | ✓ Identical |
| AQE               | 1.187 s  | 1.232 s   | -3.79%      | ✓ Identical |

---

## Physical Plan Evidence

| Optimization      | Physical Plan Operator                        |
|-------------------|-----------------------------------------------|
| Caching           | `InMemoryRelation`, `InMemoryTableScan`        |
| Partition Pruning | `PartitionFilters: [isnotnull(year), year = 2024]` |
| Broadcast Join    | `BroadcastExchange`, `BroadcastHashJoin`      |
| AQE               | `AdaptiveSparkPlan`                           |

---

## Optimization 1 – Caching

The 2024 subset of the Trips table was cached using `spark.sql(...).cache()`. Subsequent queries read from `InMemoryTableScan` instead of scanning Delta files.

**Why appropriate:** The Trips table is the central dataset used by multiple queries. Caching avoids repeated file I/O when the same data is reused.

**Trade-offs:** Cached data consumes executor memory and may be evicted under memory pressure. Most beneficial when data is accessed more than once.

---

## Optimization 2 – Partition Pruning

The optimized query added `WHERE year = 2024`. Since the table is partitioned by `year` and `month`, Spark skipped all other year partitions.

**Why appropriate:** Many analytical queries target a single year. Filtering on a partition column directly reduces the files Spark needs to read.

**Note:** The 4.22% improvement was modest, but the physical plan confirmed `PartitionFilters: year = 2024` was applied. Benefit grows with dataset size and when more partitions are eliminated.

**Trade-offs:** Only effective when filtering on partition columns. Over-partitioning can create many small files and increase overhead.

---

## Optimization 3 – Broadcast Join

The taxi-zone lookup (265 rows) was broadcast to all executors using `/*+ BROADCAST(z) */`, avoiding a shuffle of the large Trips dataset.

**Why appropriate:** The Trips table is orders of magnitude larger than the zone lookup. Broadcasting the small table eliminates the need to shuffle Trips data across the network.

**Trade-offs:** The broadcast table must fit in executor memory. Broadcasting a large table causes memory pressure or failures.

---

## Optimization 4 – AQE

The same join/aggregation query was run with `spark.sql.adaptive.enabled` set to `false` then `true`. AQE allowed Spark to adapt the physical plan at runtime based on actual data statistics.

**Why appropriate:** The query involves joins, aggregations, and shuffles — all operations where runtime data characteristics can inform better execution decisions.

**Trade-offs:** Introduces minor runtime planning overhead. Simple queries with no shuffles see little benefit.

---

## Findings

- **Broadcast join** produced the largest improvement (63.06%) — consistent with joining a large table against a 265-row lookup
- **Caching** produced the second largest improvement (49.91%) — demonstrates the cost of repeated Delta file reads
- **Partition pruning** improved by 9.64%; the physical plan confirmed pruning on `year = 2024`, but the single-year dataset limits its potential benefit
- **AQE** was 3.79% slower in this local run while preserving identical results; this small difference is normal timing variation because the broadcast join was already selected without AQE
- All four optimizations preserved result correctness