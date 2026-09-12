# Task 6: Benchmark Report

## Storage Strategies Evaluated

| | Strategy A | Strategy B |
|---|---|---|
| Partition | `year`, `month` | None (flat) |
| Description | 3 partitions — one per month (Jan, Feb, Mar) | Single flat Delta table |

Both strategies are built from the same silver taxi trips table (8,480,870 rows).

---

## Results

| Metric | Strategy A (year/month) | Strategy B (flat) |
|---|---|---|
| Ingestion time | 19.8s | 7.4s |
| Storage size | 173.6 MB | 173.7 MB |
| Parquet files | 15 | 9 |
| Q1: trips per borough | 1.04s | 0.66s |
| Q2: avg duration per day | 1.47s | 0.75s |
| Q3: avg fare per borough | 1.21s | 1.16s |

---

## Discussion

### Ingestion Time
Strategy A took 2.7× longer to ingest (19.8s vs 7.4s). Partitioning requires Spark to shuffle data by partition key before writing, then write each partition as a separate set of files. This shuffle cost is real even at Q1 2024 scale (3 months, 3 partitions).

### Storage Size
Storage size is virtually identical (173.6 MB vs 173.7 MB). Partitioning does not compress data — it only reorganizes it into subfolders. The same Parquet encoding applies regardless.

### File Count
Strategy A produced 15 parquet files vs 9 for Strategy B. With only 3 month-partitions and 8 Spark shuffle partitions, each month gets up to 5 files. More files means more file metadata overhead at read time.

### Query Latency
All three queries are marginally faster on Strategy B (flat). This is the key finding: **partitioning did not help any of these queries.**

The reason: none of the three queries filter on `year` or `month`. Trips per borough and fare per borough scan all trips regardless of time. Average duration per day aggregates by date — not by the partition column. Spark cannot prune any partitions, so Strategy A reads all 15 files while Strategy B reads 9.

### When Would Strategy A Win?
Partition pruning only activates when the query filters on the partition column. For example:

```sql
SELECT COUNT(*) FROM taxi_trips WHERE month = 1
```

This query would read only the January partition (5 files) in Strategy A, vs all 9 files in Strategy B. At larger scale (12 months, 190M rows), the benefit becomes significant. At Q1 2024 scale with 3 partitions, the overhead outweighs the savings for full-scan queries.

### When Does Partitioning Become Harmful?
At this scale, Strategy A is already marginally slower for all queries tested. The reasons:

1. **Too few rows per partition** — ~2.8M rows per month is not large enough for partition pruning to overcome file discovery overhead
2. **Queries don't filter on partition key** — all three benchmark queries scan the full dataset
3. **Small files** — 15 files vs 9 files increases metadata overhead with no read benefit

At 20× scale (12 months, 190M rows), Strategy A would win decisively on time-filtered queries. At current scale, the difference is negligible and partitioning primarily benefits future growth, not current performance.

---

## Conclusion

For Q1 2024 data at current scale, flat storage (Strategy B) is marginally faster for full-scan analytical queries. Strategy A (partitioned) is the correct long-term design because the dataset grows continuously and future queries will filter by time period — but its benefit is not yet visible at 3-month scale. The 2.7× ingestion overhead of partitioning is the most significant measured difference.
