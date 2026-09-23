# Task 3 – Query Optimization

## Setup

Each experiment ran the same query three times. The first run was excluded because Spark does extra setup on the first execution. Times shown are the middle value of runs two and three. Both versions of each query were checked to confirm they returned the same results.

---

## Results

| Technique         | Before   | After    | Change      | Results     |
|-------------------|----------|----------|-------------|-------------|
| Caching           | 0.776 s  | 0.459 s  | 40.85% faster | ✓ Identical |
| Partition pruning | 0.706 s  | 0.567 s  | 19.69% faster | ✓ Identical |
| Broadcast join    | 3.262 s  | 1.266 s  | 61.19% faster | ✓ Identical |
| AQE               | 0.953 s  | 1.161 s  | 21.83% slower | ✓ Identical |

---

## Plan Evidence

| Technique         | What appeared in the execution plan |
|-------------------|--------------------------------------|
| Caching           | `InMemoryRelation`, `InMemoryTableScan` |
| Partition pruning | `PartitionFilters: [isnotnull(year), year = 2024]` |
| Broadcast join    | `BroadcastExchange`, `BroadcastHashJoin` |
| AQE               | `AdaptiveSparkPlan` |

---

## Caching

The 2024 trips were saved in memory after the first read. Later queries read from memory instead of going back to disk each time.

**Why it helped:** Reading from memory is faster than reading from disk. Most queries use the same trips data, so keeping it in memory avoids repeated reads.

**Trade-off:** Cached data uses memory. If the dataset is too large or only queried once, caching is not worth it.

---

## Partition Pruning

Adding `WHERE year = 2024` told Spark to only read the 2024 folder on disk instead of all years.

**Why it helped:** The trips table is stored in separate folders by year and month. Filtering by year means Spark skips the other years entirely.

**Trade-off:** Only works when filtering on the columns used for folder organisation. The gain here was moderate because the dataset is mostly 2024 anyway.

---

## Broadcast Join

The taxi-zone lookup (265 rows) was sent to every machine before the join, instead of moving the large trips data around.

**Why it helped:** The trips table has 8.4 million rows. Moving that around to match zone records is expensive. Sending the 265-row lookup table to each machine instead is much cheaper.

**Trade-off:** The small table has to fit in memory on each machine. A large table cannot be broadcast this way.

---

## AQE (Adaptive Query Execution)

AQE lets Spark adjust its plan while a query is running. It was tested by running the same query with AQE off and then on.

**Result:** AQE made the query 21.83% slower. The join was already using the most efficient approach before AQE was turned on. AQE had nothing to improve but still added its own overhead.

**Trade-off:** AQE helps most when Spark has to decide between approaches at runtime. When the plan is already optimal, it adds cost instead.