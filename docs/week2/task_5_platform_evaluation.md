# Task 5 – Platform Evaluation

## Optimization Results

| Technique         | Before   | After    | Change        | Results     |
|-------------------|----------|----------|---------------|-------------|
| Caching           | 0.776 s  | 0.459 s  | 40.85% faster | ✓ Identical |
| Partition pruning | 0.706 s  | 0.567 s  | 19.69% faster | ✓ Identical |
| Broadcast join    | 3.262 s  | 1.266 s  | 61.19% faster | ✓ Identical |
| AQE               | 0.953 s  | 1.161 s  | 21.83% slower | ✓ Identical |

Run context: Spark 3.5.9, Delta Lake 3.2.1, 8,480,836 trips, 265-row zone lookup. Median of runs 2–3; run 1 excluded for cold-start.

---

## Query Timings

| Query | Rows | Time |
|---|---:|---:|
| Q1 — monthly demand by zone | 760 | 1.532 s |
| Q2 — weather and distance | 15 | 1.478 s |
| Q3 — air quality and demand | 4 | 1.145 s |
| Q4 — zone demand variance | 257 | 1.171 s |
| Q5 — weekly peak hours | 7 | 2.238 s |
| Q6 — monthly demand trend | 4 | 0.747 s |

---

## Which technique helped most?

Broadcast join gave the biggest improvement at 61.19%. The zone lookup has only 265 rows against 8.4 million trips. Without broadcast, Spark moves trip data around to match zone records which is the expensive part. Sending the small lookup to each machine instead avoids all of that.

---

## Which technique had little or no effect?

AQE made the query 21.83% slower. The join was already using the most efficient approach before AQE was switched on. AQE had nothing to change but still added its own overhead on top.

Partition pruning improved by 19.69%. It worked correctly & the execution plan confirmed it only read 2024 data. The gain was moderate because almost all the data is already from 2024.

---

## Which queries are still slow?

Q5 is the slowest at 2.238 s. It has to work out the day and hour for every single trip before it can count and rank them. Q1 takes 1.532 s because it scans all 8.4 million trips and produces 760 groups. Q4 groups the data twice, which also makes it slower than its small output suggests.

---

## Why did the results out this way?

- The zone lookup is tiny (265 rows) compared to 8.4M trips, which is the situation where broadcast join helps most
- Most data is from 2024 already, so partition pruning had little to skip
- AQE slowed things down because the plan was already optimal before it was turned on
- NYC air quality in 2024 stayed in the lower pollution bands, which is why the air quality product only has 3 rows

---

## Storage

| Product               | Size      |
|-----------------------|-----------|
| Integrated trips      | 1.19 GB   |
| daily_mobility        | 1.77 MB   |
| taxi_zone_statistics  | 57.31 KB  |
| weather_impact        | 22.55 KB  |
| air_quality_impact    | 14.39 KB  |
| **Total products**    | **1.87 MB** |

All four products together use 0.15% of the space the source table takes up.

---

## If the platform expanded to ten cities

- Add a `city` folder level so queries for one city don't read data from all others
- Increase the number of parallel tasks Spark uses.
- Group related data together on disk so less is read per query
- Only process new records on each refresh instead of rewriting everything