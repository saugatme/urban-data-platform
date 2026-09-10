# Learning Guide: Urban Data Platform — Week 1

Everything we built, why we built it, and what it means. Written for understanding, not submission.

---

## The Big Picture

We have four datasets from different city departments. Different formats, different column names, different quality. The goal is a pipeline that:

1. **Understands** each dataset (Task 1)
2. **Decides how to store** them efficiently (Task 2)
3. **Loads them safely** and automatically (Task 3)
4. **Standardizes** them into a common language (Task 4)
5. **Joins them** into one enriched analytical dataset (Task 5)

Think of it as a kitchen: Task 1 is reading the ingredients, Task 2 is organizing the pantry, Task 3 is washing and chopping, Task 4 is converting all units to metric, Task 5 is cooking the final dish.

---

## Task 1: Data Catalog

### What is a data catalog?
A data catalog is documentation that describes what a dataset contains before you touch it with code. It answers: what is one row, what makes it unique, how does it connect to other datasets, and what are the data quality risks.

### Why do this before coding?
Skipping this step causes bugs that are invisible until integration. For example: if you don't know that Air Quality has 925 monitoring sites, you'd write a time-only join and silently get 925× too many rows.

### Primary Key — why does it matter?
A primary key uniquely identifies one row. If you have a PK, you can:
- Detect and remove duplicates reliably
- Safely update specific records
- Join to other tables without row explosion

Taxi Trips has **no primary key**. We tested a composite key (vendor + timestamps + locations) and it was not unique — meaning two trips can be identical on all those columns. This means we cannot deduplicate with confidence. The right response is to document this, not invent a fake key.

### Categorical vs Numeric — why does it matter?
`vendor_id = 2` is not the number two. It's a label meaning "this trip was recorded by Vendor 2." If you treat it as numeric, you might accidentally average vendor IDs, which is meaningless.

Columns that look numeric but are actually categories:
- `vendor_id`, `payment_type`, `rate_code_id` in Taxi Trips
- `state_code`, `county_code`, `site_num` in Air Quality
- `location_id` in Taxi Zones

These should be stored as `integer`, not `double`, and never aggregated with SUM or AVG.

### The 925 sites problem
Air Quality has measurements from 925 monitoring sites. Each site records PM2.5 once per hour. A join on timestamp alone would match one taxi trip to 925 air quality rows — one per site. You must pick one representative site (or nearest site) before joining. This is a spatial + temporal join problem, not just temporal.

### What the numbers told us
- Taxi Trips: 9.55M rows, Jan–Mar 2024 only, no PK
- Weather: exactly 8,784 rows = 366 × 24 (leap year, complete, no gaps)
- Air Quality: 8.14M rows, 925 sites, full year 2024
- Taxi Zones: 265 rows, all unique, one unknown zone (ID 265)

---

## Task 2: Storage Architecture

### What is the Medallion Architecture?
A layered storage pattern where each layer represents increasing data quality:

```
raw    → original files, never modified
bronze → loaded into Delta, minimal changes
silver → cleaned, typed, standardized
gold   → joined, integrated, ready for analysis
```

Why layers? Because transformations fail. If your silver job has a bug, you reprocess from bronze without touching raw. You always have a fallback. Each layer is independently queryable.

### What is a Delta Table?
Delta is a storage format built on top of Parquet files. Parquet stores data efficiently in columns. Delta adds a **transaction log** — a folder called `_delta_log` that records every write operation.

What this gives you:
- **ACID transactions** — writes are atomic. No partial files if a job crashes.
- **Schema enforcement** — Delta rejects data that doesn't match the table schema.
- **Time travel** — query the table as it was yesterday: `VERSION AS OF 0`
- **Efficient updates** — you can update/delete specific rows (plain Parquet can't do this)

### What is partitioning?
Partitioning physically splits a table into subfolders by a column value:

```
bronze/taxi_trips/
    year=2024/
        month=1/    ← all January files here
        month=2/    ← all February files here
        month=3/    ← all March files here
```

When a query filters `WHERE month = 1`, Spark only opens the `month=1` folder and skips the rest. This is called **partition pruning** and can reduce read time from minutes to seconds on large tables.

### Why not partition everything?
Because partitioning has a cost:
- Each partition = a separate set of Parquet files
- Spark must discover and open file metadata for every partition
- If partitions are tiny, the metadata overhead exceeds the scan savings

This is called the **small files problem**. Example: partitioning Taxi Zones (265 rows) by `location_id` creates 265 folders with roughly 1 row each. Reading this is slower than reading one file.

Rule of thumb: only partition when each partition will contain millions of rows and your queries routinely filter on that column.

### Broadcast joins — why small tables don't need partitioning
When Spark joins a huge table with a tiny table, it can **broadcast** the tiny table — send a full copy to every worker node. No data shuffling needed. This is much faster than a regular join.

Weather (8,784 rows) and Taxi Zones (265 rows) will always be broadcast joined. Partitioning them would actually slow this down by adding file discovery overhead.

### Z-ordering — what is it?
Z-ordering is a technique that physically co-locates related rows within a partition. For example, after partitioning Taxi Trips by month, you can Z-order by `pickup_location_id`. Rows with similar location IDs end up in the same Parquet files. A query filtering by location can now skip most files within the partition.

Z-ordering is not needed at current scale (9.55M rows) but becomes valuable at 20× scale.

### What changes at 20× scale?
At ~190M taxi trips:
- Monthly partitions become too large (each month = ~63M rows)
- Add `day` as a third partition level
- Add Z-ordering on location columns within partitions
- Schedule `OPTIMIZE` to compact small files from incremental writes
- `VACUUM` to delete old file versions and save storage

Weather and Taxi Zones never need changes — they don't grow with trip volume.

---

## Task 3: Generic Ingestion Framework

### What does "generic" mean in software?
Generic means one piece of code handles many cases. The opposite is writing four separate scripts — one per dataset. Generic code puts the differences in configuration, not in logic.

### Why is this better?
- Bug fix in the pipeline = fixed for all datasets at once
- New dataset = add one config entry, no new code
- Easier to test — one function to test, not four

### What is the pipeline doing step by step?

**Load** — reads the file. CSV gets header inference and schema inference. Parquet already has a schema embedded.

**Standardize columns** — renames source column names to snake_case. `VendorID` → `vendor_id`. This happens via an explicit rename map per dataset, so nothing is guessed.

**Normalize timestamps** — casts string date columns to Spark `timestamp` type. Without this, date comparisons fail silently.

**Add partition columns** — derives `year` and `month` from the pickup datetime. These columns don't exist in the raw data; we compute them so Delta can partition correctly.

**Validate** — checks two things:
1. Are primary key columns null? (Null PKs are invalid rows)
2. Are there duplicate primary keys? (Violates uniqueness constraint)
Invalid rows go to a `rejected/` Delta table with a `rejection_reason` column — they don't interrupt the pipeline.

**Apply rules** — dataset-specific business filters. Taxi Trips: distance > 0, fare >= 0, passengers > 0. These removed 1,073,908 rows (11.2%) — cancelled or invalid trips.

**Save as Delta** — writes with correct partitioning per dataset config.

**Log metadata** — appends one row to `metadata/ingestion_log` Delta table. Records counts, timing, schema version. This is your pipeline health record.

### Configuration-driven design
All dataset differences live in one dictionary in `config.py`:

```python
DATASETS = {
    "taxi_trips": {
        "path": "data/raw/taxi_trips/",
        "format": "parquet",
        "primary_key": None,
        "partition_by": ["year", "month"],
        "rules": _rules_taxi_trips,
    }
}
```

The pipeline reads this and behaves accordingly. Adding dataset #5 = adding one dictionary entry. Zero pipeline changes.

### What did we actually find?
- Taxi Trips lost 11.2% of rows to business rules — expected for real-world taxi data
- All other datasets passed validation cleanly
- `uncertainty` in Air Quality was 100% null — caught by inspection, addressed in Task 4
- `time_local` in Air Quality was incorrectly typed — caught by inspection, fixed in Task 4

---

## Task 4: Common Data Model

### Why do we need a common data model?
The four datasets were built by different teams with no shared standard. Before joining them, they must agree on:
- What a timestamp looks like
- What null means
- What type a column should be

Without this, joins silently produce wrong results or fail entirely.

### Bronze vs Silver — what's the difference?
- **Bronze** = raw data loaded into Delta with column renames. Minimal changes. Preserves the original as closely as possible.
- **Silver** = bronze with the common data model applied. Correct types, nulls handled, useless columns removed. Safe to join.

Silver never removes rows — only columns are dropped or filled. If a row is wrong, it should have been caught in bronze validation.

### Three types of nulls
We learned to classify nulls before deciding what to do with them:

**Structural nulls** — the column is 100% null. It contains no information at all. Drop it.
- `snwd` (snow depth): 100% null — no snow data was collected
- `wpgt` (wind gust): 100% null — same
- `uncertainty` in Air Quality: 100% null

**Measurement nulls** — the null means "nothing was recorded" which has a real-world value of zero.
- `precipitation`: null means no rain → fill with `0.0`

**Unknown nulls** — the null means "we don't know." Don't fill with zero (that implies a known value). Use a sentinel.
- `condition_code`: 6 missing values → fill with `-1` meaning "unknown"

**Meaningful nulls** — null itself carries information. Leave as null.
- `qualifier` in Air Quality: 92% null. When present, it flags a measurement issue. Null = no flag = normal reading.

### Why cast `long` to `integer` for categorical IDs?
Parquet infers integer types as `long` (64-bit) by default. `vendor_id`, `payment_type`, and `passenger_count` don't need 64-bit range — they have a handful of distinct values. Casting to `integer` signals to anyone reading the schema that these are category labels, not large measurements. It also saves storage space.

### The broken timestamp fix
Air Quality's `time_local` column contains strings like `"14:30"` — time only, no date. When we tried to cast this to `timestamp`, Spark produced nulls or wrong values. The fix: drop `time_local` and use `date_local` (which has the date) for joins. Since we're doing hourly joins, the hour is extracted from `date_local` anyway.

---

## General Concepts Reference

**Delta Table** — Parquet files + transaction log. ACID, schema enforcement, time travel, efficient updates.

**Partition pruning** — Spark skips partition folders that don't match the query filter. Only works if the query filters on the partition column.

**Broadcast join** — Spark sends a full copy of a small table to every worker. No shuffle needed. Automatic for tables under the broadcast threshold (~10MB by default).

**Small files problem** — Too many tiny Parquet files slow both reads and writes due to file metadata overhead. Caused by over-partitioning or many small incremental writes.

**Schema enforcement** — Delta rejects writes whose schema doesn't match the table definition. Catches bugs at write time, not at query time.

**Medallion architecture** — raw → bronze → silver → gold. Each layer adds trust. Earlier layers are never modified when processing later layers.

**Configuration-driven design** — pipeline behaviour comes from data (config dictionaries), not from code. New datasets require config changes only, not code changes.

**Sentinel value** — a special value used to represent "unknown" or "not applicable." `-1` for an unknown condition code is a sentinel. Better than leaving null when downstream code can't handle nulls, but use carefully — sentinels must be documented or they become invisible bugs.

**Composite key** — a primary key made of multiple columns combined. `(year, month, day, hour)` in Weather is a composite key — no single column is unique, but the combination is.

---

## Task 5: Integration Pipeline

### What is the goal?
Build one wide table — `gold/integrated_taxi_trips` — where each row is a taxi trip enriched with weather, air quality, pickup zone, and dropoff zone. This is the analytical dataset that all future queries run against.

### What is a join?
A join combines two tables by matching rows on a common column. For example, matching `pickup_location_id` in trips to `location_id` in taxi zones gives you the borough name for each trip's pickup location.

### Why left joins everywhere?
A left join keeps all rows from the left table (trips) even if no match is found in the right table. An inner join would drop trips that have no matching weather row. Since we never want to lose valid trips just because contextual data is missing, left joins are always the right choice here.

### Why broadcast joins?
When joining a huge table (8.5M trips) with a tiny table (265 zones, 8,784 weather rows), Spark normally shuffles data across the network — expensive. A broadcast join instead sends a full copy of the small table to every worker node. No shuffling needed. Much faster.

Spark does this automatically when a table is under the broadcast threshold. Weather, air quality (after aggregation), and zones are all small enough.

### The DST bug
Daylight saving time on March 31 2024 caused clocks to jump from 2:00am to 3:00am. The weather dataset recorded two observations at 3:00am — one before and one after the clock change. When we built `weather_ts` from year/month/day/hour integers, both rows got `2024-03-31 03:00:00`. Joining trips to this would multiply any trip at that hour by 2.

Fix: deduplicate weather on `weather_ts` before joining, keeping one row per hour.

### Why is AQ daily instead of hourly?
We dropped `time_local` in Task 4 because it was a `HH:mm` string that couldn't be cast to a timestamp. Without it, `date_local` has date only — no hour. So we can't match a trip at 14:00 to the 14:00 air quality reading.

Solution: average all 24 hourly readings for the day into one `pm25_daily_avg` value, then join on date. Every trip on January 15 gets the same PM2.5 value. It's less precise but correct — no row multiplication.

**Lesson:** decisions in earlier tasks have consequences downstream. Dropping `time_local` in Task 4 was the right call (it was broken), but we should document the downstream impact.

### Why one fixed AQ site?
Air quality has 925 monitoring sites. Joining on time alone would match each trip to 925 rows — 925× row explosion. We need to pick one site per trip.

The correct approach is nearest-site selection: compute the distance from each trip's pickup coordinates to all 925 sites and pick the closest. This is expensive (8.5M × 925 distance calculations).

The practical approach: pick one representative NYC site (Queens 124) and use it for all trips. Faster, simpler, honest about the limitation.

### What the gold table looks like
Each row has everything: trip details + weather at pickup time + daily PM2.5 + pickup zone/borough + dropoff zone/borough. This is the table analysts query. They never need to join anything themselves.

### Key numbers
- Input: 8,480,870 silver trips
- Output: 8,480,870 gold rows (no trips lost)
- Weather: 100% matched
- AQ: 100% matched (Jan–Mar fully covered by Queens site 124)

---

## Task 6: Benchmarking

### What are we measuring and why?
We compare two storage strategies for the same dataset to understand how storage decisions affect real performance. The four metrics — ingestion time, storage size, file count, query latency — together tell the full story.

### The two strategies
- **Strategy A:** partitioned by `year, month` — our production design
- **Strategy B:** flat, no partitioning

### What the results mean

**Ingestion time (23.6s vs 8.2s):** Partitioning is 2.9× slower to write. Spark must shuffle all rows by partition key before writing, then write each partition separately. This is the real cost of partitioning — paid at write time, not read time.

**Storage size (173.6 MB vs 173.7 MB):** Identical. Partitioning reorganizes data into folders but doesn't change how it's compressed. Same bytes, different folder structure.

**File count (15 vs 9):** More files = more metadata overhead. With 3 month-partitions and 8 Spark workers, each month gets up to 5 files. Reading 15 files requires 15 metadata lookups vs 9.

**Query latency (Strategy B wins all three):** This is the surprising result. Partitioning was supposed to make queries faster — but it didn't. Why?

### Why didn't partitioning help queries?
Partition pruning — Spark skipping folders that don't match the query filter — only works when the query actually filters on the partition column. All three benchmark queries scan the full dataset:
- Trips per borough → no time filter
- Average duration per day → groups by date, not filtered by month
- Average fare per borough → no time filter

So Strategy A reads all 15 files and Strategy B reads all 9 files. Strategy B wins because fewer files = less overhead.

### When would Strategy A win?
Any query with a time filter:
```sql
WHERE month = 1          -- reads 5 files instead of 9
WHERE year = 2024 AND month = 2  -- reads February only
```

At 12 months and 190M rows, the difference would be dramatic. At 3 months and 8.5M rows, the benefit is invisible and the overhead is real.

### The key lesson
Partitioning is an investment in future query performance, not a guaranteed speedup today. It only pays off when:
1. The dataset is large enough that skipping partitions saves significant time
2. Queries actually filter on the partition column
3. Each partition is large enough to justify the file overhead

At Q1 2024 scale with full-scan queries, flat storage wins. Our production design (partitioned) is still correct because the dataset will grow to cover all 12 months and queries will filter by time — but this benchmark shows why you should always measure rather than assume.

### Why disable AQE for benchmarking?
Adaptive Query Execution (AQE) lets Spark re-optimize queries at runtime based on actual data statistics. For a fair comparison of storage strategies, we disable it — otherwise Spark's runtime optimizer might mask the differences we're trying to measure.