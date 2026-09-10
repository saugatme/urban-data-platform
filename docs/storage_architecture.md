# Task 2: Storage Architecture Design

## Directory Structure

```
data/
├── raw/                  # Original files, never modified
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
├── bronze/               # Loaded into Delta, minimal changes
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
├── silver/               # Cleaned, typed, standardized
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
└── gold/                 # Integrated, query-ready
    └── integrated_taxi_trips/
```

Each layer adds trust. Raw is immutable. Bronze is raw-as-Delta. Silver enforces the common data model. Gold is the analytical output.

---

## Naming Conventions

- Snake_case for all table and column names: `taxi_trips`, `pickup_location_id`
- Folder name = table name; no redundant prefixes within a layer
- Gold tables named by analytical purpose: `integrated_taxi_trips`

---

## Partitioning Strategy

| Dataset | Partition Key | Rationale |
|---|---|---|
| Taxi Trips | `year`, `month` | 9.55M rows, grows continuously, queries filter by time period |
| Air Quality | `year`, `month` | 8.14M rows, same query pattern |
| Weather | None | 8,784 rows — partitioning adds overhead with no benefit |
| Taxi Zone Lookup | None | 265 rows — lookup table, must never be partitioned |
| Integrated Trips (gold) | `year`, `month` | Inherits taxi trip volume and query patterns |

---

## Design Decisions

### Which datasets are lookup tables?
**Taxi Zone Lookup** (265 rows) and **Weather** (8,784 rows) are lookup tables. Both are small, static, and always read in full during joins. Spark will broadcast them automatically — no partitioning needed or beneficial.

### Which datasets should not be partitioned?
Weather and Taxi Zone Lookup. Partitioning creates one subfolder per partition value. At this scale, the file metadata overhead exceeds any scan savings. Partitioning 265 rows would produce ~265 near-empty files.

### Which datasets require different partitioning strategies?
Taxi Trips and Air Quality are partitioned by `(year, month)` — large volumes with time-based query patterns. Weather and Taxi Zones are unpartitioned — small reference tables always read in full.

### When does partitioning become harmful?
- Table is too small (overhead > savings)
- Partition key has too many distinct values → too many tiny files ("small files problem")
- Queries never filter on the partition column → full scan regardless
- Rule of thumb: only partition when each partition will exceed ~100MB

### If data volume increased 20×

| Change | Reason |
|---|---|
| Add `day` partition level to Taxi Trips and Air Quality | Monthly partitions become too large to scan efficiently |
| Z-order on `pickup_location_id` within partitions | Data skipping for location queries without over-partitioning |
| Z-order on `(state_code, county_code, site_num)` for Air Quality | Spatial filtering within time partitions |
| `OPTIMIZE` + `VACUUM` scheduled regularly | Compact small files from incremental writes |
| Cold partitions → cheaper object storage | Cost efficiency for rarely queried historical data |
| Weather and Taxi Zones remain unpartitioned | They do not grow with trip volume |