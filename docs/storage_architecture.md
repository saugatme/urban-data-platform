# Storage Architecture Design

## 1. Layered lake layout

```javascript
data/lake/
├── bronze/            # typed, raw-ish; exactly what the file said, + provenance
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
├── silver/            # cleaned, common-data-model compliant
├── gold/              # integrated / query-optimized outputs
│   └── integrated_taxi_trips/
└── quarantine/        # rejected rows + _quarantine_reason
└── <dataset>
```

Why Delta at every layer: ACID (a bad run never corrupts a table), schema
enforcement on write (DQ gate), time travel (audit + reprocessing), and
`MERGE` for idempotent re-ingestion.

## 2. Lookup (dimension) tables

`taxi_zones` is the model lookup table: **265 rows, static, no temporal
attribute**. Handling rules:

- Stored once at `silver/taxi_zones/`; **never partitioned** — it is smaller
  than one partition's worth of metadata overhead and is meant to be
  broadcast-joined.
- Re-ingestion is idempotent via `MERGE` on `location_id`.
- Zone *names* never appear in fact tables — facts keep `pu_location_id` /
  `do_location_id`; the borough dimension is resolved at query time or
  materialized in gold. This avoids duplicating free-text `Zone` (up to
  265 distinct values) across ~10M-row fact tables.
- Known DQ issue: LocationID 265 has null Borough/Zone → integration must
  left-join so those trips survive with null geography.

The same pattern applies to future small reference data (payment-type codes,
rate codes).

## 3. Partitioning the fact tables

**Guiding principle: partition by what you filter, not by what you store.**

### taxi_trips (~3.2M rows/month)
- **Partition by `pickup_date`** (derived, `date(pickup_ts)`), NOT by
  `pu_location_id`.
- Justification from the assignment queries:
  - "trips per borough" → after the zone join, a borough filter is
    `location_id IN (list)` — partition pruning on 265 location IDs is
    ineffective (typical borough = 10–40 of 265 values → reads most files).
  - "avg trip duration per day" → date range filter → `pickup_date`
    pruning is perfect.
  - "avg fare per borough" → same as #1.
- Daily partitions for 3 months ≈ 91 partitions; at ~35k rows/partition this
  is healthy. (Benchmark in Task 6 compares daily vs. monthly.)

### air_quality (8.1M rows, 925 sites)
- **Partition by `date_local`**; do NOT partition by site — 925 site values ×
  365 days would create ~340k tiny partitions (file-count explosion).
- Site selection is a metadata filter (NY county) applied *before* the join,
  not a partition key.

### weather (8,784 rows)
- **Not partitioned at all.** One year of hourly data is a single
  ~1 MB partition; partitioning adds a directory layer for zero pruning
  benefit at this size.

## 4. When partitioning is harmful (general rules)

1. **Partition column cardinality too high** (> a few thousand values):
   metadata overhead and small-file problem dominate (e.g., site ID in
   air_quality, a UUID, or a timestamp finer than day/hour).
2. **Partition column skewed**: 99% of rows in one partition → no pruning,
   worse than unpartitioned for the hot partition.
3. **Table smaller than ~1 GB**: pruning gain < metadata cost.
4. **Partition column updated by MERGE**: writes fan out across many
   directories; prefer ZORDER + Liquid clustering instead (Delta 3.x).
5. **Low-selectivity filter column** (like borough with 7 values, 2 of which
   cover 80% of data): ZORDER on that column beats partitioning.

## 5. 20× scale projection (≈200M trips / 60 months)

- **Ingestion**: monthly Parquet files still fine; use `maxRecordsPerFile` /
  `repartition(pickup_date)` on write to keep ~128–512 MB files per
  partition directory. Delta `OPTIMIZE ... ZORDER BY (pu_location_id)` after
  load, so borough queries skip data *within* retained files.
- **Query latency**: daily partitioning at 20× = ~1,800 partitions — still
  OK for Delta, but if file count degrades, switch to **monthly partitions
  + ZORDER on pickup location** (the Task 6 benchmark gives us the measured
  trade-off to cite).
- **air_quality at 20×**: ~160M rows → still partition by date; consider
  Liquid clustering (Delta 3.2) on (date, site) instead of Hive-style
  partitioning.
- **Metadata**: Delta transaction log per commit grows with file count, not
  row count — file-count discipline (target-size writes, periodic OPTIMIZE)
  matters more than row count at this scale.
- **Integration**: hourly as-of join against weather/air_quality stays
  cheap because those are small; the shuffle is bounded by the trip side,
  which ZORDER + partitioning keep prunable.

## 6. What the Task 6 benchmark must confirm

| Question | Benchmark evidence |
|---|---|
| Is daily or monthly partitioning better? | Query latency + file count, both schemes |
| Does borough query suffer from date partitioning? | "trips per borough" latency vs. ZORDER variant |
| Cost of over-partitioning demo | file count, storage size per scheme |