# Task 1 – Incremental Updates

The course supplies one data release. Week 3 needs a second, so `scripts/generate_incremental_updates.py` synthesises one per dataset and `src/ingestion/incremental.py` folds it into the existing Bronze tables without rebuilding them.

---

## Generating the second release

Values are not invented from scratch. The generator samples existing rows and perturbs them, which preserves each dataset's real distributions — a fabricated trip with a plausible fare, zone and duration exercises the pipeline far more honestly than a random one.

```bash
python scripts/generate_incremental_updates.py
```

Output goes to `data/incoming/`, kept separate from the original release in `data/raw/`. Files are written with **raw** column names, so the update passes through exactly the same standardisation as the first release rather than taking a shortcut into the cleaned schema.

| Dataset | Format | Contents | Schema change |
|---|---|---|---|
| `taxi_trips` | Parquet | ~7% new trips timestamped after the latest existing trip, plus ~1.5% rows copied verbatim | none |
| `weather` | CSV | 168 new hourly rows continuing the series | **`humidity`** added (20–100) |
| `air_quality` | CSV | 168 new hourly rows continuing the series | **`aqi`** added (0–500) |
| `taxi_zones` | — | no update; a fixed reference table | none |

New taxi trips are re-stamped to start after the latest existing pickup and spread across the following week, with trip distance jittered by ±10%. Trip duration is preserved from the sampled row. The duplicate rows are copied with no modification at all, which is what makes them a real test of duplicate detection.

The generator writes `data/incoming/generation_summary.json` with the exact counts it produced, so the numbers below come from a run rather than from the configured percentages.

### Generated counts

> Populate from `data/incoming/generation_summary.json` after running the generator against the full dataset. The table below is the shape that file produces.

| Dataset | Existing rows | New rows | Duplicate rows | Total in update | Schema changes |
|---|---:|---:|---:|---:|---|
| `taxi_trips` | 8,480,836 | _pending_ | _pending_ | _pending_ | none |
| `weather` | _pending_ | 168 | 0 | 168 | `humidity` |
| `air_quality` | _pending_ | 168 | 0 | 168 | `aqi` |

---

## The merge

```bash
python run_incremental_update.py
```

Two strategies, chosen by whether the dataset has a usable primary key.

### Keyed datasets — weather, air quality, taxi zones

A Delta `MERGE` with a single insert-only clause:

```python
target.alias("target")
      .merge(updates.alias("source"), condition)
      .whenNotMatchedInsertAll()
      .execute()
```

There is deliberately **no `whenMatched` clause**. A row already present is left exactly as it is, which satisfies two requirements at once: unchanged records are preserved, and re-running the pipeline inserts nothing.

### Taxi trips — no primary key

The Week 1 data catalogue documents that taxi trips have no reliable key: two genuine trips can share a vendor, timestamp, zone pair and fare. So duplicates are identified by content instead.

A SHA-256 hash is computed over the columns common to both the update and the stored table, with column names sorted so the hash does not depend on column order, and nulls given an explicit marker so they cannot collide with an empty string. Rows whose hash already exists are dropped with a left-anti join, and the remainder is appended.

This detects exactly what the brief asks for — rows byte-identical to ones already stored — while leaving genuinely distinct trips that happen to look similar untouched.

---

## Schema evolution

`humidity` and `aqi` are **documented** additions, declared per dataset in `src/ingestion/config.py`:

```python
"schema_contract": SchemaContract(allowed_new=("humidity",)),
```

Delta accepts them because `run_incremental_update.py` sets `spark.databricks.delta.schema.autoMerge.enabled` for merges, and the taxi-trips append carries `mergeSchema`. Existing rows get `NULL` for the new column, which is correct: those observations genuinely have no humidity reading.

Every update is also checked against the columns already stored, and the result is split three ways — `evolved` (documented), `unexpected` (undocumented), `missing` (dropped by the update). Unexpected drift is recorded and reported, never silently accepted, but it does not stop the run. See [Task 4](t4_validation_framework.md).

---

## Idempotency

The definition of done for this task is that running the pipeline twice inserts nothing the second time. Both strategies are built for it: the keyed merge has no `whenMatched` clause, and the hash join filters against what is already stored.

This was verified on a synthetic fixture, since the full dataset was not available on the development machine. Against a 20-row Bronze table and an update of 5 new trips, 3 verbatim duplicates and 1 trip with an unknown zone id:

- run 1 inserted the 5 new trips, ignored the 3 duplicates, rejected the bad-zone trip
- run 2 inserted 0 rows for every dataset
- `humidity` and `aqi` appeared in Bronze and were reported as `evolved`, with no unexpected drift

Full results are in [the evaluation report](evaluation_report.md).

---

## Order of operations

`run_incremental_update.py` runs Bronze merge → Silver rebuild → Gold rebuild → product refresh.

Silver and Gold are rebuilt rather than merged, because both have whole-table semantics: `enforce_common_model()` drops columns that are null across every row, and the Gold join enriches trips against hourly weather that a new release can change. Rebuilding them is a deliberate correctness trade-off, discussed in [Task 2](t2_analytical_consistency.md).
