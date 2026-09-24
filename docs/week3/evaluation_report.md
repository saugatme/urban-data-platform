# Week 3 Evaluation Report

Two kinds of evidence are reported here, and they were gathered differently.

**Correctness** was verified on a synthetic fixture, because the course datasets are not present on the development machine. The fixture is small by design — it has to be small for the expected counts to be stated exactly and checked.

**Performance** cannot be measured on a fixture. The five overheads below must be measured against the full dataset with `run_week3_evaluation.py`, and those tables are marked pending rather than filled with numbers from a 20-row table, which would be meaningless.

---

## Part 1 — Correctness (verified)

### Method

A temporary working directory is populated with a miniature platform:

| Table | Contents |
|---|---|
| `bronze/taxi_trips` | 20 trips, zones 1–3, June 2024 |
| `bronze/weather` | 12 hourly rows |
| `bronze/air_quality` | 12 hourly rows |
| `bronze/taxi_zones`, `silver/taxi_zones` | 3 zones (ids 1, 2, 3) |

The generated update deliberately mixes four cases so each assertion isolates one behaviour:

| Case | Rows | Expected outcome |
|---|---:|---|
| Genuinely new trips | 5 | inserted |
| Verbatim copies of stored rows | 3 | ignored as duplicates |
| Trip with `pickup_location_id = 999` | 1 | rejected, reason `missing_reference_taxi_zone_ids` |
| Weather / air-quality rows with `humidity` / `aqi` | 6 + 6 | inserted, columns accepted as documented evolution |

`update_all()` is then run **twice** against the same update, and the second run's insert counts are what prove idempotency.

### Results

All thirteen checks passed during development. The same behaviour is verified against the real data by running the pipeline twice:

```bash
python run_incremental_update.py    # first run: inserts the new release
python run_incremental_update.py    # second run: must insert 0 rows
```

**Merge counts**

| Run | Dataset | Processed | Inserted | Duplicates ignored | Rejected |
|---|---|---:|---:|---:|---:|
| 1 | `taxi_trips` | 8 | **5** | **3** | **1** |
| 1 | `weather` | 6 | 6 | 0 | 0 |
| 1 | `air_quality` | 6 | 6 | 0 | 0 |
| 2 | `taxi_trips` | 8 | **0** | 8 | 1 |
| 2 | `weather` | 6 | **0** | 6 | 0 |
| 2 | `air_quality` | 6 | **0** | 6 | 0 |

Run 1 processes 8 taxi rows rather than 9 because the unknown-zone trip is rejected before the merge. Run 2 re-processes the same 8 and inserts none of them — every row is recognised as already stored. Bronze finishes with 25 trips (20 + 5), not 28.

**Assertions**

| Check | Expected | Result |
|---|---|---|
| Run 1 inserted the new trips | 5 | ✓ 5 |
| Run 1 ignored the verbatim duplicates | 3 | ✓ 3 |
| Run 1 rejected the unknown-zone trip | 1, with specific reason | ✓ `{'missing_reference_taxi_zone_ids': 1}` |
| Run 2 inserted rows (taxi_trips) | 0 | ✓ 0 |
| Run 2 inserted rows (weather) | 0 | ✓ 0 |
| Run 2 inserted rows (air_quality) | 0 | ✓ 0 |
| `humidity` present in bronze weather | yes | ✓ |
| `aqi` present in bronze air_quality | yes | ✓ |
| `humidity` reported as `evolved`, not `unexpected` | yes | ✓ `{'unexpected': [], 'missing': [], 'evolved': ['humidity']}` |
| No unexpected drift on any dataset | none | ✓ all empty |
| Monitoring log populated | > 0 rows | ✓ 6 rows |
| All four monitoring queries execute | yes | ✓ |
| Final bronze trip count | 25 | ✓ 25 |

### A defect this found

The first fixture run failed with `DELTA_MERGE_UNRESOLVED_EXPRESSION` on air quality. The cause was the fixture, not the pipeline — Bronze retains `time_local` (only the Silver transform drops it) and it is part of the air-quality primary key, so a fixture omitting it was unrepresentative.

The error Delta produced named neither the offending column's side nor the dataset clearly, so `merge_keyed()` now checks both the stored table and the update for every key column up front and raises a message naming which side is missing what. The fixture was corrected to carry `time_local`.

### Why these specific checks

Each maps to a Week 3 requirement that could plausibly fail silently:

- **Duplicates ignored** — an append-only pipeline would insert them and nothing would complain.
- **Second run inserts 0** — the single strongest signal that duplicate handling is real rather than incidental, because it exercises the merge against data it has already seen.
- **Rejected with a specific reason** — the brief explicitly requires that a bad reference is neither silently dropped nor silently kept. Checking the *reason*, not just the count, is what distinguishes the two.
- **`evolved` rather than `unexpected`** — proves the schema contract is consulted, rather than all new columns simply being waved through by `mergeSchema`.

---

## Part 2 — Performance (pending full-dataset run)

Produced by:

```bash
python run_week3_evaluation.py
```

which writes `data/benchmark/week3_evaluation.json`. Run it **after** `run_incremental_update.py`, since the update and refresh timings are read back from the logs those pipelines wrote rather than re-running expensive merges.

### 1. Incremental update time

Per-dataset Bronze merge time, from `data/monitoring/pipeline_log`.

| Dataset | Processed | Inserted | Rejected | Avg seconds |
|---|---:|---:|---:|---:|
| `taxi_trips` | _pending_ | _pending_ | _pending_ | _pending_ |
| `weather` | _pending_ | _pending_ | _pending_ | _pending_ |
| `air_quality` | _pending_ | _pending_ | _pending_ | _pending_ |

The comparison worth drawing is against the Week 1 full ingestion time for the same dataset: the merge processes roughly 7% of the data, so anything close to the full-ingest time indicates the hash join, not the write, dominates.

### 2. Analytical refresh time

Per-product, from `data/gold/data_products/_product_log`.

| Product | Mode | Action taken | Rows written | Seconds |
|---|---|---|---:|---:|
| `daily_mobility` | incremental | _pending_ | _pending_ | _pending_ |
| `taxi_zone_statistics` | full | _pending_ | _pending_ | _pending_ |
| `weather_impact` | full | _pending_ | _pending_ | _pending_ |
| `air_quality_impact` | full | _pending_ | _pending_ | _pending_ |

Expect `daily_mobility` to log `full` on the **first** Week 3 refresh and `incremental` thereafter, because Week 2 wrote it unpartitioned and the first run has no watermark. That transition is itself the evidence that the fallback works as designed.

A second run with no new data should log `skipped` for all four.

### 3. Storage overhead

| Item | Size | % of Gold |
|---|---:|---:|
| Gold integrated trips | _pending_ | — |
| Data products | _pending_ | _pending_ |
| Monitoring log | _pending_ | _pending_ |
| Rejected tables | _pending_ | — |
| Incoming updates | _pending_ | — |

Week 2 measured the products at 1.87 MB against a 1.19 GB Gold table (0.15%). The monitoring log should be far smaller again — one row per dataset per run.

### 4. Validation overhead

Measured directly: each update is prepared twice, once with the dataset's rules and once with the rule list temporarily emptied, through the identical code path.

| Dataset | Rules | With rules | Without | Overhead |
|---|---|---:|---:|---:|
| `taxi_trips` | `incomplete_record`, `missing_reference_taxi_zone_ids` | _pending_ | _pending_ | _pending_ |
| `weather` | `humidity_out_of_range`, `temp_out_of_range` | _pending_ | _pending_ | _pending_ |
| `air_quality` | `aqi_out_of_range`, `sample_measurement_out_of_range` | _pending_ | _pending_ | _pending_ |

`taxi_trips` is the one to watch: it carries the only referential rule, and its `isin` predicate against 265 collected zone ids is the most expensive check in the framework. The range rules are single-column predicates and should be close to free.

### 5. Monitoring overhead

Measured as the wall-clock cost of a single `record_run()` call, averaged over five samples.

| Metric | Value |
|---|---:|
| Average per write | _pending_ |
| Writes per full pipeline run | 7 (3 merges + 2 silver + 1 gold + 1 product refresh) |
| Estimated total per run | _pending_ |

Probe rows are tagged `pipeline = 'evaluation_probe'` so they can be filtered out of the monitoring queries afterwards.

A single-row Delta append is dominated by transaction-log overhead rather than data volume, so this cost is effectively constant per call and independent of dataset size. If it proves significant relative to a fast pipeline stage, the fix is batching the run records rather than writing one per dataset.

---

## Interpretation to complete after the run

The evaluation should end by answering three questions with the numbers above:

1. **Is incremental ingestion actually cheaper than re-ingesting?** Compare the merge time against the Week 1 ingestion time for the same dataset. If the hash join over 8.48M stored rows costs more than a rebuild, the strategy is wrong for taxi trips at this scale and a surrogate key written at ingest time would be the better design.
2. **Did selective refresh pay for itself?** Compare `daily_mobility`'s incremental refresh against its full rebuild, and weigh that saving against the three products that must rebuild fully regardless.
3. **Are validation and monitoring cheap enough to leave on?** Both are permanent costs on every future run, so the answer determines whether the framework is production-viable or a demonstration.
