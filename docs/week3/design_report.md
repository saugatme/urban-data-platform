# Urban Data Integration Platform — Week 3 Design Report

**Module:** Data-intensive Computing
**Submission:** Week 3 — Operating and maintaining the platform
**Project:** Urban Data Integration Platform

## 1. Purpose

Weeks 1 and 2 built a platform that runs once: ingest a release, integrate it, query it, materialise products from it. Week 3 addresses what happens on the second release. Four capabilities are needed, and each has a failure mode that a one-shot pipeline never has to consider:

1. **Incremental ingestion** — fold new data into existing tables without rebuilding them, without re-inserting what is already there, and while accepting documented new columns.
2. **Analytical consistency** — bring the Week 2 products back into line afterwards, recomputing as little as correctness allows.
3. **Monitoring** — make every run report on itself, so operational questions can be answered from data rather than from memory.
4. **Extensible validation** — catch a wider class of bad records, without stopping the pipeline, and without editing the core framework each time a rule is added.

The design principle carried over from Week 1 is unchanged: dataset-specific facts live in configuration dictionaries, and the pipeline logic that consumes them stays generic. Week 3 extends `DATASETS` in `src/ingestion/config.py` with three new keys rather than introducing a parallel configuration system.

## 2. Simulating a second release

Only one release exists, so `scripts/generate_incremental_updates.py` synthesises a second. It samples existing rows and perturbs them rather than generating values from scratch, which preserves each dataset's real distributions — a plausible trip exercises the pipeline far more honestly than a random one. Files are written to `data/incoming/` with **raw** column names, so the update passes through the same standardisation as the first release instead of taking a shortcut into the cleaned schema.

| Dataset | Format | Contents | Schema change |
|---|---|---|---|
| `taxi_trips` | Parquet | ~7% new trips after the latest existing pickup, plus ~1.5% verbatim copies | none |
| `weather` | CSV | 168 new hourly rows | `humidity` (20–100) |
| `air_quality` | CSV | 168 new hourly rows | `aqi` (0–500) |
| `taxi_zones` | — | no update; fixed reference table | none |

The duplicates are copied with no modification at all, which is what makes them a genuine test rather than a near-miss. Exact counts are written to `data/incoming/generation_summary.json` so the documentation cites a run rather than the configured percentages.

## 3. Incremental ingestion

Two merge strategies, chosen by whether the dataset has a usable key.

**Keyed datasets** (weather, air quality, taxi zones) use a Delta `MERGE` with a single insert-only clause and deliberately **no `whenMatched` clause**. A row already present is left exactly as it is. This satisfies two requirements with one decision: unchanged records are preserved, and a repeated run inserts nothing.

**Taxi trips** have no reliable key — the Week 1 data catalogue documents that two genuine trips can share vendor, timestamp, zone pair and fare. Duplicates are therefore identified by content: a SHA-256 hash over the columns common to both sides, with column names sorted so the hash is order-independent and nulls given an explicit marker so they cannot collide with an empty string. Rows whose hash already exists are removed by a left-anti join; the rest is appended.

This detects precisely what the brief asks for — rows byte-identical to stored ones — while leaving genuinely distinct trips that merely look similar untouched.

**Schema evolution.** `humidity` and `aqi` are declared per dataset as `SchemaContract(allowed_new=(...))`. Delta accepts them because the merge enables `spark.databricks.delta.schema.autoMerge.enabled` and the append carries `mergeSchema`. Existing rows receive `NULL`, which is correct — those observations genuinely have no reading.

**Idempotency** is the definition of done for this task, and both strategies are built for it rather than achieving it incidentally.

## 4. Keeping the analytical products correct

The refresh mode is a property of the product's grain, declared in `PRODUCTS` in `src/analytics/data_products.py`.

| Product | Grain | Mode |
|---|---|---|
| `daily_mobility` | zone × day | incremental |
| `taxi_zone_statistics` | zone, whole period | full |
| `weather_impact` | temp bucket × condition, whole period | full |
| `air_quality_impact` | PM2.5 category, whole period | full |

The general rule: **a product can refresh incrementally when its grain includes the column the new data is bounded by.** Whole-period aggregates never can, because one new trip changes every row. Only `daily_mobility` is keyed by time, and only it is refreshed partially — the affected `(year, month)` partitions are recomputed and swapped in with Delta `replaceWhere`, leaving untouched months unread and unrewritten.

Work is skipped at two levels. A watermark per product, held in `data/gold/data_products/_product_log`, records the source maximum `pickup_datetime` each refresh brought the product up to. If the source watermark has not moved, the product is skipped entirely. Within an incremental product, only affected months are recomputed. The log records both the configured `refresh_mode` and the `action` actually taken, so a fallback from incremental to full is visible rather than silent.

**Schema evolution is automatic for consumers.** None of the four products selects `*`, and none references `humidity` or `aqi`, so a widened Gold schema passes them by. The Week 2 queries are likewise unaffected. *Using* a new column is a deliberate edit, which is the correct default: a column appearing in a feed is not a reason to change what a published product means.

**A known limitation, stated plainly.** Bronze is merged incrementally, but Silver and Gold are rebuilt in full. Both have whole-table semantics — `enforce_common_model()` drops columns null across every row, and the Gold join enriches trips against hourly weather a new release can change — so a partial rebuild risks inconsistency with Bronze. The saving is taken where it is both safe and largest: the Bronze merge and the product refresh. Making Gold incremental is the clearest next step, and requires proving no late-arriving weather row can affect an older partition.

## 5. Monitoring

Every pipeline writes one row per dataset per run to `data/monitoring/pipeline_log` via `record_run()`. Week 1's `ingestion_log` is retained as the record of what was ingested; the new log is the cross-pipeline operational view.

Two design points carry the weight. First, `validation_failures` is stored as a **JSON map of reason to count** rather than as columns, so adding a validation rule needs no schema migration — this is what keeps the validation framework genuinely pluggable. A `validation_failure_count` is stored alongside so the common aggregation never parses JSON. Second, the schema is **declared, not inferred**, so appends from four different pipelines stay compatible.

The `monitored()` context manager records a row with `status = "failed"` when a pipeline raises, then re-raises. Without it a crashed run would be *absent* from the log, and absence is indistinguishable from "never ran" — a monitoring system that records only successes cannot answer what broke.

Four questions are implemented as named SQL constants in `src/monitoring/queries.py`: which dataset fails validation most, which takes longest, what was rejected per run and why, and how processing time trends. The first reports a percentage as well as a count, because a dataset with ten times the volume will naturally reject more without being worse. The last partitions its `LAG` window by dataset so a trend never compares one dataset against another.

## 6. Extensible validation

Week 1 validated with one hardcoded condition per dataset. Week 3 adds a rule list, so a new check is data rather than a code change to the validation loop.

A `ValidationRule` pairs a rejection reason with a condition marking rows to reject, plus the reference data it needs. `apply_rules()` is the generic loop and does not change when a rule is added. `build_context()` loads only the references the active rules ask for, so a dataset with no referential rules costs nothing extra. Rules are applied in order and each row is attributed to the first rule it fails, so no row is counted twice.

The five required categories map onto the framework as follows. Duplicates are handled by the keyed `validate()` for keyed datasets and by the content hash for taxi trips. Invalid values use the Week 1 condition plus `value_out_of_range` rules. Missing reference records use `missing_reference` against the 265-row zone lookup, collected once into the context and applied as an `isin` predicate rather than a join. Incomplete records use `incomplete_record`. Schema drift is checked at table rather than row level, because it is a property of the file.

Drift is measured against **the columns already stored**, not a hardcoded list, so the check cannot rot when the schema legitimately changes. The result separates documented evolution from undocumented additions and from columns the update dropped.

**Nothing stops the pipeline.** Failing rows go to `data/rejected/<dataset>` with a reason, counts go to the monitoring log, and accepted rows continue. A rule whose reference data is unavailable is skipped with a printed warning rather than silently passing every row — a missing check is reported, not assumed to have succeeded. The single exception is a missing *required* column, which still raises, because the file cannot be interpreted at all.

## 7. Verification and measurement

Correctness was verified on a synthetic fixture, because the course datasets were not present on the development machine. The fixture is a 20-row Bronze table with an update of 5 new trips, 3 verbatim duplicates and 1 trip carrying a zone id absent from the lookup. It exercises every claim this report makes about behaviour: insertion, duplicate suppression, referential rejection, schema evolution, idempotency, and monitoring output.

Performance figures — incremental update time, refresh time, storage, validation and monitoring overhead — must be measured against the full dataset, and are reported in [the evaluation report](evaluation_report.md).

## 8. Reproducibility

```bash
python run_ingestion.py                          # Week 1
python run_integration.py                        # Week 1
python -m src.analytics.data_products            # Week 2

python scripts/generate_incremental_updates.py   # Week 3: make release 2
python run_incremental_update.py                 # Week 3: merge + refresh
python -m src.monitoring.queries                         # Week 3: monitoring questions
python run_week3_evaluation.py                   # Week 3: the five overheads
```

Java 17 is required. Spark 3.5 does not run reliably on Java 21: a trivial job can succeed while real shuffle work fails with a `BlockManagerId` null-pointer error, so a smoke test that only calls `range().count()` is not sufficient evidence that the runtime is supported.

Running `run_incremental_update.py` a second time is the idempotency check and should insert 0 rows for every dataset.
