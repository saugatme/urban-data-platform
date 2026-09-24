# Task 2 – Keeping the Analytical Products Correct

The Week 2 products were built once from a fixed Gold table. Week 3 adds a second release, so each product must come back into line with the new data without recomputing more than necessary.

The refresh policy lives in `PRODUCTS` in `src/analytics/data_products.py`; the machinery is in `src/analytics/data_products.py`.

---

## Which products refresh incrementally, and which cannot

The deciding question is whether new data can change a row that already exists.

| Product | Grain | Mode | Why |
|---|---|---|---|
| `daily_mobility` | zone × day | **incremental** | A trip belongs to exactly one day. New trips land in new or recent days, so untouched months keep their values. |
| `taxi_zone_statistics` | zone, whole period | **full** | Every metric is a whole-history aggregate. One new trip changes that zone's totals and averages, so no existing row survives unchanged. |
| `weather_impact` | temp bucket × condition, whole period | **full** | Same reason — the averages span the entire dataset. |
| `air_quality_impact` | PM2.5 category, whole period | **full** | Same reason, over three category rows. |

Only `daily_mobility` is partitioned by a time key, and that is exactly what makes it incrementally refreshable. The other three are small (258, 25 and 3 rows), so a full rebuild is cheap anyway — the cost of recomputing them is dominated by scanning Gold, which the incremental product has to do regardless.

This is the general rule worth stating: **a product can refresh incrementally when its grain includes the column the new data is bounded by.** Whole-period aggregates never can.

---

## How the incremental refresh works

`daily_mobility` is rewritten only for the months the new data touches:

1. Read the watermark — the `source_max_timestamp` recorded for this product on its last successful refresh.
2. Select trips newer than that watermark and collect their distinct `(year, month)` pairs.
3. Rebuild the product for only those months.
4. Write with Delta `replaceWhere` on the same `(year, month)` predicate, so only those partitions are replaced.

Untouched months are never read, rewritten, or re-versioned.

Two conditions fall back to a full rebuild, deliberately:

- **No watermark yet.** The first Week 3 refresh has nothing to compare against.
- **The product is not partitioned.** Week 2 wrote `daily_mobility` unpartitioned, so the first Week 3 refresh rewrites it partitioned by `year`/`month`; every later run can then go incremental.

---

## Avoiding unnecessary recomputation

Work is skipped at two levels.

**Whole-product skip.** Every refresh compares the Gold table's maximum `pickup_datetime` against the product's recorded watermark. If they match, the product is skipped entirely and logged with `action = "skipped"`. Re-running `run_incremental_update.py` with no new data therefore does no aggregation work at all.

**Partition-level skip.** For `daily_mobility`, only the affected months are recomputed, as described above.

The watermarks live in `data/gold/data_products/_product_log`, which extends the Week 2 metadata idea into a per-refresh history:

| Column | Purpose |
|---|---|
| `product_name` | Which product |
| `source_tables` | Provenance (`integrated_taxi_trips`) |
| `refresh_mode` | Configured mode (`incremental` / `full`) |
| `action` | What actually happened (`incremental` / `full` / `skipped`) |
| `rows_written` | Rows in the refreshed scope |
| `partitions_refreshed` | The `replaceWhere` predicate used, or null |
| `source_max_timestamp` | The watermark this refresh brought the product up to |
| `last_refreshed_at`, `execution_time_sec`, `schema_version` | Timing and versioning |

`refresh_mode` records intent and `action` records outcome. Keeping both is what makes a fallback visible — a product configured `incremental` that logged `full` is a signal, not a silent regression.

---

## Schema evolution

The new columns arrive in Bronze (`humidity` on weather, `aqi` on air quality) and flow through Silver into Gold, because those layers are rebuilt from Bronze and the writes carry `mergeSchema`.

**Automatic.** The four products keep working with no code change. None of them selects `*`, and none references `humidity` or `aqi`, so a new source column simply passes them by. Existing Week 2 queries are likewise unaffected: every one names its columns explicitly, so a widened Gold schema cannot change their output.

**Manual.** Using a new column requires a deliberate edit, which is the correct default — a column appearing in a feed is not a reason to change what a published product means. To surface `humidity` in `weather_impact`, add the aggregation to the build function and raise `SCHEMA_VERSION`. Consumers can then tell the versions apart through the product's own `schema_version` metadata column.

The one case needing care is a **type change** on an existing column rather than an addition. `mergeSchema` will not silently reconcile that, and it is reported as drift by `check_schema_drift()` rather than applied.

---

## Known limitation: Silver and Gold are rebuilt

Bronze is merged incrementally, but `run_incremental_update.py` rebuilds Silver and Gold in full for any dataset that changed.

This is a correctness decision, not an oversight. Both layers have whole-table semantics:

- `enforce_common_model()` drops columns that are null across **every** row, which cannot be decided from a partial view.
- The Gold join enriches each trip against hourly weather and air quality, and a new weather row can legitimately affect trips already stored.

A partial rebuild could therefore leave Silver or Gold inconsistent with Bronze. The saving is taken where it is both safe and largest — the Bronze merge and the product refresh.

Making Gold genuinely incremental is the clearest next step: restrict the rebuild to the `(year, month)` partitions the merge touched and write them with `replaceWhere`, exactly as `daily_mobility` does. That requires proving no late-arriving weather row can affect an older partition, which the current data does not guarantee.
