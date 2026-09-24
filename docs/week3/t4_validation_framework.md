# Task 4 – Validation Framework

Week 1 validated rows with one hardcoded condition per dataset. Week 3 keeps that condition but adds a **rule list** to each dataset config, so new checks are added as data rather than by editing the validation loop.

---

## Structure

Three pieces, in `src/ingestion/validation.py`:

| Piece | Role |
|---|---|
| `ValidationRule` | A rejection reason plus a condition that marks rows to reject, and the reference data it needs |
| `apply_rules()` | The generic loop — applies every rule in order and never changes when a rule is added |
| `build_context()` | Loads only the reference data the active rules actually ask for |

A rule's `condition` returns a Column that is **true for rows to reject**. Rules are applied in order and each row is attributed to the first rule it fails, so a row is never counted twice across reasons.

---

## Generic vs dataset-specific

The rule *builders* are generic and reusable; the *configuration* is dataset-specific.

| Builder | Generic behaviour | Used by |
|---|---|---|
| `incomplete_record(columns)` | Rejects rows with a null in any listed column | taxi_trips, taxi_zones |
| `value_out_of_range(column, low, high)` | Rejects values outside a closed range | weather, air_quality |
| `missing_reference(columns, reference)` | Rejects ids absent from a reference set | taxi_trips |

Which columns and which bounds are dataset facts, so they live in `DATASETS` in `src/ingestion/config.py`, next to the other dataset-specific settings:

```python
"weather": {
    ...
    "schema_contract": SchemaContract(allowed_new=("humidity",)),
    "validation_rules": [
        value_out_of_range("humidity", 20, 100),
        value_out_of_range("temp", -50, 60),
    ],
},
```

---

## What the rules catch

The brief asks for five categories. Four are row-level rules; schema drift is a table-level check because it is a property of the file, not of any single row.

| Category | Where it is handled |
|---|---|
| Duplicates | Keyed datasets: `validate()` ranks by primary key and rejects rank > 1. Taxi trips: the content hash in `merge_hashed()`, since the dataset has no reliable key |
| Invalid values | The Week 1 `validity_condition`, plus `value_out_of_range` rules |
| Missing reference records | `missing_reference(["pickup_location_id", "dropoff_location_id"], "taxi_zone_ids")` |
| Unexpected schema changes | `check_schema_drift()` — table-level, see below |
| Incomplete records | `incomplete_record([...])` on each dataset's required columns |

### Referential integrity

A trip whose `pickup_location_id` or `dropoff_location_id` is not in the taxi-zone lookup is rejected with the reason `missing_reference_taxi_zone_ids`. The zone set is only 265 rows, so `build_context()` collects the ids once and the rule uses an `isin` predicate rather than a join.

Reference data is loaded lazily: a dataset whose rules declare no `requires` costs nothing extra.

### Schema drift

`check_schema_drift()` compares an incoming file against **the columns already stored in the Bronze table**, not a hardcoded list. Drift is therefore always measured against what the platform actually holds, and the check does not rot when the schema legitimately changes.

Each dataset declares its permitted evolution:

```python
"schema_contract": SchemaContract(allowed_new=("humidity",))
```

The result separates three cases:

- `evolved` — a documented addition (`humidity`, `aqi`). Accepted and merged.
- `unexpected` — an undocumented new column. Recorded as drift; the run continues.
- `missing` — a baseline column the update dropped. Recorded as drift; the run continues.

---

## The pipeline never stops

No rule raises. Failing rows are moved to `data/rejected/<dataset>` with a `rejection_reason`, the per-reason counts go to the monitoring log, and the accepted rows continue to the merge.

The rejected table changes meaning between the two pipelines, deliberately. Week 1 **overwrites** it, so it holds a snapshot of the last ingest. The incremental pipeline **appends**, because rejections from each release are worth keeping — which makes it a log rather than a snapshot. Each appended row therefore carries `rejected_at` and `source_release`, so re-running an update produces distinguishable entries instead of indistinguishable copies of the same rejection. A rule whose reference data is unavailable is skipped with a printed warning rather than silently passing every row — a missing check is reported, not assumed to succeed.

The one case that still raises is `validate_schema()`, which fails when a **required** column is absent. That is deliberate: the file cannot be interpreted at all, so continuing would be meaningless.

---

## Adding a rule

Adding a check requires no change to `apply_rules()` or to any pipeline.

**Reusing a builder** — append to the dataset's `validation_rules` in `config.py`:

```python
"validation_rules": [
    value_out_of_range("humidity", 20, 100),
    value_out_of_range("wind_speed", 0, 150),   # new
],
```

**A rule with new logic** — add a builder to `validation.py`:

```python
def future_timestamp(column: str) -> ValidationRule:
    def condition(df, _context):
        if column not in df.columns:
            return lit(False)
        return col(column) > current_timestamp()
    return ValidationRule(reason=f"{column}_in_future", condition=condition)
```

**A rule needing new reference data** — declare it in `requires` and load it in `build_context()`:

```python
return ValidationRule(
    reason="missing_reference_vendors",
    condition=condition,
    requires=("vendor_ids",),
)
```

The new reason appears automatically in the rejected table and in the monitoring log's `validation_failures` map, because that map is built from whatever reasons the run produced rather than from a fixed list of columns.

---

## Verification

The referential rule and the duplicate handling were verified on a synthetic fixture (see [the evaluation report](evaluation_report.md)): a trip carrying `pickup_location_id = 999`, which is absent from the zone lookup, is rejected with reason `missing_reference_taxi_zone_ids` rather than being silently dropped or silently kept.
