# Data Validation and Rejected Rows

Validation runs before any update is appended to Bronze. A rejected record is saved separately, so valid records still load and downstream products cannot use bad data.

## Rules

| Check | Purpose |
|---|---|
| Required fields | Reject incomplete input records |
| Existing validity rule | Keep the original source-quality rules |
| Duplicate detection | Stop a release from inserting an existing record twice |
| Taxi-zone reference | Reject taxi location IDs not present in the zone lookup |
| Humidity range | Accept values from 20 to 100 only |
| AQI range | Accept values from 0 to 500 only |
| Schema comparison | Allow only the documented new columns |

## Rejected-row handling

```text
invalid or duplicate record -> rejection_reason -> data/rejected/<dataset>/
valid new record            -> Bronze append -> Silver/Gold/product refresh
```

The final release rejected 8,490 taxi rows because they were deliberate duplicates. Weather and air quality had zero rejected rows. This confirms that valid rows are not blocked by a separate bad record.

To add another validation rule, add a condition in `prepare_update()` in `src/operations/pipeline.py`. The common rejection and monitoring flow remains the same.
