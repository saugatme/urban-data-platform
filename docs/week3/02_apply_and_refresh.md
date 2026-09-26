# Incremental Processing and Product Refresh

The pipeline reads the prepared files in `data/updates/`, then keeps valid new records and excludes duplicates or invalid rows.

```text
update files -> standardise -> validate -> duplicate check -> Bronze append
                                                          -> rejected rows
Bronze changes -> affected Silver/Gold rebuild -> affected product refresh
```

## Processing rules

| Dataset | Duplicate rule | Schema change |
|---|---|---|
| Taxi trips | SHA-256 hash of shared fields | None |
| Weather | Existing date-and-hour key | Allow `humidity` |
| Air quality | Existing observation key | Allow `aqi` |

A left-anti join selects only rows not already in Bronze. Duplicate and invalid rows are written to `data/rejected/<dataset>/` with a `rejection_reason`. Existing Bronze records are never overwritten.

## Product dependencies

| Product | Refreshed when |
|---|---|
| `daily_mobility` | Taxi changes |
| `taxi_zone_statistics` | Taxi changes |
| `weather_impact` | Taxi or weather changes |
| `air_quality_impact` | Taxi or air-quality changes |

The final run changed all three source datasets, so all four products refreshed. The implementation uses a full rebuild for an affected product because the products are whole-history aggregates. It avoids any refresh when a repeated release inserts no new rows.

## Run

```powershell
scripts\operations\run_pipeline.py
```

Final result:

```text
taxi_trips: processed=517,165, inserted=508,675, rejected=8,490
weather: processed=168, inserted=168, rejected=0
air_quality: processed=168, inserted=168, rejected=0
```

If a product refresh is interrupted after data has been inserted, rebuild downstream tables without applying the release again:

```powershell
.\.venv\Scripts\python.exe scripts\operations\run_pipeline.py --refresh
```
