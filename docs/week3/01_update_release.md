# Incremental Release

The release generator creates a small follow-up delivery under `data/updates/`. The original files in `data/raw/` are not changed.

## Release contents

| Dataset | New data | Change included |
|---|---|---|
| Taxi trips | 508,675 later trips | 8,490 copied duplicate rows for the duplicate check |
| Weather | 168 hourly rows | New `humidity` column |
| Air quality | 168 hourly rows | New `aqi` column |

Taxi contains 5.32% new records relative to the 9,554,778-row source. The copied rows are 1.64% of the 517,165-row release. Both values satisfy the required release ranges.

## How it is made

Taxi records are sampled from valid source trips, moved after the latest original trip time, and keep their original duration. The copied taxi records keep their original values so that the pipeline can recognise and isolate them as duplicates.

Weather continues from the latest available hour. Air quality continues from the next observation day. `humidity` is between 20 and 100, and `aqi` is between 0 and 500.

## Run

```powershell
scripts\operations\create_update_release.py
```

The script writes `data/updates/summary.json`. Check counts before applying the release.
