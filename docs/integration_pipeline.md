# Task 5: Integration Pipeline

## Overview

The integration pipeline enriches each taxi trip with contextual information from three external datasets. The output is a single Delta table — `gold/integrated_taxi_trips` — where each row is one taxi trip with weather, air quality, pickup zone, and dropoff zone information attached.

---

## Output

| Column Group | Columns Added |
|---|---|
| Weather | `temp`, `rel_humidity`, `precipitation`, `wind_speed`, `wind_direction`, `pressure`, `condition_code` |
| Air Quality | `pm25_daily_avg` |
| Pickup Zone | `pickup_zone`, `pickup_borough`, `pickup_service_zone` |
| Dropoff Zone | `dropoff_zone`, `dropoff_borough`, `dropoff_service_zone` |

Final row count: **8,480,870** — identical to silver taxi trips. No trips were lost.

---

## Integration Strategy

### Join 1: Trips → Weather (hourly)

Weather stores time as separate `year`, `month`, `day`, `hour` integer columns. A `weather_ts` timestamp is constructed at join time and matched against the trip's `pickup_datetime` truncated to the hour.

```
trip pickup_datetime → truncate to hour → match weather_ts
```

**Hit rate:** 100% (8,480,852 / 8,480,870). 18 trips at the DST transition hour (March 31 03:00) received null weather — acceptable.

**DST issue:** The daylight saving time clock change on March 31 caused two weather rows to map to the same `weather_ts = 2024-03-31 03:00:00`. Resolved by deduplicating on `weather_ts` before joining.

### Join 2: Trips → Air Quality (daily average)

Air quality is measured by 925 monitoring sites across the US. Two decisions were required:

**Site selection:** A fixed NYC site was chosen — Queens, site 124 (state=36, county=81, site=124), coordinates (40.736, -73.822). Central to NYC taxi activity. Fixed site chosen over nearest-site computation for simplicity and reproducibility.

**Temporal granularity:** `time_local` was dropped in silver (HH:mm string, not a valid timestamp). `date_local` contains date only. All 24 hourly readings are aggregated to a daily average PM2.5, joined to trips by pickup date.

```
trip pickup_date → match aq_date → attach daily avg PM2.5
```

**Hit rate:** 100% — complete coverage for Jan–Mar 2024.

**Limitation:** All trips on a given day receive the same PM2.5 value regardless of pickup hour. Hourly resolution requires preserving `time_local` in silver.

### Join 3 & 4: Trips → Taxi Zones (pickup and dropoff)

The taxi zones table is joined twice — once for pickup, once for dropoff — adding zone name, borough, and service zone for each.

```
pickup_location_id  → location_id → pickup_zone, pickup_borough
dropoff_location_id → location_id → dropoff_zone, dropoff_borough
```

**Hit rate:** 100%. `location_id = 265` (unknown zone) is preserved — trips get "Unknown" borough rather than null.

---

## Why Left Joins

All four joins are left joins. Every valid taxi trip appears in gold regardless of whether contextual data is available. A trip with missing weather is still useful for zone-level or fare analysis.

---

## Why Broadcast Joins

Weather (8,784 rows), air quality after aggregation (~366 rows), and taxi zones (265 rows) fit entirely in memory. Spark broadcasts these to every worker, eliminating shuffle operations — the most significant performance optimization in the pipeline.

---

## Limitations

| Limitation | Impact |
|---|---|
| Single fixed AQ site | PM2.5 represents one location, not the trip's actual air quality |
| Daily AQ granularity | All trips on a day share one PM2.5 value — no hourly variation |
| Single implicit weather station | No spatial weather variation across NYC |
| Jan–Mar trips only | Gold table covers Q1 2024 only |

---

## Results

| Metric | Value |
|---|---|
| Input trips (silver) | 8,480,870 |
| Output rows (gold) | 8,480,870 |
| Weather matched | 8,480,852 (100.0%) |
| AQ matched | 8,480,852 (100.0%) |
| Partition strategy | `year`, `month` |