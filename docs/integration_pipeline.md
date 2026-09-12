# Task 5: Integration Pipeline

## Overview

The integration pipeline enriches each taxi trip with contextual information from three external datasets. The output is a single Delta table — `gold/integrated_taxi_trips` — where each row is one taxi trip with weather, air quality, pickup zone, and dropoff zone information attached.

---

## Output

| Column Group | Columns Added |
|---|---|
| Weather | `temp`, `rel_humidity`, `precipitation`, `wind_speed`, `wind_direction`, `pressure`, `condition_code` |
| Air Quality | `pm25_hourly_avg` |
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

### Join 2: Trips → Air Quality (hourly)

Air quality is measured by 925 monitoring sites across the US. Two decisions were required:

**Site selection:** A fixed NYC site was chosen — Queens, site 124 (state=36, county=81, site=124), coordinates (40.736, -73.822). Central to NYC taxi activity. Fixed site chosen over nearest-site computation for simplicity and reproducibility.

**Temporal granularity:** Silver combines `date_local` and `time_local` into `aq_timestamp`, then joins it to the trip pickup hour.

```
trip pickup_hour → match aq_timestamp → attach hourly avg PM2.5
```

**Hit rate:** 8,440,842 of 8,480,870 trips (99.5%).

**Limitation:** One fixed site represents all trips, so PM2.5 is not spatially specific.

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

Weather (8,784 rows), air quality after hourly aggregation, and taxi zones (265 rows) fit entirely in memory. Spark broadcasts these to every worker, eliminating shuffle operations — the most significant performance optimization in the pipeline.

---

## Limitations

| Limitation | Impact |
|---|---|
| Single fixed AQ site | PM2.5 represents one location, not the trip's actual air quality |
| Single implicit weather station | No spatial weather variation across NYC |
| Jan–Mar trips only | Gold table covers Q1 2024 only |

---

## Results

| Metric | Value |
|---|---|
| Input trips (silver) | 8,480,870 |
| Output rows (gold) | 8,480,870 |
| Weather matched | 8,480,852 (100.0%) |
| AQ matched | 8,440,842 (99.5%) |
| Partition strategy | `year`, `month` |
