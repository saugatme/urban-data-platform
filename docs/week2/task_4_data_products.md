# Task 4 – Analytical Data Products

Four summary tables were generated from the integrated trips dataset and saved under `data/gold/data_products/`.

---

## 1. Daily Mobility Summary

**Path:** `data/gold/data_products/daily_mobility` — 19,793 rows

Trip count, total distance, average distance, total revenue, and average fare per zone per day.

**Who uses it:** Transport planners tracking daily demand by zone.

**Why pre-compute it:** Counting and summing 8.4M trips on every query is slow. A pre-built daily summary makes those queries instant.

---

## 2. Taxi Zone Statistics

**Path:** `data/gold/data_products/taxi_zone_statistics` — 258 rows

Total trips, distance, and revenue per zone across the full dataset.

**Who uses it:** Fleet managers comparing zones.

**Why pre-compute it:** Scanning 8.4M rows to produce 258 zone totals is wasteful to repeat every time.

---

## 3. Weather Impact Summary

**Path:** `data/gold/data_products/weather_impact` — 25 rows

Average distance, fare, temperature, precipitation, and wind speed grouped by temperature range and weather condition.

**Who uses it:** Analysts studying how weather affects travel.

**Why pre-compute it:** Grouping all trips by weather on every query is expensive. This reduces it to a 25-row table.

---

## 4. Air Quality Impact Summary

**Path:** `data/gold/data_products/air_quality_impact` — 3 rows

Total trips, average PM2.5, average distance, and average fare grouped by air quality category.

**Who uses it:** Public health researchers looking at the link between air quality and taxi use.

**Why pre-compute it:** Filtering and grouping millions of rows by PM2.5 on every query is expensive. The NYC 2024 data only reached three categories (Good, Moderate, Unhealthy) because pollution levels stayed low throughout the year.

---

## Metadata

Each product stores four extra columns:

| Column | What it records |
|---|---|
| `data_source` | Where the data came from (`integrated_taxi_trips`) |
| `creation_time` | When the product was first generated |
| `refresh_time` | When it was last updated |
| `schema_version` | Version number (`1.0`) |

---

## Storage

| Product               | Size      |
|-----------------------|-----------|
| Integrated trips      | 1.19 GB   |
| daily_mobility        | 1.77 MB   |
| taxi_zone_statistics  | 57.31 KB  |
| weather_impact        | 22.55 KB  |
| air_quality_impact    | 14.39 KB  |
| **Total products**    | **1.87 MB** |
| **Ratio**             | **0.15% of source** |