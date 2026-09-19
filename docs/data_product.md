# Task 4 – Reusable Analytical Data Products

Four data products were generated from the integrated taxi dataset and stored as Delta tables under `data/gold/data_products/`.

---

## Data Products

### 1. Daily Mobility Summary
**Path:** `data/gold/data_products/daily_mobility` — 19,793 rows

Aggregates trip count, total distance, average distance, total revenue, and average fare per zone per day.

**Who uses it:** City planners and transport operations teams monitoring daily demand patterns.  
**Why materialise:** Aggregating 8.4M trips per query is expensive. Pre-computing daily summaries enables sub-second dashboard queries.

---

### 2. Taxi Zone Statistics
**Path:** `data/gold/data_products/taxi_zone_statistics` — 258 rows

Aggregates total trips, distance, and revenue per taxi zone across the full dataset.

**Who uses it:** Fleet managers and zone-level operations teams identifying high- and low-demand zones.  
**Why materialise:** A full table scan and aggregation across 8.4M rows to produce 258 summary rows is wasteful to repeat on every query.

---

### 3. Weather Impact Summary
**Path:** `data/gold/data_products/weather_impact` — 25 rows

Groups trips by temperature bucket (Cold/Mild/Hot) and Meteostat weather condition. Reports average distance, fare, temperature, precipitation, and wind speed per group.

**Who uses it:** Data scientists and demand forecasters studying how weather affects travel behaviour.  
**Why materialise:** Joining and bucketing all trips on weather columns on every query is expensive. This collapses it to a 25-row lookup.

---

### 4. Air Quality Impact Summary
**Path:** `data/gold/data_products/air_quality_impact` — 3 rows

Groups trips by EPA AQI category based on PM2.5 hourly averages. Reports total trips, average PM2.5, average distance, and average fare per category.

**Who uses it:** Public health researchers and environmental analysts studying the relationship between air quality and taxi demand.  
**Why materialise:** PM2.5 filtering, bucketing, and aggregation across millions of rows is expensive to repeat. The NYC 2024 dataset produced 3 AQI categories (Good, Moderate, Unhealthy), reflecting that PM2.5 values stayed within the lower pollution bands throughout the year.

---

## Metadata

All four products include the following metadata columns:

| Column | Description |
|---|---|
| `data_source` | Source table (`integrated_taxi_trips`) |
| `creation_time` | Timestamp when the product was generated |
| `refresh_time` | Timestamp of the most recent refresh |
| `schema_version` | Schema version (`1.0`) |

---

## Storage

Products are stored as Delta tables, supporting efficient querying, schema enforcement, and future incremental refresh via Delta `MERGE`.