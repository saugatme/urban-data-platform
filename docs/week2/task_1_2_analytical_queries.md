# Tasks 1 & 2 – Analytical Query Design and Implementation

## Dataset

The integrated Delta table (`gold/integrated_taxi_trips`) combines NYC taxi trips with
hourly weather (Meteostat condition codes, temperature, precipitation), hourly PM2.5 air
quality readings, and the taxi zone lookup. It is partitioned by `year` and `month`.
Key columns used across all queries: `pickup_datetime`, `pickup_location_id`,
`pickup_zone`, `trip_distance`, `condition_code`, `pm25_hourly_avg`, `total_amount`.

---

## Q1 – Monthly Taxi Demand by Zone

**Question:** How many trips originate from each taxi zone each month?

**Design:** Demand is measured as trip count (`COUNT(*)`), grouped by `year`, `month`,
and `pickup_location_id` / `pickup_zone`. Filtered to `year > 2023` to exclude
historical data outside the platform's operational window.

**Output columns:** `year`, `month`, `pickup_location_id`, `pickup_zone`, `taxi_demand`

---

## Q2 – Average Trip Distance by Weather Condition

**Question:** Do riders travel different distances under different weather conditions?

**Design:** Condition codes follow the Meteostat standard (1–16). Each code is mapped to
a human-readable label via `CASE`. `AVG(trip_distance)` is computed per condition.
Rows with `trip_distance = 0` are excluded as likely data errors.


**Output columns:** `weather_condition`, `trip_count`, `avg_trip_distance`

---

## Q3 – Air Quality (PM2.5) vs Taxi Demand

**Question:** Does PM2.5 pollution level correlate with taxi trip volume or distance?

**Design:** PM2.5 values are bucketed into five ranges (µg/m³): `0–10`, `10–20`,
`20–30`, `30–50`, `50+`. These align roughly with EPA good/moderate/unhealthy
thresholds while keeping ranges narrow enough to show gradients. Demand (`COUNT(*)`)
and average distance are reported per bucket. NULL PM2.5 rows are excluded.


**Output columns:** `pm25_range`, `taxi_demand`, `avg_trip_distance`

---

## Q4 – Zones with Highest Demand Variance Across Weather Conditions

**Question:** Which zones show the most unstable demand when weather changes?

**Design:** A CTE first aggregates trip count per `(pickup_zone, condition_code)` pair.
The outer query then computes `STDDEV(demand)` per zone — a high standard deviation
means demand swings significantly across weather types. `MIN`/`MAX`/`AVG` are included
for context. Only zones with at least one recorded condition are included.


**Output columns:** `pickup_zone`, `demand_variation`, `min_demand`, `max_demand`, `avg_demand`

---

## Q5 – Peak Travel Hour per Day of the Week

**Question:** At what hour does each day of the week see the most taxi trips?

**Design:** Trips are grouped by `(day_of_week, hour_of_day)` using `DAYOFWEEK` and
`HOUR`. A `ROW_NUMBER()` window function partitioned by `day_of_week` and ordered by
`trip_count DESC` selects only the single busiest hour per day (`rn = 1`). The hour is
formatted as `HH:00` for readability.
 

**Output columns:** `day_name`, `peak_hour`, `trip_count`

---

## Q6 – Monthly Demand Trend (Month-over-Month)

**Question:** How does taxi demand grow or shrink month to month through 2024?

**Design:** Trips are aggregated by year and month for `year >= 2024`. A `LAG()` window function
partitioned by year retrieves the previous month's demand, and the percentage change is computed as
`100 * (current - previous) / previous`. January returns NULL for `mom_change_pct`
as there is no prior month in that year. Partitioning keeps trends from crossing years and avoids an unpartitioned-window warning.


**Output columns:** `year`, `month`, `taxi_demand`, `mom_change_pct`

---

## Implementation Notes

All queries are implemented as functions in `src/analytics/queries.py`. Each function
accepts a `SparkSession` and returns a `DataFrame`, operating on the `trips` temp view
registered by `register_trips(spark, base)`. No query modifies the source data.