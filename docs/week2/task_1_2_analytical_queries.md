# Tasks 1 & 2 – Analytical Queries

## Dataset

The source table (`gold/integrated_taxi_trips`) contains 8,480,836 NYC taxi trips joined with hourly weather, hourly PM2.5 air quality readings, and taxi zone names. It is partitioned by `year` and `month`. The main columns used are `pickup_datetime`, `pickup_location_id`, `pickup_zone`, `trip_distance`, `condition_code`, `pm25_hourly_avg`, and `total_amount`.

---

## Q1 – Monthly Demand by Zone

How many trips started from each zone each month?

Trips are counted and grouped by year, month, and zone. Filtered to `year > 2023`.

Output: `year`, `month`, `pickup_location_id`, `pickup_zone`, `taxi_demand`

---

## Q2 – Trip Distance by Weather Condition

Do people travel different distances depending on the weather?

Weather codes (1–16) are mapped to readable names. Average trip distance is calculated per condition. Trips with zero distance are excluded.

Output: `weather_condition`, `trip_count`, `avg_trip_distance`

---

## Q3 – Air Quality vs Demand

Does air pollution affect how many trips are taken?

PM2.5 values are grouped into five ranges: `0–10`, `10–20`, `20–30`, `30–50`, `50+`. Trip count and average distance are reported per range. Rows with no PM2.5 reading are excluded.

Output: `pm25_range`, `taxi_demand`, `avg_trip_distance`

---

## Q4 – Zone Demand Variance by Weather

Which zones have the most unstable demand when weather changes?

Trips are first counted per zone per weather condition. Then the standard deviation of those counts is calculated per zone — a higher value means demand changes more depending on the weather.

Output: `pickup_zone`, `demand_variation`, `min_demand`, `max_demand`, `avg_demand`

---

## Q5 – Peak Hour per Day of the Week

What is the busiest hour for each day of the week?

Trips are grouped by day and hour. The single busiest hour per day is selected using a ranking window function.

Output: `day_name`, `peak_hour`, `trip_count`

---

## Q6 – Monthly Demand Trend

How does demand change month to month in 2024?

Trips are counted per month. Each month is compared to the previous one to calculate the percentage change. January has no prior month so its change is null. The window is partitioned by year to avoid crossing year boundaries.

Output: `year`, `month`, `taxi_demand`, `mom_change_pct`

---

## Implementation

All queries are functions in `src/analytics/queries.py`. Each takes a Spark session and returns a result table. They read from the `trips` view registered by `register_trips()`. None of them modify the source data.