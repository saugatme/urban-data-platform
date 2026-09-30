# Task 1 — Training Dataset Design

## Prediction problem

The prediction task is pickup-time **trip-duration regression**. The target, `trip_duration_minutes`, is calculated as the difference between `dropoff_datetime` and `pickup_datetime`.

## Automatic dataset generation

`src/ml/training.py` reads the integrated Gold Delta table and creates the ML source dataset. It excludes records with missing pickup context and durations outside one to 180 minutes. A lower date boundary of 2024-01-01 removes the small number of historical timestamp outliers found during data profiling.

The generated Delta table is written to:

```text
data/gold/ml/trip_duration_training/
```

It is partitioned by `dataset_split`.

## Feature groups

| Group | Features | Reason |
|---|---|---|
| Temporal | pickup hour, weekday, month | Capture regular traffic and travel patterns. |
| Location | pickup location ID, pickup borough | Describe trip origin and local mobility patterns. |
| Trip context | passenger count | Available at pickup and potentially related to trip behaviour. |
| Weather | temperature, humidity, precipitation, wind speed, condition code | Context at the pickup hour. |
| Air quality | hourly PM2.5 average | Environmental context from an independent municipal source. |

The target comes from Taxi Trips. Taxi Zone Lookup supplies borough context. Weather and Air Quality contribute hourly context after the Gold-layer joins.

## Leakage prevention and splits

Fare, total amount, observed trip distance, drop-off location, and drop-off time are excluded. They are known after or during a trip and would make a pickup-time prediction unrealistically accurate.

The script performs a chronological 70/15/15 split. The current post-Week-3 dataset has 6,268,541 training records, 1,343,120 validation records, and 1,343,585 test records. The test period is later than the fitting period, so it simulates future prediction rather than random-row evaluation.
