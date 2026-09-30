# Week 4 — ML Design Report: Trip-Duration Prediction

## Prediction problem and training dataset

The platform predicts `trip_duration_minutes` for one taxi trip. The target is calculated as the difference, in minutes, between `dropoff_datetime` and `pickup_datetime`. Records with missing timestamps, missing pickup context, or durations outside one to 180 minutes are excluded. The lower duration boundary removes trivial or erroneous trips; the upper boundary limits the influence of exceptional records on a baseline regression task.

The source is the validated `gold/integrated_taxi_trips` Delta table. The training generator filters out the small number of historical timestamp outliers before 2024 and selects only data available at pickup time. It writes `gold/ml/trip_duration_training` partitioned by `dataset_split`.

The resulting dataset contains 8,955,246 labelled trips. It uses a chronological 70/15/15 split rather than a random split:

| Split | Rows | Time period | Average duration |
|---|---:|---|---:|
| Training | 6,268,541 | 2024-01-01 to 2024-03-09 | 15.24 min |
| Validation | 1,343,120 | 2024-03-09 to 2024-03-22 | 16.31 min |
| Test | 1,343,585 | 2024-03-22 to 2025-03-20 | 15.91 min |

Chronological splitting prevents later observations from influencing model fitting. The test set is therefore a realistic future-data evaluation rather than an easier random sample.

## Feature design

The feature pipeline derives `pickup_hour`, `pickup_day_of_week`, and `pickup_month` from `pickup_datetime` inside Spark ML. It retains the following features:

| Group | Features | Reason |
|---|---|---|
| Temporal | pickup hour, weekday, month | Capture rush-hour, weekly, and seasonal travel patterns. |
| Location | pickup location ID, pickup borough | Capture geographic origin and typical local trip patterns. |
| Trip context | passenger count | Available at pickup and may distinguish travel behaviour. |
| Weather | temperature, relative humidity, precipitation, wind speed, condition code | Describe weather conditions at the pickup hour. |
| Air quality | hourly PM2.5 average | Adds environmental context from another municipal source. |

Fare amount, total amount, observed trip distance, drop-off attributes, and drop-off time are intentionally excluded. They are only available after or during the trip, so using them would introduce target leakage for a pickup-time prediction.

Weather fields have 1.14% missing values and PM2.5 has 1.75% missing values. The pipeline preserves these records and replaces missing numeric values with medians learned from the training split only. This avoids both row loss and validation/test leakage.

## Reusable feature-engineering pipeline

The Spark `Pipeline` consists of:

1. `SQLTransformer` for temporal feature derivation;
2. `Imputer` using training medians for numeric fields;
3. `StringIndexer` for pickup location, borough, and weather condition;
4. `OneHotEncoder` for indexed categories, retaining an unseen-category bucket;
5. numeric `VectorAssembler` and `StandardScaler`;
6. final `VectorAssembler`, producing a 292-dimensional sparse `features` vector;
7. a regression estimator.

The categorical and numeric feature lists are kept as separate constants in `src/ml/training.py`. A compatible integrated attribute can be added by placing it in the appropriate list; all downstream indexing, imputation, scaling, and assembly logic remains unchanged. This reduces feature-engineering duplication and makes retraining reproducible.

## Model pipeline and retraining

The workflow trains two Spark MLlib regression models. Linear Regression is the fast, interpretable baseline. Random Forest Regressor models nonlinear relationships and interactions between location, time, and context. Both are fitted only on the training split and evaluated unchanged on validation and test data.

The saved `PipelineModel` includes the learned imputation medians, category labels, one-hot configuration, scaling statistics, and fitted estimator. Consequently, a later batch is transformed exactly as training data was transformed. Retraining requires only an updated integrated Delta table and a rerun of `scripts/ml/run_training.py`.

The pipeline can support another regression target, such as fare amount, by changing the target definition and reviewing features for leakage. A classification task would replace the estimator/evaluator while reusing the same feature stages.

## Extensions

The current data supports a pickup-time estimate, not a route-aware quote. The most valuable future additions would be leakage-safe historical zone-hour duration summaries, public-holiday indicators, traffic incidents, roadworks, and route-distance estimates when the destination is known at booking time. Any new dataset should first pass through the existing ingestion, validation, common-model, and Gold-integration layers before its attributes are added to the feature lists.
