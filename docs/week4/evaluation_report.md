# Week 4 — ML Evaluation Report

## Model evaluation

The models were evaluated on the same chronological future test split. RMSE penalizes large duration errors; MAE gives the average absolute error in minutes.

| Model | Validation RMSE | Validation MAE | Test RMSE | Test MAE | Training time |
|---|---:|---:|---:|---:|---:|
| Linear Regression | 11.14 min | 7.52 min | 10.88 min | 7.63 min | baseline workflow timing below |
| Random Forest | 11.06 min | 7.45 min | 10.84 min | 7.38 min | 151.03 s |

Random Forest produces the best test result, improving MAE by 0.25 minutes (3.3%) and RMSE by 0.04 minutes (0.4%) over Linear Regression. The gain is modest relative to the additional training cost, so Linear Regression remains a strong practical baseline while Random Forest demonstrates that nonlinear interactions add limited additional signal with the current feature set.

The target is right-skewed: its mean is 15.50 minutes and median is 11.97 minutes. The actual-versus-predicted plot shows predictions concentrating around typical short and medium trip durations. The model overpredicts some very short trips and underpredicts unusually long trips. The residual histogram has a longer negative tail (`prediction - actual`), confirming that unexpectedly long trips are the main failure case.

## Feature analysis

Random Forest feature importance is dominated by pickup geography:

| Feature group | Relative importance |
|---|---:|
| Pickup borough | 0.617 |
| Pickup location ID | 0.298 |
| Passenger count | 0.066 |
| Temperature | 0.011 |
| All remaining weather, air-quality, and temporal features | < 0.003 each |

Location is therefore the strongest available predictive signal. It likely captures local road layout, typical origin-destination patterns, and borough-level traffic conditions. Pickup borough and pickup location ID are correlated, so their individual importance should not be interpreted as causal; their combined geographic importance is the meaningful result. Weather and PM2.5 remain valid contextual inputs but do not explain individual trip duration as strongly as origin location.

The generated charts are stored under `data/benchmark/week4_ml/charts/`. The residual chart intentionally clips to the central 98% of sampled residuals so a small number of extreme trips does not obscure the main distribution.

## Data-engineering comparison

Two workflows prepared and trained the same kind of model:

| Workflow | Work required | Measured end-to-end time |
|---|---|---:|
| Approach A: raw sources | Read Parquet/CSV, rename, validate, create timestamps, aggregate PM2.5, join weather/air/zones, derive features, train | 50.09 s |
| Approach B: integrated platform | Read validated integrated Delta data, derive features, train | 35.19 s |

The integrated-platform workflow reduced measured end-to-end time by **29.8%** and eliminated repeated raw schema and timestamp work. The raw workflow also initially demonstrated a realistic failure mode: an incorrectly constructed air-quality timestamp produced all-null PM2.5 values. The standard Silver transformation already solves this conversion, illustrating how central data engineering improves ML correctness as well as speed.

The comparison is directional rather than a strict identical-row benchmark because the integrated platform includes the Week 3 incremental release, whereas the raw workflow starts from the original source release. It still demonstrates that validated, standardized, and integrated Delta data reduces implementation complexity and repeated preprocessing.

## Recommendations

The largest improvement opportunity is feature quality rather than a larger forest. Leakage-safe historical median duration and trip-count features by pickup zone, hour, and weekday would provide a strong baseline expectation. If the destination is available at booking time, a pre-trip route-distance estimate and drop-off-zone feature would materially improve predictions. Public holidays, traffic incidents, and roadwork datasets would address the long-trip underprediction seen in residuals.

For future operation, add feature versioning, data/model drift monitoring, scheduled retraining after validated updates, and a promotion rule requiring a new model to meet or exceed current validation performance.
