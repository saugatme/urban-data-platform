# Model Evaluation and Visual Evidence

`scripts/ml/run_training.py` writes reusable metrics and PNG charts:

```text
data/benchmark/week4_ml/evaluation.json
data/benchmark/week4_ml/charts/model_performance.png
data/benchmark/week4_ml/charts/actual_vs_predicted.png
data/benchmark/week4_ml/charts/residual_distribution.png
data/benchmark/week4_ml/charts/feature_importance.png
```

The current evidence shows a modest Random Forest improvement over Linear Regression:

| Model | Test RMSE | Test MAE |
|---|---:|---:|
| Linear Regression | 10.88 min | 7.63 min |
| Random Forest | 10.84 min | 7.38 min |

The actual-versus-predicted and residual charts show the main limitation: both models predict ordinary short/medium trips more reliably than unusually long trips. Long trips are commonly underpredicted because route, real-time traffic, incident, and destination information are absent from the leakage-safe pickup-time feature set.

Random Forest importance is concentrated in pickup borough, pickup location ID, and passenger count. This confirms that geography is a stronger signal than the available city-wide weather and PM2.5 measurements. Borough and zone are correlated, so their separate importance values should be interpreted as a combined geographic signal rather than causal effects.

The most promising future improvement is adding leakage-safe historical duration/count features by pickup zone, hour, and weekday. If destination is known at booking time, route-distance estimates and destination-zone features would improve the prediction definition substantially.
