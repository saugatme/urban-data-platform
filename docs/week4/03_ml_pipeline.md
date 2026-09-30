# Task 3 — Reproducible ML Pipeline

The full Spark ML pipeline combines the reusable feature stages with a regression estimator. It supports two models:

| Model | Role | Configuration |
|---|---|---|
| Linear Regression | Fast, interpretable baseline | 50 iterations and L2 regularization (`regParam=0.1`). |
| Random Forest Regressor | Nonlinear comparison | 20 trees, depth 7, 50% subsampling, fixed seed 42. |

Run either model independently:

```powershell
python scripts/ml/run_training.py --model linear
python scripts/ml/run_training.py --model random_forest
```

Or train and compare both:

```powershell
python scripts/ml/run_training.py --model both
```

The default runner uses `local[2]` and a 4 GB driver heap to keep the forest
within a typical local machine's memory. On a machine with more available RAM,
increase the heap deliberately:

```powershell
python scripts/ml/run_training.py --model random_forest --driver-memory 6g
```

Each fitted `PipelineModel` includes imputation statistics, category mappings, encoding, scaling, and the trained estimator. The saved artifact can therefore transform new input in exactly the same way as training input.

```text
data/models/week4/trip_duration_linear_pipeline/
data/models/week4/trip_duration_random_forest_pipeline/
```

Retraining is a rerun of the same command after the integrated Delta table has been refreshed. The chronological split, feature definitions, model parameters, evaluation metrics, and output locations are deterministic apart from Spark runtime conditions.
