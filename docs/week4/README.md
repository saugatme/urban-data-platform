# Week 4 — Machine Learning Pipelines

Week 4 turns the validated and integrated urban-data platform into a reproducible Spark ML workflow for **trip-duration prediction**. It generates a Delta training dataset, derives reusable features, trains baseline and nonlinear models, writes evaluation results, and saves fitted pipelines.

## Prerequisites

Complete the earlier platform stages first:

```powershell
.\.venv\Scripts\Activate.ps1
python run_ingestion.py
python run_integration.py
python scripts/operations/run_pipeline.py
```

The final command is optional when Week 3 incremental releases are not required, but the reported results below use the post-Week-3 Gold table.

## Run Week 4

```powershell
python scripts/ml/run_training.py --model both
```

The command performs the following work:

1. reads `data/gold/integrated_taxi_trips`;
2. creates a leakage-safe trip-duration target and a 70/15/15 chronological split;
3. writes the split training dataset as Delta;
4. trains Linear Regression and Random Forest pipelines;
5. saves both fitted pipelines, metrics, and charts.

Use `--model linear` or `--model random_forest` to run one model only. Use `--skip-training-write` to avoid rewriting the generated training Delta table.

The default uses two local Spark workers and a 4 GB JVM heap to avoid exhausting
memory during Random Forest training. If the machine has more available RAM,
use `--driver-memory 6g`; do not use `local[*]` for the forest unless memory is
known to be sufficient.

To reproduce the raw-data versus integrated-platform comparison after model training:

```powershell
python scripts/ml/compare_workflows.py
```

## Outputs

```text
data/gold/ml/trip_duration_training/             Task 1 split training Delta table
data/models/week4/trip_duration_linear_pipeline/ fitted baseline pipeline
data/models/week4/trip_duration_random_forest_pipeline/
data/benchmark/week4_ml/evaluation.json          metrics and split boundaries
data/benchmark/week4_ml/charts/                  PNG evaluation charts
data/benchmark/week4_ml/raw_vs_platform.json     Task 4 timing comparison
```

## Task Documents

| Document | Purpose |
|---|---|
| [Task 1 — Training dataset](01_training_dataset.md) | Target, features, split strategy, and assumptions. |
| [Task 2 — Feature engineering](02_feature_engineering.md) | Reusable Spark transformation stages and missing-value handling. |
| [Task 3 — ML pipeline](03_ml_pipeline.md) | Training, evaluation, saving, and retraining workflow. |
| [Task 4 — Data-engineering comparison](04_data_engineering_evaluation.md) | Raw-source versus integrated-platform implementation and timing. |
| [Model evaluation](05_model_evaluation.md) | Metrics, diagnostic charts, feature importance, and recommendations. |
| [Week 4 ML design report](design_report.md) | Consolidated Tasks 1–3 submission narrative. |
| [Week 4 ML evaluation report](evaluation_report.md) | Consolidated Task 4 and model-evidence narrative. |

## Notebook

The exploratory and evidence-generating notebook is [week4.ipynb](../../notebooks/week4.ipynb). The scripts are the reproducible implementation; the notebook is useful for inspecting intermediate output and charts.
