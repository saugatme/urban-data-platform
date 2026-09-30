"""Evaluation and chart helpers for Week 4 models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoderModel
from pyspark.sql import DataFrame, functions as F

from src.ml.training import CATEGORICAL_FEATURES, NUMERIC_FEATURES, TARGET


def regression_metrics(predictions: DataFrame) -> dict[str, float]:
    """Return RMSE and MAE in the original target unit (minutes)."""
    evaluator = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction")
    return {
        "rmse_minutes": round(evaluator.setMetricName("rmse").evaluate(predictions), 4),
        "mae_minutes": round(evaluator.setMetricName("mae").evaluate(predictions), 4),
    }


def write_json(results: dict[str, Any], output: Path) -> None:
    """Write stable, human-readable evaluation output."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")


def _feature_group_importance(model) -> pd.DataFrame:
    """Aggregate one-hot dimensions back to their original feature groups."""
    estimator = model.stages[-1]
    if not hasattr(estimator, "featureImportances"):
        return pd.DataFrame(columns=["feature", "importance"])

    encoder = next(stage for stage in model.stages if isinstance(stage, OneHotEncoderModel))
    widths = [
        size - 1 if encoder.getDropLast() else size
        for size in encoder.categorySizes
    ]
    importances = list(estimator.featureImportances)
    offset = 0
    grouped: dict[str, float] = {}
    for feature, width in zip(CATEGORICAL_FEATURES, widths):
        grouped[feature] = sum(importances[offset: offset + width])
        offset += width
    for feature in NUMERIC_FEATURES:
        grouped[feature] = importances[offset]
        offset += 1

    return (
        pd.DataFrame(grouped.items(), columns=["feature", "importance"])
        .sort_values("importance", ascending=False)
    )


def write_charts(
    results: dict[str, dict[str, float]],
    predictions: DataFrame,
    model,
    output_dir: Path,
) -> None:
    """Write comparison, diagnostic, and feature-importance charts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    result_frame = pd.DataFrame([
        {
            "model": name.replace("_", " ").title(),
            "rmse": values["test"]["rmse_minutes"],
            "mae": values["test"]["mae_minutes"],
        }
        for name, values in results.items()
    ])

    x = range(len(result_frame))
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar([value - 0.18 for value in x], result_frame["rmse"], 0.36, label="Test RMSE")
    ax.bar([value + 0.18 for value in x], result_frame["mae"], 0.36, label="Test MAE")
    ax.set_xticks(list(x), result_frame["model"])
    ax.set_ylabel("Error (minutes)")
    ax.set_title("Test-set model performance")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "model_performance.png", dpi=160)
    plt.close(fig)

    sample = (
        predictions.select(
            F.col(TARGET).alias("actual_duration"), F.col("prediction")
        )
        .sample(False, 0.01, seed=42)
        .limit(20_000)
        .toPandas()
    )
    if sample.empty:
        return
    limit = max(sample["actual_duration"].quantile(0.99), sample["prediction"].quantile(0.99))
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(sample["actual_duration"], sample["prediction"], alpha=0.12, s=8)
    ax.plot([0, limit], [0, limit], color="red", label="Perfect prediction")
    ax.set(xlim=(0, limit), ylim=(0, limit), title="Actual versus predicted trip duration")
    ax.set_xlabel("Actual duration (minutes)")
    ax.set_ylabel("Predicted duration (minutes)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "actual_vs_predicted.png", dpi=160)
    plt.close(fig)

    sample["residual_minutes"] = sample["prediction"] - sample["actual_duration"]
    low, high = sample["residual_minutes"].quantile([0.01, 0.99])
    clipped = sample["residual_minutes"].clip(low, high)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(clipped, bins=60, edgecolor="white")
    ax.axvline(0, color="red", linestyle="--", label="Zero error")
    ax.set_title("Residual distribution (central 98%)")
    ax.set_xlabel("Prediction error: predicted - actual (minutes)")
    ax.set_ylabel("Sampled trips")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "residual_distribution.png", dpi=160)
    plt.close(fig)

    importances = _feature_group_importance(model)
    if not importances.empty:
        plot = importances.head(12).sort_values("importance")
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.barh(plot["feature"], plot["importance"])
        ax.set_title("Random Forest feature importance")
        ax.set_xlabel("Relative importance")
        fig.tight_layout()
        fig.savefig(output_dir / "feature_importance.png", dpi=160)
        plt.close(fig)
