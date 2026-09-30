"""Generate Week 4 training data, train models, evaluate, and save artifacts."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.analytics.runtime import configure_spark_temp_dir
from src.common.spark_session import get_spark
from src.ml.evaluation import regression_metrics, write_charts, write_json
from src.ml.training import (
    MODEL_ROOT,
    build_feature_pipeline,
    build_training_dataset,
    chronological_split,
    data_path,
    split_summary,
    write_training_dataset,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="data", help="Data root containing Gold Delta tables.")
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark master. The default limits local concurrency to control heap usage.",
    )
    parser.add_argument(
        "--driver-memory",
        default="4g",
        help="JVM driver heap, passed to Spark before startup (for example, 4g).",
    )
    parser.add_argument(
        "--model",
        choices=("linear", "random_forest", "both"),
        default="both",
        help="Model or models to train.",
    )
    parser.add_argument(
        "--skip-training-write",
        action="store_true",
        help="Do not overwrite the generated training Delta dataset.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_spark_temp_dir()
    spark = get_spark(
        "week4-ml-training",
        master=args.master,
        extra_configs={
            "spark.driver.memory": args.driver_memory,
            "spark.executor.memory": args.driver_memory,
            "spark.default.parallelism": "4",
        },
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        dataset = build_training_dataset(spark, args.base)
        frames = chronological_split(dataset)
        split_summary(frames).orderBy("dataset_split").show(truncate=False)

        if not args.skip_training_write:
            print(f"Writing training dataset to {write_training_dataset(frames, args.base)}")

        requested_models = (
            ("linear", "random_forest") if args.model == "both" else (args.model,)
        )
        results: dict[str, dict] = {}
        saved_models: dict[str, str] = {}
        best_predictions = None
        best_model = None
        best_name = None

        for name in requested_models:
            started = time.perf_counter()
            fitted = build_feature_pipeline(name).fit(frames.train)
            training_seconds = round(time.perf_counter() - started, 3)

            validation_predictions = fitted.transform(frames.validation)
            test_predictions = fitted.transform(frames.test)
            result = {
                "training_seconds": training_seconds,
                "validation": regression_metrics(validation_predictions),
                "test": regression_metrics(test_predictions),
            }
            results[name] = result

            model_path = data_path(args.base, f"{MODEL_ROOT}/trip_duration_{name}_pipeline")
            fitted.write().overwrite().save(model_path)
            saved_models[name] = model_path
            print(f"{name}: {result}")

            if (
                best_predictions is None
                or result["test"]["mae_minutes"] < results[best_name]["test"]["mae_minutes"]
            ):
                best_predictions = test_predictions
                best_model = fitted
                best_name = name

        output_root = Path(args.base) / "benchmark" / "week4_ml"
        report = {
            "prediction_problem": "trip_duration_minutes",
            "splits": {row["dataset_split"]: row.asDict() for row in split_summary(frames).collect()},
            "models": results,
            "saved_models": saved_models,
            "best_model_by_test_mae": best_name,
        }
        write_json(report, output_root / "evaluation.json")
        if best_predictions is not None and best_model is not None:
            write_charts(results, best_predictions, best_model, output_root / "charts")
        print(f"Evaluation written to {output_root / 'evaluation.json'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
