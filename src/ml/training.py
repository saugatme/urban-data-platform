"""Reusable training-data and Spark ML pipeline construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession, functions as F


INTEGRATED_TRIPS_PATH = "gold/integrated_taxi_trips"
TRAINING_DATASET_PATH = "gold/ml/trip_duration_training"
MODEL_ROOT = "models/week4"

TARGET = "trip_duration_minutes"
CATEGORICAL_FEATURES = ["pickup_location_id", "pickup_borough", "condition_code"]
NUMERIC_FEATURES = [
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "passenger_count",
    "temp",
    "rel_humidity",
    "precipitation",
    "wind_speed",
    "pm25_hourly_avg",
]


@dataclass(frozen=True)
class SplitFrames:
    """Chronological train, validation, and test data."""

    train: DataFrame
    validation: DataFrame
    test: DataFrame


def data_path(base: str, relative_path: str) -> str:
    """Return a Delta path while keeping the repository's configurable data root."""
    return f"{base.rstrip('/')}/{relative_path}"


def build_training_dataset(spark: SparkSession, base: str = "data") -> DataFrame:
    """Create the leakage-safe trip-duration dataset from integrated Gold data.

    Only values known at pickup time are retained.  The 2024 lower bound removes
    a small number of historical timestamp outliers present in the taxi source.
    """
    trips = spark.read.format("delta").load(data_path(base, INTEGRATED_TRIPS_PATH))

    return (
        trips.withColumn(
            TARGET,
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & (F.col("pickup_datetime") >= F.lit("2024-01-01").cast("timestamp"))
            & F.col(TARGET).between(1, 180)
            & F.col("pickup_location_id").isNotNull()
            & F.col("pickup_borough").isNotNull()
            & F.col("passenger_count").isNotNull()
        )
        .select(
            "pickup_datetime",
            TARGET,
            "pickup_location_id",
            "pickup_borough",
            "passenger_count",
            "temp",
            "rel_humidity",
            "precipitation",
            "wind_speed",
            "condition_code",
            "pm25_hourly_avg",
        )
    )


def chronological_split(dataset: DataFrame) -> SplitFrames:
    """Split data 70/15/15 by pickup time without future-information leakage."""
    cutoffs = (
        dataset.select(F.unix_timestamp("pickup_datetime").alias("pickup_seconds"))
        .agg(
            F.expr("percentile_approx(pickup_seconds, 0.70)").alias("train_cutoff"),
            F.expr("percentile_approx(pickup_seconds, 0.85)").alias("validation_cutoff"),
        )
        .first()
    )
    if cutoffs is None:
        raise ValueError("Cannot split an empty training dataset.")

    split = dataset.withColumn(
        "dataset_split",
        F.when(F.unix_timestamp("pickup_datetime") <= cutoffs["train_cutoff"], "train")
        .when(F.unix_timestamp("pickup_datetime") <= cutoffs["validation_cutoff"], "validation")
        .otherwise("test"),
    )
    return SplitFrames(
        train=split.filter(F.col("dataset_split") == "train"),
        validation=split.filter(F.col("dataset_split") == "validation"),
        test=split.filter(F.col("dataset_split") == "test"),
    )


def split_summary(frames: SplitFrames) -> DataFrame:
    """Return comparable split sizes, time boundaries, and target averages."""
    labelled = [
        frame.withColumn("dataset_split", F.lit(name))
        for name, frame in (
            ("train", frames.train),
            ("validation", frames.validation),
            ("test", frames.test),
        )
    ]
    return labelled[0].unionByName(labelled[1]).unionByName(labelled[2]).groupBy(
        "dataset_split"
    ).agg(
        F.count("*").alias("rows"),
        F.min("pickup_datetime").alias("start"),
        F.max("pickup_datetime").alias("end"),
        F.round(F.avg(TARGET), 2).alias("average_duration_minutes"),
    )


def write_training_dataset(frames: SplitFrames, base: str = "data") -> str:
    """Persist the reproducible Task 1 dataset as a partitioned Delta table."""
    # Keep the split label explicit so each Delta output partition is stable.
    frames_with_split = [
        frame.withColumn("dataset_split", F.lit(name))
        for name, frame in (
            ("train", frames.train),
            ("validation", frames.validation),
            ("test", frames.test),
        )
    ]
    output = data_path(base, TRAINING_DATASET_PATH)
    (
        frames_with_split[0]
        .unionByName(frames_with_split[1])
        .unionByName(frames_with_split[2])
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("dataset_split")
        .save(output)
    )
    return output


def build_feature_pipeline(model: Literal["linear", "random_forest"] = "linear") -> Pipeline:
    """Build the reusable feature transformations and selected regression model."""
    temporal_features = SQLTransformer(
        statement="""
            SELECT *,
                   HOUR(pickup_datetime) AS pickup_hour,
                   DAYOFWEEK(pickup_datetime) AS pickup_day_of_week,
                   MONTH(pickup_datetime) AS pickup_month
            FROM __THIS__
        """
    )
    imputer = Imputer(
        inputCols=NUMERIC_FEATURES,
        outputCols=[f"{column}_imputed" for column in NUMERIC_FEATURES],
        strategy="median",
    )
    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=f"{column}_index",
            handleInvalid="keep",
        )
        for column in CATEGORICAL_FEATURES
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{column}_index" for column in CATEGORICAL_FEATURES],
        outputCols=[f"{column}_encoded" for column in CATEGORICAL_FEATURES],
        handleInvalid="keep",
    )
    numeric_assembler = VectorAssembler(
        inputCols=[f"{column}_imputed" for column in NUMERIC_FEATURES],
        outputCol="numeric_features",
    )
    scaler = StandardScaler(
        inputCol="numeric_features",
        outputCol="scaled_numeric_features",
        withMean=False,
        withStd=True,
    )
    assembler = VectorAssembler(
        inputCols=[
            *[f"{column}_encoded" for column in CATEGORICAL_FEATURES],
            "scaled_numeric_features",
        ],
        outputCol="features",
    )

    if model == "linear":
        estimator = LinearRegression(
            featuresCol="features",
            labelCol=TARGET,
            predictionCol="prediction",
            maxIter=50,
            regParam=0.1,
        )
    elif model == "random_forest":
        estimator = RandomForestRegressor(
            featuresCol="features",
            labelCol=TARGET,
            predictionCol="prediction",
            # Conservative defaults keep a local 6+ million-row run within a
            # typical student machine's heap. They remain a nonlinear ensemble.
            numTrees=20,
            maxDepth=7,
            maxBins=64,
            subsamplingRate=0.5,
            seed=42,
        )
    else:
        raise ValueError(f"Unsupported model: {model}")

    return Pipeline(stages=[
        temporal_features,
        imputer,
        *indexers,
        encoder,
        numeric_assembler,
        scaler,
        assembler,
        estimator,
    ])
