# Task 2 — Reusable Feature Engineering Pipeline

The reusable feature pipeline is built by `build_feature_pipeline()` in `src/ml/training.py`.

| Stage | Transformation | Purpose |
|---|---|---|
| `SQLTransformer` | derives pickup hour, weekday, and month | Creates temporal features consistently for every run. |
| `Imputer` | training-median replacement for numeric nulls | Retains trips with missing hourly weather or PM2.5 context. |
| `StringIndexer` | indexes location, borough, and weather code | Converts categories to Spark ML inputs. |
| `OneHotEncoder` | sparse categorical vectors with unseen-value handling | Prevents an artificial numeric ordering of categories. |
| `VectorAssembler` | assembles numeric and categorical values | Produces the model input vector. |
| `StandardScaler` | scales numeric feature vector | Supports the Linear Regression baseline. |

The output is a 292-dimensional sparse `features` vector. The final model input retains only the target, feature vector, and required metadata; intermediate raw and transformed columns are not used by the estimator.

Weather fields have 1.14% missing values and hourly PM2.5 has 1.75%. Imputation values are fitted only on the training split, then applied unchanged to validation and test data. This avoids data leakage.

Feature lists are maintained as `CATEGORICAL_FEATURES` and `NUMERIC_FEATURES`. Adding a compatible integrated field usually requires only adding its name to one list; the generic pipeline stages do the rest.
