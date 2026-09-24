"""Pluggable validation rules shared by the ingestion and incremental pipelines.

A rule is data, not control flow: it names a rejection reason and supplies a
condition that marks rows to reject.  Adding a rule means appending a
``ValidationRule`` to a dataset's ``validation_rules`` list in
:mod:`src.ingestion.config`; the rule loop below never changes.

Rules never stop a pipeline.  Offending rows are moved to the rejected set with
a specific reason and the run continues.
"""

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, lit


@dataclass(frozen=True)
class ValidationRule:
    """One named rejection rule.

    ``condition`` receives the DataFrame and a reference context and returns a
    Column that is true for rows that must be **rejected**.  ``requires`` names
    the reference datasets the rule needs loaded into that context.
    """

    reason: str
    condition: Callable[[DataFrame, dict], Column]
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchemaContract:
    """The schema evolution a dataset is permitted to undergo.

    ``allowed_new`` lists documented additions (Week 3 adds ``humidity`` to
    weather and ``aqi`` to air quality).  The baseline is not hardcoded: it is
    read from the table already on disk, so drift is always measured against
    what the platform actually stores.
    """

    allowed_new: tuple[str, ...] = ()


def build_context(spark: SparkSession, requirements: set[str], base: str = "data") -> dict:
    """Load the reference data the requested rules need.

    Only the references actually required are read, so a dataset with no
    referential rules costs nothing extra.
    """
    context: dict = {}

    if "taxi_zone_ids" in requirements:
        zones = spark.read.format("delta").load(f"{base}/silver/taxi_zones")
        context["taxi_zone_ids"] = [
            row["location_id"] for row in zones.select("location_id").distinct().collect()
        ]

    return context


def apply_rules(
    df: DataFrame,
    rules: list[ValidationRule],
    context: dict,
) -> tuple[DataFrame, list[DataFrame]]:
    """Split ``df`` into surviving rows and one rejected frame per failed rule.

    Rules are applied in order and each row is attributed to the first rule it
    fails, so a row is never counted twice.
    """
    rejected_parts: list[DataFrame] = []

    for rule in rules:
        missing = [name for name in rule.requires if name not in context]
        if missing:
            # A rule whose reference data is unavailable is skipped loudly
            # rather than silently passing every row.
            print(f"  ! skipping rule '{rule.reason}': missing context {missing}")
            continue

        condition = rule.condition(df, context)
        failed = df.filter(condition)
        df = df.filter(~condition | condition.isNull())
        rejected_parts.append(failed.withColumn("rejection_reason", lit(rule.reason)))

    return df, rejected_parts


def check_schema_drift(
    df: DataFrame,
    baseline_columns: list[str],
    contract: SchemaContract | None,
) -> dict:
    """Compare an incoming frame against the columns already stored.

    ``evolved`` are documented additions, ``unexpected`` are undocumented ones,
    and ``missing`` are baseline columns the update dropped.  Returns a summary
    rather than raising, so the run continues and the drift is recorded.
    """
    allowed = set(contract.allowed_new) if contract else set()
    actual = set(df.columns)
    baseline = set(baseline_columns)

    added = actual - baseline
    return {
        "unexpected": sorted(added - allowed),
        "missing": sorted(baseline - actual),
        "evolved": sorted(added & allowed),
    }


# --- Reusable rule builders -------------------------------------------------
# These return ValidationRule instances so config.py stays declarative.


def incomplete_record(columns: list[str]) -> ValidationRule:
    """Reject rows where any of ``columns`` is null."""

    def condition(df: DataFrame, _context: dict) -> Column:
        present = [name for name in columns if name in df.columns]
        if not present:
            return lit(False)
        expression = col(present[0]).isNull()
        for name in present[1:]:
            expression = expression | col(name).isNull()
        return expression

    return ValidationRule(reason="incomplete_record", condition=condition)


def value_out_of_range(column: str, low: float, high: float) -> ValidationRule:
    """Reject rows whose ``column`` falls outside ``[low, high]``."""

    def condition(df: DataFrame, _context: dict) -> Column:
        if column not in df.columns:
            return lit(False)
        return col(column).isNotNull() & (~col(column).between(low, high))

    return ValidationRule(reason=f"{column}_out_of_range", condition=condition)


def missing_reference(columns: list[str], reference: str) -> ValidationRule:
    """Reject rows whose ``columns`` hold an id absent from ``reference``.

    The reference id set is small (265 taxi zones), so it is collected into the
    context once and used as an ``isin`` predicate instead of a join.
    """

    def condition(df: DataFrame, context: dict) -> Column:
        allowed = context[reference]
        present = [name for name in columns if name in df.columns]
        if not present or not allowed:
            return lit(False)
        expression = col(present[0]).isNotNull() & (~col(present[0]).isin(allowed))
        for name in present[1:]:
            expression = expression | (col(name).isNotNull() & (~col(name).isin(allowed)))
        return expression

    return ValidationRule(
        reason=f"missing_reference_{reference}",
        condition=condition,
        requires=(reference,),
    )
