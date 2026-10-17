"""Declarative data-quality rules with quarantine routing.

Each rule is a SQL boolean expression that is TRUE when a row is valid. A row
that fails any rule is sent to quarantine together with the names of every rule
it broke, so the source team can fix the feed instead of guessing.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DQ_FAILURES_COL = "_dq_failures"


class DataQualityError(RuntimeError):
    """Raised when a batch breaches the configured reject threshold."""


@dataclass(frozen=True)
class DQRule:
    name: str
    expression: str  # SQL expression, TRUE == valid

    def failed(self) -> Column:
        # NULL results (e.g. comparison on a null) count as failures.
        return ~F.coalesce(F.expr(self.expression), F.lit(False))


def flag_failures(df: DataFrame, rules: list[DQRule]) -> DataFrame:
    """Add an array column listing the names of the rules each row failed."""
    if not rules:
        return df.withColumn(DQ_FAILURES_COL, F.array().cast("array<string>"))
    checks = [F.when(rule.failed(), F.lit(rule.name)) for rule in rules]
    return df.withColumn(DQ_FAILURES_COL, F.filter(F.array(*checks), lambda x: x.isNotNull()))


def split_valid_invalid(df: DataFrame, rules: list[DQRule]) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows without the DQ column, rejected rows with reasons)."""
    flagged = flag_failures(df, rules)
    valid = flagged.filter(F.size(DQ_FAILURES_COL) == 0).drop(DQ_FAILURES_COL)
    rejected = flagged.filter(F.size(DQ_FAILURES_COL) > 0).withColumn("_rejected_at", F.current_timestamp())
    return valid, rejected


def enforce_reject_threshold(entity: str, total: int, rejected: int, max_ratio: float) -> float:
    """Circuit breaker: stop the pipeline if a feed is mostly garbage."""
    ratio = (rejected / total) if total else 0.0
    if ratio > max_ratio:
        raise DataQualityError(
            f"{entity}: rejected {rejected}/{total} rows ({ratio:.1%}) exceeds the limit of {max_ratio:.1%}. "
            "Inspect the quarantine table before re-running."
        )
    return ratio
