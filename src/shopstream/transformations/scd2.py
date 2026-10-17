"""Slowly Changing Dimension Type 2, rebuilt per touched key.

Why not the classic "close the current row, insert a new one" MERGE?
Because CDC feeds deliver changes late and out of order. If a change dated
Tuesday arrives after Wednesday's change was loaded, the classic approach makes
Tuesday's version "current" and corrupts history.

This implementation instead, for every key that received changes:
  1. unions the key's existing versions with the incoming ones,
  2. orders them by business effective time,
  3. drops "changes" that did not change any tracked attribute,
  4. recomputes effective_to / is_current with LEAD().
Keys with no incoming changes are passed through untouched, so cost scales with
the size of the change set, not the dimension.
"""
from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31 00:00:00"
SCD_COLS = ["effective_from", "effective_to", "is_current"]


def _row_hash(cols: Sequence[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def build_scd2(
    changes: DataFrame,
    existing: DataFrame | None,
    *,
    key: str,
    tracked_cols: Sequence[str],
    effective_col: str = "updated_at",
    surrogate_key: str = "sk",
) -> DataFrame:
    incoming = changes.select(
        key, *tracked_cols, F.col(effective_col).alias("effective_from"), F.lit(1).alias("_priority")
    )

    if existing is not None:
        touched_keys = incoming.select(key).distinct()
        untouched = existing.join(F.broadcast(touched_keys), key, "left_anti")
        history = (existing.join(F.broadcast(touched_keys), key, "left_semi")
                   .select(key, *tracked_cols, "effective_from", F.lit(0).alias("_priority")))
        versions = history.unionByName(incoming)
    else:
        untouched, versions = None, incoming

    # Same key + same effective time: the incoming record wins (it is a correction).
    w_dup = Window.partitionBy(key, "effective_from").orderBy(F.col("_priority").desc())
    versions = versions.withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn", "_priority")

    # Collapse consecutive versions whose tracked attributes are identical.
    w_time = Window.partitionBy(key).orderBy("effective_from")
    versions = (versions
                .withColumn("_hash", _row_hash(tracked_cols))
                .withColumn("_prev_hash", F.lag("_hash").over(w_time))
                .filter(F.col("_prev_hash").isNull() | (F.col("_hash") != F.col("_prev_hash")))
                .drop("_prev_hash"))

    next_from = F.lead("effective_from").over(w_time)
    rebuilt = (versions
               .withColumn("effective_to", F.coalesce(next_from, F.to_timestamp(F.lit(HIGH_DATE))))
               .withColumn("is_current", next_from.isNull())
               .withColumn(surrogate_key, F.xxhash64(F.col(key), F.col("effective_from")))
               .drop("_hash")
               .select(surrogate_key, key, *tracked_cols, *SCD_COLS))

    return rebuilt if untouched is None else untouched.select(rebuilt.columns).unionByName(rebuilt)
