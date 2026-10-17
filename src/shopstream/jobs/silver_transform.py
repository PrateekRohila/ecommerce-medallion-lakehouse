"""Bronze -> Silver: type, validate, quarantine, de-duplicate and upsert."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shopstream.jobs.common import JobContext
from shopstream.logging_utils import kv
from shopstream.quality import (DQ_FAILURES_COL, DataQualityError, DQRule, enforce_reject_threshold,
                                flag_failures)
from shopstream.schemas import CORRUPT_RECORD_COL
from shopstream.transformations import silver as T

JOB = "silver_transform"


@dataclass(frozen=True)
class EntitySpec:
    bronze_table: str
    silver_table: str
    standardize: Callable[[DataFrame], DataFrame]
    rules: Callable[[JobContext], list[DQRule]]
    dedupe_keys: tuple[str, ...]
    dedupe_order: tuple[str, ...]
    version_col: str


ENTITIES = (
    EntitySpec("orders", "orders", T.standardize_orders,
               lambda ctx: T.order_rules(ctx.cfg.valid_order_statuses, ctx.cfg.fx_rates_to_inr.keys()),
               dedupe_keys=("order_id",), dedupe_order=("updated_at", "_ingest_ts"),
               version_col="updated_at"),
    EntitySpec("customers", "customer_changes", T.standardize_customers,
               lambda ctx: T.customer_rules(ctx.cfg.valid_customer_tiers),
               dedupe_keys=("customer_id", "updated_at"), dedupe_order=("_ingest_ts",),
               version_col="_ingest_ts"),
    EntitySpec("products", "products", T.standardize_products,
               lambda ctx: T.product_rules(),
               dedupe_keys=("product_id",), dedupe_order=("_ingest_ts",),
               version_col="_ingest_ts"),
)


def _process(ctx: JobContext, spec: EntitySpec) -> None:
    bronze_path = ctx.cfg.path("bronze", spec.bronze_table)
    if not ctx.store.exists(bronze_path):
        ctx.audit(JOB, spec.silver_table, "SKIPPED", details="bronze table missing")
        return

    batch = ctx.store.read(bronze_path).filter(F.col("ingest_date") == F.lit(ctx.run_date))
    flagged = flag_failures(spec.standardize(batch), spec.rules(ctx)).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        total = flagged.count()
        if total == 0:
            ctx.audit(JOB, spec.silver_table, "SKIPPED", details="no rows for run date")
            return

        rejected = (flagged.filter(F.size(DQ_FAILURES_COL) > 0)
                    .withColumn("_rejected_at", F.current_timestamp())
                    .withColumn("ingest_date", F.lit(ctx.run_date)))
        n_rejected = rejected.count()
        if n_rejected:
            ctx.store.overwrite_partitions(rejected, ctx.cfg.path("quarantine", spec.silver_table),
                                           "ingest_date", [ctx.run_date])
            top = (rejected.select(F.explode(DQ_FAILURES_COL).alias("rule")).groupBy("rule").count()
                   .orderBy(F.desc("count")).limit(5).collect())
            ctx.log.warning(kv(event="rows_quarantined", entity=spec.silver_table, rows=n_rejected,
                               top_rules=";".join(f"{r['rule']}:{r['count']}" for r in top)))

        try:
            enforce_reject_threshold(spec.silver_table, total, n_rejected, ctx.cfg.max_reject_ratio)
        except DataQualityError as exc:
            ctx.audit(JOB, spec.silver_table, "FAILED", rows_read=total, rows_rejected=n_rejected, details=str(exc))
            raise

        valid = (flagged.filter(F.size(DQ_FAILURES_COL) == 0)
                 .drop(DQ_FAILURES_COL, CORRUPT_RECORD_COL)
                 .withColumn("_processed_run_date", F.lit(ctx.run_date)))
        deduped = T.dedupe_latest(valid, spec.dedupe_keys, spec.dedupe_order)
        n_written = deduped.count()

        ctx.store.upsert(deduped, ctx.cfg.path("silver", spec.silver_table),
                         keys=spec.dedupe_keys, version_col=spec.version_col)
        ctx.audit(JOB, spec.silver_table, rows_read=total, rows_written=n_written, rows_rejected=n_rejected,
                  details=f"duplicates_removed={total - n_rejected - n_written}")
    finally:
        flagged.unpersist()


def run(ctx: JobContext) -> None:
    for spec in ENTITIES:
        _process(ctx, spec)
