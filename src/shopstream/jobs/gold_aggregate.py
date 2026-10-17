"""Silver -> Gold: SCD2 customer dimension, order fact, KPI marts.

Incremental by design: only order dates touched by this run are recomputed,
and those partitions are replaced atomically.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from shopstream.jobs.common import JobContext
from shopstream.logging_utils import kv
from shopstream.transformations.gold import (CUSTOMER_TRACKED_COLS, build_customer_lifetime_metrics,
                                             build_daily_sales_kpis, build_fact_orders)
from shopstream.transformations.scd2 import build_scd2

JOB = "gold_aggregate"


def _refresh_dim_customer(ctx: JobContext) -> list[str]:
    """Apply this run's customer changes to the SCD2 dimension. Returns touched customer ids."""
    changes_all = ctx.store.read(ctx.cfg.path("silver", "customer_changes"))
    dim_path = ctx.cfg.path("gold", "dim_customer")

    if ctx.store.exists(dim_path):
        existing = ctx.store.read(dim_path)
        changes = changes_all.filter(F.col("_processed_run_date") == F.lit(ctx.run_date))
    else:  # first run: bootstrap from full history
        existing, changes = None, changes_all

    touched = [r.customer_id for r in changes.select("customer_id").distinct().collect()]
    if not touched:
        ctx.audit(JOB, "dim_customer", "SKIPPED", details="no customer changes")
        return []

    dim = build_scd2(changes, existing, key="customer_id", tracked_cols=CUSTOMER_TRACKED_COLS,
                     effective_col="updated_at", surrogate_key="customer_sk")
    ctx.store.overwrite(dim, dim_path)
    ctx.audit(JOB, "dim_customer", rows_read=len(touched), rows_written=ctx.store.read(dim_path).count(),
              details=f"customers_touched={len(touched)}")
    return touched


def run(ctx: JobContext) -> None:
    cfg, store = ctx.cfg, ctx.store
    needed = [cfg.path("silver", t) for t in ("orders", "customer_changes", "products")]
    if not all(store.exists(p) for p in needed):
        ctx.audit(JOB, "all", "SKIPPED", details="silver tables not ready")
        return

    touched_customers = _refresh_dim_customer(ctx)
    dim = store.read(cfg.path("gold", "dim_customer"))
    orders = store.read(cfg.path("silver", "orders"))
    products = store.read(cfg.path("silver", "products"))
    fact_path = cfg.path("gold", "fact_orders")

    # Which order_date partitions must be rebuilt?
    #  - dates of orders inserted/updated in this run
    #  - all dates of customers whose history changed (a late CRM change can
    #    change the tier that was valid at order time)
    if store.exists(fact_path):
        changed = orders.filter(F.col("_processed_run_date") == F.lit(ctx.run_date))
        if touched_customers:
            changed = changed.unionByName(orders.filter(F.col("customer_id").isin(touched_customers)))
    else:
        changed = orders
    affected_dates = sorted(r.order_date for r in changed.select("order_date").distinct().collect())
    if not affected_dates:
        ctx.audit(JOB, "fact_orders", "SKIPPED", details="no affected order dates")
        return
    ctx.log.info(kv(event="gold_affected_dates", count=len(affected_dates),
                    first=affected_dates[0], last=affected_dates[-1]))

    fact = build_fact_orders(orders.filter(F.col("order_date").isin(affected_dates)), products, dim,
                             fx_rates_to_inr=cfg.fx_rates_to_inr, cancelled_statuses=cfg.cancelled_statuses)
    store.overwrite_partitions(fact, fact_path, "order_date", affected_dates)
    fact_all = store.read(fact_path)
    ctx.audit(JOB, "fact_orders", rows_written=fact_all.filter(F.col("order_date").isin(affected_dates)).count(),
              details=f"partitions_rebuilt={len(affected_dates)}")

    kpis = build_daily_sales_kpis(fact_all.filter(F.col("order_date").isin(affected_dates)))
    store.overwrite_partitions(kpis, cfg.path("gold", "daily_sales_kpis"), "order_date", affected_dates)
    ctx.audit(JOB, "daily_sales_kpis", rows_written=kpis.count())

    clv = build_customer_lifetime_metrics(fact_all, dim, as_of=ctx.run_date,
                                          segment_thresholds_inr=cfg.segment_thresholds_inr)
    store.overwrite(clv, cfg.path("gold", "customer_lifetime_metrics"))
    ctx.audit(JOB, "customer_lifetime_metrics", rows_written=clv.count())
