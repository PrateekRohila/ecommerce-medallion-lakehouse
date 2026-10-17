"""Silver -> Gold: dimensional model and business KPIs."""
from __future__ import annotations

from datetime import date
from itertools import chain
from typing import Iterable, Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LOW_DATE = "1900-01-01 00:00:00"
UNKNOWN_SK = -1
MONEY = "DECIMAL(18,2)"

CUSTOMER_TRACKED_COLS = ["full_name", "email", "city", "country", "tier"]


def _fx_map(fx_rates_to_inr: Mapping[str, float]):
    return F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in fx_rates_to_inr.items()))


def build_fact_orders(
    orders: DataFrame,
    products: DataFrame,
    dim_customer: DataFrame,
    *,
    fx_rates_to_inr: Mapping[str, float],
    cancelled_statuses: Iterable[str],
) -> DataFrame:
    """One row per order, enriched with product data and the customer version
    that was valid *when the order was placed* (point-in-time / as-of join)."""
    # A customer's first CRM record often lands after their first order. Open the
    # earliest version from LOW_DATE so those orders still find their customer.
    w_first = Window.partitionBy("customer_id")
    dim = (dim_customer
           .withColumn("_join_from",
                       F.when(F.col("effective_from") == F.min("effective_from").over(w_first),
                              F.to_timestamp(F.lit(LOW_DATE)))
                        .otherwise(F.col("effective_from")))
           .select(F.col("customer_id").alias("d_customer_id"), "customer_sk", "_join_from", "effective_to",
                   F.col("tier").alias("customer_tier_at_order"),
                   F.col("country").alias("customer_country")))

    prod = products.select("product_id", "product_name", "category")

    amount_inr = (F.col("quantity") * F.col("unit_price") * _fx_map(fx_rates_to_inr)[F.col("currency")]).cast(MONEY)
    is_cancelled = F.col("status").isin(list(cancelled_statuses))

    return (orders.alias("o")
            .join(F.broadcast(prod), "product_id", "left")
            .join(dim,
                  (F.col("o.customer_id") == F.col("d_customer_id"))
                  & (F.col("o.order_ts") >= F.col("_join_from"))
                  & (F.col("o.order_ts") < F.col("effective_to")),
                  "left")
            .select(
                "order_id", "order_date", "order_ts", F.col("o.customer_id").alias("customer_id"),
                F.coalesce("customer_sk", F.lit(UNKNOWN_SK).cast("bigint")).alias("customer_sk"),
                F.coalesce("customer_tier_at_order", F.lit("UNKNOWN")).alias("customer_tier_at_order"),
                F.coalesce("customer_country", F.lit("UNKNOWN")).alias("customer_country"),
                "product_id",
                F.coalesce("product_name", F.lit("UNKNOWN")).alias("product_name"),
                F.coalesce("category", F.lit("Unknown")).alias("category"),
                "quantity", "unit_price", "currency", "status",
                amount_inr.alias("gross_amount_inr"),
                is_cancelled.alias("is_cancelled"),
                F.when(is_cancelled, F.lit(0).cast(MONEY)).otherwise(amount_inr).alias("net_revenue_inr"),
                "updated_at",
            ))


def build_daily_sales_kpis(fact_orders: DataFrame) -> DataFrame:
    active = ~F.col("is_cancelled")
    agg = (fact_orders
           .groupBy("order_date", "customer_country", "category")
           .agg(F.countDistinct("order_id").alias("total_orders"),
                F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_orders"),
                F.sum(F.when(active, F.col("quantity")).otherwise(0)).alias("units_sold"),
                F.sum("net_revenue_inr").cast(MONEY).alias("net_revenue_inr"),
                F.countDistinct("customer_id").alias("unique_customers")))
    completed = F.col("total_orders") - F.col("cancelled_orders")
    return (agg
            .withColumn("avg_order_value_inr",
                        F.when(completed > 0, F.col("net_revenue_inr") / completed).cast(MONEY))
            .withColumn("cancellation_rate", F.round(F.col("cancelled_orders") / F.col("total_orders"), 4)))


def build_customer_lifetime_metrics(
    fact_orders: DataFrame,
    dim_customer: DataFrame,
    *,
    as_of: date,
    segment_thresholds_inr: Mapping[str, float],
) -> DataFrame:
    current = dim_customer.filter("is_current").select("customer_id", "full_name", F.col("tier").alias("current_tier"))
    high = segment_thresholds_inr.get("HIGH", 50_000)
    medium = segment_thresholds_inr.get("MEDIUM", 10_000)

    metrics = (fact_orders
               .groupBy("customer_id")
               .agg(F.min("order_date").alias("first_order_date"),
                    F.max("order_date").alias("last_order_date"),
                    F.countDistinct("order_id").alias("total_orders"),
                    F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_orders"),
                    F.sum("net_revenue_inr").cast(MONEY).alias("lifetime_value_inr")))

    return (metrics
            .join(current, "customer_id", "left")
            .withColumn("recency_days", F.datediff(F.lit(as_of), F.col("last_order_date")))
            .withColumn("value_segment",
                        F.when(F.col("lifetime_value_inr") >= high, "HIGH")
                         .when(F.col("lifetime_value_inr") >= medium, "MEDIUM")
                         .otherwise("LOW"))
            .withColumn("ltv_rank", F.dense_rank().over(Window.orderBy(F.col("lifetime_value_inr").desc())))
            .withColumn("as_of_date", F.lit(as_of)))
