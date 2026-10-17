"""Bronze -> Silver: standardise, type, validate and de-duplicate."""
from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from shopstream.quality import DQRule
from shopstream.schemas import CORRUPT_RECORD_COL

LINEAGE_COLS = ["_source_file", "_ingest_ts", "_run_id"]


# --------------------------------------------------------------------- helpers
def _str(col: str) -> Column:
    """Trim and turn empty strings into NULL."""
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _try_cast(col: str, sql_type: str) -> Column:
    """Cast that returns NULL instead of failing the job (ANSI-mode safe)."""
    return F.expr(f"try_cast(trim(`{col}`) AS {sql_type})")


def _sql_in(values: Iterable[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def dedupe_latest(df: DataFrame, keys: Sequence[str], order_by: Sequence[str]) -> DataFrame:
    """Keep one row per key: the one with the highest value of `order_by` (in order)."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------- orders
def standardize_orders(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(_str("order_id")).alias("order_id"),
        F.upper(_str("customer_id")).alias("customer_id"),
        F.upper(_str("product_id")).alias("product_id"),
        _try_cast("quantity", "INT").alias("quantity"),
        _try_cast("unit_price", "DECIMAL(12,2)").alias("unit_price"),
        F.upper(_str("currency")).alias("currency"),
        F.upper(_str("status")).alias("status"),
        _try_cast("order_ts", "TIMESTAMP").alias("order_ts"),
        _try_cast("updated_at", "TIMESTAMP").alias("updated_at"),
        F.col(CORRUPT_RECORD_COL),
        *LINEAGE_COLS,
    ).withColumn("order_date", F.to_date("order_ts"))


def order_rules(valid_statuses: Iterable[str], supported_currencies: Iterable[str]) -> list[DQRule]:
    return [
        DQRule("record_is_parseable", f"{CORRUPT_RECORD_COL} IS NULL"),
        DQRule("order_id_present", "order_id IS NOT NULL"),
        DQRule("customer_id_present", "customer_id IS NOT NULL"),
        DQRule("product_id_present", "product_id IS NOT NULL"),
        DQRule("quantity_positive", "quantity > 0"),
        DQRule("unit_price_non_negative", "unit_price >= 0"),
        DQRule("order_ts_valid", "order_ts IS NOT NULL"),
        DQRule("updated_at_valid", "updated_at IS NOT NULL AND updated_at >= order_ts"),
        DQRule("status_allowed", f"status IN ({_sql_in(valid_statuses)})"),
        DQRule("currency_supported", f"currency IN ({_sql_in(supported_currencies)})"),
    ]


# ------------------------------------------------------------------- customers
def standardize_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(_str("customer_id")).alias("customer_id"),
        F.initcap(_str("full_name")).alias("full_name"),
        F.lower(_str("email")).alias("email"),
        F.initcap(_str("city")).alias("city"),
        F.upper(_str("country")).alias("country"),
        F.upper(_str("tier")).alias("tier"),
        _try_cast("updated_at", "TIMESTAMP").alias("updated_at"),
        F.col(CORRUPT_RECORD_COL),
        *LINEAGE_COLS,
    )


def customer_rules(valid_tiers: Iterable[str]) -> list[DQRule]:
    return [
        DQRule("record_is_parseable", f"{CORRUPT_RECORD_COL} IS NULL"),
        DQRule("customer_id_present", "customer_id IS NOT NULL"),
        DQRule("updated_at_valid", "updated_at IS NOT NULL"),
        DQRule("email_format", r"email RLIKE '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'"),
        DQRule("tier_allowed", f"tier IN ({_sql_in(valid_tiers)})"),
        DQRule("country_iso2", "length(country) = 2"),
    ]


# -------------------------------------------------------------------- products
def standardize_products(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(_str("product_id")).alias("product_id"),
        _str("product_name").alias("product_name"),
        F.initcap(_str("category")).alias("category"),
        _try_cast("list_price", "DECIMAL(12,2)").alias("list_price"),
        F.col(CORRUPT_RECORD_COL),
        *LINEAGE_COLS,
    )


def product_rules() -> list[DQRule]:
    return [
        DQRule("record_is_parseable", f"{CORRUPT_RECORD_COL} IS NULL"),
        DQRule("product_id_present", "product_id IS NOT NULL"),
        DQRule("product_name_present", "product_name IS NOT NULL"),
        DQRule("list_price_non_negative", "list_price >= 0"),
    ]
