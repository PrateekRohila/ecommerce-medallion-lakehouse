"""Source contracts for the landing zone.

Bronze reads every column as STRING on purpose: the raw layer must never drop or
silently null-out a value because of a bad type. Typing happens in silver, where
failed casts are caught by data-quality rules and routed to quarantine.
"""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

CORRUPT_RECORD_COL = "_corrupt_record"


def _raw_schema(*columns: str) -> StructType:
    fields = [StructField(name, StringType(), True) for name in columns]
    fields.append(StructField(CORRUPT_RECORD_COL, StringType(), True))
    return StructType(fields)


ORDERS_RAW = _raw_schema(
    "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "currency", "status", "order_ts", "updated_at",
)

CUSTOMERS_RAW = _raw_schema(
    "customer_id", "full_name", "email", "city", "country", "tier", "updated_at",
)

PRODUCTS_RAW = _raw_schema(
    "product_id", "product_name", "category", "list_price",
)

# entity -> (file format, schema). Adding a new feed = adding a line here.
SOURCES: dict[str, tuple[str, StructType]] = {
    "orders": ("json", ORDERS_RAW),
    "customers": ("csv", CUSTOMERS_RAW),
    "products": ("csv", PRODUCTS_RAW),
}
