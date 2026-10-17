from datetime import date
from datetime import datetime as dt
from decimal import Decimal

from shopstream.storage import LakeStore
from shopstream.transformations.gold import build_daily_sales_kpis, build_fact_orders
from shopstream.transformations.scd2 import build_scd2

ORDERS = ("order_id string, customer_id string, product_id string, quantity int, unit_price decimal(12,2), "
          "currency string, status string, order_ts timestamp, updated_at timestamp, order_date date")


def _fixtures(spark):
    dim = build_scd2(
        spark.createDataFrame(
            [("C1", "A", "a@x.com", "Delhi", "IN", "SILVER", dt(2026, 10, 13)),
             ("C1", "A", "a@x.com", "Delhi", "IN", "GOLD", dt(2026, 10, 15))],
            "customer_id string, full_name string, email string, city string, country string, tier string, "
            "updated_at timestamp"),
        None, key="customer_id", tracked_cols=["full_name", "email", "city", "country", "tier"],
        surrogate_key="customer_sk")
    products = spark.createDataFrame([("P1", "Book", "Books")], "product_id string, product_name string, category string")
    orders = spark.createDataFrame([
        ("O1", "C1", "P1", 2, Decimal("100.00"), "INR", "DELIVERED", dt(2026, 10, 12, 9), dt(2026, 10, 12, 9), date(2026, 10, 12)),
        ("O2", "C1", "P1", 1, Decimal("10.00"), "USD", "SHIPPED", dt(2026, 10, 16, 9), dt(2026, 10, 16, 9), date(2026, 10, 16)),
        ("O3", "C1", "P1", 1, Decimal("50.00"), "INR", "CANCELLED", dt(2026, 10, 16, 10), dt(2026, 10, 16, 10), date(2026, 10, 16)),
        ("O4", "C9", "PX", 1, Decimal("50.00"), "INR", "PLACED", dt(2026, 10, 16, 11), dt(2026, 10, 16, 11), date(2026, 10, 16)),
    ], ORDERS)
    return orders, products, dim


def test_fact_uses_customer_version_valid_at_order_time(spark):
    orders, products, dim = _fixtures(spark)
    fact = {r.order_id: r for r in build_fact_orders(
        orders, products, dim, fx_rates_to_inr={"INR": 1.0, "USD": 80.0}, cancelled_statuses=["CANCELLED"]).collect()}

    assert fact["O1"].customer_tier_at_order == "SILVER"   # before first CRM record -> earliest version
    assert fact["O2"].customer_tier_at_order == "GOLD"
    assert fact["O2"].gross_amount_inr == Decimal("800.00")  # 1 x 10 USD x 80
    assert fact["O3"].net_revenue_inr == Decimal("0.00") and fact["O3"].is_cancelled
    assert fact["O4"].customer_sk == -1 and fact["O4"].category == "Unknown"  # unknown members


def test_daily_kpis(spark):
    orders, products, dim = _fixtures(spark)
    fact = build_fact_orders(orders, products, dim, fx_rates_to_inr={"INR": 1.0, "USD": 80.0},
                             cancelled_statuses=["CANCELLED"])
    k = (build_daily_sales_kpis(fact)
         .filter("order_date = '2026-10-16' AND customer_country = 'IN'").first())
    assert (k.total_orders, k.cancelled_orders, k.units_sold) == (2, 1, 1)
    assert k.net_revenue_inr == Decimal("800.00") and k.avg_order_value_inr == Decimal("800.00")
    assert k.cancellation_rate == 0.5


def test_parquet_upsert_is_idempotent_and_ignores_stale_updates(spark, tmp_path):
    store, path = LakeStore(spark, "parquet"), str(tmp_path / "orders")
    schema = "order_id string, status string, updated_at timestamp"
    store.upsert(spark.createDataFrame([("O1", "PLACED", dt(2026, 1, 1))], schema), path, ["order_id"], "updated_at")
    store.upsert(spark.createDataFrame([("O1", "SHIPPED", dt(2026, 1, 2)), ("O2", "PLACED", dt(2026, 1, 2))], schema),
                 path, ["order_id"], "updated_at")
    stale = spark.createDataFrame([("O1", "PLACED", dt(2026, 1, 1, 12))], schema)
    store.upsert(stale, path, ["order_id"], "updated_at")
    store.upsert(stale, path, ["order_id"], "updated_at")  # re-run

    assert {r.order_id: r.status for r in store.read(path).collect()} == {"O1": "SHIPPED", "O2": "PLACED"}
