from datetime import datetime

from pyspark.sql.types import StructType

from shopstream.quality import split_valid_invalid
from shopstream.schemas import ORDERS_RAW
from shopstream.transformations.silver import dedupe_latest, order_rules, standardize_orders


def _raw_orders(spark, rows):
    cols = [f.name for f in ORDERS_RAW.fields]
    full = [tuple(r.get(c) for c in cols) + ("f.json", datetime(2026, 1, 1), "run1") for r in rows]
    schema = (StructType(list(ORDERS_RAW.fields))  # copy: StructType.add mutates in place
              .add("_source_file", "string").add("_ingest_ts", "timestamp").add("_run_id", "string"))
    return spark.createDataFrame(full, schema)


BASE = dict(order_id=" o1 ", customer_id="c1", product_id="p1", quantity="2", unit_price="10.50",
            currency="inr", status="placed", order_ts="2026-10-14T10:00:00", updated_at="2026-10-14T10:05:00")


def test_standardize_trims_uppercases_and_types(spark):
    row = standardize_orders(_raw_orders(spark, [BASE])).first()
    assert (row.order_id, row.customer_id, row.currency, row.status) == ("O1", "C1", "INR", "PLACED")
    assert row.quantity == 2 and float(row.unit_price) == 10.5
    assert str(row.order_date) == "2026-10-14"


def test_unparseable_values_are_quarantined_not_crashing(spark):
    bad = [dict(BASE, order_id="o2", quantity="two"),
           dict(BASE, order_id="o3", order_ts="14/10/2026"),
           dict(BASE, order_id="o4", currency="GBP"),
           dict(BASE, order_id="o5", updated_at="2026-10-14T09:00:00")]  # updated before ordered
    df = standardize_orders(_raw_orders(spark, [BASE, *bad]))
    valid, rejected = split_valid_invalid(df, order_rules(["PLACED"], ["INR"]))

    assert [r.order_id for r in valid.collect()] == ["O1"]
    reasons = {r.order_id: set(r._dq_failures) for r in rejected.collect()}
    assert "quantity_positive" in reasons["O2"]
    assert "order_ts_valid" in reasons["O3"]
    assert reasons["O4"] == {"currency_supported"}
    assert reasons["O5"] == {"updated_at_valid"}


def test_dedupe_keeps_latest_version(spark):
    df = spark.createDataFrame(
        [("O1", "PLACED", datetime(2026, 1, 1, 10)), ("O1", "SHIPPED", datetime(2026, 1, 2, 10)),
         ("O1", "PLACED", datetime(2026, 1, 1, 9)), ("O2", "PLACED", datetime(2026, 1, 1, 9))],
        "order_id string, status string, updated_at timestamp")
    out = {r.order_id: r.status for r in dedupe_latest(df, ["order_id"], ["updated_at"]).collect()}
    assert out == {"O1": "SHIPPED", "O2": "PLACED"}
