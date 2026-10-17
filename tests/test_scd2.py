from datetime import datetime as dt

from shopstream.transformations.scd2 import build_scd2

SCHEMA = "customer_id string, tier string, city string, updated_at timestamp"
ARGS = dict(key="customer_id", tracked_cols=["tier", "city"], surrogate_key="customer_sk")


def _versions(df, key="C1"):
    rows = df.filter(f"customer_id = '{key}'").orderBy("effective_from").collect()
    return [(r.tier, r.city, r.effective_from, r.effective_to, r.is_current) for r in rows]


def test_initial_load_creates_one_current_version_per_key(spark):
    changes = spark.createDataFrame([("C1", "SILVER", "Delhi", dt(2026, 10, 13)),
                                     ("C2", "GOLD", "Pune", dt(2026, 10, 13))], SCHEMA)
    dim = build_scd2(changes, None, **ARGS)
    assert dim.count() == 2
    assert dim.filter("is_current").count() == 2


def test_change_closes_old_version_and_noop_is_ignored(spark):
    v1 = build_scd2(spark.createDataFrame([("C1", "SILVER", "Delhi", dt(2026, 10, 13))], SCHEMA), None, **ARGS)
    changes = spark.createDataFrame([("C1", "GOLD", "Delhi", dt(2026, 10, 15)),
                                     ("C1", "GOLD", "Delhi", dt(2026, 10, 16))], SCHEMA)  # 2nd is a no-op
    assert _versions(build_scd2(changes, v1, **ARGS)) == [
        ("SILVER", "Delhi", dt(2026, 10, 13), dt(2026, 10, 15), False),
        ("GOLD", "Delhi", dt(2026, 10, 15), dt(9999, 12, 31), True),
    ]


def test_late_arriving_change_is_inserted_into_history(spark):
    base = spark.createDataFrame([("C1", "SILVER", "Delhi", dt(2026, 10, 13)),
                                  ("C1", "GOLD", "Gurugram", dt(2026, 10, 15))], SCHEMA)
    dim = build_scd2(base, None, **ARGS)
    late = spark.createDataFrame([("C1", "GOLD", "Delhi", dt(2026, 10, 14, 12))], SCHEMA)

    assert _versions(build_scd2(late, dim, **ARGS)) == [
        ("SILVER", "Delhi", dt(2026, 10, 13), dt(2026, 10, 14, 12), False),
        ("GOLD", "Delhi", dt(2026, 10, 14, 12), dt(2026, 10, 15), False),
        ("GOLD", "Gurugram", dt(2026, 10, 15), dt(9999, 12, 31), True),
    ]


def test_untouched_keys_pass_through_unchanged(spark):
    dim = build_scd2(spark.createDataFrame([("C1", "SILVER", "Delhi", dt(2026, 10, 13)),
                                            ("C2", "GOLD", "Pune", dt(2026, 10, 13))], SCHEMA), None, **ARGS)
    out = build_scd2(spark.createDataFrame([("C1", "GOLD", "Delhi", dt(2026, 10, 15))], SCHEMA), dim, **ARGS)
    assert _versions(out, "C2") == _versions(dim, "C2")
