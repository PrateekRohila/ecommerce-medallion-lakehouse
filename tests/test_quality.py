import pytest

from shopstream.quality import DataQualityError, DQRule, enforce_reject_threshold, split_valid_invalid


def test_rows_failing_rules_are_rejected_with_all_reasons(spark):
    df = spark.createDataFrame([("A", 5), (None, 3), (None, -1), ("D", None)], "id string, qty int")
    rules = [DQRule("id_present", "id IS NOT NULL"), DQRule("qty_positive", "qty > 0")]

    valid, rejected = split_valid_invalid(df, rules)

    assert [r.id for r in valid.collect()] == ["A"]
    reasons = {(r.id, r.qty): sorted(r._dq_failures) for r in rejected.collect()}
    assert reasons[(None, 3)] == ["id_present"]
    assert reasons[(None, -1)] == ["id_present", "qty_positive"]
    assert reasons[("D", None)] == ["qty_positive"]  # NULL comparison counts as a failure


def test_reject_threshold_is_a_circuit_breaker():
    assert enforce_reject_threshold("orders", 100, 5, 0.10) == 0.05
    with pytest.raises(DataQualityError):
        enforce_reject_threshold("orders", 100, 11, 0.10)
