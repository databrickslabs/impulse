"""Basic Metric tests"""

# pylint: disable=missing-function-docstring, redefined-outer-name

from mda_query_engine.analyze.metadata.metric_expression import MetricOp, MetricSelector

# --- Comparison operator tests ---


def test_eq():
    expr = MetricSelector("start_dt") == "2023-08-16T00:00:000Z"
    assert isinstance(expr, MetricOp)


def test_ne():
    expr = MetricSelector("start_dt") != "2023-08-16T00:00:000Z"
    assert isinstance(expr, MetricOp)


def test_gt():
    expr = MetricSelector("start_dt") > "2023-08-16T00:00:000Z"
    assert isinstance(expr, MetricOp)


def test_ge():
    expr = MetricSelector("duration_s") >= 5
    assert isinstance(expr, MetricOp)


def test_lt():
    expr = MetricSelector("duration_s") < 5
    assert isinstance(expr, MetricOp)


def test_le():
    expr = MetricSelector("duration_s") <= 5
    assert isinstance(expr, MetricOp)


# --- Logical combination tests ---


def test_or():
    expr1 = MetricSelector("duration_s") <= 5
    expr2 = MetricSelector("start_dt") > "2023-08-16T00:00:000Z"
    expr = expr1 | expr2
    assert isinstance(expr, MetricOp)


def test_and():
    expr1 = MetricSelector("duration_s") <= 5
    expr2 = MetricSelector("start_dt") > "2023-08-16T00:00:000Z"
    expr = expr1 & expr2
    assert isinstance(expr, MetricOp)


def test_nested_and_or_operations():
    """Test complex nested AND/OR operations."""
    expr1 = MetricSelector("brand") == "Seat"
    expr2 = MetricSelector("model") == "Leon"
    expr3 = MetricSelector("year") > 2020
    combined = (expr1 & expr2) | expr3
    assert isinstance(combined, MetricOp)
    assert combined.required_metrics() == {"brand", "model", "year"}


# --- Empty / edge-case selector tests ---


def test_empty_selector():
    """MetricSelector with empty string key should not crash."""
    expr = MetricSelector("")
    assert isinstance(expr, MetricSelector)
    assert expr.key == ""
    assert expr.required_metrics() == {""}


def test_empty_selector_comparison():
    """Comparing an empty MetricSelector should produce a valid MetricOp."""
    expr = MetricSelector("") == "value"
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {""}


def test_empty_selector_combined():
    """Empty selector combined with a normal selector should work."""
    expr = (MetricSelector("") == "x") & (MetricSelector("brand") == "Seat")
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {"", "brand"}


# --- String representation tests ---


def test_metric_selector_str_representation():
    """Test string representation of MetricSelector."""
    expr = MetricSelector("brand")
    assert str(expr) == "MetricSelector<brand>"


def test_metric_op_str_representation():
    """Test string representation of MetricOp."""
    expr = MetricSelector("brand") == "Seat"
    assert "MetricOp" in str(expr)
    assert "eq" in str(expr)


# --- required_metrics tests ---


def test_metric_selector_required_metrics():
    """Test required_metrics returns correct set."""
    expr = MetricSelector("vehicle_key")
    assert expr.required_metrics() == {"vehicle_key"}


def test_metric_op_required_metrics_single():
    """Single comparison required_metrics."""
    expr = MetricSelector("brand") == "Seat"
    assert expr.required_metrics() == {"brand"}


def test_metric_op_required_metrics_and():
    """AND-combined required_metrics should union keys."""
    expr = (MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")
    assert expr.required_metrics() == {"brand", "model"}


def test_metric_op_required_metrics_or():
    """OR on the same key should still return one element."""
    expr = (MetricSelector("brand") == "Seat") | (MetricSelector("brand") == "VW")
    assert expr.required_metrics() == {"brand"}


def test_metric_op_required_metrics_nested():
    """Deeply nested expression should collect all unique keys."""
    expr = ((MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")) | (
        MetricSelector("environment") == "test"
    )
    assert expr.required_metrics() == {"brand", "model", "environment"}
