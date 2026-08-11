"""Basic Metric tests"""

# pylint: disable=missing-function-docstring, redefined-outer-name

from impulse_query_engine.analyze.metadata.metric_expression import MetricOp, MetricSelector

# --- Evaluation-path invariants ---


def test_metric_expression_is_spark_evaluated_not_pandas():
    """Metric filtering evaluates in Spark via ``get_selector_expr``, never the
    pandas ``resolve()`` / ``build_pandas`` path.

    ``build_pandas`` was removed from ``MetricExpression`` in the array-support
    refactor. The only surviving ``build_pandas`` callers
    (``TimeSeriesCache.resolve``) receive a ``TimeSeriesSelector`` whose ``_expr``
    is always a ``TagExpression`` — a metric expression can never reach them.
    This locks the separation in: if a metric ever gained a ``build_pandas`` again
    (or lost ``get_selector_expr``), that would signal the two paths are blurring.
    """
    sel = MetricSelector("poi_defect_values")
    op = sel.contains("A")
    for expr in (sel, op):
        assert hasattr(expr, "get_selector_expr")  # the real metric eval path
        assert not hasattr(expr, "build_pandas")  # the deleted, unreachable one


def test_metric_expression_is_not_serializable():
    """Metric expressions have no ``as_dict``/``from_dict``.

    This is *why* a ``MetricSelector`` can never be smuggled into a
    ``TimeSeriesSelector._expr`` via ``from_dict`` deserialization (and thus never
    reach the pandas ``resolve`` path): there is no serialized form to resolve back
    to a metric class. Tag expressions, which legitimately populate ``_expr``, are
    serializable; metric expressions are deliberately not.
    """
    sel = MetricSelector("poi_defect_values")
    op = sel.contains_any(["A", "B"])
    for expr in (sel, op):
        assert not hasattr(expr, "as_dict")
        assert not hasattr(expr, "from_dict")


# --- Comparison operator tests ---


# --- Array-membership operator tests (structure only; evaluation in md/expressions) ---


def test_contains_builds_metric_op():
    expr = MetricSelector("poi_defect_values").contains("B1024-43")
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {"poi_defect_values"}


def test_contains_any_builds_metric_op():
    expr = MetricSelector("poi_defect_values").contains_any(["B1024-43", "U0046-13"])
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {"poi_defect_values"}


def test_contains_all_builds_metric_op():
    expr = MetricSelector("poi_defect_values").contains_all(["B1024-43", "U0046-13"])
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {"poi_defect_values"}


def test_array_ops_compose_with_scalar_metrics():
    """An array op ANDs/ORs with ordinary comparisons, unioning required metrics."""
    expr = (MetricSelector("poi_defect_values").contains_any(["A", "B"])) & (
        MetricSelector("duration_ms") > 30
    )
    assert isinstance(expr, MetricOp)
    assert expr.required_metrics() == {"poi_defect_values", "duration_ms"}


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
