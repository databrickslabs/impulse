"""Basic Pipeline tests"""

# pylint: disable=missing-function-docstring, redefined-outer-name

import operator

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesAliasSelector,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDuration,
)
from impulse_query_engine.analyze.query.aggregations.histogram2d import (
    Histogram2DCustomWeights,
    Histogram2DDuration,
)
from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.events.sequence_of_events_expression import (
    SequenceOfEventsExpression,
)
from impulse_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache


def test_where():
    expr = TimeSeriesSelector(TagSelector("name") == "test")
    expr2 = expr.where(expr == 1)
    assert isinstance(expr2, TimeSeriesOp)


def test_is_single_signal():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr2 = TimeSeriesSelector(TagSelector("name") == "test2")
    assert expr1.is_single_signal
    assert expr2.is_single_signal
    expr3 = expr1 + 1
    assert expr3.is_single_signal
    expr4 = expr1 + expr2
    assert not expr4.is_single_signal


def test_requires_udf():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr_t = expr1 + 2
    assert not expr_t.requires_udf
    expr_t = expr1 * 2
    assert not expr_t.requires_udf
    expr_t = expr1 / 2
    assert not expr_t.requires_udf
    expr_t = expr1 - 2
    assert not expr_t.requires_udf
    expr_t = expr1 % 2
    assert not expr_t.requires_udf
    expr_t = expr1.where(expr1 > 1)
    assert not expr_t.requires_udf
    expr_t = expr1.apply(lambda ts: ts * 2)
    assert expr_t.requires_udf


def test_apply_udf():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test")
    expr_t = expr1.apply(lambda ts: ts * 2)


def test_create_udf():
    func = lambda ts, scalar: ts * scalar
    prepped_func = TimeSeriesExpression.udf(func)
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr2 = prepped_func(expr1, 1.5)
    assert expr2.is_single_signal
    assert expr2.requires_udf


def test_serialize_selector():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    obj = sel.as_dict()
    assert "type" in obj
    assert "expr" in obj
    assert "alias" in obj
    assert (
        obj["type"]
        == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
    )
    assert obj["alias"] == ""
    sel_deser = TimeSeriesExpression.from_dict(obj)
    assert sel._alias == sel_deser._alias


def test_serialize_op():
    op = TimeSeriesSelector(TagSelector("name") == "test") == 123
    obj = op.as_dict()
    assert "type" in obj
    assert "alias" in obj
    assert "args" in obj
    assert "kwargs" in obj
    assert "op" in obj
    assert "optype" in obj
    assert (
        obj["type"] == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesOp"
    )
    assert obj["alias"] == ""
    assert len(obj["args"]) == 2
    assert len(obj["kwargs"]) == 0
    assert (
        obj["args"][0]["type"]
        == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
    )
    assert obj["args"][1] == 123
    op_deser = TimeSeriesExpression.from_dict(obj)
    assert op._alias == op_deser._alias


def test_serialize_op_cls():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel.where(sel == 123)
    obj = op.as_dict()
    assert "type" in obj
    assert "alias" in obj
    assert "args" in obj
    assert "kwargs" in obj
    assert "op" in obj
    assert "optype" in obj


def test_time_series_selector_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    s = str(sel)
    assert s == "TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>"


def test_time_series_alias_selector_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    alias_sel = TimeSeriesAliasSelector(sel, "my_alias")
    s = str(alias_sel)
    assert (
        s
        == "TimeSeriesAliasSelector<TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, my_alias>"
    )


def test_time_series_op_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel.where(sel == 123)
    s = str(op)
    assert s == (
        "TimeSeriesOp<where(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "TimeSeriesOp<eq(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, 123)>)>"
    )


def test_time_series_udf_str():
    func = lambda ts, scalar: ts * scalar
    prepped_func = TimeSeriesExpression.udf(func)
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = prepped_func(sel, 1.5)
    s = str(op)
    assert (
        s == "TimeSeriesUDF<<lambda>(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, 1.5)>"
    )


def test_mod_returns_time_series_op():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel % 3
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.optype == "builtin"
    assert op.args[0] is sel
    assert op.args[1] == 3


def test_mod_expression_with_expression():
    sel1 = TimeSeriesSelector(TagSelector("name") == "test1")
    sel2 = TimeSeriesSelector(TagSelector("name") == "test2")
    op = sel1 % sel2
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.args[0] is sel1
    assert op.args[1] is sel2


def test_rmod_returns_time_series_op():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = 5 % sel
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.optype == "builtin"
    assert op.args[0] == 5
    assert op.args[1] is sel


def test_rmod_expression_with_float():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = 2.5 % sel
    assert isinstance(op, TimeSeriesOp)
    assert op.args[0] == 2.5
    assert op.args[1] is sel


class TestGetSelectors:
    def test_selector_returns_self(self):
        sel = TimeSeriesSelector(TagSelector("name") == "test")
        result = sel.get_selectors()
        assert result == [sel]

    def test_selector_with_alias_flag(self):
        sel_direct = TimeSeriesSelector(TagSelector("name") == "a")
        sel_aliased = TimeSeriesSelector(TagSelector("alias") == "b", uses_alias=True)
        assert sel_direct.get_selectors() == [sel_direct]
        assert sel_aliased.get_selectors() == [sel_aliased]
        assert sel_direct.uses_alias is False
        assert sel_aliased.uses_alias is True

    def test_op_returns_all_leaf_selectors(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        op = sel_a + sel_b
        result = op.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_nested_op_returns_all_leaves(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        nested = (sel_a + sel_b).mean()
        result = nested.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_op_with_scalar_ignores_non_expressions(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        op = sel + 5
        result = op.get_selectors()
        assert result == [sel]

    def test_alias_selector_returns_all_aliases(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        alias_sel = TimeSeriesAliasSelector(sel_a, sel_b)
        result = alias_sel.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_udf_returns_leaf_selectors(self):
        sel = TimeSeriesSelector(TagSelector("name") == "test")
        udf_expr = sel.apply(lambda ts: ts * 2)
        result = udf_expr.get_selectors()
        assert result == [sel]

    def test_mixed_uses_alias_all_returned(self):
        sel_direct = TimeSeriesSelector(TagSelector("name") == "a")
        sel_aliased = TimeSeriesSelector(TagSelector("alias") == "b", uses_alias=True)
        op = sel_direct + sel_aliased
        result = op.get_selectors()
        assert len(result) == 2
        assert sel_direct in result
        assert sel_aliased in result

    def test_duplicate_selector_in_expression(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        op = sel.where(sel > 1)
        result = op.get_selectors()
        assert len(result) == 2
        assert all(s is sel for s in result)


def _noop(*args, **kwargs):
    """Stand-in UDF body; the declaration tests never invoke it."""
    return 0.0


class TestContainerMetadataDeclaration:
    """UDFs declaring container tags/metrics and their propagation."""

    def test_defaults_are_empty(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        assert sel.required_container_tags() == set()
        assert sel.required_container_metrics() == set()

    def test_apply_declares_container_metadata(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        udf_expr = sel.apply(
            _noop,
            container_tags=["vehicle_type"],
            container_metrics=["nominal_power"],
        )
        assert udf_expr.required_container_tags() == {"vehicle_type"}
        assert udf_expr.required_container_metrics() == {"nominal_power"}

    def test_udf_decorator_declares_container_metadata(self):
        @TimeSeriesExpression.udf(container_tags=["t"], container_metrics=["m"])
        def scaled(ts, container_tags, container_metrics):
            return 0.0

        sel = TimeSeriesSelector(TagSelector("name") == "x")
        expr = scaled(sel)
        assert expr.required_container_tags() == {"t"}
        assert expr.required_container_metrics() == {"m"}

    def test_bare_udf_still_works(self):
        prepped = TimeSeriesExpression.udf(lambda ts, scalar: ts * scalar)
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        expr = prepped(sel, 1.5)
        assert expr.required_container_tags() == set()
        assert expr.required_container_metrics() == set()

    def test_requirements_union_through_nesting(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        u1 = sel.apply(_noop, container_metrics=["m1"])
        u2 = sel.apply(_noop, container_tags=["t1"])
        combo = u1 + u2  # TimeSeriesOp with both UDFs as args
        assert combo.required_container_metrics() == {"m1"}
        assert combo.required_container_tags() == {"t1"}

    def test_collect_container_meta_ordered_dedup(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        u1 = sel.apply(_noop, container_metrics=["b", "a"])
        u2 = sel.apply(_noop, container_metrics=["a", "c"])
        # Sorted within each expression, deduped preserving discovery order.
        assert TimeSeriesExpression.collect_container_metrics([u1, u2]) == ["a", "b", "c"]
        assert TimeSeriesExpression.collect_container_metrics([sel]) == []

    def test_empty_cache_defaults(self):
        cache = EmptyTimeSeriesCache()
        assert cache.container_tags == {}
        assert cache.container_metrics == {}

    def test_build_injects_dicts_against_empty_cache(self):
        captured = {}

        def grab(ts, container_tags, container_metrics):
            captured["tags"] = container_tags
            captured["metrics"] = container_metrics
            return 0.0

        sel = TimeSeriesSelector(TagSelector("name") == "x")
        expr = sel.apply(grab, container_tags=["a"], container_metrics=["b"])
        # Builds against the empty cache without KeyError; values default to None.
        assert expr.build(EmptyTimeSeriesCache()) == 0.0
        assert captured["tags"] == {"a": None}
        assert captured["metrics"] == {"b": None}


class TestContainerMetadataCompositeExpressions:
    """Composite expressions propagate container needs from wrapped children.

    They carry no container metadata of their own; ``required_container_*`` unions
    the children exactly like ``required_tags`` does (a metadata-declaring
    ``TimeSeriesUDF`` wrapped inside them propagates up).
    """

    def _sel(self, name="x"):
        return TimeSeriesSelector(TagSelector("name") == name)

    def test_histogram_propagates_wrapped_udf(self):
        wrapped = self._sel().apply(_noop, container_metrics=["m"])
        h = HistogramDuration(wrapped, [0.0, 1.0])
        assert h.required_container_metrics() == {"m"}
        assert h.required_container_tags() == set()

    def test_plain_histogram_has_no_requirements(self):
        h = HistogramDuration(self._sel(), [0.0, 1.0])
        assert h.required_container_metrics() == set()
        assert h.required_container_tags() == set()

    def test_custom_weights_unions_children(self):
        h = HistogramCustomWeights(
            self._sel("a").apply(_noop, container_metrics=["m1"]),
            weights=self._sel("b").apply(_noop, container_tags=["t1"]),
            bins=[0.0, 1.0],
        )
        assert h.required_container_metrics() == {"m1"}
        assert h.required_container_tags() == {"t1"}

    def test_sequence_of_events_propagates(self):
        expr = SequenceOfEventsExpression([self._sel().apply(_noop, container_tags=["child"])])
        assert expr.required_container_tags() == {"child"}
        assert expr.required_container_metrics() == set()

    def test_calculated_channel_propagates(self):
        cc = CalculatedChannel(
            self._sel().apply(_noop, container_metrics=["m"]), {"channel_name": "x"}
        )
        assert cc.required_container_metrics() == {"m"}
        assert cc.required_container_tags() == set()

    def test_collect_across_composites(self):
        selections = [
            HistogramDuration(self._sel().apply(_noop, container_metrics=["b"]), [0.0, 1.0]),
            CalculatedChannel(
                self._sel().apply(_noop, container_metrics=["a"]), {"channel_name": "z"}
            ),
        ]
        # Deduplicated union across the selections' wrapped UDFs.
        assert set(TimeSeriesExpression.collect_container_metrics(selections)) == {"a", "b"}

    def test_histogram2d_unions_children(self):
        h = Histogram2DDuration(
            self._sel("x").apply(_noop, container_metrics=["mx"]),
            self._sel("y").apply(_noop, container_tags=["ty"]),
            [0.0, 1.0],
            [0.0, 1.0],
        )
        assert h.required_container_metrics() == {"mx"}
        assert h.required_container_tags() == {"ty"}

    def test_histogram2d_custom_weights_unions_children(self):
        h = Histogram2DCustomWeights(
            self._sel("x").apply(_noop, container_metrics=["mx"]),
            self._sel("y").apply(_noop, container_metrics=["my"]),
            self._sel("w").apply(_noop, container_tags=["tw"]),
            [0.0, 1.0],
            [0.0, 1.0],
        )
        assert h.required_container_metrics() == {"mx", "my"}
        assert h.required_container_tags() == {"tw"}

    def test_point_value_aggregator_unions_inputs_and_event(self):
        agg = PointValueAggregator(
            input_expressions=[self._sel("a").apply(_noop, container_metrics=["m"])],
            event_expression=self._sel("e").apply(_noop, container_tags=["t"]),
        )
        assert agg.required_container_metrics() == {"m"}
        assert agg.required_container_tags() == {"t"}

    def test_stats_aggregator_unions_inputs_and_event(self):
        agg = StatsAggregator(
            input_expressions=[self._sel("a").apply(_noop, container_metrics=["m"])],
            statistics=["mean"],
            event_expression=self._sel("e").apply(_noop, container_tags=["t"]),
        )
        assert agg.required_container_metrics() == {"m"}
        assert agg.required_container_tags() == {"t"}

    def test_stats_aggregator_without_event(self):
        # event_expression=None must not raise (the `is not None` guard).
        agg = StatsAggregator(
            input_expressions=[self._sel("a").apply(_noop, container_metrics=["m"])],
            statistics=["mean"],
        )
        assert agg.required_container_metrics() == {"m"}
        assert agg.required_container_tags() == set()

    def test_alias_selector_unions_aliases(self):
        alias = TimeSeriesAliasSelector(
            self._sel("a").apply(_noop, container_metrics=["m"]),
            self._sel("b").apply(_noop, container_tags=["t"]),
        )
        assert alias.required_container_metrics() == {"m"}
        assert alias.required_container_tags() == {"t"}

    def test_op_propagates_through_kwargs(self):
        # TimeSeriesOp walks both args and kwargs; a metadata UDF passed as a
        # keyword argument must still propagate.
        prepped = TimeSeriesExpression.udf(_noop)
        expr = prepped(self._sel(), other=self._sel().apply(_noop, container_metrics=["kw"]))
        assert expr.required_container_metrics() == {"kw"}
