"""Unit tests for the value-predicate pushdown analyzer (no Spark session needed)."""

import operator

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesAliasSelector,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.aggregations.histogram import HistogramCustomWeights
from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.solvers.utils.predicate_pushdown import (
    ALL_ROWS,
    ValueAnd,
    ValueComparison,
    ValueOr,
    analyze_selections,
)


def _selector(name: str = "Eng_RPM", uses_alias: bool = False) -> TimeSeriesSelector:
    return TimeSeriesSelector(TagSelector("channel_name") == name, uses_alias=uses_alias)


class TestComparisons:
    def test_greater_than(self):
        ch = _selector()
        assert analyze_selections([ch > 2000]) == {ch.selector_id: ValueComparison("gt", 2000.0)}

    def test_all_supported_operators(self):
        ch = _selector()
        for expr, expected_op in [
            (ch > 1, "gt"),
            (ch >= 1, "ge"),
            (ch < 1, "lt"),
            (ch <= 1, "le"),
            (ch == 1, "eq"),
        ]:
            assert analyze_selections([expr]) == {
                ch.selector_id: ValueComparison(expected_op, 1.0)
            }

    def test_scalar_first_operand_is_mirrored(self):
        ch = _selector()
        expr = TimeSeriesOp(operator.lt, "builtin", 2000, ch)  # 2000 < ch
        assert analyze_selections([expr]) == {ch.selector_id: ValueComparison("gt", 2000.0)}

    def test_not_equal_is_not_extracted(self):
        ch = _selector()
        assert analyze_selections([ch != 5]) == {ch.selector_id: ALL_ROWS}

    def test_nan_threshold_is_rejected(self):
        ch = _selector()
        assert analyze_selections([ch > float("nan")]) == {ch.selector_id: ALL_ROWS}


class TestBooleanCombinations:
    def test_and_of_same_selector(self):
        ch = _selector()
        expr = (ch > 2000) & (ch < 4000)
        assert analyze_selections([expr]) == {
            ch.selector_id: ValueAnd(ValueComparison("gt", 2000.0), ValueComparison("lt", 4000.0))
        }

    def test_or_of_same_selector(self):
        ch = _selector()
        expr = (ch > 4000) | (ch < 2000)
        assert analyze_selections([expr]) == {
            ch.selector_id: ValueOr(ValueComparison("gt", 4000.0), ValueComparison("lt", 2000.0))
        }

    def test_nested_combination_stays_exact(self):
        ch = _selector()
        expr = ((ch > 1) & (ch < 4)) | (ch == 7)
        assert analyze_selections([expr]) == {
            ch.selector_id: ValueOr(
                ValueAnd(ValueComparison("gt", 1.0), ValueComparison("lt", 4.0)),
                ValueComparison("eq", 7.0),
            )
        }

    def test_cross_selector_and_keeps_independent_predicates(self):
        a, b = _selector("A"), _selector("B")
        expr = (a > 1) & (b < 2)
        assert analyze_selections([expr]) == {
            a.selector_id: ValueComparison("gt", 1.0),
            b.selector_id: ValueComparison("lt", 2.0),
        }

    def test_shaped_operand_forces_or_merge(self):
        # merge_intervals gap-fills, so its intervals may overlap rows failing
        # the inner predicate: the later & must OR the needs, not AND them.
        ch = _selector()
        expr = ((ch > 1) | (ch < 0)).merge_intervals(5) & (ch > 3)
        assert analyze_selections([expr]) == {
            ch.selector_id: ValueOr(
                ValueOr(ValueComparison("gt", 1.0), ValueComparison("lt", 0.0)),
                ValueComparison("gt", 3.0),
            )
        }


class TestIntervalShapingOps:
    def test_shaping_preserves_needed_rows(self):
        ch = _selector()
        for expr in [
            (ch > 2000).merge_intervals(5),
            (ch > 2000).debounce(2),
            (ch > 2000).filter(1),
            (ch > 2000).expand(1),
            (ch > 2000).shrink_left(1),
            (ch > 2000).start_points(),
            (ch > 2000).merge_intervals(5).debounce(2),
        ]:
            assert analyze_selections([expr]) == {ch.selector_id: ValueComparison("gt", 2000.0)}

    def test_unknown_cls_op_is_opaque(self):
        ch = _selector()
        assert analyze_selections([(ch > 2000).resample(1.0)]) == {ch.selector_id: ALL_ROWS}


class TestOpaqueFallback:
    def test_all_rows_cases(self):
        ch = _selector()
        for expr in [
            ch,  # raw series selection
            ch.mean(),
            ch.sum(),
            ch.where(ch > 1),
            ch.diff() > 5,  # comparison over a non-bare operand
            (ch * 2) > 3,  # scalar arithmetic is not folded in v1
            ch.apply(lambda ts: ts * 2) > 1,  # TimeSeriesUDF
        ]:
            assert analyze_selections([expr]) == {ch.selector_id: ALL_ROWS}

    def test_two_series_comparison_is_opaque(self):
        a, b = _selector("A"), _selector("B")
        assert analyze_selections([a + b > 3]) == {
            a.selector_id: ALL_ROWS,
            b.selector_id: ALL_ROWS,
        }

    def test_aliased_selector_is_opaque(self):
        # uses_alias selectors see unit-converted values; raw table values differ.
        ch = _selector(uses_alias=True)
        assert analyze_selections([ch > 2000]) == {ch.selector_id: ALL_ROWS}

    def test_alias_selector_node_is_opaque(self):
        a, b = _selector("A"), _selector("B")
        alias = TimeSeriesAliasSelector(a, b)
        assert analyze_selections([alias]) == {
            a.selector_id: ALL_ROWS,
            b.selector_id: ALL_ROWS,
        }


class TestWhere:
    def test_gate_is_analyzed_and_series_is_all_rows(self):
        a, b = _selector("A"), _selector("B")
        assert analyze_selections([a.where(b > 1)]) == {
            a.selector_id: ALL_ROWS,
            b.selector_id: ValueComparison("gt", 1.0),
        }

    def test_series_usage_absorbs_gate_predicate_on_same_selector(self):
        ch = _selector()
        assert analyze_selections([ch.where(ch > 1)]) == {ch.selector_id: ALL_ROWS}

    def test_opaque_gate_stays_all_rows(self):
        a, b = _selector("A"), _selector("B")
        assert analyze_selections([a.where(b.rising_edges())]) == {
            a.selector_id: ALL_ROWS,
            b.selector_id: ALL_ROWS,
        }


class TestEventGatedAggregations:
    def test_stats_aggregator_analyzes_event_expression(self):
        inp, gate = _selector("A"), _selector("B")
        agg = StatsAggregator(
            input_expressions=[inp],
            statistics=["min", "max"],
            event_expression=(gate > 2000) & (gate < 4000),
        )
        assert analyze_selections([agg]) == {
            inp.selector_id: ALL_ROWS,
            gate.selector_id: ValueAnd(
                ValueComparison("gt", 2000.0), ValueComparison("lt", 4000.0)
            ),
        }

    def test_stats_aggregator_input_absorbs_shared_event_channel(self):
        ch = _selector()
        agg = StatsAggregator(
            input_expressions=[ch], statistics=["mean"], event_expression=ch > 2000
        )
        assert analyze_selections([agg]) == {ch.selector_id: ALL_ROWS}

    def test_stats_aggregator_without_event_expression(self):
        ch = _selector()
        agg = StatsAggregator(input_expressions=[ch], statistics=["mean"])
        assert analyze_selections([agg]) == {ch.selector_id: ALL_ROWS}

    def test_comparison_selection_does_not_filter_aggregator_input(self):
        # Selection A: rpm > 500; selection B: stats of the raw rpm series
        # gated by spd > 0.  B needs every rpm row, so the shared channel
        # must end up unfiltered; only the event channel keeps a predicate.
        rpm, spd = _selector("Eng_RPM"), _selector("Veh_Spd")
        agg = StatsAggregator(
            input_expressions=[rpm], statistics=["mean"], event_expression=spd > 0
        )
        assert analyze_selections([rpm > 500, agg]) == {
            rpm.selector_id: ALL_ROWS,
            spd.selector_id: ValueComparison("gt", 0.0),
        }

    def test_point_value_aggregator_analyzes_event_expression(self):
        inp, gate = _selector("A"), _selector("B")
        agg = PointValueAggregator(
            input_expressions=[inp], event_expression=(gate > 1300).start_points()
        )
        assert analyze_selections([agg]) == {
            inp.selector_id: ALL_ROWS,
            gate.selector_id: ValueComparison("gt", 1300.0),
        }


class TestHistogramDuration:
    def test_bare_selector_gets_bin_range_predicate(self):
        ch = _selector()
        assert analyze_selections([ch.histogram([800.0, 1000.0, 4000.0])]) == {
            ch.selector_id: ValueAnd(ValueComparison("ge", 800.0), ValueComparison("le", 4000.0))
        }

    def test_where_gated_selection_combines_range_and_gate(self):
        ch, gate = _selector(), _selector("B")
        agg = ch.where(gate < 20).histogram([800.0, 4000.0])
        assert analyze_selections([agg]) == {
            ch.selector_id: ValueAnd(ValueComparison("ge", 800.0), ValueComparison("le", 4000.0)),
            gate.selector_id: ValueComparison("lt", 20.0),
        }

    def test_nan_bin_edge_is_opaque(self):
        ch = _selector()
        assert analyze_selections([ch.histogram([float("nan"), 4000.0])]) == {
            ch.selector_id: ALL_ROWS
        }

    def test_complex_selection_is_opaque(self):
        ch = _selector()
        assert analyze_selections([(ch * 2).histogram([0.0, 1.0])]) == {ch.selector_id: ALL_ROWS}

    def test_custom_weights_histogram_is_opaque(self):
        # Synchronization resamples both series; every row shifts the result.
        ch, weights = _selector(), _selector("W")
        agg = HistogramCustomWeights(selection=ch, weights=weights, bins=[0.0, 1.0])
        assert analyze_selections([agg]) == {
            ch.selector_id: ALL_ROWS,
            weights.selector_id: ALL_ROWS,
        }


class TestMultiSelectionMerge:
    def test_all_rows_absorbs_predicates(self):
        # A non-analyzable usage anywhere in the query must disable the
        # filter for the shared selector entirely.
        ch = _selector()
        assert analyze_selections([ch > 2000, ch.mean()]) == {ch.selector_id: ALL_ROWS}
        assert analyze_selections([ch.mean(), ch > 2000]) == {ch.selector_id: ALL_ROWS}

    def test_predicates_of_shared_selector_are_ored(self):
        ch = _selector()
        assert analyze_selections([ch > 2000, ch < 100]) == {
            ch.selector_id: ValueOr(ValueComparison("gt", 2000.0), ValueComparison("lt", 100.0))
        }

    def test_independent_selectors_keep_independent_predicates(self):
        a, b = _selector("A"), _selector("B")
        assert analyze_selections([a > 1, b.mean()]) == {
            a.selector_id: ValueComparison("gt", 1.0),
            b.selector_id: ALL_ROWS,
        }

    def test_non_expression_selection_disables_pushdown(self):
        ch = _selector()
        assert analyze_selections([ch > 2000, object()]) is None

    def test_empty_selections(self):
        assert analyze_selections([]) == {}


def test_serialization_round_trip_analyzes_identically():
    ch = _selector()
    expr = (ch > 2000) & (ch < 4000)
    restored = TimeSeriesExpression.from_dict(expr.as_dict())
    assert analyze_selections([restored]) == analyze_selections([expr])
