# pylint: disable=missing-function-docstring
"""Planning tests for the rule-based pushdown optimizer (no data involved)."""

import math
import pickle

import pyspark.sql.types as T
import pytest
from pyspark import cloudpickle

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.aggregations.histogram import HistogramDuration
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.events.sequence_of_events_expression import (
    SequenceOfEventsExpression,
)
from impulse_query_engine.analyze.query.optimizer import (
    Pushdown,
    PushdownContext,
    PushdownPlan,
    QueryOptimizer,
    ResamplePushdownRule,
)
from impulse_query_engine.analyze.query.optimizer.traversal import Classifier, discover
from impulse_query_engine.model.series.intervals import Intervals

_LONG = PushdownContext(T.LongType(), T.LongType())
_DOUBLE = PushdownContext(T.DoubleType(), T.DoubleType())


def _channel(name: str, **kwargs) -> TimeSeriesSelector:
    return TimeSeriesSelector(TagSelector("channel_name") == name, **kwargs)


def _plan(*selections, context=_LONG, rules=None):
    return QueryOptimizer(rules).optimize(selections, context)


@pytest.fixture
def rpm():
    return _channel("rpm")


@pytest.fixture
def spd():
    return _channel("spd")


class TestPolicy:
    def test_resample_only_selection_is_pushed(self, rpm):
        plan = _plan(rpm.resample(1e6))
        assert not plan.is_empty
        assert plan.pushed_selector_ids == {rpm.selector_id}
        assert plan.raw_selector_ids == frozenset()
        assert [p.describe() for p in plan.pushdowns_by_variant.values()] == [
            "resample(sample_rate=1000000.0)"
        ]

    def test_raw_use_in_another_selection_blocks_pushdown(self, rpm):
        assert _plan(rpm.resample(1e6), rpm.max()).is_empty

    def test_raw_use_in_the_same_expression_blocks_pushdown(self, rpm):
        assert _plan(rpm.resample(1e6) + rpm).is_empty

    def test_other_channels_keep_raw_data(self, rpm, spd):
        plan = _plan(rpm.resample(1e6), spd.max(), spd.resample(1e6))
        assert plan.pushed_selector_ids == {rpm.selector_id}
        assert plan.raw_selector_ids == {spd.selector_id}

    def test_two_rates_give_two_variants(self, rpm):
        plan = _plan(rpm.resample(1e6), rpm.resample(1e5))
        assert set(plan.variant_ids.values()) == {1, 2}
        assert plan.variants_by_selector == {rpm.selector_id: (1, 2)}

    def test_equivalent_calls_share_one_variant(self, rpm):
        plan = _plan(rpm.resample(1e6), rpm.resample(sample_rate=1_000_000))
        assert plan.variants_by_selector == {rpm.selector_id: (1,)}

    def test_equal_selectors_built_separately_are_one_selector(self):
        plan = _plan(_channel("rpm").resample(1e6), _channel("rpm").max())
        assert plan.is_empty

    def test_node_shared_across_selections_and_branches(self, rpm):
        resampled = rpm.resample(1e6)
        plan = _plan(resampled + resampled, resampled.cumtrapz())
        assert plan.pushed_selector_ids == {rpm.selector_id}

    def test_non_expression_selection_gives_empty_plan(self, rpm):
        assert _plan(rpm.resample(1e6), object()).is_empty


class TestDiscovery:
    def test_inside_udf_argument(self, rpm):
        # TimeSeriesUDF stores its first operand in ``optype`` too.
        plan = _plan(rpm.resample(1e6).apply(lambda s: s))
        assert plan.pushed_selector_ids == {rpm.selector_id}

    def test_inside_calculated_channel(self, rpm):
        plan = _plan(CalculatedChannel(rpm.resample(1e6) * 2, {"channel_name": "rpm_1hz"}))
        assert plan.pushed_selector_ids == {rpm.selector_id}

    def test_inside_stats_aggregator_and_event(self, rpm, spd):
        event = spd.resample(1e6) > 50
        stats = StatsAggregator(
            input_expressions=[rpm.resample(1e6)], statistics=["max"], event_expression=event
        )
        plan = _plan(stats, event)
        assert plan.pushed_selector_ids == {rpm.selector_id, spd.selector_id}

    def test_inside_histogram_and_sequence_of_events(self, rpm, spd):
        plan = _plan(
            HistogramDuration(rpm.resample(1e6), bins=[0, 1000, 2000]),
            SequenceOfEventsExpression([spd.resample(1e6) > 10, spd.resample(1e6) > 20]),
        )
        assert plan.pushed_selector_ids == {rpm.selector_id, spd.selector_id}

    def test_aliased_selector(self):
        alias = _channel("vehicle_speed", uses_alias=True)
        assert _plan(alias.resample(1e6)).pushed_selector_ids == {alias.selector_id}

    def test_walk_disagreeing_with_get_selectors_keeps_raw_data(self, rpm):
        # One selection over-counts a pushed subtree, another hides a raw use; the
        # totals would balance, so only the per-selection guard keeps rpm raw.
        resampled = rpm.resample(1e6)
        over_counted = _Opaque({"a": resampled, "b": resampled}, reported=[rpm])
        hidden_raw_use = _Opaque({"a": rpm.resample(1e6)}, reported=[rpm, rpm])
        assert _plan(over_counted, hidden_raw_use).is_empty


class _MatchesAnyNode(ResamplePushdownRule):
    """Claims every non-selector node that reads exactly one selector, of any type."""

    name = "any_node"

    def match(self, node, operand):
        if isinstance(node, TimeSeriesSelector) or not isinstance(node, TimeSeriesExpression):
            return None
        selectors = node.get_selectors()
        if len(selectors) != 1:
            return None
        return Pushdown(rule=self, selector=selectors[0], params=(), consumed=tuple(selectors))

    def supports(self, pushdown, context):
        return True


class TestOnlyOperationsArePushedDown:
    # Only TimeSeriesOp.build serves precomputed results; other node types must
    # keep their raw rows.
    def test_aggregation_node_is_not_a_candidate(self, rpm):
        found, leaves = discover(
            HistogramDuration(rpm, bins=[0, 1000]), Classifier([_MatchesAnyNode()])
        )
        assert found == []
        assert [s.selector_id for s in leaves] == [rpm.selector_id]

    def test_udf_node_is_not_a_candidate_but_its_arguments_are(self, rpm):
        found, _ = discover(rpm.resample(1e6).apply(lambda s: s), Classifier([_MatchesAnyNode()]))
        assert [node.operation for node, _ in found] == ["resample"]

    def test_lookup_ignores_other_node_types(self, rpm):
        rule = _MatchesAnyNode()
        plan = QueryOptimizer([rule]).optimize([rpm.resample(1e6)], _LONG)
        assert plan.lookup(rpm.resample(1e6)) is not None
        assert plan.lookup(HistogramDuration(rpm, bins=[0, 1000])) is None


class _OnPushedOperand(ResamplePushdownRule):
    """Matches ``x.cumtrapz()`` on a pushed ``x``, recording the operands it sees."""

    def __init__(self, name="on_pushed", chained=False, supported=True):
        self.name = name
        self.chained = chained
        self.supported = supported
        self.seen = []

    def match(self, node, operand):
        if type(node) is not TimeSeriesOp or node.operation != "cumtrapz":
            return None
        inner = operand(node.args[0])
        self.seen.append(inner)
        if not isinstance(inner, Pushdown):
            return None
        return Pushdown(
            rule=self,
            selector=inner.selector,
            params=(("inner", inner.variant_key),),
            consumed=inner.consumed,
            source=inner if self.chained else None,
        )

    def supports(self, pushdown, context):
        return self.supported


_RESAMPLE = "resample(sample_rate=1000000.0)"


def _described(plan):
    return [p.describe() for p in plan.pushdowns_by_variant.values()]


class TestBottomUpClassification:
    def test_rules_see_their_operands_classification(self, rpm):
        rule = _OnPushedOperand()
        classify = Classifier([rule, ResamplePushdownRule()])
        for node in (rpm.cumtrapz(), rpm.resample(1e6).cumtrapz(), (rpm * 2).cumtrapz()):
            classify(node)
        channel, pushed, evaluated_in_pandas = rule.seen
        assert channel is rpm
        assert pushed.describe() == _RESAMPLE
        assert evaluated_in_pandas is None

    def test_chained_pushdown_is_not_planned_but_its_operand_is(self, rpm):
        expr = rpm.resample(1e6).cumtrapz()
        plan = _plan(expr, rules=[_OnPushedOperand(chained=True), ResamplePushdownRule()])
        assert _described(plan) == [_RESAMPLE]
        assert plan.pushed_selector_ids == {rpm.selector_id}
        assert plan.lookup(expr) is None
        assert plan.lookup(expr.args[0])[1] == 1

    def test_absorbing_rule_is_planned_as_one_variant(self, rpm):
        expr = rpm.resample(1e6).cumtrapz()
        plan = _plan(expr, rules=[_OnPushedOperand(), ResamplePushdownRule()])
        (pushdown,) = plan.pushdowns_by_variant.values()
        assert pushdown.rule.name == "on_pushed" and pushdown.source is None
        assert pushdown.consumed == (rpm,)
        hit, variant = plan.lookup(expr)
        assert hit.variant_key == pushdown.variant_key and variant == 1
        assert plan.lookup(expr.args[0]) is None

    def test_unsupported_parent_falls_back_to_its_pushed_operand(self, rpm):
        expr = rpm.resample(1e6).cumtrapz()
        plan = _plan(expr, rules=[_OnPushedOperand(supported=False), ResamplePushdownRule()])
        assert _described(plan) == [_RESAMPLE]
        assert plan.lookup(expr) is None
        assert plan.lookup(expr.args[0])[1] == 1

    def test_first_matching_rule_decides(self, rpm):
        # The UDF classifies without knowing what is supported, so an unsupported
        # first rule must not hand the node to the next one.
        expr = rpm.resample(1e6).cumtrapz()
        rules = [
            _OnPushedOperand("unsupported", supported=False),
            _OnPushedOperand("supported"),
            ResamplePushdownRule(),
        ]
        plan = _plan(expr, rules=rules)
        assert _described(plan) == [_RESAMPLE]
        assert plan.lookup(expr) is None
        assert plan.lookup(expr.args[0])[1] == 1

    def test_shared_nodes_are_matched_once(self, rpm):
        matched = []

        class _Recording(ResamplePushdownRule):
            def match(self, node, operand):
                matched.append(node)
                return super().match(node, operand)

        resampled = rpm.resample(1e6)
        plan = _plan(resampled + resampled, resampled.cumtrapz(), rules=[_Recording()])
        assert len(matched) == len({id(node) for node in matched}) == 3
        assert plan.pushed_selector_ids == {rpm.selector_id}

    def test_chained_pushdowns_are_their_own_variant_and_cannot_be_planned(self, rpm):
        inner = Classifier([ResamplePushdownRule()])(rpm.resample(1e6))
        chained = Pushdown(
            rule=inner.rule, selector=rpm, params=inner.params, consumed=(rpm,), source=inner
        )
        assert inner.variant_key == ("resample", (("sample_rate", 1e6),))
        assert chained.variant_key != inner.variant_key
        assert chained.describe() == f"{_RESAMPLE} on {_RESAMPLE}"
        with pytest.raises(ValueError, match="has a source"):
            PushdownPlan.build([inner.rule], [chained], [rpm.selector_id])


class TestRuleNames:
    @pytest.mark.parametrize("name", ["Bad", "a__b", "_x", "x_", "1x", ""])
    def test_invalid_rule_names_are_rejected(self, name):
        rule = type("_Named", (ResamplePushdownRule,), {"name": name})()
        with pytest.raises(ValueError, match="rule name"):
            QueryOptimizer([rule])

    def test_rule_names_must_be_unique(self):
        with pytest.raises(ValueError, match="unique"):
            QueryOptimizer([ResamplePushdownRule(), ResamplePushdownRule()])

    def test_plan_rejects_two_rules_sharing_a_name(self, rpm, spd):
        first, second = ResamplePushdownRule(), ResamplePushdownRule()
        plan = PushdownPlan.build(
            (first, second),
            [Classifier([first])(rpm.resample(1e6)), Classifier([second])(spd.resample(2e6))],
            [rpm.selector_id, spd.selector_id],
        )
        with pytest.raises(ValueError, match="two different pushdown rules"):
            plan.variants_by_rule()


class _Opaque(TimeSeriesExpression):
    """Node whose attributes disagree with the selectors it reports."""

    def __init__(self, attributes, reported):
        super().__init__()
        self.__dict__.update(attributes)
        self._reported = lambda: list(reported)  # not visible to the walk

    def build(self, cache):
        return None

    def get_selectors(self):
        return self._reported()

    def get_required_tag_exprs(self):
        return set()

    def required_tags(self):
        return set()

    def get_selector_expr(self):
        return None

    def __str__(self):
        return "_Opaque"


class TestResampleRule:
    @pytest.mark.parametrize(
        "make",
        [
            lambda c: (c * 2).resample(1e6),
            lambda c: c.resample(True),
            lambda c: c.resample(0),
            lambda c: c.resample(-1e6),
            lambda c: c.resample(math.nan),
            lambda c: c.resample(math.inf),
            lambda c: c.resample(c),
            lambda c: c.resample(1e6, 2),
            lambda c: c.resample(1e6, sample_rate=1e6),
            lambda c: c.resample(rate=1e6),
            lambda c: c.rolling_average(1e6),
        ],
    )
    def test_unsupported_calls_are_not_matched(self, rpm, make):
        assert Classifier([ResamplePushdownRule()])(make(rpm)) is None

    def test_default_rate(self, rpm):
        pushdown = Classifier([ResamplePushdownRule()])(rpm.resample())
        assert pushdown.params == (("sample_rate", 1.0),)
        assert pushdown.selector is rpm

    def test_points_in_time_selector_is_not_matched(self):
        poi = _channel("dtc", series_type=SeriesType.POINTS_IN_TIME)
        assert Classifier([ResamplePushdownRule()])(poi.resample(1e6)) is None

    def test_fractional_rate_needs_double_time_columns(self, rpm):
        assert _plan(rpm.resample(0.5), context=_LONG).is_empty
        assert not _plan(rpm.resample(0.5), context=_DOUBLE).is_empty

    @pytest.mark.parametrize(
        "time_type", [T.FloatType(), T.DecimalType(20, 0), T.TimestampType(), T.StringType()]
    )
    def test_unsupported_time_column_types(self, rpm, time_type):
        assert _plan(rpm.resample(1e6), context=PushdownContext(time_type, time_type)).is_empty


class TestResultTypeCheck:
    def test_misdeclared_result_type_is_dropped(self, rpm):
        class _IntervalsResample(ResamplePushdownRule):
            def result_type(self, pushdown):
                return Intervals

        assert _plan(rpm.resample(1e6), rules=[_IntervalsResample()]).is_empty

    def test_failing_type_inference_drops_candidate(self, rpm):
        class _Exploding(ResamplePushdownRule):
            def match(self, node, operand):
                pushdown = super().match(node, operand)
                if pushdown is not None:
                    node.evaluation_type = _raise
                return pushdown

        def _raise():
            raise RuntimeError("boom")

        assert _plan(rpm.resample(1e6), rules=[_Exploding()]).is_empty


class TestPlanInExecutor:
    @pytest.mark.parametrize("dumps", [pickle.dumps, cloudpickle.dumps])
    def test_plan_and_selections_survive_pickling(self, rpm, spd, dumps):
        selections = [rpm.resample(1e6).cumtrapz(), spd.resample(1e6), spd.max()]
        plan = _plan(*selections)
        plan2, selections2 = pickle.loads(dumps((plan, selections)))

        pushed = selections2[0].args[0]
        pushdown, variant = plan2.lookup(pushed)
        assert variant == 1
        assert pushdown.selector.selector_id == rpm.selector_id
        # spd is needed raw by spd.max(), so its resample is evaluated in pandas.
        assert plan2.lookup(selections2[1]) is None
        assert plan2.lookup(selections2[2]) is None
