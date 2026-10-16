# pylint: disable=missing-function-docstring
"""PushdownPlan: channel variant tagging and native calculated-channel targets."""

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.optimizer import (
    PushdownContext,
    PushdownPlan,
    QueryOptimizer,
    ResamplePushdownRule,
)
from tests.conftest import spark

_LONG = PushdownContext(T.LongType(), T.LongType())


def _channel(name, **kwargs):
    return TimeSeriesSelector(TagSelector("channel_name") == name, **kwargs)


def _cc(expr, name):
    return CalculatedChannel(expr, {"channel_name": name})


@pytest.fixture
def selectors():
    return {"pushed": _channel("rpm"), "pushed_2": _channel("spd"), "raw": _channel("temp")}


def test_channel_variants(spark, selectors):
    pushed, pushed_2, raw = selectors["pushed"], selectors["pushed_2"], selectors["raw"]
    plan = QueryOptimizer().optimize(
        [pushed.resample(1e6), pushed_2.resample(5e5), raw.max()], _LONG
    )
    pushed_variant = plan.variants_by_selector[pushed.selector_id]
    pushed_2_variant = plan.variants_by_selector[pushed_2.selector_id]
    cases = {
        "none": ([], [0]),
        "null": ([None], [0]),
        "raw": ([raw.selector_id], [0]),
        "pushed": ([pushed.selector_id], list(pushed_variant)),
        "raw_and_pushed": ([raw.selector_id, pushed.selector_id], [0]),
        "pushed_and_null": ([pushed.selector_id, None], list(pushed_variant)),
        "pushed_twice": ([pushed.selector_id, pushed.selector_id], list(pushed_variant)),
        "two_pushed": (
            [pushed.selector_id, pushed_2.selector_id],
            list(pushed_variant + pushed_2_variant),
        ),
    }
    df = spark.createDataFrame(
        [(name, ids) for name, (ids, _) in cases.items()],
        schema="name string, selector_ids array<long>",
    )
    actual = {
        r["name"]: r["variants"]
        for r in df.select(
            "name", plan.channel_variants(F.col("selector_ids")).alias("variants")
        ).collect()
    }
    assert actual == {name: expected for name, (_, expected) in cases.items()}


def test_channel_selector_ids(spark):
    df = spark.createDataFrame(
        [(0, [7, None, 7, 3]), (1, []), (2, None)], schema="i int, selector_ids array<int>"
    )
    result = df.select("i", PushdownPlan.channel_selector_ids("selector_ids").alias("ids"))
    assert result.schema["ids"].dataType.elementType == T.LongType()
    assert {r["i"]: r["ids"] for r in result.collect()} == {0: [7, 3], 1: [], 2: None}


class TestNativeTargets:
    def test_whole_expression_pushdowns(self, selectors):
        rpm, spd = selectors["pushed"], selectors["pushed_2"]
        aliased = _channel("vehicle_speed", uses_alias=True)
        channels = [
            _cc(rpm.resample(1e6), "rpm_1s"),
            _cc(rpm.resample(1e6), "rpm_1s_copy"),
            _cc(spd.resample(5e5), "spd"),
            _cc(aliased.resample(1e6), "vehicle_speed"),
        ]
        plan = QueryOptimizer().optimize(channels, _LONG)
        v_1s = plan.variant_ids[("resample", (("sample_rate", 1e6),))]
        v_half = plan.variant_ids[("resample", (("sample_rate", 5e5),))]
        assert plan.native_targets(channels) == {
            v_1s: {
                rpm.selector_id: [
                    (channels[0].channel_id, False),
                    (channels[1].channel_id, False),
                ],
                aliased.selector_id: [(channels[3].channel_id, True)],
            },
            v_half: {spd.selector_id: [(channels[2].channel_id, False)]},
        }

    def test_needs_every_selection_to_be_a_whole_pushdown(self, selectors):
        rpm, spd = selectors["pushed"], selectors["pushed_2"]
        native = _cc(rpm.resample(1e6), "rpm_1s")
        for other in (_cc(spd.resample(1e6) * 2, "spd_x2"), spd.resample(1e6)):
            selections = [native, other]
            plan = QueryOptimizer().optimize(selections, _LONG)
            assert not plan.is_empty
            assert plan.native_targets(selections) is None
        assert QueryOptimizer().optimize([native], _LONG).native_targets([]) is None

    def test_rule_with_custom_decode_is_not_native(self, selectors):
        class _CustomDecode(ResamplePushdownRule):
            def decode(self, rows, pushdown, cache):
                return super().decode(rows, pushdown, cache)

        channels = [_cc(selectors["pushed"].resample(1e6), "rpm_1s")]
        plan = QueryOptimizer([_CustomDecode()]).optimize(channels, _LONG)
        assert not plan.is_empty
        assert plan.native_targets(channels) is None
