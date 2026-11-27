# pylint: disable=missing-function-docstring
"""Parity of the Spark condition pushdown (``channel op scalar``) with the pandas evaluation."""

import fractions
import math
from dataclasses import dataclass

import numpy as np
import numpy.testing as nptest
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.optimizer import (
    CHUNK_ROWS,
    FACTOR_COL,
    PRESENCE_VARIANT,
    VARIANT_COL,
    Classifier,
    ConditionPushdownRule,
    PushdownContext,
    PushdownPlan,
    QueryOptimizer,
    apply_pushdowns,
    finish_rows,
)
from impulse_query_engine.analyze.query.solvers.default_solver import TimeSeriesCache
from impulse_query_engine.analyze.query.solvers.native_solve import native_solve_rows
from impulse_query_engine.model.series.intervals import Intervals
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import (
    CONFIG,
    channel_rows,
    random_series,
)

_CH = 7
_NO_FACTOR = object()


def _channel(uses_alias: bool = False) -> TimeSeriesSelector:
    return TimeSeriesSelector(TagSelector("channel_name") == "x", uses_alias=uses_alias)


_X = _channel()

_COMPARISONS = {
    "gt": lambda x: x > 1.5,
    "ge": lambda x: x >= 2,
    "lt": lambda x: x < 2,
    "le": lambda x: x <= 1,
    "eq": lambda x: x == 2,
    "ne": lambda x: x != 2,
    "ne_nan": lambda x: x != math.nan,
    "gt_nan": lambda x: x > math.nan,
    "lt_inf": lambda x: x < math.inf,
    "gt_minus_inf": lambda x: x > -math.inf,
    "scalar_first": lambda x: 1.5 < x,
    "scalar_first_eq": lambda x: 2 == x,
}

# (tstarts, tends, values) around merging: pieces, trailing and interior points, a point
# bridging two true samples, gaps, NaN and null values and times, overlaps.
_CASES = {
    "run": ([0, 1, 2, 3], [1, 2, 3, 4], [3, 3, 0, 3]),
    "gap": ([0, 1, 5, 6], [1, 2, 6, 7], [3, 3, 3, 3]),
    "trailing_point": ([0, 1, 2], [1, 2, 2], [3, 3, 3]),
    "trailing_point_alone": ([0, 1, 2], [1, 2, 2], [0, 0, 3]),
    "interior_point_true": ([0, 1, 1, 2], [1, 1, 2, 3], [0, 3, 0, 0]),
    "point_bridges": ([0, 1, 1, 2], [1, 1, 2, 3], [3, 0, 3, 0]),
    "point_after_true": ([0, 1, 1], [1, 1, 2], [3, 3, 0]),
    "unselected_point_between": ([0, 1, 1, 2], [1, 1, 2, 3], [3, 0, 3, 3]),
    "nan_values": ([0, 1, 2, 3], [1, 2, 3, 4], [3, math.nan, None, 3]),
    "single_point": ([5], [5], [3]),
    "single_sample": ([5], [6], [3]),
    "nothing_true": ([0, 1], [1, 2], [0, 0]),
    "all_true_long": (list(range(30)), list(range(1, 31)), [3] * 30),
    "alternating": (list(range(20)), list(range(1, 21)), [3, 0] * 10),
    "overlaps": ([0, 1, 1, 3, 3], [2, 2, 4, 3, 3], [3, 3, 3, 3, 3]),
    "negative_duration": ([0, 2, 3], [2, 1, 5], [3, 3, 3]),
    "negative_zero": ([0, 1, 2], [1, 2, 3], [-0.0, 0.0, 3]),
}

# Cases with null or NaN times, on a double time column.
_TIME_CASES = {
    "null_tstart_before_true": ([None, 0, 1], [100, 1, 2], [3, 3, 3]),
    "null_tstart_alone": ([None, 0], [5, 1], [3, 0]),
    "nan_tend": ([0, 1, 2, 3], [1, math.nan, 3, 4], [3, 3, 3, 3]),
    "nan_tend_last": ([0, 1], [1, math.nan], [3, 3]),
    "nan_tstart_last": ([0, 1, math.nan], [1, 2, 3], [3, 3, 3]),
}


@dataclass
class _Outcome:
    plan: PushdownPlan
    expected: dict
    decoded: dict
    native: dict
    shipped_rows: dict


def _evaluate(
    spark,
    expr,
    series_by_cid,
    time_type=T.LongType(),
    chunk_rows=None,
    factor=_NO_FACTOR,
    value_type=T.DoubleType(),
) -> _Outcome:
    """Evaluate *expr* on ``{cid: (tstarts, tends, values)}`` both ways.

    Pandas: the UDF's cache on the raw rows.  Spark: the pipeline, its finishing
    pass, then the UDF's cache on the shipped rows and the native solve() rows.
    """
    expr = expr.alias("x_cond")
    plan = QueryOptimizer().optimize(
        [expr], PushdownContext(time_type, time_type, value_type=value_type)
    )
    (selector,) = expr.get_selectors()
    sid = selector.selector_id
    df, variants = channel_rows(
        spark, {(cid, _CH, 1): s for cid, s in series_by_cid.items()}, time_type
    )
    df = df.withColumn("value", F.col("value").cast(value_type))
    if factor is not _NO_FACTOR:
        variants = variants.withColumn(FACTOR_COL, F.lit(factor).cast("double"))
    shipped = apply_pushdowns(
        df, variants, plan, CONFIG, include_raw=False, chunk_rows=chunk_rows or CHUNK_ROWS
    )
    finished = finish_rows(shipped.repartition(3, "container_id"), plan, CONFIG).withColumn(
        "selector_ids", F.array(F.lit(sid).cast("long"))
    )
    if factor is not _NO_FACTOR:
        finished = finished.withColumn("conversion_factor", F.lit(factor).cast("double"))

    def by_container(frame):
        pdf = frame.toPandas()
        return {int(cid): group.copy() for cid, group in pdf.groupby("container_id")}

    raw = df.withColumn("selector_ids", F.array(F.lit(sid).cast("long")))
    if factor is not _NO_FACTOR:
        raw = raw.withColumn("conversion_factor", F.lit(factor).cast("double"))
    expected = {
        cid: expr.build(TimeSeriesCache(pdf, CONFIG.col_map))
        for cid, pdf in by_container(raw).items()
    }
    shipped_pdfs = by_container(finished)
    decoded = {
        cid: expr.build(TimeSeriesCache(pdf, CONFIG.col_map, pushed=plan.pushed_nodes))
        for cid, pdf in shipped_pdfs.items()
    }
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("x_cond", T.ArrayType(T.ArrayType(T.DoubleType()))),
        ]
    )
    native_hits = plan.native_selections([expr])
    native = {}
    if native_hits is not None:
        native = {
            r["container_id"]: r["x_cond"]
            for r in native_solve_rows(finished, schema, native_hits, [expr], CONFIG).collect()
        }
    rows = {cid: pdf[pdf[VARIANT_COL] != PRESENCE_VARIANT] for cid, pdf in shipped_pdfs.items()}
    return _Outcome(plan, expected, decoded, native, rows)


def _udf_encoded(intervals: Intervals) -> list:
    """``Intervals.get_data()`` as the UDF's Arrow output encodes it (NaN as null)."""
    return [[None if math.isnan(x) else float(x) for x in pair] for pair in intervals.get_data()]


def _assert_parity(outcome: _Outcome, label=""):
    assert outcome.plan.describe().startswith("variants [1=condition("), outcome.plan.describe()
    assert outcome.expected.keys() == outcome.decoded.keys(), label
    assert outcome.native.keys() == outcome.expected.keys(), label
    for cid, expected in outcome.expected.items():
        assert isinstance(expected, Intervals)
        actual = outcome.decoded[cid]
        assert isinstance(actual, Intervals), (label, cid)
        nptest.assert_array_equal(actual.tstarts, expected.tstarts, err_msg=f"{label} {cid}")
        nptest.assert_array_equal(actual.tends, expected.tends, err_msg=f"{label} {cid}")
        assert outcome.native[cid] == _udf_encoded(expected), (label, cid)


@pytest.mark.parametrize("comparison", list(_COMPARISONS))
@pytest.mark.parametrize("chunk_rows", [None, 1, 2, 3])
def test_cases(spark, comparison, chunk_rows):
    expr = _COMPARISONS[comparison](_X)
    series = dict(enumerate(_CASES.values()))
    _assert_parity(_evaluate(spark, expr, series, chunk_rows=chunk_rows), comparison)


@pytest.mark.parametrize("chunk_rows", [None, 1, 2])
def test_null_and_nan_times(spark, chunk_rows):
    series = dict(enumerate(_TIME_CASES.values()))
    for expr in (_X > 1, _X != 0):
        outcome = _evaluate(spark, expr, series, T.DoubleType(), chunk_rows=chunk_rows)
        _assert_parity(outcome, str(chunk_rows))


@pytest.mark.parametrize("chunk_rows", [None, 1, 3, 7])
@pytest.mark.parametrize("time_type", [T.LongType(), T.DoubleType()])
def test_random_series(spark, chunk_rows, time_type):
    rng = np.random.default_rng(31)
    series = {cid: random_series(rng, 60) for cid in range(6)}
    for expr in (_X > 1, _X <= 2, _X != 3, _X == 0):
        _assert_parity(_evaluate(spark, expr, series, time_type, chunk_rows=chunk_rows), str(expr))


@pytest.mark.parametrize("chunk_rows", [None, 2])
def test_random_series_with_nan_tends_and_overlaps(spark, chunk_rows):
    rng = np.random.default_rng(37)
    series = {cid: random_series(rng, 40, nan_tend=True) for cid in range(4)}
    _assert_parity(_evaluate(spark, _X > 1, series, T.DoubleType(), chunk_rows=chunk_rows))


@pytest.mark.parametrize("chunk_rows", [None, 2])
def test_ns_epoch_times(spark, chunk_rows):
    base = 1_700_000_000_123_456_789  # above 2**53
    tstarts = [base + i * 1_000_001 for i in range(12)]
    tends = [t + 1_000_001 for t in tstarts]
    values = [5, 5, 0, 5, 5, 5, 0, 0, 5, 5, 0, 5]
    outcome = _evaluate(spark, _X > 1, {1: (tstarts, tends, values)}, chunk_rows=chunk_rows)
    _assert_parity(outcome)


def test_long_values(spark):
    series = {1: ([0, 1, 2, 3], [1, 2, 3, 4], [1, 5, 5, 1])}
    _assert_parity(_evaluate(spark, _X > 2, series, value_type=T.LongType()))


@pytest.mark.parametrize("factor", [3.6, math.nan])
def test_unit_conversion_before_the_comparison(spark, factor):
    aliased = _channel(uses_alias=True)
    series = {1: ([0, 1, 2, 3], [1, 2, 3, 4], [1.0, 2.0, 0.5, 3.0])}
    outcome = _evaluate(spark, aliased > 3, series, factor=factor)
    _assert_parity(outcome)
    if factor == 3.6:
        # All but 0.5 exceed 3 after conversion; no raw value does.
        assert outcome.native[1] == [[0.0, 2.0], [3.0, 4.0]]


def test_null_factor_compares_raw_values(spark):
    aliased = _channel(uses_alias=True)
    series = {1: ([0, 1, 2, 3], [1, 2, 3, 4], [1.0, 2.0, 0.5, 3.0])}
    outcome = _evaluate(spark, aliased > 2.5, series, factor=None)
    _assert_parity(outcome)


def test_direct_and_aliased_comparisons_are_distinct_variants():
    rule = ConditionPushdownRule()
    plan = QueryOptimizer([rule]).optimize(
        [(_X > 1).alias("a"), (_channel(uses_alias=True) > 1).alias("b")],
        PushdownContext(T.LongType(), T.LongType()),
    )
    assert len(plan.variant_ids) == 2


def test_merged_across_chunks_ships_final_intervals(spark):
    # One long true run per container is one interval, however many chunks it spans.
    n = 50
    series = {1: (list(range(n)), list(range(1, n + 1)), [3.0] * n)}
    outcome = _evaluate(spark, _X > 1, series, chunk_rows=10)
    _assert_parity(outcome)
    rows = outcome.shipped_rows[1]
    assert (rows["tstart"].tolist(), rows["tend"].tolist()) == ([0], [n])


def test_container_where_the_condition_never_holds_is_present(spark):
    series = {1: ([0, 1], [1, 2], [0.0, 0.0]), 2: ([0, 1], [1, 2], [3.0, 3.0])}
    outcome = _evaluate(spark, _X > 1, series)
    _assert_parity(outcome)
    assert outcome.native[1] == []
    assert len(outcome.shipped_rows[1]) == 0


def test_channel_with_only_nan_tstarts_is_present(spark):
    # NaN tstarts all go to a channel's last chunk; the presence row still comes along.
    nan = math.nan
    series = {1: ([nan] * 6, [1, 2, 3, 4, 5, 6], [0.0] * 6), 2: ([0, 1], [1, 2], [3.0, 3.0])}
    outcome = _evaluate(spark, _X > 1, series, T.DoubleType(), chunk_rows=2)
    _assert_parity(outcome)
    assert outcome.native[1] == []


@pytest.mark.parametrize(
    "threshold", [80, 80.5, np.float64(1.5), np.float32(0.1), np.int64(3), 2**70, math.inf]
)
def test_numeric_thresholds_are_matched(threshold):
    pushdown = Classifier([ConditionPushdownRule()])(_X > threshold)
    assert dict(pushdown.params)["threshold"] == float(threshold)


@pytest.mark.parametrize(
    "build",
    [
        lambda x: x > True,
        lambda x: x > x,
        lambda x: x.resample(1) > 2,
        lambda x: x > "1",
        # numpy compares these exactly or in extended precision, Spark in double.
        lambda x: x > fractions.Fraction(1, 3),
        lambda x: x > np.longdouble(1.5),
    ],
)
def test_not_matched(build):
    rule = ConditionPushdownRule()
    assert Classifier([rule])(build(_X)) is None


def test_points_in_time_selection_is_not_matched():
    poi = TimeSeriesSelector(
        TagSelector("channel_name") == "p", series_type=SeriesType.POINTS_IN_TIME
    )
    assert Classifier([ConditionPushdownRule()])(poi > 1) is None


def test_non_numeric_values_are_vetoed():
    plan = QueryOptimizer().optimize(
        [(_X > 1).alias("a")],
        PushdownContext(T.LongType(), T.LongType(), value_type=T.StringType()),
    )
    assert plan.is_empty


def test_result_type_and_native_value():
    rule = ConditionPushdownRule()
    plan = QueryOptimizer([rule]).optimize(
        [(_X > 1).alias("a")], PushdownContext(T.LongType(), T.LongType())
    )
    (pushdown,) = plan.pushdowns_by_variant.values()
    assert rule.result_type(pushdown) is Intervals
    assert rule.has_native_value(pushdown)
    assert not rule.emits_result_rows(pushdown)
    assert pushdown.describe() == "condition(op=gt, threshold=1.0, alias=False)"


def test_native_selections_are_all_or_nothing():
    context = PushdownContext(T.LongType(), T.LongType())
    y = TimeSeriesSelector(TagSelector("channel_name") == "y")

    def plan_of(selections):
        return QueryOptimizer().optimize(selections, context), selections

    plan, selections = plan_of([(_X > 1).alias("a"), (y <= 2).alias("b")])
    hits = plan.native_selections(selections)
    assert [variant for _, variant in hits] == [1, 2]

    # A raw selection, a nested condition and a resample selection keep the UDF.
    for other in (y.max().alias("b"), (y > 1).debounce(5).alias("b"), y.resample(2).alias("b")):
        plan, selections = plan_of([(_X > 1).alias("a"), other])
        assert plan.variant_of(selections[0]) is not None
        assert plan.native_selections(selections) is None


def test_every_use_of_the_channel_must_be_pushed():
    context = PushdownContext(T.LongType(), T.LongType())
    plan = QueryOptimizer().optimize([(_X > 1).alias("a"), _X.max().alias("b")], context)
    assert plan.is_empty
    both = QueryOptimizer().optimize(
        [(_X > 1).alias("a"), _X.resample(2).mean().alias("b")], context
    )
    assert both.describe().startswith(
        "variants [1=condition(op=gt, threshold=1.0, alias=False), 2=resample("
    )
