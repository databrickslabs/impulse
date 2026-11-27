# pylint: disable=missing-function-docstring
"""Parity of comparisons of a resampled channel (``x.resample(r) op k``) with pandas.

The condition rule absorbs the resample and computes both in one pass.
"""

import math
import re

import numpy as np
import numpy.testing as nptest
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.query.optimizer import (
    CHUNK_ROWS,
    PRESENCE_VARIANT,
    VARIANT_COL,
    Classifier,
    ConditionPushdownRule,
    PushdownContext,
    QueryOptimizer,
    ResamplePushdownRule,
    apply_pushdowns,
    finish_rows,
)
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.condition_rule_test import (
    _CASES as _SAMPLE_CASES,
    _CH,
    _TIME_CASES,
    _X,
    _assert_parity,
    _channel,
    _evaluate,
    _evaluate_all,
    _udf_encoded,
)
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import (
    CONFIG,
    channel_rows,
    random_series,
)
from tests.impulse_query_engine.unit.analyze.query.optimizer.resample_rule_test import (
    _CASES as _RESAMPLE_CASES,
    _as_seconds,
)

_COMPARISONS = {
    "gt": lambda x: x > 1.5,
    "le": lambda x: x <= 2,
    "eq": lambda x: x == 2,
    "ne": lambda x: x != 2,
    "ne_nan": lambda x: x != math.nan,
    "scalar_first": lambda x: 2.5 < x,
}

# The resample rule's cases (trailing, interior and isolated points, gaps, grids off the
# sample boundaries), as (tstarts, tends, values).
_RESAMPLED = {name: case[:3] for name, case in _RESAMPLE_CASES.items()}
# The condition rule's cases around merging; the resample matches pandas unless
# intervals overlap.
_MERGING = {
    name: case
    for name, case in _SAMPLE_CASES.items()
    if name not in ("overlaps", "negative_duration")
}


def _absorbed(outcome) -> None:
    (pushdown,) = outcome.plan.pushdowns_by_variant.values()
    assert pushdown.describe().startswith("condition(") and " absorbing resample(" in (
        pushdown.describe()
    ), outcome.plan.describe()


def _assert_close_parity(outcome, label=""):
    """Parity up to one ulp of the times: numpy may fuse the grid's multiply-add."""
    assert outcome.expected.keys() == outcome.decoded.keys() == outcome.native.keys(), label
    for cid, expected in outcome.expected.items():
        actual = outcome.decoded[cid]
        assert isinstance(actual, Intervals) and len(actual) == len(expected), (label, cid)
        nptest.assert_array_max_ulp(actual.tstarts, expected.tstarts, maxulp=1)
        nptest.assert_array_max_ulp(actual.tends, expected.tends, maxulp=1)
        native = np.array(outcome.native[cid], dtype=np.float64).reshape(-1, 2)
        nptest.assert_array_max_ulp(native[:, 0], expected.tstarts, maxulp=1)
        nptest.assert_array_max_ulp(native[:, 1], expected.tends, maxulp=1)


# Few samples per chunk put chunk boundaries inside buckets and next to points.
@pytest.mark.parametrize("comparison", list(_COMPARISONS))
@pytest.mark.parametrize("chunk_rows", [None, 1, 3])
@pytest.mark.parametrize("rate", [1, 3])
def test_resample_cases(spark, comparison, chunk_rows, rate):
    expr = _COMPARISONS[comparison](_X.resample(rate))
    series = dict(enumerate(_RESAMPLED.values()))
    outcome = _evaluate(spark, expr, series, chunk_rows=chunk_rows)
    _absorbed(outcome)
    _assert_parity(outcome, comparison)


@pytest.mark.parametrize("chunk_rows", [None, 2])
@pytest.mark.parametrize("rate", [1, 2])
def test_merging_cases(spark, chunk_rows, rate):
    series = dict(enumerate(_MERGING.values()))
    for expr in (_X.resample(rate) > 1.5, _X.resample(rate) != 3):
        outcome = _evaluate(spark, expr, series, chunk_rows=chunk_rows)
        _absorbed(outcome)
        _assert_parity(outcome, str(rate))


@pytest.mark.parametrize("chunk_rows", [None, 1, 3, 7])
@pytest.mark.parametrize("time_type", [T.LongType(), T.DoubleType()])
def test_random_series(spark, chunk_rows, time_type):
    rng = np.random.default_rng(41)
    series = {cid: random_series(rng, n) for cid, n in enumerate([1, 2, 40, 60, 60, 80])}
    for expr in (_X.resample(1) > 1, _X.resample(3) <= 2, _X.resample(2) != 3):
        outcome = _evaluate(spark, expr, series, time_type, chunk_rows=chunk_rows)
        _assert_parity(outcome, str(expr))


@pytest.mark.filterwarnings("ignore:All-NaN slice encountered:RuntimeWarning")
@pytest.mark.parametrize("chunk_rows", [None, 2, 7])
def test_nan_tends(spark, chunk_rows):
    rng = np.random.default_rng(43)
    series = {}
    while len(series) < 8:
        tstarts, tends, values = random_series(rng, int(rng.integers(2, 60)), nan_tend=True)
        samples = ([float(t) for t in tstarts], tends, values)
        try:
            SampleSeries(*samples).resample(2)
        except ValueError:
            # A block without a finite tend has no end (see the next test).
            continue
        if any(np.isnan(tends)):
            series[len(series)] = samples
    for expr in (_X.resample(2) > 1, _X.resample(2) != 0):
        _assert_parity(_evaluate(spark, expr, series, T.DoubleType(), chunk_rows=chunk_rows))


def _spark_only(spark, expr, series_by_cid, time_type):
    """The rows Spark ships for *expr*, without evaluating it in pandas."""
    plan = QueryOptimizer().optimize([expr.alias("x")], PushdownContext(time_type, time_type))
    df, variants = channel_rows(
        spark, {(cid, _CH, 1): s for cid, s in series_by_cid.items()}, time_type
    )
    shipped = apply_pushdowns(df, variants, plan, CONFIG, include_raw=False)
    return finish_rows(shipped, plan, CONFIG).collect()


@pytest.mark.filterwarnings("ignore:All-NaN slice encountered:RuntimeWarning")
def test_block_without_a_finite_tend_fails_like_pandas(spark):
    # The comparison never holds, yet the resample cannot grid the block: pandas fails,
    # and so must Spark rather than return no intervals.
    nan = float("nan")
    samples = ([0.0, 1.0, 2.0], [nan, 2.0, 3.0], [1.0, 2.0, 3.0])
    with pytest.raises(ValueError):
        SampleSeries(*samples).resample(1)
    with pytest.raises(Exception, match="resample:"):
        _spark_only(spark, _X.resample(1) > 100, {1: samples}, T.DoubleType())


@pytest.mark.filterwarnings("ignore:All-NaN slice encountered:RuntimeWarning")
@pytest.mark.parametrize("name", list(_TIME_CASES))
def test_null_and_nan_times(spark, name):
    # The resample cannot place a sample without a tstart on its grid.  Where pandas
    # gets a result, Spark gets the same or fails, never other intervals.  (Where
    # pandas fails, the resample on its own still gets a result in Spark.)
    series = {1: _TIME_CASES[name]}
    expr = _X.resample(1) > 1
    try:
        SampleSeries(*_TIME_CASES[name]).resample(1)
    except ValueError:
        pytest.skip("pandas cannot resample these samples")
    try:
        _spark_only(spark, expr, series, T.DoubleType())
    except Exception as error:  # pylint: disable=broad-except
        assert "resample:" in str(error)
        return
    _assert_parity(_evaluate(spark, expr, series, T.DoubleType()), name)


@pytest.mark.parametrize("chunk_rows", [None, 7])
def test_ns_epoch_longs_above_2_pow_53(spark, chunk_rows):
    # Integral grids: exact for every numpy build.
    rng = np.random.default_rng(47)
    base = 1_700_000_000_123_456_789
    steps = rng.integers(900_000, 1_100_000, size=400)  # ~1 ms, jittered
    times = base + np.concatenate([[0], np.cumsum(steps)])
    values = np.repeat(rng.integers(0, 2, size=40) * 5.0, 10).tolist()
    series = {1: (times[:-1].tolist(), times[1:].tolist(), values)}
    for rate in (10_000_007, 100_000_000):
        outcome = _evaluate(spark, _X.resample(rate) > 1, series, chunk_rows=chunk_rows)
        _assert_parity(outcome, str(rate))
        assert len(outcome.expected[1]) > 1


@pytest.mark.parametrize("chunk_rows", [None, 1, 8])
def test_seconds_as_double(spark, chunk_rows):
    rng = np.random.default_rng(53)
    series = {cid: _as_seconds(random_series(rng, n)) for cid, n in enumerate([2, 40, 90, 150])}
    for rate in (0.1, 0.25, 0.6):
        outcome = _evaluate(spark, _X.resample(rate) > 1, series, T.DoubleType(), chunk_rows)
        _absorbed(outcome)
        _assert_close_parity(outcome, str(rate))


@pytest.mark.parametrize("factor", [3.6, math.nan])
def test_unit_conversion_before_the_comparison(spark, factor):
    aliased = _channel(uses_alias=True)
    series = {1: ([0, 1, 2, 3], [1, 2, 3, 4], [1.0, 2.0, 0.5, 3.0])}
    outcome = _evaluate(spark, aliased.resample(2) > 3, series, factor=factor)
    _assert_parity(outcome)
    if factor == 3.6:
        # The grid points 0 and 2 hold 2.0 and 0.5: only 2.0 exceeds 3 once converted.
        assert outcome.native[1] == [[0.0, 2.0]]


def test_plain_and_resampled_comparisons_of_one_channel(spark):
    rng = np.random.default_rng(59)
    series = {cid: random_series(rng, 50) for cid in range(4)}
    exprs = [_X > 1, _X.resample(3) > 1, _X.resample(3) <= 1, _X.resample(2) > 1]
    outcomes = _evaluate_all(spark, exprs, series, chunk_rows=3)
    assert len(outcomes[0].plan.variant_ids) == 4
    for expr, outcome in zip(exprs, outcomes, strict=True):
        _assert_parity(outcome, str(expr))


def test_true_run_across_chunks_ships_few_pieces(spark):
    # Buckets of a true run join within each chunk; the finishing pass joins the chunks.
    n = 200
    series = {1: (list(range(n)), list(range(1, n + 1)), [3.0] * n)}
    outcome = _evaluate(spark, _X.resample(2) > 1, series, chunk_rows=40)
    _assert_parity(outcome)
    assert outcome.native[1] == [[0.0, float(n)]]
    assert outcome.pieces <= math.ceil(n / 40)


def test_variants_and_description():
    context = PushdownContext(T.LongType(), T.LongType())
    exprs = [
        _X.resample(1e6) > 4000,
        _X.resample(1_000_000) > 4000,
        _X.resample(2e6) > 4000,
        _X > 4000,
        _X.resample(1e6),
    ]
    plan = QueryOptimizer().optimize([e.alias(f"s{i}") for i, e in enumerate(exprs)], context)
    variants = [plan.variant_of(e)[1] for e in exprs]
    assert variants == [1, 1, 2, 3, 4]
    assert plan.pushdowns_by_variant[1].describe() == (
        "condition(op=gt, threshold=4000.0, alias=False) "
        "absorbing resample(sample_rate=1000000.0)"
    )
    # The absorbed resample is computed by the comparison, not on its own.
    assert plan.variant_of(exprs[0].args[0]) is None
    assert plan.pushdowns_by_variant[1].consumed == (_X,)


def test_aliased_resample_is_its_own_variant():
    context = PushdownContext(T.LongType(), T.LongType())
    aliased = _channel(uses_alias=True)
    plan = QueryOptimizer().optimize(
        [(_X.resample(2) > 1).alias("a"), (aliased.resample(2) > 1).alias("b")], context
    )
    assert len(plan.variant_ids) == 2


def test_unsupported_resample_falls_back():
    rule = ConditionPushdownRule()
    # A fractional rate on integral times runs the whole expression in pandas.
    expr = (_X.resample(2.5) > 1).alias("a")
    plan = QueryOptimizer().optimize([expr], PushdownContext(T.LongType(), T.LongType()))
    assert plan.is_empty
    # Values the comparison cannot compute keep the resample pushed on its own.
    expr = (_X.resample(2) > 1).alias("a")
    plan = QueryOptimizer().optimize(
        [expr], PushdownContext(T.LongType(), T.LongType(), value_type=T.StringType())
    )
    assert plan.describe().startswith("variants [1=resample("), plan.describe()
    assert plan.variant_of(expr.args[0]) is not None
    # Without the resample rule, the operand is not a pushdown to absorb.
    assert Classifier([rule])(_X.resample(2) > 1) is None


def test_raw_use_of_the_channel_blocks_the_composition():
    context = PushdownContext(T.LongType(), T.LongType())
    plan = QueryOptimizer().optimize(
        [(_X.resample(2) > 1).alias("a"), _X.max().alias("b")], context
    )
    assert plan.is_empty


def test_resample_of_a_composition_is_not_absorbed():
    # Only a resample of the channel itself is absorbed.
    pushdown = Classifier([ResamplePushdownRule(), ConditionPushdownRule()])(
        (_X * 2).resample(2) > 1
    )
    assert pushdown is None


def test_native_value_is_the_udf_encoding(spark):
    series = {1: ([0, 1, 2, 3], [1, 2, 3, 4], [3.0, 0.0, 3.0, 3.0])}
    outcome = _evaluate(spark, _X.resample(1) > 1, series)
    _assert_parity(outcome)
    assert outcome.native[1] == _udf_encoded(outcome.expected[1]) == [[0.0, 1.0], [2.0, 4.0]]


def _optimized_plan(spark, exprs) -> str:
    plan = QueryOptimizer().optimize(
        [e.alias(f"s{i}") for i, e in enumerate(exprs)],
        PushdownContext(T.LongType(), T.LongType()),
    )
    df, variants = channel_rows(spark, {(1, _CH, 1): ([0, 1], [1, 2], [1.0, 2.0])})
    result = apply_pushdowns(df, variants, plan, CONFIG, include_raw=False, chunk_rows=CHUNK_ROWS)
    result = result.where(F.col(VARIANT_COL) != PRESENCE_VARIANT)
    return result._jdf.queryExecution().executedPlan().toString()


def test_only_linear_window_frames(spark):
    plan = _optimized_plan(spark, [_X.resample(2) > 1])
    assert "specifiedwindowframe(RowFrame, currentrow$(), unboundedfollowing$())" not in plan


def test_no_sort_beyond_the_resample_alone(spark):
    # The joins of buckets run in the order the pipeline already sorts the kept samples in.
    def sorts(plan):
        return len(re.findall(r"\bSort \[", plan))

    resample_only = _optimized_plan(spark, [_X.resample(2)])
    composed = _optimized_plan(spark, [_X.resample(2) > 1])
    assert sorts(composed) == sorts(resample_only)


def test_keep_filter_does_not_inline_the_rule_columns(spark):
    plan = QueryOptimizer().optimize(
        [(_X.resample(1) > 1).alias("a"), (_X.resample(2) > 1).alias("b")],
        PushdownContext(T.LongType(), T.LongType()),
    )
    df, variants = channel_rows(spark, {(1, _CH, 1): ([0, 1], [1, 2], [1.0, 2.0])})
    result = apply_pushdowns(df, variants, plan, CONFIG, include_raw=False)
    optimized = result._jdf.queryExecution().optimizedPlan().toString()
    conditions = re.findall(r"Filter (.*)", optimized)
    assert conditions and max(len(c) for c in conditions) < 1_000
