from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pyspark.sql.types as T
import pytest

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from mda_query_engine.analyze.query.events import SequenceOfEventsExpression
from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from mda_query_engine.model.series.intervals import Intervals

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MockIntervalsExpression(TimeSeriesExpression):
    """A TimeSeriesExpression that returns a pre-built Intervals from build()."""

    def __init__(self, intervals: Intervals, tags: set[str] = None, selector=None):
        super().__init__(is_single_signal=False)
        self._intervals = intervals
        self._tags = tags or set()
        self._selector = selector

    def build(self, cache):
        return self._intervals

    def required_tags(self) -> set[str]:
        return set(self._tags)

    def get_required_tag_exprs(self):
        return set()

    def get_selector_expr(self):
        return self._selector

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return []

    def __str__(self) -> str:  # pragma: no cover
        return "MockIntervalsExpression"


def _make(tstarts, tends) -> MockIntervalsExpression:
    """Convenience: create a MockIntervalsExpression with given interval arrays."""
    return MockIntervalsExpression(Intervals(np.array(tstarts), np.array(tends)))


# ---------------------------------------------------------------------------
# Build / sequence algorithm tests
# ---------------------------------------------------------------------------


def test_two_expressions_single_sequence():
    """I_0=[(0,10)], I_1=[(5,15)] → overlap → one sequence (0,15)."""
    expr = SequenceOfEventsExpression([_make([0], [10]), _make([5], [15])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 1
    assert result.tstarts[0] == pytest.approx(0)
    assert result.tends[0] == pytest.approx(15)


def test_two_expressions_no_sequence():
    """I_0=[(0,10)], I_1=[(11,20)] → gap → no sequence."""
    expr = SequenceOfEventsExpression([_make([0], [10]), _make([11], [20])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 0


def test_two_expressions_boundary():
    """I_0=[(0,10)], I_1=[(10,20)] → tstart==tend → boundary counts as sequence (0,20)."""
    expr = SequenceOfEventsExpression([_make([0], [10]), _make([10], [20])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 1
    assert result.tstarts[0] == pytest.approx(0)
    assert result.tends[0] == pytest.approx(20)


def test_multiple_chains():
    """I_0=[(0,10),(20,30)], I_1=[(5,15),(25,35)] → two independent sequences."""
    expr = SequenceOfEventsExpression([_make([0, 20], [10, 30]), _make([5, 25], [15, 35])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 2
    starts = sorted(result.tstarts.tolist())
    ends = sorted(result.tends.tolist())
    assert starts == pytest.approx([0, 20])
    assert ends == pytest.approx([15, 35])


def test_three_levels():
    """I_0=[(0,10)], I_1=[(5,20)], I_2=[(15,30)] → chain (0→20→30)."""
    expr = SequenceOfEventsExpression([_make([0], [10]), _make([5], [20]), _make([15], [30])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 1
    assert result.tstarts[0] == pytest.approx(0)
    assert result.tends[0] == pytest.approx(30)


def test_empty_first_level():
    """If level 0 is empty, result must be empty."""
    expr = SequenceOfEventsExpression([_make([], []), _make([5], [15])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 0


def test_empty_second_level():
    """If any later level is empty, result must be empty."""
    expr = SequenceOfEventsExpression([_make([0], [10]), _make([], [])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 0


def test_empty_expressions_raises():
    """Passing an empty list to __init__ must raise ValueError."""
    with pytest.raises(ValueError, match="at least one expression"):
        SequenceOfEventsExpression([])


def test_single_expression():
    """With only one expression all its intervals are returned as-is."""
    expr = SequenceOfEventsExpression([_make([0, 20], [10, 30])])
    result = expr.build(EmptyTimeSeriesCache())
    assert len(result) == 2
    assert result.tstarts.tolist() == pytest.approx([0, 20])
    assert result.tends.tolist() == pytest.approx([10, 30])


def test_build_with_empty_cache():
    """build() with EmptyTimeSeriesCache on empty child must not raise."""
    expr = SequenceOfEventsExpression([_make([], []), _make([], [])])
    result = expr.build(EmptyTimeSeriesCache())
    assert isinstance(result, Intervals)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# dtype
# ---------------------------------------------------------------------------


def test_dtype():
    """dtype() must equal ArrayType(ArrayType(DoubleType()))."""
    expr = SequenceOfEventsExpression([_make([0], [10])])
    assert expr.dtype() == T.ArrayType(T.ArrayType(T.DoubleType()))


# ---------------------------------------------------------------------------
# required_tags / get_required_tag_exprs
# ---------------------------------------------------------------------------


def test_required_tags():
    """required_tags() returns the union of all children's required_tags()."""
    e1 = MockIntervalsExpression(Intervals.empty(), tags={"a"})
    e2 = MockIntervalsExpression(Intervals.empty(), tags={"b"})
    expr = SequenceOfEventsExpression([e1, e2])
    assert expr.required_tags() == {"a", "b"}


def test_required_tags_overlap():
    """required_tags() deduplicates overlapping tags."""
    e1 = MockIntervalsExpression(Intervals.empty(), tags={"a", "c"})
    e2 = MockIntervalsExpression(Intervals.empty(), tags={"b", "c"})
    expr = SequenceOfEventsExpression([e1, e2])
    assert expr.required_tags() == {"a", "b", "c"}


def test_get_required_tag_exprs_union():
    """get_required_tag_exprs() returns union from children."""
    tag_expr_a = MagicMock()
    tag_expr_b = MagicMock()

    class _Expr(MockIntervalsExpression):
        def __init__(self, tag_exprs):
            super().__init__(Intervals.empty())
            self._tag_exprs = tag_exprs

        def get_required_tag_exprs(self):
            return set(self._tag_exprs)

    expr = SequenceOfEventsExpression([_Expr([tag_expr_a]), _Expr([tag_expr_b])])
    result = expr.get_required_tag_exprs()
    assert result == {tag_expr_a, tag_expr_b}


# ---------------------------------------------------------------------------
# get_selector_expr
# ---------------------------------------------------------------------------


def test_get_selectors():
    """get_selectors() collects leaf selectors from all child expressions."""
    sel_a = TimeSeriesSelector(TagSelector("name") == "a")
    sel_b = TimeSeriesSelector(TagSelector("name") == "b")
    expr = SequenceOfEventsExpression([sel_a, sel_b])
    result = expr.get_selectors()
    assert len(result) == 2
    assert sel_a in result
    assert sel_b in result


def test_get_selectors_nested():
    """get_selectors() recurses through nested SequenceOfEventsExpression children."""
    sel_a = TimeSeriesSelector(TagSelector("name") == "a")
    sel_b = TimeSeriesSelector(TagSelector("name") == "b")
    op = sel_a + sel_b
    expr = SequenceOfEventsExpression([op])
    result = expr.get_selectors()
    assert len(result) == 2
    assert sel_a in result
    assert sel_b in result


def test_get_selectors_single_child():
    """get_selectors() with a single selector child returns that selector."""
    sel = TimeSeriesSelector(TagSelector("name") == "signal")
    expr = SequenceOfEventsExpression([sel])
    result = expr.get_selectors()
    assert result == [sel]


def test_get_selector_expr():
    """get_selector_expr() combines children with | (OR)."""
    selector = TimeSeriesSelector(TagSelector("name") == "test")
    expr = SequenceOfEventsExpression([selector, selector])
    combined = expr.get_selector_expr()
    assert combined is not None


def test_get_selector_expr_single():
    """get_selector_expr() with a single child returns that child's selector."""
    sentinel = object()
    e = MockIntervalsExpression(Intervals.empty(), selector=sentinel)
    expr = SequenceOfEventsExpression([e])
    assert expr.get_selector_expr() is sentinel


# ---------------------------------------------------------------------------
# __str__
# ---------------------------------------------------------------------------


def test_str():
    """str() contains 'SequenceOfEventsExpression'."""
    expr = SequenceOfEventsExpression([_make([0], [10])])
    assert "SequenceOfEventsExpression" in str(expr)


# ---------------------------------------------------------------------------
# Query-level tests with shared fixtures
# ---------------------------------------------------------------------------


def test_sequence_query_overlapping_ranges_has_event_instance_for_container_1(
    spark, basic_narrow_db
):
    """Container 1 should have at least one instance for overlapping speed ranges."""
    query = basic_narrow_db.query
    veh_spd = query.channel(channel_name="Vehicle Speed Sensor")

    seq_expr = SequenceOfEventsExpression(
        [
            (veh_spd > 10) & (veh_spd < 15),
            (veh_spd > 9) & (veh_spd < 18),
        ]
    ).alias("sequence_event")

    metric_container_id = query.metric("container_id")
    df = (
        query.where(metric_container_id == 1)
        .select(seq_expr)
        .solve(spark, solver=BasicNarrowSolver(spark))
    )

    rows = df.select("sequence_event").collect()
    assert len(rows) == 1
    assert rows[0]["sequence_event"] is not None
    assert len(rows[0]["sequence_event"]) > 0


def test_sequence_query_nested_less_than_has_event_instance_for_container_1(
    spark, basic_narrow_db
):
    """Container 1 should have at least one instance for nested less-than ranges."""
    query = basic_narrow_db.query
    veh_spd = query.channel(channel_name="Vehicle Speed Sensor")

    seq_expr = SequenceOfEventsExpression(
        [
            veh_spd < 15,
            veh_spd < 12,
        ]
    ).alias("sequence_event")

    metric_container_id = query.metric("container_id")
    df = (
        query.where(metric_container_id == 1)
        .select(seq_expr)
        .solve(spark, solver=BasicNarrowSolver(spark))
    )

    rows = df.select("sequence_event").collect()
    assert len(rows) == 1
    assert rows[0]["sequence_event"] is not None
    assert len(rows[0]["sequence_event"]) > 0
