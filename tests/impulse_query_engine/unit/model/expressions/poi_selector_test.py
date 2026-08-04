"""PoiSelector unit tests — the data-free type probe and the TSAL contract.

These run with no Spark and no data: everything hinges on ``build(EmptyTimeSeriesCache())``
returning an empty :class:`PointsInTime`, which is what lets events validate a POI
expression at construction time.

Under Option D, POI is a pure occurrence log — a ``PoiSelector`` always evaluates to
``PointsInTime`` (no attribute / ``PointsInTimeSeries`` path). Row filtering uses the
dedicated POI predicate DSL (``q.poi_metric(...)`` in a ``.having(...)`` predicate), not
``TagExpression``.
"""

# pylint: disable=missing-function-docstring, redefined-outer-name

import numpy as np
import pandas as pd
import pytest

from impulse_query_engine.analyze.metadata.poi_expression import (
    PoiMetricSelector,
    PoiPredicate,
    poi_kind_predicate,
)
from impulse_query_engine.analyze.metadata.poi_selector import PoiSelector, poi_expr
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression
from impulse_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.model.series.points_in_time import PointsInTime


class _FakePoiCache(SeriesCache):
    """A minimal cache that serves a fixed POI frame, ignoring the selector."""

    def __init__(self, rows: pd.DataFrame):
        self._rows = rows

    def resolve(self, selection):
        return []

    def resolve_poi(self, selection):
        return self._rows

    def load_blob(self, mid, cid, uses_alias: bool = False):
        return None


# --- the type probe: build on an empty cache yields an empty PointsInTime --------------


def test_build_on_empty_cache_is_points_in_time():
    poi = PoiSelector(poi_expr(poi_type="aeb"))
    result = poi.build(EmptyTimeSeriesCache())
    assert isinstance(result, PointsInTime)
    assert len(result) == 0


def test_evaluation_type_is_always_points_in_time():
    assert PoiSelector(poi_expr(poi_type="aeb")).evaluation_type() is PointsInTime
    # ...even with a having predicate attached
    filtered = PoiSelector(poi_expr(poi_type="aeb")).having(PoiMetricSelector("duration") > 5)
    assert filtered.evaluation_type() is PointsInTime


# --- require_evaluation_type gating (the PointsInTimeEvent / consumer path) -------------


def test_require_evaluation_type_accepts_points():
    PoiSelector(poi_expr(poi_type="aeb")).require_evaluation_type(
        PointsInTime, owner="PointsInTimeEvent"
    )  # must not raise


# --- the sibling contract: stays out of the channel pipeline ---------------------------


def test_get_selectors_empty_keeps_poi_out_of_channel_pipeline():
    poi = PoiSelector(poi_expr(poi_type="aeb"))
    assert poi.get_selectors() == []
    assert TimeSeriesExpression.collect_selectors([poi]) == []


def test_get_poi_selectors_collects_self():
    poi = PoiSelector(poi_expr(poi_type="aeb"))
    assert poi.get_poi_selectors() == [poi]
    assert TimeSeriesExpression.collect_poi_selectors([poi]) == [poi]


def test_poi_selectors_collected_through_operators():
    a = PoiSelector(poi_expr(poi_type="aeb"))
    b = PoiSelector(poi_expr(poi_type="ldw"))
    composed = a & b
    assert len(TimeSeriesExpression.collect_poi_selectors([composed])) == 2


def test_collect_poi_selectors_dedups_by_id():
    a = PoiSelector(poi_expr(poi_type="aeb"))
    a2 = PoiSelector(poi_expr(poi_type="aeb"))  # same predicate → same selector_id
    assert len(TimeSeriesExpression.collect_poi_selectors([a, a2])) == 1


# --- having(): fluent, immutable, definition-hash-distinct -----------------------------


def test_having_returns_new_selector_and_is_immutable():
    base = PoiSelector(poi_expr(poi_type="aeb"))
    filtered = base.having(PoiMetricSelector("duration") > 5)
    assert filtered is not base
    # base is unchanged
    assert base.selector_id == PoiSelector(poi_expr(poi_type="aeb")).selector_id


def test_having_changes_definition_hash():
    base = PoiSelector(poi_expr(poi_type="aeb"))
    filtered = base.having(PoiMetricSelector("duration") > 5)
    assert filtered.selector_id != base.selector_id


def test_having_chains():
    base = PoiSelector(poi_expr(poi_type="aeb"))
    two = base.having(PoiMetricSelector("duration") > 5).having(PoiMetricSelector("occurrences") == 1)
    # both predicates recorded, distinct from single-having
    one = base.having(PoiMetricSelector("duration") > 5)
    assert two.selector_id != one.selector_id
    assert "occurrences" in two.required_tags()
    assert "duration" in two.required_tags()


def test_required_tags_exposes_referenced_poi_columns():
    poi = PoiSelector(poi_expr(poi_type="aeb")).having(PoiMetricSelector("duration") > 5)
    assert poi.required_tags() == {"poi_type", "duration"}


# --- identity / definition hashing -----------------------------------------------------


def test_selector_id_stable_for_same_predicate():
    a = PoiSelector(poi_expr(poi_type="aeb"))
    b = PoiSelector(poi_expr(poi_type="aeb"))
    assert a.selector_id == b.selector_id
    assert isinstance(a.selector_id, int)


def test_str_includes_predicate():
    poi = PoiSelector(poi_expr(poi_type="aeb")).having(PoiMetricSelector("duration") > 5)
    s = str(poi)
    assert "poi_type" in s and "duration" in s


def test_dtype_is_points_in_time():
    assert PoiSelector(poi_expr(poi_type="aeb")).dtype() == PointsInTime.empty().dtype()


# --- build against real POI rows -------------------------------------------------------


def test_build_dedups_and_sorts_instants():
    rows = pd.DataFrame({"ts": [30, 10, 10, 20]})
    result = PoiSelector(poi_expr(poi_type="aeb")).build(_FakePoiCache(rows))
    assert isinstance(result, PointsInTime)
    np.testing.assert_array_equal(result.tstarts, np.array([10.0, 20.0, 30.0]))


def test_build_empty_frame_is_empty_points():
    result = PoiSelector(poi_expr(poi_type="aeb")).build(_FakePoiCache(pd.DataFrame({"ts": []})))
    assert isinstance(result, PointsInTime)
    assert len(result) == 0


# --- the predicate DSL (poi_expression) ------------------------------------------------


def test_poi_kind_predicate_requires_a_filter():
    with pytest.raises(ValueError, match="at least one kind filter"):
        poi_kind_predicate()


def test_poi_kind_predicate_ands_equalities_and_lists_columns():
    pred = poi_kind_predicate(poi_type="aeb", event_type="computed")
    assert isinstance(pred, PoiPredicate)
    assert pred.required_columns() == {"poi_type", "event_type"}


def test_poi_metric_predicate_no_cast_needed():
    # a numeric comparison builds a PoiPredicate referencing the column; no cast_type given
    pred = PoiMetricSelector("duration") > 5
    assert isinstance(pred, PoiPredicate)
    assert pred.required_columns() == {"duration"}


def test_predicate_str_is_stable():
    p1 = str(PoiMetricSelector("duration") > 5)
    p2 = str(PoiMetricSelector("duration") > 5)
    assert p1 == p2
    assert "duration" in p1 and ">" in p1
