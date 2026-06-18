"""Tests for the query-engine PointValueAggregator.build()."""

from unittest.mock import MagicMock

import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.model.series.points_in_time import PointsInTime
from impulse_query_engine.model.series.sample_series import SampleSeries


def _sel(name):
    """A real channel selector (no Spark needed for tree-walking)."""
    return TimeSeriesSelector(TagSelector("channel_name") == name)


def _agg_with_real_exprs(with_event=True):
    event = _sel("Distance").rising_edges() if with_event else None
    return PointValueAggregator(
        input_expressions=[_sel("Vehicle Speed"), _sel("Engine RPM")],
        event_expression=event,
    )


def _expr_returning(obj):
    """An expression stub whose build() returns a fixed object."""
    expr = MagicMock()
    expr.build.return_value = obj
    return expr


def _series():
    # valid over [0,1)->10, [1,2)->20, [2,3)->30
    return SampleSeries(tstarts=[0.0, 1.0, 2.0], tends=[1.0, 2.0, 3.0], values=[10.0, 20.0, 30.0])


def test_dtype_struct():
    agg = PointValueAggregator(input_expressions=[], event_expression=None)
    dtype = agg.dtype()
    assert isinstance(dtype, T.StructType)
    assert [f.name for f in dtype.fields] == ["point_timestamps", "values"]


def test_build_samples_each_series_at_points():
    points = PointsInTime([0.5, 1.5, 2.5])
    agg = PointValueAggregator(
        input_expressions=[_expr_returning(_series())],
        event_expression=_expr_returning(points),
    )

    point_timestamps, values = agg.build(cache=MagicMock())

    assert point_timestamps == [[0.5, 1.5, 2.5]]
    assert values == [[10.0, 20.0, 30.0]]


def test_build_per_series_indexing():
    points = PointsInTime([0.5, 2.5])
    agg = PointValueAggregator(
        input_expressions=[_expr_returning(_series()), _expr_returning(_series())],
        event_expression=_expr_returning(points),
    )

    point_timestamps, values = agg.build(cache=MagicMock())

    # one inner list per input series
    assert point_timestamps == [[0.5, 2.5], [0.5, 2.5]]
    assert values == [[10.0, 30.0], [10.0, 30.0]]


def test_build_drops_points_outside_coverage():
    # 5.0 is outside the series coverage [0, 3) and must be omitted for that series
    points = PointsInTime([0.5, 5.0])
    agg = PointValueAggregator(
        input_expressions=[_expr_returning(_series())],
        event_expression=_expr_returning(points),
    )

    point_timestamps, values = agg.build(cache=MagicMock())

    assert point_timestamps == [[0.5]]
    assert values == [[10.0]]


def test_build_empty_series_yields_empty_lists():
    points = PointsInTime([0.5, 1.5])
    agg = PointValueAggregator(
        input_expressions=[_expr_returning(SampleSeries.empty())],
        event_expression=_expr_returning(points),
    )

    point_timestamps, values = agg.build(cache=MagicMock())

    assert point_timestamps == [[]]
    assert values == [[]]


# ---------------------------------------------------------------------------
# Expression-tree helpers (no cache / no Spark) — used by the solver to resolve
# channels and serialize the definition.
# ---------------------------------------------------------------------------
def test_str_contains_class_name():
    assert "PointValueAggregator" in str(_agg_with_real_exprs())


def test_required_tags_unions_inputs_and_event():
    assert _agg_with_real_exprs().required_tags() == {"channel_name"}


def test_get_required_tag_exprs_nonempty():
    exprs = _agg_with_real_exprs().get_required_tag_exprs()
    assert isinstance(exprs, set) and len(exprs) >= 1


def test_get_selector_expr_combines_inputs_and_event():
    assert _agg_with_real_exprs().get_selector_expr() is not None


def test_get_selectors_includes_event_selector():
    # two input selectors + one from the event expression
    assert len(_agg_with_real_exprs().get_selectors()) == 3


def test_tree_helpers_without_event_expression():
    agg = _agg_with_real_exprs(with_event=False)
    assert agg.required_tags() == {"channel_name"}
    assert len(agg.get_required_tag_exprs()) >= 1
    assert agg.get_selector_expr() is not None
    assert len(agg.get_selectors()) == 2  # no event selector
