"""Tests for the query-engine PointValueAggregator.build()."""

from unittest.mock import MagicMock

import pyspark.sql.types as T

from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.model.series.points_in_time import PointsInTime
from impulse_query_engine.model.series.sample_series import SampleSeries


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
