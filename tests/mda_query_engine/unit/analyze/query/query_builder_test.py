# pylint: disable=missing-function-docstring, redefined-outer-name
import pyspark.sql.types as T

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from mda_query_engine.model.series import Intervals
from mda_query_engine.model.series.points_in_time import PointsInTime
from mda_query_engine.model.series.sample_series import SampleSeries


def test_timeseries_selector_dtype(narrow_db):
    query = narrow_db.query
    expr = TimeSeriesSelector(TagSelector("name") == "test")
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 1
    assert isinstance(result_dtypes[0], T.BinaryType)
    assert isinstance(result_objects[0], SampleSeries)


def test_interval_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    expr = (ts > 0) & (ts < 1)
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 1
    assert isinstance(result_dtypes[0], T.ArrayType)
    assert isinstance(result_objects[0], Intervals)


def test_pointsInTime_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    expr = ts.rising_edge()
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 1
    assert isinstance(result_dtypes[0], T.ArrayType)
    assert isinstance(result_objects[0], PointsInTime)


def test_timeseries_where_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    expr = ts.where(ts > 0)
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 1
    assert isinstance(result_dtypes[0], T.BinaryType)
    assert isinstance(result_objects[0], SampleSeries)


def test_multiple_selections_dtype(narrow_db):
    query = narrow_db.query
    ts1 = TimeSeriesSelector(TagSelector("name") == "test_1")
    ts2 = TimeSeriesSelector(TagSelector("name") == "test_2")
    expr1 = ts1.where(ts1 > 0)
    expr2 = ts2 > 0
    query.select(expr1, expr2)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 2
    assert isinstance(result_dtypes[0], T.BinaryType)
    assert isinstance(result_objects[0], SampleSeries)
    assert isinstance(result_dtypes[1], T.ArrayType)
    assert isinstance(result_objects[1], Intervals)


def test_empy_selection(narrow_db):
    query = narrow_db.query
    query.select()
    result_objects, result_dtypes = query._determine_result_objects_dtypes()

    assert query.selections == []
    assert len(result_objects) == 0
    assert len(result_dtypes) == 0


# --- Tests for SampleSeries list-of-lists return type ---


def test_sample_series_dtype_is_binary():
    ss = SampleSeries([0, 1, 2], [1, 2, 3], [10.0, 20.0, 30.0])
    dtype = ss.dtype()
    assert isinstance(dtype, T.BinaryType)


def test_sample_series_get_data_structure():
    ss = SampleSeries([0, 1, 2], [1, 2, 3], [10.0, 20.0, 30.0])
    data = ss.get_data()
    assert isinstance(data, list)
    assert len(data) == 3
    for row in data:
        assert isinstance(row, list)
        assert len(row) == 3
        assert all(isinstance(v, float) for v in row)


def test_sample_series_get_data_values():
    ss = SampleSeries([0, 1], [1, 2], [42.0, 99.0])
    data = ss.get_data()
    assert data == [[0.0, 1.0, 42.0], [1.0, 2.0, 99.0]]


def test_sample_series_get_data_empty():
    ss = SampleSeries.empty()
    data = ss.get_data()
    assert data == []


def test_timeseries_selector_returns_binary_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    query.select(ts)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    dtype = result_dtypes[0]
    assert isinstance(dtype, T.BinaryType)


def test_timeseries_where_returns_binary_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    expr = ts.where(ts > 0)
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    dtype = result_dtypes[0]
    assert isinstance(dtype, T.BinaryType)


def test_timeseries_selector_dtype_matches_sample_series_dtype():
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    ss = SampleSeries.empty()
    assert ts.dtype() == ss.dtype()
