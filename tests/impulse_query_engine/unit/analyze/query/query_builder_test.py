# pylint: disable=missing-function-docstring, redefined-outer-name
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    PoiValueType,
    SeriesType,
    TimeSeriesSelector,
)
from impulse_query_engine.model.series import Intervals
from impulse_query_engine.model.series.points_in_time import PointsInTime
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries
from impulse_query_engine.model.series.sample_series import SampleSeries


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


def test_pointsInTimeSeries_dtype(narrow_db):
    query = narrow_db.query
    ts = TimeSeriesSelector(TagSelector("name") == "test")
    expr = ts.where(ts.rising_edge())
    query.select(expr)
    result_objects, result_dtypes = query._determine_result_objects_dtypes()
    assert len(result_objects) == len(result_dtypes) == 1
    assert isinstance(result_dtypes[0], T.ArrayType)
    assert isinstance(result_objects[0], PointsInTimeSeries)


# ---------------------------------------------------------------------------
# TimeSeriesExpression.evaluation_type (builds against an empty cache, no Spark)
# ---------------------------------------------------------------------------
def _eval_ts():
    return TimeSeriesSelector(TagSelector("name") == "test")


def test_evaluation_type_sample_series():
    assert _eval_ts().evaluation_type() is SampleSeries


def test_evaluation_type_intervals():
    assert (_eval_ts() > 0).evaluation_type() is Intervals


def test_evaluation_type_points_in_time():
    assert _eval_ts().rising_edge().evaluation_type() is PointsInTime


def test_evaluation_type_points_in_time_series():
    ts = _eval_ts()
    assert ts.where(ts.rising_edge()).evaluation_type() is PointsInTimeSeries


def test_evaluation_type_scalar():
    # scalar aggregations evaluate to a (numpy) float; np.float64 subclasses float
    assert issubclass(_eval_ts().mean().evaluation_type(), float)


# ---------------------------------------------------------------------------
# TimeSeriesExpression.require_evaluation_type (raises on mismatch)
# ---------------------------------------------------------------------------
def test_require_evaluation_type_passes_on_match():
    # returns None and does not raise when the type matches
    assert _eval_ts().require_evaluation_type(SampleSeries, owner="Test") is None


def test_require_evaluation_type_raises_on_mismatch():
    with pytest.raises(
        ValueError, match="Owner requires an expression that evaluates to Intervals"
    ):
        _eval_ts().require_evaluation_type(Intervals, owner="Owner")


def test_require_evaluation_type_reports_actual_and_example():
    with pytest.raises(ValueError, match="got SampleSeries"):
        _eval_ts().require_evaluation_type(Intervals, owner="Owner", example="channel > 0")


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


# ---------------------------------------------------------------------------
# QueryBuilder.poi_channel — dtype accepts the enum OR its string value
# ---------------------------------------------------------------------------
class TestPoiChannelDtypeArg:
    """``poi_channel(dtype=...)`` must accept both ``PoiValueType`` and the plain
    string value (``"double"`` / ``"string"``). A regression guard: a plain
    ``dtype="string"`` used to be stored verbatim (a ``str``, not the enum), so the
    ``is PoiValueType.STRING`` identity checks silently fell through and a string
    POI channel behaved as numeric — blowing up on the first string comparison.
    """

    def test_default_dtype_is_double(self, narrow_db):
        sel = narrow_db.query.poi_channel(channel_name="DTC_count")
        assert sel.series_type is SeriesType.POINTS_IN_TIME
        assert sel.value_type is PoiValueType.DOUBLE

    def test_enum_string_dtype(self, narrow_db):
        sel = narrow_db.query.poi_channel(channel_name="DTC", dtype=PoiValueType.STRING)
        assert sel.value_type is PoiValueType.STRING

    def test_plain_string_dtype_coerced_to_enum(self, narrow_db):
        # the design-doc form: poi_channel(..., dtype="string")
        sel = narrow_db.query.poi_channel(channel_name="DTC", dtype="string")
        assert sel.value_type is PoiValueType.STRING

    def test_plain_string_double_dtype_coerced_to_enum(self, narrow_db):
        sel = narrow_db.query.poi_channel(channel_name="DTC_count", dtype="double")
        assert sel.value_type is PoiValueType.DOUBLE

    def test_invalid_dtype_raises(self, narrow_db):
        with pytest.raises(ValueError):
            narrow_db.query.poi_channel(channel_name="DTC", dtype="int")

    def test_string_poi_types_as_struct_regardless_of_arg_form(self, narrow_db):
        # both arg forms must produce an identical string-typed result dtype
        enum_sel = narrow_db.query.poi_channel(channel_name="DTC", dtype=PoiValueType.STRING)
        str_sel = narrow_db.query.poi_channel(channel_name="DTC", dtype="string")
        assert enum_sel.dtype() == str_sel.dtype()
        # string POI serializes as array<struct<tstart,value>>, not array<array<double>>
        assert isinstance(str_sel.dtype(), T.ArrayType)
        assert isinstance(str_sel.dtype().elementType, T.StructType)

    def test_string_poi_equality_evaluates_to_points_in_time(self, narrow_db):
        # dtype="string" must yield a string series so `== "code"` works at plan time
        dtc = narrow_db.query.poi_channel(channel_name="DTC", dtype="string")
        assert (dtc == "P0301").evaluation_type() is PointsInTime

    def test_string_poi_mean_rejected_at_build_time(self, narrow_db):
        dtc = narrow_db.query.poi_channel(channel_name="DTC", dtype="string")
        with pytest.raises(TypeError, match="string-valued"):
            dtc.mean().evaluation_type()
