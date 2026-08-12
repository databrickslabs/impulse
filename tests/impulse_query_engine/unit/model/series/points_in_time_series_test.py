"""Tests for PointsInTimeSeries"""

# pylint: disable=missing-function-docstring, redefined-outer-name
import numpy as np
import numpy.testing as nptest
import pyspark.sql.types as T
import pytest

from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.points_in_time import PointsInTime
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries
from impulse_query_engine.model.series.sample_series import SampleSeries

# --- core ---------------------------------------------------------------------------------------


def test_init_length_mismatch():
    with pytest.raises(AssertionError):
        PointsInTimeSeries([0, 1], [1])


def test_len_empty():
    assert len(PointsInTimeSeries.empty()) == 0


def test_len():
    assert len(PointsInTimeSeries([0, 1, 2], [3, 4, 5])) == 3


def test_get_data_empty():
    assert PointsInTimeSeries.empty().get_data() == []


def test_get_data():
    pts = PointsInTimeSeries([0, 1], [10, 20])
    assert pts.get_data() == [[0.0, 10.0], [1.0, 20.0]]


def test_dtype():
    assert PointsInTimeSeries.empty().dtype() == T.ArrayType(T.ArrayType(T.DoubleType()))


def test_start_end_time_empty():
    pts = PointsInTimeSeries.empty()
    assert np.isnan(pts.start_time())
    assert np.isnan(pts.end_time())


def test_start_end_time():
    pts = PointsInTimeSeries([2, 5, 9], [1, 2, 3])
    assert pts.start_time() == 2
    assert pts.end_time() == 9


def test_str():
    assert str(PointsInTimeSeries([0, 2], [1, 2])) == "<PointsInTimeSeries(0.0..cnt:2..2.0)>"
    assert str(PointsInTimeSeries.empty()) == "<PointsInTimeSeries(nan..cnt:0..nan)>"


def test_to_points_in_time():
    pts = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    pit = pts.to_points_in_time()
    assert isinstance(pit, PointsInTime)
    nptest.assert_array_equal(pit.tstarts, [0, 1, 2])


# --- arithmetic ---------------------------------------------------------------------------------


def test_add_scalar():
    pts = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    result = pts + 5
    nptest.assert_array_equal(result.tstarts, [0, 1, 2])
    nptest.assert_array_equal(result.values, [15, 25, 35])


def test_radd_scalar():
    pts = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    nptest.assert_array_equal((5 + pts).values, [15, 25, 35])


def test_rsub_scalar():
    pts = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    nptest.assert_array_equal((100 - pts).values, [90, 80, 70])


def test_add_series_common_timestamps():
    p1 = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    p2 = PointsInTimeSeries([1, 2, 3], [1, 2, 3])
    result = p1 + p2
    nptest.assert_array_equal(result.tstarts, [1, 2])
    nptest.assert_array_equal(result.values, [21, 32])


def test_div_series_common_timestamps():
    p1 = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    p2 = PointsInTimeSeries([1, 2, 3], [1, 2, 3])
    nptest.assert_array_equal((p1 / p2).values, [20, 15])


def test_op_series_disjoint_timestamps_empty():
    p1 = PointsInTimeSeries([0, 1], [10, 20])
    p2 = PointsInTimeSeries([5, 6], [1, 2])
    assert len(p1 + p2) == 0


def test_add_sample_series_operand():
    # Sampling a SampleSeries (valid over intervals) at the point series' instants.
    pts = PointsInTimeSeries([5, 15, 25], [100, 200, 300])
    s = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    result = pts + s
    nptest.assert_array_equal(result.tstarts, [5, 15, 25])
    nptest.assert_array_equal(result.values, [101, 202, 303])


# --- comparisons (return PointsInTime) ----------------------------------------------------------


def test_gt_scalar_returns_points_in_time():
    pts = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    result = pts > 15
    assert isinstance(result, PointsInTime)
    nptest.assert_array_equal(result.tstarts, [1, 2])


def test_eq_series():
    p1 = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    p2 = PointsInTimeSeries([1, 2, 3], [20, 99, 3])
    result = p1 == p2
    assert isinstance(result, PointsInTime)
    nptest.assert_array_equal(result.tstarts, [1])


# --- aggregations -------------------------------------------------------------------------------


def test_aggregations():
    pts = PointsInTimeSeries([0, 1, 2, 3], [10, 20, 30, 40])
    assert pts.sum() == 100
    assert pts.mean() == 25
    assert pts.min() == 10
    assert pts.max() == 40
    assert pts.count() == 4


def test_aggregations_empty():
    pts = PointsInTimeSeries.empty()
    assert np.isnan(pts.sum())
    assert np.isnan(pts.mean())
    assert np.isnan(pts.min())
    assert np.isnan(pts.max())
    assert pts.count() == 0


# --- string values ------------------------------------------------------------------------------
# String-valued series support sampling and equality only; arithmetic, ordering
# and numeric reductions are rejected. Timestamps stay numeric regardless.


def test_string_values_stored_as_object_with_numeric_timestamps():
    pts = PointsInTimeSeries([1, 2, 3], ["P108B", "U0046", "P108B"])
    assert pts._is_string is True
    assert pts.values.dtype == object
    assert pts.tstarts.dtype == np.float64
    nptest.assert_array_equal(pts.values, ["P108B", "U0046", "P108B"])


def test_empty_series_defaults_to_numeric():
    # No observed value type -> numeric (backward-compatible default).
    assert PointsInTimeSeries.empty()._is_string is False


def test_numeric_series_is_not_string():
    assert PointsInTimeSeries([0, 1], [10, 20])._is_string is False


def test_string_eq_scalar_returns_points_in_time():
    pts = PointsInTimeSeries([1, 2, 3], ["P108B", "U0046", "P108B"])
    result = pts == "P108B"
    assert isinstance(result, PointsInTime)
    nptest.assert_array_equal(result.tstarts, [1, 3])


def test_string_ne_scalar_returns_points_in_time():
    pts = PointsInTimeSeries([1, 2, 3], ["P108B", "U0046", "P108B"])
    nptest.assert_array_equal((pts != "P108B").tstarts, [2])


def test_string_eq_series_matches_on_value_and_timestamp():
    p1 = PointsInTimeSeries([1, 2, 3], ["A", "B", "C"])
    p2 = PointsInTimeSeries([2, 3, 4], ["X", "C", "C"])
    # Common timestamps {2,3}; values equal only at t=3 ("C" == "C").
    nptest.assert_array_equal((p1 == p2).tstarts, [3])


def test_string_synchronized_with_sample_series_samples_values():
    pts = PointsInTimeSeries([5, 15, 25], ["a", "b", "c"])
    s = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    a, b = pts.synchronized(s)
    nptest.assert_array_equal(a.tstarts, [5, 15, 25])
    nptest.assert_array_equal(a.values, ["a", "b", "c"])
    nptest.assert_array_equal(b.values, [1, 2, 3])


def test_string_get_data_pairs_double_timestamp_with_string_value():
    pts = PointsInTimeSeries([1, 2], ["P108B", "U0046"])
    assert pts.get_data() == [[1.0, "P108B"], [2.0, "U0046"]]


def test_string_dtype_is_struct_of_double_and_string():
    pts = PointsInTimeSeries([1, 2], ["P108B", "U0046"])
    assert pts.dtype() == T.ArrayType(
        T.StructType(
            [
                T.StructField("tstart", T.DoubleType()),
                T.StructField("value", T.StringType()),
            ]
        )
    )


@pytest.mark.parametrize(
    "op",
    [
        lambda p: p + "x",
        lambda p: "x" + p,
        lambda p: p - 1,
        lambda p: 1 - p,
        lambda p: p * 2,
        lambda p: p / 2,
    ],
)
def test_string_arithmetic_raises(op):
    pts = PointsInTimeSeries([1, 2], ["A", "B"])
    with pytest.raises(TypeError, match="string-valued"):
        op(pts)


@pytest.mark.parametrize(
    "op",
    [
        lambda p: p > "A",
        lambda p: p >= "A",
        lambda p: p < "Z",
        lambda p: p <= "Z",
    ],
)
def test_string_ordering_comparison_raises(op):
    pts = PointsInTimeSeries([1, 2], ["A", "B"])
    with pytest.raises(TypeError, match="string-valued"):
        op(pts)


@pytest.mark.parametrize("reduction", ["sum", "mean", "min", "max"])
def test_string_reductions_raise(reduction):
    pts = PointsInTimeSeries([1, 2], ["A", "B"])
    with pytest.raises(TypeError, match="string-valued"):
        getattr(pts, reduction)()


def test_string_count_is_allowed():
    # count is structural (not value-dependent), so it works for strings.
    assert PointsInTimeSeries([1, 2, 3], ["A", "B", "C"]).count() == 3


# --- plane_sweep --------------------------------------------------------------------------------


def test_plane_sweep_vs_sample_series():
    pts = PointsInTimeSeries([0.5, 1.5, 2.5], [0, 0, 0])
    s = SampleSeries([0, 1, 2], [1, 2, 3], [10, 20, 30])
    assert PointsInTimeSeries.plane_sweep(pts, s) == [(0, 0), (1, 1), (2, 2)]


def test_plane_sweep_trailing_zero_duration_closed():
    # last sample [3,3) is a closed point: a query exactly at 3 matches it.
    pts = PointsInTimeSeries([0.5, 3.0], [0, 0])
    s = SampleSeries([0, 3], [1, 3], [10, 30])
    assert PointsInTimeSeries.plane_sweep(pts, s) == [(0, 0), (1, 1)]


def test_plane_sweep_vs_intervals():
    pts = PointsInTimeSeries([5, 15, 25], [0, 0, 0])
    intervals = Intervals([0], [20])
    assert PointsInTimeSeries.plane_sweep(pts, intervals) == [(0, 0), (1, 0)]


def test_plane_sweep_vs_points_in_time():
    pts = PointsInTimeSeries([0, 1, 2], [0, 0, 0])
    pit = PointsInTime([1, 2])
    assert PointsInTimeSeries.plane_sweep(pts, pit) == [(1, 0), (2, 1)]


def test_plane_sweep_vs_points_in_time_series():
    p1 = PointsInTimeSeries([0, 1, 2], [0, 0, 0])
    p2 = PointsInTimeSeries([1, 2, 3], [0, 0, 0])
    assert PointsInTimeSeries.plane_sweep(p1, p2) == [(1, 0), (2, 1)]


def test_plane_sweep_empty():
    pts = PointsInTimeSeries([0, 1], [0, 0])
    assert PointsInTimeSeries.plane_sweep(pts, SampleSeries.empty()) == []
    assert PointsInTimeSeries.plane_sweep(PointsInTimeSeries.empty(), pts) == []


def test_plane_sweep_unsupported_type():
    pts = PointsInTimeSeries([0, 1], [0, 0])
    with pytest.raises(NotImplementedError):
        PointsInTimeSeries.plane_sweep(pts, 5)


# --- synchronized -------------------------------------------------------------------------------


def test_synchronized_with_sample_series():
    pts = PointsInTimeSeries([5, 15, 25], [100, 200, 300])
    s = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    a, b = pts.synchronized(s)
    nptest.assert_array_equal(a.tstarts, [5, 15, 25])
    nptest.assert_array_equal(b.tstarts, [5, 15, 25])
    nptest.assert_array_equal(a.values, [100, 200, 300])
    nptest.assert_array_equal(b.values, [1, 2, 3])


def test_synchronized_drops_out_of_interval_points():
    pts = PointsInTimeSeries([5, 50], [100, 200])
    s = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    a, b = pts.synchronized(s)
    nptest.assert_array_equal(a.tstarts, [5])
    nptest.assert_array_equal(a.values, [100])
    nptest.assert_array_equal(b.values, [1])


def test_synchronized_with_points_in_time_series():
    p1 = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    p2 = PointsInTimeSeries([1, 2, 3], [1, 2, 3])
    a, b = p1.synchronized(p2)
    nptest.assert_array_equal(a.tstarts, [1, 2])
    nptest.assert_array_equal(a.values, [20, 30])
    nptest.assert_array_equal(b.values, [1, 2])


def test_synchronized_rejects_valueless_operand():
    pts = PointsInTimeSeries([0, 1], [10, 20])
    with pytest.raises(NotImplementedError):
        pts.synchronized(Intervals([0], [2]))
    with pytest.raises(NotImplementedError):
        pts.synchronized(PointsInTime([0, 1]))


# --- synchronized_all ---------------------------------------------------------------------------


def test_synchronized_all():
    p1 = PointsInTimeSeries([0, 1, 2], [10, 20, 30])
    p2 = PointsInTimeSeries([1, 2, 3], [1, 2, 3])
    p3 = PointsInTimeSeries([1, 2, 3], [7, 8, 9])
    res = p1.synchronized_all([p2, p3])
    assert len(res) == 3
    for series in res:
        nptest.assert_array_equal(series.tstarts, [1, 2])
    nptest.assert_array_equal(res[0].values, [20, 30])
    nptest.assert_array_equal(res[1].values, [1, 2])
    nptest.assert_array_equal(res[2].values, [7, 8])


def test_synchronized_all_with_sample_series():
    pts = PointsInTimeSeries([5, 15, 25], [100, 200, 300])
    s = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    res = pts.synchronized_all([s])
    assert len(res) == 2
    nptest.assert_array_equal(res[0].tstarts, [5, 15, 25])
    nptest.assert_array_equal(res[0].values, [100, 200, 300])
    nptest.assert_array_equal(res[1].values, [1, 2, 3])
