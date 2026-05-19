"""Tests for SampleSeries"""

# pylint: disable=missing-function-docstring, redefined-outer-name
import numpy as np
import numpy.testing as nptest

from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries


def test_init():
    sample_series = SampleSeries([0, 1], [1, 2], [3, 4])
    assert len(sample_series) == 2
    assert sample_series.duration_ms() == 2000.0


def test_start_time():
    sample_series = SampleSeries.empty()
    assert np.isnan(sample_series.start_time())
    sample_series = SampleSeries([0, 1], [1, 2], [3, 4])
    assert sample_series.start_time() == 0.0


def test_end_time():
    sample_series = SampleSeries.empty()
    assert np.isnan(sample_series.end_time())
    sample_series = SampleSeries([0, 1], [1, 2], [3, 4])
    assert sample_series.end_time() == 2.0


def test_sparse1():
    sample_series = SampleSeries.empty()
    assert len(sample_series) == len(sample_series.sparse())


def test_sparse2():
    sample_series = SampleSeries([0], [1], [1])
    sparse = sample_series.sparse()
    assert len(sparse) == 1
    nptest.assert_array_equal(sparse.tstarts, [0])
    nptest.assert_array_equal(sparse.tends, [1])
    nptest.assert_array_equal(sparse.values, [1])


def test_sparse3():
    sample_series = SampleSeries([0, 1], [1, 2], [1, 1])
    sparse = sample_series.sparse()
    assert len(sparse) == 1
    nptest.assert_array_equal(sparse.tstarts, [0])
    nptest.assert_array_equal(sparse.tends, [2])
    nptest.assert_array_equal(sparse.values, [1])


def test_sparse4():
    sample_series = SampleSeries([0, 1], [1, 2], [1, 2])
    sparse = sample_series.sparse()
    assert len(sparse) == 2
    nptest.assert_array_equal(sparse.tstarts, [0, 1])
    nptest.assert_array_equal(sparse.tends, [1, 2])
    nptest.assert_array_equal(sparse.values, [1, 2])


def test_sparse5():
    sample_series = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 2])
    sparse = sample_series.sparse()
    assert len(sparse) == 2
    nptest.assert_array_equal(sparse.tstarts, [0, 2])
    nptest.assert_array_equal(sparse.tends, [2, 3])
    nptest.assert_array_equal(sparse.values, [1, 2])


def test_sparse6():
    sample_series = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 2])
    sparse = sample_series.sparse()
    assert len(sparse) == 2
    nptest.assert_array_equal(sparse.tstarts, [0, 1])
    nptest.assert_array_equal(sparse.tends, [1, 3])
    nptest.assert_array_equal(sparse.values, [1, 2])


def test_sparse7():
    sample_series = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [1, 1, 2, 2])
    sparse = sample_series.sparse()
    assert len(sparse) == 2
    nptest.assert_array_equal(sparse.tstarts, [0, 2])
    nptest.assert_array_equal(sparse.tends, [2, 4])
    nptest.assert_array_equal(sparse.values, [1, 2])


def test_sparse_nan():
    sample_series = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [1, np.nan, np.nan, 2])
    sparse = sample_series.sparse()
    assert len(sparse) == 3
    nptest.assert_array_equal(sparse.tstarts, [0, 1, 3])
    nptest.assert_array_equal(sparse.tends, [1, 3, 4])
    nptest.assert_array_equal(sparse.values, [1, np.nan, 2])


def test_nan_ratio_empty():
    sample_series = SampleSeries.empty()
    assert np.isnan(sample_series.nan_ratio())


def test_nan_ratio1():
    sample_series = SampleSeries([0, 1], [1, 2], [1, np.nan])
    assert sample_series.nan_ratio() == 0.5


def test_nan_ratio2():
    sample_series = SampleSeries([0, 1], [1, 4], [1, np.nan])
    assert sample_series.nan_ratio() == 0.75


def test_nan_ratio3():
    sample_series = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [1, np.nan, np.nan, np.nan])
    assert sample_series.nan_ratio() == 0.75


def test_durations_empty():
    series = SampleSeries.empty()
    nptest.assert_array_equal(series.durations(), [])


def test_durations1():
    series = SampleSeries([0], [1], [1])
    nptest.assert_array_equal(series.durations(), [1])


def test_durations2():
    series = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    nptest.assert_array_equal(series.durations(), [1, 1, 1])


def test_durations3():
    series = SampleSeries([0, 3, 5], [1, 5, 10], [1, 1, 1])
    nptest.assert_array_equal(series.durations(), [1, 2, 5])


def test_sample_rate_empty():
    series = SampleSeries.empty()
    assert np.isnan(series.sample_rate())


def test_sample_rate1():
    series = SampleSeries([1, 2, 3], [2, 3, 4], [1, 1, 1])
    assert series.sample_rate() == 1.0


def test_sample_rate2():
    series = SampleSeries([1, 2, 8], [2, 8, 10], [1, 1, 1])
    assert series.sample_rate() == ((1.0 + 6.0 + 2.0) / 3.0)


def test_sync_sample_series_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    s1s, s2s = s1.synchronized(s2)
    assert len(s1s) == 0
    assert len(s2s) == 0
    s1 = SampleSeries([1], [2], [0])
    s1s, s2s = s1.synchronized(s2)
    assert len(s1s) == 0
    assert len(s2s) == 0
    s1 = SampleSeries([1], [2], [0])
    s1s, s2s = s2.synchronized(s1)
    assert len(s1s) == 0
    assert len(s2s) == 0


def test_sync_sample_series1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 2, 2])
    s1s, s2s = s1.synchronized(s2)
    nptest.assert_array_equal(s1s.tstarts, [0, 1, 2])
    nptest.assert_array_equal(s1s.tstarts, s2s.tstarts)
    nptest.assert_array_equal(s1s.tends, [1, 2, 3])
    nptest.assert_array_equal(s1s.tends, s2s.tends)
    nptest.assert_array_equal(s1s.values, [1, 1, 1])
    nptest.assert_array_equal(s2s.values, [2, 2, 2])


def test_sync_sample_series2():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([5, 15], [15, 25], [4, 5])
    s1s, s2s = s1.synchronized(s2)
    nptest.assert_array_equal(s1s.tstarts, s2s.tstarts)
    nptest.assert_array_equal(s1s.tends, s2s.tends)
    nptest.assert_array_equal([5, 10, 15, 20], s1s.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25], s1s.tends)
    nptest.assert_array_equal([1, 2, 2, 3], s1s.values)
    nptest.assert_array_equal([4, 4, 5, 5], s2s.values)


def test_sync_all_sample_series1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 2, 2])
    s1s, s2s = s1.synchronized_all([s2])
    nptest.assert_array_equal(s1s.tstarts, [0, 1, 2])
    nptest.assert_array_equal(s1s.tstarts, s2s.tstarts)
    nptest.assert_array_equal(s1s.tends, [1, 2, 3])
    nptest.assert_array_equal(s1s.tends, s2s.tends)
    nptest.assert_array_equal(s1s.values, [1, 1, 1])
    nptest.assert_array_equal(s2s.values, [2, 2, 2])


def test_sync_all_sample_series2():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 2, 2])
    s3 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 2, 2])
    s1s, s2s, s3s = s1.synchronized_all([s2, s3])
    nptest.assert_array_equal(s1s.tstarts, [0, 1, 2])
    nptest.assert_array_equal(s1s.tstarts, s2s.tstarts)
    nptest.assert_array_equal(s1s.tends, [1, 2, 3])
    nptest.assert_array_equal(s1s.tends, s2s.tends)
    nptest.assert_array_equal(s1s.values, [1, 1, 1])
    nptest.assert_array_equal(s2s.values, [2, 2, 2])
    nptest.assert_array_equal(s2s.tstarts, s3s.tstarts)
    nptest.assert_array_equal(s2s.tends, s3s.tends)


def test_sync_all_sample_series3():
    s1 = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [1, 1, 1, 1])
    s2 = SampleSeries([1, 2, 3, 4], [2, 3, 4, 5], [2, 2, 2, 2])
    s3 = SampleSeries([0, 2, 4], [2, 4, 6], [3, 3, 3])
    s1s, s2s, s3s = s1.synchronized_all([s2, s3])
    nptest.assert_array_equal(s1s.tstarts, [1, 2, 3])
    nptest.assert_array_equal(s1s.tstarts, s2s.tstarts)
    nptest.assert_array_equal(s1s.tends, [2, 3, 4])
    nptest.assert_array_equal(s1s.tends, s2s.tends)
    nptest.assert_array_equal(s1s.values, [1, 1, 1])
    nptest.assert_array_equal(s2s.values, [2, 2, 2])
    nptest.assert_array_equal(s2s.tstarts, s3s.tstarts)
    nptest.assert_array_equal(s2s.tends, s3s.tends)


def test_add_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    assert len(s1 + s2) == 0


def test_add1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [4, 5, 6])
    result = s1 + s2
    nptest.assert_array_equal([5, 7, 9], result.values)


def test_add2():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([5, 15, 25], [15, 25, 35], [4, 5, 6])
    result = s1 + s2
    nptest.assert_array_equal([5, 10, 15, 20, 25], result.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25, 30], result.tends)
    nptest.assert_array_equal([5, 6, 7, 8, 9], result.values)


def test_add3():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([15, 25], [16, 26], [4, 5])
    result = s1 + s2
    nptest.assert_array_equal([15, 25], result.tstarts)
    nptest.assert_array_equal([16, 26], result.tends)
    nptest.assert_array_equal([6, 8], result.values)


def test_add4():
    s1 = SampleSeries([0, 1, 2], [1, 2, 2], [1, 2, 3])
    s2 = SampleSeries([0, 1, 2], [1, 2, 2], [4, 5, 6])
    result = s1 + s2
    nptest.assert_array_equal([5, 7, 9], result.values)


def test_add5():
    s1 = SampleSeries([0, 1, 2], [1, 2, 2], [1, 2, 3])
    s2 = SampleSeries([0, 1, 2], [1, 2, 2], [4, 5, 6])
    result = s1.where(s1 > 1) + s2.where(s2 < 6)
    nptest.assert_array_equal([7], result.values)


def test_radd1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = 1 + s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([2, 3, 4], result.values)


def test_sub_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    assert len(s1 - s2) == 0


def test_sub1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [4, 5, 6])
    result = s1 - s2
    nptest.assert_array_equal([-3, -3, -3], result.values)


def test_sub2():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([5, 15, 25], [15, 25, 35], [4, 5, 6])
    result = s1 - s2
    nptest.assert_array_equal([5, 10, 15, 20, 25], result.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25, 30], result.tends)
    nptest.assert_array_equal([-3, -2, -3, -2, -3], result.values)


def test_sub3():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([15, 25], [16, 26], [4, 5])
    result = s1 - s2
    nptest.assert_array_equal([15, 25], result.tstarts)
    nptest.assert_array_equal([16, 26], result.tends)
    nptest.assert_array_equal([-2, -2], result.values)


def test_rsub1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = 1 - s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([0, -1, -2], result.values)


def test_mul_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    assert len(s1 * s2) == 0


def test_mul1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [4, 5, 6])
    result = s1 * s2
    nptest.assert_array_equal([4, 10, 18], result.values)


def test_mul2():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([5, 15, 25], [15, 25, 35], [4, 5, 6])
    result = s1 * s2
    nptest.assert_array_equal([5, 10, 15, 20, 25], result.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25, 30], result.tends)
    nptest.assert_array_equal([4, 8, 10, 15, 18], result.values)


def test_mul3():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    s2 = SampleSeries([15, 25], [16, 26], [4, 5])
    result = s1 * s2
    nptest.assert_array_equal([15, 25], result.tstarts)
    nptest.assert_array_equal([16, 26], result.tends)
    nptest.assert_array_equal([8, 15], result.values)


def test_rmul1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = 2 * s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([2, 4, 6], result.values)


def test_div_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    assert len(s1 / s2) == 0


def test_div1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 4])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [4, 4, 4])
    result = s1 / s2
    nptest.assert_array_equal([0.25, 0.5, 1], result.values)


def test_div2():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 4])
    s2 = SampleSeries([5, 15, 25], [15, 25, 35], [1, 2, 4])
    result = s1 / s2
    nptest.assert_array_equal([5, 10, 15, 20, 25], result.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25, 30], result.tends)
    nptest.assert_array_equal([1, 2, 1, 2, 1], result.values)


def test_div3():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 5])
    s2 = SampleSeries([15, 25], [16, 26], [4, 5])
    result = s1 / s2
    nptest.assert_array_equal([15, 25], result.tstarts)
    nptest.assert_array_equal([16, 26], result.tends)
    nptest.assert_array_equal([0.5, 1], result.values)


def test_rdiv1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 4])
    result = 2 / s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([2, 1, 0.5], result.values)


def test_mod_empty():
    s1 = SampleSeries.empty()
    s2 = SampleSeries.empty()
    assert len(s1 % s2) == 0


def test_mod_scalar():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [5, 7, 9])
    result = s1 % 3
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([2, 1, 0], result.values)


def test_mod_series_same_times():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [7, 10, 12])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [3, 4, 5])
    result = s1 % s2
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([1, 2, 2], result.values)


def test_mod_series_overlapping_times():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [7, 10, 12])
    s2 = SampleSeries([5, 15, 25], [15, 25, 35], [3, 4, 5])
    result = s1 % s2
    nptest.assert_array_equal([5, 10, 15, 20, 25], result.tstarts)
    nptest.assert_array_equal([10, 15, 20, 25, 30], result.tends)
    nptest.assert_array_equal([1, 1, 2, 0, 2], result.values)


def test_rmod_scalar():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 3, 4])
    result = 10 % s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([0, 1, 2], result.values)


def test_rmod_reversed_operands():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [2, 3, 4])
    result = 5 % s1
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([1, 2, 1], result.values)


def test_sum_empty():
    s1 = SampleSeries.empty()
    assert np.isnan(s1.sum())


def test_sum():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    assert 6 == s1.sum()


def test_min_empty():
    s1 = SampleSeries.empty()
    assert np.isnan(s1.min())


def test_min():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    assert 1 == s1.min()


def test_max_empty():
    s1 = SampleSeries.empty()
    assert np.isnan(s1.max())


def test_max():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    assert 3 == s1.max()


def test_mean_empty():
    s1 = SampleSeries.empty()
    assert np.isnan(s1.mean())


def test_mean1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    assert 2 == s1.mean()


def test_mean2():
    s1 = SampleSeries([0, 1, 2], [1, 2, 4], [1, 3, 4])
    assert 3 == s1.mean()


def test_gt_empty():
    intvl = SampleSeries.empty() > 0
    assert len(intvl) == 0


def test_gt_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) > 0
    assert len(intvl) == 1
    nptest.assert_array_equal([1], intvl.tstarts)
    nptest.assert_array_equal([2], intvl.tends)


def test_gt_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 0, 0])
    intvl = s1 > s2
    assert len(intvl) == 1
    nptest.assert_array_equal([1], intvl.tstarts)
    nptest.assert_array_equal([2], intvl.tends)


def test_ge_empty():
    intvl = SampleSeries.empty() >= 0
    assert len(intvl) == 0


def test_ge_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) >= 1
    assert len(intvl) == 1
    nptest.assert_array_equal([1], intvl.tstarts)
    nptest.assert_array_equal([2], intvl.tends)


def test_ge_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    intvl = s1 >= s2
    assert len(intvl) == 1
    nptest.assert_array_equal([0], intvl.tstarts)
    nptest.assert_array_equal([3], intvl.tends)


def test_lt_empty():
    intvl = SampleSeries.empty() < 0
    assert len(intvl) == 0


def test_lt_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) < 1
    assert len(intvl) == 2
    nptest.assert_array_equal([0, 2], intvl.tstarts)
    nptest.assert_array_equal([1, 3], intvl.tends)


def test_lt_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 0, 1])
    intvl = s1 < s2
    assert len(intvl) == 2
    nptest.assert_array_equal([0, 2], intvl.tstarts)
    nptest.assert_array_equal([1, 3], intvl.tends)


def test_le_empty():
    intvl = SampleSeries.empty() <= 0
    assert len(intvl) == 0


def test_le_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) <= 1
    assert len(intvl) == 1
    nptest.assert_array_equal([0], intvl.tstarts)
    nptest.assert_array_equal([3], intvl.tends)


def test_le_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 0, 1])
    intvl = s1 <= s2
    assert len(intvl) == 2
    nptest.assert_array_equal([0, 2], intvl.tstarts)
    nptest.assert_array_equal([1, 3], intvl.tends)


def test_eq_empty():
    intvl = SampleSeries.empty() == 0
    assert len(intvl) == 0


def test_eq_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) == 1
    assert len(intvl) == 1
    nptest.assert_array_equal([1], intvl.tstarts)
    nptest.assert_array_equal([2], intvl.tends)


def test_eq_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    intvl = s1 == s2
    assert len(intvl) == 1
    nptest.assert_array_equal([1], intvl.tstarts)
    nptest.assert_array_equal([2], intvl.tends)


def test_ne_empty():
    intvl = SampleSeries.empty() != 0
    assert len(intvl) == 0


def test_ne_scalar():
    intvl = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0]) != 1
    assert len(intvl) == 2
    nptest.assert_array_equal([0, 2], intvl.tstarts)
    nptest.assert_array_equal([1, 3], intvl.tends)


def test_ne_series():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [0, 1, 0])
    s2 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 1, 1])
    intvl = s1 != s2
    assert len(intvl) == 2
    nptest.assert_array_equal([0, 2], intvl.tstarts)
    nptest.assert_array_equal([1, 3], intvl.tends)


def test_where_empty():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.where(Intervals.empty())
    assert len(result) == 0


def test_where_non_overlapping():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.where(Intervals([100], [1001]))
    assert len(result) == 0


def test_where1():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.where(Intervals([0], [1]))
    assert len(result) == 1
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([1], result.tends)
    nptest.assert_array_equal([1], result.values)


def test_where2():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.where(Intervals([0], [3]))
    assert len(result) == 3
    nptest.assert_array_equal([0, 1, 2], result.tstarts)
    nptest.assert_array_equal([1, 2, 3], result.tends)
    nptest.assert_array_equal([1, 2, 3], result.values)


def test_where3():
    s1 = SampleSeries([0, 10, 20], [10, 20, 30], [1, 2, 3])
    result = s1.where(Intervals([5], [15]))
    assert len(result) == 2
    nptest.assert_array_equal([5, 10], result.tstarts)
    nptest.assert_array_equal([10, 15], result.tends)
    nptest.assert_array_equal([1, 2], result.values)


def test_where4():
    s1 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 3])
    result = s1.where(s1 > 1)
    assert len(result) == 2
    nptest.assert_array_equal([10, 20], result.tstarts)
    nptest.assert_array_equal([20, 20], result.tends)
    nptest.assert_array_equal([2, 3], result.values)


def test_where5():
    s1 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 1])
    result = s1.where(s1 > 1)
    assert len(result) == 1
    nptest.assert_array_equal([10], result.tstarts)
    nptest.assert_array_equal([20], result.tends)
    nptest.assert_array_equal([2], result.values)


def test_where6():
    s1 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 3])
    result = s1.where((s1 > 1) & (s1 < 5))
    assert len(result) == 2
    nptest.assert_array_equal([10, 20], result.tstarts)
    nptest.assert_array_equal([20, 20], result.tends)
    nptest.assert_array_equal([2, 3], result.values)


def test_where7():
    s1 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 3])
    result = s1.where((s1 > 1) & (s1 < 3))
    assert len(result) == 1
    nptest.assert_array_equal([10], result.tstarts)
    nptest.assert_array_equal([20], result.tends)
    nptest.assert_array_equal([2], result.values)


def test_where8():
    s1 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 3])
    s2 = SampleSeries([10, 20], [20, 40], [1, 2])
    result = s1.where((s1 > 1) & (s2 > 1))
    assert len(result) == 1
    nptest.assert_array_equal([20], result.tstarts)
    nptest.assert_array_equal([20], result.tends)
    nptest.assert_array_equal([3], result.values)


def test_where9():
    s1 = SampleSeries([10, 20], [20, 40], [1, 2])
    s2 = SampleSeries([0, 10, 20], [10, 20, 20], [1, 2, 3])
    result = s1.where((s1 > 0) & (s2 < 4))
    assert len(result) == 2
    nptest.assert_array_equal([10, 20], result.tstarts)
    nptest.assert_array_equal([20, 20], result.tends)
    nptest.assert_array_equal([1, 2], result.values)


def test_where_complex():
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.where(s1 == 2)
    assert len(result) == 1
    nptest.assert_array_equal([1], result.tstarts)
    nptest.assert_array_equal([2], result.tends)
    nptest.assert_array_equal([2], result.values)


def test_intervals_between_falling_edges_empty():
    s1 = SampleSeries.empty()
    result = s1.intervals_between_falling_edges()
    assert len(result) == 0


def test_intervals_between_falling_edges_no_falling_edges():
    # values [1, 2, 3] are strictly increasing -> no falling edges -> empty
    s1 = SampleSeries([0, 1, 2], [1, 2, 3], [1, 2, 3])
    result = s1.intervals_between_falling_edges()
    assert len(result) == 0


def test_intervals_between_falling_edges_one_falling_edge():
    # values [1, 0] -> falling edge at index 1 (tstart=1)
    s1 = SampleSeries([0, 1], [1, 2], [1, 0])
    result = s1.intervals_between_falling_edges()
    assert len(result) == 2
    nptest.assert_array_equal(result.tstarts, [0, 1])
    nptest.assert_array_equal(result.tends, [1, 2])


def test_intervals_between_falling_edges_multiple():
    # values [3, 2, 5, 1, 4] -> falling at indices 1 (3->2), 3 (5->1)
    # tstarts=[0,1,2,3,4], tends=[1,2,3,4,5]
    # intervals: [0, 1), [1, 3), [3, 5]
    s1 = SampleSeries([0, 1, 2, 3, 4], [1, 2, 3, 4, 5], [3, 2, 5, 1, 4])
    result = s1.intervals_between_falling_edges()
    assert len(result) == 3
    nptest.assert_array_equal(result.tstarts, [0, 1, 3])
    nptest.assert_array_equal(result.tends, [1, 3, 5])


def test_intervals_between_falling_edges_continuous_blocks():
    # Gap between tends[2]=3 and tstarts[3]=5 -> two continuous blocks: (0,2) and (3,4)
    # Block 0: values [3,2,1] -> falling at 1,2 -> intervals [0,1), [1,2), [2,3]
    # Block 1: values [5,4] -> falling at 4 -> intervals [5,6), [6,7]
    s1 = SampleSeries([0, 1, 2, 5, 6], [1, 2, 3, 6, 7], [3, 2, 1, 5, 4])
    result = s1.intervals_between_falling_edges()
    assert len(result) == 5
    nptest.assert_array_equal(result.tstarts, [0, 1, 2, 5, 6])
    nptest.assert_array_equal(result.tends, [1, 2, 3, 6, 7])


def test_histogram1():
    s1 = SampleSeries(
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [5, 5, 5, 5, 1, 1, 1, 1, 1, 1],
    )
    bins = list(np.arange(0, 11, 1))
    hist, bin_edges = s1.histogram(bins)
    expected_hist = [0.0, 6.0, 0.0, 0.0, 0.0, 4.0, 0.0, 0.0, 0.0, 0.0]
    nptest.assert_array_equal(expected_hist, hist)
    nptest.assert_array_equal(bin_edges, bins)


def test_histogram2():
    s1 = SampleSeries(
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [5, 5, 5, 5, 1, 1, 1, 1, 1, 1],
    )
    hist, bin_edges = s1.histogram()
    expected_hist = [10.0]
    nptest.assert_array_equal(expected_hist, hist)


def test_resample1():
    s1 = SampleSeries([0, 1], [1, 3], [1, 2])
    result = s1.resample(sample_rate=1.0)
    exp_tstarts = [0, 1, 2]
    exp_tends = [1, 2, 3]
    exp_values = [1, 2, 2]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample2():
    s1 = SampleSeries([0, 4, 5], [4, 5, 6], [1, 3, 2])
    result = s1.resample(sample_rate=1.0)
    exp_tstarts = [0, 1, 2, 3, 4, 5]
    exp_tends = [1, 2, 3, 4, 5, 6]
    exp_values = [1, 1, 1, 1, 3, 2]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample3():
    s1 = SampleSeries([0, 4, 6], [4, 6, 7], [1, 3, 2])
    result = s1.resample(sample_rate=1.0)
    exp_tstarts = [0, 1, 2, 3, 4, 5, 6]
    exp_tends = [1, 2, 3, 4, 5, 6, 7]
    exp_values = [1, 1, 1, 1, 3, 3, 2]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample4():
    s1 = SampleSeries([0, 1, 6], [1, 3, 7], [1, 2, 4])
    result = s1.resample(sample_rate=1)
    exp_tstarts = [0, 1, 2, 6]
    exp_tends = [1, 2, 3, 7]
    exp_values = [1, 2, 2, 4]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample5():
    s1 = SampleSeries([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 10], [5, 5, 5, 4, 5, 1])
    result = s1.where(s1 < 5).resample(sample_rate=1.0)
    exp_tstarts = [3, 5, 6, 7, 8, 9]
    exp_tends = [4, 6, 7, 8, 9, 10]
    exp_values = [4, 1, 1, 1, 1, 1]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample6():
    s1 = SampleSeries([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 11], [5, 5, 5, 4, 5, 1, 2])
    res1 = s1.where(s1 < 5).resample(sample_rate=2.0)
    res2 = s1.where(s1 < 6).resample(sample_rate=1.0)
    result = res1 + res2
    exp_tstarts = [3, 5, 6, 7, 8, 9, 10]
    exp_tends = [4, 6, 7, 8, 9, 10, 11]
    exp_values = [8, 2, 2, 2, 2, 2, 3]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample7():
    s1 = SampleSeries([0, 4, 5, 6], [4, 5, 6, 6], [1, 3, 2, 3])
    result = s1.resample(sample_rate=1.0)
    exp_tstarts = [0, 1, 2, 3, 4, 5, 6]
    exp_tends = [1, 2, 3, 4, 5, 6, 6]
    exp_values = [1, 1, 1, 1, 3, 2, 3]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_resample8():
    s1 = SampleSeries([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 10], [5, 5, 5, 4, 5, 1, 2])
    res1 = s1.where(s1 < 5).resample(sample_rate=1.0)
    res2 = s1.where(s1 < 6).resample(sample_rate=1.0)
    result = res1 + res2
    exp_tstarts = [3, 5, 6, 7, 8, 9, 10]
    exp_tends = [4, 6, 7, 8, 9, 10, 10]
    exp_values = [8, 2, 2, 2, 2, 2, 4]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_rolling_average1():
    s1 = SampleSeries([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 11], [5, 5, 5, 3, 5, 1, 2])
    s1_avg = s1.rolling_average(window_size=3)
    exp_tstarts = [0, 3, 6, 9]
    exp_tends = [3, 6, 9, 11]
    exp_values = [5, 3, 1, 1.5]
    nptest.assert_array_equal(exp_tstarts, s1_avg.tstarts)
    nptest.assert_array_equal(exp_tends, s1_avg.tends)
    nptest.assert_array_equal(exp_values, s1_avg.values)


def test_rolling_average2():
    s1 = SampleSeries([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 11], [5, 5, 5, 3, 5, 1, 4])
    s1_avg = s1.where(s1 < 5).rolling_average(window_size=3)
    exp_tstarts = [3, 5, 8]
    exp_tends = [4, 8, 11]
    exp_values = [3, 1, 2]
    nptest.assert_array_equal(exp_tstarts, s1_avg.tstarts)
    nptest.assert_array_equal(exp_tends, s1_avg.tends)
    nptest.assert_array_equal(exp_values, s1_avg.values)


def test_trapz1():
    s1 = SampleSeries([1, 2, 3], [2, 3, 4], [1, 2, 3])
    result = s1.trapz()
    exp_result = 4.0
    assert result == exp_result


def test_trapz2():
    s1 = SampleSeries([1, 2, 3, 4], [2, 3, 4, 4], [1, 2, 3, 4])
    result = s1.trapz()
    exp_result = 0 + (1 + 2) / 2 + (2 + 3) / 2 + (3 + 4) / 2
    assert result == exp_result


def test_trapz3():
    s1 = SampleSeries([1, 2, 3, 4], [2, 3, 4, 4], [1, 2, 3, 3])
    result = s1.trapz()
    exp_result = 0 + (1 + 2) / 2 + (2 + 3) / 2 + (3 + 3) / 2
    assert result == exp_result


def test_trapz4():
    s1 = SampleSeries([1, 2, 3, 8, 9], [2, 3, 4, 9, 10], [1, 2, 3, 6, 7])
    result = s1.trapz()
    exp_result = 0 + (1 + 2) / 2 + (2 + 3) / 2 + 0 + (6 + 7) / 2
    assert result == exp_result


def test_cumtrapz1():
    s1 = SampleSeries([1, 2, 3], [2, 3, 4], [1, 2, 3])
    result = s1.cumtrapz()
    exp_tstarts = [1, 2, 3]
    exp_tends = [2, 3, 4]
    exp_values = [0, (1 + 2) / 2, 1.5 + (2 + 3) / 2]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_cumtrapz2():
    s1 = SampleSeries([1, 2, 3, 4], [2, 3, 4, 4], [1, 2, 3, 4])
    result = s1.cumtrapz()
    exp_tstarts = [1, 2, 3, 4]
    exp_tends = [2, 3, 4, 4]
    exp_values = [0, 0 + (1 + 2) / 2, 0 + 1.5 + (2 + 3) / 2, 0 + 1.5 + 2.5 + (3 + 4) / 2]
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_cumtrapz3():
    s1 = SampleSeries([1, 2, 3, 8, 9], [2, 3, 4, 9, 10], [1, 2, 3, 6, 7])
    result = s1.cumtrapz()
    exp_tstarts = [1, 2, 3, 8, 9]
    exp_tends = [2, 3, 4, 9, 10]
    exp_values = [
        0,
        0 + (1 + 2) / 2,
        0 + 1.5 + (2 + 3) / 2,
        0 + 1.5 + (2 + 3) / 2,
        0 + 1.5 + (2 + 3) / 2 + (6 + 7) / 2,
    ]
    print(result.tstarts, result.tends, result.values)
    nptest.assert_array_equal(exp_tstarts, result.tstarts)
    nptest.assert_array_equal(exp_tends, result.tends)
    nptest.assert_array_equal(exp_values, result.values)


def test_histogram2d():
    s1 = SampleSeries(
        tstarts=np.array([0, 1, 2, 3]), tends=np.array([1, 2, 3, 4]), values=[0, 1, 2, 3]
    )

    x_bins = np.array([0, 1, 2, 3])
    y_bins = np.array([0, 1, 2, 3])
    H, x_bins, y_bins = s1.histogram2d(s1, x_bins=x_bins, y_bins=y_bins)

    expected_H = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 2.0]]
    nptest.assert_array_equal(expected_H, H)


def test_histogram2d_empty_sample_series():
    s1 = SampleSeries(
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [5, 5, 5, 5, 1, 1, 1, 1, 1, 1],
    )

    empty_sample_series = SampleSeries.empty()

    x_bins = list(np.arange(0, 11, 3))
    y_bins = list(np.arange(0, 8, 2))
    H, x_bins, y_bins = s1.histogram2d(empty_sample_series, x_bins=x_bins, y_bins=y_bins)

    expected_H_empty_series = [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
    nptest.assert_array_equal(expected_H_empty_series, H)

    H, x_bins, y_bins = empty_sample_series.histogram2d(
        empty_sample_series, x_bins=x_bins, y_bins=y_bins
    )
    nptest.assert_array_equal(expected_H_empty_series, H)

    # missing bins
    H, x_bins, y_bins = s1.histogram2d(empty_sample_series, x_bins=None, y_bins=None)
    nptest.assert_array_equal([[0.0]], H)
    nptest.assert_array_equal([-np.inf, np.inf], x_bins)
    nptest.assert_array_equal([-np.inf, np.inf], y_bins)


# =============================================================================
# Tests for diff() method
# =============================================================================


def test_diff_empty():
    """Empty series should return empty series"""
    series = SampleSeries.empty()
    result = series.diff()
    assert len(result) == 0


def test_diff_single_sample():
    """Single sample should return 0"""
    series = SampleSeries([0], [1], [100])
    result = series.diff()
    assert len(result) == 1
    nptest.assert_array_equal(result.tstarts, [0])
    nptest.assert_array_equal(result.tends, [1])
    nptest.assert_array_equal(result.values, [0])


def test_diff_two_samples():
    """Two samples: first=0, second=difference"""
    series = SampleSeries([0, 1], [1, 2], [100, 150])
    result = series.diff()
    assert len(result) == 2
    nptest.assert_array_equal(result.tstarts, [0, 1])
    nptest.assert_array_equal(result.tends, [1, 2])
    nptest.assert_array_equal(result.values, [0, 50])


def test_diff_multiple_samples():
    """Multiple samples with positive and negative differences"""
    series = SampleSeries([0, 1, 2, 3, 4], [1, 2, 3, 4, 5], [100, 150, 130, 130, 200])
    result = series.diff()
    assert len(result) == 5
    nptest.assert_array_equal(result.tstarts, [0, 1, 2, 3, 4])
    nptest.assert_array_equal(result.tends, [1, 2, 3, 4, 5])
    nptest.assert_array_equal(result.values, [0, 50, -20, 0, 70])


def test_diff_with_gap():
    """After a gap, difference should reset to 0"""
    series = SampleSeries([0, 1, 5, 6], [1, 2, 6, 7], [100, 150, 120, 200])
    result = series.diff()
    assert len(result) == 4
    nptest.assert_array_equal(result.tstarts, [0, 1, 5, 6])
    nptest.assert_array_equal(result.tends, [1, 2, 6, 7])
    nptest.assert_array_equal(result.values, [0, 50, 0, 80])


def test_diff_multiple_gaps():
    """Multiple gaps should each reset to 0"""
    series = SampleSeries([0, 1, 5, 6, 10], [1, 2, 6, 7, 11], [100, 150, 200, 180, 300])
    result = series.diff()
    nptest.assert_array_equal(result.values, [0, 50, 0, -20, 0])


def test_diff_all_gaps():
    """All disconnected intervals should all be 0"""
    series = SampleSeries([0, 5, 10], [1, 6, 11], [100, 200, 300])
    result = series.diff()
    nptest.assert_array_equal(result.values, [0, 0, 0])


def test_diff_nan_current():
    """NaN in current value propagates"""
    series = SampleSeries([0, 1, 2], [1, 2, 3], [100, np.nan, 150])
    result = series.diff()
    assert result.values[0] == 0
    assert np.isnan(result.values[1])  # NaN - 100 = NaN
    assert np.isnan(result.values[2])  # 150 - NaN = NaN


def test_diff_nan_previous():
    """NaN in previous value propagates to next diff"""
    series = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [100, np.nan, np.nan, 200])
    result = series.diff()
    assert result.values[0] == 0
    assert np.isnan(result.values[1])
    assert np.isnan(result.values[2])
    assert np.isnan(result.values[3])


def test_diff_nan_with_gap():
    """Gap resets to 0 even after NaN"""
    series = SampleSeries([0, 1, 5], [1, 2, 6], [100, np.nan, 200])
    result = series.diff()
    assert result.values[0] == 0
    assert np.isnan(result.values[1])
    assert result.values[2] == 0  # Gap resets


def test_diff_preserves_timestamps():
    """diff() should preserve original timestamps"""
    series = SampleSeries([0.5, 1.5, 2.5], [1.5, 2.5, 3.5], [10, 20, 15])
    result = series.diff()
    nptest.assert_array_equal(result.tstarts, [0.5, 1.5, 2.5])
    nptest.assert_array_equal(result.tends, [1.5, 2.5, 3.5])


def test_diff_negative_values():
    """diff() should handle negative values"""
    series = SampleSeries([0, 1, 2], [1, 2, 3], [-100, -50, -150])
    result = series.diff()
    nptest.assert_array_equal(result.values, [0, 50, -100])


def test_diff_preserves_length():
    """diff() should return same length as original"""
    series = SampleSeries([0, 1, 2, 3], [1, 2, 3, 4], [10, 20, 30, 40])
    result = series.diff()
    assert len(result) == len(series)
