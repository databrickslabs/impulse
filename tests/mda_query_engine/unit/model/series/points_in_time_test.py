"""Tests for SampleSeries"""

# pylint: disable=missing-function-docstring, redefined-outer-name
import numpy.testing as nptest

from mda_query_engine.model.series.points_in_time import PointsInTime


def test_len():
    pit = PointsInTime.empty()
    assert len(pit) == 0
    pit = PointsInTime([0.123, 1.5])
    assert len(pit) == 2


def test_and_empty():
    pit1 = PointsInTime.empty()
    pit2 = PointsInTime.empty()
    assert len(pit1 & pit2) == 0


def test_and1():
    pit1 = PointsInTime([0, 1, 2])
    pit2 = PointsInTime([1])
    result = pit1 & pit2
    nptest.assert_array_equal(result.tstarts, [1])


def test_and2():
    pit1 = PointsInTime([0, 1, 2, 4])
    pit2 = PointsInTime([1, 4])
    result = pit1 & pit2
    nptest.assert_array_equal(result.tstarts, [1, 4])


def test_or_empty():
    pit1 = PointsInTime.empty()
    pit2 = PointsInTime.empty()
    assert len(pit1 | pit2) == 0


def test_or1():
    pit1 = PointsInTime([0, 1, 2])
    pit2 = PointsInTime([1])
    result = pit1 | pit2
    nptest.assert_array_equal(result.tstarts, [0, 1, 2])


def test_or2():
    pit1 = PointsInTime([0, 1, 2])
    pit2 = PointsInTime([1.5, 2.5])
    result = pit1 | pit2
    nptest.assert_array_equal(result.tstarts, [0, 1, 1.5, 2, 2.5])
