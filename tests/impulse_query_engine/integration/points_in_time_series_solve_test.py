"""Integration tests for solving a PointsInTimeSeries end-to-end against test data.

Exercises the full solve pipeline for ``channel.where(channel.rising_edges())`` (per-container
channel loading, expression evaluation in the grouped-map UDF, ``get_data()`` ->
``array<array<double>>`` serialization, and the collect/toPandas round-trip) using the
``basic_narrow_db`` fixture.
"""

import math

import pyspark.sql.types as T

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries


def test_where_rising_edges_solves_to_points_in_time_series(spark, basic_narrow_db):
    """Solving channel.where(channel.rising_edges()) yields a PointsInTimeSeries column."""
    query = basic_narrow_db.query
    eng_rpm = query.channel(channel_name="Engine RPM")

    # "edges" is a PointsInTime (array<double>); "pit" is a PointsInTimeSeries
    # (array<array<double>>) sampling the channel at those instants.
    result = query.select(
        eng_rpm.rising_edges().alias("edges"),
        eng_rpm.where(eng_rpm.rising_edges()).alias("pit"),
    ).solve(spark, solver=DefaultSolver(spark))

    # array-storage representation, verified end to end
    assert result.schema["edges"].dataType == T.ArrayType(T.DoubleType())
    assert result.schema["pit"].dataType == T.ArrayType(T.ArrayType(T.DoubleType()))

    rows = result.collect()
    assert {row.container_id for row in rows} == {1, 2, 3}

    saw_non_empty = False
    for row in rows:
        edges = row["edges"]  # flat [t, ...]
        pit = row["pit"]  # nested [[t, v], ...]
        # every rising-edge instant is a sample tstart, so no point is dropped: the value series is
        # sampled at exactly those instants and its timestamps equal the rising-edge points.
        assert [point[0] for point in pit] == list(edges)
        for point in pit:
            assert len(point) == 2
            assert not math.isnan(point[1])
        if pit:
            saw_non_empty = True

    assert saw_non_empty  # Engine RPM has rising edges in the test data


def test_interval_start_and_end_points_solve_to_points_in_time(spark, basic_narrow_db):
    """Solving (channel > x).start_points() / .end_points() yields PointsInTime columns whose
    values are exactly the start / end boundary of each comparison window, end to end."""
    query = basic_narrow_db.query
    eng_rpm = query.channel(channel_name="Engine RPM")

    # "windows" is the Intervals where the engine is running; "starts"/"ends" are the
    # PointsInTime at those windows' boundaries.
    result = query.select(
        (eng_rpm > 0).alias("windows"),  # array<array<double>>  [[s, e], ...]
        (eng_rpm > 0).start_points().alias("starts"),  # array<double>  [s, ...]
        (eng_rpm > 0).end_points().alias("ends"),  # array<double>  [e, ...]
    ).solve(spark, solver=DefaultSolver(spark))

    # array-storage representation, verified end to end
    assert result.schema["windows"].dataType == T.ArrayType(T.ArrayType(T.DoubleType()))
    assert result.schema["starts"].dataType == T.ArrayType(T.DoubleType())
    assert result.schema["ends"].dataType == T.ArrayType(T.DoubleType())

    rows = result.collect()
    assert {row.container_id for row in rows} == {1, 2, 3}

    saw_non_empty = False
    for row in rows:
        windows = row["windows"]  # [[s, e], ...]
        # start_points / end_points are exactly the boundaries of each window
        assert list(row["starts"]) == [w[0] for w in windows]
        assert list(row["ends"]) == [w[1] for w in windows]
        if windows:
            saw_non_empty = True

    assert saw_non_empty  # Engine RPM is > 0 somewhere in the test data


def test_points_in_time_series_round_trips_via_to_pandas(spark, basic_narrow_db):
    """toPandas returns the raw array representation (no deserialization to an object)."""
    query = basic_narrow_db.query
    eng_rpm = query.channel(channel_name="Engine RPM")

    pdf = query.select(
        eng_rpm.where(eng_rpm.rising_edges()).alias("pit"),
    ).toPandas(spark, solver=DefaultSolver(spark))

    assert len(pdf) == 3
    for pit in pdf["pit"]:
        # PointsInTimeSeries has no requires_deserialization, so it stays as nested [t, v] arrays
        assert not isinstance(pit, PointsInTimeSeries)
        for point in pit:
            assert len(point) == 2
    assert any(len(pit) > 0 for pit in pdf["pit"])
