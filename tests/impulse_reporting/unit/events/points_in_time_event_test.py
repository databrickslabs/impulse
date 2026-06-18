# pylint: disable=missing-function-docstring, redefined-outer-name
import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from tests.conftest import basic_narrow_db, spark


def _points_expr():
    """A bare expression that evaluates to PointsInTime (no Spark needed)."""
    return TimeSeriesSelector(None).rising_edges()


def test_as_dict():
    event = PointsInTimeEvent(name="my_event_1", expr=_points_expr())
    d = event.as_dict()
    assert d.get("event_type") == "POINTS_IN_TIME_EVENT"
    assert d.get("event_name") == "my_event_1"
    assert d.get("event_id") == event.get_id()
    assert d.get("report_id") == -1


def test_get_event_type_str():
    event = PointsInTimeEvent(name="e", expr=_points_expr())
    assert event.get_event_type_str() == "POINTS_IN_TIME_EVENT"


def test_as_spark_row():
    row = PointsInTimeEvent(name="e", expr=_points_expr()).as_spark_row()
    assert len(row) == 9
    assert row.event_type == "POINTS_IN_TIME_EVENT"


def test_definition_hash_ignores_name():
    e1 = PointsInTimeEvent(name="name_a", expr=_points_expr())
    e2 = PointsInTimeEvent(name="name_b", expr=_points_expr())
    assert e1.determine_definition_hash() == e2.determine_definition_hash()


def test_init_rejects_intervals_expression():
    with pytest.raises(ValueError, match="PointsInTime"):
        PointsInTimeEvent(name="e", expr=TimeSeriesSelector(None) > 0)


def test_init_rejects_sample_series_expression():
    with pytest.raises(ValueError, match="PointsInTime"):
        PointsInTimeEvent(name="e", expr=TimeSeriesSelector(None))


def test_determine_events(spark, basic_narrow_db):
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())

    solver = DefaultSolver(spark)
    solved_df = basic_narrow_db.query.select(event.get_expression()).solve(spark, solver)
    df = PointsInTimeEvent.determine_events(spark, [event], solved_df=solved_df)

    assert {"container_id", "event_instance_id", "event_id", "start_ts", "end_ts"}.issubset(
        set(df.columns)
    )
    assert df.count() > 0
    # every instance is a zero-duration point
    assert df.filter(F.col("start_ts") != F.col("end_ts")).count() == 0


def test_determine_events_requires_solved_df(spark):
    event = PointsInTimeEvent(name="e", expr=_points_expr())
    with pytest.raises(ValueError, match="requires solved_df"):
        PointsInTimeEvent.determine_events(spark, [event])
