"""Unit tests for the reporting PointValueAggregator."""

from unittest.mock import MagicMock

import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from tests.conftest import basic_narrow_db, spark


def _pit_event(name="pit"):
    """A PointsInTimeEvent built from a bare selector (no Spark needed)."""
    return PointsInTimeEvent(name=name, expr=TimeSeriesSelector(None).rising_edges())


def _aggregator(name="pva"):
    return PointValueAggregator(
        name=name,
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        event=_pit_event(),
    )


def test_as_dict():
    d = _aggregator(name="pva_1").as_dict()
    assert d["agg_type"] == "point_value_aggregator"
    assert d["name"] == "pva_1"
    assert d["visual_id"] == _aggregator(name="pva_1").get_id()
    assert d["statistics"] == ["value"]
    assert d["channel_names"] == ["Signal"]
    assert d["report_id"] == -1


def test_as_spark_row():
    row = _aggregator().as_spark_row()
    assert row.agg_type == "point_value_aggregator"
    assert row.statistics == ["value"]


def test_definition_hash_ignores_name():
    e1 = PointValueAggregator(
        name="a",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["c"],
        event=_pit_event(),
    )
    e2 = PointValueAggregator(
        name="b",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["c"],
        event=_pit_event(),
    )
    assert e1.determine_definition_hash() == e2.determine_definition_hash()


def test_init_rejects_non_sample_series_input():
    """Input expressions must evaluate to SampleSeries."""
    with pytest.raises(ValueError, match="SampleSeries"):
        PointValueAggregator(
            name="bad",
            input_expressions=[TimeSeriesSelector(None) > 0],  # Intervals
            channel_names=["c"],
            event=_pit_event(),
        )


def test_init_rejects_event_not_evaluating_to_points_in_time():
    """An event whose expression yields Intervals (e.g. BasicEvent) is rejected."""
    with pytest.raises(ValueError, match="PointsInTime"):
        PointValueAggregator(
            name="bad",
            input_expressions=[TimeSeriesSelector(None)],
            channel_names=["c"],
            event=BasicEvent(name="b", expr=TimeSeriesSelector(None) > 0),
        )


def test_init_rejects_missing_event():
    with pytest.raises(ValueError, match="PointsInTime"):
        PointValueAggregator(
            name="bad",
            input_expressions=[TimeSeriesSelector(None)],
            channel_names=["c"],
            event=None,
        )


def test_init_rejects_container_event():
    """ContainerEvent has no expression (whole-series scope) — not a points event."""
    with pytest.raises(ValueError, match="PointsInTime"):
        PointValueAggregator(
            name="bad",
            input_expressions=[TimeSeriesSelector(None)],
            channel_names=["c"],
            event=ContainerEvent(name="c"),
        )


def test_init_accepts_any_event_evaluating_to_points_in_time():
    """Acceptance is by expression type, not event class: any event whose expression
    evaluates to PointsInTime is accepted (not only PointsInTimeEvent)."""
    fake_event = MagicMock()
    fake_event.get_expression.return_value = TimeSeriesSelector(None).rising_edges()

    agg = PointValueAggregator(
        name="ok",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["c"],
        event=fake_event,
    )
    assert agg.get_event() is fake_event


def test_channel_names_length_mismatch():
    with pytest.raises(ValueError, match="Length mismatch"):
        PointValueAggregator(
            name="bad",
            input_expressions=[TimeSeriesSelector(None)],
            channel_names=["c1", "c2"],
            event=_pit_event(),
        )


def test_determine_aggregations(spark, basic_narrow_db):
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
    agg = PointValueAggregator(
        name="rpm_at_edges",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        event=event,
    )

    solver = DefaultSolver(spark)
    solved_df = basic_narrow_db.query.select(agg.get_expression()).solve(spark, solver)
    df = PointValueAggregator.determine_aggregations(spark, [agg], solved_df=solved_df)

    expected = {
        "container_id",
        "visual_id",
        "channel_name",
        "event_id",
        "event_instance_id",
        "aggregation_label",
        "statistic_value",
    }
    assert expected.issubset(set(df.columns))
    assert df.count() > 0
    # every row is a single sampled value labelled "value"
    assert df.filter(F.col("aggregation_label") != "value").count() == 0
    # the sampled values are real (non-null)
    assert df.filter(F.col("statistic_value").isNotNull()).count() > 0


def test_determine_events_requires_solved_df(spark):
    with pytest.raises(ValueError, match="requires solved_df"):
        PointValueAggregator.determine_aggregations(spark, [_aggregator()])


def test_determine_metadata_df(spark):
    agg = _aggregator(name="meta_pva")
    df = PointValueAggregator.determine_metadata_df(spark, [agg])
    assert df.count() == 1
    assert df.collect()[0]["agg_type"] == "point_value_aggregator"


def test_get_expression_str():
    s = _aggregator().get_expression_str()
    assert isinstance(s, str)
    assert s != "NA"
    assert "PointValueAggregator" in s


def test_empty_input_expressions_raises():
    with pytest.raises(ValueError, match="At least one input expression"):
        PointValueAggregator(
            name="bad",
            input_expressions=[],
            channel_names=[],
            event=_pit_event(),
        )
