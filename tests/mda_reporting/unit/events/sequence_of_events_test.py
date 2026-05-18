import pytest

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.events import SequenceOfEventsExpression
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from mda_reporting.events.sequence_of_events import SequenceOfEvents
from tests.conftest import basic_narrow_db, spark


def test_sequence_of_events_init():
    """Test SequenceOfEvents initialization with required parameters."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )

    assert event.name == "test_soe"
    assert event.expression is not None
    assert isinstance(event.expression, SequenceOfEventsExpression)
    assert event.description is None
    assert event.required_channels is None


def test_sequence_of_events_init_with_optional_params():
    """Test SequenceOfEvents initialization with optional parameters."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
        desc="desc",
        required_channels=["ch1"],
    )

    assert event.name == "test_soe"
    assert event.expression is not None
    assert isinstance(event.expression, SequenceOfEventsExpression)
    assert event.description == "desc"
    assert event.required_channels == ["ch1"]


def test_get_name():
    """Test get_name method."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    assert event.get_name() == "test_soe"


def test_get_id():
    """Test get_id method determinism and differentiation by name."""
    event1 = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    event2 = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    event3 = SequenceOfEvents(
        name="test_soe_2",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )

    assert event1.get_id() == event2.get_id()
    assert event1.get_id() != event3.get_id()


def test_get_expression():
    """Test get_expression method returns TimeSeriesExpression."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    expression = event.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_get_expression_str():
    """Test get_expression_str method."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    expr_str = event.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_as_dict():
    """Test as_dict method."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    event_dict = event.as_dict()

    assert isinstance(event_dict, dict)
    assert event_dict.get("event_id") == event.get_id()
    assert event_dict.get("report_id") == -1
    assert event_dict.get("event_name") == "test_soe"
    assert event_dict.get("event_description") is None
    assert event_dict.get("required_channels") is None
    assert event_dict.get("event_expression") == event.get_expression_str()
    assert "definition_hash" in event_dict


def test_as_spark_row():
    """Test as_spark_row method."""
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    row = event.as_spark_row()
    assert len(row) == 9  # Should have 9 fields as defined in the schema


def test_determine_metadata_df(spark):
    """Test determine_metadata_df method."""
    event1 = SequenceOfEvents(
        name="test_soe_1",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    event2 = SequenceOfEvents(
        name="test_soe_2",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )

    metadata_df = SequenceOfEvents.determine_metadata_df(spark, [event1, event2])

    assert metadata_df is not None
    assert "event_id" in metadata_df.columns
    assert "report_id" in metadata_df.columns
    assert "event_name" in metadata_df.columns
    assert "event_type" in metadata_df.columns
    assert "event_description" in metadata_df.columns
    assert "required_channels" in metadata_df.columns
    assert "event_expression" in metadata_df.columns
    assert "definition_hash" in metadata_df.columns
    assert metadata_df.count() == 2


def test_determine_events(spark, basic_narrow_db):
    """Test determine_events method with sequence-compatible expressions."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    expr1 = eng_rpm > 1000
    expr2 = eng_rpm > 500

    event = SequenceOfEvents(name="test_soe", expressions=[expr1, expr2])

    solved_df = basic_narrow_db.query.select(event.get_expression()).solve(
        spark=spark, solver=KeyValueStoreSolver(spark)
    )

    df = SequenceOfEvents.determine_events(
        spark,
        [event],
        solved_df=solved_df,
    )

    assert df is not None
    assert "container_id" in df.columns
    assert "event_instance_id" in df.columns
    assert "event_id" in df.columns
    assert "start_ts" in df.columns
    assert "end_ts" in df.columns
    assert df.count() > 0


def test_determine_events_requires_solved_df(spark):
    event = SequenceOfEvents(
        name="test_soe",
        expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
    )
    with pytest.raises(ValueError, match="requires solved_df"):
        SequenceOfEvents.determine_events(spark, [event])
