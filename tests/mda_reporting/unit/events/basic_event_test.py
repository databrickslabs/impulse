from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_reporting.events.basic_event import BasicEvent
from tests.conftest import basic_narrow_db, spark


def test_as_dict():
    """Test as_dict method"""
    event = BasicEvent(name="my_event_1", expr=TimeSeriesSelector(None))
    event_dict = event.as_dict()
    assert isinstance(event_dict, dict)
    assert event_dict.get("event_id") == event.get_id()
    assert event_dict.get("report_id") == -1  # Default value for report_id
    assert event_dict.get("event_type") == "BASIC_EVENT"
    assert event_dict.get("event_name") == "my_event_1"
    assert event_dict.get("event_expression") == event.get_expression_str()
    assert event_dict.get("event_description") is None
    assert event_dict.get("required_channels") is None


def test_as_spark_row():
    """Test as_spark_row method"""
    event = BasicEvent(name="my_event_1", expr=TimeSeriesSelector(None))
    row = event.as_spark_row()
    assert len(row) == 9


def test_get_event_type_str():
    """Test get_event_type_str method."""
    event = BasicEvent(name="my_event", expr=TimeSeriesSelector(None))
    assert event.get_event_type_str() == "BASIC_EVENT"


def test_basic_event_init():
    """Test BasicEvent initialization with required parameters"""
    expr = TimeSeriesSelector(None)
    event = BasicEvent(name="test_event", expr=expr)

    assert event.name == "test_event"
    assert event.expression is not None
    assert event.description is None
    assert event.required_channels is None


def test_basic_event_init_with_optional_params():
    """Test BasicEvent initialization with all optional parameters"""
    expr = TimeSeriesSelector(None)

    event = BasicEvent(
        name="test_event",
        expr=expr,
        desc="Test event description",
        required_channels=["test_signal"],
    )

    assert event.name == "test_event"
    assert event.expression is not None
    assert event.description == "Test event description"
    assert event.required_channels == ["test_signal"]


def test_get_name():
    """Test get_name method"""
    event = BasicEvent(name="my_event", expr=TimeSeriesSelector(None))
    assert event.get_name() == "my_event"


def test_get_expression():
    """Test get_expression method returns TimeSeriesExpression"""
    expr = TimeSeriesSelector(None)
    event = BasicEvent(name="test", expr=expr)
    expression = event.get_expression()
    assert expression is not None
    # Expression should be set during initialization
    assert hasattr(expression, "__str__")


def test_get_expression_str():
    """Test get_expression_str method"""
    expr = TimeSeriesSelector(None)
    event = BasicEvent(name="test", expr=expr)
    expr_str = event.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_determine_events(spark, basic_narrow_db):
    """Test determine_events method"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event_expr = eng_rpm > 1000
    event = BasicEvent(name="test_event", expr=event_expr)

    df = BasicEvent.determine_events(
        spark, basic_narrow_db.query, BasicNarrowSolver(spark), [event]
    )

    assert df is not None
    assert "container_id" in df.columns
    assert "event_instance_id" in df.columns
    assert "event_id" in df.columns
    assert "start_ts" in df.columns
    assert "end_ts" in df.columns
    assert df.count() > 0  # Ensure that some data is returned


def test_determine_events_several_events(spark, basic_narrow_db):
    """Test determine_events method"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    event2 = BasicEvent(name="test_event_2", expr=eng_rpm < 1000)

    df = BasicEvent.determine_events(
        spark, basic_narrow_db.query, BasicNarrowSolver(spark), [event, event2]
    )

    assert df is not None
    assert df.select("event_id").distinct().count() == 2  # Ensure both events are present
    assert df.count() > 0  # Ensure that some data is returned


def test_determine_metadata_df(spark):
    """Test determine_metadata_df method"""
    event = BasicEvent(
        name="test_event",
        expr=TimeSeriesSelector(None),
        required_channels=["test_signal"],
    )

    # Call the static method to determine metadata DataFrame
    metadata_df = BasicEvent.determine_metadata_df(spark, [event])

    assert metadata_df is not None
    assert "event_id" in metadata_df.columns
    assert "report_id" in metadata_df.columns
    assert "event_name" in metadata_df.columns
    assert "event_type" in metadata_df.columns
    assert "event_description" in metadata_df.columns
    assert "required_channels" in metadata_df.columns
    assert "event_expression" in metadata_df.columns
    assert metadata_df.count() == 1  # Should return one row for the single event


# ---------------------------------------------------------------------------
# Definition hash tests
# ---------------------------------------------------------------------------
def test_definition_hash_exists_in_as_dict():
    """Verify definition_hash key is present and non-null in as_dict output."""
    event = BasicEvent(name="hash_event", expr=TimeSeriesSelector(None))
    d = event.as_dict()
    assert "definition_hash" in d
    assert d["definition_hash"] is not None
    assert isinstance(d["definition_hash"], int)


def test_definition_hash_exists_in_spark_row():
    """Verify definition_hash field is present in as_spark_row output."""
    event = BasicEvent(name="hash_event", expr=TimeSeriesSelector(None))
    row = event.as_spark_row()
    assert hasattr(row, "definition_hash")
    assert row.definition_hash is not None
    assert isinstance(row.definition_hash, int)


def test_definition_hash_is_deterministic():
    """Same expression must always produce the same hash."""
    expr = TimeSeriesSelector(None)
    ev1 = BasicEvent(name="ev", expr=expr)
    ev2 = BasicEvent(name="ev", expr=expr)
    assert ev1.determine_definition_hash() == ev2.determine_definition_hash()


def test_definition_hash_ignores_name():
    """Hash must not change when only the name differs."""
    expr = TimeSeriesSelector(None)
    ev1 = BasicEvent(name="name_a", expr=expr)
    ev2 = BasicEvent(name="name_b", expr=expr)
    assert ev1.determine_definition_hash() == ev2.determine_definition_hash()


def test_definition_hash_ignores_description():
    """Hash must not change when only desc differs."""
    expr = TimeSeriesSelector(None)
    ev1 = BasicEvent(name="ev", expr=expr, desc="Description A")
    ev2 = BasicEvent(name="ev", expr=expr, desc="Description B")
    assert ev1.determine_definition_hash() == ev2.determine_definition_hash()


def test_definition_hash_ignores_required_channels():
    """Hash must not change when only required_channels differs."""
    expr = TimeSeriesSelector(None)
    ev1 = BasicEvent(name="ev", expr=expr, required_channels=["ch1"])
    ev2 = BasicEvent(name="ev", expr=expr, required_channels=["ch1", "ch2"])
    assert ev1.determine_definition_hash() == ev2.determine_definition_hash()
