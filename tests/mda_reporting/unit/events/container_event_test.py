"""Unit tests for ContainerEvent."""

import pyspark.sql.functions as f

from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_reporting.events.container_event import ContainerEvent
from tests.conftest import basic_narrow_db, spark


# ---------------------------------------------------------------------------
# Constructor / basic attributes
# ---------------------------------------------------------------------------
def test_container_event_init():
    """Test ContainerEvent initialisation with required parameters."""
    event = ContainerEvent(name="container_evt")

    assert event.name == "container_evt"
    assert event.description is None


def test_container_event_init_with_desc():
    """Test ContainerEvent initialisation with optional description."""
    event = ContainerEvent(name="container_evt", desc="full container")

    assert event.name == "container_evt"
    assert event.description == "full container"


# ---------------------------------------------------------------------------
# get_id
# ---------------------------------------------------------------------------
def test_get_id():
    """Test get_id method returns unique identifier"""
    event = ContainerEvent(name="test_event")
    event_id = event.get_id()
    assert isinstance(event_id, int)
    assert event_id > 0


def test_get_id_deterministic():
    """Same name must produce the same ID."""
    ev1 = ContainerEvent(name="evt")
    ev2 = ContainerEvent(name="evt")
    assert ev1.get_id() == ev2.get_id()


def test_get_id_unique_for_different_names():
    """Different names should (almost certainly) produce different IDs."""
    ev1 = ContainerEvent(name="evt_a")
    ev2 = ContainerEvent(name="evt_b")
    assert ev1.get_id() != ev2.get_id()


def test_get_id_positive():
    """ID must be a positive 32-bit integer."""
    event = ContainerEvent(name="pos_check")
    assert event.get_id() > 0
    assert event.get_id() <= 0x7FFFFFFF


# ---------------------------------------------------------------------------
# get_expression
# ---------------------------------------------------------------------------
def test_get_expression_returns_none():
    """ContainerEvent has no time-series expression."""
    event = ContainerEvent(name="no_expr")
    assert event.get_expression() is None


def test_get_name():
    """Test get_name method"""
    event = ContainerEvent(name="my_event")
    assert event.get_name() == "my_event"

    # Different names should produce different IDs
    event2 = ContainerEvent(name="other_event")
    assert event.get_id() != event2.get_id()

    # Same name should produce same ID
    event3 = ContainerEvent(name="my_event")
    assert event.get_id() == event3.get_id()


# ---------------------------------------------------------------------------
# as_dict
# ---------------------------------------------------------------------------
def test_as_dict():
    """Verify structure and content of as_dict output."""
    event = ContainerEvent(name="my_container_event", desc="test description")
    d = event.as_dict()

    assert isinstance(d, dict)
    assert d["event_id"] == event.get_id()
    assert d["report_id"] == -1  # default
    assert d["event_type"] == "CONTAINER_EVENT"
    assert d["event_name"] == "my_container_event"
    assert d["event_description"] == "test description"
    assert d["required_channels"] is None
    assert d["event_expression"] == "NA"
    assert "definition_hash" in d
    assert isinstance(d["definition_hash"], int)


def test_as_dict_with_description():
    """Test as_dict method with description"""
    event = ContainerEvent(name="my_event_1", desc="My description")
    event_dict = event.as_dict()
    assert event_dict.get("event_description") == "My description"


def test_get_event_type_str():
    """ContainerEvent should return CONTAINER_EVENT as event type string."""
    event = ContainerEvent(name="my_container_event")
    assert event.get_event_type_str() == "CONTAINER_EVENT"


# ---------------------------------------------------------------------------
# as_spark_row
# ---------------------------------------------------------------------------
def test_as_spark_row():
    """Verify as_spark_row returns a Row with the expected number of fields."""
    event = ContainerEvent(name="row_evt")
    row = event.as_spark_row()
    assert len(row) == 9


# ---------------------------------------------------------------------------
# definition hash
# ---------------------------------------------------------------------------
def test_definition_hash_deterministic():
    """Same event type must produce the same definition hash."""
    ev1 = ContainerEvent(name="a")
    ev2 = ContainerEvent(name="b")
    assert ev1.determine_definition_hash() != ev2.determine_definition_hash()


def test_definition_hash_ignores_description():
    """Hash must not change when only desc differs."""
    ev1 = ContainerEvent(name="ev", desc="Alpha")
    ev2 = ContainerEvent(name="ev", desc="Beta")
    assert ev1.determine_definition_hash() == ev2.determine_definition_hash()


# ---------------------------------------------------------------------------
# determine_events (integration-ish, needs Spark)
# ---------------------------------------------------------------------------
def test_determine_events(spark, basic_narrow_db):
    """Test determine_events returns a valid event instance fact DataFrame."""
    event = ContainerEvent(name="full_container")

    df = ContainerEvent.determine_events(
        spark,
        [event],
        query=basic_narrow_db.query,
        solver=BasicNarrowSolver(spark),
    )

    assert df is not None
    assert "container_id" in df.columns
    assert "event_instance_id" in df.columns
    assert "event_id" in df.columns
    assert "start_ts" in df.columns
    assert "end_ts" in df.columns
    assert df.count() > 0

    # Compare event_instance_id with crc32(container_id)

    df_with_test = df.withColumn("test_values", f.crc32(f.col("container_id").cast("string")))
    for row in df_with_test.collect():
        assert row.event_instance_id == row.test_values, (
            f"container_id={row.container_id}: "
            f"event_instance_id={row.event_instance_id} != test_values={row.test_values}"
        )

    total_rows = df.count()
    unique_rows = df.select("event_instance_id").distinct().count()

    assert unique_rows == total_rows, (
        f"Found duplicate event_instance_id values: total_rows={total_rows}, "
        f"unique_event_instance_ids={unique_rows}"
    )


# ---------------------------------------------------------------------------
# determine_metadata_df (needs Spark)
# ---------------------------------------------------------------------------
def test_determine_metadata_df(spark):
    """Test determine_metadata_df returns a valid event dimension DataFrame."""
    event = ContainerEvent(name="meta_evt", desc="description")

    metadata_df = ContainerEvent.determine_metadata_df(spark, [event])

    assert metadata_df is not None
    assert "event_id" in metadata_df.columns
    assert "report_id" in metadata_df.columns
    assert "event_name" in metadata_df.columns
    assert "event_type" in metadata_df.columns
    assert "event_description" in metadata_df.columns
    assert "required_channels" in metadata_df.columns
    assert "event_expression" in metadata_df.columns
    assert "definition_hash" in metadata_df.columns
    assert metadata_df.count() == 1

    row = metadata_df.collect()[0]
    assert row.event_expression == "NA"
    assert row.required_channels is None
