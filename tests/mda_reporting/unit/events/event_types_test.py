import pytest
from pyspark.sql.types import StructType

from mda_reporting.events.event_types import EventType


def test_basic_event():
    """
    Test get_fact_table_name and get_dimension_table_name for BASIC_EVENT type.

    Notes
    -----
    Verifies that the BASIC_EVENT type returns the expected
    fact and dimension table names.
    """
    result = EventType.BASIC_EVENT.get_fact_table_name()
    assert result == "event_instance_fact"

    result = EventType.BASIC_EVENT.get_dimension_table_name()
    assert result == "event_dimension"


def test_container_event():
    """
    Test get_fact_table_name and get_dimension_table_name for CONTAINER_EVENT type.
    """
    result = EventType.CONTAINER_EVENT.get_fact_table_name()
    assert result == "event_instance_fact"

    result = EventType.CONTAINER_EVENT.get_dimension_table_name()
    assert result == "event_dimension"


def test_all_event_type_methods_supported():
    """
    Test that all methods work for all EventType enum values.

    Notes
    -----
    Loops through all EventType enum values and verifies that
    get_fact_table_name(), get_dimension_table_name(), and get_schema()
    return valid values without raising ValueError. This ensures all enum
    values are properly supported in all match statements.

    Raises
    ------
    AssertionError
        If any enum value doesn't return valid results from any method.
    ValueError
        If any enum value is not supported in any match statement.
    """
    for event_type in EventType:
        # Test get_fact_table_name()
        fact_table_name = event_type.get_fact_table_name()
        assert isinstance(fact_table_name, str)
        assert len(fact_table_name) > 0

        # Test get_dimension_table_name()
        dimension_table_name = event_type.get_dimension_table_name()
        assert isinstance(dimension_table_name, str)
        assert len(dimension_table_name) > 0

        # Test get_schema()
        schema = event_type.get_fact_schema()
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0


def test_unsupported_event_type():
    """
    Test that unsupported EventType raises ValueError.

    Notes
    -----
    This test creates a mock unsupported event type and verifies
    that calling methods raises a ValueError.
    """

    class UnsupportedEventType:
        pass

    unsupported_type = UnsupportedEventType()
    with pytest.raises(ValueError):
        EventType.get_fact_table_name(unsupported_type)

    with pytest.raises(ValueError):
        EventType.get_fact_schema(unsupported_type)

    with pytest.raises(ValueError):
        EventType.get_dimension_table_name(unsupported_type)

    with pytest.raises(ValueError):
        EventType.get_dimension_schema(unsupported_type)


def test_get_any_for_fact_table():
    """Test that get_any_for_fact_table returns a valid EventType."""
    et = EventType.get_any_for_fact_table("event_instance_fact")
    assert et in (EventType.BASIC_EVENT, EventType.CONTAINER_EVENT)


def test_get_any_for_fact_table_unknown():
    """Test that unknown table name raises ValueError."""
    with pytest.raises(ValueError, match="No EventType found"):
        EventType.get_any_for_fact_table("non_existent_table")


def test_get_any_for_dimension_table():
    """Test that get_any_for_dimension_table returns a valid EventType."""
    et = EventType.get_any_for_dimension_table("event_dimension")
    assert et in (EventType.BASIC_EVENT, EventType.CONTAINER_EVENT)
    assert et.get_dimension_table_name() == "event_dimension"


def test_get_any_for_unknown_fact_table_raises():
    with pytest.raises(ValueError, match="No EventType found for fact table"):
        EventType.get_any_for_fact_table("unknown_fact_table")


def test_get_any_for_unknown_dimension_table_raises():
    with pytest.raises(ValueError, match="No EventType found for dimension table"):
        EventType.get_any_for_dimension_table("unknown_dimension_table")


def test_get_any_for_dimension_table_unknown():
    """Test that unknown table name raises ValueError."""
    with pytest.raises(ValueError, match="No EventType found"):
        EventType.get_any_for_dimension_table("non_existent_table")
