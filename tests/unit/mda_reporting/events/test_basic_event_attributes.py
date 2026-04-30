"""Unit tests for the attributes field on BasicEvent and ContainerEvent."""

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_reporting.events.basic_event import BasicEvent
from mda_reporting.events.container_event import ContainerEvent
from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA


# ---------------------------------------------------------------------------
# BasicEvent — default attributes
# ---------------------------------------------------------------------------
class TestBasicEventAttributes:

    def test_default_attributes_is_empty_dict(self):
        """BasicEvent without attributes kwarg should default to {}."""
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None))
        assert event.attributes == {}

    def test_custom_attributes(self):
        """BasicEvent with explicit attributes should store them."""
        attrs = {"limit_type": "warning", "limit_direction": "lower"}
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None), attributes=attrs)
        assert event.attributes == attrs

    def test_as_dict_includes_attributes(self):
        """as_dict must include the 'attributes' key with correct value."""
        attrs = {"limit_type": "error"}
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None), attributes=attrs)
        d = event.as_dict()
        assert "attributes" in d
        assert d["attributes"] == attrs

    def test_as_dict_default_attributes(self):
        """as_dict must include attributes == {} when none are provided."""
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None))
        d = event.as_dict()
        assert d["attributes"] == {}

    def test_definition_hash_excludes_attributes(self):
        """Changing attributes must not change the definition hash."""
        expr = TimeSeriesSelector(None)
        ev1 = BasicEvent(name="e", expr=expr, attributes={"limit_type": "warning"})
        ev2 = BasicEvent(name="e", expr=expr, attributes={"limit_type": "error"})
        ev3 = BasicEvent(name="e", expr=expr)

        assert ev1.determine_definition_hash() == ev2.determine_definition_hash()
        assert ev1.determine_definition_hash() == ev3.determine_definition_hash()

    def test_as_spark_row_has_attributes_field(self):
        """as_spark_row must include an 'attributes' field."""
        event = BasicEvent(
            name="e",
            expr=TimeSeriesSelector(None),
            attributes={"k": "v"},
        )
        row = event.as_spark_row()
        assert hasattr(row, "attributes")
        assert row.attributes == {"k": "v"}

    def test_as_spark_row_field_count(self):
        """Spark row must have 9 fields matching EVENT_DIMENSION_SCHEMA."""
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None))
        row = event.as_spark_row()
        assert len(row) == len(EVENT_DIMENSION_SCHEMA.fields)

    def test_none_attributes_becomes_empty_dict(self):
        """Passing attributes=None should be treated as {}."""
        event = BasicEvent(name="e", expr=TimeSeriesSelector(None), attributes=None)
        assert event.attributes == {}


# ---------------------------------------------------------------------------
# ContainerEvent — attributes
# ---------------------------------------------------------------------------
class TestContainerEventAttributes:

    def test_default_attributes_is_empty_dict(self):
        """ContainerEvent without attributes kwarg should default to {}."""
        event = ContainerEvent(name="c")
        assert event.attributes == {}

    def test_custom_attributes(self):
        """ContainerEvent with explicit attributes should store them."""
        attrs = {"scope": "full_container"}
        event = ContainerEvent(name="c", attributes=attrs)
        assert event.attributes == attrs

    def test_as_dict_includes_attributes(self):
        """as_dict must include the 'attributes' key."""
        attrs = {"scope": "full_container"}
        event = ContainerEvent(name="c", attributes=attrs)
        d = event.as_dict()
        assert "attributes" in d
        assert d["attributes"] == attrs

    def test_as_dict_default_attributes(self):
        """as_dict must include attributes == {} when none are provided."""
        event = ContainerEvent(name="c")
        d = event.as_dict()
        assert d["attributes"] == {}

    def test_definition_hash_excludes_attributes(self):
        """Changing attributes must not change the definition hash."""
        ev1 = ContainerEvent(name="c", attributes={"a": "1"})
        ev2 = ContainerEvent(name="c", attributes={"b": "2"})
        ev3 = ContainerEvent(name="c")

        assert ev1.determine_definition_hash() == ev2.determine_definition_hash()
        assert ev1.determine_definition_hash() == ev3.determine_definition_hash()

    def test_as_spark_row_has_attributes_field(self):
        """as_spark_row must include an 'attributes' field."""
        event = ContainerEvent(name="c", attributes={"k": "v"})
        row = event.as_spark_row()
        assert hasattr(row, "attributes")
        assert row.attributes == {"k": "v"}

    def test_as_spark_row_field_count(self):
        """Spark row must have 9 fields matching EVENT_DIMENSION_SCHEMA."""
        event = ContainerEvent(name="c")
        row = event.as_spark_row()
        assert len(row) == len(EVENT_DIMENSION_SCHEMA.fields)
