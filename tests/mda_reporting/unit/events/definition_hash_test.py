"""Unit tests for definition hash method in BasicEvent."""

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_reporting.events.basic_event import BasicEvent


class TestBasicEventDefinitionHash:
    """Test suite for BasicEvent.determine_definition_hash()."""

    def test_definition_hash_returns_int(self):
        """Test that determine_definition_hash returns an integer."""
        expr = TimeSeriesSelector(None)
        event = BasicEvent(name="test_event", expr=expr)

        hash_value = event.determine_definition_hash()
        assert isinstance(hash_value, int)

    def test_same_expression_produces_same_hash(self):
        """Test that identical expressions produce the same hash."""
        expr = TimeSeriesSelector(None)

        event1 = BasicEvent(name="event_a", expr=expr)
        event2 = BasicEvent(name="event_b", expr=expr)

        # Same expression, different names -> same hash
        assert event1.determine_definition_hash() == event2.determine_definition_hash()

    def test_hash_excludes_name(self):
        """Test that hash doesn't change when only name changes."""
        expr = TimeSeriesSelector(None)

        event1 = BasicEvent(name="event_version_1", expr=expr)
        event2 = BasicEvent(name="event_version_2", expr=expr)

        assert event1.determine_definition_hash() == event2.determine_definition_hash()

    def test_hash_excludes_description(self):
        """Test that hash doesn't change when only description changes."""
        expr = TimeSeriesSelector(None)

        event1 = BasicEvent(name="test_event", expr=expr, desc="Description v1")
        event2 = BasicEvent(name="test_event", expr=expr, desc="Description v2")

        assert event1.determine_definition_hash() == event2.determine_definition_hash()

    def test_hash_is_consistent_across_instances(self):
        """Test that hash is consistent for same expression across multiple calls."""
        expr = TimeSeriesSelector(None)
        event = BasicEvent(name="test_event", expr=expr)

        hash1 = event.determine_definition_hash()
        hash2 = event.determine_definition_hash()
        hash3 = event.determine_definition_hash()

        assert hash1 == hash2 == hash3

    def test_get_id_differs_from_definition_hash(self):
        """Test that get_id and determine_definition_hash produce different values."""
        expr = TimeSeriesSelector(None)

        event1 = BasicEvent(name="event_a", expr=expr)
        event2 = BasicEvent(name="event_b", expr=expr)

        # get_id includes name -> different IDs
        assert event1.get_id() != event2.get_id()

        # definition_hash excludes name -> same hash
        assert event1.determine_definition_hash() == event2.determine_definition_hash()

    def test_as_dict_includes_definition_hash(self):
        """Test that as_dict includes the definition_hash field."""
        expr = TimeSeriesSelector(None)
        event = BasicEvent(name="test_event", expr=expr)

        result = event.as_dict()

        assert "definition_hash" in result
        assert result["definition_hash"] == event.determine_definition_hash()

    def test_hash_value_within_long_range(self):
        """Test that hash value fits within signed 64-bit long range."""
        expr = TimeSeriesSelector(None)
        event = BasicEvent(name="test_event", expr=expr)

        hash_value = event.determine_definition_hash()

        # Signed 64-bit long range
        min_long = -(2**63)
        max_long = 2**63 - 1

        assert min_long <= hash_value <= max_long
