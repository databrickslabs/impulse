"""Unit tests for dimension schemas with definition_hash field."""

from pyspark.sql.types import LongType

from mda_reporting.persist.dimension_schema import (
    HISTOGRAM_DIMENSION_SCHEMA,
    HISTOGRAM2D_DIMENSION_SCHEMA,
    EVENT_DIMENSION_SCHEMA,
)


class TestHistogramDimensionSchema:
    """Test suite for HISTOGRAM_DIMENSION_SCHEMA."""

    def test_has_definition_hash_field(self):
        """Test that schema contains definition_hash field."""
        field_names = [field.name for field in HISTOGRAM_DIMENSION_SCHEMA.fields]
        assert "definition_hash" in field_names

    def test_definition_hash_is_long_type(self):
        """Test that definition_hash field is LongType."""
        field = next(f for f in HISTOGRAM_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert isinstance(field.dataType, LongType)

    def test_definition_hash_is_nullable(self):
        """Test that definition_hash field is nullable."""
        field = next(f for f in HISTOGRAM_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert field.nullable is True


class TestHistogram2DDimensionSchema:
    """Test suite for HISTOGRAM2D_DIMENSION_SCHEMA."""

    def test_has_definition_hash_field(self):
        """Test that schema contains definition_hash field."""
        field_names = [field.name for field in HISTOGRAM2D_DIMENSION_SCHEMA.fields]
        assert "definition_hash" in field_names

    def test_definition_hash_is_long_type(self):
        """Test that definition_hash field is LongType."""
        field = next(f for f in HISTOGRAM2D_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert isinstance(field.dataType, LongType)

    def test_definition_hash_is_nullable(self):
        """Test that definition_hash field is nullable."""
        field = next(f for f in HISTOGRAM2D_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert field.nullable is True


class TestEventDimensionSchema:
    """Test suite for EVENT_DIMENSION_SCHEMA."""

    def test_has_definition_hash_field(self):
        """Test that schema contains definition_hash field."""
        field_names = [field.name for field in EVENT_DIMENSION_SCHEMA.fields]
        assert "definition_hash" in field_names

    def test_has_event_type_field(self):
        """Test that schema contains event_type field."""
        field_names = [field.name for field in EVENT_DIMENSION_SCHEMA.fields]
        assert "event_type" in field_names

    def test_definition_hash_is_long_type(self):
        """Test that definition_hash field is LongType."""
        field = next(f for f in EVENT_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert isinstance(field.dataType, LongType)

    def test_definition_hash_is_nullable(self):
        """Test that definition_hash field is nullable."""
        field = next(f for f in EVENT_DIMENSION_SCHEMA.fields if f.name == "definition_hash")
        assert field.nullable is True
