"""Schema smoke tests for mda_query_engine core tables.

Verifies that each schema can be used to create an empty DataFrame and that
key field names and types match the expectations consumers rely on.
"""

import pyspark.sql.types as T

from mda_query_engine.schema import (
    CHANNEL_TAGS,
    CHANNEL_VALUE_LABELS,
    CHANNELS_SCHEMA,
    CONTAINER_TAGS,
)


def _field(schema: T.StructType, name: str) -> T.StructField:
    return schema[name]


class TestChannelValueLabels:
    def test_has_required_fields(self):
        required = {"channel_id", "numeric_value", "label"}
        assert required <= {f.name for f in CHANNEL_VALUE_LABELS}

    def test_numeric_value_is_double(self):
        assert isinstance(_field(CHANNEL_VALUE_LABELS, "numeric_value").dataType, T.DoubleType)

    def test_channel_id_is_long(self):
        assert isinstance(_field(CHANNEL_VALUE_LABELS, "channel_id").dataType, T.LongType)

    def test_label_is_non_nullable_string(self):
        f = _field(CHANNEL_VALUE_LABELS, "label")
        assert isinstance(f.dataType, T.StringType)
        assert f.nullable is False


class TestCoreSchemasShape:
    def test_container_tags_keys(self):
        assert {"container_id", "key", "value"} <= {f.name for f in CONTAINER_TAGS}

    def test_channel_tags_keys(self):
        assert {"container_id", "channel_id", "key", "value"} <= {f.name for f in CHANNEL_TAGS}

    def test_channels_schema_keys(self):
        assert {"container_id", "channel_id", "tstart", "tend", "value"} <= {f.name for f in CHANNELS_SCHEMA}
