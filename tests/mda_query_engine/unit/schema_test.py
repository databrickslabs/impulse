"""Schema smoke tests for mda_query_engine core tables.

Verifies that each schema can be used to create an empty DataFrame and that
key field names and types match the expectations consumers rely on.
"""

import pyspark.sql.types as T

from mda_query_engine.schema import (
    CHANNEL_TAGS,
    CHANNELS_SCHEMA,
    CONTAINER_TAGS,
)


def _field(schema: T.StructType, name: str) -> T.StructField:
    return schema[name]


class TestCoreSchemasShape:
    def test_container_tags_keys(self):
        assert {"container_id", "key", "value"} <= {f.name for f in CONTAINER_TAGS}

    def test_channel_tags_keys(self):
        assert {"container_id", "channel_id", "key", "value"} <= {f.name for f in CHANNEL_TAGS}

    def test_channels_schema_keys(self):
        assert {"container_id", "channel_id", "tstart", "tend", "value"} <= {f.name for f in CHANNELS_SCHEMA}
