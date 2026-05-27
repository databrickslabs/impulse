"""``MeasurementDB.event_instance_fact`` LEFT JOIN'd to the perception
side-car populates ``object_id`` for track-scoped perception rows and
leaves it NULL otherwise.

Requires the Spark + delta-pip toolchain (CI / FEVM workspace).
"""

import pytest

from pyspark.sql import Row
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

import mda_query_engine.perception.schema as S
from mda_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from mda_query_engine.perception.perception_db import PerceptionDBConfig


pytestmark = pytest.mark.usefixtures("spark")


@pytest.fixture
def perception_db(spark):
    fact_rows = [
        # Track-scoped perception row — object_id should land via side-car.
        Row(container_id=1, event_instance_id=111, event_id=999, start_ts=100, end_ts=200),
        # Channel-only row (BasicEvent) — no side-car, object_id NULL.
        Row(container_id=1, event_instance_id=222, event_id=888, start_ts=300, end_ts=400),
        # Non-track-scoped perception row — no side-car, object_id NULL.
        Row(container_id=1, event_instance_id=333, event_id=777, start_ts=500, end_ts=600),
    ]
    fact_df = spark.createDataFrame(fact_rows, schema=EVENT_INSTANCE_FACT_SCHEMA)

    side_car_rows = [
        Row(container_id=1, event_id=999, event_instance_id=111, object_id=42),
    ]
    side_car_df = spark.createDataFrame(side_car_rows, schema=S.PERCEPTION_EVENT_INSTANCE_OBJECTS)

    debug_tables = {
        "event_instance_fact": fact_df,
        "perception_event_instance_objects": side_car_df,
    }
    measurement_cfg = MeasurementDBConfig.for_debug({})
    measurement_cfg.debug_tables = debug_tables

    perception_cfg = PerceptionDBConfig.for_debug(debug_tables)

    return MeasurementDB(
        config=measurement_cfg,
        ws=create_autospec(WorkspaceClient),
        perception_config=perception_cfg,
        event_instance_fact_table="event_instance_fact",
    )


class TestEventInstanceFactJoin:
    def test_track_scoped_row_carries_object_id(self, spark, perception_db):
        joined = perception_db.event_instance_fact(spark)
        track_scoped = (
            joined.where("event_instance_id = 111").select("object_id").collect()
        )
        assert len(track_scoped) == 1
        assert track_scoped[0]["object_id"] == 42

    def test_channel_only_row_has_null_object_id(self, spark, perception_db):
        joined = perception_db.event_instance_fact(spark)
        ch_only = joined.where("event_instance_id = 222").select("object_id").collect()
        assert len(ch_only) == 1
        assert ch_only[0]["object_id"] is None

    def test_non_scoped_perception_row_has_null_object_id(self, spark, perception_db):
        joined = perception_db.event_instance_fact(spark)
        non_scoped = joined.where("event_instance_id = 333").select("object_id").collect()
        assert len(non_scoped) == 1
        assert non_scoped[0]["object_id"] is None

    def test_joined_row_count_matches_fact_row_count(self, spark, perception_db):
        joined = perception_db.event_instance_fact(spark)
        # LEFT JOIN never duplicates or drops fact rows when the side-car PK is unique.
        assert joined.count() == 3
