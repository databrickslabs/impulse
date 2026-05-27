"""SequenceOfEvents over track-scoped perception steps — end-to-end
through a Spark session, exercising the side-car materializer.

A two-step sequence where both steps are ``ot(track_scope=True)`` predicates
emits ``event_instance_fact`` rows only for windows where the same
``object_id`` satisfies both steps in temporal order within ``max_overlap``;
the LakeVision side-car carries the matching ``object_id``.

Requires the Spark + delta-pip toolchain (CI / FEVM workspace).
"""

import pytest

import pyspark.sql.types as T
from pyspark.sql import Row

from mda_reporting.events.sequence_of_events import SequenceOfEvents

import mda_query_engine.perception.schema as S
from mda_query_engine.perception.events import materialize_event_instance_object_rows
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


pytestmark = pytest.mark.usefixtures("spark")


def _build_solved_df_for_sequence(spark, seq, container_id, windows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType(), False),
            T.StructField(
                seq.get_name(),
                T.ArrayType(T.ArrayType(T.DoubleType())),
                True,
            ),
        ]
    )
    return spark.createDataFrame(
        [Row(container_id=container_id, **{seq.get_name(): windows})],
        schema=schema,
    )


class TestTwoStepTrackScopedSequenceEmitsSideCar:
    def test_one_window_per_object_one_side_car_row_each(self, spark):
        scoped = ObjectTrackAccessor()(track_scope=True)
        seq = SequenceOfEvents(
            name="lane_change_left",
            expressions=[
                scoped.lane_offset == -1,
                scoped.lane_offset == 0,
            ],
            max_overlap=10_000.0,
        )
        # Two objects each yield one chained window.
        windows = [
            [100.0, 300.0, 10.0],
            [600.0, 800.0, 20.0],
        ]
        solved_df = _build_solved_df_for_sequence(spark, seq, 1, windows)

        side_car_df = materialize_event_instance_object_rows(spark, [seq], solved_df)
        rows = side_car_df.collect()
        assert len(rows) == 2
        object_ids = sorted(r["object_id"] for r in rows)
        assert object_ids == [10, 20]

        expected_fields = {f.name for f in S.PERCEPTION_EVENT_INSTANCE_OBJECTS}
        assert set(side_car_df.schema.fieldNames()) == expected_fields


class TestNonScopedSequenceProducesNoSideCar:
    def test_unscoped_sequence_yields_empty_side_car(self, spark):
        ot = ObjectTrackAccessor()
        seq = SequenceOfEvents(
            name="lane_change_anywhere",
            expressions=[
                ot.lane_offset == -1,
                ot.lane_offset == 0,
            ],
        )
        windows = [[100.0, 300.0]]
        solved_df = _build_solved_df_for_sequence(spark, seq, 1, windows)

        side_car_df = materialize_event_instance_object_rows(spark, [seq], solved_df)
        assert side_car_df.count() == 0
