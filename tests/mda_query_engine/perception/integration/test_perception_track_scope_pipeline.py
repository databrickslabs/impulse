"""Per-object windowing on PerceptionEvent — end-to-end through a Spark
session and a Delta-backed event_instance_fact + side-car table.

Requires the Spark + delta-pip toolchain (CI / FEVM workspace). Locally
the unit-tier coverage in ``tests/mda_query_engine/perception/unit/test_perception_solver_track_scope.py``
exercises the per-container UDF on pure pandas inputs.
"""

import pytest

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

import mda_query_engine.perception.schema as S
from mda_query_engine.perception.events.perception_event import PerceptionEvent
from mda_query_engine.perception.query.perception_solver import PerceptionSolver
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


pytestmark = pytest.mark.usefixtures("spark")


def _build_solved_df_for_event(spark, event, container_id, windows):
    """Synthesize a wide solved_df row matching what PerceptionSolver
    would emit for the given event. ``windows`` is a list of either
    [start, end] (non-track-scoped) or [start, end, object_id] (track-scoped)
    inner arrays.
    """
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType(), False),
            T.StructField(
                event.get_name(),
                T.ArrayType(T.ArrayType(T.DoubleType())),
                True,
            ),
        ]
    )
    return spark.createDataFrame(
        [Row(container_id=container_id, **{event.get_name(): windows})],
        schema=schema,
    )


class TestTrackScopedEventEmitsOneRowPerObjectWindow:
    def test_two_objects_two_windows_with_object_id(self, spark):
        scoped = ObjectTrackAccessor()(track_scope=True)
        evt = PerceptionEvent(
            name="cyclist_per_obj",
            expr=scoped.detection_class("cyclist"),
        )
        # Two object_ids produce two windows.
        windows = [
            [100.0, 200.0, 10.0],
            [600.0, 700.0, 20.0],
        ]
        solved_df = _build_solved_df_for_event(spark, evt, 1, windows)

        fact_df = PerceptionEvent.determine_events(
            spark, [evt], solved_df=solved_df
        )
        side_car_df = PerceptionEvent.determine_perception_event_instance_objects(
            spark, [evt], solved_df=solved_df
        )

        fact_rows = fact_df.collect()
        side_car_rows = side_car_df.collect()
        assert len(fact_rows) == 2
        assert len(side_car_rows) == 2
        side_car_object_ids = sorted(r["object_id"] for r in side_car_rows)
        assert side_car_object_ids == [10, 20]

    def test_non_scoped_event_emits_no_side_car_rows(self, spark):
        ot = ObjectTrackAccessor()
        evt = PerceptionEvent(
            name="cyclist_anywhere",
            expr=ot.detection_class("cyclist"),
        )
        windows = [
            [100.0, 200.0],
            [600.0, 700.0],
        ]
        solved_df = _build_solved_df_for_event(spark, evt, 1, windows)

        fact_df = PerceptionEvent.determine_events(
            spark, [evt], solved_df=solved_df
        )
        side_car_df = PerceptionEvent.determine_perception_event_instance_objects(
            spark, [evt], solved_df=solved_df
        )

        assert fact_df.count() == 2
        assert side_car_df.count() == 0


class TestMinDurationDebounce:
    def test_min_duration_drops_short_windows(self, spark):
        scoped = ObjectTrackAccessor()(track_scope=True)
        # min_duration_ms is in milliseconds, frame_ts is microseconds —
        # one window long enough (600 us == 0.6 ms), one too short.
        # Use 500 ms threshold → 500_000 microseconds.
        evt = PerceptionEvent(
            name="long_only",
            expr=scoped.detection_class("cyclist"),
            min_duration_ms=500,
        )
        windows = [
            [100.0, 100.0 + 600_000.0, 10.0],  # 600 ms — kept
            [2_000_000.0, 2_000_000.0 + 100_000.0, 20.0],  # 100 ms — dropped
        ]
        solved_df = _build_solved_df_for_event(spark, evt, 1, windows)

        fact_df = PerceptionEvent.determine_events(
            spark, [evt], solved_df=solved_df
        )
        side_car_df = PerceptionEvent.determine_perception_event_instance_objects(
            spark, [evt], solved_df=solved_df
        )

        assert fact_df.count() == 1
        assert side_car_df.count() == 1
        assert side_car_df.collect()[0]["object_id"] == 10

    def test_no_min_duration_retains_all_windows(self, spark):
        scoped = ObjectTrackAccessor()(track_scope=True)
        evt = PerceptionEvent(
            name="all_durations",
            expr=scoped.detection_class("cyclist"),
        )
        windows = [
            [100.0, 100.0 + 600_000.0, 10.0],
            [2_000_000.0, 2_000_000.0 + 100_000.0, 20.0],
        ]
        solved_df = _build_solved_df_for_event(spark, evt, 1, windows)

        fact_df = PerceptionEvent.determine_events(
            spark, [evt], solved_df=solved_df
        )
        assert fact_df.count() == 2


class TestSideCarSchemaMatchesSideCarRows:
    def test_side_car_dataframe_matches_schema(self, spark):
        scoped = ObjectTrackAccessor()(track_scope=True)
        evt = PerceptionEvent(name="x", expr=scoped.detection_class("cyclist"))
        windows = [[100.0, 200.0, 10.0]]
        solved_df = _build_solved_df_for_event(spark, evt, 1, windows)

        side_car_df = PerceptionEvent.determine_perception_event_instance_objects(
            spark, [evt], solved_df=solved_df
        )
        expected_fields = {f.name for f in S.PERCEPTION_EVENT_INSTANCE_OBJECTS}
        actual_fields = set(side_car_df.schema.fieldNames())
        assert expected_fields == actual_fields
