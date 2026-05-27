"""PerceptionEvent — a report event over a compound predicate that may
include perception leaves.

Mirrors ``BasicEvent`` in shape and ``determine_events`` behaviour.
``get_event_type_str()`` returns ``"PERCEPTION_EVENT"``; the class name
itself signals that ``object_tracks`` is an input, so no separate
``required_sources`` field is needed.

When the predicate's perception leaves carry ``track_scope=True`` (see
``ObjectTrackAccessor``), windows form per ``(container_id, object_id)``;
a LakeVision-side companion table records the per-window ``object_id``.
The fact-table contract from the foundation perception ADR is unchanged:
``event_instance_fact`` still gets one row per matched window.
"""

from __future__ import annotations

from collections.abc import Mapping

import pyspark.sql.functions as f
import pyspark.sql.types as t
import zlib
from pyspark.sql import Row, SparkSession

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.events.event import Event
from mda_reporting.events.basic_event import BasicEvent
from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from mda_reporting.util.event_instance_util import generate_event_instance_id_column
from mda_reporting.util.report_entity_util import ReportEntityUtil

from mda_query_engine.perception.tsal.perception_selector import (
    is_track_scoped,
    perception_selectors_track_scope_values,
)


class PerceptionEvent(Event):
    """Event whose predicate may include perception leaves over ``object_tracks``.

    ``min_duration_ms`` is an optional post-window-formation debounce: any
    emitted window shorter than the threshold is dropped before write. The
    same threshold applies regardless of whether the predicate is
    track-scoped.

    Mixing track-scoped and non-track-scoped perception selectors inside a
    single predicate raises at construction time — see
    ``track_scope`` discussion in the future-decisions doc.
    """

    def __init__(
        self,
        name: str,
        expr: TimeSeriesExpression,
        desc: str | None = None,
        required_channels: list[str] | None = None,
        attributes: Mapping[str, str] | None = None,
        min_duration_ms: int | None = None,
    ):
        scope_values = perception_selectors_track_scope_values(expr)
        if scope_values == {True, False}:
            raise ValueError(
                f"PerceptionEvent {name!r}: a single predicate may not mix "
                "track-scoped and non-track-scoped perception selectors. "
                "Use two events if you need both shapes."
            )
        if min_duration_ms is not None:
            if not isinstance(min_duration_ms, int) or min_duration_ms < 0:
                raise ValueError(
                    f"PerceptionEvent {name!r}: min_duration_ms must be a "
                    f"non-negative int (got {min_duration_ms!r})."
                )

        Event.__init__(self, name)
        self.expression = expr.alias(name)
        self.description = desc
        self.required_channels = required_channels
        self.min_duration_ms = min_duration_ms
        normalized_attributes: dict[str, str] = {}
        if attributes is not None:
            normalized_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.attributes = normalized_attributes

    @property
    def track_scope(self) -> bool:
        return is_track_scoped(self.expression)

    def get_id(self) -> int:
        return zlib.crc32(self.name.encode()) & 0x7FFFFFFF

    def get_expression(self) -> TimeSeriesExpression | None:
        return self.expression

    def get_event_type_str(self) -> str:
        return "PERCEPTION_EVENT"

    def determine_definition_hash(self) -> int:
        import hashlib

        hash_input = self.get_expression_str()
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        return {
            "event_id": self.get_id(),
            "report_id": self.report_id,
            "event_type": self.get_event_type_str(),
            "event_name": self.name,
            "event_description": self.description,
            "required_channels": self.required_channels,
            "event_expression": self.get_expression_str(),
            "definition_hash": self.determine_definition_hash(),
            "attributes": self.attributes,
        }

    def as_spark_row(self) -> Row:
        return Row(**self.as_dict())

    @classmethod
    def determine_events(
        cls,
        spark: SparkSession,
        events: list["PerceptionEvent"],
        *,
        solved_df=None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df=None,
    ):
        if solved_df is None:
            raise ValueError(
                "PerceptionEvent.determine_events requires solved_df. "
                "Provide a pre-solved DataFrame from the perception solver."
            )

        event_names = [event.get_name() for event in events]
        exploded = (
            solved_df.select("container_id", *event_names)
            .unpivot(
                f.col("container_id"),
                event_names,
                variableColumnName="event_name",
                valueColumnName="value",
            )
            .select(
                "container_id",
                "event_name",
                f.explode(f.col("value")).alias("event_instance"),
            )
            .withColumn("start_ts", f.col("event_instance").getItem(0))
            .withColumn("end_ts", f.col("event_instance").getItem(1))
        )
        exploded = cls._apply_min_duration_filter(exploded, events)
        df = (
            exploded
            .withColumn(
                "event_instance_id",
                generate_event_instance_id_column(event_type=cls),
            )
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
            )
            .select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())
            .where(f.col("start_ts") < f.col("end_ts"))
        )
        return df

    @staticmethod
    def _apply_min_duration_filter(exploded_df, events: list["PerceptionEvent"]):
        """Drop windows shorter than each event's ``min_duration_ms``.

        Per-event filter keyed off ``event_name``. Events without a
        ``min_duration_ms`` pass through unaffected. The threshold is
        converted from milliseconds to microseconds to match the unit
        convention of ``object_tracks.frame_ts``.
        """
        active = [e for e in events if getattr(e, "min_duration_ms", None) is not None]
        if not active:
            return exploded_df
        duration_us = (f.col("end_ts") - f.col("start_ts"))
        keep = f.lit(True)
        for evt in active:
            threshold_us = int(evt.min_duration_ms) * 1000
            keep = keep & (
                (f.col("event_name") != f.lit(evt.get_name()))
                | (duration_us >= f.lit(threshold_us))
            )
        return exploded_df.where(keep)

    @classmethod
    def determine_perception_event_instance_objects(
        cls,
        spark: SparkSession,
        events: list["PerceptionEvent"],
        *,
        solved_df=None,
    ):
        """Materialize the LakeVision side-car rows for track-scoped events.

        Returns a DataFrame matching ``PERCEPTION_EVENT_INSTANCE_OBJECTS``:
        one row per ``event_instance_fact`` row from a track-scoped
        ``PerceptionEvent``. Non-track-scoped events contribute nothing.
        ``event_instance_id`` is generated with the same expression as
        ``determine_events`` so the two DataFrames are joinable on
        ``(container_id, event_id, event_instance_id)``.

        Delegates to the shared side-car materializer, which also handles
        track-scoped ``SequenceOfEvents`` (mda_reporting) instances under
        the LakeVision-side join path.
        """
        from mda_query_engine.perception.events.side_car import materialize_event_instance_object_rows

        if solved_df is None:
            raise ValueError(
                "PerceptionEvent.determine_perception_event_instance_objects "
                "requires solved_df."
            )
        return materialize_event_instance_object_rows(spark, events, solved_df)

    @classmethod
    def determine_metadata_df(cls, spark: SparkSession, events: list["PerceptionEvent"]):
        rows = [event.as_spark_row() for event in events]
        return spark.createDataFrame(rows, schema=EVENT_DIMENSION_SCHEMA)
