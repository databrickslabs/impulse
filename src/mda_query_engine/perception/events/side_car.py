"""Side-car materializer for per-object event windows.

Reads a wide solved DataFrame produced by ``PerceptionSolver`` and emits
the LakeVision side-car rows that carry ``object_id`` per matched window.
Works for any event whose expression contains track-scoped perception
selectors — ``PerceptionEvent`` today, ``SequenceOfEvents`` (mda_reporting)
when its steps include track-scoped perception predicates.

The Core ``event_instance_fact`` writer continues to write the standard
``(container_id, event_instance_id, event_id, start_ts, end_ts)`` shape;
the side-car write below runs alongside it using the same
``event_instance_id`` generator so the two tables join cleanly.
"""

from __future__ import annotations

from typing import Iterable

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

from mda_reporting.util.event_instance_util import generate_event_instance_id_column
from mda_reporting.util.report_entity_util import ReportEntityUtil

from mda_query_engine.perception.schema.scenario import PERCEPTION_EVENT_INSTANCE_OBJECTS
from mda_query_engine.perception.tsal.perception_selector import is_track_scoped


def _filter_min_duration(exploded_df, events: list):
    """Per-event ``min_duration_ms`` debounce. Events without the attribute
    pass through unaffected. Threshold is interpreted in milliseconds and
    converted to microseconds (matching the unit of ``frame_ts`` and the
    per-window ``start_ts``/``end_ts`` carried through the solver).
    """
    active = [
        e for e in events
        if getattr(e, "min_duration_ms", None) is not None
    ]
    if not active:
        return exploded_df
    duration_us = f.col("end_ts") - f.col("start_ts")
    keep = f.lit(True)
    for evt in active:
        threshold_us = int(evt.min_duration_ms) * 1000
        keep = keep & (
            (f.col("event_name") != f.lit(evt.get_name()))
            | (duration_us >= f.lit(threshold_us))
        )
    return exploded_df.where(keep)


def materialize_event_instance_object_rows(
    spark: SparkSession,
    events: Iterable,
    solved_df,
):
    """Emit side-car rows for track-scoped events in ``events``.

    Walks ``events`` for any whose expression has at least one perception
    selector with ``track_scope=True``. For each such event, explodes the
    matching column in ``solved_df`` (a wide DataFrame where each event has
    its own array column carrying ``[start, end, object_id]`` triples for
    track-scoped windows) and projects the schema
    ``PERCEPTION_EVENT_INSTANCE_OBJECTS``.

    Returns an empty DataFrame matching the side-car schema when no event
    in ``events`` is track-scoped.
    """
    events_list = list(events)
    track_scoped = [
        e for e in events_list
        if is_track_scoped(getattr(e, "expression", None))
    ]
    if not track_scoped:
        return spark.createDataFrame([], schema=PERCEPTION_EVENT_INSTANCE_OBJECTS)

    event_names = [e.get_name() for e in track_scoped]
    side_car = (
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
        .withColumn("object_id", f.col("event_instance").getItem(2).cast(t.LongType()))
    )
    side_car = _filter_min_duration(side_car, track_scoped)
    side_car = (
        side_car
        .where(f.col("start_ts") < f.col("end_ts"))
        .where(f.col("object_id").isNotNull())
        .withColumn(
            "event_instance_id",
            generate_event_instance_id_column(event_type=None),
        )
        .withColumn(
            "event_id",
            ReportEntityUtil.get_event_id_column(
                elements=track_scoped, element_name="event_name"
            ),
        )
        .select(
            f.col("container_id").cast(t.LongType()).alias("container_id"),
            f.col("event_id").cast(t.LongType()).alias("event_id"),
            f.col("event_instance_id").cast(t.LongType()).alias("event_instance_id"),
            f.col("object_id").cast(t.LongType()).alias("object_id"),
        )
        .dropDuplicates(["container_id", "event_id", "event_instance_id"])
    )
    return side_car
