"""Utility functions for event instance ID generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Column

if TYPE_CHECKING:
    from impulse_reporting.events.event import Event


def generate_event_instance_id_column(
    event_type: type[Event] | None = None,
    container_id_col: str = "container_id",
    event_name_col: str = "event_name",
    start_ts_col: str = "start_ts",
    end_ts_col: str = "end_ts",
    window_index_col: str = "window_index",
) -> Column:
    """
    Generate an event_instance_id column.

    The id is an xxHash64 of ``container_id::event_name::start_ts::end_ts``.
    For ``ContainerEvent`` only ``container_id`` is hashed, since a container
    event produces exactly one instance per container. For ``TimeWindowEvent``
    the window's position replaces the timestamps
    (``container_id::event_name::window_index``), so the event fact and the
    aggregations scoped to it agree without bit-identical window boundaries.
    The result is a signed 64-bit long (may be negative), wide enough to keep
    this merge/join key collision-free at scale.

    Parameters
    ----------
    event_type : type[Event] or None, optional
        The event class.  When the class is ``ContainerEvent``, the
        ``container_id`` column is hashed; for ``TimeWindowEvent`` the
        window-index hash is returned. For any other value (including
        ``None`` for backward-compatibility) the timestamp-based hash column is
        returned.
    container_id_col : str, optional
        Name of the container ID column, defaults to "container_id".
    event_name_col : str, optional
        Name of the event name column, defaults to "event_name".
    start_ts_col : str, optional
        Name of the start timestamp column, defaults to "start_ts".
    end_ts_col : str, optional
        Name of the end timestamp column, defaults to "end_ts".
    window_index_col : str, optional
        Name of the window position column (``TimeWindowEvent`` only), defaults
        to "window_index".

    Returns
    -------
    pyspark.sql.Column
        A column expression for the event_instance_id.
    """
    from impulse_reporting.events.container_event import ContainerEvent
    from impulse_reporting.events.time_window_event import TimeWindowEvent

    if event_type is ContainerEvent:
        return f.xxhash64(f.col(container_id_col).cast("string"))

    if event_type is TimeWindowEvent:
        return f.xxhash64(
            f.concat_ws(
                "::",
                f.col(container_id_col),
                f.col(event_name_col),
                f.col(window_index_col),
            )
        )

    return f.xxhash64(
        f.concat_ws(
            "::",
            f.col(container_id_col),
            f.col(event_name_col),
            f.col(start_ts_col),
            f.col(end_ts_col),
        )
    )
