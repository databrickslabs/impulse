"""Event-window filtering for Phase 4 / scenario-layer tables.

The default ingestion mode for `object_tracks` and Phase 4 tables is *TSAL-gated*:
write rows only for frames whose timestamp falls inside an `event_instance_fact`
window, plus a configurable pre/post buffer. This keeps fleet-scale row counts
tractable (see `ObjectTracksConfig` and the perception data model notes on
TSAL-gated ingestion).

This module exposes two surfaces, one for each side of the pipeline:

  ``in_any_window``                   — pure Python predicate for per-row filtering
                                        in adapter code that iterates dataset
                                        annotations one at a time.
  ``filter_dataframe_to_windows``     — Spark DataFrame helper for notebook code
                                        that has the candidate rows and the event
                                        windows both as DataFrames.

Both apply the same semantics: a row at `frame_ts` matches if there exists at
least one window for the same `container_id` whose ``[start_ts - pre_buffer_us,
end_ts + post_buffer_us]`` includes `frame_ts`.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DEFAULT_BUFFER_US = 500_000  # 500 ms pre/post buffer per ObjectTracksConfig default


def in_any_window(
    frame_ts: int,
    windows: Iterable[tuple[int, int]],
    pre_buffer_us: int = DEFAULT_BUFFER_US,
    post_buffer_us: int = DEFAULT_BUFFER_US,
) -> bool:
    """Return True if ``frame_ts`` falls inside any (start, end) window after
    applying the pre/post buffers.

    Used by adapter row builders that iterate annotations in Python — e.g. a
    LiDAR mapper that has the scene's annotations in a list and only wants to
    emit rows for those inside an event window.
    """
    for start_ts, end_ts in windows:
        if (start_ts - pre_buffer_us) <= frame_ts <= (end_ts + post_buffer_us):
            return True
    return False


def filter_dataframe_to_windows(
    rows: DataFrame,
    events: DataFrame,
    *,
    pre_buffer_us: int = DEFAULT_BUFFER_US,
    post_buffer_us: int = DEFAULT_BUFFER_US,
    ts_col: str = "frame_ts",
    id_col: str = "container_id",
    event_start_col: str = "start_ts",
    event_end_col: str = "end_ts",
) -> DataFrame:
    """Filter ``rows`` to those falling inside any event window in ``events``.

    Both DataFrames must carry the recording identifier (``id_col``, default
    ``container_id``). ``rows`` must have a single timestamp column (``ts_col``,
    default ``frame_ts``). ``events`` must have start/end timestamp columns
    (``event_start_col`` / ``event_end_col``, default ``start_ts`` / ``end_ts``).

    Implemented as an inner join on container with a range predicate, then a
    distinct on the row columns so a row that matches multiple overlapping
    windows is not duplicated. Used by notebook code that has rows and events
    both as Spark DataFrames.
    """
    row_cols = rows.columns
    windowed_events = events.select(
        F.col(id_col),
        (F.col(event_start_col) - F.lit(pre_buffer_us)).alias("__lakevision_window_start"),
        (F.col(event_end_col) + F.lit(post_buffer_us)).alias("__lakevision_window_end"),
    )
    joined = rows.join(windowed_events, on=id_col, how="inner").where(
        (F.col(ts_col) >= F.col("__lakevision_window_start"))
        & (F.col(ts_col) <= F.col("__lakevision_window_end"))
    )
    return joined.select(*row_cols).dropDuplicates()
