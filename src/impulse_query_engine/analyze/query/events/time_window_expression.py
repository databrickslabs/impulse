from __future__ import annotations

import math
import numbers

import numpy as np
import pandas as pd
import pyarrow as pa
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.model.series.intervals import Intervals

# Reuse SolverConfig's internal column names for the container bounds in the channel time
# frame (see solvers.utils.window_bounds.with_window_bounds) rather than re-declaring the
# literals here. These are the keys under which the solve exposes them via
# ``SeriesCache.container_metrics``.
# A default instance suffices since the names are config-invariant.
_SOLVER_CONFIG = SolverConfig()

# Default upper bound on the windows per container. A window_length in the wrong unit for the
# boundaries (e.g. 60 meant as seconds over ns epochs) would otherwise yield billions of
# windows: numpy would allocate arrays of that size and the event fact explode as many rows.
MAX_WINDOWS_PER_CONTAINER = 1_000_000

_WINDOW_LIMIT_HINT = (
    "Check that window_length is in the unit of the channel timestamps "
    "(solver_config.channel_time_unit) and, for numeric container boundaries in another "
    "unit, that solver_config.container_time_unit is set, or raise the limit "
    "(TimeWindowEvent max_windows_per_container, TimeWindowExpression max_windows)."
)

# Windows after which explode_windows emits an output batch (between containers only).
_BATCH_WINDOWS = 100_000


def validate_max_windows(max_windows: int, param_name: str = "max_windows") -> int:
    """Return *max_windows* as an int, raising unless it is a positive integer.

    Parameters
    ----------
    max_windows : int
        Maximum number of windows per container.
    param_name : str, optional
        Name of the caller's parameter, used in the error message (default
        ``"max_windows"``).

    Returns
    -------
    int
        The validated limit.

    Raises
    ------
    ValueError
        If *max_windows* is not a positive integer.
    """
    if (
        isinstance(max_windows, bool)
        or not isinstance(max_windows, numbers.Integral)
        or max_windows <= 0
    ):
        raise ValueError(f"{param_name} must be a positive integer, got {max_windows!r}.")
    return int(max_windows)


def tile_windows(
    start, stop, window_length: float, max_windows: int = MAX_WINDOWS_PER_CONTAINER
) -> tuple[np.ndarray, np.ndarray]:
    """Tile ``[start, stop]`` into consecutive windows of length *window_length*.

    The one window implementation behind ``TimeWindowEvent``: the solve calls it through
    :meth:`TimeWindowExpression.build` (scoped aggregations), the event fact through
    :func:`explode_windows`.  ``event_instance_id`` hashes each window's boundaries, so
    both sides must produce identical windows, which a single function guarantees as long as
    both pass in the same values.  Both read the same Spark-computed bounds
    (``solvers.utils.window_bounds.with_window_bounds``), but receive them as Python or numpy
    integers or floats (pandas turns long columns with nulls into ``float64``), or as
    ``None`` / ``NaN``.  The bounds are therefore converted to ``float`` first: integer ->
    double rounds to the nearest double on either path, so the arithmetic below runs on
    identical doubles.

    Window ``i`` spans ``[start + i * W, min(start + (i + 1) * W, stop)]``, so the last one
    is clamped to *stop*; windows with ``start_i >= end_i`` (possible only through rounding)
    are dropped.

    Parameters
    ----------
    start, stop : float, int, None
        Container bounds in the channel time frame.
    window_length : float
        Fixed window length, in the same unit as the bounds. Strictly positive.
    max_windows : int, optional
        Maximum number of windows (default :data:`MAX_WINDOWS_PER_CONTAINER`).

    Returns
    -------
    tuple of numpy.ndarray
        ``(starts, ends)`` as float64 arrays; empty when a bound is null, NaN or infinite,
        or the span is not strictly positive.

    Raises
    ------
    ValueError
        If the span would produce more than *max_windows* windows.
    """
    empty = (np.empty(0), np.empty(0))
    if pd.isna(start) or pd.isna(stop):
        return empty
    start, stop = float(start), float(stop)
    # NaN / infinite bounds (e.g. an unfinished recording) yield no windows, like nulls.
    if not (math.isfinite(start) and math.isfinite(stop) and stop > start):
        return empty

    # Compared before the int conversion, since an overflowing span gives an infinite count.
    window_count = np.ceil((stop - start) / window_length)
    if window_count > max_windows:
        raise ValueError(
            f"TimeWindowExpression: {window_count:.0f} windows of length {window_length} "
            f"over a container span of {stop - start} exceed "
            f"max_windows={max_windows}. {_WINDOW_LIMIT_HINT}"
        )
    indices = np.arange(int(window_count))
    starts = start + indices * window_length
    ends = np.minimum(start + (indices + 1) * window_length, stop)
    keep = starts < ends
    return starts[keep], ends[keep]


def explode_windows(
    df: DataFrame,
    *,
    id_col: str,
    start_col: str,
    stop_col: str,
    windows: list[tuple[str, float, int]],
    output_cols: tuple[str, str, str],
) -> DataFrame:
    """One row per window of each container, for several window lengths in one pass.

    Used by the reporting ``TimeWindowEvent`` for its event fact, so the event fact and the
    solve share one window implementation (:func:`tile_windows`).  The rows are built from
    numpy arrays in a ``mapInArrow``, in output batches of about 100,000 windows that never
    split a container's windows of one event, so the Python worker's memory stays bounded by
    the batch size and ``max_windows``, independent of the number of containers and events.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        One row per container, with *id_col* and the bounds in the channel time frame.
    id_col : str
        Container id column; kept with its name and type (e.g. long or string) in the output.
    start_col, stop_col : str
        Container bound columns (numeric).
    windows : list of tuple
        ``(name, window_length, max_windows)`` per event: the event name of its rows, the
        window length (in the unit of the bounds, strictly positive) and the maximum number
        of windows per container. A container exceeding it fails the query with an error
        naming the limit.
    output_cols : tuple of str
        Names of the output columns for the event name, the window start and the window end.

    Returns
    -------
    pyspark.sql.DataFrame
        Columns *id_col* and *output_cols* (event name as string, window start and end as
        double); no rows for a container whose bound is null, NaN or infinite, or whose span
        is not strictly positive.

    Raises
    ------
    ValueError
        If a ``max_windows`` is not a positive integer, or *id_col* and *output_cols* are not
        four distinct names.
    """
    if len({id_col, *output_cols}) != 4:
        raise ValueError(
            f"explode_windows needs four distinct output column names, got id_col={id_col!r} "
            f"and output_cols={output_cols!r}."
        )
    windows = [(name, float(length), validate_max_windows(m)) for name, length, m in windows]
    name_col, window_start_col, window_end_col = output_cols
    schema = T.StructType(
        [
            df.schema[id_col],
            T.StructField(name_col, T.StringType()),
            T.StructField(window_start_col, T.DoubleType()),
            T.StructField(window_end_col, T.DoubleType()),
        ]
    )
    column_names = schema.names

    def tile(batches):
        yield from _window_batches(
            batches, id_col, start_col, stop_col, windows, column_names, _BATCH_WINDOWS
        )

    return df.select(id_col, start_col, stop_col).mapInArrow(tile, schema)


def _window_batches(batches, id_col, start_col, stop_col, windows, column_names, batch_windows):
    """Yield :func:`explode_windows` output batches named *column_names* for Arrow *batches*.

    A batch is emitted once it holds at least *batch_windows* windows, after a container's
    complete windows of one event, and at the end of each input batch.  A batch therefore
    holds at most ``batch_windows - 1`` plus one event's ``max_windows`` windows.
    """
    event_names = pa.array([name for name, _, _ in windows], pa.string())
    for batch in batches:
        ids = batch.column(id_col)
        bounds = zip(
            batch.column(start_col).to_pylist(), batch.column(stop_col).to_pylist(), strict=True
        )
        parts, pending = [], 0
        for row, (start, stop) in enumerate(bounds):
            for event, (_, length, max_windows) in enumerate(windows):
                starts, ends = tile_windows(start, stop, length, max_windows)
                parts.append((row, event, starts, ends))
                pending += len(starts)
                if pending >= batch_windows:
                    yield _record_batch(column_names, ids, event_names, parts)
                    parts, pending = [], 0
        if pending:
            yield _record_batch(column_names, ids, event_names, parts)


def _record_batch(column_names, ids, event_names, parts) -> pa.RecordBatch:
    """Output batch for the ``(row, event, starts, ends)`` *parts* of one input batch."""
    counts = [len(starts) for _, _, starts, _ in parts]
    rows = np.repeat([row for row, _, _, _ in parts], counts)
    events = np.repeat([event for _, event, _, _ in parts], counts)
    return pa.RecordBatch.from_arrays(
        [
            ids.take(rows),
            event_names.take(events),
            pa.array(np.concatenate([starts for _, _, starts, _ in parts])),
            pa.array(np.concatenate([ends for _, _, _, ends in parts])),
        ],
        names=column_names,
    )


class TimeWindowExpression(TimeSeriesExpression):
    """Produce consecutive fixed-duration windows spanning a measurement container.

    The windows are derived purely from the container's ``start_ts`` / ``stop_ts`` metadata
    (no channel data), so the expression declares no selectors and instead requests the
    container bounds in the channel time frame via :meth:`required_container_metrics`
    (computed by ``solvers.utils.window_bounds.with_window_bounds``).  Windows tile those
    bounds with a fixed length ``window_length`` (expressed in the same time unit as the
    channel timestamps); the final window is clamped to the stop bound when the last full
    window would overrun it.

    Visual timeline (window_length = W)::

        time --->
        container: | ------------------------------- |
        windows:   | --W-- | --W-- | --W-- | -rest- |

    This is the query-engine counterpart of the reporting ``TimeWindowEvent``.  It evaluates
    to :class:`Intervals`, so it can scope a ``StatsAggregator`` (one statistic per window).
    The windows come from :func:`tile_windows`, which the reporting event fact also uses (via
    :func:`explode_windows`), so both produce the same windows in the same order.

    Attributes
    ----------
    channel_time_unit : str or None
        ``solver_config.channel_time_unit``, set by the reporting ``TimeWindowEvent``.
    channel_time_origin : str
        ``solver_config.channel_time_origin`` (default ``"epoch"``), set the same way.
    container_time_unit : str or None
        ``solver_config.container_time_unit``, set the same way.

    Both are descriptive only: :meth:`build` does not convert (the solver computes the
    bounds).  They are part of the string form, so the definition hashes of the event and of
    every aggregation scoped to it change with the channel time frame.
    """

    def __init__(self, window_length: float, max_windows: int = MAX_WINDOWS_PER_CONTAINER):
        """
        Initialize a TimeWindowExpression.

        Parameters
        ----------
        window_length : float
            Fixed window length, in the same time unit as the underlying timestamps
            (e.g. milliseconds-since-epoch). Must be strictly positive and finite.
        max_windows : int, optional
            Maximum number of windows per container (default
            :data:`MAX_WINDOWS_PER_CONTAINER`); :meth:`build` raises beyond it. Not part
            of the string form, since it only decides between an error and a result.

        Raises
        ------
        ValueError
            If ``window_length`` is not strictly positive and finite, or ``max_windows``
            is not a positive integer.
        """
        # inf / NaN must be rejected too: inf gives a zero window count and NaN an undefined
        # one in tile_windows.
        if window_length is None or not math.isfinite(window_length) or window_length <= 0:
            raise ValueError(
                f"TimeWindowExpression requires a strictly positive, finite window_length, "
                f"got {window_length!r}."
            )
        # Store as float so the string form (and thus the event definition hash) is stable
        # regardless of whether an int or float was passed: 10 and 10.0 are the same window
        # and must not trigger a spurious full recompute in incremental mode.
        self.window_length = float(window_length)
        self.max_windows = validate_max_windows(max_windows)
        self.channel_time_unit: str | None = None
        self.channel_time_origin: str = "epoch"
        self.container_time_unit: str | None = None
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        """
        Return a string representation of the TimeWindowExpression.

        The ``window_length`` and the channel time frame are included so they flow into the
        definition hashes of the event and of the aggregations scoped to it.  Unset units
        and the default ``"epoch"`` origin are omitted, keeping the default string
        unchanged.

        Returns
        -------
        str
            String representation of the object.
        """
        frame = ""
        if self.channel_time_unit is not None:
            frame += f", channel_time_unit={self.channel_time_unit}"
        if self.channel_time_origin != "epoch":
            frame += f", channel_time_origin={self.channel_time_origin}"
        if self.container_time_unit is not None:
            frame += f", container_time_unit={self.container_time_unit}"
        return f"TimeWindowExpression<window_length={self.window_length}{frame}>"

    def dtype(self):
        """
        Return the Spark data type of the result.

        Returns
        -------
        pyspark.sql.types.ArrayType
            Same dtype as Intervals: ArrayType(ArrayType(DoubleType())).
        """
        return Intervals.empty().dtype()

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return required tag expressions (none: windows use container metrics only).

        Returns
        -------
        set of TagExpression
        """
        return set()

    def required_tags(self) -> set[str]:
        """
        Return required tags (none).

        Returns
        -------
        set of str
        """
        return set()

    def required_container_tags(self) -> set[str]:
        """
        Return required container tags (none).

        Returns
        -------
        set of str
        """
        return set()

    def required_container_metrics(self) -> set[str]:
        """
        Return the container-metric columns needed to bound the windows.

        Returns
        -------
        set of str
            The container start/stop in the channel time frame, which the solver derives
            from ``start_ts`` / ``stop_ts`` (``solvers.utils.window_bounds.with_window_bounds``).
        """
        return {_SOLVER_CONFIG.window_start_col, _SOLVER_CONFIG.window_stop_col}

    def get_selectors(self) -> list[TimeSeriesSelector]:
        """
        Return channel selectors (none: windows depend on no channel data).

        Returns
        -------
        list of TimeSeriesSelector
        """
        return []

    def get_selector_expr(self):
        """
        Return the combined selector expression (none).

        Returns
        -------
        None
        """
        return None

    def build(self, cache: SeriesCache) -> Intervals:
        """
        Build the fixed-duration windows spanning the container.

        Parameters
        ----------
        cache : SeriesCache
            Cache exposing the requested container metrics via ``container_metrics``.

        Returns
        -------
        Intervals
            Consecutive fixed-length windows over the container bounds, with the final
            window clamped to the stop bound. Empty when the bounds are absent (e.g. the
            empty cache used for type validation), NaN or infinite, or non-positive in span.

        Raises
        ------
        ValueError
            If the container would produce more than ``max_windows`` windows.
        """
        starts, ends = tile_windows(
            cache.container_metrics.get(_SOLVER_CONFIG.window_start_col),
            cache.container_metrics.get(_SOLVER_CONFIG.window_stop_col),
            self.window_length,
            self.max_windows,
        )
        if len(starts) == 0:
            return Intervals.empty()
        return Intervals(starts, ends, del_last_empty=True)
