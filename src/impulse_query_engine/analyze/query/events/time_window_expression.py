from __future__ import annotations

import datetime
import math
import numbers

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.model.series.intervals import Intervals

# Reuse SolverConfig's canonical internal (post-``column_name_mapping``) column names for
# the measurement start/stop timestamps rather than re-declaring the literals here. These
# are the keys under which the solve exposes them via ``SeriesCache.container_metrics``, and
# they are the same names ``ContainerEvent`` relies on. A default instance suffices since
# the names are config-invariant.
_SOLVER_CONFIG = SolverConfig()

# Default upper bound on the windows per container. A window_length in the wrong unit for the
# boundaries (e.g. 60 meant as seconds over ns epochs) would otherwise yield billions of
# windows: Spark's sequence fails with an opaque COLLECTION_SIZE_LIMIT_EXCEEDED and numpy
# allocates arrays of that size.
MAX_WINDOWS_PER_CONTAINER = 1_000_000

_WINDOW_LIMIT_HINT = (
    "Check that window_length is in the epoch unit of the container boundaries "
    "(solver_config.epoch_unit), or raise the limit (TimeWindowEvent "
    "max_windows_per_container, TimeWindowExpression max_windows)."
)


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


def _is_finite(col: Column) -> Column:
    """True for finite doubles; false for NaN / +-inf; null for null."""
    return ~F.isnan(col) & (F.abs(col) != F.lit(float("inf")))


def window_intervals_col(
    start_ts: Column,
    stop_ts: Column,
    window_length: float,
    max_windows: int = MAX_WINDOWS_PER_CONTAINER,
) -> Column:
    """Spark counterpart of :meth:`TimeWindowExpression.build`.

    Computes the same fixed-duration windows natively in Spark, so the reporting
    ``TimeWindowEvent`` can materialize windows for every container without a solve.
    The ``event_instance_id`` of a window hashes its position in the returned array, so
    the windows computed here must match the ones ``build`` computes for scoped
    aggregations in **count and order**.  Both therefore run the same IEEE-754 operations
    in the same order on the same doubles (which also keeps the stored boundaries
    identical): cast the boundaries to double *before* subtracting,
    ``count = ceil((stop - start) / W)``, ``start_i = start + i * W``,
    ``end_i = min(start + (i + 1) * W, stop)``, and drop windows with
    ``start_i >= end_i``.  Keep the two implementations in sync.

    Parameters
    ----------
    start_ts : pyspark.sql.Column
        Container start timestamp.
    stop_ts : pyspark.sql.Column
        Container stop timestamp.
    window_length : float
        Fixed window length, in the same time unit as the timestamps. Must be strictly
        positive.
    max_windows : int, optional
        Maximum number of windows per container (default
        :data:`MAX_WINDOWS_PER_CONTAINER`). A container exceeding it fails the query with
        an error naming the limit.

    Returns
    -------
    pyspark.sql.Column
        ``array<array<double>>`` with one ``[start, end]`` pair per window; empty when a
        boundary is null, NaN or infinite, or the span is not strictly positive.
    """
    max_windows = validate_max_windows(max_windows)
    start, stop = start_ts.cast("double"), stop_ts.cast("double")
    w = F.lit(float(window_length))
    count = F.ceil((stop - start) / w)
    windows = F.transform(
        F.sequence(F.lit(0), count - F.lit(1)),
        lambda i: F.array(start + i * w, F.least(start + (i + F.lit(1)) * w, stop)),
    )
    windows = F.filter(windows, lambda p: p[0] < p[1])
    too_many = F.raise_error(
        F.concat(
            F.lit("TimeWindowExpression: "),
            count.cast("string"),
            F.lit(f" windows of length {float(window_length)} over a container span of "),
            (stop - start).cast("string"),
            F.lit(f" exceed max_windows={max_windows}. {_WINDOW_LIMIT_HINT}"),
        )
    )
    # Gate on finite boundaries and a positive span before anything reaches sequence:
    # sequence(0, -1) yields [0, -1] (a descending sequence), not an empty array, and Spark
    # orders NaN above every number, so a NaN stop_ts would pass ``stop > start`` alone.
    valid = _is_finite(start) & _is_finite(stop) & (stop > start)
    return (
        F.when(valid & (count > F.lit(max_windows)), too_many)
        .when(valid, windows)
        .otherwise(F.array().cast("array<array<double>>"))
    )


class TimeWindowExpression(TimeSeriesExpression):
    """Produce consecutive fixed-duration windows spanning a measurement container.

    The windows are derived purely from the container's ``start_ts`` / ``stop_ts`` metadata
    (no channel data), so the expression declares no selectors and instead requests those
    container metrics via :meth:`required_container_metrics`.  Windows tile
    ``[start_ts, stop_ts]`` with a fixed length ``window_length`` (expressed in the same time
    unit as the underlying timestamps); the final window is clamped to ``stop_ts`` when the
    last full window would overrun it.

    Visual timeline (window_length = W)::

        time --->
        container: | ------------------------------- |
        windows:   | --W-- | --W-- | --W-- | -rest- |

    This is the query-engine counterpart of the reporting ``TimeWindowEvent``.  It evaluates
    to :class:`Intervals`, so it can scope a ``StatsAggregator`` (one statistic per window).
    The reporting event fact computes the same windows natively via
    :func:`window_intervals_col`; the two must produce the same windows in the same order.

    Attributes
    ----------
    epoch_unit : str or None
        Epoch unit the solver converts ``TIMESTAMP`` boundaries to
        (``solver_config.epoch_unit``), set by the reporting ``TimeWindowEvent``.
        Descriptive only: :meth:`build` does not convert (the solver does).  It is part of
        the string form, so the definition hashes of the event and of every aggregation
        scoped to it change with the unit.
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
        # inf / NaN must be rejected too: inf gives a zero window count, for which Spark's
        # sequence(0, -1) emits a bogus window, and NaN crashes the solve in build().
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
        self.epoch_unit: str | None = None
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        """
        Return a string representation of the TimeWindowExpression.

        The ``window_length`` (and ``epoch_unit``, when set) is included so it flows into
        the definition hashes of the event and of the aggregations scoped to it.  An unset
        ``epoch_unit`` is omitted, keeping the string identical to the unit-less form.

        Returns
        -------
        str
            String representation of the object.
        """
        unit = f", epoch_unit={self.epoch_unit}" if self.epoch_unit is not None else ""
        return f"TimeWindowExpression<window_length={self.window_length}{unit}>"

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
            The measurement start/stop timestamp columns.
        """
        return {_SOLVER_CONFIG.start_ts_col, _SOLVER_CONFIG.stop_ts_col}

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
            Consecutive fixed-length windows over ``[start_ts, stop_ts]``, with the final
            window clamped to ``stop_ts``. Empty when the container boundaries are absent
            (e.g. the empty cache used for type validation), NaN or infinite, or
            non-positive in span.

        Raises
        ------
        TypeError
            If a boundary is a date/time value rather than an epoch number.
        ValueError
            If the container would produce more than ``max_windows`` windows.
        """
        start_ts = cache.container_metrics.get(_SOLVER_CONFIG.start_ts_col)
        stop_ts = cache.container_metrics.get(_SOLVER_CONFIG.stop_ts_col)

        if start_ts is None or stop_ts is None:
            return Intervals.empty()

        for name, value in (("start_ts", start_ts), ("stop_ts", stop_ts)):
            # pd.Timestamp subclasses datetime.datetime; dates and numpy datetimes too.
            if isinstance(value, (datetime.date, np.datetime64)):
                raise TypeError(
                    f"TimeWindowExpression needs epoch-number container boundaries, but "
                    f"{name} is {type(value).__name__}. For TIMESTAMP columns, set "
                    "solver_config.epoch_unit to the epoch unit of the channel sample "
                    "timestamps so start_ts / stop_ts are converted before the solve."
                )

        # Mirror window_intervals_col exactly: convert to double *before* subtracting.  A
        # long column reaches pandas as int64 or float64 depending on the group (nulls
        # force float64), and an exact int64 span can round differently from the double
        # span for large values (e.g. ns epochs), changing the window count.
        start_ts, stop_ts = float(start_ts), float(stop_ts)
        # Same gate as window_intervals_col: NaN / infinite boundaries (e.g. an unfinished
        # recording) yield no windows, like nulls.
        if not (math.isfinite(start_ts) and math.isfinite(stop_ts) and stop_ts > start_ts):
            return Intervals.empty()

        # Number of windows covering the span; the last one is clamped to stop_ts below.
        # The span is strictly positive (guarded above) and window_length is strictly
        # positive (enforced in __init__), so window_count >= 1.  Compared before the int
        # conversion, since an overflowing span gives an infinite count.
        window_count = np.ceil((stop_ts - start_ts) / self.window_length)
        if window_count > self.max_windows:
            raise ValueError(
                f"TimeWindowExpression: {window_count:.0f} windows of length {self.window_length} "
                f"over a container span of {stop_ts - start_ts} exceed "
                f"max_windows={self.max_windows}. {_WINDOW_LIMIT_HINT}"
            )
        window_count = int(window_count)

        indices = np.arange(window_count)
        starts = start_ts + indices * self.window_length
        ends = np.minimum(start_ts + (indices + 1) * self.window_length, stop_ts)
        return Intervals(starts, ends, del_last_empty=True)
