from __future__ import annotations

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


def window_intervals_col(start_ts: Column, stop_ts: Column, window_length: float) -> Column:
    """Spark counterpart of :meth:`TimeWindowExpression.build`.

    Computes the same fixed-duration windows natively in Spark, so the reporting
    ``TimeWindowEvent`` can materialize windows for every container without a solve.
    The ``event_instance_id`` of a window hashes its ``start_ts`` / ``end_ts``, so the
    windows computed here must be **bit-identical** to the ones ``build`` computes for
    scoped aggregations.  Both therefore run the same IEEE-754 operations in the same
    order on the same doubles: cast the boundaries to double *before* subtracting,
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

    Returns
    -------
    pyspark.sql.Column
        ``array<array<double>>`` with one ``[start, end]`` pair per window; empty when the
        boundaries are null or the span is not strictly positive.
    """
    start, stop = start_ts.cast("double"), stop_ts.cast("double")
    w = F.lit(float(window_length))
    count = F.ceil((stop - start) / w)
    windows = F.transform(
        F.sequence(F.lit(0), count - F.lit(1)),
        lambda i: F.array(start + i * w, F.least(start + (i + F.lit(1)) * w, stop)),
    )
    windows = F.filter(windows, lambda p: p[0] < p[1])
    # Gate on a positive span: sequence(0, -1) yields [0, -1] (a descending sequence),
    # not an empty array, so degenerate containers would otherwise emit bogus windows.
    return F.when(stop > start, windows).otherwise(F.array().cast("array<array<double>>"))


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
    :func:`window_intervals_col`; the two must stay bit-identical.
    """

    def __init__(self, window_length: float):
        """
        Initialize a TimeWindowExpression.

        Parameters
        ----------
        window_length : float
            Fixed window length, in the same time unit as the underlying timestamps
            (e.g. milliseconds-since-epoch). Must be strictly positive.

        Raises
        ------
        ValueError
            If ``window_length`` is not strictly positive.
        """
        if window_length is None or window_length <= 0:
            raise ValueError(
                f"TimeWindowExpression requires a strictly positive window_length, "
                f"got {window_length!r}."
            )
        # Store as float so the string form (and thus the event definition hash) is stable
        # regardless of whether an int or float was passed: 10 and 10.0 are the same window
        # and must not trigger a spurious full recompute in incremental mode.
        self.window_length = float(window_length)
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        """
        Return a string representation of the TimeWindowExpression.

        The ``window_length`` is included so it flows into the event's definition hash.

        Returns
        -------
        str
            String representation of the object.
        """
        return f"TimeWindowExpression<window_length={self.window_length}>"

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
            (e.g. the empty cache used for type validation) or non-positive in span.
        """
        start_ts = cache.container_metrics.get(_SOLVER_CONFIG.start_ts_col)
        stop_ts = cache.container_metrics.get(_SOLVER_CONFIG.stop_ts_col)

        if start_ts is None or stop_ts is None:
            return Intervals.empty()

        # Mirror window_intervals_col exactly: convert to double *before* subtracting.  A
        # long column reaches pandas as int64 or float64 depending on the group (nulls
        # force float64), and an exact int64 span can round differently from the double
        # span for large values (e.g. ns epochs), changing the window count.
        start_ts, stop_ts = float(start_ts), float(stop_ts)
        if not stop_ts > start_ts:
            return Intervals.empty()

        # Number of windows covering the span; the last one is clamped to stop_ts below.
        # The span is strictly positive (guarded above) and window_length is strictly
        # positive (enforced in __init__), so window_count >= 1.
        window_count = int(np.ceil((stop_ts - start_ts) / self.window_length))

        indices = np.arange(window_count)
        starts = start_ts + indices * self.window_length
        ends = np.minimum(start_ts + (indices + 1) * self.window_length, stop_ts)
        return Intervals(starts, ends, del_last_empty=True)
