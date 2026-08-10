"""PointsInTimeSeries class implementation"""

from __future__ import annotations

from collections.abc import Sized

import numpy as np
import numpy.typing as npt
import pyspark.sql.types as T

from .intervals import Intervals
from .points_in_time import PointsInTime
from .sample_series import SampleSeries

FloatOrNaN = float | np.float64


class PointsInTimeSeries:
    def __init__(self, tstarts: Sized, values: Sized):
        """
        Initialize the PointsInTimeSeries object.

        A PointsInTimeSeries associates a value to each timestamp. Unlike a SampleSeries,
        a value is only defined *at* its timestamp and is not considered valid in between
        consecutive timestamps.

        Parameters
        ----------
        tstarts : Sized
            Array-like of time points.
        values : Sized
            Array-like of values, one per time point.
        """
        assert len(tstarts) == len(values)
        self.tstarts = np.array(tstarts, dtype=np.float64)
        self.values = np.array(values, dtype=np.float64)
        # todo evaluate: we could use this and add a new self.values_str for example so we don't need PointsInTimeSeriesString
        # add self._series_type this distinguises between both

    def dtype(self):
        """
        Returns the Spark data type for PointsInTimeSeries.

        Returns
        -------
        pyspark.sql.types.ArrayType
            Spark ArrayType for points in time series: [[tstart_1, value_1], ...].
        """
        #todo needs if else
        return T.ArrayType(T.ArrayType(T.DoubleType()))

    def get_data(self) -> list:
        """
        Returns the series as a list of [tstart, value] lists.

        Returns
        -------
        list
            List of [tstart, value] pairs.
        """
        if len(self) == 0:
            return []
        return np.column_stack([self.tstarts, self.values]).tolist()

    def __len__(self) -> int:
        """
        Returns the number of points in the series.

        Returns
        -------
        int
            Number of points.
        """
        return len(self.tstarts)

    def start_time(self) -> FloatOrNaN:
        """
        Returns the time of the first point.

        Returns
        -------
        float
            Start time or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return self.tstarts[0]

    def end_time(self) -> FloatOrNaN:
        """
        Returns the time of the last point.

        Returns
        -------
        float
            End time or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return self.tstarts[-1]

    def to_points_in_time(self) -> PointsInTime:
        """
        Returns the timestamps of this series as a PointsInTime object (dropping the values).

        Returns
        -------
        PointsInTime
            Points in time with this series' timestamps.
        """
        return PointsInTime(self.tstarts)

    @staticmethod
    def __sweep_points_in_intervals(
        point_ts: npt.NDArray, interval_ts: npt.NDArray, interval_te: npt.NDArray
    ) -> list[tuple[int, int]]:
        """
        Plane sweep matching points against half-open intervals ``[interval_ts, interval_te)``.

        Both inputs are assumed sorted in ascending order. A trailing zero-duration interval
        ``[t, t)`` is treated as the closed point ``{t}``, so a query point exactly at ``t`` matches
        it. This mirrors ``SampleSeries.__pit_overlaps_interval`` and keeps the final value of a
        SampleSeries reachable by point sampling (its last sample is the closed endpoint ``[t, t)``).

        Parameters
        ----------
        point_ts : numpy.ndarray
            Sorted point timestamps.
        interval_ts : numpy.ndarray
            Sorted interval start times.
        interval_te : numpy.ndarray
            Interval end times.

        Returns
        -------
        list of tuple
            ``(point_idx, interval_idx)`` pairs for each point contained in an interval.
        """
        if len(point_ts) == 0 or len(interval_ts) == 0:
            return []
        pairs = []
        idx1 = 0
        idx2 = 0
        last = len(interval_ts) - 1
        last_closed = interval_ts[last] == interval_te[last]
        while idx1 < len(point_ts) and idx2 < len(interval_ts):
            if point_ts[idx1] < interval_ts[idx2]:  # point is before the current interval start
                idx1 += 1
            else:  # point is at or after the current interval start
                idx1i = idx1
                while idx1i < len(point_ts) and (
                    interval_te[idx2] > point_ts[idx1i]
                    # a trailing zero-duration interval is closed at its endpoint
                    or (idx2 == last and last_closed and point_ts[idx1i] == interval_te[idx2])
                ):
                    pairs.append((idx1i, idx2))
                    idx1i += 1
                idx2 += 1
        return pairs

    @staticmethod
    def __sweep_matching_points(ts_a: npt.NDArray, ts_b: npt.NDArray) -> list[tuple[int, int]]:
        """
        Match points by exact timestamp equality.

        Parameters
        ----------
        ts_a : numpy.ndarray
            First set of timestamps.
        ts_b : numpy.ndarray
            Second set of timestamps.

        Returns
        -------
        list of tuple
            ``(a_idx, b_idx)`` pairs for timestamps present in both sets.
        """
        if len(ts_a) == 0 or len(ts_b) == 0:
            return []
        _, a_idx, b_idx = np.intersect1d(ts_a, ts_b, return_indices=True)
        return [(int(a), int(b)) for a, b in zip(a_idx, b_idx, strict=True)]

    @staticmethod
    def plane_sweep(
        points, other: SampleSeries | Intervals | PointsInTime | PointsInTimeSeries
    ) -> list[tuple[int, int]]:
        """
        Find overlaps between a point-like series and another series/interval object.

        Parameters
        ----------
        points : PointsInTimeSeries or PointsInTime
            Point-like object exposing a ``tstarts`` attribute.
        other : SampleSeries, Intervals, PointsInTime, or PointsInTimeSeries
            Object to find overlaps with. Interval-like objects (SampleSeries, Intervals) match
            points contained in their half-open intervals; point-like objects (PointsInTime,
            PointsInTimeSeries) match points with equal timestamps.

        Returns
        -------
        list of tuple
            ``(points_idx, other_idx)`` index pairs indicating overlaps.

        Raises
        ------
        NotImplementedError
            If ``other`` is not one of the supported types.
        """
        if isinstance(other, (SampleSeries, Intervals)):
            return PointsInTimeSeries.__sweep_points_in_intervals(
                points.tstarts, other.tstarts, other.tends
            )
        if isinstance(other, (PointsInTime, PointsInTimeSeries)):
            return PointsInTimeSeries.__sweep_matching_points(points.tstarts, other.tstarts)
        raise NotImplementedError(
            "other must be SampleSeries, Intervals, PointsInTime or PointsInTimeSeries"
        )

    @staticmethod
    def __gather(values: npt.NDArray, pairs: list[tuple[int, int]], pos: int) -> list:
        """
        Collect ``values`` at the index found at position ``pos`` of each pair.

        Parameters
        ----------
        values : numpy.ndarray
            Array to index into.
        pairs : list of tuple
            Index pairs produced by ``plane_sweep``.
        pos : int
            Tuple position (0 or 1) holding the index to use.

        Returns
        -------
        list
            Collected values.
        """
        return [values[pair[pos]] for pair in pairs]

    def synchronized(
        self, other: SampleSeries | PointsInTimeSeries
    ) -> tuple[PointsInTimeSeries, PointsInTimeSeries]:
        """
        Synchronize this point series with a value-carrying series.

        The result grid is the subset of this series' timestamps that overlap ``other``: points
        contained in one of ``other``'s intervals (when ``other`` is a SampleSeries) or points with
        a matching timestamp (when ``other`` is a PointsInTimeSeries). Out-of-overlap points are
        dropped. Both returned series share the same timestamps.

        Parameters
        ----------
        other : SampleSeries or PointsInTimeSeries
            Value-carrying series to synchronize with.

        Returns
        -------
        tuple of PointsInTimeSeries
            Synchronized (this series, other series).

        Raises
        ------
        NotImplementedError
            If ``other`` does not carry values (Intervals, PointsInTime).
        """
        if not isinstance(other, (SampleSeries, PointsInTimeSeries)):
            raise NotImplementedError("synchronized requires a SampleSeries or PointsInTimeSeries")
        pairs = PointsInTimeSeries.plane_sweep(self, other)
        tstarts = PointsInTimeSeries.__gather(self.tstarts, pairs, 0)
        return (
            PointsInTimeSeries(tstarts, PointsInTimeSeries.__gather(self.values, pairs, 0)),
            PointsInTimeSeries(tstarts, PointsInTimeSeries.__gather(other.values, pairs, 1)),
        )

    def synchronized_all(
        self, others: list[SampleSeries | PointsInTimeSeries]
    ) -> tuple[PointsInTimeSeries, ...]:
        """
        Synchronize this point series with multiple value-carrying series.

        The common grid is narrowed against each operand in turn; all previously synchronized
        series are re-aligned to the narrowed grid.

        Parameters
        ----------
        others : list of SampleSeries or PointsInTimeSeries
            Series to synchronize with.

        Returns
        -------
        tuple of PointsInTimeSeries
            Synchronized series, one per input series (this series first).

        Raises
        ------
        NotImplementedError
            If any operand does not carry values (Intervals, PointsInTime).
        """
        synced_list = [self]
        for other in others:
            if not isinstance(other, (SampleSeries, PointsInTimeSeries)):
                raise NotImplementedError(
                    "synchronized_all requires SampleSeries or PointsInTimeSeries operands"
                )
            grid = synced_list[0]
            pairs = PointsInTimeSeries.plane_sweep(grid, other)
            tstarts = PointsInTimeSeries.__gather(grid.tstarts, pairs, 0)
            new_synced = [
                PointsInTimeSeries(tstarts, PointsInTimeSeries.__gather(s.values, pairs, 0))
                for s in synced_list
            ]
            new_synced.append(
                PointsInTimeSeries(tstarts, PointsInTimeSeries.__gather(other.values, pairs, 1))
            )
            synced_list = new_synced
        return tuple(synced_list)

    def _apply_basic_op(self, operation, other: float | SampleSeries | PointsInTimeSeries):
        """
        Apply a basic arithmetic operation to this series and another operand.

        Parameters
        ----------
        operation : callable
            Numpy operation to apply.
        other : float, SampleSeries, or PointsInTimeSeries
            Operand for the operation. Series operands are aligned via ``synchronized``.

        Returns
        -------
        PointsInTimeSeries
            Resulting series.
        """
        if isinstance(other, (SampleSeries, PointsInTimeSeries)):
            s0, s1 = self.synchronized(other)
            return PointsInTimeSeries(s0.tstarts, operation(s0.values, s1.values))
        return PointsInTimeSeries(self.tstarts, operation(self.values, other))

    def _apply_basic_rop(self, operation, other: float | SampleSeries | PointsInTimeSeries):
        """
        Apply a basic arithmetic operation with operands reversed.

        Parameters
        ----------
        operation : callable
            Numpy operation to apply.
        other : float, SampleSeries, or PointsInTimeSeries
            Operand for the operation. Series operands are aligned via ``synchronized``.

        Returns
        -------
        PointsInTimeSeries
            Resulting series.
        """
        if isinstance(other, (SampleSeries, PointsInTimeSeries)):
            s0, s1 = self.synchronized(other)
            return PointsInTimeSeries(s0.tstarts, operation(s1.values, s0.values))
        return PointsInTimeSeries(self.tstarts, operation(other, self.values))

    def __add__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Add another series or scalar to this series."""
        return self._apply_basic_op(np.add, other)

    def __radd__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Add this series to another series or scalar (reversed operands)."""
        return self._apply_basic_rop(np.add, other)

    def __sub__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Subtract another series or scalar from this series."""
        return self._apply_basic_op(np.subtract, other)

    def __rsub__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Subtract this series from another series or scalar (reversed operands)."""
        return self._apply_basic_rop(np.subtract, other)

    def __mul__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Multiply this series by another series or scalar."""
        return self._apply_basic_op(np.multiply, other)

    def __rmul__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Multiply another series or scalar by this series (reversed operands)."""
        return self._apply_basic_rop(np.multiply, other)

    def __truediv__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Divide this series by another series or scalar."""
        return self._apply_basic_op(np.true_divide, other)

    def __rtruediv__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTimeSeries:
        """Divide another series or scalar by this series (reversed operands)."""
        return self._apply_basic_rop(np.true_divide, other)

    def __apply_op(
        self, operation, other: float | SampleSeries | PointsInTimeSeries
    ) -> PointsInTime:
        """
        Apply a comparison operation and return the points where the condition holds.

        Parameters
        ----------
        operation : callable
            Numpy comparison operation.
        other : float, SampleSeries, or PointsInTimeSeries
            Operand for comparison. Series operands are aligned via ``synchronized``.

        Returns
        -------
        PointsInTime
            Points in time where the condition is True.
        """
        if isinstance(other, (SampleSeries, PointsInTimeSeries)):
            s0, s1 = self.synchronized(other)
            idx = operation(s0.values, s1.values)
            return PointsInTime(s0.tstarts[idx])
        idx = operation(self.values, other)
        return PointsInTime(self.tstarts[idx])

    def __gt__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is greater than another."""
        return self.__apply_op(np.greater, other)

    def __ge__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is greater than or equal to another."""

        # todo check here for double checking if its a string value
        return self.__apply_op(np.greater_equal, other)

    def __lt__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is less than another."""
        return self.__apply_op(np.less, other)

    def __le__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is less than or equal to another."""
        return self.__apply_op(np.less_equal, other)

    def __eq__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is equal to another."""
        return self.__apply_op(np.equal, other)

    def __ne__(self, other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime:
        """Return points where this series is not equal to another."""
        return self.__apply_op(np.not_equal, other)

    __hash__ = None

    def count(self) -> int:
        """
        Returns the number of points in the series.

        Returns
        -------
        int
            Number of points.
        """
        return len(self)

    def sum(self) -> FloatOrNaN:
        """
        Returns the sum of the values.

        Returns
        -------
        float
            Sum of values, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.sum(self.values)

    def mean(self) -> FloatOrNaN:
        """
        Returns the mean of the values.

        Returns
        -------
        float
            Mean of values, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.mean(self.values)

    def min(self) -> FloatOrNaN:
        """
        Returns the minimum value.

        Returns
        -------
        float
            Minimum value, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.min(self.values)

    def max(self) -> FloatOrNaN:
        """
        Returns the maximum value.

        Returns
        -------
        float
            Maximum value, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.max(self.values)

    def __str__(self) -> str:
        """
        Returns a string representation of the PointsInTimeSeries.

        Returns
        -------
        str
            String representation.
        """
        start = self.start_time()
        end = self.end_time()
        count = len(self)
        return f"<PointsInTimeSeries({start}..cnt:{count}..{end})>"

    def __repr__(self) -> str:
        """
        Returns a string representation for debugging.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    @staticmethod
    def empty() -> PointsInTimeSeries:
        """
        Returns an empty PointsInTimeSeries.

        Returns
        -------
        PointsInTimeSeries
            Empty PointsInTimeSeries object.
        """
        return PointsInTimeSeries([], [])
