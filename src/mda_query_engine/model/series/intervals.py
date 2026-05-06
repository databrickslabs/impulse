"""Intervals class implementation"""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import pyspark.sql.types as T

from .points_in_time import PointsInTime


class Intervals:
    def __init__(
        self, tstarts: npt.NDArray, tends: npt.NDArray, merge_overlaps=False, del_last_empty=False
    ):
        """
        Initialize the Intervals object.

        Parameters
        ----------
        tstarts : numpy.ndarray or array-like
            Array of interval start times.
        tends : numpy.ndarray or array-like
            Array of interval end times.
        merge_overlaps : bool, optional
            If True, merge overlapping and consecutive intervals (default is False).
        del_last_empty : bool, optional
            If True, remove empty intervals at the end (default is False).
        """
        self.tstarts = np.array(tstarts, dtype=np.float64)
        self.tends = np.array(tends, dtype=np.float64)
        if merge_overlaps:
            self.merge_overlaps(inplace=True)
        # filter out empty
        if del_last_empty:
            non_empty_intervals = ~(self.tstarts >= self.tends)
        else:
            non_empty_intervals = np.append(
                ~(self.tstarts[:-1] >= self.tends[:-1]), ~(self.tstarts[-1:] > self.tends[-1:])
            )
        self.tstarts = self.tstarts[non_empty_intervals]
        self.tends = self.tends[non_empty_intervals]

    def dtype(self):
        """
        Returns the Spark data type for intervals.

        Returns
        -------
        pyspark.sql.types.ArrayType
            Spark ArrayType for intervals: [[tstart_1, tend_1], ..., [tstart_N, tend_N]].
        """
        return T.ArrayType(T.ArrayType(T.DoubleType()))

    def get_data(self) -> list:
        """
        Returns a list of [tstart, tend] pairs.

        Returns
        -------
        list
            List of interval start and end pairs.
        """
        return [list(a) for a in zip(self.tstarts, self.tends, strict=False)]

    def merge_overlaps(self, inplace=False) -> Intervals:
        """
        Merge overlapping and consecutive intervals together.

        Parameters
        ----------
        inplace : bool, optional
            If True, modifies the current object in place (default is False).

        Returns
        -------
        Intervals
            Intervals object with merged intervals.
        """
        if len(self) == 0:
            if inplace:
                return self
            return Intervals.empty()
        equal_idx = list(self.tstarts[1:] <= self.tends[:-1])
        # special case for the last interval, which could be a point in time (length 0)
        if (
            len(self) > 1
            and (self.tstarts[-1] == self.tends[-1])
            and (self.tends[-2] == self.tends[-1])
        ):
            equal_idx[-1] = False
        tstarts_idx = ~np.array([False] + equal_idx)
        tends_idx = ~np.array(equal_idx + [False])
        if not inplace:
            return Intervals(
                self.tstarts[tstarts_idx], self.tends[tends_idx], merge_overlaps=False
            )
        self.tstarts = self.tstarts[tstarts_idx]
        self.tends = self.tends[tends_idx]
        return self

    def merge_intervals(self, d: float) -> Intervals:
        """
        Merge intervals whose gap is strictly less than d time units.

        Parameters
        ----------
        d : float
            Maximum gap (in time units) between consecutive intervals to merge.

        Returns
        -------
        Intervals
            New Intervals object with close intervals merged.

        Raises
        ------
        ValueError
            If d is negative.
        """
        if d < 0:
            raise ValueError("merge_intervals threshold must be non-negative")
        if len(self) == 0:
            return Intervals.empty()
        gaps = self.tstarts[1:] - self.tends[:-1]
        should_merge = gaps < d
        tstarts_mask = ~np.concatenate([[False], should_merge])
        tends_mask = ~np.concatenate([should_merge, [False]])
        return Intervals(self.tstarts[tstarts_mask], self.tends[tends_mask])

    def debounce(self, d: float) -> Intervals:
        """
        Keep intervals only after the signal state has sustained for at least d
        time units (debounce / sustaining semantics).

        Short intervals (duration < d) that follow a confirmed event within
        debounce tolerance are absorbed into that event.  Short intervals that
        are isolated (no confirmed event yet, or gap from the last confirmed end
        is >= d) are discarded.  Long intervals (duration >= d) always start or
        extend a confirmed event.

        The difference from ``merge_intervals`` and ``filter`` is best shown
        with an example.  Consider ``d = 3`` (signal must sustain 3 units)::

            original signal:    ________--__--__---------------____-____----__----__--__---
            merge_intervals(3): ________-----------------------____-____------------------- (3 events)
            filter(3):          ________________---------------_________----__----______--- (4 events)
            debounce(3):        ________________---------------_________------------------- (2 events)

        Parameters
        ----------
        d : float
            Debounce threshold in time units.  The signal must sustain for at
            least this long to be recognised as a valid event.

        Returns
        -------
        Intervals
            New Intervals object with debounced intervals.

        Raises
        ------
        ValueError
            If d is negative.
        """
        if d < 0:
            raise ValueError("Debounce threshold must be non-negative")
        if len(self) == 0:
            return Intervals.empty()

        result_starts = []
        result_ends = []

        confirmed_start = None
        confirmed_end = None

        for i in range(len(self)):
            duration = self.tends[i] - self.tstarts[i]
            is_long = duration >= d

            if confirmed_start is None:
                # No confirmed output yet
                if is_long:
                    confirmed_start = self.tstarts[i]
                    confirmed_end = self.tends[i]
                # else: short with no confirmed block → discard
            else:
                gap = self.tstarts[i] - confirmed_end
                if gap < d:
                    # Within debounce tolerance → extend confirmed block
                    confirmed_end = self.tends[i]
                # Gap too large
                elif is_long:
                    # Finalize current confirmed block, start a new one
                    result_starts.append(confirmed_start)
                    result_ends.append(confirmed_end)
                    confirmed_start = self.tstarts[i]
                    confirmed_end = self.tends[i]
                    # else: short interval beyond tolerance → discard

        # Finalize last confirmed block
        if confirmed_start is not None:
            result_starts.append(confirmed_start)
            result_ends.append(confirmed_end)

        return Intervals(np.array(result_starts), np.array(result_ends))

    def filter(self, d: float) -> Intervals:
        """
        Remove intervals whose duration is strictly less than d time units.

        Parameters
        ----------
        d : float
            Minimum duration (in time units) for an interval to be kept.

        Returns
        -------
        Intervals
            New Intervals object with short intervals removed.

        Raises
        ------
        ValueError
            If d is negative.
        """
        if d < 0:
            raise ValueError("Filter duration threshold must be non-negative")
        if len(self) == 0:
            return Intervals.empty()
        durations = self.tends - self.tstarts
        keep = durations >= d
        return Intervals(self.tstarts[keep], self.tends[keep])

    def starts(self) -> np.ndarray[np.float64]:
        """
        Returns an array of all start times.

        Returns
        -------
        numpy.ndarray
            Array of interval start times.
        """
        return self.tstarts

    def ends(self) -> np.ndarray[np.float64]:
        """
        Returns an array of all end times.

        Returns
        -------
        numpy.ndarray
            Array of interval end times.
        """
        return self.tends

    def start_time(self) -> np.float64:
        """
        Returns the start time of the first interval.

        Returns
        -------
        float
            Start time of the first interval, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return self.tstarts[0]

    def end_time(self) -> np.int64:
        """
        Returns the end time of the last interval.

        Returns
        -------
        float
            End time of the last interval, or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return self.tends[-1]

    def duration_ms(self) -> np.float64:
        """
        Returns the total duration in milliseconds.

        Returns
        -------
        float
            Total duration (end_time - start_time) in milliseconds.
        """
        return (self.end_time() - self.start_time()) * 1000.0

    def durations(self) -> npt.NDArray:
        """
        Returns an array containing the durations of all intervals.

        Returns
        -------
        numpy.ndarray
            Array of durations for all intervals.
        """
        if len(self) == 0:
            return np.array([])
        return self.tends - self.tstarts

    def expand_left(self, width: float) -> Intervals:
        """
        Expands the left bound of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to expand the left bound (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with expanded left bounds.
        """
        return Intervals(self.tstarts - width, self.tends, merge_overlaps=True)

    def expand_right(self, width: float) -> Intervals:
        """
        Expands the right bound of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to expand the right bound (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with expanded right bounds.
        """
        return Intervals(self.tstarts, self.tends + width, merge_overlaps=True)

    def expand(self, width: float) -> Intervals:
        """
        Expands both bounds of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to expand both bounds (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with expanded bounds.
        """
        return Intervals(self.tstarts - width, self.tends + width, merge_overlaps=True)

    def shrink_left(self, width: float) -> Intervals:
        """
        Shrinks the left bound of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to shrink the left bound (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with shrunk left bounds.
        """
        return Intervals(
            self.tstarts + width, self.tends, merge_overlaps=True, del_last_empty=True
        )

    def shrink_right(self, width: float) -> Intervals:
        """
        Shrinks the right bound of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to shrink the right bound (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with shrunk right bounds.
        """
        return Intervals(
            self.tstarts, self.tends - width, merge_overlaps=True, del_last_empty=True
        )

    def shrink(self, width: float) -> Intervals:
        """
        Shrinks both bounds of all intervals by width seconds.

        Parameters
        ----------
        width : float
            Amount to shrink both bounds (in seconds).

        Returns
        -------
        Intervals
            New Intervals object with shrunk bounds.
        """
        return Intervals(
            self.tstarts + width, self.tends - width, merge_overlaps=True, del_last_empty=True
        )

    def __and__(self, other: Intervals | PointsInTime) -> Intervals | PointsInTime:
        """
        Returns the intersection with another Intervals or PointsInTime object.

        Parameters
        ----------
        other : Intervals or PointsInTime
            Object to intersect with.

        Returns
        -------
        Intervals or PointsInTime
            Intersection result.
        """
        pairs = Intervals.plane_sweep(self, other)
        if isinstance(other, PointsInTime):
            starts = [other.tstarts[i2] for i1, i2 in pairs]
            return PointsInTime(starts)
        starts = np.array([max(self.tstarts[i1], other.tstarts[i2]) for i1, i2 in pairs])
        ends = np.array([min(self.tends[i1], other.tends[i2]) for i1, i2 in pairs])
        return Intervals(starts, ends)

    def __or__(self, other: Intervals) -> Intervals:
        """
        Returns the union with another Intervals object.

        Parameters
        ----------
        other : Intervals
            Intervals object to union with.

        Returns
        -------
        Intervals
            Union of intervals.
        """
        starts = np.append(self.tstarts, other.tstarts)
        ends = np.append(self.tends, other.tends)
        sortidx = np.argsort(starts)
        return Intervals(starts[sortidx], ends[sortidx], merge_overlaps=True)

    def __len__(self) -> int:
        """
        Returns the number of intervals.

        Returns
        -------
        int
            Number of intervals.
        """
        return len(self.tstarts)

    @staticmethod
    def __plane_sweep_pit(
        pits: PointsInTime, intervals: Intervals, invert_order=False
    ) -> list[tuple[int, int]]:
        """
        Finds intersections between PointsInTime and Intervals using plane sweep.

        Parameters
        ----------
        pits : PointsInTime
            Points in time to check for intersection.
        intervals : Intervals
            Intervals to check for intersection.
        invert_order : bool, optional
            If True, invert the order of returned pairs (default is False).

        Returns
        -------
        list of tuple
            List of index pairs indicating intersections.
        """
        if len(pits) == 0 or len(intervals) == 0:
            return []
        pairs = []
        idx1 = 0
        idx2 = 0
        while idx1 < len(pits) and idx2 < len(intervals):
            if (
                pits.tstarts[idx1] < intervals.tstarts[idx2]
            ):  # PIT is before current interval start
                idx1 += 1
            else:  # PIT is after current interval start
                # scan forward
                idx1i = idx1
                while idx1i < len(pits) and intervals.tends[idx2] > pits.tstarts[idx1i]:
                    if invert_order:
                        pairs.append((idx2, idx1i))
                    else:
                        pairs.append((idx1i, idx2))
                    idx1i += 1
                idx2 += 1
        return pairs

    @staticmethod
    def __pit_overlaps_interval(
        pit: np.float64, intervals: Intervals, idx: int, is_empty_int: bool
    ) -> bool:
        """
        Check if a given point in time overlaps with an interval.

        Parameters
        ----------
        pit : float
            Point in time to check.
        intervals : Intervals
            Intervals object.
        idx : int
            Index of the interval to check.
        is_empty_int : bool
            Whether the interval is empty.

        Returns
        -------
        bool
            True if the point overlaps with the interval, False otherwise.
        """
        interval = (intervals.tstarts[idx], intervals.tends[idx])
        is_last_int = idx == len(intervals) - 1
        return (pit >= interval[0]) and (
            (pit < interval[1]) or ((pit == interval[1]) and is_last_int and is_empty_int)
        )

    @staticmethod
    def __plane_sweep_intervals(
        intervals1: Intervals, intervals2: Intervals
    ) -> list[tuple[int, int]]:
        """
        Finds intersections between two Intervals objects using plane sweep.

        Parameters
        ----------
        intervals1 : Intervals
            First Intervals object.
        intervals2 : Intervals
            Second Intervals object.

        Returns
        -------
        list of tuple
            List of index pairs indicating intersections.
        """
        if len(intervals1) == 0 or len(intervals2) == 0:
            return []
        pairs = []
        idx1 = 0
        idx2 = 0
        i1_last_empty = intervals1.tstarts[-1] == intervals1.tends[-1]
        i2_last_empty = intervals2.tstarts[-1] == intervals2.tends[-1]
        while idx1 < len(intervals1) and idx2 < len(intervals2):
            if intervals1.tstarts[idx1] < intervals2.tstarts[idx2]:
                # scan forward
                idx2i = idx2
                while (
                    idx2i < len(intervals2) and intervals1.tends[idx1] > intervals2.tstarts[idx2i]
                ):
                    pairs.append((idx1, idx2i))
                    idx2i += 1
                idx1 += 1

            # if last interval of i1 is a point in time, check if it overlaps with current interval of i2
            elif (
                (idx1 == len(intervals1) - 1)
                and i1_last_empty
                and Intervals.__pit_overlaps_interval(
                    intervals1.tstarts[idx1], intervals2, idx2, i2_last_empty
                )
            ):
                pairs.append((idx1, idx2))
                idx1 += 1

            # if last interval of i2 is a point in time, check if it overlaps with current interval of i1
            elif (
                (idx2 == len(intervals2) - 1)
                and i2_last_empty
                and Intervals.__pit_overlaps_interval(
                    intervals2.tstarts[idx2], intervals1, idx1, i1_last_empty
                )
            ):
                pairs.append((idx1, idx2))
                idx2 += 1

            else:
                # scan forward
                idx1i = idx1
                while (
                    idx1i < len(intervals1) and intervals2.tends[idx2] > intervals1.tstarts[idx1i]
                ):
                    pairs.append((idx1i, idx2))
                    idx1i += 1
                idx2 += 1
        return pairs

    @staticmethod
    def plane_sweep(
        obj1: Intervals | PointsInTime, obj2: Intervals | PointsInTime
    ) -> list[tuple[int, int]]:
        """
        Find intersections between intervals or points in time.

        Parameters
        ----------
        obj1 : Intervals or PointsInTime
            First object to check for intersection.
        obj2 : Intervals or PointsInTime
            Second object to check for intersection.

        Returns
        -------
        list of tuple
            List of index pairs indicating intersections.

        Raises
        ------
        NotImplementedError
            If argument types are not Intervals or PointsInTime.
        """
        if isinstance(obj1, PointsInTime):
            if isinstance(obj2, Intervals):
                return Intervals.__plane_sweep_pit(obj1, obj2)
            else:  # obj2 is PointsInTime
                return obj1 & obj2
        elif isinstance(obj2, Intervals):
            return Intervals.__plane_sweep_intervals(obj1, obj2)
        else:  # obj2 is PointsInTime
            return Intervals.__plane_sweep_pit(obj2, obj1, invert_order=True)
        raise NotImplementedError("Unexpected argument types. Should be Intervals or PointsInTime")

    @staticmethod
    def empty():
        """
        Returns an Intervals object with no intervals.

        Returns
        -------
        Intervals
            Empty Intervals object.
        """
        return Intervals([], [])
