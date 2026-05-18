"""SampleSeries class implementation"""

from __future__ import annotations

import os
from collections.abc import Sized
from typing import Union

import numpy as np
import numpy.typing as npt
from scipy.integrate import cumulative_trapezoid
from scipy.interpolate import interp1d

from .intervals import Intervals
from .points_in_time import PointsInTime

FloatOrNaN = float | np.float64

import pyspark.sql.types as T


class SampleSeries:
    def __init__(self, tstarts: Sized, tends: Sized, values: Sized):
        """
        Initialize the SampleSeries object.

        Parameters
        ----------
        tstarts : Sized
            Array-like of interval start times.
        tends : Sized
            Array-like of interval end times.
        values : Sized
            Array-like of sample values.
        """
        assert len(tstarts) == len(tends)
        assert len(tstarts) == len(values)
        self.tstarts = np.array(tstarts, dtype=np.float64)
        self.tends = np.array(tends, dtype=np.float64)
        self.values = np.array(values, dtype=np.float64)
        self.continuous_interval_indices = self._get_continuous_interval_indices()
        self.requires_deserialization = True

    def dtype(self):
        """
        Returns the Spark data type for SampleSeries.

        Returns
        -------
        pyspark.sql.types.BinaryType
            Spark BinaryType for serialized SampleSeries.
        """
        return T.BinaryType()

    def get_data(self) -> list:
        """
        Returns the series as a list of [tstart, tend, value] lists.

        Returns
        -------
        list
            List of [tstart, tend, value] triples.
        """
        if len(self) == 0:
            return []
        return np.column_stack([self.tstarts, self.tends, self.values]).tolist()

    def _get_continuous_interval_indices(self) -> list[(int, int)]:
        """
        Identify indices of continuous intervals in the series.

        Returns
        -------
        list of tuple
            List of (start, end) indices for continuous intervals.
        """
        diff = self.tstarts[1:] - self.tends[:-1] == 0
        indices = np.where(diff == False)[0]
        result = []
        current_index = 0
        for i in indices:
            result.append((current_index, i))
            current_index = i + 1
        if current_index < len(self.tstarts):
            result.append((current_index, len(self.tstarts) - 1))
        return result

    def sparse(self) -> SampleSeries:
        """
        Returns a sparse version of this SampleSeries, merging consecutive samples with the same value.

        Returns
        -------
        SampleSeries
            New sparse SampleSeries.
        """
        if len(self) < 2:
            return SampleSeries(self.tstarts, self.tends, self.values)
        prev_s = self.tstarts[0]
        prev_e = self.tends[0]
        prev_v = self.values[0]
        starts = []
        ends = []
        values = []
        # TODO: improve, is there a better way?
        for start, end, value in zip(
            self.tstarts[1:], self.tends[1:], self.values[1:], strict=False
        ):
            if (value == prev_v) or (np.isnan(value) and np.isnan(prev_v)):
                prev_e = end
                continue
            starts.append(prev_s)
            ends.append(prev_e)
            values.append(prev_v)
            prev_s = start
            prev_e = end
            prev_v = value
        # append end
        starts.append(prev_s)
        ends.append(prev_e)
        values.append(prev_v)
        return SampleSeries(starts, ends, values)

    def _apply_basic_op(self, operation, other: float | SampleSeries):
        """
        Apply a basic arithmetic operation to this series and another operand.

        Parameters
        ----------
        operation : callable
            Numpy operation to apply.
        other : float or SampleSeries
            Operand for the operation.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        if isinstance(other, SampleSeries):
            ts0, ts1 = self.synchronized(other)
            return SampleSeries(ts0.tstarts, ts1.tends, operation(ts0.values, ts1.values))
        return SampleSeries(self.tstarts, self.tends, operation(self.values, other))

    def _apply_basic_rop(self, operation, other: float | SampleSeries):
        """
        Apply a basic arithmetic operation with operands reversed.

        Parameters
        ----------
        operation : callable
            Numpy operation to apply.
        other : float or SampleSeries
            Operand for the operation.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        if isinstance(other, SampleSeries):
            ts0, ts1 = other.synchronized(self)
            return SampleSeries(ts0.tstarts, ts1.tends, operation(ts0.values, ts1.values))
        return SampleSeries(self.tstarts, self.tends, operation(other, self.values))

    def __len__(self) -> int:
        """
        Returns the number of samples in the series.

        Returns
        -------
        int
            Number of samples.
        """
        return len(self.tstarts)

    def __add__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Add another SampleSeries or scalar to this series.

        Parameters
        ----------
        other : SampleSeries or float
            Operand to add.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_op(np.add, other)

    def __radd__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Add this series to another SampleSeries or scalar (reversed operands).

        Parameters
        ----------
        other : SampleSeries or float
            Operand to add.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_rop(np.add, other)

    def __sub__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Subtract another SampleSeries or scalar from this series.

        Parameters
        ----------
        other : SampleSeries or float
            Operand to subtract.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_op(np.subtract, other)

    def __rsub__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Subtract this series from another SampleSeries or scalar (reversed operands).

        Parameters
        ----------
        other : SampleSeries or float
            Operand to subtract.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_rop(np.subtract, other)

    def __mul__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Multiply this series by another SampleSeries or scalar.

        Parameters
        ----------
        other : SampleSeries or float
            Operand to multiply.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_op(np.multiply, other)

    def __rmul__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Multiply another SampleSeries or scalar by this series (reversed operands).

        Parameters
        ----------
        other : SampleSeries or float
            Operand to multiply.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_rop(np.multiply, other)

    def __truediv__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Divide this series by another SampleSeries or scalar.

        Parameters
        ----------
        other : SampleSeries or float
            Operand to divide.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_op(np.divide, other)

    def __rtruediv__(self, other: SampleSeries | float) -> SampleSeries:
        """
        Divide another SampleSeries or scalar by this series (reversed operands).

        Parameters
        ----------
        other : SampleSeries or float
            Operand to divide.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_rop(np.divide, other)

    def __mod__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return the modulus of this series and another SampleSeries or scalar.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for modulus.

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_op(np.mod, other)

    def __rmod__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return the modulus of another SampleSeries or scalar and this series.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for modulus (reversed operands).

        Returns
        -------
        SampleSeries
            Resulting SampleSeries.
        """
        return self._apply_basic_rop(np.mod, other)

    def __apply_op(self, operation, other: SampleSeries | float) -> Intervals:
        """
        Apply a comparison operation and return intervals where the condition holds.

        Parameters
        ----------
        operation : callable
            Numpy comparison operation.
        other : SampleSeries or float
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where the condition is True.
        """
        if isinstance(other, SampleSeries):
            s1, s2 = self.synchronized(other)
            idx = operation(s1.values, s2.values)
            return Intervals(s1.tstarts[idx], s1.tends[idx], merge_overlaps=True)
        idx = operation(self.values, other)
        return Intervals(self.tstarts[idx], self.tends[idx], merge_overlaps=True)

    def __gt__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is greater than another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.greater, other)

    def __ge__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is greater than or equal to another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.greater_equal, other)

    def __lt__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is less than another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.less, other)

    def __le__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is less than or equal to another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.less_equal, other)

    def __eq__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is equal to another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.equal, other)

    def __ne__(self, other: int | float | SampleSeries) -> Intervals:
        """
        Return intervals where this series is not equal to another.

        Parameters
        ----------
        other : int, float, or SampleSeries
            Operand for comparison.

        Returns
        -------
        Intervals
            Intervals where condition holds.
        """
        return self.__apply_op(np.not_equal, other)

    def sample_count(self) -> int:
        """
        Returns the number of samples in this SampleSeries.

        Returns
        -------
        int
            Number of samples.
        """
        return len(self)

    def unique_times(self) -> npt.NDArray:
        """
        Returns a sorted array of all unique start and end times.

        Returns
        -------
        numpy.ndarray
            Array of unique times.
        """
        return np.array(sorted(set(self.tstarts).union(self.tends)))

    def start_time(self) -> FloatOrNaN:
        """
        Returns the start time of the first sample.

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
        Returns the end time of the last sample.

        Returns
        -------
        float
            End time or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return self.tends[-1]

    def duration_ms(self) -> FloatOrNaN:
        """
        Returns the total duration in milliseconds.

        Returns
        -------
        float
            Duration in milliseconds.
        """
        return (self.end_time() - self.start_time()) * 1000.0

    def nan_ratio(self) -> FloatOrNaN:
        """
        Returns the ratio of NaN samples to all samples, weighted by duration.

        Returns
        -------
        float
            Ratio of NaN durations to total duration.
        """
        if len(self) == 0:
            return np.nan
        durations = self.durations()
        nan_idx = np.isnan(self.values)
        return durations[nan_idx].sum() / durations.sum()

    def durations(self) -> npt.NDArray:
        """
        Returns an array of durations for all samples.

        Returns
        -------
        numpy.ndarray
            Array of durations (in seconds).
        """
        if len(self) == 0:
            return np.array([])
        return self.tends - self.tstarts

    def sample_rate(self) -> FloatOrNaN:
        """
        Returns the average sample rate (mean duration).

        Returns
        -------
        float
            Average sample rate or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.average(self.durations())

    def sum(self) -> FloatOrNaN:
        """
        Returns the sum of all values, weighted by duration.

        Returns
        -------
        float
            Weighted sum of values.
        """
        if len(self) == 0:
            return np.nan
        return np.nansum(self.values * self.durations())

    def min(self) -> FloatOrNaN:
        """
        Returns the minimum value in the series.

        Returns
        -------
        float
            Minimum value or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.nanmin(self.values)

    def max(self) -> FloatOrNaN:
        """
        Returns the maximum value in the series.

        Returns
        -------
        float
            Maximum value or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        return np.nanmax(self.values)

    def mean(self) -> FloatOrNaN:
        """
        Returns the mean value, weighted by durations.

        Returns
        -------
        float
            Weighted mean value or NaN if empty.
        """
        if len(self) == 0:
            return np.nan
        d = self.durations()
        return np.nansum(self.values * d) / d.sum()

    def rising_edges(self) -> PointsInTime:
        """
        Returns points in time where the value rises compared to the previous sample.

        Returns
        -------
        PointsInTime
            Points where rising edges occur.
        """
        mask = self.values[1:] > self.values[:-1]
        return PointsInTime(self.tstarts[1:][mask])

    def falling_edges(self) -> PointsInTime:
        """
        Returns points in time where the value falls compared to the previous sample.

        Returns
        -------
        PointsInTime
            Points where falling edges occur.
        """
        mask = self.values[1:] < self.values[:-1]
        return PointsInTime(self.tstarts[1:][mask])

    def rising_edge(self) -> PointsInTime:
        """
        Alias for rising_edges().

        Returns
        -------
        PointsInTime
            Points where rising edges occur.
        """
        return self.rising_edges()

    def falling_edge(self) -> PointsInTime:
        """
        Alias for falling_edges().

        Returns
        -------
        PointsInTime
            Points where falling edges occur.
        """
        return self.falling_edges()

    def intervals_between_falling_edges(self) -> Intervals:
        """
        Build intervals [tstart, tend] from falling edges of the series.

        Processes each continuous interval of the series separately.
        Within each continuous block, each interval starts at a falling
        edge and ends at the timestamp before the next falling edge.
        The first interval in a block starts at the block's first timestamp;
        the last interval in a block ends at the block's last timestamp.
        Blocks with no falling edges contribute no intervals.

        Returns
        -------
        Intervals
            Intervals between consecutive falling edges.
        """
        if len(self) == 0:
            return Intervals.empty()

        all_tstarts = []
        all_tends = []

        for start_index, stop_index in self.continuous_interval_indices:
            block_tstarts = self.tstarts[start_index : stop_index + 1]
            block_tends = self.tends[start_index : stop_index + 1]
            block_values = self.values[start_index : stop_index + 1]

            if len(block_values) < 2:
                continue

            mask = block_values[1:] < block_values[:-1]
            fe_local = np.where(mask)[0] + 1  # indices within block

            if len(fe_local) == 0:
                continue

            block_interval_tstarts = np.concatenate([[block_tstarts[0]], block_tstarts[fe_local]])
            block_interval_tends = np.concatenate([block_tends[fe_local - 1], [block_tends[-1]]])
            all_tstarts.extend(block_interval_tstarts)
            all_tends.extend(block_interval_tends)

        if len(all_tstarts) == 0:
            return Intervals.empty()
        return Intervals(np.array(all_tstarts), np.array(all_tends))

    def diff(self) -> "SampleSeries":
        """
        Calculate the difference between consecutive values.

        Returns
        -------
        SampleSeries
            New SampleSeries with difference values, preserving original timestamps.
            The first value of each continuous segment is 0 (no previous value to diff from).
        """
        if len(self) == 0:
            return SampleSeries.empty()

        if len(self) == 1:
            return SampleSeries(self.tstarts.copy(), self.tends.copy(), np.array([0.0]))

        result_values = np.zeros(len(self.values), dtype=self.values.dtype)

        for start_idx, end_idx in self.continuous_interval_indices:
            segment_values = self.values[start_idx : end_idx + 1]

            if len(segment_values) > 1:
                segment_diffs = np.diff(segment_values)
                result_values[start_idx + 1 : end_idx + 1] = segment_diffs

        return SampleSeries(self.tstarts.copy(), self.tends.copy(), result_values)

    def histogram(
        self,
        bins: npt.ArrayLike = None,
        weights: SampleSeries = None,
        weight_type: str = None,
    ) -> tuple[npt.NDArray, npt.NDArray]:
        """
        Compute a histogram of the sample values using the specified bins.

        Parameters
        ----------
        bins : array_like, optional
            Bin edges for the histogram. If None, uses [-np.inf, np.inf].
        weights : SampleSeries, optional
            Custom weights series. If None, uses sample durations as weights.
        weight_type : str, optional
            Type of weighting to use. Options:
            - None (default): Use weights values directly (or durations if weights is None)
            - 'time': Multiply weights values by their durations

        Returns
        -------
        hist : ndarray
            The values of the histogram.
        bin_edges : ndarray
            The edges of the bins.

        Examples
        --------
        >>> series = SampleSeries([0, 1], [1, 2], [10, 20])
        >>> hist, edges = series.histogram(bins=[0, 15, 30])  # duration weighted
        >>> hist, edges = series.histogram(bins=[0, 15, 30], weights=custom_weights)
        >>> hist, edges = series.histogram(bins=[0, 15, 30], weights=custom_weights, weight_type='time')
        """
        if weights is None:
            weights_vector = self.durations()
        elif weight_type is None:
            weights_vector = weights.values
        elif weight_type == "time":
            weights_vector = weights.values * weights.durations()
        else:
            raise ValueError(
                f'weight_type options are: None, "time". {weight_type} is not supported'
            )

        if bins is None:
            bins = [-np.inf, np.inf]
        if len(self) == 0:
            return np.zeros(len(bins) - 1), bins

        hist, bin_edges = np.histogram(self.values, weights=weights_vector, bins=bins)
        return hist, bin_edges

    def histogram2d(
        self,
        y_series: SampleSeries,
        x_bins: npt.ArrayLike,
        y_bins: npt.ArrayLike,
        weights: SampleSeries = None,
        weight_type: str = None,
    ) -> tuple[npt.NDArray, npt.NDArray, npt.NDArray]:
        """
        Compute a bi-dimensional histogram of the sample values using the specified x and y bins.

        Parameters
        ----------
        y_series : SampleSeries
            The second sample series for the y-axis.
        x_bins : array_like
            Bin edges for the x-axis.
        y_bins : array_like
            Bin edges for the y-axis.
        weights : SampleSeries, optional
            Custom weights series. If None, uses sample durations as weights.
        weight_type : str, optional
            Type of weighting to use. Options:
            - None (default): Use weights values directly (or durations if weights is None)
            - 'time': Multiply weights values by their durations

        Returns
        -------
        hist2d : ndarray
            The 2D histogram array.
        x_edges : ndarray
            The edges of the bins for the x-axis.
        y_edges : ndarray
            The edges of the bins for the y-axis.

        Examples
        --------
        >>> x_series = SampleSeries([0, 1], [1, 2], [10, 20])
        >>> y_series = SampleSeries([0, 1], [1, 2], [5, 15])
        >>> hist2d, x_edges, y_edges = x_series.histogram2d(y_series, x_bins=[0, 15, 30], y_bins=[0, 10, 20])
        >>> hist2d, x_edges, y_edges = x_series.histogram2d(y_series, x_bins=[0, 15, 30], y_bins=[0, 10, 20], weights=custom_weights)
        >>> hist2d, x_edges, y_edges = x_series.histogram2d(y_series, x_bins=[0, 15, 30], y_bins=[0, 10, 20], weights=custom_weights, weight_type='time')
        """
        if x_bins is None:
            x_bins = [-np.inf, np.inf]
        if y_bins is None:
            y_bins = [-np.inf, np.inf]

        if len(self) == 0 or len(y_series) == 0:
            return np.zeros((len(x_bins) - 1, len(y_bins) - 1)), x_bins, y_bins

        if weights is None:
            # x_ts, y_ts = self.synchronized(y_series)
            weights_vector = self.durations()
        elif weight_type is None:
            weights_vector = weights.values
        elif weight_type == "time":
            weights_vector = weights.values * weights.durations()
        else:
            raise ValueError(
                f'weight_type options are: None, "time". {weight_type} is not supported'
            )

        hist2d, x_bins, y_bins = np.histogram2d(
            x=self.values,
            y=y_series.values,
            bins=(x_bins, y_bins),
            weights=weights_vector,
        )

        return hist2d, x_bins, y_bins

    @staticmethod
    def __pit_overlaps_interval(
        pit: np.float64, series: SampleSeries, idx: int, is_empty_int: bool
    ) -> bool:
        """
        Check if a given point in time overlaps with an interval at index idx.

        Parameters
        ----------
        pit : float
            Point in time to check.
        series : SampleSeries
            Series to check against.
        idx : int
            Index of interval.
        is_empty_int : bool
            Whether the interval is empty.

        Returns
        -------
        bool
            True if overlap, False otherwise.
        """
        interval = (series.tstarts[idx], series.tends[idx])
        is_last_int = idx == len(series) - 1
        return (pit >= interval[0]) and (
            (pit < interval[1]) or ((pit == interval[1]) and is_last_int and is_empty_int)
        )

    @staticmethod
    def __plane_sweep(series1: SampleSeries, series2: SampleSeries | Intervals | PointsInTime):
        """
        Forward scan based plane sweep to find overlapping intervals.

        Parameters
        ----------
        series1 : SampleSeries
            First series.
        series2 : SampleSeries, Intervals, or PointsInTime
            Second series or intervals.

        Returns
        -------
        list of tuple
            List of index pairs indicating overlaps.
        """
        if len(series1) == 0 or len(series2) == 0 or isinstance(series2, PointsInTime):
            return []
        pairs = []
        idx1 = 0
        idx2 = 0
        s1_last_empty = series1.tstarts[-1] == series1.tends[-1]
        s2_last_empty = series2.tstarts[-1] == series2.tends[-1]
        while idx1 < len(series1) and idx2 < len(series2):
            if series1.tstarts[idx1] < series2.tstarts[idx2]:
                # scan forward
                idx2i = idx2
                while idx2i < len(series2) and series1.tends[idx1] > series2.tstarts[idx2i]:
                    pairs.append((idx1, idx2i))
                    idx2i += 1
                idx1 += 1

            # if last interval of series1 is a point in time, check if it overlaps with current interval of series2
            elif (
                (idx1 == len(series1) - 1)
                and s1_last_empty
                and SampleSeries.__pit_overlaps_interval(
                    series1.tstarts[idx1], series2, idx2, s2_last_empty
                )
            ):
                pairs.append((idx1, idx2))
                idx1 += 1

            # if last interval of series2 is a point in time, check if it overlaps with current interval of series1
            elif (
                (idx2 == len(series2) - 1)
                and s2_last_empty
                and SampleSeries.__pit_overlaps_interval(
                    series2.tstarts[idx2], series1, idx1, s1_last_empty
                )
            ):
                pairs.append((idx1, idx2))
                idx2 += 1

            else:
                # scan forward
                idx1i = idx1
                while idx1i < len(series1) and series2.tends[idx2] > series1.tstarts[idx1i]:
                    pairs.append((idx1i, idx2))
                    idx1i += 1
                idx2 += 1

        return pairs

    def synchronized(self, other: SampleSeries):
        """
        Synchronize this series with another, aligning intervals.

        Parameters
        ----------
        other : SampleSeries
            Series to synchronize with.

        Returns
        -------
        tuple of SampleSeries
            Synchronized SampleSeries objects.
        """
        pairs = SampleSeries.__plane_sweep(self, other)
        starts = [max(self.tstarts[i1], other.tstarts[i2]) for i1, i2 in pairs]
        ends = [min(self.tends[i1], other.tends[i2]) for i1, i2 in pairs]
        values1 = [self.values[i1] for i1, _ in pairs]
        values2 = [other.values[i2] for _, i2 in pairs]
        return (
            SampleSeries(starts, ends, values1),
            SampleSeries(starts, ends, values2),
        )

    def synchronized_all(self, others: list[SampleSeries]):
        """
        Synchronize this series with multiple other SampleSeries.

        Parameters
        ----------
        others : list of SampleSeries
            List of series to synchronize.

        Returns
        -------
        tuple of SampleSeries
            Synchronized SampleSeries objects.
        """
        synced_list = [self]
        for other in others:
            tmp_synced_ts = synced_list[0]
            pairs = SampleSeries.__plane_sweep(tmp_synced_ts, other)
            starts = [max(tmp_synced_ts.tstarts[i1], other.tstarts[i2]) for i1, i2 in pairs]
            ends = [min(tmp_synced_ts.tends[i1], other.tends[i2]) for i1, i2 in pairs]
            tmp_synced_list = []
            for synced_ts in synced_list:
                tmp_values = [synced_ts.values[i1] for i1, _ in pairs]
                tmp_synced_list.append(SampleSeries(starts, ends, tmp_values))
            synced_list.clear()
            synced_list.extend(tmp_synced_list)
            other_values = [other.values[i2] for _, i2 in pairs]
            synced_list.append(SampleSeries(starts, ends, other_values))
        return tuple(synced_list)

    def where(self, other: Intervals) -> SampleSeries:
        """
        Returns a SampleSeries where the given intervals are defined.

        Parameters
        ----------
        other : Intervals
            Intervals to filter by.

        Returns
        -------
        SampleSeries
            Filtered SampleSeries.
        """
        pairs = SampleSeries.__plane_sweep(self, other)
        starts = [max(self.tstarts[i1], other.tstarts[i2]) for i1, i2 in pairs]
        ends = [min(self.tends[i1], other.tends[i2]) for i1, i2 in pairs]
        values = [self.values[i1] for i1, _ in pairs]
        return SampleSeries(starts, ends, values)

    def _interp1d(
        self,
        kind="previous",
        copy=False,
        bounds_error=False,
        fill_value=np.nan,
        assume_sorted=True,
    ):
        """
        Create an interpolation function for the series.

        Parameters
        ----------
        kind : str, optional
            Type of interpolation (default 'previous').
        copy : bool, optional
            If True, copy input arrays (default False).
        bounds_error : bool, optional
            If True, raise error for out-of-bounds (default False).
        fill_value : float, optional
            Value to use for out-of-bounds (default np.nan).
        assume_sorted : bool, optional
            If True, assume input is sorted (default True).

        Returns
        -------
        interp1d
            Interpolation function.
        """
        times = np.append(self.tstarts, self.tends[-1])
        values = np.append(self.values, self.values[-1])
        return interp1d(
            times,
            values,
            kind=kind,
            copy=copy,
            bounds_error=bounds_error,
            fill_value=fill_value,
            assume_sorted=assume_sorted,
        )

    def resample(self, sample_rate=1.0):
        """
        Resample the series at a given sample rate.

        Parameters
        ----------
        sample_rate : float, optional
            Desired sample rate (default 1.0).

        Returns
        -------
        SampleSeries
            Resampled SampleSeries.
        """
        if len(self) < 2:
            return SampleSeries(self.tstarts, self.tends, self.values)

        new_tstarts = []
        new_tends = []
        new_values = []
        for start_index, stop_index in self.continuous_interval_indices:
            t_min = np.nanmin(self.tstarts[start_index : stop_index + 1])
            t_max = np.nanmax(self.tends[start_index : stop_index + 1])
            tmp_tstarts = np.arange(t_min, t_max + sample_rate, sample_rate)
            tstarts = tmp_tstarts[tmp_tstarts < t_max]
            # last interval of length 0 is a closed interval. Allow resampled series to start and end at last timestamp
            if (stop_index == len(self) - 1) and (self.tstarts[-1] == self.tends[-1]):
                tstarts = tmp_tstarts[tmp_tstarts <= t_max]
            tends = np.append(tstarts[1:], t_max)
            values = self._interp1d()(tstarts)
            new_tstarts.extend(tstarts)
            new_tends.extend(tends)
            new_values.extend(values)

        return SampleSeries(new_tstarts, new_tends, new_values)

    def _get_rolling_intervals(self, window_size=1.0) -> Intervals:
        """
        Generate rolling intervals for a given window size.

        Parameters
        ----------
        window_size : float, optional
            Size of the rolling window (default 1.0).

        Returns
        -------
        Intervals
            Rolling intervals.
        """
        window_starts = []
        window_ends = []
        for start_index, stop_index in self.continuous_interval_indices:
            t_min = np.nanmin(self.tstarts[start_index : stop_index + 1])
            t_max = np.nanmax(self.tends[start_index : stop_index + 1])
            tmp_starts = np.arange(t_min, t_max + window_size, window_size)
            tmp_starts = tmp_starts[tmp_starts <= t_max]
            tmp_ends = np.append(tmp_starts[1:], t_max)
            window_starts.extend(tmp_starts)
            window_ends.extend(tmp_ends)

        return Intervals(np.array(window_starts), np.array(window_ends))

    def rolling_average(self, window_size=1.0, evenly_spaced=False) -> SampleSeries:
        """
        Compute rolling average over a window.

        Parameters
        ----------
        window_size : float, optional
            Size of the rolling window (default 1.0).
        evenly_spaced : bool, optional
            If True, use evenly spaced weights (default False).

        Returns
        -------
        SampleSeries
            Rolling average SampleSeries.
        """
        result_tstarts = []
        result_tends = []
        result_values = []

        rolling_intervals = self._get_rolling_intervals(window_size)
        window_starts = rolling_intervals.tstarts
        window_ends = rolling_intervals.tends
        window_series = self.where(rolling_intervals)
        for i in range(len(window_starts)):
            mask = (window_series.tstarts >= window_starts[i]) & (
                window_series.tstarts < window_ends[i]
            )
            values = window_series.values[mask]
            if values.size == 0:
                continue
            weights = None
            if not evenly_spaced and values.size > 1:
                weights = window_series.durations()[mask]

            result_tstarts.append(window_starts[i])
            result_tends.append(window_ends[i])
            result_values.append(np.average(values, weights=weights))

        return SampleSeries(result_tstarts, result_tends, result_values)

    def rolling_stats(
        self, window_size=1.0, evenly_spaced=False
    ) -> tuple[SampleSeries, SampleSeries, SampleSeries]:
        """
        Compute rolling min, max, and average over a window.

        Parameters
        ----------
        window_size : float, optional
            Size of the rolling window (default 1.0).
        evenly_spaced : bool, optional
            If True, use evenly spaced weights (default False).

        Returns
        -------
        min_series : SampleSeries
            Rolling minimum values.
        max_series : SampleSeries
            Rolling maximum values.
        avg_series : SampleSeries
            Rolling average values.
        """
        result_tstarts = []
        result_tends = []
        result_values_avg = []
        result_values_min = []
        result_values_max = []

        rolling_intervals = self._get_rolling_intervals(window_size)
        window_starts = rolling_intervals.tstarts
        window_ends = rolling_intervals.tends
        window_series = self.where(rolling_intervals)
        for i in range(len(window_starts)):
            mask = (window_series.tstarts >= window_starts[i]) & (
                window_series.tstarts < window_ends[i]
            )
            values = window_series.values[mask]
            if values.size == 0:
                continue
            weights = None
            if not evenly_spaced and values.size > 1:
                weights = window_series.durations()[mask]

            result_tstarts.append(window_starts[i])
            result_tends.append(window_ends[i])
            result_values_min.append(np.min(values))
            result_values_max.append(np.max(values))
            result_values_avg.append(np.average(values, weights=weights))

        return (
            SampleSeries(result_tstarts, result_tends, result_values_min),
            SampleSeries(result_tstarts, result_tends, result_values_max),
            SampleSeries(result_tstarts, result_tends, result_values_avg),
        )

    def trapz(self) -> float:
        """
        Perform discrete integration using the composite trapezoidal rule.

        Returns
        -------
        float
            Integrated value.
        """
        result = 0.0
        _trapz = np.trapezoid if hasattr(np, "trapezoid") else np.trapz
        for start_index, stop_index in self.continuous_interval_indices:
            tmp_values = self.values[start_index : stop_index + 1]
            tmp_starts = self.tstarts[start_index : stop_index + 1]
            result = result + _trapz(y=tmp_values, x=tmp_starts)
        return result

    def cumtrapz(self) -> SampleSeries:
        """
        Perform cumulative discrete integration using the trapezoidal rule.

        Returns
        -------
        SampleSeries
            Cumulative integrated SampleSeries.
        """
        res_values = []
        for start_index, stop_index in self.continuous_interval_indices:
            tmp_values = self.values[start_index : stop_index + 1]
            tmp_starts = self.tstarts[start_index : stop_index + 1]
            tmp_res_values = cumulative_trapezoid(y=tmp_values, x=tmp_starts, initial=0)
            if len(res_values) > 0:
                tmp_res_values += res_values[-1]
            res_values.extend(tmp_res_values)
        return SampleSeries(self.tstarts, self.tends, res_values)

    def __str__(self) -> str:
        """
        Returns a string representation of the SampleSeries.

        Returns
        -------
        str
            String representation.
        """
        start = self.start_time()
        end = self.end_time()
        count = len(self)
        return f"<SampleSeries({start}..cnt:{count}..{end})>"

    def __repr__(self) -> str:
        """
        Returns a string representation for debugging.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    def serialize(self):
        """
        Serialize and compress the SampleSeries.

        Returns
        -------
        bytes
            Compressed serialized data.
        """
        import pickle as pkl

        import lz4.frame as lz4f

        # make sure directory exists
        pickled = pkl.dumps(self)
        compressed = lz4f.compress(pickled)
        return compressed

    @staticmethod
    def deserialize(d):
        """
        Deserialize a compressed SampleSeries.

        Parameters
        ----------
        d : bytes
            Compressed serialized data.

        Returns
        -------
        SampleSeries
            Deserialized SampleSeries object.
        """
        import pickle as pkl

        import lz4.frame as lz4f

        pickled = lz4f.decompress(d)
        return pkl.loads(pickled)

    def to_pickle(self, uri: str):
        """
        Write the SampleSeries to disk at the given URI.

        Parameters
        ----------
        uri : str
            File path to write to.
        """
        directory = os.path.dirname(uri)
        try:  # make sure directory exists
            os.makedirs(directory)
        except:
            pass
        compressed = self.serialize()
        with open(uri, "wb") as writer:
            writer.write(compressed)

    @staticmethod
    def from_pickle(uri: str):
        """
        Read a pickled SampleSeries from disk.

        Parameters
        ----------
        uri : str
            File path to read from.

        Returns
        -------
        SampleSeries
            Loaded SampleSeries object.
        """
        with open(uri, "rb") as reader:
            return SampleSeries.deserialize(reader.read())

    @staticmethod
    def from_timestamps(times, values):
        """
        Create a SampleSeries from timestamps and values.

        Parameters
        ----------
        times : array-like
            Array of timestamps.
        values : array-like
            Array of values.

        Returns
        -------
        SampleSeries
            Constructed SampleSeries.
        """
        times = np.array(times, dtype=np.float64)
        return SampleSeries(list(times[:-1]) + [times[-1]], list(times[1:]) + [times[-1]], values)

    @staticmethod
    def empty() -> SampleSeries:
        """
        Returns an empty SampleSeries.

        Returns
        -------
        SampleSeries
            Empty SampleSeries object.

        Examples
        --------
        from mda_query_engine.model.series.sample_series import SampleSeries
        SampleSeries.empty()
        <SampleSeries(nan..cnt:0..nan)>
        """
        return SampleSeries([], [], [])
