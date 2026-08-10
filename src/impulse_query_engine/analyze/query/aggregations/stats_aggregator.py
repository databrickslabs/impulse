"""StatsAggregator class for computing statistics within event intervals."""

import numpy as np
import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.aggregations.statistic_type import StatisticType
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries

from .aggregation import Aggregation

# Define supported statistics and their types
NUMERIC_STATISTICS = {stat.value for stat in StatisticType}
STRING_STATISTICS = {}


class StatsAggregator(Aggregation):
    """
    Aggregation that computes statistics on time series data within event intervals.

    This aggregator evaluates input expressions to get SampleSeries instances,
    filters them by event intervals, and computes the requested statistics
    for each interval.
    """

    def __init__(
        self,
        input_expressions: list[TimeSeriesExpression],
        statistics: list[str],
        event_expression: TimeSeriesExpression = None,
    ):
        """
        Initialize a StatsAggregator.

        Parameters
        ----------
        input_expressions : list of TimeSeriesExpression
            List of TimeSeriesExpression instances to compute statistics on.
            When evaluated, each expression will yield a SampleSeries.
        statistics : list of str
            List of statistic types to compute (e.g., ['min', 'max', 'mean', 'median']).
            Supported numeric statistics: 'min', 'max', 'mean', 'median'.
            Supported string statistics: 'mode', 'unique_count'.
            Special statistics: 'start' (first value), 'end' (last value).
        event_expression : TimeSeriesExpression
            TimeSeriesExpression defining event intervals for statistics computation.
            When evaluated, it yields an instance of Intervals.
        """
        self.input_expressions = input_expressions
        self.event_expression = event_expression
        self.statistics = statistics

        # Separate numeric and string statistics for processing
        self._numeric_stats = [
            s for s in statistics if s in NUMERIC_STATISTICS or s in {"start", "end"}
        ]
        self._string_stats = [s for s in statistics if s in STRING_STATISTICS]

    def __str__(self) -> str:
        """
        Return a string representation of the StatsAggregator object.

        Returns
        -------
        str
            String representation of the StatsAggregator object.
        """
        return (
            f"<StatsAggregator input_expressions={self.input_expressions}, "
            f"event_expression={self.event_expression}, statistics={self.statistics}>"
        )

    def dtype(self) -> T.StructType:
        """
        Return the Spark data type for the aggregation result.

        The schema supports a dynamic number of statistics with different types:
        - Numeric statistics (min, max, mean, median, start, end) as DoubleType
        - String statistics (mode, unique_count) as StringType

        Returns
        -------
        pyspark.sql.types.StructType
            Data type for the aggregation result.
        """
        return T.StructType(
            [
                T.StructField(
                    "event_timestamps",
                    T.ArrayType(T.ArrayType(T.DoubleType())),
                    nullable=True,
                ),
                T.StructField(
                    "numeric_values",
                    T.ArrayType(T.ArrayType(T.MapType(T.StringType(), T.DoubleType()))),
                    nullable=True,
                ),
                T.StructField(
                    "string_values",
                    T.ArrayType(T.ArrayType(T.MapType(T.StringType(), T.StringType()))),
                    nullable=True,
                ),
            ]
        )

    def build(
        self, cache: SeriesCache
    ) -> tuple[list[list[float]], list[list[dict[str, float]]], list[list[dict[str, str]]]]:
        """
        Build the statistics aggregation from the cache.

        This method:
        1. Evaluates each TimeSeriesExpression in input_expressions to get SampleSeries.
        2. Evaluates event_expression to get Intervals defining event time ranges.
        3. Filters each SampleSeries to only include samples within event intervals.
        4. Computes requested statistics within each event interval.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        tuple
            A 3-tuple containing:
            - event_timestamps: List of [start, end] pairs for each event interval
            - numeric_values: List of lists of dicts with numeric statistics per
              input expression and interval
            - string_values: List of lists of dicts with string statistics per
              input expression and interval (if any)
        """
        # Step 1: Evaluate input expressions to get SampleSeries instances
        sample_series_list: list[SampleSeries] = []
        for expr in self.input_expressions:
            series = expr.build(cache)
            sample_series_list.append(series)

        # Step 2: Evaluate event_expression to get Intervals
        if self.event_expression is None:
            # Create a single interval covering the entire series
            # Find min start and max end across all series
            start_times = [series.start_time() for series in sample_series_list if len(series) > 0]
            end_times = [series.end_time() for series in sample_series_list if len(series) > 0]

            if start_times and end_times:
                min_start = min(t for t in start_times if not np.isnan(t))
                max_end = max(t for t in end_times if not np.isnan(t))
                intervals = Intervals(tstarts=[min_start], tends=[max_end])
            else:
                # All series are empty
                intervals = Intervals.empty()

            # No pre-filtering needed when there's no event expression
            sample_series_filtered = sample_series_list

        else:
            intervals = self.event_expression.build(cache)
            sample_series_filtered = [s.where(intervals) for s in sample_series_list]

        event_timestamps = []
        numeric_values = []
        string_values = []

        for series in sample_series_filtered:
            numeric_values_in_series = []
            for interval in intervals.get_data():
                t_start = interval[0]
                t_end = interval[1]

                if t_end == t_start:
                    continue
                event_timestamps.append([t_start, t_end])
                numeric_values_in_series.append(
                    self._calculate_aggregations(series, t_start, t_end)
                )

            numeric_values.append(numeric_values_in_series)
        return (event_timestamps, numeric_values, string_values)

    def _calculate_aggregations(self, sample_series, t_start, t_end) -> dict[str, float]:
        """
        Compute the requested statistics on ``sample_series`` for the interval ``[t_start, t_end]``.

        Samples that fall in ``[t_start, t_end]`` are expected to already lie inside
        those bounds (clipped upstream by ``SampleSeries.where`` in ``build``, or
        naturally within them when no event expression is set).
        """

        mask = (sample_series.tends > t_start) & (sample_series.tstarts < t_end)

        t_starts = sample_series.tstarts[mask]
        t_ends = sample_series.tends[mask]
        durations = t_ends - t_starts
        values = sample_series.values[mask]

        results = {}

        if values.size == 0 or np.all(np.isnan(values)):
            return {stat: np.nan for stat in self.statistics}

        for stat in self.statistics:
            if stat == "start":
                results["start"] = sample_series.values[mask][0]
            elif stat == "end":
                results["end"] = sample_series.values[mask][-1]
            elif stat == "min":
                results["min"] = np.nanmin(sample_series.values[mask])
            elif stat == "max":
                results["max"] = np.nanmax(sample_series.values[mask])
            elif stat == "mean":
                mean = np.divide(np.nansum(values * durations), np.nansum(durations))
                results["mean"] = mean
            elif stat == "median":
                results["median"] = float(self.weighted_median(durations=durations, values=values))
            else:
                raise ValueError(
                    f"Unsupported statistic type: {stat}\n"
                    "Available options are 'min', 'max', 'mean', "
                    "'median', 'start', 'end'."
                )

        return results

    def required_tags(self) -> set[str]:
        """
        Return the union of required tags across all input expressions and event expression.

        Returns
        -------
        set of str
            Set of required tags for the aggregation.
        """
        tags = set()
        for expr in self.input_expressions:
            tags = tags.union(expr.required_tags())
        tags = tags.union(self.event_expression.required_tags()) if self.event_expression else tags
        return tags

    def get_selector_expr(self):
        """
        Return the union of selector expressions for all input expressions and event expression.

        Returns
        -------
        Any
            Combined selector expression for the aggregation.
        """
        selector_expr = None
        for expr in self.input_expressions:
            expr_selector = expr.get_selector_expr()
            if selector_expr is None:
                selector_expr = expr_selector
            else:
                selector_expr = selector_expr | expr_selector

        event_selector = (
            self.event_expression.get_selector_expr() if self.event_expression else None
        )
        # If either selector is None, return the other; only combine when both exist
        if selector_expr is None:
            return event_selector
        if event_selector is None:
            return selector_expr
        return selector_expr | event_selector

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the union of required tag expressions across all input expressions
        and event expression.

        Returns
        -------
        set of TagExpression
            Set of required tag expressions for the aggregation.
        """
        tag_exprs = set()
        for expr in self.input_expressions:
            tag_exprs = tag_exprs.union(expr.get_required_tag_exprs())
        tag_exprs = (
            tag_exprs.union(self.event_expression.get_required_tag_exprs())
            if self.event_expression
            else tag_exprs
        )
        return tag_exprs

    def get_selectors(self) -> list[TimeSeriesSelector]:
        result: list[TimeSeriesSelector] = []
        for expr in self.input_expressions:
            result.extend(expr.get_selectors())
        if self.event_expression is not None:
            result.extend(self.event_expression.get_selectors())
        return result

    def weighted_median(self, durations, values):
        """Calculate duration-weighted median for RLE compressed data."""
        # Extract the slice

        # Remove NaN values
        valid_mask = ~np.isnan(values)
        valid_values = values[valid_mask]
        valid_durations = durations[valid_mask]

        if len(valid_values) == 0:
            return np.nan

        # Sort by value
        sorted_indices = np.argsort(valid_values)
        sorted_values = valid_values[sorted_indices]
        sorted_durations = valid_durations[sorted_indices]

        # Find median: value where cumulative duration reaches 50%
        cumsum = np.cumsum(sorted_durations)
        total_duration = cumsum[-1]
        median_idx = np.searchsorted(cumsum, total_duration / 2.0)

        return sorted_values[median_idx]
