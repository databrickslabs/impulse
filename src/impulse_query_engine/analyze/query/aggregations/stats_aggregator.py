"""StatsAggregator class for computing statistics within event intervals."""

from collections.abc import Callable

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
from .custom_statistic import (
    CrossChannelStatistic,
    PerChannelStatistic,
    normalize_cross_channel_statistics,
    normalize_per_channel_statistics,
)

# Define supported statistics and their types
NUMERIC_STATISTICS = {stat.value for stat in StatisticType}
STRING_STATISTICS = {}
BUILTIN_STATISTIC_NAMES = NUMERIC_STATISTICS | {"start", "end"} | set(STRING_STATISTICS)


class StatsAggregator(Aggregation):
    """
    Aggregation that computes statistics on time series data within event intervals.

    This aggregator evaluates input expressions to get SampleSeries instances,
    filters them by event intervals, and computes the requested statistics
    for each interval.

    Besides the built-in statistics, two kinds of custom statistics can be
    injected:

    - ``per_channel_custom_statistics`` are computed once per input channel and
      interval, exactly like built-ins, and ride in ``numeric_values``.
    - ``cross_channel_custom_statistics`` are computed once per interval across
      their declared input channels and are returned in ``cross_channel_values``.
    """

    def __init__(
        self,
        input_expressions: list[TimeSeriesExpression],
        statistics: list[str] | None = None,
        event_expression: TimeSeriesExpression = None,
        cross_channel_custom_statistics: dict[str, Callable | CrossChannelStatistic] | None = None,
        per_channel_custom_statistics: dict[str, Callable | PerChannelStatistic] | None = None,
        input_names: list[str] | None = None,
    ):
        """
        Initialize a StatsAggregator.

        Parameters
        ----------
        input_expressions : list of TimeSeriesExpression
            List of TimeSeriesExpression instances to compute statistics on.
            When evaluated, each expression will yield a SampleSeries.
        statistics : list of str, optional
            List of statistic types to compute (e.g., ['min', 'max', 'mean', 'median']).
            Supported numeric statistics: 'min', 'max', 'mean', 'median'.
            Supported string statistics: 'mode', 'unique_count'.
            Special statistics: 'start' (first value), 'end' (last value).
        event_expression : TimeSeriesExpression
            TimeSeriesExpression defining event intervals for statistics computation.
            When evaluated, it yields an instance of Intervals.
        cross_channel_custom_statistics : dict, optional
            Mapping of statistic name to a callable or ``CrossChannelStatistic``
            descriptor. Each statistic is computed once per event interval:
            ``func(series: list[SampleSeries], t_start: float, t_end: float,
            **params) -> float``, where ``params`` is the descriptor's params
            mapping (empty for plain callables). The series are clipped to the
            interval and ordered like the descriptor's ``inputs`` declaration
            (all input expressions in input order when no inputs are declared).
            Series may be empty; return ``float("nan")`` for an undefined result.
            Exceptions propagate and fail the query. Callables are cloudpickled
            to Spark executors — use module-level importable functions and never
            capture Spark objects.
        per_channel_custom_statistics : dict, optional
            Mapping of statistic name to a callable or ``PerChannelStatistic``
            descriptor, computed once per input channel and event interval,
            exactly like a built-in statistic: ``func(series: SampleSeries,
            t_start: float, t_end: float, **params) -> float``.
            The series is clipped to the interval and may be empty (unlike
            built-ins, the callable is invoked even for empty/all-NaN intervals).
            The same pickling guidance as for cross-channel statistics applies.
        input_names : list of str, optional
            Names of the input expressions, parallel to ``input_expressions``.
            Required when any cross-channel statistic declares ``inputs``.
        """
        self.input_expressions = input_expressions
        self.event_expression = event_expression
        self.statistics = statistics if statistics is not None else []
        self.input_names = input_names
        self.cross_channel_custom_statistics = normalize_cross_channel_statistics(
            cross_channel_custom_statistics
        )
        self.per_channel_custom_statistics = normalize_per_channel_statistics(
            per_channel_custom_statistics
        )
        self._validate_custom_statistic_names()
        self._validate_input_names()
        self._cross_channel_input_indices = self._resolve_cross_channel_inputs()

        # Separate numeric and string statistics for processing
        self._numeric_stats = [
            s for s in self.statistics if s in NUMERIC_STATISTICS or s in {"start", "end"}
        ]
        self._string_stats = [s for s in self.statistics if s in STRING_STATISTICS]

    def _validate_custom_statistic_names(self) -> None:
        """
        Ensure custom statistic names collide neither with built-ins nor each other.

        Raises
        ------
        ValueError
            If a custom statistic name shadows a built-in statistic or is present
            in both custom-statistics mappings.
        """
        custom_names = set(self.cross_channel_custom_statistics) | set(
            self.per_channel_custom_statistics
        )
        builtin_collisions = custom_names & BUILTIN_STATISTIC_NAMES
        if builtin_collisions:
            raise ValueError(
                "Custom statistic names collide with built-in statistics: "
                f"{sorted(builtin_collisions)}"
            )
        kind_collisions = set(self.cross_channel_custom_statistics) & set(
            self.per_channel_custom_statistics
        )
        if kind_collisions:
            raise ValueError(
                "Statistic names used in both cross_channel_custom_statistics and "
                f"per_channel_custom_statistics: {sorted(kind_collisions)}"
            )

    def _validate_input_names(self) -> None:
        """
        Validate ``input_names`` against ``input_expressions``.

        Raises
        ------
        ValueError
            If ``input_names`` has a different length than ``input_expressions``
            or contains duplicate names.
        """
        if self.input_names is None:
            return
        if len(self.input_names) != len(self.input_expressions):
            raise ValueError(
                f"Length mismatch: input_names has {len(self.input_names)} elements, "
                f"but input_expressions has {len(self.input_expressions)} elements."
            )
        if len(set(self.input_names)) != len(self.input_names):
            raise ValueError(f"input_names must be unique, got {self.input_names}")

    def _resolve_cross_channel_inputs(self) -> dict[str, list[int] | None]:
        """
        Resolve each cross-channel statistic's declared inputs to expression indices.

        Returns
        -------
        dict of str to (list of int or None)
            Per statistic, the indices into ``input_expressions`` in declared
            order, or ``None`` when the statistic consumes all inputs.

        Raises
        ------
        ValueError
            If inputs are declared without ``input_names`` or reference a name
            that is not in ``input_names``.
        """
        indices: dict[str, list[int] | None] = {}
        for name, statistic in self.cross_channel_custom_statistics.items():
            if statistic.inputs is None:
                indices[name] = None
                continue
            if self.input_names is None:
                raise ValueError(
                    f"Cross-channel statistic '{name}' declares inputs "
                    f"{statistic.inputs}, but no input_names were provided."
                )
            unknown = [ch for ch in statistic.inputs if ch not in self.input_names]
            if unknown:
                raise ValueError(
                    f"Cross-channel statistic '{name}' references unknown input "
                    f"channels {unknown}; available input_names: {self.input_names}"
                )
            indices[name] = [self.input_names.index(ch) for ch in statistic.inputs]
        return indices

    def __str__(self) -> str:
        """
        Return a string representation of the StatsAggregator object.

        Returns
        -------
        str
            String representation of the StatsAggregator object.
        """
        cross_channel = {
            name: statistic.inputs
            for name, statistic in self.cross_channel_custom_statistics.items()
        }
        return (
            f"<StatsAggregator input_expressions={self.input_expressions}, "
            f"event_expression={self.event_expression}, statistics={self.statistics}, "
            f"cross_channel_custom_statistics={cross_channel}, "
            f"per_channel_custom_statistics={list(self.per_channel_custom_statistics)}>"
        )

    def dtype(self) -> T.StructType:
        """
        Return the Spark data type for the aggregation result.

        The schema supports a dynamic number of statistics with different types:
        - Numeric statistics (min, max, mean, median, start, end) as DoubleType
        - String statistics (mode, unique_count) as StringType
        - Cross-channel custom statistics as one map per event interval
          (interval-ordered; empty array when none are configured)

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
                T.StructField(
                    "cross_channel_values",
                    T.ArrayType(T.MapType(T.StringType(), T.DoubleType())),
                    nullable=True,
                ),
            ]
        )

    def build(self, cache: SeriesCache) -> tuple[
        list[list[float]],
        list[list[dict[str, float]]],
        list[list[dict[str, str]]],
        list[dict[str, float]],
    ]:
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
            A 4-tuple containing:
            - event_timestamps: List of [start, end] pairs for each event interval,
              repeated once per input expression (series-major order)
            - numeric_values: List of lists of dicts with numeric statistics per
              input expression and interval
            - string_values: List of lists of dicts with string statistics per
              input expression and interval (if any)
            - cross_channel_values: One dict of cross-channel statistics per
              non-degenerate event interval, aligned with the interval order of
              ``numeric_values`` (not with the repeated ``event_timestamps``);
              empty when no cross-channel statistics are configured
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

        cross_channel_values = []
        if self.cross_channel_custom_statistics:
            for interval in intervals.get_data():
                t_start = interval[0]
                t_end = interval[1]

                if t_end == t_start:
                    continue
                cross_channel_values.append(
                    self._calculate_cross_channel_statistics(
                        sample_series_filtered, t_start, t_end
                    )
                )

        return (event_timestamps, numeric_values, string_values, cross_channel_values)

    def _calculate_aggregations(self, sample_series, t_start, t_end) -> dict[str, float]:
        """
        Compute the requested statistics on ``sample_series`` for the interval ``[t_start, t_end]``.

        Samples that fall in ``[t_start, t_end]`` are expected to already lie inside
        those bounds (clipped upstream by ``SampleSeries.where`` in ``build``, or
        naturally within them when no event expression is set).

        Built-in statistics are NaN for empty/all-NaN intervals; per-channel custom
        statistics are always invoked (possibly with an empty series) and decide
        their own undefined-value handling.
        """

        mask = (sample_series.tends > t_start) & (sample_series.tstarts < t_end)

        t_starts = sample_series.tstarts[mask]
        t_ends = sample_series.tends[mask]
        durations = t_ends - t_starts
        values = sample_series.values[mask]

        results = {}

        if values.size == 0 or np.all(np.isnan(values)):
            results = {stat: np.nan for stat in self.statistics}
        else:
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
                    results["median"] = float(
                        self.weighted_median(durations=durations, values=values)
                    )
                else:
                    raise ValueError(
                        f"Unsupported statistic type: {stat}\n"
                        "Available options are 'min', 'max', 'mean', "
                        "'median', 'start', 'end'."
                    )

        if self.per_channel_custom_statistics:
            channel_series = SampleSeries(tstarts=t_starts, tends=t_ends, values=values)
            for name, statistic in self.per_channel_custom_statistics.items():
                results[name] = self._coerce_stat_result(
                    name,
                    statistic.func(channel_series, t_start, t_end, **(statistic.params or {})),
                )

        return results

    def _calculate_cross_channel_statistics(
        self, series_list: list[SampleSeries], t_start: float, t_end: float
    ) -> dict[str, float]:
        """
        Compute all cross-channel custom statistics for the interval ``[t_start, t_end]``.

        Each statistic receives the series of its declared inputs (all inputs when
        none are declared), clipped to the interval and in declared order. Clipping
        is memoized per channel so channels shared between statistics are clipped
        only once per interval.

        Parameters
        ----------
        series_list : list of SampleSeries
            The evaluated input expressions, already filtered by the event intervals.
        t_start : float
            Interval start time.
        t_end : float
            Interval end time.

        Returns
        -------
        dict of str to float
            One value per cross-channel statistic.
        """
        interval = Intervals(tstarts=[t_start], tends=[t_end])
        clipped: dict[int, SampleSeries] = {}

        def clip(index: int) -> SampleSeries:
            if index not in clipped:
                clipped[index] = series_list[index].where(interval)
            return clipped[index]

        results = {}
        for name, statistic in self.cross_channel_custom_statistics.items():
            indices = self._cross_channel_input_indices[name]
            if indices is None:
                indices = range(len(series_list))
            statistic_series = [clip(i) for i in indices]
            results[name] = self._coerce_stat_result(
                name,
                statistic.func(statistic_series, t_start, t_end, **(statistic.params or {})),
            )
        return results

    @staticmethod
    def _coerce_stat_result(name: str, value) -> float:
        """
        Coerce a custom statistic's return value to ``float``.

        Raises
        ------
        TypeError
            If the value is not convertible to ``float``; the message names the
            offending statistic.
        """
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Custom statistic '{name}' must return a float-convertible scalar, "
                f"got {type(value).__name__}"
            ) from exc

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
