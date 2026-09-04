"""StatsAggregator reporting class for computing statistics within event intervals."""

from __future__ import annotations

import functools
import hashlib
from collections.abc import Callable

import pyspark.sql.functions as f
import zlib
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StringType,
)

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    CrossChannelStatistic,
    PerChannelStatistic,
    normalize_cross_channel_statistics,
    normalize_per_channel_statistics,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import (
    StatsAggregator as QueryEngineStatsAggregator,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries
from impulse_reporting.aggregations.aggregation import Aggregation
from impulse_reporting.events.event import Event
from impulse_reporting.persist.dimension_schema import STATS_AGGREGATOR_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import STATS_AGGREGATOR_FACT_SCHEMA
from impulse_reporting.util.event_instance_util import generate_event_instance_id_column
from impulse_reporting.util.report_entity_util import ReportEntityUtil


class StatsAggregator(Aggregation):
    """Class representing a statistics aggregation in a report.

    This aggregation computes various statistics (min, max, mean, median, etc.)
    on time series data within defined event intervals.
    """

    def __init__(
        self,
        name: str,
        input_expressions: list[TimeSeriesExpression],
        channel_names: list[str],
        statistics: list[str],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = "stats_aggregator",
        values_unit: str = None,
        cross_channel_custom_statistics: list[CrossChannelStatistic] | None = None,
        per_channel_custom_statistics: list[PerChannelStatistic] | None = None,
    ):
        """
        Initialize a StatsAggregator object.

        Parameters
        ----------
        name : str
            Name of the statistics aggregation.
        input_expressions : list of TimeSeriesExpression
            List of time series expressions to compute statistics on.
        channel_names : list of str
            Names of the signals associated with input expressions. Must be the same length as input_expressions.
        statistics : list of str
            List of statistic types to compute (e.g., ['min', 'max', 'mean', 'median']).
        event : Event, optional
            Event defining intervals for statistics computation. If None, statistics
            are computed over the entire time series.
        desc : str, optional
            Description of the aggregation.
        agg_type : str, optional
            Type of aggregation, defaults to "stats_aggregator".
        values_unit : str, optional
            Unit of the statistic values.
        cross_channel_custom_statistics : list of CrossChannelStatistic, optional
            Custom statistics computed once per event interval across each
            descriptor's declared input channels (referencing ``channel_names``;
            all channels when none are declared). Fact rows carry the
            descriptor's ``channel_name`` (applied to all its output labels),
            defaulting to each output's ``aggregation_label``. See
            ``impulse_query_engine`` ``StatsAggregator`` for the callable contract.
        per_channel_custom_statistics : list of PerChannelStatistic, optional
            Custom statistics computed once per input channel and event interval,
            exactly like a built-in statistic. Fact rows carry the real channel
            names. Descriptor ``params`` are passed to the callable as keyword
            arguments.
        """
        Aggregation.__init__(self, name)
        self.input_expressions = input_expressions
        self.channel_names = channel_names
        self._validate_channel_names()
        for expr in self.input_expressions:
            expr.require_evaluation_type(
                SampleSeries, owner="StatsAggregator", example="a channel selection"
            )
        self.statistics = statistics
        self.cross_channel_custom_statistics = normalize_cross_channel_statistics(
            cross_channel_custom_statistics
        )
        self._validate_cross_channel_channel_names()
        self.per_channel_custom_statistics = normalize_per_channel_statistics(
            per_channel_custom_statistics
        )
        self.event = event
        self._validate_event_evaluation_type(
            self.event, Intervals, example="(channel > 2000) & (channel < 5000)"
        )
        self.desc = desc
        self.agg_type = agg_type
        self.values_unit = values_unit
        self.expression = self._set_expression()

    def _validate_cross_channel_channel_names(self) -> None:
        """
        Validate the ``channel_name`` of each cross-channel statistic descriptor.

        Raises
        ------
        ValueError
            If a descriptor's ``channel_name`` is set but not a non-empty string.
        """
        for statistic in self.cross_channel_custom_statistics:
            if statistic.channel_name is None:
                continue
            if not isinstance(statistic.channel_name, str) or not statistic.channel_name:
                raise ValueError(
                    f"channel_name of cross-channel statistic "
                    f"{statistic.aggregation_labels!r} must be a non-empty string, "
                    f"got {statistic.channel_name!r}"
                )

    def _validate_channel_names(self) -> None:
        """
        Validate that channel_names and input_expressions have the same length.

        Raises
        ------
        ValueError
            If the lengths of channel_names and input_expressions do not match.
        """
        if len(self.channel_names) != len(self.input_expressions):
            raise ValueError(
                f"Length mismatch: channel_names has {len(self.channel_names)} elements, "
                f"but input_expressions has {len(self.input_expressions)} elements. "
                "They must have the same length."
            )

    def get_id(self) -> int:
        """
        Get a unique identifier for the statistics aggregation.

        Returns
        -------
        int
            Unique identifier for the statistics aggregation.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF

    def get_event(self) -> Event:
        """
        Get the event associated with the aggregation.

        Returns
        -------
        Event
            The event associated with the aggregation, or None if not set.
        """
        return self.event

    def get_expression(self) -> TimeSeriesExpression:
        """
        Get the time series expression for the statistics aggregation.

        Returns
        -------
        TimeSeriesExpression
            The time series expression for the statistics aggregation.
        """
        return self.expression

    def get_expression_str(self) -> str:
        """
        Get a string representation of the time series expression.

        Returns
        -------
        str
            String representation of the time series expression.
        """
        if isinstance(self.expression, TimeSeriesExpression):
            return self.expression.__str__()
        else:
            return "NA"

    def _set_expression(self) -> TimeSeriesExpression:
        """
        Set the expression for the statistics aggregation.

        Creates a query engine StatsAggregator via the `.stats()` method
        on the first input expression.

        Returns
        -------
        TimeSeriesExpression
            The configured statistics aggregation expression.
        """
        if not self.input_expressions or len(self.input_expressions) == 0:
            raise ValueError("At least one input expression is required")

        query_eng_stats_agg = QueryEngineStatsAggregator(
            input_expressions=self.input_expressions,
            statistics=self.statistics,
            event_expression=self.event.get_expression() if self.event else None,
            cross_channel_custom_statistics=self.cross_channel_custom_statistics,
            per_channel_custom_statistics=self.per_channel_custom_statistics,
            input_names=self.channel_names,
        ).alias(self.name)

        return query_eng_stats_agg

    @staticmethod
    def _custom_statistic_labels(
        statistics: list[PerChannelStatistic | CrossChannelStatistic],
    ) -> list[str]:
        """
        Flatten custom statistics to the output labels they produce.

        These are the values that appear as ``aggregation_label`` in the fact rows.
        """
        labels: list[str] = []
        for statistic in statistics:
            labels.extend(statistic.aggregation_labels)
        return labels

    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the statistics aggregation.

        Returns
        -------
        dict
            Dictionary containing aggregation metadata.
        """
        return {
            "visual_id": self.get_id(),
            "report_id": self.report_id,
            "name": self.name,
            "page_number": self.page_number,
            "description": self.desc,
            "agg_type": self.agg_type if self.agg_type else "stats_aggregator",
            "statistics": (
                list(self.statistics)
                + self._custom_statistic_labels(self.per_channel_custom_statistics)
                + self._custom_statistic_labels(self.cross_channel_custom_statistics)
            ),
            "channel_names": self.channel_names,
            "signal_expressions": [expr.__str__() for expr in self.input_expressions],
            "values_unit": self.values_unit,
            "definition_hash": self.determine_definition_hash(),
        }

    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the statistics aggregation.

        Returns
        -------
        Row
            Spark Row containing aggregation metadata.
        """
        return Row(**self.as_dict())

    @classmethod
    def determine_aggregations(
        cls,
        spark: SparkSession,
        aggregations: list[StatsAggregator],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Determine and process aggregations for a list of StatsAggregator visuals.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for computation.
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.
        solved_df : DataFrame, optional
            Pre-solved wide DataFrame from centralized batch solve. Required.
        query : QueryBuilder, optional
            Query builder (unused, kept for interface compatibility).
        solver : QuerySolver, optional
            Solver (unused, kept for interface compatibility).
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers (unused, kept for interface compatibility).

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing the processed stats aggregations.
        """
        if solved_df is None:
            raise ValueError(
                "StatsAggregator.determine_aggregations requires solved_df. "
                "Provide a pre-solved DataFrame from the centralized batch-solve flow."
            )

        stats_names = [stats_agg.get_name() for stats_agg in aggregations]

        result = solved_df.select("container_id", *stats_names)

        # Single pass over the solved struct: per-channel and cross-channel stats
        # are exploded together (see _explode_stats_values), so ``result`` and the
        # upstream solve are traversed only once. The two channel-name passes run in
        # sequence over the one frame: _add_channel_name_column names signal_index
        # >= 0 rows, then _add_cross_channel_name_column names the signal_index == -1
        # rows while preserving the per-channel names already set.
        df = (
            result.transform(StatsAggregator._unpivot_measurement_info(stats_names))
            .transform(StatsAggregator._extract_stats_info)
            .transform(StatsAggregator._add_event_id_column(aggregations))
            .transform(StatsAggregator._add_event_name_column(aggregations))
            .transform(StatsAggregator._explode_stats_values)
            .transform(StatsAggregator._add_channel_name_column(aggregations))
            .transform(StatsAggregator._add_cross_channel_name_column(aggregations))
            .transform(StatsAggregator._add_event_instance_id_column(aggregations))
            .transform(StatsAggregator._add_visual_id_column(aggregations))
            .select(STATS_AGGREGATOR_FACT_SCHEMA.fieldNames())
        )
        return df

    @staticmethod
    def _unpivot_measurement_info(stats_names: list[str]) -> Callable[..., DataFrame]:
        """
        Unpivot the measurement info columns into long format.

        Parameters
        ----------
        stats_names : list of str
            List of statistics aggregation names to unpivot.

        Returns
        -------
        function
            Function that unpivots the DataFrame columns into long format.
        """

        def _(df: DataFrame) -> DataFrame:
            return df.unpivot(
                f.col("container_id"),
                stats_names,
                variableColumnName="stats_name",
                valueColumnName="value",
            )

        return _

    @staticmethod
    def _extract_stats_info(df: DataFrame) -> DataFrame:
        """
        Extract statistics values and event timestamps from the struct column.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing statistics struct column.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with separate columns for event timestamps, numeric values, and string values.
        """
        return (
            df.withColumn("event_timestamps", f.col("value.event_timestamps"))
            .withColumn("numeric_values", f.col("value.numeric_values"))
            .withColumn("string_values", f.col("value.string_values"))
            .withColumn("cross_channel_values", f.col("value.cross_channel_values"))
        )

    @staticmethod
    def _add_event_id_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add an event_id column to the DataFrame based on the provided visuals.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the event_id column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            event_id_column = ReportEntityUtil.get_event_id_column(
                elements=aggregations, element_name="stats_name"
            )
            return df.withColumn("event_id", event_id_column)

        return _

    @staticmethod
    def _add_event_name_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add an event_name column to the DataFrame based on the provided aggregations.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the event_name column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            # Build mapping from aggregation name to event name
            name_to_event_name = {}
            for agg in aggregations:
                if agg and agg.get_event():
                    name_to_event_name[agg.get_name()] = agg.get_event().get_name()

            col_expr = None
            for agg_name, event_name in name_to_event_name.items():
                if event_name is None:
                    continue
                elif col_expr is None:
                    col_expr = f.when(f.col("stats_name") == f.lit(agg_name), f.lit(event_name))
                else:
                    col_expr = col_expr.when(
                        f.col("stats_name") == f.lit(agg_name), f.lit(event_name)
                    )

            event_name_column = (
                col_expr.otherwise(None)
                if col_expr is not None
                else f.lit(None).cast(StringType())
            )
            return df.withColumn("event_name", event_name_column)

        return _

    @staticmethod
    def _explode_stats_values(df: DataFrame) -> DataFrame:
        """
        Explode per-channel and cross-channel statistics into one row per
        (signal, interval, statistic) in a single pass.

        Per-channel rows carry ``signal_index`` 0..N-1 (mapped to a real channel
        name downstream); cross-channel rows carry ``signal_index = -1``. Both are
        exploded from a single combined signal axis so ``df`` (and the upstream
        solve) is traversed only once.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing ``event_timestamps``, ``numeric_values`` and
            ``cross_channel_values``.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with exploded statistics for each signal and interval.
        """
        # Step 1: Explode by signal index to get one row per signal.
        #
        # Per-channel stats live in ``numeric_values`` (array<array<map>>, one inner
        # list per signal). Cross-channel stats live in ``cross_channel_values``
        # (array<map>, one map per interval) — exactly one signal's worth. Prepend
        # them as the first entry of the signal axis so both explode in a single
        # pass; ``signal_index = pos - 1`` then maps cross-channel to -1 and real
        # signals to 0..N-1. This avoids forking ``df`` (which would re-run the
        # upstream solve). An empty ``cross_channel_values`` prepends ``[[]]`` — a
        # length-0 interval list that zips to zero rows, so no spurious -1 row.
        all_signal_values = f.concat(
            f.array(f.col("cross_channel_values")), f.col("numeric_values")
        )
        df_with_signal = df.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            "event_timestamps",
            f.posexplode(all_signal_values).alias("pos", "signal_stats_per_interval"),
        ).withColumn("signal_index", f.col("pos") - 1)

        # Step 2: Explode by interval - zip event_timestamps with signal_stats_per_interval.
        # Both are aligned per interval (event_timestamps is canonical, one entry per
        # interval), so the zip lines up 1:1.
        df_with_interval = df_with_signal.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            "signal_index",
            f.posexplode(
                f.arrays_zip(f.col("event_timestamps"), f.col("signal_stats_per_interval"))
            ).alias("interval_index", "zipped"),
        )

        # Step 3: Extract start_ts, end_ts and statistics map from zipped struct
        df_with_timestamps = df_with_interval.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            "signal_index",
            f.col("zipped.event_timestamps").getItem(0).alias("start_ts"),
            f.col("zipped.event_timestamps").getItem(1).alias("end_ts"),
            f.col("zipped.signal_stats_per_interval").alias("statistics"),
        )

        # Step 4: Explode the statistics map into individual rows (aggregation_label, statistic_value)
        return df_with_timestamps.select(
            "container_id",
            "stats_name",
            "event_name",
            "event_id",
            "signal_index",
            "start_ts",
            "end_ts",
            f.explode(f.col("statistics")).alias("aggregation_label", "statistic_value"),
        )

    @staticmethod
    def _add_cross_channel_name_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add a channel_name column for cross-channel statistic rows.

        This pass runs after ``_add_channel_name_column`` over the same frame, so
        it only touches cross-channel rows (``signal_index == -1``) and preserves
        the per-channel ``channel_name`` already set on ``signal_index >= 0`` rows.

        For a cross-channel row, a descriptor with an explicit ``channel_name``
        applies it to all of that descriptor's output rows (matched on its
        ``aggregation_labels``); cross-channel rows without an explicit
        ``channel_name`` default to their ``aggregation_label``, which is non-null
        and stable as required by the fact table's merge keys. A ``channel_name``
        equal to a real input channel name is allowed and pivots the statistic
        into that channel's rows.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the channel_name column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            is_cross_channel = f.col("signal_index") == f.lit(-1)
            col_expr = None
            for agg in aggregations:
                if agg is None:
                    continue
                agg_name = agg.get_name()
                for statistic in agg.cross_channel_custom_statistics:
                    if statistic.channel_name is None:
                        continue
                    labels = statistic.aggregation_labels
                    condition = (
                        is_cross_channel
                        & (f.col("stats_name") == f.lit(agg_name))
                        & (f.col("aggregation_label").isin(labels))
                    )
                    if col_expr is None:
                        col_expr = f.when(condition, f.lit(statistic.channel_name))
                    else:
                        col_expr = col_expr.when(condition, f.lit(statistic.channel_name))

            # Cross-channel rows without an explicit descriptor channel_name default
            # to their aggregation_label; per-channel rows keep the channel_name the
            # previous pass set (never overwritten here).
            cross_channel_default = f.when(is_cross_channel, f.col("aggregation_label")).otherwise(
                f.col("channel_name")
            )
            channel_name_column = (
                col_expr.otherwise(cross_channel_default)
                if col_expr is not None
                else cross_channel_default
            )
            return df.withColumn("channel_name", channel_name_column)

        return _

    @staticmethod
    def _add_channel_name_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add a channel_name column to the DataFrame based on signal_index and aggregation channel_names.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the channel_name column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            # Build a nested when expression: for each stats_name, map signal_index to channel_name
            col_expr = None
            for agg in aggregations:
                if agg is None:
                    continue
                agg_name = agg.get_name()
                channel_names = agg.channel_names

                # For this aggregation, create when conditions for each signal index
                for idx, channel_name in enumerate(channel_names):
                    condition = (f.col("stats_name") == f.lit(agg_name)) & (
                        f.col("signal_index") == f.lit(idx)
                    )
                    if col_expr is None:
                        col_expr = f.when(condition, f.lit(channel_name))
                    else:
                        col_expr = col_expr.when(condition, f.lit(channel_name))

            channel_name_column = (
                col_expr.otherwise(None)
                if col_expr is not None
                else f.lit(None).cast(StringType())
            )
            return df.withColumn("channel_name", channel_name_column)

        return _

    @staticmethod
    def _add_event_instance_id_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add an event_instance_id column, matching ``event_instance_fact``.

        The id comes from ``generate_event_instance_id_column``: a ``ContainerEvent``
        gets ``xxhash64(container_id)`` (one id per container), all other event types get
        the timestamp-based hash. The container-event case is applied per row (keyed on
        ``stats_name``) since a frame may mix event types.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the event_instance_id column to a DataFrame.
        """
        from impulse_reporting.events.container_event import ContainerEvent

        def _(df: DataFrame) -> DataFrame:
            container_event_stats_names = [
                agg.get_name()
                for agg in aggregations
                if agg and isinstance(agg.get_event(), ContainerEvent)
            ]

            timestamp_based_id = generate_event_instance_id_column()
            if container_event_stats_names:
                event_instance_id_column = f.when(
                    f.col("stats_name").isin(container_event_stats_names),
                    generate_event_instance_id_column(event_type=ContainerEvent),
                ).otherwise(timestamp_based_id)
            else:
                event_instance_id_column = timestamp_based_id

            return df.withColumn("event_instance_id", event_instance_id_column)

        return _

    @staticmethod
    def _add_visual_id_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., DataFrame]:
        """
        Add a visual_id column to the DataFrame based on the provided visuals.

        Parameters
        ----------
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        function
            Function that adds the visual_id column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            visual_id_column = StatsAggregator.get_visual_id_column(aggregations, "stats_name")
            return df.withColumn("visual_id", visual_id_column)

        return _

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, stats_aggregators: list[StatsAggregator]
    ) -> DataFrame:
        """
        Create a metadata DataFrame for the provided StatsAggregator aggregations.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for DataFrame creation.
        stats_aggregators : list of StatsAggregator
            List of StatsAggregator aggregations.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing metadata for the stats aggregations.
        """
        stats_rows = [stats_agg.as_spark_row() for stats_agg in stats_aggregators]
        return spark.createDataFrame(stats_rows, schema=STATS_AGGREGATOR_DIMENSION_SCHEMA)

    def determine_definition_hash(self) -> int:
        """
        Calculate definition hash for stats aggregator.

        Only includes computation-affecting attributes:
        - input_expressions
        - statistics to be calculated
        - event expression if there is any
        - channel_names, and each cross-channel descriptor's channel_name. These
          are the fact table's ``channel_name`` merge key, so a rename must force
          a recompute (a changed definition recomputes and prunes all containers);
          otherwise, in incremental mode, already-processed containers would keep
          rows under the old name.
        - custom statistics (labels, kind, declared input indices, params, and
          function bytecode, so implementation or input-wiring changes invalidate
          cached results; only appended when custom statistics are configured)

        Excludes: name, desc, units, page_number, report_id.

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        # Build hash input from result-affecting attributes only
        event_expr_str = (
            self.event.get_expression().__str__()
            if self.event and self.event.get_expression()
            else ""
        )

        input_expr_strs = ",".join([expr.__str__() for expr in self.input_expressions])
        stats_strs = ",".join(self.statistics)

        hash_components = [
            input_expr_strs,  # Input expressions
            stats_strs,  # statistics aggregation types
            event_expr_str,  # Event expression
            repr(self.channel_names),  # fact-table channel_name merge key
        ]

        custom_fingerprints = [
            self._fingerprint_custom_statistic(
                "per_channel",
                statistic.aggregation_labels,
                statistic.func,
                inputs_repr="",
                params=statistic.params,
            )
            for statistic in sorted(
                self.per_channel_custom_statistics, key=lambda s: tuple(s.aggregation_labels)
            )
        ]
        for statistic in sorted(
            self.cross_channel_custom_statistics, key=lambda s: tuple(s.aggregation_labels)
        ):
            if statistic.inputs is None:
                inputs_repr = "all"
            else:
                # indices, not names: consistent renames keep the hash stable
                inputs_repr = repr([self.channel_names.index(ch) for ch in statistic.inputs])
            custom_fingerprints.append(
                self._fingerprint_custom_statistic(
                    "cross_channel",
                    statistic.aggregation_labels,
                    statistic.func,
                    inputs_repr=inputs_repr,
                    params=statistic.params,
                    channel_name=statistic.channel_name,
                )
            )
        if custom_fingerprints:
            hash_components.append("|".join(custom_fingerprints))

        hash_input = "::".join(hash_components)

        # Use SHA-256 and return as int (truncated to fit LongType)
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    @staticmethod
    def _fingerprint_custom_statistic(
        kind: str,
        aggregation_labels: list[str],
        func: Callable,
        inputs_repr: str,
        params: dict | None = None,
        channel_name: str | None = None,
    ) -> str:
        """
        Build a stable fingerprint for a custom statistic.

        The fingerprint covers the statistic's kind, output labels, declared input
        indices, provisioned params, the fact-table ``channel_name`` (cross-channel
        only), and the function's bytecode, constants, and default argument values
        (``__defaults__`` / ``__kwdefaults__``). ``functools.partial`` wrappers are
        unwrapped with their bound arguments included, so changed parameters change
        the fingerprint. Note that bytecode hashing does not detect changes inside
        helper functions called by the statistic or in captured closure variables,
        and is sensitive to the Python version. Callables without ``__code__`` fall
        back to a labels-only fingerprint.

        Parameters
        ----------
        kind : str
            Either ``per_channel`` or ``cross_channel``.
        aggregation_labels : list of str
            The statistic's output labels (its identity).
        func : Callable
            The statistic's callable.
        inputs_repr : str
            Representation of the declared input indices.
        params : dict, optional
            The statistic's provisioned params.
        channel_name : str, optional
            The cross-channel descriptor's ``channel_name`` (the fact-table merge
            key); ``None`` for per-channel statistics and cross-channel statistics
            that default to their label.

        Returns
        -------
        str
            The fingerprint string.
        """
        params_repr = repr(sorted((params or {}).items()))
        partial_reprs = []
        while isinstance(func, functools.partial):
            partial_reprs.append(f"{func.args!r}:{sorted((func.keywords or {}).items())!r}")
            func = func.func
        code = getattr(func, "__code__", None)
        if code is None:
            digest = "no-code"
        else:
            defaults_repr = repr(func.__defaults__)
            kwdefaults_repr = repr(sorted((func.__kwdefaults__ or {}).items()))
            digest = hashlib.sha256(
                code.co_code
                + repr(code.co_consts).encode()
                + defaults_repr.encode()
                + kwdefaults_repr.encode()
                + "|".join(partial_reprs).encode()
            ).hexdigest()
        return (
            f"{kind}:{aggregation_labels!r}:{inputs_repr}:{params_repr}:{channel_name!r}:{digest}"
        )
