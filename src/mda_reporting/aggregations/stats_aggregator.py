"""StatsAggregator reporting class for computing statistics within event intervals."""

from __future__ import annotations

import hashlib
from collections.abc import Callable

import pyspark.sql.functions as f
import zlib
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StringType,
)

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from mda_query_engine.analyze.query.aggregations.stats_aggregator import (
    StatsAggregator as QueryEngineStatsAggregator,
)
from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.aggregations.aggregation import Aggregation
from mda_reporting.events.event import Event
from mda_reporting.persist.dimension_schema import STATS_AGGREGATOR_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import STATS_AGGREGATOR_FACT_SCHEMA
from mda_reporting.util.event_instance_util import generate_event_instance_id_column
from mda_reporting.util.report_entity_util import ReportEntityUtil


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
        """
        Aggregation.__init__(self, name)
        self.input_expressions = input_expressions
        self.channel_names = channel_names
        self._validate_channel_names()
        self.statistics = statistics
        self.event = event
        self.desc = desc
        self.agg_type = agg_type
        self.values_unit = values_unit
        self.expression = self._set_expression()

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
        ).alias(self.name)

        return query_eng_stats_agg

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
            "statistics": self.statistics,
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
        query: QueryBuilder,
        solver: QuerySolver,
        aggregations: list[StatsAggregator],
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Determine and process aggregations for a list of StatsAggregator visuals.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for computation.
        query : QueryBuilder
            Query builder for constructing the query.
        solver : QuerySolver
            Solver for executing the query.
        aggregations : list of StatsAggregator
            List of StatsAggregator visual aggregations.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing the processed stats aggregations.
        """
        stats_expressions = []
        stats_names = []
        for stats_agg in aggregations:
            stats_expressions.append(stats_agg.get_expression())
            stats_names.append(stats_agg.get_name())

        stats_query = query.select(*stats_expressions)
        result = stats_query.solve(
            spark=spark,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

        df = (
            result.transform(StatsAggregator._unpivot_measurement_info(stats_names))
            .transform(StatsAggregator._extract_stats_info)
            .transform(StatsAggregator._add_event_id_column(aggregations))
            .transform(StatsAggregator._add_event_name_column(aggregations))
            .transform(StatsAggregator._explode_stats_values)
            .transform(StatsAggregator._add_channel_name_column(aggregations))
            .transform(StatsAggregator._add_event_instance_id_column)
            .transform(StatsAggregator._add_visual_id_column(aggregations))
            .select(STATS_AGGREGATOR_FACT_SCHEMA.fieldNames())
        )
        return df

    @staticmethod
    def _unpivot_measurement_info(stats_names: list[str]) -> Callable[..., "DataFrame"]:
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
        )

    @staticmethod
    def _add_event_id_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., "DataFrame"]:
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
    ) -> Callable[..., "DataFrame"]:
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
        Explode the statistics values into individual rows per signal and interval.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing nested statistics values.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with exploded statistics for each signal and interval.
        """
        # Step 1: Explode by signal index to get one row per signal
        # numeric_values is array of arrays: [[{stats for interval 0}, {stats for interval 1}], ...]
        # Each outer array element corresponds to a signal
        df_with_signal = df.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            "event_timestamps",
            f.posexplode(f.col("numeric_values")).alias(
                "signal_index", "signal_stats_per_interval"
            ),
        )

        # Step 2: Explode by interval - zip event_timestamps with signal_stats_per_interval
        # event_timestamps: [[start, end], [start, end], ...]
        # signal_stats_per_interval: [{stats}, {stats}, ...]
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
    def _add_channel_name_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., "DataFrame"]:
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
    def _add_event_instance_id_column(df: DataFrame) -> DataFrame:
        """
        Add an event_instance_id column to the DataFrame.

        The event_instance_id uniquely identifies each event interval instance
        within a container and event combination.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing interval_index column.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with event_instance_id column added.
        """
        return df.withColumn("event_instance_id", generate_event_instance_id_column())

    @staticmethod
    def _add_visual_id_column(
        aggregations: list[StatsAggregator],
    ) -> Callable[..., "DataFrame"]:
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

        Excludes: name, desc, signal_name, units, page_number, report_id

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
        ]
        hash_input = "::".join(hash_components)

        # Use SHA-256 and return as int (truncated to fit LongType)
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)
