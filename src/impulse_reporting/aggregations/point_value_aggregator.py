"""PointValueAggregator reporting class: sample channels at PointsInTimeEvent instants."""

from __future__ import annotations

import hashlib

import pyspark.sql.functions as f
import zlib
from pyspark.sql import DataFrame, Row, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator as QueryEnginePointValueAggregator,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.model.series.sample_series import SampleSeries
from impulse_reporting.aggregations.aggregation import Aggregation
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.events.event import Event
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from impulse_reporting.persist.dimension_schema import STATS_AGGREGATOR_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import STATS_AGGREGATOR_FACT_SCHEMA


class PointValueAggregator(Aggregation):
    """Reporting aggregation that samples channels at the instants of a PointsInTimeEvent.

    Each input expression (a ``SampleSeries``) is sampled at every instant of the
    ``event`` (a :class:`PointsInTimeEvent`). One fact row is produced per (channel,
    instant) with the sampled value, written to the shared ``stats_aggregator_fact``
    table. The ``aggregation_label`` is always ``"value"`` and each row's
    ``event_instance_id`` matches the corresponding zero-duration instance materialized
    by the ``PointsInTimeEvent``.
    """

    def __init__(
        self,
        name: str,
        input_expressions: list[TimeSeriesExpression],
        channel_names: list[str],
        event: PointsInTimeEvent,
        desc: str = None,
        agg_type: str = "point_value_aggregator",
        values_unit: str = None,
    ):
        """
        Initialize a PointValueAggregator object.

        Parameters
        ----------
        name : str
            Name of the aggregation.
        input_expressions : list of TimeSeriesExpression
            Channel expressions to sample. Each must evaluate to a ``SampleSeries``.
        channel_names : list of str
            Display names for the signals. Must be the same length as input_expressions.
        event : PointsInTimeEvent
            Event whose instants define where the channels are sampled. Required; its
            expression must evaluate to ``PointsInTime``.
        desc : str, optional
            Description of the aggregation.
        agg_type : str, optional
            Type of aggregation, defaults to "point_value_aggregator".
        values_unit : str, optional
            Unit of the sampled values.
        """
        Aggregation.__init__(self, name)
        self.input_expressions = input_expressions
        self.channel_names = channel_names
        self._validate_channel_names()
        for expr in self.input_expressions:
            expr.require_evaluation_type(
                SampleSeries, owner="PointValueAggregator", example="a channel selection"
            )
        if not isinstance(event, PointsInTimeEvent):
            raise ValueError(
                "PointValueAggregator requires a PointsInTimeEvent as 'event' (its expression "
                f"must evaluate to PointsInTime); got {type(event).__name__}"
            )
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
        Get a unique identifier for the aggregation.

        Returns
        -------
        int
            Unique identifier for the aggregation.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF

    def get_event(self) -> Event:
        """
        Get the event associated with the aggregation.

        Returns
        -------
        Event
            The event associated with the aggregation.
        """
        return self.event

    def get_expression(self) -> TimeSeriesExpression:
        """
        Get the time series expression for the aggregation.

        Returns
        -------
        TimeSeriesExpression
            The time series expression for the aggregation.
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
        Set the expression for the aggregation.

        Wraps the inputs and the event's points into a query-engine
        PointValueAggregator.

        Returns
        -------
        TimeSeriesExpression
            The configured point-value aggregation expression.
        """
        if not self.input_expressions or len(self.input_expressions) == 0:
            raise ValueError("At least one input expression is required")

        query_eng_agg = QueryEnginePointValueAggregator(
            input_expressions=self.input_expressions,
            event_expression=self.event.get_expression(),
        ).alias(self.name)

        return query_eng_agg

    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the aggregation.

        Returns
        -------
        dict
            Dictionary containing aggregation metadata. Reuses the stats-aggregator
            dimension schema; ``statistics`` holds the single pseudo-statistic
            ``"value"`` and ``agg_type`` distinguishes these rows.
        """
        return {
            "visual_id": self.get_id(),
            "report_id": self.report_id,
            "name": self.name,
            "page_number": self.page_number,
            "description": self.desc,
            "agg_type": self.agg_type if self.agg_type else "point_value_aggregator",
            "statistics": ["value"],
            "channel_names": self.channel_names,
            "signal_expressions": [expr.__str__() for expr in self.input_expressions],
            "values_unit": self.values_unit,
            "definition_hash": self.determine_definition_hash(),
        }

    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the aggregation.

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
        aggregations: list[PointValueAggregator],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Determine and process aggregations for a list of PointValueAggregator visuals.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for computation.
        aggregations : list of PointValueAggregator
            List of PointValueAggregator visual aggregations.
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
            DataFrame of fact rows matching ``STATS_AGGREGATOR_FACT_SCHEMA``.
        """
        if solved_df is None:
            raise ValueError(
                "PointValueAggregator.determine_aggregations requires solved_df. "
                "Provide a pre-solved DataFrame from the centralized batch-solve flow."
            )

        agg_names = [agg.get_name() for agg in aggregations]

        result = solved_df.select("container_id", *agg_names)

        df = (
            result.transform(cls._unpivot_measurement_info(agg_names))
            .transform(cls._extract_point_info)
            .transform(StatsAggregator._add_event_id_column(aggregations))
            .transform(StatsAggregator._add_event_name_column(aggregations))
            .transform(cls._explode_point_values)
            .transform(StatsAggregator._add_channel_name_column(aggregations))
            .transform(StatsAggregator._add_event_instance_id_column)
            .transform(StatsAggregator._add_visual_id_column(aggregations))
            .select(STATS_AGGREGATOR_FACT_SCHEMA.fieldNames())
        )
        return df

    @staticmethod
    def _unpivot_measurement_info(agg_names: list[str]):
        """
        Unpivot the aggregation result columns into long format.

        The variable column is named ``stats_name`` so the shared StatsAggregator
        column helpers (event_id / event_name / channel_name / visual_id) can be reused.
        """

        def _(df: DataFrame) -> DataFrame:
            return df.unpivot(
                f.col("container_id"),
                agg_names,
                variableColumnName="stats_name",
                valueColumnName="value",
            )

        return _

    @staticmethod
    def _extract_point_info(df: DataFrame) -> DataFrame:
        """
        Extract the per-series point timestamps and values from the struct column.
        """
        return df.withColumn("point_timestamps", f.col("value.point_timestamps")).withColumn(
            "values", f.col("value.values")
        )

    @staticmethod
    def _explode_point_values(df: DataFrame) -> DataFrame:
        """
        Explode the per-series point arrays into one row per (signal, point).

        Each point becomes a zero-duration row (``start_ts == end_ts``) with the constant
        ``aggregation_label`` ``"value"`` and ``statistic_value`` set to the sampled value.
        """
        # Step 1: explode by signal index — point_timestamps and values are both
        # indexed [signal][point]; zip and explode to get one row per signal.
        df_with_signal = df.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            f.posexplode(f.arrays_zip(f.col("point_timestamps"), f.col("values"))).alias(
                "signal_index", "signal_zip"
            ),
        )

        # Step 2: explode by point — zip this signal's timestamps with its values.
        df_with_point = df_with_signal.select(
            "container_id",
            "stats_name",
            "event_id",
            "event_name",
            "signal_index",
            f.posexplode(
                f.arrays_zip(f.col("signal_zip.point_timestamps"), f.col("signal_zip.values"))
            ).alias("point_index", "point_zip"),
        )

        # Step 3: a point is a zero-duration instance (start_ts == end_ts); the single
        # value lands as statistic_value under the constant label "value".
        return df_with_point.select(
            "container_id",
            "stats_name",
            "event_name",
            "event_id",
            "signal_index",
            f.col("point_zip.point_timestamps").cast("long").alias("start_ts"),
            f.col("point_zip.point_timestamps").cast("long").alias("end_ts"),
            f.lit("value").alias("aggregation_label"),
            f.col("point_zip.values").alias("statistic_value"),
        )

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, aggregations: list[PointValueAggregator]
    ) -> DataFrame:
        """
        Create a metadata DataFrame for the provided PointValueAggregator aggregations.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for DataFrame creation.
        aggregations : list of PointValueAggregator
            List of PointValueAggregator aggregations.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing metadata for the aggregations.
        """
        rows = [agg.as_spark_row() for agg in aggregations]
        return spark.createDataFrame(rows, schema=STATS_AGGREGATOR_DIMENSION_SCHEMA)

    def determine_definition_hash(self) -> int:
        """
        Calculate the definition hash for the aggregation.

        Only includes computation-affecting attributes: input expressions and the event
        expression. Excludes name, description, units, page_number, report_id.

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        event_expr_str = (
            self.event.get_expression().__str__()
            if self.event and self.event.get_expression()
            else ""
        )

        input_expr_strs = ",".join([expr.__str__() for expr in self.input_expressions])

        hash_components = [
            input_expr_strs,  # Input expressions
            "value",  # constant aggregation label
            event_expr_str,  # Event (points) expression
        ]
        hash_input = "::".join(hash_components)

        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)
