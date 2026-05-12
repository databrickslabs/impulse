from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod

import pyspark.sql.functions as f
import zlib
from pyspark.sql import Row, SparkSession

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.aggregations.aggregation import Aggregation
from mda_reporting.events.event import Event
from mda_reporting.persist.dimension_schema import HISTOGRAM_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import HISTOGRAM_FACT_SCHEMA
from mda_reporting.util.report_entity_util import ReportEntityUtil


class Histogram(Aggregation, ABC):
    """Class representing a histogram aggregation in a report."""

    def __init__(
        self,
        name: str,
        base_expr: TimeSeriesExpression,
        bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = None,
        channel_name: str = None,
        values_unit: str = None,
        bins_unit: str = None,
    ):
        """
        Initialize a Histogram object.

        Parameters
        ----------
        name : str
            Name of the histogram aggregation.
        base_expr : TimeSeriesExpression
            Base time series expression to compute the histogram from.
        bins : list of float
            List of bin edges for the histogram.
        event : Event, optional
            Optional event to filter the base expression.
        desc : str, optional
            Description of the histogram.
        agg_type : str, optional
            Type of aggregation, defaults to "NA".
        channel_name : str, optional
            Name of the signal associated with the histogram.
        values_unit : str, optional
            Unit of the histogram values.
        bins_unit : str, optional
            Unit of the histogram bins.
        """
        Aggregation.__init__(self, name)
        self.base_expr = base_expr
        self.bins = bins
        self.event = event
        self.desc = desc
        self.agg_type = agg_type
        self.channel_name = channel_name
        self.values_unit = values_unit
        self.bins_unit = bins_unit

    def get_id(self) -> int:
        """
        Get a unique identifier for the histogram aggregation.

        Returns
        -------
        int
            Unique identifier for the histogram aggregation.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF  # Ensures positive 32-bit int

    def get_event(self) -> Event:
        """
        Get the event associated with the histogram.
        Returns
        -------
        Event
            The event associated with the histogram, or None if not set.
        """

        return self.event

    def get_expression(self) -> TimeSeriesExpression:
        """
        Get the time series expression for the histogram aggregation.
        Returns
        -------
        TimeSeriesExpression
            The time series expression for the histogram aggregation.
        """
        return self.expression

    def get_expression_str(self) -> str:
        """
        Get a string representation of the time series expression for the histogram aggregation.
        Returns
        -------
        str
            String representation of the time series expression for the histogram aggregation.
        """
        if isinstance(self.expression, TimeSeriesExpression):
            return self.expression.__str__()
        else:
            return "NA"

    def determine_definition_hash(self) -> int:
        """
        Calculate definition hash for histogram.

        Only includes computation-affecting attributes:
        - base_expr: The expression being histogrammed
        - bins: Bin edges
        - event expression: Event filter (if present)
        - weights expression: Custom weights (if present, e.g. HistogramCustomWeights)

        Excludes: name, desc, signal_name, units, page_number, report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        # Build hash input from result-affecting attributes only
        event_expr_str = (
            self.event.get_expression().__str__() if self.event and self.event.__str__() else ""
        )
        weights_expr = getattr(self, "weights_expr", None)
        weights_expr_str = weights_expr.__str__() if weights_expr else ""
        hash_components = [
            str(self.base_expr),  # The expression being histogrammed
            str(sorted(self.bins)),  # Bin edges (sorted for consistency)
            event_expr_str,  # Event filter expression
            weights_expr_str,  # Weights expression
        ]
        hash_input = "::".join(hash_components)

        # Use SHA-256 and return as int (truncated to fit LongType)
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the histogram aggregation.

        Returns
        -------
        dict
            Dictionary containing histohram aggregation metadata.
        """
        return {
            "visual_id": self.get_id(),
            "report_id": self.report_id,
            "name": self.name,
            "page_number": self.page_number,
            "description": self.desc,
            "agg_type": self.agg_type if self.agg_type else "NA",
            "bins": self.bins,
            "channel_name": self.channel_name,
            "signal_expression": self.base_expr.__str__(),
            "weights_channel_name": None,
            "weights_expression": None,
            "values_unit": self.values_unit,
            "bins_unit": self.bins_unit,
            "definition_hash": self.determine_definition_hash(),
        }

    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the histogram aggregation.

        Returns
        -------
        Row
            Spark Row containing histogram aggregation metadata.
        """
        return Row(**self.as_dict())

    @classmethod
    def determine_aggregations(
        cls,
        spark: SparkSession,
        aggregations: list[Histogram],
        *,
        solved_df: "DataFrame" = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df=None,
    ):
        """
        Determine histogram aggregation instances and return a Spark DataFrame.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        aggregations : list of Histogram
            List of Histogram objects to process.
        solved_df : DataFrame, optional
            Pre-solved wide DataFrame from centralized batch solve. Required.
        query : QueryBuilder, optional
            Query builder (unused, kept for interface compatibility).
        solver : QuerySolver, optional
            Query solver (unused, kept for interface compatibility).
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers (unused, kept for interface compatibility).

        Returns
        -------
        DataFrame
            Spark DataFrame containing histogram aggregation facts.
        """
        if solved_df is None:
            raise ValueError(
                "Histogram.determine_aggregations requires solved_df. "
                "Provide a pre-solved DataFrame from the centralized batch-solve flow."
            )

        hist_names = [hist.get_name() for hist in aggregations]

        df = (
            solved_df.select("container_id", *hist_names)
            .unpivot(
                f.col("container_id"),
                hist_names,
                variableColumnName="hist_name",
                valueColumnName="value",
            )
            .withColumn("hist_values", f.col("value.H"))
            .withColumn("hist_bins", f.col("value.bin_edges"))
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(
                    elements=aggregations, element_name="hist_name"
                ),
            )
            .select(
                "container_id",
                "hist_name",
                "event_id",
                "hist_bins",
                f.posexplode(f.col("hist_values")).alias("bin_ID", "hist_value"),
            )
            .withColumn("lower_bound", f.col("hist_bins").getItem(f.col("bin_ID")))
            .withColumn("upper_bound", f.col("hist_bins").getItem(f.col("bin_ID") + 1))
            .withColumn("bin_name", f.concat_ws("-", "lower_bound", "upper_bound"))
            .withColumn("visual_id", Histogram.get_visual_id_column(aggregations, "hist_name"))
            .select(HISTOGRAM_FACT_SCHEMA.fieldNames())
        )
        return df

    @classmethod
    def determine_metadata_df(cls, spark: SparkSession, histograms: list[Histogram]):
        """
        Create a Spark DataFrame containing histogram aggregation metadata.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        histograms : list of Histogram
            List of Histogram objects.

        Returns
        -------
        DataFrame
            Spark DataFrame containing histogram aggregation metadata.
        """
        histograms = [hist.as_spark_row() for hist in histograms]
        return spark.createDataFrame(histograms, schema=HISTOGRAM_DIMENSION_SCHEMA)

    @abstractmethod
    def _set_expression(self) -> TimeSeriesExpression:
        """sets the expression for the histogram based on the base expression and event."""
        pass


class HistogramDuration(Histogram):
    """Class representing a histogram duration aggregation in a report."""

    def __init__(
        self,
        name: str,
        base_expr: TimeSeriesExpression,
        bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = "histogram_duration",
        channel_name: str = None,
        values_unit: str = None,
        bins_unit: str = None,
    ):
        """
        Initialize a HistogramDuration object.

        Parameters
        ----------
        name : str
            Name of the histogram duration aggregation.
        base_expr : TimeSeriesExpression
            Base time series expression to compute the histogram duration from.
        bins : list of float
            List of bin edges for the histogram duration.
        event : Event, optional
            Optional event to filter the base expression.
        desc : str, optional
            Description of the histogram duration.
        agg_type : str, optional
            Type of aggregation, defaults to "histogram_duration".
        channel_name : str, optional
            Name of the signal associated with the histogram duration.
        values_unit : str, optional
            Unit of the histogram duration values.
        bins_unit : str, optional
            Unit of the histogram duration bins.
        """
        super().__init__(
            name,
            base_expr,
            bins,
            event,
            desc,
            agg_type,
            channel_name,
            values_unit,
            bins_unit,
        )
        self.expression = self._set_expression()

    def _set_expression(self) -> TimeSeriesExpression:
        event_expression = self.event.get_expression() if self.event else None
        expression = self.base_expr.where(event_expression) if event_expression else self.base_expr
        return expression.histogram(self.bins).alias(self.name)


class HistogramCustomWeights(Histogram):
    """Class representing a histogram with a custom weight in a report."""

    def __init__(
        self,
        name: str,
        base_expr: TimeSeriesExpression,
        weights_expr: TimeSeriesExpression,
        bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = "histogram_custom_weights",
        channel_name: str = None,
        weights_channel_name: str = None,
        values_unit: str = None,
        bins_unit: str = None,
        channel_interp_kind: str = "previous",
        weights_interp_kind: str = "previous",
        math_fct_for_weights: str = None,
        math_fct_kwargs: dict = None,
        weight_type: str = None,
    ):
        """
        Initialize a HistogramCustomWeights object.

        Parameters
        ----------
        name : str
            Name of the custom histogram aggregation.
        base_expr : TimeSeriesExpression
            Base time series expression to compute the histogram from.
        weights_expr : TimeSeriesExpression
            Time series expression to use as custom weights for the histogram.
        bins : list of float
            List of bin edges for the histogram.
        event : Event, optional
            Optional event to filter the base expression.
        desc : str, optional
            Description of the histogram.
        agg_type : str, optional
            Type of aggregation, defaults to "histogram_custom_weights".
        channel_name : str, optional
            Name of the signal associated with the histogram.
        weights_channel_name : str, optional
            Name of the weights signal associated with the histogram.
        values_unit : str, optional
            Unit of the histogram values.
        bins_unit : str, optional
            Unit of the histogram bins.
        channel_interp_kind : str, optional
            Interpolation method for the channel values (default is 'previous').
        weights_interp_kind : str, optional
            Interpolation method for the weights (default is 'previous').
        math_fct_for_weights : callable, optional
            Optional function to apply to the weights before aggregation.
        math_fct_kwargs : dict, optional
            Additional keyword arguments to pass to the math function for weights (default is {}).
        weight_type: str, optional
            If the custom weighted signal is required to be weighted with time, it must be provided as 'time'.
            By default it is set to None, this option is provided to prevent errors from RLE compression method of the channels.
        """
        super().__init__(
            name,
            base_expr,
            bins,
            event,
            desc,
            agg_type,
            channel_name,
            values_unit,
            bins_unit,
        )
        self.weights_expr = weights_expr
        self.weights_channel_name = weights_channel_name
        self.channel_interp_kind = channel_interp_kind
        self.weights_interp_kind = weights_interp_kind
        self.math_fct_for_weights = math_fct_for_weights
        self.math_fct_kwargs = math_fct_kwargs
        self.weight_type = weight_type
        self.expression = self._set_expression()

    def _set_expression(self) -> TimeSeriesExpression:
        event_expression = self.event.get_expression() if self.event else None
        expression = self.base_expr.where(event_expression) if event_expression else self.base_expr
        weights_expression = (
            self.weights_expr.where(event_expression) if event_expression else self.weights_expr
        )
        return expression.histogram_custom_weights(
            bins=self.bins,
            weights=weights_expression,
            channel_interp_kind=self.channel_interp_kind,
            weights_interp_kind=self.weights_interp_kind,
            math_fct_for_weights=self.math_fct_for_weights,
            math_fct_kwargs=self.math_fct_kwargs,
            weight_type=self.weight_type,
        ).alias(self.name)

    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the histogram aggregation.

        Returns
        -------
        dict
            Dictionary containing histogram aggregation metadata.
        """
        return {
            "visual_id": self.get_id(),
            "report_id": self.report_id,
            "name": self.name,
            "page_number": self.page_number,
            "description": self.desc,
            "agg_type": self.agg_type if self.agg_type else "NA",
            "bins": self.bins,
            "channel_name": self.channel_name,
            "signal_expression": self.base_expr.__str__(),
            "weights_channel_name": self.weights_channel_name,
            "weights_expression": (self.weights_expr.__str__() if self.weights_expr else None),
            "values_unit": self.values_unit,
            "bins_unit": self.bins_unit,
            "definition_hash": self.determine_definition_hash(),
        }


class HistogramDistance(HistogramCustomWeights):
    def __init__(
        self,
        name,
        base_expr,
        weights_expr,
        bins,
        event=None,
        desc=None,
        channel_name=None,
        weights_channel_name=None,
        values_unit=None,
        bins_unit=None,
    ):
        agg_type = "histogram_distance"
        math_fct_for_weights = "diff"
        channel_interp_kind = "previous"
        weights_interp_kind = "previous"
        # TODO: once linear interp implementation is done change weights_interp_kind to 'linear'

        super().__init__(
            name,
            base_expr,
            weights_expr,
            bins,
            event,
            desc,
            agg_type,
            channel_name,
            weights_channel_name,
            values_unit,
            bins_unit,
            channel_interp_kind,
            weights_interp_kind,
            math_fct_for_weights,
            None,
            None,
        )
