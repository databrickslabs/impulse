from __future__ import annotations

import hashlib
import zlib
from abc import ABC, abstractmethod
from collections.abc import Callable

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.aggregations.aggregation import Aggregation
from impulse_reporting.events.event import Event
from impulse_reporting.persist.dimension_schema import HISTOGRAM2D_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import HISTOGRAM2D_FACT_SCHEMA
from impulse_reporting.util.report_entity_util import ReportEntityUtil


class Histogram2D(Aggregation, ABC):
    """Class representing a 2D histogram aggregation in a report."""

    def __init__(
        self,
        name: str,
        x_expr: TimeSeriesExpression,
        y_expr: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = None,
        x_channel_name: str = None,
        y_channel_name: str = None,
        values_unit: str = None,
        x_bins_unit: str = None,
        y_bins_unit: str = None,
    ):
        """
        Initialize a Histogram2D object.
        Parameters
        ----------
        name : str
            Name of the histogram aggregation.
        x_expr : TimeSeriesExpression
            Time series expression for the x-axis.
        y_expr : TimeSeriesExpression
            Time series expression for the y-axis.
        x_bins : list of float
            List of bin edges for the x-axis.
        y_bins : list of float
            List of bin edges for the y-axis.
        event : Event, optional
            Optional event to filter the expressions.
        desc : str, optional
            Description of the histogram.
        agg_type : str, optional
            Type of aggregation.
        x_channel_name : str, optional
            Name of the signal associated with the x-axis.
        y_channel_name : str, optional
            Name of the signal associated with the y-axis.
        values_unit : str, optional
            Unit of the histogram values.
        x_bins_unit : str, optional
            Unit of the x-axis bins.
        y_bins_unit : str, optional
            Unit of the y-axis bins.
        """
        Aggregation.__init__(self, name)
        self.x_expr = x_expr
        self.y_expr = y_expr
        self.x_bins = x_bins
        self.y_bins = y_bins
        self.event = event
        self.desc = desc
        self.agg_type = agg_type if agg_type else "histogram_duration"
        self.x_channel_name = x_channel_name
        self.y_channel_name = y_channel_name
        self.values_unit = values_unit
        self.x_bins_unit = x_bins_unit
        self.y_bins_unit = y_bins_unit

    def get_id(self) -> int:
        """
        Returns a unique identifier for the histogram2d aggregation.
        Returns
        -------
        int
            Unique identifier for the histogram2d aggregation.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF  # Ensures positive 32-bit int

    def get_event(self) -> Event:
        """
        Get the event associated with the histogram2d aggregation.
        Returns
        -------
        Event
            The event associated with the histogram2d aggregation.
        """
        return self.event

    def get_expression(self) -> TimeSeriesExpression:
        """
        Get the time series expression for the histogram2d aggregation.
        Returns
        -------
        TimeSeriesExpression
            The time series expression for the histogram2d aggregation.
        """
        return self.expression

    def get_expression_str(self) -> str:
        """
        Get a string representation of the time series expression for the histogram2d aggregation.
        Returns
        -------
        str
            String representation of the time series expression for the histogram2d aggregation.
        """
        if isinstance(self.expression, TimeSeriesExpression):
            return self.expression.__str__()
        else:
            return "NA"

    def determine_definition_hash(self) -> int:
        """
        Calculate definition hash for histogram2d.

        Only includes computation-affecting attributes:
        - x_expr: The x-axis expression
        - y_expr: The y-axis expression
        - x_bins: X-axis bin edges
        - y_bins: Y-axis bin edges
        - event expression: Event filter (if present)
        - weights expression: Custom weights (if present, e.g. Histogram2DCustomWeights)

        Excludes: name, desc, signal names, units, page_number, report_id

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
        weights_expr = getattr(self, "weights_expr", None)
        weights_expr_str = (
            weights_expr.__str__() if weights_expr and weights_expr.__str__() else ""
        )
        hash_components = [
            str(self.x_expr),  # X-axis expression
            str(self.y_expr),  # Y-axis expression
            str(sorted(self.x_bins)),  # X-axis bin edges (sorted for consistency)
            str(sorted(self.y_bins)),  # Y-axis bin edges (sorted for consistency)
            event_expr_str,  # Event filter expression
            weights_expr_str,  # Weights expression
        ]
        hash_input = "::".join(hash_components)

        # Use SHA-256 and return as int (truncated to fit LongType)
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """
        Convert the histogram2d aggregation to a dictionary representation.

        Returns
        -------
        dict
            Dictionary containing all relevant attributes of the histogram aggregation.
        """
        return {
            "visual_id": self.get_id(),
            "report_id": self.report_id,
            "page_number": self.page_number,
            "name": self.name,
            "description": self.desc,
            "agg_type": self.agg_type,
            "x_bins": self.x_bins,
            "y_bins": self.y_bins,
            "x_channel_name": self.x_channel_name,
            "x_signal_expression": self.x_expr.__str__(),
            "y_channel_name": self.y_channel_name,
            "y_signal_expression": self.y_expr.__str__(),
            "weights_channel_name": None,
            "weights_expression": None,
            "values_unit": self.values_unit,
            "x_bins_unit": self.x_bins_unit,
            "y_bins_unit": self.y_bins_unit,
            "definition_hash": self.determine_definition_hash(),
        }

    def as_spark_row(self) -> Row:
        """
        Convert the histogram2d aggregation to a Spark Row.

        Returns
        -------
        pyspark.sql.Row
            Spark Row containing the histogram aggregation data.
        """
        return Row(**self.as_dict())

    @classmethod
    def determine_aggregations(
        cls,
        spark: SparkSession,
        aggregations: list[Histogram2D],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Determine and process aggregations for a list of Histogram2D visuals.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for computation.
        aggregations : list of Histogram2D
            List of Histogram2D visual aggregations.
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
            DataFrame containing the processed histogram2d aggregations.
        """
        if solved_df is None:
            raise ValueError(
                "Histogram2D.determine_aggregations requires solved_df. "
                "Provide a pre-solved DataFrame from the centralized batch-solve flow."
            )

        hist_names = [hist.get_name() for hist in aggregations]

        result = solved_df.select("container_id", *hist_names)

        df = (
            result.transform(Histogram2D._unpivot_measurement_info(hist_names))
            .transform(Histogram2D._extract_histogram2d_info)
            .transform(Histogram2D._add_event_id_column(aggregations))
            .transform(Histogram2D._explode_histogram2d_values)
            .transform(Histogram2D._extract_histogram2d_bin_info)
            .transform(Histogram2D._add_visual_id_column(aggregations))
            .select(HISTOGRAM2D_FACT_SCHEMA.fieldNames())
        )
        return df

    @staticmethod
    def _add_visual_id_column(
        aggregations: list[Histogram2D],
    ) -> Callable[..., DataFrame]:
        """
        Add a visual_id column to the DataFrame based on the provided visuals.

        Parameters
        ----------
        aggregations : list of Histogram2D
            List of Histogram2D visual aggregations.

        Returns
        -------
        function
            Function that adds the visual_id column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            visual_id_column = Histogram2D.get_visual_id_column(aggregations, "hist_name")
            return df.withColumn("visual_id", visual_id_column)

        return _

    @staticmethod
    def _extract_histogram2d_bin_info(df: DataFrame) -> DataFrame:
        """
        Extract bin information for the 2D histogram from the DataFrame.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing histogram2d data.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with additional columns for bin bounds and names.
        """
        return (
            df.withColumn("x_lower_bound", f.col("x_hist_bins").getItem(f.col("x_bin_id")))
            .withColumn("y_lower_bound", f.col("y_hist_bins").getItem(f.col("y_bin_id")))
            .withColumn("x_upper_bound", f.col("x_hist_bins").getItem(f.col("x_bin_id") + 1))
            .withColumn("y_upper_bound", f.col("y_hist_bins").getItem(f.col("y_bin_id") + 1))
            .withColumn("x_bin_name", f.concat_ws("-", "x_lower_bound", "x_upper_bound"))
            .withColumn("y_bin_name", f.concat_ws("-", "y_lower_bound", "y_upper_bound"))
        )

    @staticmethod
    def _explode_histogram2d_values(df: DataFrame) -> DataFrame:
        """
        Unnest the 2D histogram values into individual bin values.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing nested histogram2d values.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with exploded histogram values for each bin.
        """
        return df.select(
            "container_id",
            "hist_name",
            "event_id",
            "x_hist_bins",
            "y_hist_bins",
            f.posexplode(f.col("hist_values")).alias("x_bin_id", "y_hist_values"),
        ).select(
            "container_id",
            "hist_name",
            "event_id",
            "x_hist_bins",
            "y_hist_bins",
            "x_bin_id",
            f.posexplode(f.col("y_hist_values")).alias("y_bin_id", "hist_value"),
        )

    @staticmethod
    def _add_event_id_column(
        aggregations: list[Histogram2D],
    ) -> Callable[..., DataFrame]:
        """
        Add an event_id column to the DataFrame based on the provided visuals.

        Parameters
        ----------
        aggregations : list of Histogram2D
            List of Histogram2D visual aggregations.

        Returns
        -------
        function
            Function that adds the event_id column to a DataFrame.
        """

        def _(df: DataFrame) -> DataFrame:
            event_id_column = ReportEntityUtil.get_event_id_column(
                elements=aggregations, element_name="hist_name"
            )
            return df.withColumn("event_id", event_id_column)

        return _

    @staticmethod
    def _extract_histogram2d_info(df: DataFrame) -> DataFrame:
        """
        Extract histogram2d values and bin edges from the struct column.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing histogram2d struct column.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with separate columns for histogram values and bin edges.
        """
        return (
            df.withColumn("hist_values", f.col("value.H"))
            .withColumn("x_hist_bins", f.col("value.xedges"))
            .withColumn("y_hist_bins", f.col("value.yedges"))
        )

    @staticmethod
    def _unpivot_measurement_info(hist_names: list[str]) -> Callable[..., DataFrame]:
        """
        Unpivot the measurement info columns into long format.

        Parameters
        ----------
        hist_names : list of str
            List of histogram names to unpivot.

        Returns
        -------
        function
            Function that unpivots the DataFrame columns into long format.
        """

        def _(df: DataFrame) -> DataFrame:
            return df.unpivot(
                f.col("container_id"),
                hist_names,
                variableColumnName="hist_name",
                valueColumnName="value",
            )

        return _

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, histograms: list[Histogram2D]
    ) -> DataFrame:
        """
        Create a metadata DataFrame for the provided Histogram2D aggregations.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession
            Spark session to use for DataFrame creation.
        histograms : list of Histogram2D
            List of Histogram2D aggregations.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing metadata for the histogram aggregations.
        """
        histograms = [hist.as_spark_row() for hist in histograms]
        return spark.createDataFrame(histograms, schema=HISTOGRAM2D_DIMENSION_SCHEMA)

    @abstractmethod
    def _set_expression(self):
        """sets the expression for the histogram based on the base expression and event."""
        pass


class Histogram2DDuration(Histogram2D):
    """Class representing a 2D histogram aggregation in a report.
    This class uses duration series as the weight of the histogram."""

    def __init__(
        self,
        name: str,
        x_expr: TimeSeriesExpression,
        y_expr: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = "histogram_duration",
        x_channel_name: str = None,
        y_channel_name: str = None,
        values_unit: str = None,
        x_bins_unit: str = None,
        y_bins_unit: str = None,
    ):
        """
        Initialize a Histogram2D object.
        Parameters
        ----------
        name : str
            Name of the histogram aggregation.
        x_expr : TimeSeriesExpression
            Time series expression for the x-axis.
        y_expr : TimeSeriesExpression
            Time series expression for the y-axis.
        x_bins : list of float
            List of bin edges for the x-axis.
        y_bins : list of float
            List of bin edges for the y-axis.
        event : Event, optional
            Optional event to filter the expressions.
        desc : str, optional
            Description of the histogram.
        agg_type : str, optional
            Type of aggregation.
        x_channel_name : str, optional
            Name of the signal associated with the x-axis.
        y_channel_name : str, optional
            Name of the signal associated with the y-axis.
        values_unit : str, optional
            Unit of the histogram values.
        x_bins_unit : str, optional
            Unit of the x-axis bins.
        y_bins_unit : str, optional
            Unit of the y-axis bins.
        """
        super().__init__(
            name,
            x_expr,
            y_expr,
            x_bins,
            y_bins,
            event,
            desc,
            agg_type,
            x_channel_name,
            y_channel_name,
            values_unit,
            x_bins_unit,
            y_bins_unit,
        )
        self.x_expr = x_expr
        self.y_expr = y_expr
        self.x_bins = x_bins
        self.y_bins = y_bins
        self.event = event
        self.expression = self._set_expression()
        self.desc = desc
        self.agg_type = agg_type
        self.x_channel_name = x_channel_name
        self.y_channel_name = y_channel_name
        self.values_unit = values_unit
        self.x_bins_unit = x_bins_unit
        self.y_bins_unit = y_bins_unit

    def _set_expression(self) -> TimeSeriesExpression:
        """
        Determines the expression for the histogram based on the base expression and event.
        Returns
        -------
        TimeSeriesExpression
            The histogram2d based on the time series expression.
        """
        event_expression = self.event.get_expression() if self.event else None
        x_expression = self.x_expr.where(event_expression) if event_expression else self.x_expr
        y_expression = self.y_expr.where(event_expression) if event_expression else self.y_expr

        return x_expression.histogram2d(y_expression, self.x_bins, self.y_bins).alias(self.name)


class Histogram2DCustomWeights(Histogram2D):
    """Class representing a 2D histogram aggregation with custom weights in a report."""

    def __init__(
        self,
        name: str,
        x_expr: TimeSeriesExpression,
        y_expr: TimeSeriesExpression,
        weights_expr: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
        event: Event | None = None,
        desc: str = None,
        agg_type: str = "histogram2d_custom_weights",
        x_channel_name: str = None,
        y_channel_name: str = None,
        weights_channel_name: str = None,
        values_unit: str = None,
        x_bins_unit: str = None,
        y_bins_unit: str = None,
        channel_interp_kind: str = "previous",
        weights_interp_kind: str = "previous",
        math_fct_for_weights: str = None,
        math_fct_kwargs: dict = None,
        weight_type: str = None,
    ):
        """
        Initialize a Histogram2DCustomWeights object.

        Parameters
        ----------
        name : str
            Name of the histogram aggregation.
        x_expr : TimeSeriesExpression
            Time series expression for the x-axis.
        y_expr : TimeSeriesExpression
            Time series expression for the y-axis.
        weights_expr : TimeSeriesExpression
            Time series expression for the weights.
        x_bins : list of float
            List of bin edges for the x-axis.
        y_bins : list of float
            List of bin edges for the y-axis.
        event : Event, optional
            Optional event to filter the expressions.
        desc : str, optional
            Description of the histogram.
        agg_type : str, optional
            Type of aggregation, defaults to 'histogram2d_custom_weights'.
        x_channel_name : str, optional
            Name of the signal associated with the x-axis.
        y_channel_name : str, optional
            Name of the signal associated with the y-axis.
        weights_channel_name : str, optional
            Name of the signal associated with the weights.
        values_unit : str, optional
            Unit of the histogram values.
        x_bins_unit : str, optional
            Unit of the x-axis bins.
        y_bins_unit : str, optional
            Unit of the y-axis bins.
        channel_interp_kind : str, optional
            Interpolation method for the channel values, defaults to 'previous'.
        weights_interp_kind : str, optional
            Interpolation method for the weights, defaults to 'previous'.
        math_fct_for_weights : str, optional
            Optional function name to apply to the weights before aggregation.
            Example: 'diff' to compute the difference of consecutive weight values.
        math_fct_kwargs : dict, optional
            Additional keyword arguments to pass to the math function for weights,
            defaults to an empty dictionary.
        weight_type: str, optional
            If the custom weighted signal is required to be weighted with time, it must be provided as 'time'.
            By default it is set to None, this option is provided to prevent errors from RLE compression method of the channels.
        """
        super().__init__(
            name=name,
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=x_bins,
            y_bins=y_bins,
            event=event,
            desc=desc,
            agg_type=agg_type,
            x_channel_name=x_channel_name,
            y_channel_name=y_channel_name,
            values_unit=values_unit,
            x_bins_unit=x_bins_unit,
            y_bins_unit=y_bins_unit,
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
        x_expression = self.x_expr.where(event_expression) if event_expression else self.x_expr
        y_expression = self.y_expr.where(event_expression) if event_expression else self.y_expr
        weights_expression = (
            self.weights_expr.where(event_expression) if event_expression else self.weights_expr
        )
        return x_expression.histogram2d_custom_weights(
            y_selection=y_expression,
            weights_selection=weights_expression,
            x_bins=self.x_bins,
            y_bins=self.y_bins,
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
            "page_number": self.page_number,
            "name": self.name,
            "description": self.desc,
            "agg_type": self.agg_type,
            "x_bins": self.x_bins,
            "y_bins": self.y_bins,
            "x_channel_name": self.x_channel_name,
            "x_signal_expression": self.x_expr.__str__(),
            "y_channel_name": self.y_channel_name,
            "y_signal_expression": self.y_expr.__str__(),
            "weights_channel_name": self.weights_channel_name,
            "weights_expression": self.weights_expr.__str__(),
            "values_unit": self.values_unit,
            "x_bins_unit": self.x_bins_unit,
            "y_bins_unit": self.y_bins_unit,
            "definition_hash": self.determine_definition_hash(),
        }


class Histogram2DDistance(Histogram2DCustomWeights):
    """
    Class representing a 2D histogram aggregation weighted by distance.

    This class extends Histogram2DCustomWeights to compute a 2D histogram
    where the weights are derived from the difference of consecutive weight
    values (using the 'diff' math function), typically representing distance.
    """

    def __init__(
        self,
        name: str,
        x_expr: TimeSeriesExpression,
        y_expr: TimeSeriesExpression,
        weights_expr: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
        event: Event | None = None,
        desc: str = None,
        x_channel_name: str = None,
        y_channel_name: str = None,
        values_unit: str = None,
        x_bins_unit: str = None,
        y_bins_unit: str = None,
        channel_interp_kind: str = "previous",
        weights_interp_kind: str = "previous",
        math_fct_kwargs: dict = None,
    ):
        """
        Initialize a Histogram2DDistance object.

        Parameters
        ----------
        name : str
            Name of the histogram aggregation.
        x_expr : TimeSeriesExpression
            Time series expression for the x-axis.
        y_expr : TimeSeriesExpression
            Time series expression for the y-axis.
        weights_expr : TimeSeriesExpression
            Time series expression for the weights (e.g., cumulative distance).
        x_bins : list of float
            List of bin edges for the x-axis.
        y_bins : list of float
            List of bin edges for the y-axis.
        event : Event, optional
            Optional event to filter the expressions.
        desc : str, optional
            Description of the histogram.
        x_channel_name : str, optional
            Name of the signal associated with the x-axis.
        y_channel_name : str, optional
            Name of the signal associated with the y-axis.
        values_unit : str, optional
            Unit of the histogram values.
        x_bins_unit : str, optional
            Unit of the x-axis bins.
        y_bins_unit : str, optional
            Unit of the y-axis bins.
        channel_interp_kind : str, optional
            Interpolation method for the channel values, defaults to 'previous'.
        weights_interp_kind : str, optional
            Interpolation method for the weights, defaults to 'previous'.
        math_fct_kwargs : dict, optional
            Additional keyword arguments to pass to the 'diff' math function,
            defaults to an empty dictionary.
        """
        math_fct_for_weights = "diff"
        agg_type = "histogram2d_distance"

        super().__init__(
            name=name,
            x_expr=x_expr,
            y_expr=y_expr,
            weights_expr=weights_expr,
            x_bins=x_bins,
            y_bins=y_bins,
            event=event,
            desc=desc,
            agg_type=agg_type,
            x_channel_name=x_channel_name,
            y_channel_name=y_channel_name,
            values_unit=values_unit,
            x_bins_unit=x_bins_unit,
            y_bins_unit=y_bins_unit,
            channel_interp_kind=channel_interp_kind,
            weights_interp_kind=weights_interp_kind,
            math_fct_for_weights=math_fct_for_weights,
            math_fct_kwargs=math_fct_kwargs,
        )
