from typing import Any

import numpy as np
import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression

from ...metadata.time_series_expression import TimeSeriesExpression, TimeSeriesSelector
from ..solvers.series_cache import SeriesCache
from .aggregation import Aggregation


class Histogram2DDuration(Aggregation):
    """2D Histogram aggregation."""

    def __init__(
        self,
        x_selection: TimeSeriesExpression,
        y_selection: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
    ):
        """
        Initialize a Histogram2D aggregation.

        Parameters
        ----------
        x_selection : TimeSeriesExpression
            Time series expression for the x-axis.
        y_selection : TimeSeriesExpression
            Time series expression for the y-axis.
        x_bins : list of float
            Bin edges for the x-axis.
        y_bins : list of float
            Bin edges for the y-axis.
        """

        self.x_selection = x_selection
        self.y_selection = y_selection
        self.x_bins = x_bins
        self.y_bins = y_bins

    def __str__(self):
        """
        Return a string representation of the Histogram2D object.
        Returns
        -------
        str
            String representation of the Histogram2D object.
        """
        return (
            f"<Histogram2D x_selection={self.x_selection}, y_selection={self.y_selection}, x_bins={self.x_bins}, "
            f"y_bins={self.y_bins}>"
        )

    def dtype(self) -> T.StructType:
        """
        Return the data type of the aggregation result.
        Returns
        -------
        pyspark.sql.types.StructType
            Data type of the aggregation result.
        """
        return T.StructType(
            [
                T.StructField("H", T.ArrayType(T.ArrayType(T.DoubleType()))),
                T.StructField("xedges", T.ArrayType(T.DoubleType())),
                T.StructField("yedges", T.ArrayType(T.DoubleType())),
            ]
        )

    def build(self, cache: SeriesCache) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Build the bi-dimensional histogram of the two time series.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.
        Returns
        -------
        hist2d : np.ndarray
            2D histogram array.
        x_edges : np.ndarray
            Bin edges for the x-axis.
        y_edges : np.ndarray
            Bin edges for the y-axis.
        """
        x_ts = self.x_selection.build(cache)
        y_ts = self.y_selection.build(cache)
        x_ts, y_ts = x_ts.synchronized(y_ts)

        hist2d, x_edges, y_edges = x_ts.histogram2d(y_ts, x_bins=self.x_bins, y_bins=self.y_bins)
        return hist2d, x_edges, y_edges

    def required_tags(self) -> set[str]:
        """
        Return the set of required tags for both time series.

        Returns
        -------
        set of str
            Set of required tags for the aggregation.
        """
        return self.x_selection.required_tags().union(self.y_selection.required_tags())

    def get_selector_expr(self):
        """
        Return the selector expression for the aggregation.

        Returns
        -------
        Any
            Selector expression for the aggregation.
        """
        x_selector_expr = self.x_selection.get_selector_expr()
        y_selector_expr = self.y_selection.get_selector_expr()
        return x_selector_expr | y_selector_expr

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the set of required tag expressions for the aggregation.

        Returns
        -------
        set of TagExpression
            Set of required tag expressions for the aggregation.
        """
        return self.x_selection.get_required_tag_exprs().union(
            self.y_selection.get_required_tag_exprs()
        )

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return self.x_selection.get_selectors() + self.y_selection.get_selectors()


class Histogram2DCustomWeights(Aggregation):
    """Class representing a 2D histogram aggregation in a report with custom weights."""

    def __init__(
        self,
        x_selection: TimeSeriesExpression,
        y_selection: TimeSeriesExpression,
        weights_expr: TimeSeriesExpression,
        x_bins: list[float],
        y_bins: list[float],
        channel_interp_kind: str = "previous",
        weights_interp_kind: str = "previous",
        math_fct_for_weights: str = None,
        math_fct_kwargs: dict[str, Any] = None,
        weight_type: str = None,
    ):
        """
        Initialize a Histogram2DCustomWeights aggregation.

        Parameters
        ----------
        x_selection : TimeSeriesExpression
            Time series expression for the x-axis.
        y_selection : TimeSeriesExpression
            Time series expression for the y-axis.
        weights_expr : TimeSeriesExpression
            Time series expression for the weights.
        x_bins : list of float
            Bin edges for the x-axis.
        y_bins : list of float
            Bin edges for the y-axis.
        channel_interp_kind : str, optional
            Interpolation method for channel data (default: 'previous').
        weights_interp_kind : str, optional
            Interpolation method for weights data (default: 'previous').
        math_fct_for_weights : str, optional
            Name of mathematical function to apply to weights (default: None).
        math_fct_kwargs : dict, optional
            Keyword arguments for math_fct_for_weights (default: {}).
        weight_type : str, optional
            Type of weighting to use. Options:
            - None (default): Use custom weights only
            - 'time': Multiply custom weights by sample duration
        """
        self.x_selection = x_selection
        self.y_selection = y_selection
        self.x_bins = x_bins
        self.y_bins = y_bins
        self.weights_expr = weights_expr
        self.channel_interp_kind = channel_interp_kind
        self.weights_interp_kind = weights_interp_kind
        self.math_fct_for_weights = math_fct_for_weights
        self.math_fct_kwargs = math_fct_kwargs
        self.weight_type = weight_type

    def __str__(self):
        """
        Return a string representation of the Histogram2D object.
        Returns
        -------
        str
            String representation of the Histogram2D object.
        """
        return (
            f"<Histogram2D x_selection={self.x_selection}, y_selection={self.y_selection}, weights_expr={self.weights_expr}, x_bins={self.x_bins}, "
            f"y_bins={self.y_bins}>"
        )

    def dtype(self) -> T.StructType:
        """
        Return the data type of the aggregation result.
        Returns
        -------
        pyspark.sql.types.StructType
            Data type of the aggregation result.
        """
        return T.StructType(
            [
                T.StructField("H", T.ArrayType(T.ArrayType(T.DoubleType()))),
                T.StructField("xedges", T.ArrayType(T.DoubleType())),
                T.StructField("yedges", T.ArrayType(T.DoubleType())),
            ]
        )

    def build(self, cache: SeriesCache) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Build the bi-dimensional histogram of the two time series.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.
        Returns
        -------
        hist2d : np.ndarray
            2D histogram array.
        x_edges : np.ndarray
            Bin edges for the x-axis.
        y_edges : np.ndarray
            Bin edges for the y-axis.
        """
        x_ts = self.x_selection.build(cache)
        y_ts = self.y_selection.build(cache)
        weights_ts = self.weights_expr.build(cache)

        x_ts, y_ts, weights_ts = x_ts.synchronized_all(
            [y_ts, weights_ts],
        )

        # apply math function to synchronized weights channel if specified using also the kwargs if specified
        if self.math_fct_for_weights is not None:
            math_func = getattr(weights_ts, self.math_fct_for_weights)
            math_fct_kwargs = self.math_fct_kwargs or {}
            weights_series_math_imp = math_func(**math_fct_kwargs)
        else:
            weights_series_math_imp = weights_ts

        hist2d, x_edges, y_edges = x_ts.histogram2d(
            y_ts,
            x_bins=self.x_bins,
            y_bins=self.y_bins,
            weights=weights_series_math_imp,
            weight_type=self.weight_type,
        )
        return hist2d, x_edges, y_edges

    def required_tags(self) -> set[str]:
        """
        Return the set of required tags for all time series (x, y, and weights).

        Returns
        -------
        set of str
            Set of required tags for the aggregation.
        """
        return (
            self.x_selection.required_tags()
            .union(self.y_selection.required_tags())
            .union(self.weights_expr.required_tags())
        )

    def get_selector_expr(self):
        """
        Return the selector expression for the aggregation.

        Returns
        -------
        Any
            Selector expression for the aggregation.
        """
        x_selector_expr = self.x_selection.get_selector_expr()
        y_selector_expr = self.y_selection.get_selector_expr()
        w_selector_expr = self.weights_expr.get_selector_expr()
        return x_selector_expr | y_selector_expr | w_selector_expr

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the set of required tag expressions for the aggregation.

        Returns
        -------
        set of TagExpression
            Set of required tag expressions for the aggregation.
        """
        return (
            self.x_selection.get_required_tag_exprs()
            .union(self.y_selection.get_required_tag_exprs())
            .union(self.weights_expr.get_required_tag_exprs())
        )

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return (
            self.x_selection.get_selectors()
            + self.y_selection.get_selectors()
            + self.weights_expr.get_selectors()
        )
