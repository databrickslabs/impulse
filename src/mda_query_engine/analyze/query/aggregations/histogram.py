from typing import Any

import pandas as pd
import numpy as np

import pyspark.sql.types as T

from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression
from .aggregation import Aggregation


class HistogramDuration(Aggregation):
    def __init__(self, selection, bins: list[float], aggregation_level: str = "container"):
        """
        Initialize a HistogramDuration aggregation.

        Parameters
        ----------
        selection : TimeSeriesExpression
            Time series expression to aggregate.
        bins : list of float
            Bin edges for the histogram.
        aggregation_level : str, optional
            Level of aggregation ('container' by default).
        """
        self.selection = selection
        self.bins = bins
        self.aggregation_level = aggregation_level

    def __str__(self):
        """
        Return a string representation of the HistogramDuration object.
        Returns
        -------
        str
            String representation of the HistogramDuration object.
        """
        return f"<Histogram selection={self.selection}, bins={self.bins}, aggregation_level={self.aggregation_level}>"

    def dtype(self):
        """
        Return the Spark data type for the aggregation result.

        Returns
        -------
        pyspark.sql.types.StructType
            Data type for the aggregation result.
        """
        return T.StructType(
            [
                T.StructField("H", T.ArrayType(T.DoubleType())),
                T.StructField("bin_edges", T.ArrayType(T.DoubleType())),
            ]
        )

    def build(self, cache) -> tuple[np.ndarray, np.ndarray]:
        """
        Build the histogram from the cache.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        hist : np.ndarray
            Histogram values.
        bin_edges : np.ndarray
            Bin edges for the histogram.
        """
        ts = self.selection.build(cache)
        hist, bin_edges = ts.histogram(self.bins)
        return hist, bin_edges

    def reduce(self, results, aggregation_level="global", **kwargs):
        """
        Reduce results to a DataFrame.

        Parameters
        ----------
        results : Any
            Results to reduce.
        aggregation_level : str, optional
            Level of aggregation ('global' by default).
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        pd.DataFrame
            DataFrame containing bin start, bin end, bin center, and values.
        """
        bin_start = np.array(self.bins[:-1])
        bin_end = np.array(self.bins[1:])
        bin_center = (bin_end - bin_start) / 2.0
        if aggregation_level == "global":
            values = results.result.sum()
        else:
            raise Exception("nyi")

        return pd.DataFrame(
            {"bin_start": bin_start, "bin_end": bin_end, "bin_center": bin_center, "value": values}
        )

    def required_tags(self):
        """
        Return the set of required tags for the aggregation.

        Returns
        -------
        set of str
            Set of required tags.
        """
        return self.selection.required_tags()

    def get_selector_expr(self):
        """
        Return the selector expression for the aggregation.

        Returns
        -------
        Any
            Selector expression for the aggregation.
        """
        return self.selection.get_selector_expr()

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the set of required tag expressions for the aggregation.

        Returns
        -------
        set of TagExpression
            Set of required tag expressions.
        """
        return self.selection.get_required_tag_exprs()


class HistogramCustomWeights(Aggregation):
    def __init__(
        self,
        selection: TimeSeriesExpression,
        weights: TimeSeriesExpression,
        bins: list[float],
        aggregation_level: str = "container",
        channel_interp_kind: str = "previous",
        weights_interp_kind: str = "previous",
        math_fct_for_weights: str = None,
        math_fct_kwargs: dict[str, Any] = None,
        weight_type: str = None,
    ):
        self.selection = selection
        self.weights = weights
        self.bins = bins
        self.aggregation_level = aggregation_level
        self.channel_interp_kind = channel_interp_kind
        self.weights_interp_kind = weights_interp_kind
        self.math_fct_for_weights = math_fct_for_weights
        self.math_fct_kwargs = math_fct_kwargs
        self.weight_type = weight_type

    def __str__(self):
        """
        Return a string representation of the Histogram object.
        Returns
        -------
        str
            String representation of the Histogram object.
        """
        return f"<Histogram channel={self.selection}, weights={self.weights}, bins={self.bins}, channel_interp_kind={self.channel_interp_kind}, weights_interp_kind={self.weights_interp_kind}, math_fct_for_weights={self.math_fct_for_weights}, math_fct_kwargs={self.math_fct_kwargs}>"

    def dtype(self):
        """
        Return the Spark data type for the aggregation result.

        Returns
        -------
        pyspark.sql.types.StructType
            Data type for the aggregation result.
        """
        return T.StructType(
            [
                T.StructField("H", T.ArrayType(T.DoubleType())),
                T.StructField("bin_edges", T.ArrayType(T.DoubleType())),
            ]
        )

    def build(self, cache):
        selection_series = self.selection.build(cache)
        weights_series = self.weights.build(cache)

        # todo once linear interpolation is supported, use channel_interp_kind and weights_interp_kind in synchronized method call
        selection_series_synced, weights_series_synced = selection_series.synchronized(
            weights_series
        )
        # apply math function to synchronized weights channel if specified using also the kwargs if specified
        if self.math_fct_for_weights is not None:
            math_func = getattr(weights_series_synced, self.math_fct_for_weights)
            math_fct_kwargs = self.math_fct_kwargs or {}
            weights_series_math_imp = math_func(**math_fct_kwargs)
        else:
            weights_series_math_imp = weights_series_synced

        hist, bin_edges = selection_series_synced.histogram(
            self.bins, weights=weights_series_math_imp, weight_type=self.weight_type
        )
        return hist, bin_edges

    def reduce(self, results, aggregation_level="global", **kwargs):
        """
        Reduce results to a DataFrame.

        Parameters
        ----------
        results : Any
            Results to reduce.
        aggregation_level : str, optional
            Level of aggregation ('global' by default).
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        pd.DataFrame
            DataFrame containing bin start, bin end, bin center, and values.
        """
        bin_start = np.array(self.bins[:-1])
        bin_end = np.array(self.bins[1:])
        bin_center = (bin_end - bin_start) / 2.0
        if aggregation_level == "global":
            values = results.result.sum()
        else:
            raise Exception("nyi")

        return pd.DataFrame(
            {
                "bin_start": bin_start,
                "bin_end": bin_end,
                "bin_center": bin_center,
                "value": values,
            }
        )

    def required_tags(self) -> set[str]:
        """
        Return the set of required tags for both time series.

        Returns
        -------
        set of str
            Set of required tags for the aggregation.
        """
        return self.selection.required_tags().union(self.weights.required_tags())

    def get_selector_expr(self):
        """
        Return the selector expression for the aggregation.

        Returns
        -------
        Any
            Selector expression for the aggregation.
        """
        selection_expr = self.selection.get_selector_expr()
        weights_expr = self.weights.get_selector_expr()
        return selection_expr | weights_expr

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the set of required tag expressions for the aggregation.

        Returns
        -------
        set of TagExpression
            Set of required tag expressions for the aggregation.
        """
        return self.selection.get_required_tag_exprs().union(self.weights.get_required_tag_exprs())
