"""PointValueAggregator: sample SampleSeries values at points in time."""

import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache

from .aggregation import Aggregation


class PointValueAggregator(Aggregation):
    """
    Aggregation that samples time series at points in time.

    This aggregator evaluates each input expression to a SampleSeries and samples it at
    the instants defined by ``event_expression`` (which evaluates to a PointsInTime).
    For every input series it returns, per point, the value of the sample valid at that
    instant. Unlike :class:`StatsAggregator` (which computes statistics over event
    *intervals*), this produces one (timestamp, value) pair per point in time.
    """

    def __init__(
        self,
        input_expressions: list[TimeSeriesExpression],
        event_expression: TimeSeriesExpression,
    ):
        """
        Initialize a PointValueAggregator.

        Parameters
        ----------
        input_expressions : list of TimeSeriesExpression
            Expressions to sample. Each yields a SampleSeries when evaluated.
        event_expression : TimeSeriesExpression
            Expression defining the points in time at which to sample. When evaluated,
            it yields an instance of PointsInTime.
        """
        self.input_expressions = input_expressions
        self.event_expression = event_expression

    def __str__(self) -> str:
        """
        Return a string representation of the PointValueAggregator object.

        Returns
        -------
        str
            String representation of the PointValueAggregator object.
        """
        return (
            f"<PointValueAggregator input_expressions={self.input_expressions}, "
            f"event_expression={self.event_expression}>"
        )

    def dtype(self) -> T.StructType:
        """
        Return the Spark data type for the aggregation result.

        The result is a struct holding, per input series, the sampled point timestamps
        and the corresponding values:
        - ``point_timestamps``: array (per series) of arrays of point timestamps.
        - ``values``: array (per series) of arrays of sampled values.

        Returns
        -------
        pyspark.sql.types.StructType
            Data type for the aggregation result.
        """
        return T.StructType(
            [
                T.StructField(
                    "point_timestamps",
                    T.ArrayType(T.ArrayType(T.DoubleType())),
                    nullable=True,
                ),
                T.StructField(
                    "values",
                    T.ArrayType(T.ArrayType(T.DoubleType())),
                    nullable=True,
                ),
            ]
        )

    def build(self, cache: SeriesCache) -> tuple[list[list[float]], list[list[float]]]:
        """
        Build the point-value aggregation from the cache.

        This method:
        1. Evaluates ``event_expression`` to get the PointsInTime to sample at.
        2. Evaluates each input expression to a SampleSeries.
        3. Samples each SampleSeries at the points via ``SampleSeries.where(points)``,
           which yields a PointsInTimeSeries holding the value valid at each point.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        tuple
            A 2-tuple ``(point_timestamps, values)`` where each is a list indexed by
            input expression, holding that series' sampled point timestamps and values.
            A point that falls outside a series' coverage is omitted for that series.
        """
        points = self.event_expression.build(cache)

        point_timestamps: list[list[float]] = []
        values: list[list[float]] = []
        for expr in self.input_expressions:
            sample_series = expr.build(cache)
            pit_series = sample_series.where(points)
            point_timestamps.append(pit_series.tstarts.tolist())
            values.append(pit_series.values.tolist())
        return (point_timestamps, values)

    def required_tags(self) -> set[str]:
        """
        Return the union of required tags across all input expressions and the event expression.

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
        Return the union of selector expressions for all input expressions and the event expression.

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
        and the event expression.

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
        """
        Return all leaf selectors reachable from the input and event expressions.

        Returns
        -------
        list of TimeSeriesSelector
            Leaf selectors.
        """
        result: list[TimeSeriesSelector] = []
        for expr in self.input_expressions:
            result.extend(expr.get_selectors())
        if self.event_expression is not None:
            result.extend(self.event_expression.get_selectors())
        return result

    def get_poi_channel_selectors(self) -> list:
        """Return POI channel selectors reachable from the input/event expressions,
        so a ``poi_channel`` used as an input or event is discovered by Stage P."""
        result: list = []
        for expr in self.input_expressions:
            result.extend(expr.get_poi_channel_selectors())
        if self.event_expression is not None:
            result.extend(self.event_expression.get_poi_channel_selectors())
        return result
