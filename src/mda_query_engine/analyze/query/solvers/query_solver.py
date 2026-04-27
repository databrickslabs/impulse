import abc
from abc import ABC

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from mda_query_engine.analyze.metadata.metric_expression import MetricExpression
from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from .solver_config import SolverConfig


class QuerySolver(ABC):
    """Abstract base class for query solvers.

    Defines a 6-stage filter pipeline that all solvers must implement:
    filter_container_tags -> filter_container_metrics -> filter_channel_tags ->
    filter_channel_metrics -> filter_candidates -> solve.

    ``filter_container_metrics`` must return a DataFrame that includes **all
    columns** needed for container dimensions and event bounds (e.g.
    ``container_id``, ``start_ts``/``stop_ts`` or ``start_dt``/``stop_dt``),
    not only ``container_id``.
    """

    def __init__(self, config: SolverConfig = None):
        self.config = config or SolverConfig()

    def _build_expr(self, filters):
        """
        Build a combined selector expression from a list of filter expressions.

        Parameters
        ----------
        filters : list
            List of filter expressions.
            Example: [MetricOp<or_(MetricOp<and_(MetricOp<eq(MetricSelector<vehicle_key>,Seat_Leon)>]
        Returns
        -------
        pyspark.sql.Column or None
            Combined selector expression or None if no filters.
        """
        expr = None
        for filt in filters:
            if expr is None:
                expr = filt.get_selector_expr()
            else:
                expr = expr | filt.get_selector_expr()
        return expr

    @abc.abstractmethod
    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Abstract method to filter containers by tags.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame filtered by container tags.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_container_tags method.")

    @abc.abstractmethod
    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Abstract method to filter containers by metrics.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame from filter_container_tags stage.
        pre_filtered_containers_df : pyspark.sql.DataFrame, optional
            Pre-filtered containers for incremental processing.
            When provided, restricts processing to only these containers.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered container metrics.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError(
            "Each solver must implement the filter_container_metrics method."
        )

    @abc.abstractmethod
    def filter_channel_tags(self, spark, query, container_df) -> DataFrame:
        """
        Stage 3: Filter channels by measurements and tags.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame containing container information.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered channel tags.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_channel_tags method.")

    @abc.abstractmethod
    def filter_channel_metrics(self, spark, query, channel_df) -> DataFrame:
        """
        Stage 4: Filter channels by metrics.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        channel_df : pyspark.sql.DataFrame
            DataFrame containing channel information.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered channel metrics.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_channel_metrics method.")

    @abc.abstractmethod
    def solve(self, query, channels_df, selections, dtypes):
        """
        Stage 6: Solve query.

        Parameters
        ----------
        query : QueryBuilder
            Query object containing database and filter information.
        channels_df : pyspark.sql.DataFrame
            DataFrame containing channel information.
        selections : list
            List of selection expressions to apply.
        dtypes : list
            List of data types for each selection.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing results for each container.
        """
        pass
