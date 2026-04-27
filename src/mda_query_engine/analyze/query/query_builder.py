from typing import Self

import pandas as pd
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from mda_query_engine.analyze.metadata.metric_expression import MetricSelector
from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    RequiresDeserialization,
    TimeSeriesSelector,
)
from mda_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from .solvers.blob_solver import BlobSolver
from .solvers.query_solver import QuerySolver
from mda_query_engine.telemetry import telemetry_logger


class QueryBuilder:
    def __init__(self, db: "mda_query_engine.analyze.MeasurementDB"):
        """
        Initialize the QueryBuilder.

        Parameters
        ----------
        db : mda_query_engine.analyze.MeasurementDB
            Measurement database object.

        """
        self.db = db
        self.ws = db.ws
        self.filters = []
        self.selections = []
        self.result_objects = []
        self.result_dtypes = []

    def where(self, *args):
        """
        Add filter expressions to the query.

        Parameters
        ----------
        *args : list
            Filter expressions to be added.
        Returns
        -------
        QueryBuilder
            The updated QueryBuilder instance.
        """
        if len(args) == 0:
            return self
        filtered_args = [arg for arg in args if arg is not None]
        self.filters.extend(filtered_args)
        return self

    def filter(self, *args):
        """
        Alias for where().

        Parameters
        ----------
        *args : list
            Filter expressions to be added.

        Returns
        -------
        QueryBuilder
            The updated QueryBuilder instance.
        """
        return self.where(*args)

    def havingTag(self, **kwargs):
        """
        Add tag-based filters to the query.

        Parameters
        ----------
        **kwargs : dict
            Tag-value pairs to filter by.

        Returns
        -------
        QueryBuilder
            The updated QueryBuilder instance.
        """
        for k, arg in kwargs.items():
            self.filters.append(TagSelector(k) == arg)
        return self

    def tag(self, key: str, cast_type: str | None = None) -> TagSelector:
        """
        Create a tag selector for the given key.

        Parameters
        ----------
        key : str
            Name of the tag (element_id in the EAV table).
        cast_type : str or None, optional
            Spark type to cast the tag value to before comparison
            (e.g. ``"int"``, ``"double"``, ``"string"``).

        Returns
        -------
        TagSelector
            Tag selector object.
        """
        return TagSelector(key, cast_type=cast_type)

    def metric(self, name) -> MetricSelector:
        """
        Create a metric selector for the given name.

        Parameters
        ----------
        name : str
           Name of the metric.

        Returns
        -------
        MetricSelector
           Metric selector object.
        """
        return MetricSelector(name)

    def channel(self, **kwargs) -> TimeSeriesSelector:
        """
        Create a time series selector for the given channel tags.

        Parameters
        ----------
        **kwargs : dict
            Channel tag-value pairs.

        Returns
        -------
        TimeSeriesSelector
            Time series selector object.
        """
        expr = None
        for k, arg in kwargs.items():
            if not expr:
                expr = TagSelector(k) == str(arg)
            else:
                expr = expr & (TagSelector(k) == str(arg))
        return TimeSeriesSelector(expr)

    def select(self, *args) -> Self:
        """
        Set the selection expressions for the query.

        Parameters
        ----------
        *args : list
            Selection expressions.

        Returns
        -------
        QueryBuilder
            The updated QueryBuilder instance.
        """
        self.selections = list(args)
        return self

    def _determine_result_dtypes(self, default_dtype: T = T.DoubleType()):
        """
        Determine result data types for the selections by building each
        against an empty cache and inspecting the result's dtype.

        Parameters
        ----------
        default_dtype : pyspark.sql.types.DataType, optional
            Default data type to use if not specified (default is DoubleType).

        Returns
        -------
        list
            List of Spark data types for each selection.
        """
        result_dtypes = []
        for s in self.selections:
            result_object = s.build(EmptyTimeSeriesCache())
            dtype = default_dtype
            if hasattr(result_object, "dtype") and callable(result_object.dtype):
                dtype = result_object.dtype()
            elif hasattr(s, "dtype") and callable(s.dtype):
                dtype = s.dtype()
            result_dtypes.append(dtype)
        return result_dtypes

    def _determine_result_objects_dtypes(self, default_dtype: T = T.DoubleType()):
        """
        Determine result objects and their data types for the selections.

        Parameters
        ----------
        default_dtype : pyspark.sql.types.DataType, optional
            Default data type to use if not specified (default is DoubleType).

        Returns
        -------
        tuple
            Tuple of (result_objects, result_dtypes).
        """
        result_objects = []
        result_dtypes = []
        for s in self.selections:
            result_object = s.build(EmptyTimeSeriesCache())
            result_objects.append(result_object)
            dtype = default_dtype
            if hasattr(result_object, "dtype") and callable(result_object.dtype):
                dtype = result_object.dtype()
            elif hasattr(s, "dtype") and callable(s.dtype):
                dtype = s.dtype()
            result_dtypes.append(dtype)
        return (result_objects, result_dtypes)

    @telemetry_logger("query", "solve")
    def solve(
        self,
        spark,
        solver: QuerySolver = BlobSolver(),
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """
        Execute the query using the specified solver and return a Spark DataFrame.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        solver : QuerySolver, optional
            Query solver to use (default is BlobSolver).
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered container metrics DataFrame for incremental processing.
            When provided, only these containers will be processed.
            When None, all containers matching query filters are processed (full mode).

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing query results.
        """  # determining result types
        (
            self.result_objects,
            self.result_dtypes,
        ) = self._determine_result_objects_dtypes()

        # create Query
        tags_df = solver.filter_container_tags(spark, self)
        metrics_df = solver.filter_container_metrics(
            spark, self, tags_df, pre_filtered_containers_df
        )
        channel_tags_df = solver.filter_channel_tags(spark, self, metrics_df)
        channel_metrics_df = solver.filter_channel_metrics(spark, self, channel_tags_df)

        return solver.solve(self, channel_metrics_df, self.selections, self.result_dtypes)

    @telemetry_logger("query", "to_pandas")
    def toPandas(self, spark, solver: QuerySolver = BlobSolver()) -> pd.DataFrame:
        """
        Execute the query and collect results into a Pandas DataFrame.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        solver : QuerySolver, optional
            Query solver to use (default is BlobSolver).

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame containing query results.
        """
        df = self.solve(spark, solver)
        pdf = df.toPandas()
        for selection, result_object in zip(self.selections, self.result_objects, strict=False):
            if isinstance(selection, RequiresDeserialization):
                pdf[selection._alias] = pdf[selection._alias].apply(
                    lambda x: selection.deserialize(x)
                )
            elif hasattr(result_object, "requires_deserialization"):
                pdf[selection._alias] = pdf[selection._alias].apply(
                    lambda x: result_object.deserialize(x)
                )
        return pdf
