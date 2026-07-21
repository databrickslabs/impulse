from typing import Self

import pandas as pd
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.metadata.metric_expression import MetricSelector
from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    RequiresDeserialization,
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.channels.calculated_channel import (
    CalculatedChannel,
)
from impulse_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from impulse_query_engine.model.series.sample_series import SampleSeries

from .solvers.blob_solver import BlobSolver
from .solvers.query_solver import QuerySolver
from impulse_query_engine.telemetry import telemetry_logger


class QueryBuilder:
    def __init__(self, db: "impulse_query_engine.analyze.MeasurementDB"):
        """
        Initialize the QueryBuilder.

        Parameters
        ----------
        db : impulse_query_engine.analyze.MeasurementDB
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

    def channel_with_alias(self, **kwargs) -> TimeSeriesSelector:
        if self.db.config.channel_mapping_table is None:
            raise ValueError("channel_mapping_table is not configured")

        expr = None
        for k, arg in kwargs.items():
            if not expr:
                expr = TagSelector(k) == str(arg)
            else:
                expr = expr & (TagSelector(k) == str(arg))
        return TimeSeriesSelector(expr, uses_alias=True)

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

        channel_metrics_df = self._run_filter_pipeline(spark, solver, pre_filtered_containers_df)

        return solver.solve(self, channel_metrics_df, self.selections, self.result_dtypes)

    def _run_filter_pipeline(self, spark, solver, pre_filtered_containers_df) -> DataFrame:
        """Run the shared metadata filter pipeline and return the channel-match frame.

        Extracts the selector split, the four filter stages
        (container tags → container metrics → channel tags → channel metrics) and
        the optional channel-alias resolution that both :meth:`solve` and
        :meth:`solve_calculated_channels` drive before their differing final
        ``solver`` call.  Returns the ``(container_id, channel_id, selector_ids …)``
        DataFrame identifying the channels selected by the current selections.
        """
        # extract selectors upfront
        direct_selectors = TimeSeriesExpression.collect_selectors(
            self.selections, uses_alias=False
        )
        aliased_selectors = TimeSeriesExpression.collect_selectors(
            self.selections, uses_alias=True
        )

        # create Query
        tags_df = solver.filter_container_tags(spark, self)
        metrics_df = solver.filter_container_metrics(
            spark, self, tags_df, pre_filtered_containers_df
        )
        channel_tags_df = solver.filter_channel_tags(spark, self.db, metrics_df, direct_selectors)
        channel_metrics_df = solver.filter_channel_metrics(
            spark, self.db, channel_tags_df, direct_selectors
        )

        if len(aliased_selectors) > 0:
            # Aliased resolution must run against the full tag-filtered container
            # set (metrics_df).
            aliased_channel_metrics_df = solver.filter_aliased_channel_metrics(
                spark, self.db, metrics_df, aliased_selectors
            )
            channel_metrics_df = solver.resolve_channel_selections(
                spark, channel_metrics_df, aliased_channel_metrics_df
            )

        return channel_metrics_df

    @telemetry_logger("query", "solve_calculated_channels")
    def solve_calculated_channels(
        self,
        spark,
        solver: QuerySolver = BlobSolver(),
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """
        Compute calculated channels and return a narrow silver-shaped DataFrame.

        Every selection must be a :class:`CalculatedChannel`.  This runs the same
        metadata filter pipeline as :meth:`solve` (resolving the input channels
        each calculated channel depends on), then evaluates each calculated
        channel per container and emits rows in the silver ``channel_data`` shape
        — ``container_id, channel_id, tstart, tend, value`` — plus a single
        ``identity`` ``MapType(string, string)`` column holding each channel's
        identity dict.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        solver : QuerySolver, optional
            Query solver to use.  Must implement ``solve_calculated_channels``
            (``DefaultSolver`` does); the default ``BlobSolver`` does not.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered container metrics for incremental processing.  When
            provided, only these containers are processed; when None, all
            containers matching the query filters are processed.

        Returns
        -------
        pyspark.sql.DataFrame
            Narrow DataFrame ``[container_id, channel_id, tstart, tend, value,
            identity]``.

        Raises
        ------
        ValueError
            If any selection is not a ``CalculatedChannel``, or if a wrapped
            expression does not evaluate to a ``SampleSeries``.
        """
        self._validate_calculated_channels()

        channel_metrics_df = self._run_filter_pipeline(spark, solver, pre_filtered_containers_df)

        return solver.solve_calculated_channels(self, channel_metrics_df, self.selections)

    def _validate_calculated_channels(self) -> None:
        """Validate the selections for :meth:`solve_calculated_channels`.

        Every selection must be a ``CalculatedChannel`` and each wrapped
        expression must evaluate to a ``SampleSeries``.  Identity key sets need
        not match across selections — the identity is emitted as a single
        self-describing ``MapType`` column, so heterogeneous keys are fine.
        """
        if not self.selections:
            raise ValueError(
                "solve_calculated_channels() requires at least one CalculatedChannel."
            )

        for i, s in enumerate(self.selections):
            if not isinstance(s, CalculatedChannel):
                raise ValueError(
                    "solve_calculated_channels() requires all selections to be "
                    f"CalculatedChannel; got {type(s).__name__} at index {i}."
                )
            s.expr.require_evaluation_type(
                SampleSeries,
                owner="CalculatedChannel",
                example="q.channel(channel_name='raw_speed') * 3.6",
            )

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
