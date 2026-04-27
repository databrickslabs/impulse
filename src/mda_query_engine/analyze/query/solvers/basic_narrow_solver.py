from functools import partial
from collections.abc import Iterable

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from mda_query_engine.analyze.metadata.metric_expression import MetricExpression
from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression
from mda_query_engine.model.series.sample_series import SampleSeries
from .query_solver import QuerySolver
from .series_cache import SeriesCache
from .solver_config import SolverConfig
from .utils.interval_encoder import IntervalEncoder


class BasicNarrowTimeSeriesCache(SeriesCache):
    def __init__(self, pdf, col_map: dict[str, str]):
        """
        Initialize the BasicNarrowTimeSeriesCache.

        Parameters
        ----------
        pdf : pd.DataFrame
            DataFrame containing time series data.
        col_map : dict[str, str]
            Mapping with keys ``"cid"``, ``"ch"``, ``"ts"``, ``"te"``,
            ``"val"`` to the actual column names in *pdf*.
        """
        self._cid_col = col_map["cid"]
        self._ch_col = col_map["ch"]
        self._ts_col = col_map["ts"]
        self._te_col = col_map["te"]
        self._val_col = col_map["val"]

        self.mdf = (
            pdf.drop(columns=[self._ts_col, self._te_col, self._val_col])
            .drop_duplicates()
            .reset_index()
        )
        self.pdf = pdf.sort_values([self._cid_col, self._ch_col, self._ts_col]).reset_index()

    def resolve(self, selection):
        """
        Resolve selected tags/metrics to a list of candidates.

        Parameters
        ----------
        selection : Any
            The selection object specifying tags or metrics.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the resolved candidates.
        """
        idx = selection._expr.build_pandas(self.mdf)
        return self.mdf[idx]

    def load_blob(self, mid, cid):
        """
        Load a time series blob from the DataFrame.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.

        Returns
        -------
        SampleSeries
            The loaded sample series object.
        """
        s = self.pdf[(self.pdf[self._cid_col] == mid) & (self.pdf[self._ch_col] == cid)]
        return SampleSeries(s[self._ts_col], s[self._te_col], s[self._val_col])


class BasicNarrowSolver(QuerySolver):
    def __init__(
        self,
        spark,
        config: SolverConfig = None,
        is_raw_data: bool = False,
        drop_implausible_data: bool = False,
    ):
        """
        Initialize the BasicNarrowSolver.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        config : SolverConfig, optional
            Solver configuration.  When *None* a default :class:`SolverConfig`
            is used (backward-compatible column names).
        is_raw_data : bool, optional
            Whether the input data is raw point data (timestamp column)
            rather than RLE format (tstart/tend columns).
        drop_implausible_data : bool, optional
            Whether to drop data points marked as implausible before
            processing.  Requires an ``is_plausible`` column in the
            silver layer.
        """
        super().__init__(config)
        self.spark = spark
        self.is_raw_data = is_raw_data
        self.drop_im_plausible_data: bool = drop_implausible_data
        self.interval_encoder: IntervalEncoder = IntervalEncoder(
            timestamp_col_name="timestamp",
            drop_implausible_data_points=self.drop_im_plausible_data,
        )

    @staticmethod
    def _solve_udf(pdf, selections: Iterable, col_map: dict[str, str]) -> pd.DataFrame:
        """
        UDF to solve for a single container by applying selections.

        Parameters
        ----------
        pdf : pd.DataFrame
        selections : Iterable
            List of selection expressions to apply.
        col_map : dict[str, str]
            Column name mapping for the cache.

        Returns
        -------
        pd.DataFrame
            DataFrame containing results for each selection.
        """
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=col_map)
        cid_col = col_map["cid"]
        result = {cid_col: [pdf[cid_col].iloc[0]]}
        for s in selections:
            res = s.build(cache)
            if hasattr(res, "serialize") and callable(res.serialize):
                res = res.serialize()
            elif hasattr(res, "get_data") and callable(res.get_data):
                res = res.get_data()
            result[s._alias] = [res]
        return pd.DataFrame(result)

    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Generate DataFrame filtered by container tags.

        The BasicNarrowSolver does not filter by container tags,
        so it returns an empty DataFrame.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.

        Returns
        -------
        pyspark.sql.DataFrame
            Empty DataFrame.
        """
        return spark.createDataFrame([], schema=T.StructType([]))

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Filter containers by metrics.

        Returns full container metrics (not just container_id) so that
        ContainerDimension and ContainerEvent can access start_ts/stop_ts.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame from filter_container_tags stage (unused by this solver).
        pre_filtered_containers_df : pyspark.sql.DataFrame, optional
            Pre-filtered containers for incremental processing.
            When provided, restricts processing to only these containers.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered container metrics.
        """
        filters = []
        for filt in query.filters:
            if isinstance(filt, MetricExpression):
                filters.append(filt)
        # Use pre-filtered containers if provided (incremental mode)
        if pre_filtered_containers_df is not None:
            metrics = pre_filtered_containers_df
        else:
            metrics = query.db.container_metrics(self.spark)
        # apply filter
        if len(filters) > 0:
            expr = self._build_expr(filters)
            return metrics.where(expr).dropDuplicates([self.config.container_id_col])

        return metrics.dropDuplicates([self.config.container_id_col])

    def filter_channel_tags(self, spark, query, container_df) -> DataFrame:
        """
        Pass through container DataFrame.

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
            The input container DataFrame.
        """
        return container_df

    def filter_channel_metrics(self, spark, query, container_df) -> DataFrame:
        """
        Filter channels by metrics and required tags.

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
            DataFrame containing filtered channel information.
        """
        channel_metrics_df = query.db.channel_metrics(spark)
        filters = []
        required_tags = []
        for selection in query.selections:
            if not isinstance(selection, TimeSeriesExpression):
                continue
            filters.append(selection)
            required_tags.extend(selection.required_tags())
        required_tags = set(required_tags)
        expr = self._build_expr(filters)
        if len(filters) > 0:
            channel_metrics_df = channel_metrics_df.where(expr)
        result = channel_metrics_df.join(
            F.broadcast(container_df.select(self.config.container_id_col)),
            on=[self.config.container_id_col],
            how="inner",
        )
        # ToDo: Determine a selector id for every selection and add it to the result
        for tag in required_tags:
            result = result.withColumnRenamed(tag, f"ct_{tag}")
        return result.select(
            self.config.container_id_col,
            self.config.channel_id_col,
            *[f"ct_{tag}" for tag in required_tags],
        )

    def solve(self, query, channels_df, selections, dtypes) -> DataFrame:
        """
        Solve the query by grouping channels and applying selections.

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
        col_map = self.config.col_map

        q = query.db.channels(self.spark)

        if self.is_raw_data:
            # Calculate the tend info and prepare the data for the solving step.
            q = self.interval_encoder.prepare_channels_df(q)

        schema_entries = [T.StructField(self.config.container_id_col, T.LongType())]
        for s, dtype in zip(selections, dtypes, strict=False):
            schema_entries.append(T.StructField(s._alias, dtype))
        schema = T.StructType(schema_entries)
        solve_udf = F.pandas_udf(
            partial(BasicNarrowSolver._solve_udf, selections=selections, col_map=col_map),
            schema,
            F.PandasUDFType.GROUPED_MAP,
        )
        df = q.join(
            F.broadcast(channels_df), on=[self.config.container_id_col, self.config.channel_id_col]
        )

        container_count = df.select(self.config.container_id_col).distinct().count()
        if container_count == 0:
            return self.spark.createDataFrame([], schema=schema)
        res = (
            df.repartition(container_count, self.config.container_id_col)
            .groupBy(self.config.container_id_col)
            .apply(solve_udf)
        )
        return res
