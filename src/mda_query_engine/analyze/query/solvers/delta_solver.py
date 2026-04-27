from collections.abc import Iterable
from functools import partial

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from mda_query_engine.analyze.metadata.metric_expression import MetricExpression
from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression
from mda_query_engine.model.series.sample_series import SampleSeries
from .query_solver import QuerySolver
from .series_cache import SeriesCache
from .solver_config import SolverConfig
from .utils.interval_encoder import IntervalEncoder


class DeltaTimeSeriesCache(SeriesCache):
    def __init__(self, pdf, col_map: dict[str, str]):
        """
        Initialize the DeltaTimeSeriesCache.

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


class DeltaSolver(QuerySolver):
    def __init__(
        self,
        spark,
        config: SolverConfig = None,
        is_raw_data: bool = True,
        drop_implausible_data: bool = False,
    ):
        """
        Initialize the DeltaSolver.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        config : SolverConfig, optional
            Solver configuration.  When *None* a default :class:`SolverConfig`
            is used (backward-compatible column names).
        is_raw_data : bool
            Indicates whether the input data is raw point data (with a timestamp column) or already in RLE format
            (with tstart and tend columns).
        drop_implausible_data: bool
            Specifies whether we should drop implausible data points before RLE encoding.
            IMPORTANT: The silver layer needs the is_plausible column for this to work.
            If this is set to True, all data points which are marked as implausible will be dropped before RLE encoding.
        """
        super().__init__(config)
        self.spark = spark
        self.is_raw_data: bool = is_raw_data
        self.drop_im_plausible_data: bool = drop_implausible_data

        self.interval_encoder: IntervalEncoder = IntervalEncoder(
            timestamp_col_name="timestamp",
            drop_implausible_data_points=self.drop_im_plausible_data,
        )

    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Stage 1: Generate DataFrame filtered by container tags.

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
        """
        filters = []
        required_tags = []
        for filt in query.filters:
            if isinstance(filt, TagExpression):
                filters.append(filt)
                required_tags.extend(filt.required_tags())
        required_tags = set(required_tags)
        # read df
        tags = query.db.container_tags(spark)
        # apply filters
        if len(filters) > 0:
            tags = tags.where(F.col("key").isin(required_tags))
            tags = tags.groupBy("container_id")
            tags = tags.pivot("key", list(required_tags)).agg({"value": "first"})
            expr = self._build_expr(filters)
            tags = tags.where(expr)
            for tag in required_tags:
                tags = tags.withColumnRenamed(tag, f"mt_{tag}")
        else:
            tags = tags.select("container_id").distinct()
        return tags

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Stage 2: Filter containers by metrics.

        Returns full container metrics (not just container_id) so that
        ContainerDimension and ContainerEvent can access start_ts/stop_ts.

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

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered container metrics.
        """
        filters = []
        tag_count = 0
        for filt in query.filters:
            if isinstance(filt, MetricExpression):
                filters.append(filt)
            if isinstance(filt, TagExpression):
                tag_count += 1
        # apply filter
        if len(filters) > 0:
            # Use pre-filtered containers if provided (incremental mode)
            if pre_filtered_containers_df is not None:
                metrics = pre_filtered_containers_df
            else:
                metrics = query.db.container_metrics(spark)
            expr = self._build_expr(filters)
            metrics = metrics.where(expr)
            # filter by tags (only if there were tags defined)
            if tag_count > 0:
                container_ids = container_df.select("container_id").distinct()
                return metrics.join(F.broadcast(container_ids), on=["container_id"], how="inner")
            else:
                return metrics
        else:
            # No metric filters: read full container_metrics, restrict by tags/pre-filter
            if pre_filtered_containers_df is not None:
                metrics = pre_filtered_containers_df
            else:
                metrics = query.db.container_metrics(spark)
            container_ids = container_df.select("container_id").distinct()
            return metrics.join(F.broadcast(container_ids), on=["container_id"], how="inner")

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
        """
        mids = container_df.select("container_id").distinct()

        filters = []
        required_tags = []
        for selection in query.selections:
            if not isinstance(selection, TimeSeriesExpression):
                continue
            filters.append(selection)
            required_tags.extend(selection.required_tags())
        required_tags = set(required_tags)
        tbl = query.db.channel_tags(spark)
        expr = self._build_expr(filters)
        # filter by tags
        tags = (
            tbl.where(F.col("key").isin(required_tags))
            .join(F.broadcast(mids), on=["container_id"], how="inner")
            .groupBy("container_id", "channel_id")
            .pivot("key", list(required_tags))
            .agg(F.first(F.col("value")))
            .where(expr)
        )
        for tag in required_tags:
            tags = tags.withColumnRenamed(tag, f"ct_{tag}")
        return tags

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
        """
        cids = channel_df.select("channel_id").distinct()
        tbl = query.db.channel_metrics(spark)
        metrics = tbl.select("container_id", "channel_id", "sample_count").join(
            F.broadcast(channel_df), on=["container_id", "channel_id"], how="inner"
        )
        for col in ["sample_count"]:
            metrics = metrics.withColumnRenamed(col, f"cm_{col}")
        return metrics

    @staticmethod
    def _solve_udf(pdf, selections: Iterable, col_map: dict[str, str]):
        """
        UDF to solve for a single container by applying selections.

        Parameters
        ----------
        pdf : pd.DataFrame
            DataFrame containing time series data for a container.
        selections : Iterable
            List of selection expressions to apply.
        col_map : dict[str, str]
            Column name mapping for the cache.

        Returns
        -------
        pd.DataFrame
            DataFrame containing results for each selection.
        """
        cache = DeltaTimeSeriesCache(pdf, col_map=col_map)
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

    def solve(self, query, channels_df, selections, dtypes):
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

        schema_entries = [T.StructField(self.config.container_id_col, T.LongType())]
        for s, dtype in zip(selections, dtypes, strict=False):
            schema_entries.append(T.StructField(s._alias, dtype))
        schema = T.StructType(schema_entries)

        if self.is_raw_data:
            # Calculate the tend info and prepare the data for the solving step.
            q = self.interval_encoder.prepare_channels_df(q)

        solve_udf = F.pandas_udf(
            partial(DeltaSolver._solve_udf, selections=selections, col_map=col_map),
            schema,
            F.PandasUDFType.GROUPED_MAP,
        )
        df = q.join(
            F.broadcast(channels_df), on=[self.config.container_id_col, self.config.channel_id_col]
        )
        res = df.groupBy(self.config.container_id_col).apply(solve_udf)
        return res
