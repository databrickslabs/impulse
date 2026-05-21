from collections.abc import Iterable
from functools import partial

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.metadata.metric_expression import MetricExpression
from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.model.series.sample_series import SampleSeries

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

        meta = pdf.drop(columns=[self._ts_col, self._te_col, self._val_col])
        self.mdf = meta.drop_duplicates(subset=[self._cid_col, self._ch_col]).reset_index()
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
        if "selector_ids" in self.mdf.columns:
            idx = self.mdf["selector_ids"].apply(
                lambda arr: arr is not None and selection.selector_id in arr
            )
            return self.mdf[idx]
        idx = selection._expr.build_pandas(self.mdf)
        return self.mdf[idx]

    def load_blob(self, mid, cid, uses_alias: bool = False):
        """
        Load a time series blob from the DataFrame.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.
        uses_alias : bool, optional
            Unused by this cache (no unit conversion); accepted for
            interface compatibility with :class:`SeriesCache`.

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
        self.drop_implausible_data: bool = drop_implausible_data

        self.interval_encoder: IntervalEncoder = IntervalEncoder(
            timestamp_col_name="timestamp",
            drop_implausible_data_points=self.drop_implausible_data,
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
        tags = query.db.container_tags(spark)
        tags = self._apply_column_mapping(tags, self.config.container_tags.column_name_mapping)
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
        cm_mapping = self.config.container_metrics.column_name_mapping
        if len(filters) > 0:
            if pre_filtered_containers_df is not None:
                metrics = pre_filtered_containers_df
            else:
                metrics = query.db.container_metrics(self.spark)
            metrics = self._apply_column_mapping(metrics, cm_mapping)
            expr = self._build_expr(filters)
            metrics = metrics.where(expr)
            if tag_count > 0:
                container_ids = container_df.select("container_id").distinct()
                return metrics.join(F.broadcast(container_ids), on=["container_id"], how="inner")
            else:
                return metrics
        else:
            if pre_filtered_containers_df is not None:
                metrics = pre_filtered_containers_df
            else:
                metrics = self._apply_column_mapping(query.db.container_metrics(spark), cm_mapping)
            container_ids = container_df.select("container_id").distinct()
            return metrics.join(F.broadcast(container_ids), on=["container_id"], how="inner")

    def filter_channel_tags(self, spark, db, container_df, selectors) -> DataFrame:
        """
        Stage 3: Filter channels by tags and compute ``selector_id``.

        Extracts leaf selectors, pivots the channel-tags table, filters
        matching channels, and assigns each row its ``selector_id`` so
        that Stage 4 can be a simple passthrough.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        container_df : pyspark.sql.DataFrame
            DataFrame containing container information.
        selectors : list[TimeSeriesSelector]
            Non-aliased (direct) selectors.

        Returns
        -------
        pyspark.sql.DataFrame
            ``(container_id, channel_id, selector_id)``
        """
        container_id_col = self.config.container_id_col
        channel_id_col = self.config.channel_id_col
        mids = container_df.select(container_id_col).distinct()

        if len(selectors) == 0:
            return self._empty_channel_match_df(spark)

        required_tags = set()
        for selector in selectors:
            required_tags.update(selector.required_tags())

        tbl = db.channel_tags(spark)
        tbl = self._apply_column_mapping(tbl, self.config.channel_tags.column_name_mapping)
        expr = self._build_expr(selectors)

        tags = (
            tbl.where(F.col("key").isin(required_tags))
            .join(F.broadcast(mids), on=[container_id_col], how="inner")
            .groupBy(container_id_col, channel_id_col)
            .pivot("key", list(required_tags))
            .agg(F.first(F.col("value")))
            .where(expr)
        )
        tags = tags.withColumn("selector_id", self._build_selector_id_expr(selectors))
        return tags.select(container_id_col, channel_id_col, "selector_id")

    def filter_channel_metrics(self, spark, db, channel_df, selectors) -> DataFrame:
        """
        Stage 4: Join with ``channel_metrics`` to restrict to channels that
        have metric entries.

        The input *channel_df* already carries a ``selector_id`` column
        from Stage 3.  This stage inner-joins it with the channel-metrics
        table so that channels without any recorded samples are excluded,
        then wraps ``selector_id`` into an array ``selector_ids``.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        channel_df : pyspark.sql.DataFrame
            DataFrame from :meth:`filter_channel_tags` with columns
            ``(container_id, channel_id, selector_id)``.
        selectors : list[TimeSeriesSelector]
            Non-aliased selectors (unused — selector_id comes from Stage 3).

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with ``(container_id, channel_id, selector_ids)``
            where ``selector_ids`` is an array column.
        """
        container_id_col = self.config.container_id_col
        channel_id_col = self.config.channel_id_col
        tbl = db.channel_metrics(spark)
        tbl = self._apply_column_mapping(tbl, self.config.channel_metrics.column_name_mapping)
        metrics = tbl.select(container_id_col, channel_id_col).join(
            F.broadcast(channel_df),
            on=[container_id_col, channel_id_col],
            how="inner",
        )
        metrics = metrics.withColumn("selector_ids", F.array(F.col("selector_id"))).drop(
            "selector_id"
        )
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
        q = self._apply_column_mapping(q, self.config.channels.column_name_mapping)

        if self.is_raw_data:
            # Calculate the tend info and prepare the data for the solving step.
            q = self.interval_encoder.prepare_channels_df(q)

        schema_entries = [T.StructField(self.config.container_id_col, T.LongType())]
        for s, dtype in zip(selections, dtypes, strict=False):
            schema_entries.append(T.StructField(s._alias, dtype))
        schema = T.StructType(schema_entries)

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
