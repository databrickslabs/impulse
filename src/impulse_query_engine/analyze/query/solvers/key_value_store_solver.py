from __future__ import annotations

from collections.abc import Iterable
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Window

from impulse_query_engine.analyze.metadata.metric_expression import MetricExpression
from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.model.series.sample_series import SampleSeries

from .query_solver import QuerySolver
from .series_cache import SeriesCache
from .solver_config import SolverConfig
from .utils.interval_encoder import IntervalEncoder

if TYPE_CHECKING:
    from impulse_query_engine.measurement_db import MeasurementDB


class KVSTimeSeriesCache(SeriesCache):
    def __init__(self, pdf, col_map: dict[str, str]):
        """
        Initialize the KVSTimeSeriesCache.

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


class KeyValueStoreSolver(QuerySolver):
    """
    Solver for querying container metadata from a narrow/EAV key-value-store table.

    This solver reads container tags from a narrow-format table where each
    attribute is stored as a separate row (entity_id, element_id, value) and
    pivots it to wide format for filtering. It then filters the container_metrics
    table and resolves channel aliases via the channel_mapping table.

    Physical column names that differ from the framework-internal names are
    translated via per-table ``column_name_mapping`` entries at the point
    where each table is read.  All subsequent processing uses the internal
    column names exposed by :class:`SolverConfig`.

    Parameters
    ----------
    spark : SparkSession
        Spark session used for query execution.
    config : SolverConfig or None
        Optional configuration.  When *None* (default) no filtering by
        project or toolbox is applied.
    is_raw_data : bool, optional
        Whether the input data is raw point data (timestamp column)
        rather than RLE format (tstart/tend columns).
    drop_implausible_data : bool, optional
        Whether to drop data points marked as implausible before
        processing.  Requires an ``is_plausible`` column in the
        silver layer.
    """

    def __init__(
        self,
        spark,
        config: SolverConfig | None = None,
        is_raw_data: bool = False,
        drop_implausible_data: bool = False,
    ):
        super().__init__(config=config)
        self.spark = spark
        self.is_raw_data = is_raw_data
        self.drop_implausible_data: bool = drop_implausible_data
        self.interval_encoder: IntervalEncoder = IntervalEncoder(
            timestamp_col_name="timestamp",
            drop_implausible_data_points=self.drop_implausible_data,
        )

    # ------------------------------------------------------------------
    # Solver stages
    # ------------------------------------------------------------------

    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Filter container tags from the key-value-store table (narrow/EAV format).

        If no ``container_tags_table`` is configured on the database, this
        stage is a no-op and an empty DataFrame is returned: the solver is
        operating on a wide-only data model (no narrow container_tags table).

        Otherwise, reads the narrow-format key-value-store table, applies the
        per-table ``column_name_mapping`` to rename physical columns to
        internal names, then applies the top-level ``project_id`` filter
        and any per-table ``container_tags.filters``.  Pivots to wide format
        if tag filters are present.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            The query object containing filters and db info.

        Returns
        -------
        DataFrame
            A DataFrame containing the filtered container_ids.
            If no ``container_tags_table`` is configured, an empty DataFrame.
            If no tag filters are present, returns distinct container_ids.
            Otherwise, returns pivoted data with filter expressions applied.
        """
        if query.db.config.container_tags_table is None:
            return spark.createDataFrame([], schema=T.StructType([]))

        container_id_col = self.config.container_id_col

        filters = []
        required_elements = []
        for filt in query.filters:
            if isinstance(filt, TagExpression):
                filters.append(filt)
                required_elements.extend(filt.required_tags())
        required_elements = set(required_elements)

        tags = query.db.container_tags(self.spark)
        tags = self._apply_column_mapping(tags, self.config.container_tags.column_name_mapping)

        if self.config.project_id is not None:
            tags = tags.where(F.col(self.config.project_id_col) == self.config.project_id)

        for col_name, value in self.config.container_tags.filters.items():
            tags = tags.where(F.col(col_name) == value)

        if len(filters) == 0:
            return tags.select(container_id_col).distinct()

        tag_key_col = self.config.tag_key_col
        tags = tags.where(F.col(tag_key_col).isin(required_elements))

        tags = tags.groupBy(container_id_col)
        tags = tags.pivot(tag_key_col, list(required_elements)).agg(
            F.first(self.config.tag_value_col)
        )

        expr = self._build_expr(filters)
        tags = tags.where(expr)

        return tags.select(container_id_col).distinct()

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Filter container_metrics and join with tag-filtered container IDs.

        Reads the ``container_metrics`` table, applies the per-table
        ``column_name_mapping`` to rename physical columns to internal names,
        applies the top-level ``project_id`` filter, any per-table
        ``container_metrics.filters``, and any ``MetricExpression`` filters
        extracted from the query.  Finally, inner-joins the result with the
        tag-filtered container DataFrame.

        If no ``container_tags_table`` is configured on the database, the
        join with ``container_df`` is skipped: stage 1 produced no
        container IDs because no narrow tag table exists.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame containing tag-filtered container IDs (output of
            :meth:`filter_container_tags`).
        pre_filtered_containers_df : pyspark.sql.DataFrame, optional
            Pre-filtered container_metrics DataFrame.  When provided, it
            replaces the read from ``query.db.container_metrics``.

        Returns
        -------
        pyspark.sql.DataFrame
            Filtered container metrics with all original columns preserved.
            Deduplicated by ``container_id``.
        """
        container_id_col = self.config.container_id_col

        metric_filters = [filt for filt in query.filters if isinstance(filt, MetricExpression)]

        if pre_filtered_containers_df is not None:
            metrics = pre_filtered_containers_df
        else:
            metrics = query.db.container_metrics(self.spark)

        metrics = self._apply_column_mapping(
            metrics, self.config.container_metrics.column_name_mapping
        )

        if self.config.project_id is not None:
            metrics = metrics.where(F.col(self.config.project_id_col) == self.config.project_id)

        for col_name, value in self.config.container_metrics.filters.items():
            metrics = metrics.where(F.col(col_name) == value)

        if len(metric_filters) > 0:
            metrics = metrics.where(self._build_expr(metric_filters))

        if query.db.config.container_tags_table is None:
            return metrics.dropDuplicates([container_id_col])

        return metrics.join(
            F.broadcast(container_df.select(container_id_col)),
            on=container_id_col,
            how="inner",
        ).dropDuplicates([container_id_col])

    def filter_channel_tags(self, spark, db, container_df, selectors) -> DataFrame:
        """
        Pass through container DataFrame.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        container_df : pyspark.sql.DataFrame
            DataFrame containing container information.
        selectors : list[TimeSeriesSelector]
            Non-aliased selectors (unused by this solver).

        Returns
        -------
        pyspark.sql.DataFrame
            The input container DataFrame.
        """
        return container_df

    def filter_channel_metrics(self, spark, db, container_df, selectors) -> DataFrame:
        """
        Filter channels by metrics and required tags.

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
            DataFrame with ``(container_id, channel_id, selector_ids)``.
        """
        container_id_col = self.config.container_id_col
        channel_id_col = self.config.channel_id_col
        channel_metrics_df = db.channel_metrics(spark)
        channel_metrics_df = self._apply_column_mapping(
            channel_metrics_df, self.config.channel_metrics.column_name_mapping
        )
        if len(selectors) == 0:
            return self._empty_channel_match_df(spark)

        channel_metrics_df = channel_metrics_df.where(self._build_expr(selectors))
        result = channel_metrics_df.join(
            F.broadcast(container_df.select(container_id_col)),
            on=[container_id_col],
            how="inner",
        )
        result = result.withColumn(
            "selector_ids", F.array(self._build_selector_id_expr(selectors))
        )
        return result.select(container_id_col, channel_id_col, "selector_ids")

    def filter_aliased_channel_metrics(
        self, spark, db: MeasurementDB, container_df, selectors
    ) -> DataFrame:
        """
        Resolve aliased channel selections via the channel_mapping table.

        Applies the per-table ``column_name_mapping`` to rename physical
        columns, then applies the top-level ``project_id`` filter and any
        per-table ``channel_mapping.filters``, and finally joins with
        channel_metrics to resolve aliases.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        container_df : pyspark.sql.DataFrame
            DataFrame containing tag-filtered container IDs.
        selectors : list[TimeSeriesSelector]
            Aliased selectors extracted from the query.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with ``(container_id, channel_id, selector_ids)``
            where ``selector_ids`` is an array column.
        """
        container_id_col = self.config.container_id_col
        channel_id_col = self.config.channel_id_col

        if len(selectors) == 0:
            return self._empty_channel_match_df(spark)

        channel_mapping = db.channel_mapping(spark)
        channel_mapping = self._apply_column_mapping(
            channel_mapping, self.config.channel_mapping.column_name_mapping
        )

        if self.config.project_id is not None:
            channel_mapping = channel_mapping.where(
                F.col(self.config.project_id_col) == self.config.project_id
            )

        for col_name, value in self.config.channel_mapping.filters.items():
            channel_mapping = channel_mapping.where(F.col(col_name) == value)

        resolved_mapping = channel_mapping.where(self._build_expr(selectors))

        channel_metrics = db.channel_metrics(spark).join(
            F.broadcast(container_df.select(container_id_col)),
            on=[container_id_col],
            how="inner",
        )
        alias_priority_col = self.config.alias_priority_col

        resolved = channel_metrics.join(
            resolved_mapping.select(
                F.col("source_channel").alias("_map_source_channel"),
                F.col("data_key").alias("_map_data_key"),
                F.col("channel_alias"),
                F.col(alias_priority_col),
            ),
            on=[
                channel_metrics["channel_name"] == F.col("_map_source_channel"),
                channel_metrics["data_key"] == F.col("_map_data_key"),
            ],
            how="inner",
        )

        dedup_window = Window.partitionBy(container_id_col, "channel_alias").orderBy(
            F.col(alias_priority_col).asc_nulls_last()
        )
        resolved = resolved.withColumn("_rank", F.row_number().over(dedup_window))
        resolved = resolved.where(F.col("_rank") == 1).drop("_rank")

        resolved = resolved.withColumn(
            "selector_ids", F.array(self._build_selector_id_expr(selectors))
        )
        return resolved.select(container_id_col, channel_id_col, "selector_ids")

    def resolve_channel_selections(
        self, spark, channel_metrics_df, aliased_channel_metrics_df
    ) -> DataFrame:
        """
        Union direct and aliased channel metrics, combining selector_ids.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        channel_metrics_df : pyspark.sql.DataFrame
            Direct channel metrics with ``selector_ids`` array column.
        aliased_channel_metrics_df : pyspark.sql.DataFrame
            Aliased channel metrics with ``selector_ids`` array column.

        Returns
        -------
        pyspark.sql.DataFrame
            Merged DataFrame with ``(container_id, channel_id, selector_ids)``.
        """
        merged = channel_metrics_df.unionByName(aliased_channel_metrics_df)
        return merged.groupBy(
            self.config.container_id_col,
            self.config.channel_id_col,
        ).agg(F.flatten(F.collect_list("selector_ids")).alias("selector_ids"))

    # ------------------------------------------------------------------
    # Solve
    # ------------------------------------------------------------------

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
        cache = KVSTimeSeriesCache(pdf, col_map=col_map)
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
        q = self._apply_column_mapping(q, self.config.channels.column_name_mapping)

        if self.is_raw_data:
            # Calculate the tend info and prepare the data for the solving step.
            q = self.interval_encoder.prepare_channels_df(q)

        schema_entries = [T.StructField(self.config.container_id_col, T.LongType())]
        for s, dtype in zip(selections, dtypes, strict=False):
            schema_entries.append(T.StructField(s._alias, dtype))
        schema = T.StructType(schema_entries)
        solve_udf = F.pandas_udf(
            partial(KeyValueStoreSolver._solve_udf, selections=selections, col_map=col_map),
            schema,
            F.PandasUDFType.GROUPED_MAP,
        )
        df = q.join(
            F.broadcast(channels_df), on=[self.config.container_id_col, self.config.channel_id_col]
        )

        container_count = channels_df.select(self.config.container_id_col).distinct().count()
        if container_count == 0:
            return self.spark.createDataFrame([], schema=schema)
        res = (
            df.repartition(container_count, self.config.container_id_col)
            .groupBy(self.config.container_id_col)
            .apply(solve_udf)
        )
        return res
