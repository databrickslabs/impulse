from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from mda_query_engine.analyze.metadata.metric_expression import MetricExpression
from mda_query_engine.analyze.metadata.tag_expression import TagExpression

from .basic_narrow_solver import BasicNarrowSolver
from .solver_config import SolverConfig

if TYPE_CHECKING:
    from mda_query_engine.measurement_db import MeasurementDB


class KeyValueStoreSolver(BasicNarrowSolver):
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
    """

    def __init__(self, spark, config: SolverConfig | None = None):
        super().__init__(spark, config=config)

    # ------------------------------------------------------------------
    # Solver stages
    # ------------------------------------------------------------------

    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Filter container tags from the key-value-store table (narrow/EAV format).

        Reads the narrow-format key-value-store table, applies the per-table
        ``column_name_mapping`` to rename physical columns to internal names,
        then applies the top-level ``project_id`` filter and any per-table
        ``container_tags.filters``.  Pivots to wide format if tag filters
        are present.

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
            If no tag filters are present, returns distinct container_ids.
            Otherwise, returns pivoted data with filter expressions applied.
        """
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

        metric_filters = [
            filt for filt in query.filters if isinstance(filt, MetricExpression)
        ]

        if pre_filtered_containers_df is not None:
            metrics = pre_filtered_containers_df
        else:
            metrics = query.db.container_metrics(self.spark)

        metrics = self._apply_column_mapping(
            metrics, self.config.container_metrics.column_name_mapping
        )

        if self.config.project_id is not None:
            metrics = metrics.where(
                F.col(self.config.project_id_col) == self.config.project_id
            )

        for col_name, value in self.config.container_metrics.filters.items():
            metrics = metrics.where(F.col(col_name) == value)

        if len(metric_filters) > 0:
            metrics = metrics.where(self._build_expr(metric_filters))

        return metrics.join(
            F.broadcast(container_df.select(container_id_col)),
            on=container_id_col,
            how="inner",
        ).dropDuplicates([container_id_col])

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
