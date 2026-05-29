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
            DataFrame containing time series data.  When the column named by
            ``col_map["conv"]`` is present, :meth:`load_blob` multiplies the
            loaded values by that per-channel factor.  All rows of a given
            ``(cid, ch)`` slice are expected to share the same factor.
        col_map : dict[str, str]
            Mapping with keys ``"cid"``, ``"ch"``, ``"ts"``, ``"te"``,
            ``"val"``, ``"conv"`` to the actual column names in *pdf*.  The
            ``"conv"`` column is optional in *pdf*.
        """
        self._cid_col = col_map["cid"]
        self._ch_col = col_map["ch"]
        self._ts_col = col_map["ts"]
        self._te_col = col_map["te"]
        self._val_col = col_map["val"]
        self._conv_col = col_map.get("conv")
        self._has_conversion = self._conv_col is not None and self._conv_col in pdf.columns

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

        When the underlying *pdf* carries a conversion-factor column (the
        column named by ``col_map["conv"]``) **and** the caller is an
        aliased selector (``uses_alias=True``), the returned values are
        multiplied by that factor.  Direct selectors on the same physical
        channel always receive raw values — unit conversion is a property
        of the alias, not of the channel.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.
        uses_alias : bool, optional
            ``True`` when the calling selector resolved via channel_mapping.
            Gates the per-channel conversion factor; defaults to ``False``.

        Returns
        -------
        SampleSeries
            The loaded sample series object.
        """
        s = self.pdf[(self.pdf[self._cid_col] == mid) & (self.pdf[self._ch_col] == cid)]
        values = s[self._val_col]
        if self._has_conversion and len(s) > 0 and uses_alias:
            factor = s[self._conv_col].iloc[0]
            if pd.notna(factor):
                values = values * factor
        return SampleSeries(s[self._ts_col], s[self._te_col], values)


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

        When the database is configured with a ``unit_conversion_table`` and
        the ``channel_mapping`` table carries ``source_unit`` / ``target_unit``
        columns, this method also propagates the effective unit pair on each
        resolved row.  The effective ``source_unit`` is computed as
        ``COALESCE(channel_metrics.unit, channel_mapping.source_unit)`` so
        that the authoritative per-channel physical unit on
        ``channel_metrics`` takes precedence over the mapping-level default
        when present.  ``target_unit`` is always taken from the mapping —
        there is no analogous column on ``channel_metrics``.

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
            DataFrame with
            ``(container_id, channel_id, <metrics-side join keys>,
            channel_alias, alias_priority, selector_ids)`` where
            ``selector_ids`` is an array column.  The metrics-side join key
            columns come from ``effective_alias_join_keys`` (default:
            ``channel_name``, ``data_key``) and are deduplicated in case the
            same physical column appears on both sides of a join-key tuple.
            When unit conversion is active (see above), also carries
            ``source_unit`` and ``target_unit`` columns.
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

        channel_metrics = db.channel_metrics(spark)
        channel_metrics = self._apply_column_mapping(
            channel_metrics, self.config.channel_metrics.column_name_mapping
        )
        channel_metrics = channel_metrics.join(
            F.broadcast(container_df.select(container_id_col)),
            on=[container_id_col],
            how="inner",
        )
        alias_priority_col = self.config.alias_priority_col
        channel_alias_col = self.config.channel_alias_col
        join_keys = self.config.effective_alias_join_keys

        source_unit_col = self.config.source_unit_col
        target_unit_col = self.config.target_unit_col
        unit_col = self.config.unit_col
        has_unit_cols = (
            db.config.unit_conversion_table is not None
            and source_unit_col in resolved_mapping.columns
            and target_unit_col in resolved_mapping.columns
        )
        metrics_has_unit = unit_col in channel_metrics.columns

        # Mapping-side projection: one aliased copy per mapping_col plus the
        # alias / priority columns (and the optional unit columns, aliased
        # with the ``_map_`` prefix so we can coalesce the source unit with
        # ``channel_metrics.unit`` after the join).
        mapping_select_cols = [
            F.col(mapping_col).alias(f"_map_{mapping_col}") for mapping_col, _ in join_keys
        ]
        mapping_select_cols.extend([F.col(channel_alias_col), F.col(alias_priority_col)])
        if has_unit_cols:
            mapping_select_cols.extend(
                [
                    F.col(source_unit_col).alias("_map_source_unit"),
                    F.col(target_unit_col).alias("_map_target_unit"),
                ]
            )

        resolved = channel_metrics.join(
            resolved_mapping.select(*mapping_select_cols),
            on=[
                channel_metrics[metrics_col] == F.col(f"_map_{mapping_col}")
                for mapping_col, metrics_col in join_keys
            ],
            how="inner",
        )

        # Materialize the effective source_unit / target_unit. The source unit
        # comes from ``channel_metrics.unit`` when present (authoritative
        # physical unit of the channel) and falls back to the mapping
        # ``source_unit`` otherwise.  The target unit is always taken from
        # the mapping — there is no per-channel "target" on
        # ``channel_metrics``; the target is a user choice on the alias.
        if has_unit_cols:
            if metrics_has_unit:
                resolved = resolved.withColumn(
                    source_unit_col,
                    F.coalesce(channel_metrics[unit_col], F.col("_map_source_unit")),
                )
            else:
                resolved = resolved.withColumn(source_unit_col, F.col("_map_source_unit"))
            resolved = resolved.withColumn(target_unit_col, F.col("_map_target_unit"))

        dedup_window = Window.partitionBy(container_id_col, channel_alias_col).orderBy(
            F.col(alias_priority_col).asc_nulls_last()
        )
        resolved = resolved.withColumn("_rank", F.row_number().over(dedup_window))
        resolved = resolved.where(F.col("_rank") == 1).drop("_rank")

        resolved = resolved.withColumn(
            "selector_ids", F.array(self._build_selector_id_expr(selectors))
        )
        join_key_metrics_cols = list(dict.fromkeys(metrics_col for _, metrics_col in join_keys))
        out_cols = [
            container_id_col,
            channel_id_col,
            *join_key_metrics_cols,
            channel_alias_col,
            alias_priority_col,
            "selector_ids",
        ]
        if has_unit_cols:
            out_cols.extend([source_unit_col, target_unit_col])
        return resolved.select(*out_cols)

    def resolve_channel_selections(
        self, spark, channel_metrics_df, aliased_channel_metrics_df
    ) -> DataFrame:
        """
        Union direct and aliased channel metrics, combining selector_ids.

        When the aliased side carries ``source_unit`` / ``target_unit``
        columns (added by :meth:`filter_aliased_channel_metrics` when a
        unit conversion table is configured), those columns are preserved
        through the union and aggregation.  Direct selectors produce null
        unit columns, which causes the downstream conversion-factor join
        in :meth:`solve` to leave their values unchanged.

        Validates that each ``(container_id, channel_id)`` carries at most
        one distinct ``source_unit`` and one distinct ``target_unit``.  Per
        physical channel the unit-conversion model can attach only one
        factor; conflicting aliases would otherwise pick an arbitrary
        target and silently mis-convert one of them.

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
            Merged DataFrame with ``(container_id, channel_id, selector_ids)``
            (plus ``source_unit`` / ``target_unit`` when present on the
            aliased side).

        Raises
        ------
        ValueError
            If two or more aliased selectors resolve to the same physical
            channel with conflicting ``source_unit`` or ``target_unit``
            values.  Up to three offending channels are listed in the
            message.
        """
        source_unit_col = self.config.source_unit_col
        target_unit_col = self.config.target_unit_col
        has_unit_cols = (
            source_unit_col in aliased_channel_metrics_df.columns
            and target_unit_col in aliased_channel_metrics_df.columns
        )

        # ``filter_aliased_channel_metrics`` emits extra columns
        # (metrics-side join keys, channel_alias, alias_priority) for the
        # channel mapping resolution dimension; the solve pipeline only
        # consumes (container_id, channel_id, selector_ids[, source_unit,
        # target_unit]) and unionByName requires matching schemas.
        aliased_solve_cols = [
            self.config.container_id_col,
            self.config.channel_id_col,
            "selector_ids",
        ]
        if has_unit_cols:
            aliased_solve_cols.extend([source_unit_col, target_unit_col])
        aliased_for_union = aliased_channel_metrics_df.select(*aliased_solve_cols)

        merged = channel_metrics_df.unionByName(
            aliased_for_union, allowMissingColumns=has_unit_cols
        )

        agg_exprs = [F.flatten(F.collect_list("selector_ids")).alias("selector_ids")]
        if has_unit_cols:
            # collect_set serves a dual purpose: (a) it deduplicates so we
            # can detect a conflict by size > 1, and (b) the single
            # remaining element materializes the scalar unit value the
            # downstream code expects.
            agg_exprs.append(F.collect_set(source_unit_col).alias("_source_units"))
            agg_exprs.append(F.collect_set(target_unit_col).alias("_target_units"))

        grouped = merged.groupBy(
            self.config.container_id_col,
            self.config.channel_id_col,
        ).agg(*agg_exprs)

        if has_unit_cols:
            # TODO(unit-conversion): lift this limitation by attaching the
            # conversion factor to the selector instead of the channel row
            # (see PR #30 review).
            conflicts = (
                grouped.where((F.size("_source_units") > 1) | (F.size("_target_units") > 1))
                .select(
                    self.config.container_id_col,
                    self.config.channel_id_col,
                    "_source_units",
                    "_target_units",
                )
                .limit(3)
                .collect()
            )
            if conflicts:
                details = [
                    f"(container_id={row[self.config.container_id_col]}, "
                    f"channel_id={row[self.config.channel_id_col]}): "
                    f"source_units={sorted(row['_source_units'])}, "
                    f"target_units={sorted(row['_target_units'])}"
                    for row in conflicts
                ]
                raise ValueError(
                    "Conflicting unit conversions on the same physical channel "
                    "(first 3 shown):\n" + "\n".join(details)
                )
            # Empty sets (direct-only channels) yield null via
            # try_element_at, matching the prior F.first(ignorenulls=True)
            # behavior.  Plain element_at raises on empty arrays in Spark 4.
            grouped = (
                grouped.withColumn(source_unit_col, F.try_element_at("_source_units", F.lit(1)))
                .withColumn(target_unit_col, F.try_element_at("_target_units", F.lit(1)))
                .drop("_source_units", "_target_units")
            )

        return grouped

    # ------------------------------------------------------------------
    # Unit conversion
    # ------------------------------------------------------------------

    def _validate_unit_conversion_table(self, uc_table: DataFrame) -> None:
        """Raise ``ValueError`` if the unit_conversion table contains rows
        whose ``conversion_factor`` is null, zero, or negative.

        ``conversion_factor`` is conceptually a strictly-positive number.
        A zero on the source side silently corrupts values to all-zero;
        a zero on the target side raises a cryptic Spark
        ``ArithmeticException`` deep in the conversion path under Spark 4
        ANSI mode; a negative value flips signs; a null silently skips
        conversion (contract violation, not corruption).  Catching all
        four cases here turns each into a clear, actionable error
        naming the offending row.

        Parameters
        ----------
        uc_table : pyspark.sql.DataFrame
            The ``unit_conversion`` table **after**
            ``_apply_column_mapping`` has been applied.

        Raises
        ------
        ValueError
            If any row has ``conversion_factor IS NULL`` or
            ``conversion_factor <= 0``.  Up to three offending rows are
            listed in the message.
        """
        unit_col = self.config.unit_col
        group_id_col = self.config.group_id_col
        factor_col = self.config.conversion_factor_col

        bad_rows = (
            uc_table.where(F.col(factor_col).isNull() | (F.col(factor_col) <= 0))
            .select(group_id_col, unit_col, factor_col)
            .limit(3)
            .collect()
        )
        if bad_rows:
            details = [
                f"(group_id={row[group_id_col]}, unit={row[unit_col]}, "
                f"conversion_factor={row[factor_col]})"
                for row in bad_rows
            ]
            raise ValueError(
                "Invalid conversion_factor in unit_conversion table "
                "(must be a positive non-null number; first 3 shown):\n" + "\n".join(details)
            )

    def _compute_conversion_factors(self, spark, query, channels_df: DataFrame) -> DataFrame:
        """
        Join *channels_df* with the unit conversion table to compute a
        per-channel combined conversion factor.

        The unit conversion table associates each unit with a base-unit
        scaling factor inside a unit family (``group_id``).  For a row with
        ``source_unit = S``, ``target_unit = T`` belonging to family ``G``:

        - ``_src_factor`` converts a value in ``S`` to the base unit of ``G``.
        - ``_tgt_factor`` converts a value in ``T`` to the base unit of ``G``.
        - The combined factor that converts ``S`` to ``T`` is
          ``_src_factor / _tgt_factor``.

        Rows whose source or target unit is missing on the table — or whose
        source/target units belong to different families — receive a null
        factor.  Null factors are treated as "no conversion" by the cache.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        query : QueryBuilder
            Query object carrying the configured ``db``.
        channels_df : pyspark.sql.DataFrame
            DataFrame that already carries ``source_unit`` / ``target_unit``
            columns (added by :meth:`filter_aliased_channel_metrics`).

        Returns
        -------
        pyspark.sql.DataFrame
            *channels_df* augmented with a ``conversion_factor`` column.

        Raises
        ------
        ValueError
            If the ``unit_conversion`` table contains a row with a null,
            zero, or negative ``conversion_factor``.  See
            :meth:`_validate_unit_conversion_table` for the underlying
            check.
        """
        uc_table = query.db.unit_conversion(spark)
        uc_table = self._apply_column_mapping(
            uc_table, self.config.unit_conversion.column_name_mapping
        )
        self._validate_unit_conversion_table(uc_table)

        unit_col = self.config.unit_col
        group_id_col = self.config.group_id_col
        factor_col = self.config.conversion_factor_col
        source_unit_col = self.config.source_unit_col
        target_unit_col = self.config.target_unit_col

        # Source-side join: fetch _src_factor and _src_group_id.
        channels_df = channels_df.join(
            F.broadcast(
                uc_table.select(
                    F.col(unit_col).alias("_src_unit"),
                    F.col(factor_col).alias("_src_factor"),
                    F.col(group_id_col).alias("_src_group_id"),
                )
            ),
            on=[channels_df[source_unit_col] == F.col("_src_unit")],
            how="left",
        ).drop("_src_unit")

        # Target-side join: must belong to the same unit family.
        channels_df = channels_df.join(
            F.broadcast(
                uc_table.select(
                    F.col(unit_col).alias("_tgt_unit"),
                    F.col(factor_col).alias("_tgt_factor"),
                    F.col(group_id_col).alias("_tgt_group_id"),
                )
            ),
            on=[
                channels_df[target_unit_col] == F.col("_tgt_unit"),
                F.col("_src_group_id") == F.col("_tgt_group_id"),
            ],
            how="left",
        ).drop("_tgt_unit", "_tgt_group_id")

        channels_df = channels_df.withColumn(
            factor_col,
            F.col("_src_factor") / F.col("_tgt_factor"),
        ).drop("_src_factor", "_src_group_id", "_tgt_factor")

        return channels_df

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

        When a ``unit_conversion_table`` is configured on the database and
        *channels_df* carries ``source_unit`` / ``target_unit`` columns
        (added upstream by :meth:`filter_aliased_channel_metrics`),
        per-channel conversion factors are computed and propagated into
        the grouped-map UDF so that time-series values are converted from
        the source to the target unit on the fly.

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
        source_unit_col = self.config.source_unit_col
        target_unit_col = self.config.target_unit_col

        has_conversion_table = getattr(query.db.config, "unit_conversion_table", None) is not None
        has_unit_cols = (
            source_unit_col in channels_df.columns and target_unit_col in channels_df.columns
        )

        if has_conversion_table and has_unit_cols:
            channels_df = self._compute_conversion_factors(self.spark, query, channels_df)

        for col_name in (source_unit_col, target_unit_col):
            if col_name in channels_df.columns:
                channels_df = channels_df.drop(col_name)

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
