from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.config.config_parser import ImpulseConfig


class ContainerDimension:
    """Helper class to handle extracted silver container dimensions."""

    @staticmethod
    def get_dimension(
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        config: ImpulseConfig,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """
        Retrieves the configured measurement dimensions for the matching set
        of containers from the silver ``container_metrics`` table.

        Uses the solver filter pipeline (filter_container_tags -> filter_container_metrics)
        to resolve the matching set of containers and their full metrics.
        ``filter_container_metrics`` applies ``container_metrics.column_name_mapping``
        (physical → internal) before returning, so the DataFrame's columns
        are already the internal (post-mapping) names. This method then
        selects exactly the columns listed in ``config.measurement_dimensions``
        — entries must therefore reference the **post-mapping** (internal)
        names, not the physical silver column names. Post-mapping column
        names pass through to gold unchanged.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            The query builder used for the report.
        solver : QuerySolver
            The solver instance to use for query execution.
        config : ImpulseConfig
            The configuration object containing the report configuration.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            A DataFrame containing the selected measurement dimensions for the
            matching set of containers.

        Raises
        ------
        ValueError
            If any column listed in ``config.measurement_dimensions`` is not
            present in the post-mapping ``container_metrics`` DataFrame.
        """
        measurement_dimensions = config.measurement_dimensions

        container_tags_df = solver.filter_container_tags(spark, query)
        df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )

        missing = [c for c in measurement_dimensions if c not in df.columns]
        if missing:
            raise ValueError(
                "Configured measurement_dimensions columns are not present in "
                f"the container_metrics DataFrame: {missing}. Available "
                f"columns: {df.columns}. Note: measurement_dimensions entries "
                "must be the post-mapping (internal) column names, i.e. the "
                "names that exist after container_metrics.column_name_mapping "
                "has been applied. If your physical silver column has a "
                "different name, add it to that mapping and reference the "
                "internal name here."
            )

        return df.select(*measurement_dimensions).transform(
            ContainerDimension._add_config_hash(config)
        )

    @staticmethod
    def _add_config_hash(config: ImpulseConfig) -> Callable[..., "DataFrame"]:
        """
        Adds a configuration hash column to the DataFrame based on the provided configuration.
        This information can be used to track which configuration was used to generate the data.
        Parameters
        ----------
        config : ImpulseConfig
            The configuration object to generate the hash from.
        Returns
        -------
        Callable[..., DataFrame]
            A function that takes a DataFrame and returns a DataFrame with an added config_hash column.
        """

        def _(df: DataFrame) -> DataFrame:
            config_hash = config.model_dump_json().encode("utf-8")
            return df.withColumn("config_hash", F.hash(F.lit(config_hash)))

        return _


class ChannelMappingResolutionDimension:
    """Helper class to handle the channel mapping resolution dimension.

    Persists the result of
    :meth:`QuerySolver.filter_aliased_channel_metrics` so downstream BI
    consumers can join on ``(container_id, channel_id, channel_alias)``
    to recover the physical join keys, alias priority, and resolved unit
    pair.
    """

    @staticmethod
    def get_dimension(
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        aliased_selectors: list[TimeSeriesSelector],
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame | None:
        """
        Compute the channel mapping resolution dimension for the report.

        Returns ``None`` when the report has no aliased selectors — there
        is nothing to resolve, and the persist step is a no-op.

        Otherwise runs the solver's container-side filter pipeline
        (``filter_container_tags`` → ``filter_container_metrics``) so the
        result honors ``pre_filtered_containers_df`` for incremental mode,
        then calls ``filter_aliased_channel_metrics`` with the aliased
        selectors collected from the report's events and aggregations.
        The internal ``selector_ids`` column is dropped since it is a
        runtime artifact and not part of the dimension contract.

        Notes
        -----
        Solver-capability is *not* checked here. If a report has aliased
        selectors, the configured solver must support alias resolution —
        otherwise ``QueryBuilder.solve`` upstream of this call has
        already raised ``NotImplementedError``. We rely on that invariant
        instead of introspecting the solver class.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            The query builder used for the report.
        solver : QuerySolver
            The solver instance to use for query execution.
        aliased_selectors : list[TimeSeriesSelector]
            Aliased selectors (``uses_alias=True``) collected from the
            report's events and aggregations. May be empty.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame or None
            DataFrame with columns
            ``(container_id, channel_id, <metrics-side join keys>,
            channel_alias, alias_priority[, source_unit, target_unit])``,
            or ``None`` if the report has no aliased selectors.
        """
        if not aliased_selectors:
            return None

        container_tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )

        resolved = solver.filter_aliased_channel_metrics(
            spark, query.db, container_df, aliased_selectors
        )

        if "selector_ids" in resolved.columns:
            resolved = resolved.drop("selector_ids")

        return resolved

    @staticmethod
    def get_dimension_for_scopes(
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        changed_aliased_selectors: list[TimeSeriesSelector],
        unchanged_aliased_selectors: list[TimeSeriesSelector],
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame | None:
        """
        Compute the channel mapping resolution dimension honoring the
        report's changed/unchanged definition split.

        Mirrors the scoping the fact pipeline uses: aliases referenced by
        *changed* definitions are resolved over **all** containers
        (``pre_filtered_containers_df=None``), while aliases referenced
        only by *unchanged* definitions stay scoped to
        ``pre_filtered_containers_df``. This keeps incremental runs cheap
        without leaving older containers unresolved when a new alias is
        introduced by a changed definition.

        In full (non-incremental) mode ``pre_filtered_containers_df`` is
        ``None``, so both scopes resolve over all containers and the union
        is equivalent to resolving the combined selector set in one pass.

        Aliases already covered by the changed set are excluded from the
        unchanged set (by ``selector_id``) so the two scopes resolve
        disjoint aliases. A given alias maps to one stable ``selector_id``,
        so disjoint ``selector_id`` sets yield disjoint ``channel_alias``
        values and therefore no ``(container_id, channel_alias)`` collision
        across the two results — which also keeps the downstream Delta
        ``MERGE`` from seeing duplicate source rows for a merge key.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            The query builder used for the report.
        solver : QuerySolver
            The solver instance to use for query execution.
        changed_aliased_selectors : list[TimeSeriesSelector]
            Aliased selectors from changed definitions (resolved over all
            containers). May be empty.
        unchanged_aliased_selectors : list[TimeSeriesSelector]
            Aliased selectors from unchanged definitions (resolved over
            ``pre_filtered_containers_df``). May be empty.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame or None
            The combined resolution dimension, or ``None`` if neither
            scope has aliased selectors.
        """
        changed_ids = {selector.selector_id for selector in changed_aliased_selectors}
        unchanged_only_selectors = [
            selector
            for selector in unchanged_aliased_selectors
            if selector.selector_id not in changed_ids
        ]

        changed_df = ChannelMappingResolutionDimension.get_dimension(
            spark=spark,
            query=query,
            solver=solver,
            aliased_selectors=changed_aliased_selectors,
            pre_filtered_containers_df=None,
        )
        unchanged_df = ChannelMappingResolutionDimension.get_dimension(
            spark=spark,
            query=query,
            solver=solver,
            aliased_selectors=unchanged_only_selectors,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

        if changed_df is None:
            return unchanged_df
        if unchanged_df is None:
            return changed_df

        # ``source_unit`` / ``target_unit`` only appear for selectors whose
        # mapping rows carry them, so the two scopes may differ in columns.
        return changed_df.unionByName(unchanged_df, allowMissingColumns=True)
