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
        config: ImpulseConfig,
        aliased_selectors: list[TimeSeriesSelector],
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame | None:
        """
        Compute the channel mapping resolution dimension for the report.

        Returns ``None`` when the dimension cannot be built — either the
        solver does not override ``filter_aliased_channel_metrics`` (e.g.
        ``DeltaSolver``, ``BlobSolver``, ``InMemorySolver``) or the report
        has no aliased selectors. In both cases the persist step should be
        a no-op.

        When buildable, runs the solver's container-side filter pipeline
        (``filter_container_tags`` → ``filter_container_metrics``) so the
        result honors ``pre_filtered_containers_df`` for incremental mode,
        then calls ``filter_aliased_channel_metrics`` with the aliased
        selectors collected from the report's events and aggregations.
        The internal ``selector_ids`` column is dropped since it is a
        runtime artifact and not part of the dimension contract.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            The query builder used for the report.
        solver : QuerySolver
            The solver instance to use for query execution.
        config : ImpulseConfig
            The configuration object; used to attach ``config_hash``.
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
            channel_alias, alias_priority[, source_unit, target_unit],
            config_hash)``, or ``None`` if the dimension is not
            applicable.
        """
        if (
            type(solver).filter_aliased_channel_metrics
            is QuerySolver.filter_aliased_channel_metrics
        ):
            return None

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

        return resolved.transform(ContainerDimension._add_config_hash(config))
