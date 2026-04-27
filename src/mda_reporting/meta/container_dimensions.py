from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.config.config_parser import MdaConfig, MeasurementDimensions


class ContainerDimension:
    """Helper class to handle extracted silver container dimensions."""

    @staticmethod
    def get_dimension(
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        config: MdaConfig,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """
        Retrieves meta dimensions for the specified units under test (UUTs) from the silver container_metrics table.

        Uses the solver filter pipeline (filter_container_tags -> filter_container_metrics)
        to resolve the matching set of containers and their full metrics.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            The query builder used for the report.
        solver : QuerySolver
            The solver instance to use for query execution.
        config : MdaConfig
            The configuration object containing the report configuration.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            A DataFrame containing the selected measurement dimensions for the specified UUTs.
        """
        measurement_dimensions = config.measurement_dimensions

        desired_container_metrics_columns = [
            dimension.get_column() for dimension in measurement_dimensions
        ]

        container_tags_df = solver.filter_container_tags(spark, query)
        df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )
        df_renamed = ContainerDimension._rename_dimension_cols(df, measurement_dimensions)
        return df_renamed.select(*desired_container_metrics_columns).transform(
            ContainerDimension._add_config_hash(config)
        )

    @staticmethod
    def _rename_dimension_cols(
        df: DataFrame, measurement_dimensions: list[MeasurementDimensions]
    ) -> DataFrame:
        """
        Renames the columns of the DataFrame to match the er gold layer schema.

        Parameters
        ----------
        df : DataFrame
            The DataFrame containing the measurement dimensions.
        measurement_dimensions : list[MeasurementDimensions]
            List of measurement dimension columns to rename.
        Returns
        -------
        DataFrame
            A DataFrame with renamed columns based on the measurement dimensions.
        """
        renamed_columns = []
        for column_name in measurement_dimensions:
            silver_layer_name = MeasurementDimensions(column_name).map_gold_name_to_silver()
            if silver_layer_name in df.columns:
                renamed_columns.append(F.col(silver_layer_name).alias(column_name.value))

        return df.select(*renamed_columns)

    @staticmethod
    def _add_config_hash(config: MdaConfig) -> Callable[..., "DataFrame"]:
        """
        Adds a configuration hash column to the DataFrame based on the provided configuration.
        This information can be used to track which configuration was used to generate the data.
        Parameters
        ----------
        config : MdaConfig
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
