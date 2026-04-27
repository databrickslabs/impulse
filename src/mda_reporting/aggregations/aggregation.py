from __future__ import annotations

import abc
from abc import ABC

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType

from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.events.event import Event


class Aggregation(ABC):
    """Abstract base class for report aggregations."""

    def __init__(self, name):
        """
        Initialize an Aggregation object.

        Parameters
        ----------
        name : str
            Name of the aggregation.
        """
        self.name = name
        self.page_number = -1  # Default value indicating no page assigned
        self.report_id = -1  # Default value indicating no report assigned

    def set_page_number(self, page_number: int):
        """
        Set the page number for the aggregation.

        Parameters
        ----------
        page_number : int
            The page number to set.

        Returns
        -------
        None
        """
        self.page_number = page_number

    def set_report_id(self, report_id: int):
        """
        Set the report ID for the aggregation.

        Parameters
        ----------
        report_id : int
            The report identifier to set.

        Returns
        -------
        None
        """
        self.report_id = report_id

    @abc.abstractmethod
    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the aggregation.

        Returns
        -------
        dict
            Dictionary containing aggregation metadata.
        """
        pass

    def get_name(self) -> str:
        """
        Get the name of the aggregation.

        Returns
        -------
        str
            The name of the aggregation.
        """
        return self.name

    @abc.abstractmethod
    def get_id(self) -> int:
        """
        Get a unique identifier for the aggregation.

        Returns
        -------
        int
            Unique identifier for the aggregation.
        """
        pass

    @abc.abstractmethod
    def get_event(self) -> Event:
        """
        Get the corresponding event for the aggregation.

        Returns
        -------
        Event
            The event associated with the aggregation.
        """
        pass

    @abc.abstractmethod
    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the aggregation.

        Returns
        -------
        Row
            Spark Row containing aggregation metadata.
        """
        pass

    @abc.abstractmethod
    def determine_definition_hash(self) -> int:
        """
        Calculate hash from result-relevant instance attributes only.

        This hash is used to detect when an aggregation's computation logic
        has changed, requiring full reprocessing of all containers.

        The hash should only include attributes that affect computation results,
        excluding UI/metadata attributes like name, description, signal_name,
        units, and page_number.

        MUST INCLUDE: base_expr, bins, event (if present)
        MUST EXCLUDE: name, desc, signal_name, units, page_number, report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def determine_aggregations(
        cls,
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        aggregations: list[Aggregation],
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Build a Spark DataFrame of aggregation facts.

        Parameters
        ----------
        spark : SparkSession
          Spark session for data processing.
        query : QueryBuilder
          Query builder for constructing aggregation queries.
        solver : QuerySolver
          Query solver for executing queries.
        aggregations : list of Aggregation
          List of Aggregation objects to process.
        pre_filtered_containers_df : DataFrame, optional
          Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
          Spark DataFrame containing aggregation instance facts.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def determine_metadata_df(cls, spark: SparkSession, histograms: list[Aggregation]):
        """
        Create a Spark DataFrame containing aggregation metadata.

        Parameters
        ----------
        spark : SparkSession
           Spark session for data processing.
        histograms : list of Aggregation
           List of Aggregation objects.

        Returns
        -------
        DataFrame
           Spark DataFrame containing aggregation metadata.
        """
        pass

    @staticmethod
    def get_visual_id_column(aggregations: list[Aggregation], hist_name: str) -> Column:
        """
        Generates a Spark SQL expression that maps visual (aggregation) names to their corresponding IDs.

        Parameters
        ----------
        aggregations : list of Aggregation
            List of Aggregation objects to map from.
        hist_name : str
            The name of the column containing visual (aggregation) names.

        Returns
        -------
        pyspark.sql.Column
            A Spark SQL Column expression mapping names to IDs, or None if no match is found.
        """
        col_expr = None
        for agg in aggregations:
            aggregation_id = agg.get_id()

            if aggregation_id is None:
                continue
            elif col_expr is None:
                col_expr = f.when(f.col(hist_name) == f.lit(agg.get_name()), f.lit(aggregation_id))
            else:
                col_expr = col_expr.when(
                    f.col(hist_name) == f.lit(agg.get_name()), f.lit(aggregation_id)
                )
        return (
            col_expr.otherwise(None) if col_expr is not None else f.lit(None).cast(IntegerType())
        )
