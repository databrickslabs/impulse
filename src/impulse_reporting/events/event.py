from __future__ import annotations

import abc
from abc import ABC

from pyspark.sql import DataFrame, Row, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver


class Event(ABC):
    """Abstract base class for report events."""

    def __init__(self, name):
        """
        Initialize an Event object.

        Parameters
        ----------
        name : str
            Name of the event.
        """
        self.name = name
        self.report_id = -1  # Default value indicating no report assigned

    def get_name(self) -> str:
        """
        Get the name of the event.

        Returns
        -------
        str
            The name of the event.
        """
        return self.name

    def set_report_id(self, report_id: int):
        """
        Set the report ID for the event.

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
    def get_id(self) -> int:
        """
        Get the unique identifier for the event.

        Returns
        -------
        int
            Unique identifier for the event.
        """
        pass

    @abc.abstractmethod
    def get_expression(self) -> TimeSeriesExpression | None:
        """
        Get the expression associated with the event.

        Returns
        -------
        TimeSeriesExpression or None
            The time series expression for the event.
        """
        pass

    def get_expression_str(self) -> str:
        """
        Get the string representation of the event's expression.

        Returns
        -------
        str
            String representation of the event's expression, or "NA" if not available.
        """
        if hasattr(self, "expression") and isinstance(self.expression, TimeSeriesExpression):
            return self.expression.__str__()
        else:
            return "NA"

    @abc.abstractmethod
    def get_event_type_str(self) -> str:
        """Return event type identifier as a string.

        Returns
        -------
        str
            Event type as a string (for example ``BASIC_EVENT``).
        """
        pass

    @abc.abstractmethod
    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the event.

        Returns
        -------
        dict
            Dictionary containing representation of the event.
        """
        pass

    @abc.abstractmethod
    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the event.

        Returns
        -------
        Row
            Spark Row containing event info.
        """
        pass

    @abc.abstractmethod
    def determine_definition_hash(self) -> int:
        """
        Calculate hash from result-relevant instance attributes only.

        This hash is used to detect when an event's computation logic
        has changed, requiring full reprocessing of all containers.

        The hash should only include attributes that affect computation results,
        excluding UI/metadata attributes like name, description, and required_channels.

        MUST INCLUDE: expression
        MUST EXCLUDE: name, description, required_channels, report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def determine_events(
        cls,
        spark: SparkSession,
        events: list[Event],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ):
        """
        Determine event instances and return a Spark DataFrame of event facts.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        events : list of Event
            List of Event objects to process.
        solved_df : DataFrame, optional
            Pre-solved wide DataFrame from centralized batch solve.
        query : QueryBuilder, optional
            Query builder for constructing event queries (ContainerEvent path).
        solver : QuerySolver, optional
            Query solver for executing queries (ContainerEvent path).
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Spark DataFrame containing event instance facts.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def determine_metadata_df(cls, spark: SparkSession, histograms: list[Event]):
        """
        Create a Spark DataFrame containing event metadata.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        histograms : list of Event
            List of Event objects.

        Returns
        -------
        DataFrame
            Spark DataFrame containing event metadata.
        """
        pass
