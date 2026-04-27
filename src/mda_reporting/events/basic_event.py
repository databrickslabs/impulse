from __future__ import annotations

import hashlib
from collections.abc import Mapping

import pyspark.sql.functions as f
import zlib
from pyspark.sql import Row, SparkSession

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.events.event import Event
from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from mda_reporting.util.event_instance_util import generate_event_instance_id_column
from mda_reporting.util.report_entity_util import ReportEntityUtil


class BasicEvent(Event):
    """Class representing a basic event in a report."""

    def __init__(
        self,
        name: str,
        expr: TimeSeriesExpression,
        desc: str = None,
        required_channels: list[str] = None,
        attributes: Mapping[str, str] | None = None,
    ):
        """
        Initialize a BasicEvent object.

        Parameters
        ----------
        name : str
            Name of the event.
        expr : TimeSeriesExpression
            Time series expression for the event.
        desc : str, optional
            Description of the event.
        required_channels : list of str, optional
            List of required channels for the event.
        attributes : Mapping[str, str], optional
            Key-value metadata for the event (e.g. limit_type, limit_direction).
        """
        Event.__init__(self, name)
        self.expression = expr.alias(name)
        self.description = desc
        self.required_channels = required_channels
        normalized_attributes: dict[str, str] = {}
        if attributes is not None:
            normalized_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.attributes = normalized_attributes

    def get_id(self) -> int:
        """
        Returns a unique identifier for the event.

        Returns
        -------
        int
            Unique positive 32-bit integer identifier for the event.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF  # Ensures positive 32-bit int

    def get_expression(self) -> TimeSeriesExpression | None:
        """
        Get the time series expression associated with the event.

        Returns
        -------
        TimeSeriesExpression or None
            The time series expression for the event.
        """
        return self.expression

    def get_event_type_str(self) -> str:
        """Get the event type string for BasicEvent.

        Returns
        -------
        str
            Event type string.
        """
        return "BASIC_EVENT"

    def determine_definition_hash(self) -> int:
        """
        Calculate definition hash for basic event.

        Only includes the expression (computation logic), which is the
        only attribute that affects the event results.

        Excludes: name, description, required_channels, report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        # Only the expression affects results
        hash_input = self.get_expression_str()

        # Use SHA-256 and return as int (truncated to fit LongType)
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """
        Get a dictionary representation of the event.

        Returns
        -------
        dict
            Dictionary containing event metadata.
        """
        return {
            "event_id": self.get_id(),
            "report_id": self.report_id,
            "event_type": self.get_event_type_str(),
            "event_name": self.name,
            "event_description": self.description,
            "required_channels": self.required_channels,
            "event_expression": self.get_expression_str(),
            "definition_hash": self.determine_definition_hash(),
            "attributes": self.attributes,
        }

    def as_spark_row(self) -> Row:
        """
        Get a Spark Row representation of the event.

        Returns
        -------
        Row
            Spark Row containing event metadata.
        """
        return Row(**self.as_dict())

    @classmethod
    def determine_events(
        cls,
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        events: list[BasicEvent],
        pre_filtered_containers_df=None,
    ):
        """
        Extract event fact table for the given list of BasicEvent objects.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        query : QueryBuilder
            Query builder for constructing event queries.
        solver : QuerySolver
            Query solver for executing queries.
        events : list of BasicEvent
            List of BasicEvent objects to process.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Spark DataFrame containing event instance facts.
        """
        event_expressions = []
        event_names = []
        for event in events:
            event_expressions.append(event.get_expression())
            event_names.append(event.get_name())

        event_query = query.select(*event_expressions)

        df = (
            event_query.solve(
                spark=spark,
                solver=solver,
                pre_filtered_containers_df=pre_filtered_containers_df,
            )
            .unpivot(
                f.col("container_id"),
                event_names,
                variableColumnName="event_name",
                valueColumnName="value",
            )
            .select(
                "container_id",
                "event_name",
                f.explode(f.col("value")).alias("event_instance"),
            )
            .withColumn("start_ts", f.col("event_instance").getItem(0))
            .withColumn("end_ts", f.col("event_instance").getItem(1))
            .withColumn(
                "event_instance_id",
                generate_event_instance_id_column(event_type=BasicEvent),
            )
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
            )
            .select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())
            .where(f.col("start_ts") < f.col("end_ts"))  # Ensure valid time intervals
        )
        return df

    @classmethod
    def determine_metadata_df(cls, spark: SparkSession, events: list[BasicEvent]):
        """
        Create a Spark DataFrame containing event metadata.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        events : list of BasicEvent
            List of BasicEvent objects.

        Returns
        -------
        DataFrame
            Spark DataFrame containing event metadata.
        """
        events = [event.as_spark_row() for event in events]
        return spark.createDataFrame(events, schema=EVENT_DIMENSION_SCHEMA)
