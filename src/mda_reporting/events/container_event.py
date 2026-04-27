"""ContainerEvent — an event spanning the full measurement container."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import zlib
from pyspark.sql import DataFrame, Row, SparkSession

from mda_query_engine.analyze.query.query_builder import QueryBuilder
from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver
from mda_reporting.config.config_parser import MeasurementDimensions
from mda_reporting.events.event import Event
from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from mda_reporting.util.event_instance_util import generate_event_instance_id_column
from mda_reporting.util.report_entity_util import ReportEntityUtil

if TYPE_CHECKING:
    from mda_query_engine.analyze.metadata.time_series_expression import (
        TimeSeriesExpression,
    )


class ContainerEvent(Event):
    """Event that treats the full measurement container as a single event instance.

    Unlike ``BasicEvent``, no time-series expression is needed — the event
    boundaries are derived directly from the container's ``start_ts`` and
    ``stop_ts`` metadata.
    """

    def __init__(self, name: str, desc: str = None, attributes: dict[str, str] = None):
        """
        Initialise a ContainerEvent.

        Parameters
        ----------
        name : str
            Name of the event.
        desc : str, optional
            Human-readable description.
        attributes : dict, optional
            Key-value metadata for the event.
        """
        super().__init__(name)
        self.description = desc
        self.attributes = attributes if attributes is not None else {}

    # ------------------------------------------------------------------
    # Instance methods
    # ------------------------------------------------------------------

    def get_id(self) -> int:
        """Return a unique identifier derived from the event name.

        Returns
        -------
        int
            Positive 32-bit integer identifier.
        """
        hash_input = f"{self.name}"
        return zlib.crc32(hash_input.encode()) & 0x7FFFFFFF  # Ensures positive 32-bit int

    def get_expression(self) -> "TimeSeriesExpression | None":
        """ContainerEvent has no time-series expression.

        Returns
        -------
        None
        """
        return None

    def get_event_type_str(self) -> str:
        """Get the event type string for ContainerEvent.

        Returns
        -------
        str
            Event type string.
        """
        return "CONTAINER_EVENT"

    def determine_definition_hash(self) -> int:
        """Calculate definition hash.

        The hash only captures computation-relevant attributes.
        For a ``ContainerEvent`` the identity is fully determined by the
        fact that it is a container event (there is no expression to vary),
        so the name of the event is hashed.

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        hash_input = self.name
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """Return a dictionary representation of the event.

        Returns
        -------
        dict
        """
        return {
            "event_id": self.get_id(),
            "report_id": self.report_id,
            "event_type": self.get_event_type_str(),
            "event_name": self.name,
            "event_description": self.description,
            "required_channels": None,
            "event_expression": self.get_expression_str(),
            "definition_hash": self.determine_definition_hash(),
            "attributes": self.attributes,
        }

    def as_spark_row(self) -> Row:
        """Return a Spark ``Row`` representation.

        Returns
        -------
        Row
        """
        return Row(**self.as_dict())

    # ------------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------------

    @classmethod
    def determine_events(
        cls,
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        events: list["ContainerEvent"],
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """Determine event instances from container metadata.

        Resolves matching containers via the solver's filter pipeline and
        produces one event instance per container.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        query : QueryBuilder
            Query builder with filters applied.
        solver : QuerySolver
            Solver whose filter pipeline is used for container resolution.
        events : list of ContainerEvent
            List of ContainerEvent objects (only the first is used for naming).
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_INSTANCE_FACT_SCHEMA``.
        """
        # Resolve containers via solver filter pipeline
        container_tags_df = solver.filter_container_tags(spark, query)
        container_metrics_df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )

        # Rename silver columns to gold event fact column names and cast
        # timestamps from TIMESTAMP to LongType so the DataFrame is
        # union-compatible with BasicEvent (which produces numeric ts).
        df = (
            container_metrics_df.withColumnRenamed(
                MeasurementDimensions.CONTAINER_ID.value, "container_id"
            )
            .withColumnRenamed(MeasurementDimensions.START_TS.value, "start_ts")
            .withColumnRenamed(MeasurementDimensions.STOP_TS.value, "end_ts")
            .withColumn("start_ts", f.col("start_ts").cast("long"))
            .withColumn("end_ts", f.col("end_ts").cast("long"))
        )

        # Add event_name from the first event in the list
        event_name = events[0].get_name()
        df = df.withColumn("event_name", f.lit(event_name))

        df = df.withColumn(
            "event_instance_id",
            generate_event_instance_id_column(event_type=ContainerEvent),
        )

        # Add event_id column
        df = df.withColumn(
            "event_id",
            ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
        )

        # Select only the columns defined in the fact schema
        return df.select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, events: list["ContainerEvent"]
    ) -> DataFrame:
        """Create a Spark DataFrame containing event metadata.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        events : list of ContainerEvent
            List of ContainerEvent objects.

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_DIMENSION_SCHEMA``.
        """
        rows = [event.as_spark_row() for event in events]
        return spark.createDataFrame(rows, schema=EVENT_DIMENSION_SCHEMA)
