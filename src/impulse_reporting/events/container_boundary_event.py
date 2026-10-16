"""ContainerBoundaryEvent — base for events derived from container boundaries."""

from __future__ import annotations

import hashlib
import zlib
from collections.abc import Mapping

from pyspark.sql import DataFrame, Row, SparkSession

from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.events.event import Event
from impulse_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA


class ContainerBoundaryEvent(Event):
    """Base class for events whose instances are derived from container boundaries.

    The instances are resolved from ``container_metrics`` (``start_ts`` / ``stop_ts``)
    via the solver's filter pipeline instead of the centralized channel solve, so every
    filtered container yields instances regardless of its channel data.  The report
    therefore excludes these event types from the solvable expressions and dispatches
    them with ``query`` / ``solver`` rather than ``solved_df``.

    Subclasses set ``description`` and ``attributes`` (via :meth:`_normalize_attributes`),
    and ``required_channels`` when they have any; :meth:`as_dict` writes them to
    ``event_dimension``.
    """

    required_channels: list[str] | None = None

    @staticmethod
    def _normalize_attributes(attributes: Mapping[str, str] | None) -> dict[str, str]:
        """Return *attributes* with string keys and values (empty when ``None``)."""
        return {str(k): str(v) for k, v in (attributes or {}).items()}

    @staticmethod
    def _sha256_long(text: str) -> int:
        """SHA-256 of *text*, truncated to a signed 64-bit int (the ``definition_hash``
        type)."""
        hash_bytes = hashlib.sha256(text.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """Return the event's ``event_dimension`` row as a dictionary.

        Returns
        -------
        dict
            Event metadata keyed by the ``event_dimension`` column names.
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

    def get_id(self) -> int:
        """Return a unique identifier derived from the event name.

        Returns
        -------
        int
            Positive 32-bit integer identifier.
        """
        return zlib.crc32(self.name.encode()) & 0x7FFFFFFF

    def as_spark_row(self) -> Row:
        """Return a Spark ``Row`` representation of :meth:`as_dict`.

        Returns
        -------
        Row
        """
        return Row(**self.as_dict())

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, events: list[ContainerBoundaryEvent]
    ) -> DataFrame:
        """Create a Spark DataFrame containing event metadata.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        events : list of ContainerBoundaryEvent
            Events of one container-boundary type.

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_DIMENSION_SCHEMA``.
        """
        rows = [event.as_spark_row() for event in events]
        return spark.createDataFrame(rows, schema=EVENT_DIMENSION_SCHEMA)

    @staticmethod
    def resolve_container_metrics(
        spark: SparkSession,
        query: QueryBuilder,
        solver: QuerySolver,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """Resolve the filtered containers' metrics via the solver filter pipeline.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        query : QueryBuilder
            Query builder with filters applied.
        solver : QuerySolver
            Solver whose filter pipeline is used for container resolution.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Column-mapped ``container_metrics`` rows of the matching containers, with the
            original ``start_ts`` / ``stop_ts``.
        """
        container_tags_df = solver.filter_container_tags(spark, query)
        return solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )
