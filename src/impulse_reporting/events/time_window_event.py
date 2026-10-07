"""TimeWindowEvent — splits each container into consecutive fixed-duration windows."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    TimeWindowExpression,
    validate_max_windows,
    window_intervals_col,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.events.container_boundary_event import ContainerBoundaryEvent
from impulse_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from impulse_reporting.util.event_instance_util import generate_event_instance_id_column
from impulse_reporting.util.report_entity_util import ReportEntityUtil


class TimeWindowEvent(ContainerBoundaryEvent):
    """Event that divides each measurement container into consecutive fixed windows.

    Unlike ``ContainerEvent`` (one instance per container), a ``TimeWindowEvent`` emits one
    event instance per fixed-duration slice, tiling the container's ``start_ts`` / ``stop_ts``
    span with windows of length ``window_length``.  The final slice is clamped to the
    container end.

    The event fact is computed natively in Spark from ``container_metrics`` (via
    :func:`window_intervals_col`), so every filtered container gets windows regardless of
    its channel data.  Aggregations scoped to this event evaluate the
    :class:`TimeWindowExpression` in the solve, which computes the same windows in the
    same order.  ``event_instance_id`` hashes the window's position rather than its
    boundaries, so both sides match without relying on bit-identical doubles.
    """

    def __init__(
        self,
        name: str,
        window_length: float,
        desc: str = None,
        required_channels: list[str] = None,
        attributes: Mapping[str, str] = None,
        max_windows_per_container: int = MAX_WINDOWS_PER_CONTAINER,
    ):
        """
        Initialize a TimeWindowEvent object.

        Parameters
        ----------
        name : str
            Name of the event.
        window_length : float
            Fixed window length, in the same time unit as the underlying timestamps
            (e.g. milliseconds-since-epoch). Must be strictly positive and finite.
        desc : str, optional
            Description of the event.
        required_channels : list of str, optional
            List of required channels for the event. Informational; stored in the event
            dimension table.
        attributes : Mapping[str, str], optional
            Key-value metadata for the event. ``window_length`` is surfaced here
            automatically (without overriding a user-supplied key).
        max_windows_per_container : int, optional
            Maximum number of windows per container (default 1,000,000). A container
            exceeding it fails the report with an error naming the limit, which usually
            means ``window_length`` is in the wrong unit for the boundaries. Not part of
            the definition hash.

        Raises
        ------
        ValueError
            If ``window_length`` is not strictly positive and finite, or
            ``max_windows_per_container`` is not a positive integer.
        """
        ContainerBoundaryEvent.__init__(self, name)
        # window_length is validated by TimeWindowExpression. max_windows_per_container is
        # validated here so the error names this event's parameter, not the expression's.
        max_windows_per_container = validate_max_windows(
            max_windows_per_container, param_name="max_windows_per_container"
        )
        self.expression = TimeWindowExpression(
            window_length, max_windows=max_windows_per_container
        ).alias(name)
        # Use the expression's normalized (float) length everywhere, so the event fact,
        # the solve and event_dimension all see the same value for 10 and 10.0.
        self.window_length = self.expression.window_length
        self.max_windows_per_container = self.expression.max_windows
        self.description = desc
        self.required_channels = required_channels
        normalized_attributes: dict[str, str] = {}
        if attributes is not None:
            normalized_attributes = {str(k): str(v) for k, v in attributes.items()}
        # Surface the window length for traceability in event_dimension, without
        # clobbering an explicit user-supplied attribute of the same key.
        normalized_attributes.setdefault("window_length", str(self.window_length))
        self.attributes = normalized_attributes

    def set_channel_time(self, unit: str | None, origin: str = "epoch") -> None:
        """Record the channel time frame the windows are computed in.

        Set by ``Report.add_event`` from the report's ``solver_config``.  Stored on the
        expression, whose string form feeds the definition hashes of this event and of the
        aggregations scoped to it.

        Parameters
        ----------
        unit : str or None
            The report's ``solver_config.channel_time_unit``.
        origin : str, optional
            The report's ``solver_config.channel_time_origin`` (default ``"epoch"``).
        """
        self.expression.channel_time_unit = unit
        self.expression.channel_time_origin = origin

    def get_expression(self) -> TimeSeriesExpression | None:
        """
        Get the time series expression associated with the event.

        Returns
        -------
        TimeSeriesExpression or None
            The time-window expression for the event.
        """
        return self.expression

    def get_event_type_str(self) -> str:
        """Get the event type string for TimeWindowEvent.

        Returns
        -------
        str
            Event type string.
        """
        return "TIME_WINDOW_EVENT"

    def determine_definition_hash(self) -> int:
        """
        Calculate definition hash for the time-window event.

        Only includes the expression string, which encodes the attributes that affect the
        event results: ``window_length`` and the channel time frame (``channel_time_unit``,
        ``channel_time_origin``; omitted while unset / default). Resizing the window or
        changing the time frame therefore forces a full recompute in incremental mode.

        Excludes: name, description, required_channels, max_windows_per_container,
        report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
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

    @classmethod
    def determine_events(
        cls,
        spark: SparkSession,
        events: list[TimeWindowEvent],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """
        Extract the event fact table for the given list of TimeWindowEvent objects.

        Resolves the matching containers via the solver's filter pipeline (like
        ``ContainerEvent``) and computes each event's windows natively from the
        containers' ``start_ts`` / ``stop_ts`` in the channel time frame
        (``SolverConfig.with_window_bounds``), so every filtered container gets windows.
        Each window becomes one event instance (``start_ts < end_ts``) whose
        ``event_instance_id`` hashes its position among the container's windows. The solve
        computes the same windows in the same order for scoped aggregations (see
        :func:`window_intervals_col`), so the ids match.

        Parameters
        ----------
        spark : SparkSession
            Spark session for data processing.
        events : list of TimeWindowEvent
            List of TimeWindowEvent objects to process.
        solved_df : DataFrame, optional
            Not used by TimeWindowEvent (kept for interface compatibility).
        query : QueryBuilder, optional
            Query builder with filters applied.
        solver : QuerySolver, optional
            Solver whose filter pipeline is used for container resolution.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Spark DataFrame containing event instance facts.
        """
        container_metrics_df = cls.resolve_container_metrics(
            spark, query, solver, pre_filtered_containers_df
        )
        # The windows are computed in the channel time frame, from the same bounds the solve
        # uses for scoped aggregations (fails fast on the schema, e.g. when TIMESTAMP
        # boundaries lack solver_config.channel_time_unit).
        container_metrics_df = solver.config.with_window_bounds(container_metrics_df)
        start_ts = f.col(solver.config.window_start_col)
        stop_ts = f.col(solver.config.window_stop_col)

        # One (event_name, windows) struct per event, exploded in a single pass over the
        # containers. posexplode yields each window's position, which the
        # event_instance_id hashes (scoped aggregations use the same position).
        per_event = f.array(
            *[
                f.struct(
                    f.lit(event.get_name()).alias("event_name"),
                    window_intervals_col(
                        start_ts,
                        stop_ts,
                        event.window_length,
                        max_windows=event.max_windows_per_container,
                    ).alias("windows"),
                )
                for event in events
            ]
        )

        df = (
            container_metrics_df.select(
                f.col(solver.config.container_id_col).alias("container_id"),
                f.explode(per_event).alias("event"),
            )
            .select(
                "container_id",
                f.col("event.event_name").alias("event_name"),
                f.posexplode(f.col("event.windows")).alias("window_index", "event_instance"),
            )
            .withColumn("start_ts", f.col("event_instance").getItem(0))
            .withColumn("end_ts", f.col("event_instance").getItem(1))
            .withColumn(
                "event_instance_id",
                generate_event_instance_id_column(event_type=TimeWindowEvent),
            )
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
            )
            .select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())
        )
        return df
