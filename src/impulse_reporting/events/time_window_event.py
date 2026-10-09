"""TimeWindowEvent — splits each container into consecutive fixed-duration windows."""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pyarrow as pa
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    TimeWindowExpression,
    tile_windows,
    validate_max_windows,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.analyze.query.solvers.utils.window_bounds import with_window_bounds
from impulse_reporting.events.container_boundary_event import ContainerBoundaryEvent
from impulse_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from impulse_reporting.util.event_instance_util import generate_event_instance_id_column
from impulse_reporting.util.report_entity_util import ReportEntityUtil

# Columns of the window rows next to container_id, named as the event fact and its
# event_instance_id hash expect them.
_EVENT_NAME_COL = "event_name"
_START_TS_COL = "start_ts"
_END_TS_COL = "end_ts"

# Windows after which _explode_windows emits an output batch.
_BATCH_WINDOWS = 100_000


class TimeWindowEvent(ContainerBoundaryEvent):
    """Event that divides each measurement container into consecutive fixed windows.

    Unlike ``ContainerEvent`` (one instance per container), a ``TimeWindowEvent`` emits one
    event instance per fixed-duration slice, tiling the container's ``start_ts`` / ``stop_ts``
    span with windows of length ``window_length``.  The final slice is clamped to the
    container end.

    The event fact is computed from ``container_metrics`` alone (in a ``mapInArrow``), so
    every filtered container gets windows regardless of its channel data.  Aggregations
    scoped to this event evaluate the :class:`TimeWindowExpression` in the solve.  Both use
    the same window function (``tile_windows``), so they produce identical windows, and the
    timestamp-based ``event_instance_id`` (like for other interval events) matches on both
    sides.
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
        self.attributes = self._normalize_attributes(attributes)
        # Surface the window length for traceability in event_dimension, without
        # clobbering an explicit user-supplied attribute of the same key.
        self.attributes.setdefault("window_length", str(self.window_length))

    def set_channel_time(
        self, unit: str | None, origin: str = "epoch", container_unit: str | None = None
    ) -> None:
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
        container_unit : str or None, optional
            The report's ``solver_config.container_time_unit``.
        """
        self.expression.channel_time_unit = unit
        self.expression.channel_time_origin = origin
        self.expression.container_time_unit = container_unit

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
        ``channel_time_origin``, ``container_time_unit``; omitted while unset / default).
        Resizing the window or changing the time frame therefore forces a full recompute in
        incremental mode.

        Excludes: name, description, required_channels, max_windows_per_container,
        report_id

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        return self._sha256_long(self.get_expression_str())

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
        (``solvers.utils.window_bounds.with_window_bounds``), so every filtered container
        gets windows.
        Each window becomes one event instance (``start_ts < end_ts``) whose
        ``event_instance_id`` hashes its boundaries. The solve uses the same window function
        for scoped aggregations (see :func:`tile_windows`), so the ids match.

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
        container_metrics_df = with_window_bounds(container_metrics_df, solver.config)
        windows_df = _explode_windows(
            container_metrics_df,
            id_col=solver.config.container_id_col,
            start_col=solver.config.window_start_col,
            stop_col=solver.config.window_stop_col,
            windows=[
                (event.get_name(), event.window_length, event.max_windows_per_container)
                for event in events
            ],
        )
        return (
            windows_df.withColumn(
                "event_instance_id",
                generate_event_instance_id_column(
                    event_type=TimeWindowEvent,
                    event_name_col=_EVENT_NAME_COL,
                    start_ts_col=_START_TS_COL,
                    end_ts_col=_END_TS_COL,
                ),
            )
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(
                    elements=events, element_name=_EVENT_NAME_COL
                ),
            )
            .select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())
        )


def _explode_windows(
    df: DataFrame,
    *,
    id_col: str,
    start_col: str,
    stop_col: str,
    windows: list[tuple[str, float, int]],
) -> DataFrame:
    """One row per window of each container, for all events in one pass.

    Each container's windows come from :func:`tile_windows`, like those of the solve.  The
    rows are built from numpy arrays in a ``mapInArrow``, in output batches of about 100,000
    windows that never split a container's windows of one event, so the Python worker's
    memory stays bounded by the batch size and ``max_windows``, independent of the number of
    containers and events.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        One row per container, with *id_col* and the bounds in the channel time frame.
    id_col : str
        Container id column; kept with its name and type (e.g. long or string) in the output.
    start_col, stop_col : str
        Container bound columns (numeric).
    windows : list of tuple
        ``(name, window_length, max_windows)`` per event: the event name of its rows, the
        window length (in the unit of the bounds) and the maximum number of windows per
        container. A container exceeding it fails the query with an error naming the limit.

    Returns
    -------
    pyspark.sql.DataFrame
        Columns *id_col*, ``event_name`` (string), ``start_ts`` and ``end_ts`` (double); no
        rows for a container whose bound is null, NaN or infinite, or whose span is not
        strictly positive.
    """
    schema = T.StructType(
        [
            df.schema[id_col],
            T.StructField(_EVENT_NAME_COL, T.StringType()),
            T.StructField(_START_TS_COL, T.DoubleType()),
            T.StructField(_END_TS_COL, T.DoubleType()),
        ]
    )
    column_names = schema.names

    def tile(batches):
        yield from _window_batches(
            batches, id_col, start_col, stop_col, windows, column_names, _BATCH_WINDOWS
        )

    return df.select(id_col, start_col, stop_col).mapInArrow(tile, schema)


def _window_batches(batches, id_col, start_col, stop_col, windows, column_names, batch_windows):
    """Yield :func:`_explode_windows` output batches named *column_names* for Arrow *batches*.

    A batch is emitted once it holds at least *batch_windows* windows, after a container's
    complete windows of one event, and at the end of each input batch.  A batch therefore
    holds at most ``batch_windows - 1`` plus one event's ``max_windows`` windows.
    """
    event_names = pa.array([name for name, _, _ in windows], pa.string())
    for batch in batches:
        ids = batch.column(id_col)
        bounds = zip(
            batch.column(start_col).to_pylist(), batch.column(stop_col).to_pylist(), strict=True
        )
        parts, pending = [], 0
        for row, (start, stop) in enumerate(bounds):
            for event, (_, length, max_windows) in enumerate(windows):
                starts, ends = tile_windows(start, stop, length, max_windows)
                parts.append((row, event, starts, ends))
                pending += len(starts)
                if pending >= batch_windows:
                    yield _record_batch(column_names, ids, event_names, parts)
                    parts, pending = [], 0
        if pending:
            yield _record_batch(column_names, ids, event_names, parts)


def _record_batch(column_names, ids, event_names, parts) -> pa.RecordBatch:
    """Output batch for the ``(row, event, starts, ends)`` *parts* of one input batch."""
    counts = [len(starts) for _, _, starts, _ in parts]
    rows = np.repeat([row for row, _, _, _ in parts], counts)
    events = np.repeat([event for _, event, _, _ in parts], counts)
    return pa.RecordBatch.from_arrays(
        [
            ids.take(rows),
            event_names.take(events),
            pa.array(np.concatenate([starts for _, _, starts, _ in parts])),
            pa.array(np.concatenate([ends for _, _, _, ends in parts])),
        ],
        names=column_names,
    )
