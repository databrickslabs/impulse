"""ContainerBoundaryEvent — base for events derived from container boundaries."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.events.event import Event


class ContainerBoundaryEvent(Event):
    """Base class for events whose instances are derived from container boundaries.

    The instances are resolved from ``container_metrics`` (``start_ts`` / ``stop_ts``)
    via the solver's filter pipeline instead of the centralized channel solve, so every
    filtered container yields instances regardless of its channel data.  The report
    therefore excludes these event types from the solvable expressions and dispatches
    them with ``query`` / ``solver`` rather than ``solved_df``.

    Attributes
    ----------
    epoch_unit : str or None
        ``solver_config.epoch_unit`` of the report the event belongs to, set by
        ``Report.add_event``.  It decides the unit of ``TIMESTAMP`` boundaries, so
        subclasses fold it into their definition hash.
    """

    epoch_unit: str | None = None

    def set_epoch_unit(self, epoch_unit: str | None) -> None:
        """Set the epoch unit ``TIMESTAMP`` container boundaries are converted to.

        Parameters
        ----------
        epoch_unit : str or None
            The report's ``solver_config.epoch_unit``.
        """
        self.epoch_unit = epoch_unit

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
            Column-mapped ``container_metrics`` rows of the matching containers, with
            ``TIMESTAMP`` boundaries converted to epoch numbers when
            ``solver.config.epoch_unit`` is set (unchanged otherwise).
        """
        container_tags_df = solver.filter_container_tags(spark, query)
        container_metrics_df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )
        # Same transform as the solve's container metadata, so the event boundaries and
        # those seen by scoped aggregations are identical.
        return solver.config.normalize_container_boundaries(container_metrics_df)
