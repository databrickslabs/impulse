from __future__ import annotations

import hashlib
from collections.abc import Mapping

from pyspark.sql import DataFrame, Row, SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.channels.calculated_channel import (
    CalculatedChannel as QeCalculatedChannel,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.model.series.sample_series import SampleSeries
from impulse_reporting.persist.dimension_schema import CALCULATED_CHANNEL_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import CALCULATED_CHANNEL_FACT_SCHEMA


class CalculatedChannel:
    """A reporting-layer calculated (derived) channel.

    Orchestration counterpart to a query-engine ``CalculatedChannel``: it wraps a
    :class:`TimeSeriesExpression` built from the operator DSL (e.g.
    ``q.channel(channel_name="raw_speed") * 3.6``) plus an ``identity`` dict, and
    is driven by :class:`Report` to compute the channel across containers, persist
    the narrow result to a gold fact table, and update it incrementally.

    Structurally parallels :class:`BasicEvent` (holds an aliased expression,
    name-derived id, SHA-256 definition hash) but — like ``ContainerEvent`` — it
    drives its own solve via ``QueryBuilder.solve_calculated_channels`` rather than
    riding the centralized wide ``solved_df``.  Accordingly :meth:`get_expression`
    returns ``None`` so it is excluded from the batch solve.

    Parameters
    ----------
    name : str
        Name of the calculated channel (used as the entity id seed's fallback and
        stored on the dimension row).
    expr : TimeSeriesExpression
        The wrapped expression; must evaluate to a ``SampleSeries``.
    identity : Mapping[str, str]
        Output identity.  Any non-empty set of keys; the whole dict is emitted
        per fact row in a single ``identity`` ``MapType(string, string)`` column
        and seeds the deterministic ``channel_id``.
    desc : str, optional
        Human-readable description (stored on the dimension row, excluded from the
        definition hash).
    attributes : Mapping[str, str], optional
        Key-value metadata stored on the dimension row.
    """

    def __init__(
        self,
        name: str,
        expr: TimeSeriesExpression,
        identity: Mapping[str, str],
        desc: str = None,
        attributes: Mapping[str, str] = None,
    ):
        self.name = name
        self.report_id = -1
        self.description = desc

        expr.require_evaluation_type(
            SampleSeries,
            owner="CalculatedChannel",
            example="q.channel(channel_name='raw_speed') * 3.6",
        )

        self.identity = {str(k): str(v) for k, v in identity.items()}
        if not self.identity:
            raise ValueError(
                "CalculatedChannel requires a non-empty identity dict "
                "(e.g. {'channel_name': 'speed_kmh', 'data_key': 'CALC'}); it defines "
                "the output identity and seeds the deterministic channel_id."
            )

        normalized_attributes: dict[str, str] = {}
        if attributes is not None:
            normalized_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.attributes = normalized_attributes

        # The wrapped query-engine channel owns the deterministic id and the
        # canonical identity encoding, so fact.channel_id == get_id() ==
        # dimension.channel_id with a single source of truth.
        self.expression = QeCalculatedChannel(expr, self.identity)

    def canonical_identity(self) -> str:
        """Public, order-independent identity key.

        Two channels with the same ``identity`` (regardless of key insertion
        order) share this value and therefore the same ``channel_id``.  Used by
        :class:`Report` to reject duplicate channel identities.  Delegates to the
        wrapped query-engine channel so both layers encode identity identically.
        """
        return self.expression.canonical_identity()

    def get_name(self) -> str:
        """Return the channel name."""
        return self.name

    def set_report_id(self, report_id: int):
        """Set the owning report id."""
        self.report_id = report_id

    def get_id(self) -> int:
        """Return the deterministic entity id (also the fact/dimension ``channel_id``)."""
        return self.expression.channel_id

    def get_expression(self) -> TimeSeriesExpression | None:
        """Return ``None`` — calculated channels drive their own narrow solve.

        Returning ``None`` keeps this channel out of the centralized wide batch
        solve (``collect_solvable_expressions``), mirroring ``ContainerEvent``.
        """
        return None

    def get_expression_str(self) -> str:
        """String form of the wrapped expression (identity + expr, no name/desc)."""
        if isinstance(self.expression, TimeSeriesExpression):
            return self.expression.__str__()
        return "NA"

    def get_channel_type_str(self) -> str:
        """Channel type string, matching the ``ChannelType`` enum member name."""
        return "CALCULATED_CHANNEL"

    def determine_definition_hash(self) -> int:
        """Hash of the computation-affecting definition (expression + identity)."""
        payload = f"{self.canonical_identity()}|{self.expression.expr}"
        hash_bytes = hashlib.sha256(payload.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """Dictionary representation of the dimension metadata.

        ``identity`` is a plain dict, persisted as a ``MapType(string, string)``
        column that mirrors the fact identity (no fixed per-key columns).
        """
        return {
            "channel_id": self.get_id(),
            "report_id": self.report_id,
            "channel_type": self.get_channel_type_str(),
            "channel_description": self.description,
            "channel_expression": self.get_expression_str(),
            "identity": self.identity,
            "definition_hash": self.determine_definition_hash(),
            "attributes": self.attributes,
        }

    def as_spark_row(self) -> Row:
        """Spark Row representation of the dimension metadata."""
        return Row(**self.as_dict())

    @classmethod
    def determine_calculated_channels(
        cls,
        spark: SparkSession,
        channels: list[CalculatedChannel],
        *,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame | None:
        """Solve the given channels and shape the result into fact rows.

        Drives ``QueryBuilder.solve_calculated_channels`` (the narrow, many-rows-
        per-container endpoint) with the report's ``query`` + ``solver``, then
        projects to :data:`CALCULATED_CHANNEL_FACT_SCHEMA`.  Because each channel's
        ``channel_id`` was fixed to its entity id at construction, no id-join is
        needed.

        Parameters
        ----------
        spark : SparkSession
            Spark session (unused directly; kept for interface parity).
        channels : list of CalculatedChannel
            Channels to solve; identity keys may differ across channels.
        query : QueryBuilder
            Query builder used to select and solve the channels.
        solver : QuerySolver
            Solver implementing ``solve_calculated_channels`` (a ``DefaultSolver``).
        pre_filtered_containers_df : DataFrame, optional
            Incremental container subset; ``None`` processes all containers.

        Returns
        -------
        DataFrame or None
            Narrow fact DataFrame, or ``None`` when there are no channels.
        """
        if not channels:
            return None

        qe_channels = [channel.expression for channel in channels]
        df = query.select(*qe_channels).solve_calculated_channels(
            spark, solver, pre_filtered_containers_df
        )
        return df.select(*CALCULATED_CHANNEL_FACT_SCHEMA.fieldNames())

    @classmethod
    def determine_metadata_df(cls, spark: SparkSession, channels: list[CalculatedChannel]):
        """Create the dimension DataFrame for the given channels.

        ``identity`` is a ``MapType(string, string)`` (same self-describing
        representation as the fact table), which ``createDataFrame`` builds
        directly from the plain dict returned by :meth:`as_dict`.
        """
        rows = [channel.as_spark_row() for channel in channels]
        return spark.createDataFrame(rows, schema=CALCULATED_CHANNEL_DIMENSION_SCHEMA)
