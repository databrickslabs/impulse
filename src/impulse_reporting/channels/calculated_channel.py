from __future__ import annotations

import hashlib
from collections.abc import Mapping

import pyspark.sql.functions as F
import pyspark.sql.types as T
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
from impulse_reporting.channels.calculated_channel_kpis import (
    DEFAULT_KPIS,
    build_kpi_columns,
)
from impulse_reporting.persist.dimension_schema import CALCULATED_CHANNEL_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import CALCULATED_CHANNEL_FACT_SCHEMA


def _union_identity_keys(channels: list[CalculatedChannel]) -> list[str]:
    """Return the sorted union of identity keys across ``channels``.

    Used to build the dynamic identity columns of the calculated-channel metrics
    table: every key any channel declares becomes a column (null on channels that
    omit it). Sorting gives a stable, order-independent column layout.
    """
    keys: set[str] = set()
    for channel in channels:
        keys.update(channel.identity)
    return sorted(keys)


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
    riding the centralized wide ``solved_df``.  It is dispatched separately from
    the batch solve (never passed to ``collect_solvable_expressions``).

    Parameters
    ----------
    name : str
        Name of the calculated channel (used as the entity id seed's fallback and
        stored on the dimension row).
    expr : TimeSeriesExpression
        The wrapped expression; must evaluate to a ``SampleSeries``.
    identity : Mapping[str, str]
        Channel identity.  Any non-empty set of keys; seeds the deterministic
        ``channel_id`` and is stored once on ``calculated_channel_dimension`` as a
        ``MapType(string, string)`` column (joined to the fact via ``channel_id``,
        not repeated on fact rows).
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

    def get_expression(self) -> TimeSeriesExpression:
        """
        Return the wrapped query-engine ``CalculatedChannel`` expression.
        """
        return self.expression

    def get_expression_str(self) -> str:
        """String form of the wrapped expression (identity + expr, no name/desc)."""
        return str(self.expression)

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

        ``identity`` is a plain dict, persisted on the dimension as a
        ``MapType(string, string)`` column (no fixed per-key columns).
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
            Spark session, forwarded to ``QueryBuilder.solve_calculated_channels``.
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

        ``identity`` is a self-describing ``MapType(string, string)`` column,
        which ``createDataFrame`` builds directly from the plain dict returned by
        :meth:`as_dict`.
        """
        rows = [channel.as_spark_row() for channel in channels]
        return spark.createDataFrame(rows, schema=CALCULATED_CHANNEL_DIMENSION_SCHEMA)

    @classmethod
    def determine_channel_metrics(
        cls,
        spark: SparkSession,
        channels: list[CalculatedChannel],
        fact_df: DataFrame | None,
        *,
        attribute_columns: list[str] | None = None,
        kpis: list[str] | None = None,
    ) -> DataFrame | None:
        """Derive a silver-shaped ``channel_metrics`` DataFrame from the fact rows.

        The calculated-channel fact table already matches the silver ``channels``
        table; this builds its companion ``channel_metrics`` so the pair can serve
        as an Impulse silver source.  Metrics are aggregated **directly from the
        narrow fact rows** (``container_id, channel_id, tstart, tend, value``),
        grouped by ``(container_id, channel_id)``.

        The output schema is **dynamic**: fixed columns ``container_id,
        channel_id, data_type`` plus one column per configured KPI (see ``kpis``),
        one per identity key (the union across all ``channels``), and one per
        configured attribute key.  Identity/attribute values are pulled from
        each channel's in-memory ``identity`` / ``attributes`` dicts (null where a
        channel omits a key).  On an identity/attribute key collision, identity wins
        and the attribute is skipped.

        Parameters
        ----------
        spark : SparkSession
            Session used to build the per-channel metadata frame.
        channels : list of CalculatedChannel
            The channels whose fact rows are in ``fact_df``; supply identity and
            attributes.
        fact_df : DataFrame or None
            Narrow fact DataFrame (output of :meth:`determine_calculated_channels`).
            ``None`` returns ``None``.
        attribute_columns : list of str, optional
            Attribute keys to surface as columns.  Default/empty → no attribute
            columns.  A key no channel defines yields an all-null column.
        kpis : list of str, optional
            KPI names to compute (see ``calculated_channel_kpis.KPI_BUILDERS``); the
            output carries one column per name, in order.  ``None`` → the default
            KPIs (``duration, min, max, mean``).

        Returns
        -------
        DataFrame or None
            The dynamic-schema metrics DataFrame, or ``None`` when ``fact_df`` is
            ``None``.
        """
        if fact_df is None:
            return None

        attribute_columns = list(attribute_columns or [])
        kpis = list(kpis) if kpis is not None else list(DEFAULT_KPIS)

        # KPI aggregations are built from the registry (single extension point), so
        # the computed columns and the projection below both follow ``kpis``.
        agg_df = fact_df.groupBy("container_id", "channel_id").agg(*build_kpi_columns(kpis))

        # Union of identity keys (stable order); attribute columns lose to identity
        # on a key collision.
        identity_keys = _union_identity_keys(channels)
        effective_attribute_keys = [k for k in attribute_columns if k not in identity_keys]

        # Per-channel metadata frame. channel_id is cast to the fact's channel_id
        # type so the join keys line up regardless of int/long width.
        channel_id_type = fact_df.schema["channel_id"].dataType
        meta_schema = T.StructType(
            [T.StructField("channel_id", channel_id_type, False)]
            + [T.StructField(k, T.StringType(), True) for k in identity_keys]
            + [T.StructField(k, T.StringType(), True) for k in effective_attribute_keys]
        )
        meta_rows = []
        for channel in channels:
            row: dict = {"channel_id": channel.get_id()}
            for key in identity_keys:
                row[key] = channel.identity.get(key)
            for key in effective_attribute_keys:
                row[key] = channel.attributes.get(key)
            meta_rows.append(Row(**row))
        meta_df = spark.createDataFrame(meta_rows, schema=meta_schema)

        result = agg_df.join(meta_df, on="channel_id", how="left").withColumn(
            "data_type", F.lit("double")
        )

        ordered_columns = (
            ["container_id", "channel_id"]
            + identity_keys
            + effective_attribute_keys
            + ["data_type"]
            + kpis
        )
        return result.select(*ordered_columns)
