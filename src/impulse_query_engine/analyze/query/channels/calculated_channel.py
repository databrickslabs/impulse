"""CalculatedChannel: a labeled derived time-series channel."""

import zlib

import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache


class CalculatedChannel(TimeSeriesExpression):
    """A derived channel: a wrapped time-series expression plus output identity.

    A ``CalculatedChannel`` wraps an arbitrary :class:`TimeSeriesExpression`
    built from the operator DSL (e.g. ``q.channel(channel_name="raw_speed") * 3.6``
    or ``rpm + speed``).  Like the other ``TimeSeriesExpression`` leaves/nodes
    (``TimeSeriesSelector``, ``TimeSeriesOp``) it ``build()``s to a
    :class:`SampleSeries` — it is a *labeled derived signal*, not a reduction, so
    it is a plain ``TimeSeriesExpression`` rather than an ``Aggregation``.
    :meth:`QueryBuilder.solve_calculated_channels` explodes that series into
    narrow rows matching the silver ``channel_data`` shape
    (``container_id, channel_id, tstart, tend, value``) plus a single
    ``identity`` ``MapType(string, string)`` column holding the whole identity
    dict.

    Parameters
    ----------
    expr : TimeSeriesExpression
        The wrapped expression; must ``build()`` to a ``SampleSeries``.
    identity : dict of str
        Identity for the output rows, e.g.
        ``{"channel_name": "Eng_RPM", "data_key": "TM"}``.  The whole dict is
        emitted per row in a single ``identity`` ``MapType(string, string)``
        column (keys are arbitrary).  Must be non-empty; the identity also seeds
        the deterministic :attr:`channel_id`.

    Notes
    -----
    ``_alias`` is set in ``__init__`` (via ``super().__init__(alias=...)``) — as
    the identity values joined by ``"::"`` (e.g. ``"Eng_RPM::TM"``) — rather than
    left for the caller to chain ``.alias(...)``: reading a missing ``_alias``
    would trigger
    ``TimeSeriesExpression.__getattr__`` and silently return a callable instead
    of raising.  The alias is not consumed by the calculated-channels solve path
    (that emits the identity column directly); it exists so the object stays a
    well-formed ``TimeSeriesExpression``.  A later ``.alias(...)`` still overrides
    it.

    Examples
    --------
    ::

        CalculatedChannel(rpm * 3.6, {"channel_name": "speed_kmh", "data_key": "CALC"})
    """

    _IDENTITY_SEPARATOR = "::"

    def __init__(
        self,
        expr: TimeSeriesExpression,
        identity: dict[str, str],
    ):
        if not identity:
            raise ValueError(
                "CalculatedChannel requires a non-empty identity dict "
                "(e.g. {'channel_name': 'Eng_RPM', 'data_key': 'TM'}); identity "
                "defines the output identifier columns and seeds the "
                "deterministic channel_id."
            )
        super().__init__(
            alias=self._IDENTITY_SEPARATOR.join(str(v) for v in identity.values()),
            is_single_signal=getattr(expr, "is_single_signal", True),
            requires_udf=getattr(expr, "requires_udf", False),
        )
        self.expr = expr
        self.identity = dict(identity)

    def __str__(self) -> str:
        return f"<CalculatedChannel identity={self.identity}, expr={self.expr}>"

    def canonical_identity(self) -> str:
        """Return a stable string encoding of the identity, used for the id hash.

        Keys are sorted so the encoding (and the derived ``channel_id``) is
        independent of kwarg order.
        """
        return self._IDENTITY_SEPARATOR.join(
            f"{k}={self.identity[k]}" for k in sorted(self.identity)
        )

    @property
    def channel_id(self) -> int:
        """Deterministic output ``channel_id`` derived from the identity.

        A CRC32 of :meth:`canonical_identity` masked to a positive int32, so the
        value is stable across runs/processes and fits both ``IntegerType`` and
        ``LongType`` source ``channel_id`` columns.  Determinism makes writes
        idempotent and joins predictable; sharing this one derivation across the
        query-engine and reporting layers keeps their ids in lockstep.
        """
        return zlib.crc32(self.canonical_identity().encode()) & 0x7FFFFFFF

    def build(self, cache: SeriesCache):
        """Evaluate the wrapped expression against the cache (yields a SampleSeries)."""
        return self.expr.build(cache)

    def dtype(self) -> T.DataType:
        """Spark type of ``build()``'s result: a serialized ``SampleSeries``.

        Matches :meth:`TimeSeriesSelector.dtype` (``BinaryType``), since the
        wrapped expression evaluates to a ``SampleSeries``.  The narrow
        calculated-channels solve path does not consume this — it emits its own
        ``container_id, channel_id, tstart, tend, value`` schema — but keeping it
        honest makes the expression safe if ever routed through ``solve()``.
        """
        return T.BinaryType()

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return self.expr.get_selectors()

    def get_selector_expr(self):
        return self.expr.get_selector_expr()

    def get_required_tag_exprs(self) -> set[TagExpression]:
        return self.expr.get_required_tag_exprs()

    def required_tags(self) -> set[str]:
        return self.expr.required_tags()
