"""CalculatedChannel: a labeled derived time-series channel."""

import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache

# Sentinel distinguishing "derive a deterministic channel_id" (the default) from
# an explicit ``channel_id=None`` (emit a SQL null).  A plain default of ``None``
# could not tell these two intents apart.
_AUTO = object()


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
    ``identity`` ``VARIANT`` column holding the whole identity dict.

    Parameters
    ----------
    expr : TimeSeriesExpression
        The wrapped expression; must ``build()`` to a ``SampleSeries``.
    identity : dict of str
        Identity for the output rows, e.g.
        ``{"channel_name": "Eng_RPM", "data_key": "TM"}``.  The whole dict is
        emitted per row in a single ``identity`` ``VARIANT`` column (keys are
        arbitrary).  Must be non-empty; the identity also seeds the
        deterministic ``channel_id`` hash.
    channel_id : int or None, optional
        Output ``channel_id`` for every emitted row.  When omitted (the
        ``_AUTO`` sentinel) a deterministic id is derived from the identity by
        the solver, typed to match the source ``channel_id`` column.  Pass an
        ``int`` to use it verbatim, or ``None`` to emit a SQL null.

    Notes
    -----
    ``_alias`` is set explicitly in ``__init__`` — as the identity values joined
    by ``"::"`` (e.g. ``"Eng_RPM::TM"``) — rather than left for the caller to
    chain ``.alias(...)``: reading a missing ``_alias`` would trigger
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

    _ALIAS_SEPARATOR = "::"

    def __init__(
        self,
        expr: TimeSeriesExpression,
        identity: dict[str, str],
        *,
        channel_id=_AUTO,
    ):
        if not identity:
            raise ValueError(
                "CalculatedChannel requires a non-empty identity dict "
                "(e.g. {'channel_name': 'Eng_RPM', 'data_key': 'TM'}); identity "
                "defines the output identifier columns and seeds the "
                "deterministic channel_id."
            )
        self.expr = expr
        self.identity = dict(identity)
        self._explicit_channel_id = channel_id
        self._alias = self._ALIAS_SEPARATOR.join(str(v) for v in identity.values())
        self.is_single_signal = getattr(expr, "is_single_signal", True)
        self.requires_udf = getattr(expr, "requires_udf", False)

    def __str__(self) -> str:
        return f"<CalculatedChannel identity={self.identity}, expr={self.expr}>"

    def canonical_identity(self) -> str:
        """Return a stable string encoding of the identity, used for the id hash.

        Keys are sorted so the encoding (and the derived ``channel_id``) is
        independent of kwarg order.
        """
        return "&".join(f"{k}={self.identity[k]}" for k in sorted(self.identity))

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
