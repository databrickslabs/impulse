"""The POI (point of interest) TSAL expression leaf.

A ``PoiSelector`` is a **sibling** of :class:`TimeSeriesSelector` — it subclasses
:class:`TimeSeriesExpression` so it composes with every operator (``+ - * / & |``) and
every core-model method reachable through ``TimeSeriesExpression.__getattr__``, but it is
deliberately **not** a ``TimeSeriesSelector``. See ``POI_PROPOSAL_REVIEW.md`` §2 for why:
``TimeSeriesSelector`` carries the ``RequiresDeserialization`` marker, and the
``toPandas()`` deserialization gate (``query_builder.py``) fires on that marker's
``isinstance`` — routing POI results (``array<double>``) through
``SampleSeries.deserialize`` → ``lz4f.decompress`` and crashing. Not inheriting avoids
the whole problem.

**POI is a pure occurrence log (Option D).** A ``PoiSelector`` always evaluates to a
:class:`PointsInTime` — the instants at which the matching occurrences happened. It does
**not** carry the POI table's own snapshot columns (``vehicle_wheel_speed`` etc.) as
values: those are redundant with the measured ``channels``, so a value *at* an occurrence
is obtained by sampling the real channel — ``q.channel("Vehicle Speed Sensor").where(poi)``
— which is typed by the channel and needs no POI-attribute machinery.

Row filtering uses a **dedicated POI predicate** (:mod:`poi_expression`), not the EAV
``TagExpression``:

- ``q.poi(poi_type="aeb")`` — kind filter (equality kwargs).
- ``q.poi(poi_type="aeb").having(q.poi_metric("duration") > 5)`` — an extra row predicate.

``build(cache)`` returns an empty :class:`PointsInTime` when the cache has no POI data —
which is exactly what makes ``build(EmptyTimeSeriesCache())`` work as a data-free type
probe, so ``require_evaluation_type`` can validate a POI expression at construction time
with no Spark.
"""

from __future__ import annotations

import zlib
from typing import TYPE_CHECKING, Any

import numpy as np

from impulse_query_engine.analyze.metadata.poi_expression import (
    PoiPredicate,
    poi_kind_predicate,
)
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.model.series.points_in_time import PointsInTime

if TYPE_CHECKING:
    from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache


class PoiSelector(TimeSeriesExpression):
    """A TSAL leaf selecting points of interest from the configured POI table.

    Always evaluates to :class:`PointsInTime`.

    Parameters
    ----------
    kind_predicate : PoiPredicate
        The ANDed equality predicate identifying which POI rows this selector matches
        (e.g. ``poi_type == "aeb"``). Built by :func:`poi_expression.poi_kind_predicate`
        from the ``**kind_filters`` passed to ``QueryBuilder.poi``.
    having : list of PoiPredicate or None, optional
        Extra row predicates (e.g. ``q.poi_metric("duration") > 5``), ANDed with the kind
        predicate and pushed down Spark-side in ``DefaultSolver.filter_poi``. Normally set
        through :meth:`having` rather than the constructor.
    """

    def __init__(
        self,
        kind_predicate: PoiPredicate,
        *,
        having: list[PoiPredicate] | None = None,
    ):
        self._kind_predicate = kind_predicate
        self._having = list(having) if having else []
        super().__init__(is_single_signal=True)

    # ------------------------------------------------------------------
    # Fluent row filtering
    # ------------------------------------------------------------------

    def having(self, *predicates: PoiPredicate) -> "PoiSelector":
        """Return a new ``PoiSelector`` with extra row predicate(s) ANDed in.

        Named ``having`` rather than ``where`` deliberately: on a
        :class:`TimeSeriesExpression`, ``where`` already means "sample this series at those
        points" (``channel.where(poi)``), so reusing it as a row filter would give one word
        two meanings. ``having`` reads as "restrict the source's rows" and returns a new
        immutable selector, so it chains: ``q.poi(poi_type="aeb").having(a).having(b)``.

        Parameters
        ----------
        *predicates : PoiPredicate
            Row predicates over POI columns, e.g. ``q.poi_metric("duration") > 5``.

        Returns
        -------
        PoiSelector
            A new selector; the original is unchanged.
        """
        return PoiSelector(
            self._kind_predicate, having=[*self._having, *predicates]
        )

    # ------------------------------------------------------------------
    # Identity / definition hashing
    # ------------------------------------------------------------------

    @property
    def _predicate(self) -> PoiPredicate:
        """The full row predicate: kind equality ANDed with any ``having`` predicates."""
        pred = self._kind_predicate
        for extra in self._having:
            pred = pred & extra
        return pred

    @property
    def selector_id(self) -> int:
        """crc32 of the predicate — matches ``TimeSeriesSelector.selector_id`` derivation.

        Kept consistent with the channel selectors so the same
        ``QuerySolver._build_selector_id_expr`` ``WHEN…THEN`` machinery can tag POI rows
        with a ``selector_id`` in Spark.
        """
        return zlib.crc32(str(self._predicate).encode())

    def __str__(self) -> str:
        """String form — feeds the definition hash.

        Includes the full row predicate (kind + ``having``) so two differently-filtered
        POIs of the same kind are treated as **distinct definitions** by the incremental
        definition-hash comparator.
        """
        return f"PoiSelector<{self._predicate}>"

    # ------------------------------------------------------------------
    # The type-correct core
    # ------------------------------------------------------------------

    def build(self, cache: SeriesCache) -> PointsInTime:
        """Resolve this POI selector against *cache* into a :class:`PointsInTime`.

        Returns an empty ``PointsInTime`` when the cache has no POI rows — this is what
        makes ``build(EmptyTimeSeriesCache())`` a valid, data-free type probe.
        ``EmptyTimeSeriesCache`` and the definition-time probe therefore always land here
        with an empty frame; the solver's ``TimeSeriesCache`` overrides ``resolve_poi`` to
        return this container's matching, deduped POI instants.

        Parameters
        ----------
        cache : SeriesCache
            Cache exposing ``resolve_poi(selection)`` (a concrete default on
            :class:`SeriesCache` returning an empty frame).

        Returns
        -------
        PointsInTime
            The occurrence instants (sorted, unique).
        """
        rows = cache.resolve_poi(self)
        if rows is None or len(rows) == 0:
            return PointsInTime.empty()
        ts = np.asarray(rows["ts"], dtype=np.float64)
        # np.unique returns sorted + unique, satisfying PointsInTime's assume_unique contract.
        return PointsInTime(np.unique(ts))

    def dtype(self):
        """Spark wire type — owned by the core-model type, never hardcoded here."""
        return PointsInTime.empty().dtype()

    # ------------------------------------------------------------------
    # TimeSeriesExpression contract (the abstract methods)
    # ------------------------------------------------------------------

    def get_required_tag_exprs(self) -> set:
        # POI predicates are not TagExpressions; nothing consumes tag *expressions* for a
        # POI leaf (get_selectors() == [] keeps it out of the channel-tag pivot), so the
        # set is empty. required_columns() below exposes the referenced POI columns.
        return set()

    def required_tags(self) -> set[str]:
        # The POI columns this selector references. Only ever unioned by aggregation
        # wrappers; never reaches the channel-tag pivot (get_selectors() == []).
        return self._predicate.required_columns()

    def get_selector_expr(self):
        # The Spark predicate. Applied in DefaultSolver.filter_poi, keyed by selector_id;
        # the channel pipeline never sees this because get_selectors() returns [].
        return self._predicate.get_selector_expr()

    def get_selectors(self) -> list:
        # Empty by design: keeps POI out of filter_channel_tags / filter_channel_metrics.
        return []

    def get_poi_selectors(self) -> list["PoiSelector"]:
        # The parallel collection path, mirrored by TimeSeriesExpression.get_poi_selectors.
        return [self]

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def as_dict(self) -> dict[str, Any]:
        obj = TimeSeriesExpression.as_dict(self)
        obj["type"] = "PoiSelector"
        obj["kind_predicate"] = self._kind_predicate.as_dict()
        obj["having"] = [p.as_dict() for p in self._having]
        return obj


def poi_expr(**kind_filters) -> PoiPredicate:
    """Alias for :func:`poi_expression.poi_kind_predicate`.

    Kept as the builder behind ``QueryBuilder.poi``.
    """
    return poi_kind_predicate(**kind_filters)
