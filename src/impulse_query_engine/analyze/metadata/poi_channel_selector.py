"""POI channel selection.

A ``PoiChannelSelector`` treats one ``poi_type`` as a channel: the POI rows of
that type, read as ``(timestamp, value)`` per container, become a
``PointsInTimeSeries`` (a value at each instant).

A POI Channel
- is resolved against the *wide* ``poi`` table with plain column-equality,
- stays **out** of the channel pipeline (``get_selectors() -> []``) and is
  collected via the parallel ``get_poi_channel_selectors()`` walker,
- builds to a ``PointsInTimeSeries`` instead of a ``SampleSeries``.

Row refinements (``network="FD3"`` …) are applied via :meth:`having`, kept
separate from the ``poi_type`` identity passed to ``q.poi_channel``.
"""

from __future__ import annotations

import zlib

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries
from impulse_query_engine.model.series.points_in_time_series_string import (
    PointsInTimeSeriesString,
)

# Accepted ``dtype`` values and the empty series each maps to. ``"double"`` is
# the default (numeric), so POI channels stay numeric unless a categorical value
# type is requested explicitly.
_SERIES_FOR_DTYPE = {
    "double": PointsInTimeSeries,
    "string": PointsInTimeSeriesString,
}


class PoiChannelSelector(TimeSeriesExpression):
    def __init__(self, poi_type: str, dtype: str = "double", having: dict | None = None):
        """
        Initialize a PoiChannelSelector.

        Parameters
        ----------
        poi_type : str
            The ``poi_type`` identifying this channel (e.g. ``"charging_error"``).
        dtype : str, optional
            How to interpret the POI ``value``: ``"double"`` (default, numeric →
            :class:`PointsInTimeSeries`) or ``"string"`` (categorical →
            :class:`PointsInTimeSeriesString`). The source ``value`` column is a
            single string column; this only decides interpretation at build time.
        having : dict or None, optional
            Column-equality refinements applied to the POI rows (e.g.
            ``{"network": "FD3"}``). Prefer building these via :meth:`having`.
        """
        if dtype not in _SERIES_FOR_DTYPE:
            raise ValueError(
                f"PoiChannelSelector dtype must be one of {sorted(_SERIES_FOR_DTYPE)}; got {dtype!r}."
            )
        self.poi_type = poi_type
        self.value_dtype = dtype
        self._having = dict(having) if having else {}
        TimeSeriesExpression.__init__(self, is_single_signal=True)

    def having(self, **filters) -> "PoiChannelSelector":
        """
        Return a new selector with extra column-equality row refinements ANDed on.

        Immutable: the receiver is unchanged (``dtype`` is preserved).

        Parameters
        ----------
        **filters : dict
            ``column == value`` restrictions on the POI rows (e.g. ``network="FD3"``).

        Returns
        -------
        PoiChannelSelector
        """
        return PoiChannelSelector(
            self.poi_type, dtype=self.value_dtype, having={**self._having, **filters}
        )

    @property
    def row_filters(self) -> dict:
        """The column-equality refinements applied on top of ``poi_type``."""
        return self._having

    @property
    def selector_id(self) -> int:
        """Stable identity over ``(poi_type, dtype, sorted(having))``.

        ``dtype`` is part of the identity because two selectors differing only by
        value interpretation produce different output series types, so they must
        resolve as distinct channels.
        """
        key = (self.poi_type, self.value_dtype, tuple(sorted(self._having.items())))
        return zlib.crc32(str(key).encode())

    def _empty_series(self):
        """The empty result series matching this selector's ``dtype``."""
        return _SERIES_FOR_DTYPE[self.value_dtype].empty()

    def dtype(self):
        """Spark data type of the selection result.

        ``array<array<double>>`` for numeric (``dtype="double"``), or the
        string series' struct-of-parallel-arrays for ``dtype="string"``. The solve
        UDF serializes the built series via ``get_data()``.
        """
        return self._empty_series().dtype()

    def build(self, cache) -> PointsInTimeSeries:
        """
        Build a PointsInTimeSeries (numeric or string) from this container's POI rows.

        Structurally identical to :meth:`TimeSeriesSelector.build`: ``resolve`` the
        rows this selector matches (POI union rows were stamped with this
        selector's ``selector_id`` in Stage P), read their ``(container_id,
        channel_id)``, then ``load_poi_blob`` with this selector's ``dtype`` so the
        value column is interpreted numerically or kept categorical. Runs inside the
        per-container solve UDF; rows are already resident (no I/O).

        Parameters
        ----------
        cache : SeriesCache
            Per-container cache exposing ``resolve`` and ``load_poi_blob``.

        Returns
        -------
        PointsInTimeSeries or PointsInTimeSeriesString
        """
        candidates = cache.resolve(self)
        if len(candidates) == 0:
            return self._empty_series()
        mid = candidates.container_id.iloc[0]
        cid = candidates.channel_id.iloc[0]
        return cache.load_poi_blob(mid, cid, dtype=self.value_dtype)

    # --- pipeline routing: POI channels are NOT channel-pipeline selectors ---

    def get_selectors(self) -> list:
        """POI channels do not participate in the channel pipeline."""
        return []

    def get_poi_channel_selectors(self) -> list["PoiChannelSelector"]:
        """This node is a POI channel selector."""
        return [self]

    def get_required_tag_exprs(self) -> set:
        """No EAV tag expressions are required (POI is a wide table)."""
        return set()

    def required_tags(self) -> set[str]:
        """No channel tags are required."""
        return set()

    def get_selector_expr(self):
        """POI channels are resolved by the solver, not via a tag selector expr."""
        return None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self._having:
            refine = ",".join(f"{k}={v}" for k, v in sorted(self._having.items()))
            return f"PoiChannelSelector<{self.poi_type}|{refine}>"
        return f"PoiChannelSelector<{self.poi_type}>"

        #todo should we do it like MetricSelector?? There it's just key?
        # todo we need to decide how the layout looks like that this receives
