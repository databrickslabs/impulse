from abc import ABC, abstractmethod

import pandas as pd

from impulse_query_engine.model.series.sample_series import SampleSeries


class SeriesCache(ABC):
    @abstractmethod
    def resolve(self, selection) -> pd.DataFrame:
        """
        Resolve a channel (time-series) selector to its candidate rows.

        Parameters
        ----------
        selection : TimeSeriesSelector
            The channel selector whose tag expression (``selection._expr``)
            identifies the matching channel(s). Metric filters do not flow
            through here — they are evaluated in Spark via ``get_selector_expr``
            during ``filter_container_metrics``.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the resolved candidates.
        """
        pass

    @abstractmethod
    def load_blob(
        self,
        mid,
        cid,
        uses_alias: bool = False,
        series_type=None,
        value_type=None,
    ) -> SampleSeries:
        """
        Resolve given mid and cid to a series.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.
        uses_alias : bool, optional
            ``True`` when the calling selector resolves the channel via a
            ``channel_mapping`` alias.  Caches that perform unit conversion
            (e.g. :class:`TimeSeriesCache`) only apply the per-channel
            conversion factor when this is ``True``, so a direct selector
            on the same physical channel always returns raw values.
            Defaults to ``False`` (direct / no-conversion semantics).
        series_type : SeriesType, optional
            The calling selector's series type. The selector (not a per-row
            data column) decides which object to build: ``POINTS_IN_TIME`` builds
            a :class:`PointsInTimeSeries`, otherwise a :class:`SampleSeries`.
            ``None`` (default) builds a :class:`SampleSeries`.
        value_type : SeriesValueType, optional
            For a ``POINTS_IN_TIME`` selector, its declared value type
            (``DOUBLE`` / ``STRING``); selects the numeric vs string value
            column. Ignored otherwise. The declared type is validated against
            the silver metadata in the solve prelude, so the data stays
            authoritative.

        Returns
        -------
        SampleSeries or PointsInTimeSeries
            The loaded series object.
        """
        pass
