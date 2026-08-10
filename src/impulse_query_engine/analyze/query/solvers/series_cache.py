from abc import ABC, abstractmethod

import pandas as pd

from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries
from impulse_query_engine.model.series.sample_series import SampleSeries


class SeriesCache(ABC):
    @abstractmethod
    def resolve(self, selection) -> pd.DataFrame:
        """
        Resolve selected tags/metrics to a list of candidates.

        Parameters
        ----------
        selection : Any
            The selection object specifying tags or metrics.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the resolved candidates.
        """
        pass

    @abstractmethod
    def load_blob(self, mid, cid, uses_alias: bool = False) -> SampleSeries:
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

        Returns
        -------
        SampleSeries
            The loaded sample series object.
        """
        pass

    @abstractmethod
    def load_poi_blob(self, mid, cid) -> PointsInTimeSeries:
        """
        Load a POI channel as a :class:`PointsInTimeSeries` — the POI twin of
        :meth:`load_blob`.

        Concrete default: **no POI rows** — an empty series. Caches that back
        ``poi_channel`` selections (e.g. :class:`TimeSeriesCache`) override this
        to slice the container's POI rows from the shared ``(cid, ch)`` index.
        The default lets caches that never see POI — notably the empty cache used
        for type inference in ``_determine_result_objects_dtypes`` — build a
        ``PoiChannelSelector`` (to an empty ``PointsInTimeSeries``) for free.

        Parameters
        ----------
        mid : Any
            Container id.
        cid : Any
            The POI channel's synthetic channel id.

        Returns
        -------
        PointsInTimeSeries
        """
        pass
