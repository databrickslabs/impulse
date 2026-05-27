from abc import ABC, abstractmethod

import pandas as pd

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
            (e.g. :class:`KVSTimeSeriesCache`) only apply the per-channel
            conversion factor when this is ``True``, so a direct selector
            on the same physical channel always returns raw values.
            Defaults to ``False`` (direct / no-conversion semantics).

        Returns
        -------
        SampleSeries
            The loaded sample series object.
        """
        pass
