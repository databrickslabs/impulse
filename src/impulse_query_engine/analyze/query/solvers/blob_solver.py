import os
from functools import partial

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.model.series.sample_series import SampleSeries

from .query_solver import QuerySolver
from .series_cache import SeriesCache


class TimeSeriesCache(SeriesCache):
    def __init__(self, df, base_uri):
        """
        Initialize the TimeSeriesCache.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing metadata for time series.
        base_uri : str
            Base URI where blobs are stored.
        """
        self.df = df
        self.base_uri = base_uri

    def resolve(self, selection):
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
        if "selector_ids" in self.df.columns:
            idx = self.df["selector_ids"].apply(
                lambda arr: arr is not None and selection.selector_id in arr
            )
            return self.df[idx]
        idx = selection._expr.build_pandas(self.df)
        return self.df[idx]

    def load_blob(self, container_id, channel_id, uses_alias: bool = False):
        """
        Load a time series blob from disk.

        Parameters
        ----------
        container_id : Any
            Container ID.
        channel_id : Any
            Channel ID.
        uses_alias : bool, optional
            Unused by this cache (no unit conversion); accepted for
            interface compatibility with :class:`SeriesCache`.

        Returns
        -------
        SampleSeries
            Loaded sample series object.
        """
        conid = container_id
        cid = channel_id
        fn = os.path.join(self.base_uri, f"container={conid}", cid)
        return SampleSeries.from_pickle(fn)


class BlobSolver(QuerySolver):
    def __init__(self):
        """
        Initialize the BlobSolver.
        """
        pass

    def filter_container_tags(self, spark, query) -> DataFrame:
        """Passthrough — returns an empty DataFrame."""
        return spark.createDataFrame([], schema=T.StructType([]))

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """Passthrough — returns container_df unchanged."""
        return container_df

    def filter_channel_tags(self, spark, db, container_df, selectors) -> DataFrame:
        """Passthrough — returns container_df unchanged."""
        return container_df

    def filter_channel_metrics(self, spark, db, channel_df, selectors) -> DataFrame:
        """Passthrough — returns channel_df unchanged."""
        return channel_df

    @staticmethod
    def _solve_container(row, selections=None, base_uri=None):
        """
        Solve for a single container by loading blobs and applying selections.

        Parameters
        ----------
        row : pyspark.sql.Row
            Row containing container and channel information.
        selections : list, optional
            List of selection expressions to apply.
        base_uri : str, optional
            Base URI for blob storage.

        Returns
        -------
        dict
            Dictionary with container_id and results for each selection.
        """
        data = row.asDict()
        conid = data["container_id"]
        data["container_id"] = [conid] * len(data["channel_id"])
        df = pd.DataFrame(data)
        cache = TimeSeriesCache(df, base_uri)
        result = {"container_id": conid}
        for s in selections:
            result[s._alias] = s.build(cache)
        return result

    def solve(self, query, channels_df, selections, dtypes=None):
        """
        Solve the query by grouping channels and applying selections.

        Parameters
        ----------
        query : QueryBuilder
            Query object containing database and filter information.
        channels_df : pyspark.sql.DataFrame
            DataFrame containing channel information.
        selections : list
            List of selection expressions to apply.
        dtypes : list, optional
            List of data types for each selection.

        Returns
        -------
        pd.DataFrame
            DataFrame containing results for each container.
        """
        aggs = []
        for c in channels_df.columns:
            if c == "container_id":
                continue
            aggs.append(F.collect_list(F.col(c)).alias(c))
        df_grouped = channels_df.groupBy("container_id").agg(*aggs)
        df_grouped = df_grouped.rdd.map(
            partial(
                BlobSolver._solve_container, selections=selections, base_uri=query.db.channel_uri()
            )
        )
        return pd.DataFrame(df_grouped.collect())
