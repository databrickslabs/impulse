from pyspark.sql import DataFrame

from .query_solver import QuerySolver
from .series_cache import SeriesCache


class InMemoryCache(SeriesCache):
    pass


class InMemorySolver(QuerySolver):
    def filter_container_tags(self, spark, query) -> DataFrame:
        raise NotImplementedError

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        raise NotImplementedError

    def filter_channel_tags(self, spark, db, container_df, selectors) -> DataFrame:
        raise NotImplementedError

    def filter_channel_metrics(self, spark, db, channel_df, selectors) -> DataFrame:
        raise NotImplementedError

    def solve(self, query, channels_df, selections, dtypes=None):
        raise NotImplementedError
