from pyspark.sql import DataFrame

from .query_solver import QuerySolver
from .series_cache import SeriesCache


class InMemoryCache(SeriesCache):
    pass


class InMemorySolver(QuerySolver):
    def filter_container_tags(self, spark, query, required_container_tags=None) -> DataFrame:
        raise NotImplementedError

    def filter_container_metrics(
        self,
        spark,
        query,
        container_df,
        pre_filtered_containers_df=None,
        required_container_tags=None,
        required_container_metrics=None,
    ) -> DataFrame:
        raise NotImplementedError

    def filter_channel_tags(
        self, spark, db, container_df, selectors, container_meta_cols=None
    ) -> DataFrame:
        raise NotImplementedError

    def filter_channel_metrics(
        self, spark, db, channel_df, selectors, container_meta_cols=None
    ) -> DataFrame:
        raise NotImplementedError

    def solve(
        self,
        query,
        channels_df,
        selections,
        dtypes=None,
        container_tag_cols=None,
        container_metric_cols=None,
    ):
        raise NotImplementedError
