from __future__ import annotations

import abc
from abc import ABC
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

if TYPE_CHECKING:
    from impulse_query_engine.measurement_db import MeasurementDB

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)

from .solver_config import SolverConfig


class QuerySolver(ABC):
    """Abstract base class for query solvers.

    Defines a 6-stage filter pipeline that all solvers must implement:
    filter_container_tags -> filter_container_metrics -> filter_channel_tags ->
    filter_channel_metrics -> filter_candidates -> solve.

    ``filter_container_metrics`` must return a DataFrame that includes **all
    columns** needed for container dimensions and event bounds (e.g.
    ``container_id``, ``start_ts``/``stop_ts`` or ``start_dt``/``stop_dt``),
    not only ``container_id``.
    """

    def __init__(self, config: SolverConfig = None):
        self.config = config or SolverConfig()

    @staticmethod
    def _apply_column_mapping(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        """Rename DataFrame columns according to a physical → internal mapping."""
        for physical, internal in mapping.items():
            if physical != internal:
                df = df.withColumnRenamed(physical, internal)
        return df

    def _build_expr(self, filters):
        """
        Build a combined selector expression from a list of filter expressions.

        Parameters
        ----------
        filters : list
            List of filter expressions.
            Example: [MetricOp<or_(MetricOp<and_(MetricOp<eq(MetricSelector<vehicle_key>,Seat_Leon)>]
        Returns
        -------
        pyspark.sql.Column or None
            Combined selector expression or None if no filters.
        """
        expr = None
        for filt in filters:
            if expr is None:
                expr = filt.get_selector_expr()
            else:
                expr = expr | filt.get_selector_expr()
        return expr

    def _build_solve_output_schema(self, channels: DataFrame, selections, dtypes) -> T.StructType:
        """Build the grouped-map UDF output schema for the ``solve`` stage.

        The ``container_id`` field type is derived from *channels* (the
        column-mapped channels DataFrame) so it matches the physical type of
        the source table instead of being hardcoded.  Each selection
        contributes one field, named after its ``_alias`` and typed by the
        paired entry in *dtypes*.

        Parameters
        ----------
        channels : pyspark.sql.DataFrame
            The column-mapped channels DataFrame whose ``container_id`` column
            type is authoritative for the grouped output.
        selections : Iterable
            Selection expressions; each contributes a result field.
        dtypes : Iterable
            Output Spark data types, paired positionally with *selections*.

        Returns
        -------
        pyspark.sql.types.StructType
            ``[container_id, <selection aliases…>]`` schema.
        """
        entries = [
            T.StructField(
                self.config.container_id_col,
                channels.schema[self.config.container_id_col].dataType,
            )
        ]
        for s, dtype in zip(selections, dtypes, strict=False):
            entries.append(T.StructField(s._alias, dtype))
        return T.StructType(entries)

    def _build_calculated_channels_udf_schema(self, channels: DataFrame) -> T.StructType:
        """Build the grouped-map UDF return schema for calculated channels.

        This describes what :meth:`DefaultSolver._solve_calculated_channels_udf`
        returns — the narrow silver ``channel_data`` columns
        (``container_id, channel_id, tstart, tend, value``).  The ``identity``
        map is *not* part of this schema: it is constant per ``channel_id`` and
        appended by :meth:`solve_calculated_channels` after the UDF runs, so it
        never crosses the Arrow boundary.

        The ``container_id`` and ``channel_id`` field types are derived from
        *channels* (the column-mapped channels DataFrame) so they match the
        physical source types instead of being hardcoded.

        Parameters
        ----------
        channels : pyspark.sql.DataFrame
            The column-mapped channels DataFrame whose ``container_id`` /
            ``channel_id`` column types are authoritative.

        Returns
        -------
        pyspark.sql.types.StructType
            ``[container_id, channel_id, tstart, tend, value]``.
        """
        return T.StructType(
            [
                T.StructField(
                    self.config.container_id_col,
                    channels.schema[self.config.container_id_col].dataType,
                ),
                T.StructField(
                    self.config.channel_id_col,
                    channels.schema[self.config.channel_id_col].dataType,
                ),
                T.StructField(self.config.tstart_col, T.LongType()),
                T.StructField(self.config.tend_col, T.LongType()),
                T.StructField(self.config.value_col, T.DoubleType()),
            ]
        )

    def _empty_channel_match_df(self, spark, db: MeasurementDB) -> DataFrame:
        """Return an empty ``(container_id, channel_id, selector_ids)`` DataFrame.

        The ``container_id`` and ``channel_id`` types are derived from the
        column-mapped ``channel_metrics`` table (the source of the real
        channel-match rows this empty frame is union'd/joined with) so the
        schemas stay compatible regardless of the physical id types.
        """
        ref = self._apply_column_mapping(
            db.channel_metrics(spark), self.config.channel_metrics.column_name_mapping
        )
        return spark.createDataFrame(
            [],
            schema=T.StructType(
                [
                    T.StructField(
                        self.config.container_id_col,
                        ref.schema[self.config.container_id_col].dataType,
                    ),
                    T.StructField(
                        self.config.channel_id_col,
                        ref.schema[self.config.channel_id_col].dataType,
                    ),
                    T.StructField("selector_ids", T.ArrayType(T.IntegerType())),
                ]
            ),
        )

    def _build_selector_id_expr(self, filters) -> Column:
        """Build a Spark ``Column`` that maps rows to their ``selector_id``.

        Produces a chained ``F.when`` expression: for each selector in
        *filters*, if the row satisfies the selector's tag expression the
        column evaluates to that selector's ``selector_id``.

        Parameters
        ----------
        filters : Iterable[TimeSeriesSelector]
            Selectors whose ``get_selector_expr()`` and ``selector_id`` are
            used to build the ``WHEN … THEN …`` chain.

        Returns
        -------
        pyspark.sql.Column
            A column expression suitable for ``df.withColumn("selector_id", …)``.
        """
        selector_expr = None
        for selection in filters:
            if selector_expr is None:
                selector_expr = F.when(selection.get_selector_expr(), F.lit(selection.selector_id))
            else:
                selector_expr = selector_expr.when(
                    selection.get_selector_expr(), F.lit(selection.selector_id)
                )
        return selector_expr

    @abc.abstractmethod
    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Abstract method to filter containers by tags.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame filtered by container tags.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_container_tags method.")

    @abc.abstractmethod
    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Abstract method to filter containers by metrics.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame from filter_container_tags stage.
        pre_filtered_containers_df : pyspark.sql.DataFrame, optional
            Pre-filtered containers for incremental processing.
            When provided, restricts processing to only these containers.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered container metrics.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError(
            "Each solver must implement the filter_container_metrics method."
        )

    @abc.abstractmethod
    def filter_channel_tags(self, spark, db: MeasurementDB, container_df, selectors) -> DataFrame:
        """
        Stage 3: Filter channels by measurements and tags.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        container_df : pyspark.sql.DataFrame
            DataFrame containing container information.
        selectors : list[TimeSeriesSelector]
            Non-aliased (direct) selectors extracted from the query.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered channel tags.

        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_channel_tags method.")

    @abc.abstractmethod
    def filter_channel_metrics(self, spark, db: MeasurementDB, channel_df, selectors) -> DataFrame:
        """
        Stage 4: Filter channels by metrics.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        channel_df : pyspark.sql.DataFrame
            DataFrame containing channel information.
        selectors : list[TimeSeriesSelector]
            Non-aliased (direct) selectors extracted from the query.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with ``(container_id, channel_id, selector_ids)``
            where ``selector_ids`` is an array column.
        Raises
        ------
        NotImplementedError
            If not implemented by subclass.
        """
        raise NotImplementedError("Each solver must implement the filter_channel_metrics method.")

    def filter_aliased_channel_metrics(
        self, spark, db: MeasurementDB, container_df, selectors
    ) -> DataFrame:
        """
        Resolve aliased channel selections via the channel_mapping table.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        db : MeasurementDB
            Measurement database for table access.
        container_df : pyspark.sql.DataFrame
            DataFrame containing filtered container IDs.
        selectors : list[TimeSeriesSelector]
            Aliased selectors extracted from the query.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with
            ``(container_id, channel_id, <metrics-side join keys>,
            channel_alias, alias_priority, selector_ids)`` where
            ``selector_ids`` is an array column.  Implementations that
            support unit conversion additionally include ``source_unit``
            and ``target_unit`` columns.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support aliased channel resolution"
        )

    def resolve_channel_selections(
        self, spark, channel_metrics_df, aliased_channel_metrics_df
    ) -> DataFrame:
        """
        Union direct and aliased channel metrics, combining selector_ids.

        Only called when aliased selectors are present.  The default
        implementation raises ``NotImplementedError``; solvers that support
        aliasing (e.g. ``DefaultSolver``) must override this.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        channel_metrics_df : pyspark.sql.DataFrame
            Direct channel metrics with ``selector_ids`` array column.
        aliased_channel_metrics_df : pyspark.sql.DataFrame
            Aliased channel metrics with ``selector_ids`` array column.

        Returns
        -------
        pyspark.sql.DataFrame
            Merged DataFrame with ``(container_id, channel_id, selector_ids)``.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support aliased channel resolution"
        )

    def filter_candidates(self, query, channel_df) -> DataFrame:
        """
        Stage 5: Select best channel candidate.

        Parameters
        ----------
        query : QueryBuilder
            Query object containing filters and db info.
        channel_df : pyspark.sql.DataFrame
            DataFrame containing channel information.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing selected channel candidates.
        """
        pass

    @abc.abstractmethod
    def solve(self, query, channels_df, selections, dtypes):
        """
        Stage 6: Solve query.

        Parameters
        ----------
        query : QueryBuilder
            Query object containing database and filter information.
        channels_df : pyspark.sql.DataFrame
            DataFrame containing channel information.
        selections : list
            List of selection expressions to apply.
        dtypes : list
            List of data types for each selection.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing results for each container.
        """
        pass

    def solve_calculated_channels(self, query, channels_df, selections) -> DataFrame:
        """
        Solve calculated channels into a narrow, silver-shaped DataFrame.

        Optional stage, parallel to :meth:`solve` but emitting many rows per
        container (the exploded ``SampleSeries`` of each ``CalculatedChannel``)
        instead of one wide row.  Solvers that support calculated channels
        (e.g. ``DefaultSolver``) override this; the base implementation raises.

        Parameters
        ----------
        query : QueryBuilder
            Query object containing database and filter information.
        channels_df : pyspark.sql.DataFrame
            Channel-match DataFrame from the filter pipeline.
        selections : list
            List of ``CalculatedChannel`` selections to evaluate.

        Returns
        -------
        pyspark.sql.DataFrame
            Narrow ``[container_id, channel_id, tstart, tend, value,
            identity]`` DataFrame (``identity`` is a ``MapType(string, string)``).
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support calculated channels"
        )
