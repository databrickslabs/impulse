import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig


class IntervalEncoder:
    """Convert RAW point samples into ``[tstart, tend)`` intervals, keeping every sample.

    Within each ``(container_id, channel_id)`` the samples are ordered by
    timestamp and each sample's ``tend`` is set to the *next* sample's
    timestamp (via a ``LEAD`` window function).  The last sample has no
    successor, so its ``tend`` coalesces to its own timestamp -- a
    zero-length interval that carries no duration.

    Only **duplicate points** are dropped: a row is a duplicate when both its
    ``value`` and ``timestamp`` equal the next row's (compared with
    ``eqNullSafe``, so two ``NULL`` values count as equal).  Every other
    sample is kept as its own interval, so the original timestamps are
    preserved.
    """

    def __init__(
        self, config: SolverConfig | None = None, drop_implausible_data_points: bool = False
    ):
        """
        Initialize the IntervalEncoder.
        Parameters
        ----------
        config : SolverConfig
            Solver configuration providing the internal column names.

        drop_implausible_data_points : bool, optional
            Whether to drop implausible data points before returning.  If True, data points where ``is_plausible``
            is not True will be removed.  Default is False.
        """

        if config is None:
            raise ValueError("SolverConfig must be provided to IntervalEncoder.")

        self.config: SolverConfig = config
        self.drop_implausible_data_points: bool = drop_implausible_data_points

    def prepare_channels_df(self, df: DataFrame) -> DataFrame:
        """Normalize a channels DataFrame to interval format.

        If the DataFrame already contains a ``tend`` column it is returned
        unchanged.  Otherwise ``tend`` is derived from ``timestamp`` using
        the ``LEAD`` window function and the column is renamed to ``tstart``.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Channel data.  Must contain ``container_id``, ``channel_id``,
            ``value`` and either ``tend`` (already RLE) or ``timestamp``
            (raw point data).

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with columns ``container_id``, ``channel_id``,
            ``tstart``, ``tend``, ``value``.

        Raises
        ------
        ValueError
            If the DataFrame has neither ``tend`` nor ``timestamp``.
        """
        if self.config.tend_col in df.columns:
            return df

        # if the data isn't RLE encoded we need a timestamp column to determine the tend info.
        if self.config.timestamp_col not in df.columns:
            raise ValueError(
                f"DataFrame must contain either a '{self.config.tend_col}' column (RLE format) "
                f"or a '{self.config.timestamp_col}' column (raw point data)."
            )
        return (
            df.transform(self._extract_next_data_point_info)
            .transform(self._drop_duplicate_data_points)
            .transform(self._determine_end_timestamp)
            .transform(self._remove_implausible_data_points)
        )

    def _remove_implausible_data_points(self, df) -> DataFrame:
        """
        If ``drop_implausible_data_points`` is ``True``, return a transform that filters out rows where the
        ``is_plausible`` column is not ``True``.
        """

        if self.drop_implausible_data_points:
            if self.config.is_plausible_col not in df.columns:
                raise ValueError(
                    f"DataFrame must contain an '{self.config.is_plausible_col}' column "
                    "to drop implausible data points."
                )
            return df.filter(F.col(self.config.is_plausible_col))
        else:
            return df

    def _determine_end_timestamp(self, df: DataFrame) -> DataFrame:
        """Convert the pre-computed next-timestamp column into ``tend``.

        Sets ``tend = COALESCE(_timestamp_of_next_data_point, timestamp)`` so
        that every row except the last one in a partition gets the next row's
        timestamp as its end.  The last row falls back to its own timestamp
        (``tend = tstart``).

        Renames ``timestamp`` to ``tstart`` and drops the intermediate
        ``_timestamp_of_next_data_point`` column.

        Requires
        --------
        The DataFrame must already contain a ``timestamp_of_next_data_point``
        column (added by ``_extract_next_data_point_info``).
        """
        end_ts = F.coalesce(
            F.col("_timestamp_of_next_data_point"), F.col(self.config.timestamp_col)
        )
        return (
            df.withColumn(self.config.tend_col, end_ts)
            .withColumnRenamed(self.config.timestamp_col, self.config.tstart_col)
            .drop("_timestamp_of_next_data_point")
        )

    def _drop_duplicate_data_points(self, df: DataFrame) -> DataFrame:
        """Remove exact duplicate data points.

        A row is considered a duplicate when both its ``value`` and
        ``timestamp`` are identical to the *next* row's (as determined by
        ``LEAD`` over ``WS``).  The comparison uses ``eqNullSafe`` so that
        two ``NULL`` values are treated as equal.

        The last row in each partition is never flagged because ``LEAD``
        returns ``NULL`` for it, and a non-null timestamp can never be
        null-safe-equal to ``NULL``.

        Drops the intermediate ``_value_of_next_data_point`` column after
        filtering.
        """
        is_duplicate = (
            F.col(self.config.value_col).eqNullSafe(F.col("_value_of_next_data_point"))
        ) & (F.col(self.config.timestamp_col).eqNullSafe(F.col("_timestamp_of_next_data_point")))
        return (
            df.withColumn("is_duplicate", is_duplicate)
            .filter(~F.col("is_duplicate"))
            .drop("is_duplicate", "_value_of_next_data_point")
        )

    def _extract_next_data_point_info(self, df: DataFrame) -> DataFrame:
        """Attach the next row's timestamp and value as new columns.

        Uses ``LEAD`` over the window to add:

        * ``timestamp_of_next_data_point`` -- the next row's ``timestamp``,
          or ``NULL`` for the last row in each partition.
        * ``value_of_next_data_point`` -- the next row's ``value``, or
          ``NULL`` for the last row in each partition.

        These columns are consumed downstream by
        ``_drop_duplicate_data_points`` and ``_determine_end_timestamp``.
        """
        ws = Window.partitionBy(
            F.col(self.config.container_id_col), F.col(self.config.channel_id_col)
        ).orderBy(F.col(self.config.timestamp_col).asc(), F.col(self.config.value_col).desc())

        timestamp_of_next_data_point = F.lead(F.col(self.config.timestamp_col)).over(ws)
        value_of_next_data_point = F.lead(F.col((self.config.value_col))).over(ws)

        return (
            df.transform(self._drop_null_timestamps)
            .withColumn("_timestamp_of_next_data_point", timestamp_of_next_data_point)
            .withColumn("_value_of_next_data_point", value_of_next_data_point)
        )

    def _drop_null_timestamps(self, df: DataFrame) -> DataFrame:
        """Drop rows where the timestamp is NULL."""
        return df.filter(F.col(self.config.timestamp_col).isNotNull())
