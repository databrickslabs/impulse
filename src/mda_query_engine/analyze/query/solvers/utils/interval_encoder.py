import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window


class IntervalEncoder:
    """Utility class for encoding raw channel data into compatible format."""

    def __init__(
        self, timestamp_col_name: str = "timestamp", drop_implausible_data_points: bool = False
    ):
        """
        Initialize the IntervalEncoder.
        Parameters
        ----------
        timestamp_col_name : str, optional
            Name of the timestamp column in the input DataFrame when it is not already in RLE format.  Default is "timestamp".
        drop_implausible_data_points : bool, optional
            Whether to drop implausible data points before returning.  If True, data points where ``is_plausible``
            is not True will be removed.  Default is False.
        """
        self.timestamp_col_name: str = timestamp_col_name
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
        if "tend" in df.columns:
            return df

        # if the data isn't RLE encoded we need a timestamp column to determine the tend info.
        if self.timestamp_col_name not in df.columns:
            raise ValueError(
                "DataFrame must contain either a 'tend' column (RLE format) "
                "or a 'timestamp' column (raw point data)."
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
            if "is_plausible" not in df.columns:
                raise ValueError(
                    "DataFrame must contain an 'is_plausible' column "
                    "to drop implausible data points."
                )
            return df.filter(F.col("is_plausible"))
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
        end_ts = F.coalesce(F.col("_timestamp_of_next_data_point"), F.col("timestamp"))
        return (
            df.withColumn("tend", end_ts)
            .withColumnRenamed(self.timestamp_col_name, "tstart")
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
        is_duplicate = (F.col("value").eqNullSafe(F.col("_value_of_next_data_point"))) & (
            F.col(self.timestamp_col_name).eqNullSafe(F.col("_timestamp_of_next_data_point"))
        )
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
        ws = Window.partitionBy(F.col("container_id"), F.col("channel_id")).orderBy(
            F.col(self.timestamp_col_name).asc(), F.col("value").desc()
        )

        timestamp_of_next_data_point = F.lead(F.col(self.timestamp_col_name)).over(ws)
        value_of_next_data_point = F.lead(F.col("value")).over(ws)

        return (
            df.transform(self._drop_null_timestamps)
            .withColumn("_timestamp_of_next_data_point", timestamp_of_next_data_point)
            .withColumn("_value_of_next_data_point", value_of_next_data_point)
        )

    def _drop_null_timestamps(self, df: DataFrame) -> DataFrame:
        """Drop rows where the timestamp is NULL."""
        return df.filter(F.col(self.timestamp_col_name).isNotNull())
