import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig


class RleEncoder:
    """Run-length encode RAW point samples into ``[tstart, tend)`` intervals.

    Within each ``(container_id, channel_id)`` the samples are ordered by
    timestamp and a new interval starts whenever ``value`` changes.  Each
    resulting **run** -- one or more consecutive samples sharing the same
    value -- becomes a single interval spanning from the run's first
    timestamp (``tstart``) to the timestamp at which the value next changes
    (``tend``).  This removes redundant points from signals that stay
    constant over time.
    """

    def __init__(
        self,
        config: SolverConfig | None = None,
        drop_implausible_data_points: bool = False,
    ):
        """
        Initialize the RleEncoder.

        Parameters
        ----------
        config : SolverConfig
            Solver configuration providing the internal column names.

        drop_implausible_data_points : bool, optional
            Whether to drop implausible data points before encoding.  If True, rows
            where ``is_plausible`` is not True are removed.  Default is False.
        """

        if config is None:
            raise ValueError("SolverConfig must be provided to RleEncoder.")

        self.config: SolverConfig = config
        self.drop_implausible_data_points: bool = drop_implausible_data_points

    def prepare_channels_df(self, df: DataFrame) -> DataFrame:
        """Run-length encode a raw channels DataFrame.

        Consecutive rows within the same container/channel that carry an identical
        ``value`` are merged into one interval spanning from the first timestamp of
        the interval (``tstart``) to the timestamp at which the value next changes
        (``tend``).

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Channel data.  Must contain the configured container id and channel id
            columns, ``value`` and the timestamp column (``timestamp_col_name``).

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with the container id and channel id columns, ``tstart``,
            ``tend`` and ``value`` -- one row per constant-value interval.

        Notes
        -----
        With ``drop_implausible_data_points=False`` (the default), the
        ``is_plausible`` column is ignored entirely: implausible samples are
        encoded like any other sample and merge into the surrounding interval
        when their value matches.

        With ``drop_implausible_data_points=True``, an implausible sample
        always forces an interval boundary, even when its value matches its
        neighbours.  Because interval ids are assigned over the *full* input
        and implausible rows are only removed afterwards, a dropped sample
        splits the surrounding interval in two rather than being bridged
        over -- it simply is not emitted as its own interval.
        """
        return (
            df.transform(self._check_required_column_exists)
            .transform(self._assign_interval_ids)
            .transform(self._remove_implausible_data_points)
            .transform(self._aggregate_intervals)
        )

    def _check_required_column_exists(self, df: DataFrame) -> DataFrame:
        """Check that the required column for dropping implausible data points exists."""

        if self.drop_implausible_data_points and self.config.is_plausible_col not in df.columns:
            raise ValueError(
                f"DataFrame must contain an '{self.config.is_plausible_col}' column "
                "to drop implausible data points."
            )
        else:
            return df

    def _assign_interval_ids(self, df: DataFrame) -> DataFrame:
        """Tag each row with the id of the interval it belongs to.

        A new interval begins whenever ``value`` differs from the previous row's value
        (ordered by timestamp within each container/channel).  The running sum of
        these change flags yields a ``value_id`` that is constant for the duration
        of an interval.  ``next_time`` -- the following row's timestamp, or the row's own
        timestamp for the last row in a partition -- is attached so the interval's end
        can be derived by :meth:`_aggregate_intervals`.

        Returns
        -------
        pyspark.sql.DataFrame
            The input DataFrame with the intermediate ``prev_value``,
            ``next_time``, ``value_diff`` and ``value_id`` columns added.
        """
        w = Window.partitionBy(
            F.col(self.config.container_id_col), F.col(self.config.channel_id_col)
        ).orderBy(F.col(self.config.timestamp_col).asc())

        running_w = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        prev_value = F.lag(F.col(self.config.value_col)).over(w)
        next_time = F.coalesce(
            F.lead(F.col(self.config.timestamp_col)).over(w), F.col(self.config.timestamp_col)
        )

        if self.drop_implausible_data_points:
            value_diff_condition = (F.col(self.config.value_col) == F.col("prev_value")) & (
                F.col(self.config.is_plausible_col)
            )
        else:
            value_diff_condition = F.col(self.config.value_col) == F.col("prev_value")
        value_diff = F.when(value_diff_condition, F.lit(0)).otherwise(F.lit(1))
        value_id = F.sum(F.col("value_diff")).over(running_w)

        return (
            df.withColumn("prev_value", prev_value)
            .withColumn("next_time", next_time)
            .withColumn("value_diff", value_diff)
            .withColumn("value_id", value_id)
        )

    def _remove_implausible_data_points(self, df: DataFrame) -> DataFrame:
        """Optionally drop rows flagged as implausible after interval assignment.

        When ``drop_implausible_data_points`` is ``True``, filters out rows whose
        ``is_plausible`` column is not ``True`` (dropping ``False`` and ``NULL``).
        Because this runs *after* :meth:`_assign_interval_ids`, the implausible sample
        has already served as an interval boundary: dropping it splits the surrounding
        interval in two rather than merging across it, and no interval is emitted for
        the implausible value itself.  When ``False``, the DataFrame is returned
        unchanged.

        Raises
        ------
        ValueError
            If filtering is enabled but the ``is_plausible`` column is absent.
        """
        if not self.drop_implausible_data_points:
            return df
        return df.filter(F.col(self.config.is_plausible_col))

    def _aggregate_intervals(self, df: DataFrame) -> DataFrame:
        """Collapse each interval's rows into a single ``(tstart, tend, value)`` row.

        Groups the tagged rows by container/channel/interval and reduces each interval to its
        start timestamp, end timestamp and (constant) value, dropping the
        intermediate ``value_id``.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with the container id and channel id columns, ``tstart``,
            ``tend`` and ``value``.
        """
        return (
            df.groupBy(
                F.col(self.config.container_id_col),
                F.col(self.config.channel_id_col),
                F.col("value_id"),
            )
            .agg(
                F.min(F.col(self.config.timestamp_col)).alias(self.config.tstart_col),
                F.max(F.col("next_time")).alias(self.config.tend_col),
                F.first(F.col(self.config.value_col)).alias(self.config.value_col),
            )
            .drop("value_id")
        )
