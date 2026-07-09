import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig


class RleEncoder:
    """Utility class for run-length encoding raw channel data.

    Consecutive samples that share the same ``value`` within a ``container_id`` /
    ``channel_id`` are collapsed into a single interval, removing redundant points
    from signals that stay constant over time.
    """

    def __init__(
        self,
        config: SolverConfig | None = None,
        timestamp_col_name: str = "timestamp",
        drop_implausible_data_points: bool = False,
    ):
        """
        Initialize the RleEncoder.

        Parameters
        ----------
        config : SolverConfig
            Solver configuration providing the container id and channel id column names.
        timestamp_col_name : str, optional
            Name of the timestamp column in the input DataFrame.  Default is "timestamp".
        drop_implausible_data_points : bool, optional
            Whether to drop implausible data points before encoding.  If True, rows
            where ``is_plausible`` is not True are removed.  Default is False.
        """

        if config is None:
            raise ValueError("SolverConfig must be provided to RleEncoder.")

        self.config: SolverConfig = config
        self.timestamp_col_name: str = timestamp_col_name
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
        Interval ids are assigned over the *full* input, so an implausible sample
        still acts as an interval boundary.  Implausible rows are only removed
        afterwards (when ``drop_implausible_data_points`` is set), so a dropped
        implausible sample splits the surrounding interval rather than being bridged
        over -- it simply is not emitted as its own interval.
        """
        return (
            df.transform(self._assign_interval_ids)
            .transform(self._remove_implausible_data_points)
            .transform(self._aggregate_intervals)
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
        if "is_plausible" not in df.columns:
            raise ValueError(
                "DataFrame must contain an 'is_plausible' column "
                "to drop implausible data points."
            )
        return df.filter(F.col("is_plausible"))

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
        ).orderBy(F.col(self.timestamp_col_name).asc())

        running_w = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        prev_value = F.lag(F.col(self.config.value_col)).over(w)
        next_time = F.coalesce(
            F.lead(F.col(self.timestamp_col_name)).over(w), F.col(self.timestamp_col_name)
        )

        if self.drop_implausible_data_points:
            value_diff_condition = (F.col(self.config.value_col) == F.col("prev_value")) & (
                F.col("is_plausible")
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
                F.min(F.col(self.timestamp_col_name)).alias(self.config.tstart_col),
                F.max(F.col("next_time")).alias(self.config.tend_col),
                F.first(F.col(self.config.value_col)).alias(self.config.value_col),
            )
            .drop("value_id")
        )
