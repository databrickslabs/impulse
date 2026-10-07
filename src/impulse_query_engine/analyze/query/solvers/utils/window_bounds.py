"""Container bounds in the channel time frame, for ``TimeWindowEvent`` windows.

Both the ``TimeWindowEvent`` event fact and the solve (``TimeWindowExpression`` via the
container metadata) derive their window bounds here, from the raw ``container_metrics``
``start_ts`` / ``stop_ts`` and the ``SolverConfig`` channel time settings
(``channel_time_unit``, ``channel_time_origin``, ``container_time_unit``).
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig

# Nanoseconds per time unit, for converting container boundaries into the channel unit.
_NANOS_PER_UNIT = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}


def with_window_bounds(df: DataFrame, config: SolverConfig) -> DataFrame:
    """Add the container start/stop in the channel time frame, for ``TimeWindowEvent``.

    A ``TimeWindowEvent`` tiles each container into windows that must be in the same time
    frame as the channel timestamps (``config.channel_time_unit``,
    ``config.channel_time_origin``).  This adds ``config.window_start_col`` /
    ``config.window_stop_col``, derived from the raw ``start_ts`` / ``stop_ts``, which stay
    unchanged for UDFs, ``ContainerEvent`` and ``measurement_dimension``:

    - origin ``"epoch"``: ``TIMESTAMP`` boundaries as epoch numbers in
      ``channel_time_unit``; numeric boundaries converted from ``container_time_unit`` to
      ``channel_time_unit`` (as they are when unset);
    - origin ``"container_start"``: ``0`` and ``stop_ts - start_ts``, converted the same way
      (the difference is taken first, in the boundaries' own unit).

    ``TIMESTAMP`` values are converted via ``unix_micros``, which is exact and independent of
    the session time zone; ``"s"`` / ``"ms"`` give doubles, ``"us"`` / ``"ns"`` longs.
    Numeric boundaries converted to a finer unit are multiplied by an integer (exact,
    keeping longs), to a coarser unit divided (doubles).  The event fact and the solve both
    call this, so their windows use the same bounds.  The types are checked on the schema,
    so a missing setting fails before any Spark job runs.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Column-mapped ``container_metrics`` frame (or a projection of it) with ``start_ts``
        and ``stop_ts``.
    config : SolverConfig
        Solver configuration holding the channel time settings and column names.

    Returns
    -------
    pyspark.sql.DataFrame
        *df* with the two window-bound columns added.

    Raises
    ------
    ValueError
        If ``start_ts`` / ``stop_ts`` are missing, are ``TIMESTAMP_NTZ`` or ``DATE``, mix
        ``TIMESTAMP`` and numeric types, or are ``TIMESTAMP`` while ``channel_time_unit`` is
        unset or ``container_time_unit`` is set.
    """
    types = {field.name: field.dataType for field in _boundary_fields(df, config)}
    missing = [c for c in (config.start_ts_col, config.stop_ts_col) if c not in types]
    if missing:
        raise ValueError(
            f"TimeWindowEvent needs the container_metrics columns {missing} to compute "
            f"its windows. Available columns: {df.columns}"
        )
    for name, dtype in types.items():
        if isinstance(dtype, (T.TimestampNTZType, T.DateType)):
            raise ValueError(
                f"container_metrics column '{name}' has type {dtype.simpleString()}, "
                "which cannot be converted to an epoch unambiguously (it carries no time "
                "zone). Use a TIMESTAMP or epoch-number column."
            )
    is_timestamp = {isinstance(dtype, T.TimestampType) for dtype in types.values()}
    if len(is_timestamp) > 1:
        raise ValueError(
            f"container_metrics columns '{config.start_ts_col}' and '{config.stop_ts_col}' "
            "must both be TIMESTAMP or both be numeric to compute TimeWindowEvent windows."
        )
    timestamps = is_timestamp.pop()
    if timestamps and config.channel_time_unit is None:
        raise ValueError(
            f"TimeWindowEvent needs its windows in the channel time frame, but "
            f"container_metrics '{config.start_ts_col}' / '{config.stop_ts_col}' are "
            "TIMESTAMP columns. Set query_engine.solver_config.channel_time_unit to the "
            "unit of the channel timestamps (one of 's', 'ms', 'us', 'ns'), and "
            "channel_time_origin to 'container_start' if they are relative to the "
            "container start."
        )
    if timestamps and config.container_time_unit is not None:
        raise ValueError(
            f"container_time_unit only applies to numeric container_metrics "
            f"'{config.start_ts_col}' / '{config.stop_ts_col}', but they are TIMESTAMP "
            "columns, which carry their own unit. Remove container_time_unit."
        )

    start, stop = F.col(config.start_ts_col), F.col(config.stop_ts_col)
    unit = config.channel_time_unit
    if config.channel_time_origin == "container_start":
        window_start = F.lit(0)
        # Subtract exactly in microseconds before scaling to the channel unit.
        window_stop = (
            _micros_in_unit(F.unix_micros(stop) - F.unix_micros(start), unit)
            if timestamps
            else _container_to_channel_unit(stop - start, config)
        )
    elif timestamps:
        window_start = _micros_in_unit(F.unix_micros(start), unit)
        window_stop = _micros_in_unit(F.unix_micros(stop), unit)
    else:
        window_start = _container_to_channel_unit(start, config)
        window_stop = _container_to_channel_unit(stop, config)
    return df.withColumn(config.window_start_col, window_start).withColumn(
        config.window_stop_col, window_stop
    )


def _boundary_fields(df: DataFrame, config: SolverConfig) -> list[T.StructField]:
    """Return the container start/stop timestamp fields present on *df*."""
    names = {config.start_ts_col, config.stop_ts_col}
    return [field for field in df.schema.fields if field.name in names]


def _container_to_channel_unit(col: Column, config: SolverConfig) -> Column:
    """Numeric boundary *col* converted from ``container_time_unit`` to ``channel_time_unit``
    (unchanged when unset or equal)."""
    if (
        config.container_time_unit is None
        or config.container_time_unit == config.channel_time_unit
    ):
        return col
    source = _NANOS_PER_UNIT[config.container_time_unit]
    target = _NANOS_PER_UNIT[config.channel_time_unit]
    if source > target:
        # Finer target unit: an integer factor keeps long boundaries exact. A long literal
        # widens INT boundaries to long (int * int would stay int and overflow, e.g. epoch
        # seconds * 1000); doubles and decimals keep their type.
        return col * F.lit(source // target).cast(T.LongType())
    return col / F.lit(float(target // source))


def _micros_in_unit(micros: Column, unit: str | None) -> Column:
    """Microseconds converted to *unit*."""
    if unit == "s":
        return micros / F.lit(1e6)
    if unit == "ms":
        return micros / F.lit(1e3)
    if unit == "ns":
        return micros * F.lit(1000)
    return micros
