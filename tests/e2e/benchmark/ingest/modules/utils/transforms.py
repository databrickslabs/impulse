"""Spark transformations from the Renumics star schema to the Impulse silver layer.

Mapping:
- container = one trip (``session_id``; one ``trip_hash`` directory in the source)
- channel   = one signal (``signal_id``) within a trip
- time      = epoch microseconds of the (time-normalized, timezone-less) source timestamp

All functions are pure DataFrame → DataFrame so they can be unit-tested with a local
SparkSession. Callers must run with ``spark.sql.session.timeZone = UTC`` so that
TIMESTAMP_NTZ values convert to epoch microseconds without a timezone shift.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from .constants import CHANNELS_FORMAT_RLE
from .schema_definitions import (
    CONTAINER_METRICS_SCHEMA,
    TIMESERIES_TIME_SQL_TYPE,
    TIMESERIES_VALUE_SQL_TYPE,
)

PERCENTILES = (0.01, 0.10, 0.90, 0.99)
PERCENTILE_COLUMNS = ("pz1", "pz10", "pz90", "pz99")
PERCENTILE_ACCURACY = 10_000

# channel_tags key -> dim_signal_info column
CHANNEL_TAG_COLUMNS = {
    "channel_name": "signal_name",
    "signal_description": "signal_description",
    "unit": "unit",
    "physical_quantity": "physical_quantity",
    "category": "category",
    "category_description": "category_description",
    "subsystem": "subsystem",
    "subsystem_description": "subsystem_description",
    "component": "component",
    "group": "group",
    "is_categorical": "is_categorical",
    "dtype": "dtype",
    "signal_mapping": "signal_mapping",
    "storage_partition": "partition",
}

# container_metrics columns that are also exposed as EAV container_tags
CONTAINER_TAG_COLUMNS = (
    "vehicle_key",
    "manufacturer",
    "vehicle_type",
    "powertrain_type",
    "n_motor",
    "battery_voltage_oc",
    "empty_weight",
    "number_of_passengers",
    "tires",
    "driver",
    "trip_name",
    "starting_temperature",
    "trip_start_dt",
    "session_id",
    "trip_hash",
    "file_path",
    "file_count",
    "size_mb",
)


def use_utc_session(spark) -> None:
    """Pin the session timezone so TIMESTAMP_NTZ → epoch conversions are shift-free."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def is_inf(column):
    """True for +Inf / -Inf (portable; F.isinf is not available on all Spark runtimes)."""
    return (column == F.lit(float("inf"))) | (column == F.lit(float("-inf")))


def is_finite(column):
    """True when column is a non-null, non-NaN, non-infinite number."""
    return column.isNotNull() & ~F.isnan(column) & ~is_inf(column)


def ntz_to_epoch_us(column):
    """Epoch microseconds of a TIMESTAMP_NTZ, read as UTC wall-clock time."""
    return F.unix_micros(column.cast("timestamp")).cast(TIMESERIES_TIME_SQL_TYPE)


def samples_df(fact_df: DataFrame) -> DataFrame:
    """Raw fact rows → Impulse RAW samples ``(container_id, channel_id, timestamp, value)``.

    ``float_value`` and ``int_value`` are mutually exclusive in the source; ``string_value``
    is NULL throughout the published dataset and is not carried over.
    """
    return fact_df.select(
        F.col("session_id").cast("bigint").alias("container_id"),
        F.col("signal_id").cast("int").alias("channel_id"),
        ntz_to_epoch_us(F.col("timestamp")).alias("timestamp"),
        F.coalesce(F.col("float_value"), F.col("int_value").cast(TIMESERIES_VALUE_SQL_TYPE))
        .cast(TIMESERIES_VALUE_SQL_TYPE)
        .alias("value"),
    )


def rle_encode(samples: DataFrame) -> DataFrame:
    """Collapse consecutive equal samples into ``[tstart, tend)`` intervals per channel.

    Each interval ends at the timestamp of the next run's first sample; the final interval
    of a channel ends at its last sample (zero length when that sample starts a new run),
    matching how Impulse derives ``tend`` from RAW data.
    """
    w = Window.partitionBy("container_id", "channel_id").orderBy("timestamp")
    run_window = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    is_first = F.lag("timestamp").over(w).isNull()
    same_as_prev = F.col("value").eqNullSafe(F.lag("value").over(w))
    return (
        samples.withColumn("_next_ts", F.lead("timestamp").over(w))
        .withColumn("_new_run", F.when(is_first | ~same_as_prev, 1).otherwise(0))
        .withColumn("_run", F.sum("_new_run").over(run_window))
        .groupBy("container_id", "channel_id", "_run")
        .agg(
            F.min("timestamp").alias("tstart"),
            F.max(F.coalesce(F.col("_next_ts"), F.col("timestamp"))).alias("tend"),
            F.first("value").alias("value"),
        )
        .select(
            "container_id",
            "channel_id",
            F.col("tstart").cast(TIMESERIES_TIME_SQL_TYPE),
            F.col("tend").cast(TIMESERIES_TIME_SQL_TYPE),
            F.col("value").cast(TIMESERIES_VALUE_SQL_TYPE),
        )
    )


def channel_metrics_df(
    channels_df: DataFrame,
    channels_format: str,
    signal_info_df: DataFrame,
) -> DataFrame:
    """Per-(container, channel) statistics from the silver ``channels`` table.

    RLE: ``sample_count`` counts intervals, ``mean`` is duration-weighted (falls back to the
    plain mean when all intervals have zero length). RAW: plain sample statistics.
    ``std`` and percentiles are always unweighted over finite values.
    """
    if channels_format.upper() == CHANNELS_FORMAT_RLE:
        t_begin, t_end = F.col("tstart"), F.col("tend")
        weight = (F.col("tend") - F.col("tstart")).cast("double")
    else:
        t_begin = t_end = F.col("timestamp")
        weight = None

    valid = is_finite(F.col("value"))
    finite_value = F.when(valid, F.col("value"))
    plain_mean = F.avg(finite_value)
    if weight is None:
        mean = plain_mean
    else:
        weighted_mean = F.try_divide(
            F.sum(F.when(valid, F.col("value") * weight)),
            F.sum(F.when(valid, weight)),
        )
        mean = F.coalesce(F.when(F.sum(F.when(valid, weight)) > 0, weighted_mean), plain_mean)

    stats = (
        channels_df.groupBy("container_id", "channel_id")
        .agg(
            F.count(F.lit(1)).alias("sample_count"),
            F.avg(F.when(valid, 0.0).otherwise(1.0)).alias("nan_ratio"),
            F.min(finite_value).alias("min"),
            F.max(finite_value).alias("max"),
            mean.alias("mean"),
            F.stddev(finite_value).alias("std"),
            F.percentile_approx(finite_value, list(PERCENTILES), PERCENTILE_ACCURACY).alias("_pz"),
            F.min(t_begin).alias("begin_ts"),
            F.max(t_end).alias("end_ts"),
        )
        .withColumn("duration_s", ((F.col("end_ts") - F.col("begin_ts")) / 1e6).cast("double"))
    )
    for i, name in enumerate(PERCENTILE_COLUMNS):
        stats = stats.withColumn(name, F.col("_pz")[i])

    signal_names = signal_info_df.select(
        F.col("signal_id").cast("int").alias("channel_id"),
        F.col("signal_name").alias("channel_name"),
        F.col("unit"),
    )
    return stats.join(F.broadcast(signal_names), on="channel_id", how="left").select(
        F.col("container_id").cast("bigint"),
        F.col("channel_id").cast("int"),
        "channel_name",
        "unit",
        F.lit("DOUBLE").alias("value_type"),
        F.col("sample_count").cast("bigint"),
        F.col("nan_ratio").cast("double"),
        *[
            F.col(c).cast(TIMESERIES_VALUE_SQL_TYPE).alias(c)
            for c in ("min", "max", "mean", "std", *PERCENTILE_COLUMNS)
        ],
        F.col("begin_ts").cast(TIMESERIES_TIME_SQL_TYPE),
        F.col("end_ts").cast(TIMESERIES_TIME_SQL_TYPE),
        "duration_s",
        F.try_divide(F.col("sample_count"), F.col("duration_s"))
        .cast("double")
        .alias("sample_rate_hz"),
    )


def _unpivot_tags(df: DataFrame, id_cols: list[str], value_cols: list[str]) -> DataFrame:
    """Unpivot wide columns into EAV ``(key, value)`` rows, dropping empty values."""
    as_strings = df.select(*id_cols, *[F.col(c).cast("string").alias(c) for c in value_cols])
    return as_strings.unpivot(id_cols, value_cols, "key", "value").where(
        F.col("value").isNotNull() & (F.trim(F.col("value")) != "")
    )


def channel_tags_df(channel_keys_df: DataFrame, signal_info_df: DataFrame) -> DataFrame:
    """EAV ``channel_tags`` for the given ``(container_id, channel_id)`` pairs."""
    signal_tags = _unpivot_tags(
        signal_info_df.select(
            F.col("signal_id").cast("int").alias("channel_id"),
            *[F.col(src).alias(key) for key, src in CHANNEL_TAG_COLUMNS.items()],
        ),
        ["channel_id"],
        list(CHANNEL_TAG_COLUMNS),
    )
    return (
        channel_keys_df.select("container_id", "channel_id")
        .distinct()
        .join(F.broadcast(signal_tags), on="channel_id", how="inner")
        .select(
            F.col("container_id").cast("bigint"),
            F.col("channel_id").cast("int"),
            "key",
            "value",
        )
    )


def container_metrics_df(
    channel_metrics: DataFrame,
    status_batch: DataFrame,
    trip_info: DataFrame,
    vehicle_info: DataFrame,
    vehicle_state: DataFrame,
) -> DataFrame:
    """One row per ingested container, enriched with trip, vehicle and state dimensions.

    Time bounds come from the batch's ``channel_metrics`` (no extra scan of ``channels``).
    """
    bounds = channel_metrics.groupBy("container_id").agg(
        F.countDistinct("channel_id").cast("int").alias("num_channels"),
        F.min("begin_ts").alias("start_ts"),
        F.max("end_ts").alias("stop_ts"),
    )
    trips = trip_info.select(
        F.col("session_id").cast("bigint").alias("container_id"),
        F.col("session_id").cast("int").alias("session_id"),
        "trip_name",
        F.col("starting_temperature").cast("bigint").alias("starting_temperature"),
        F.col("trip_start_timestamp").cast("timestamp").alias("trip_start_dt"),
    )
    vehicles = vehicle_info.select(
        F.col("vehicle_id").cast("int").alias("vehicle_id"),
        F.col("vehicle_name").alias("vehicle_key"),
        "manufacturer",
        "vehicle_type",
        "powertrain_type",
        *[
            F.col(c).cast("double").alias(c)
            for c in ("n_motor", "battery_voltage_oc", "empty_weight")
        ],
    )
    states = vehicle_state.select(
        F.col("vehicle_state_id").cast("int").alias("vehicle_state_id"),
        F.col("number_of_passengers").cast("bigint").alias("number_of_passengers"),
        "tires",
        "driver",
    )
    batch = status_batch.select(
        F.col("container_id").cast("bigint"),
        "trip_hash",
        F.col("source_path").alias("file_path"),
        F.col("vehicle_id").cast("int"),
        F.col("vehicle_state_id").cast("int"),
        F.col("file_count").cast("int"),
        (F.col("size_bytes") / (1024 * 1024)).cast("double").alias("size_mb"),
    )
    df = (
        bounds.join(batch, on="container_id", how="inner")
        .join(F.broadcast(trips), on="container_id", how="left")
        .join(F.broadcast(vehicles), on="vehicle_id", how="left")
        .join(F.broadcast(states), on="vehicle_state_id", how="left")
        .withColumn("start_dt", F.timestamp_micros(F.col("start_ts")))
        .withColumn("stop_dt", F.timestamp_micros(F.col("stop_ts")))
        .withColumn("duration_s", ((F.col("stop_ts") - F.col("start_ts")) / 1e6).cast("double"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    return df.select(*[F.col(f.name).cast(f.dataType) for f in CONTAINER_METRICS_SCHEMA.fields])


def container_tags_df(container_metrics: DataFrame) -> DataFrame:
    """EAV ``container_tags`` from the descriptive ``container_metrics`` columns."""
    return _unpivot_tags(container_metrics, ["container_id"], list(CONTAINER_TAG_COLUMNS))
