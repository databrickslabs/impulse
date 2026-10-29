import pyspark.sql.types as T

# Impulse silver-layer conventions: channel time in epoch microseconds (long), values as double.
TIMESERIES_TIME_TYPE = T.LongType()
TIMESERIES_TIME_SQL_TYPE = "bigint"

TIMESERIES_VALUE_TYPE = T.DoubleType()
TIMESERIES_VALUE_SQL_TYPE = "double"

# The 11 columns present in every fact_signal_data source file. The two
# `_ducklake_internal_*` columns exist only in some files and are storage metadata,
# so an explicit read schema drops them and keeps reads consistent across files.
FACT_SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("timestamp", T.TimestampNTZType()),
        T.StructField("signal_id", T.IntegerType()),
        T.StructField("session_id", T.IntegerType()),
        T.StructField("vehicle_id", T.IntegerType()),
        T.StructField("vehicle_state_id", T.IntegerType()),
        T.StructField("float_value", T.DoubleType()),
        T.StructField("int_value", T.LongType()),
        T.StructField("string_value", T.StringType()),
        T.StructField("trip_hash", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("vehicle_name", T.StringType()),
    ]
)

STATUS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("trip_hash", T.StringType(), nullable=False),
        T.StructField("source_path", T.StringType(), nullable=False),
        T.StructField("vehicle_id", T.IntegerType()),
        T.StructField("vehicle_state_id", T.IntegerType()),
        T.StructField("file_count", T.IntegerType()),
        T.StructField("size_bytes", T.LongType()),
        T.StructField("run_id", T.StringType()),
        T.StructField("status", T.StringType(), nullable=False),
        T.StructField("_load_ts", T.TimestampType()),
        T.StructField("_processing_done_ts", T.TimestampType()),
    ]
)

CHANNELS_RLE_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("tstart", TIMESERIES_TIME_TYPE, nullable=False),
        T.StructField("tend", TIMESERIES_TIME_TYPE, nullable=False),
        T.StructField("value", TIMESERIES_VALUE_TYPE, nullable=True),
    ]
)

CHANNELS_RAW_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("timestamp", TIMESERIES_TIME_TYPE, nullable=False),
        T.StructField("value", TIMESERIES_VALUE_TYPE, nullable=True),
    ]
)

CHANNEL_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("channel_name", T.StringType()),
        T.StructField("unit", T.StringType()),
        T.StructField("value_type", T.StringType()),
        T.StructField("sample_count", T.LongType()),
        T.StructField("nan_ratio", T.DoubleType()),
        T.StructField("min", TIMESERIES_VALUE_TYPE),
        T.StructField("max", TIMESERIES_VALUE_TYPE),
        T.StructField("mean", TIMESERIES_VALUE_TYPE),
        T.StructField("std", TIMESERIES_VALUE_TYPE),
        T.StructField("pz1", TIMESERIES_VALUE_TYPE),
        T.StructField("pz10", TIMESERIES_VALUE_TYPE),
        T.StructField("pz90", TIMESERIES_VALUE_TYPE),
        T.StructField("pz99", TIMESERIES_VALUE_TYPE),
        T.StructField("begin_ts", TIMESERIES_TIME_TYPE),
        T.StructField("end_ts", TIMESERIES_TIME_TYPE),
        T.StructField("duration_s", T.DoubleType()),
        T.StructField("sample_rate_hz", T.DoubleType()),
    ]
)

TAGS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("key", T.StringType()),
        T.StructField("value", T.StringType()),
    ]
)

CONTAINER_TAGS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("key", T.StringType()),
        T.StructField("value", T.StringType()),
    ]
)

CONTAINER_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("vehicle_key", T.StringType()),
        T.StructField("session_id", T.IntegerType()),
        T.StructField("trip_hash", T.StringType()),
        T.StructField("trip_name", T.StringType()),
        T.StructField("vehicle_id", T.IntegerType()),
        T.StructField("manufacturer", T.StringType()),
        T.StructField("vehicle_type", T.StringType()),
        T.StructField("powertrain_type", T.StringType()),
        T.StructField("n_motor", T.DoubleType()),
        T.StructField("battery_voltage_oc", T.DoubleType()),
        T.StructField("empty_weight", T.DoubleType()),
        T.StructField("vehicle_state_id", T.IntegerType()),
        T.StructField("number_of_passengers", T.LongType()),
        T.StructField("tires", T.StringType()),
        T.StructField("driver", T.StringType()),
        T.StructField("starting_temperature", T.LongType()),
        T.StructField("trip_start_dt", T.TimestampType()),
        T.StructField("start_ts", TIMESERIES_TIME_TYPE),
        T.StructField("stop_ts", TIMESERIES_TIME_TYPE),
        T.StructField("start_dt", T.TimestampType()),
        T.StructField("stop_dt", T.TimestampType()),
        T.StructField("duration_s", T.DoubleType()),
        T.StructField("num_channels", T.IntegerType()),
        T.StructField("file_path", T.StringType()),
        T.StructField("file_count", T.IntegerType()),
        T.StructField("size_mb", T.DoubleType()),
        T.StructField("_ingested_at", T.TimestampType()),
    ]
)

# (table name, empty schema, cluster columns) for every silver table except `channels`,
# whose schema depends on the configured channels_format.
SILVER_TABLE_SPECS = [
    ("channel_metrics", CHANNEL_METRICS_SCHEMA, ["container_id", "channel_id"]),
    ("channel_tags", TAGS_SCHEMA, ["container_id", "channel_id"]),
    ("container_metrics", CONTAINER_METRICS_SCHEMA, ["container_id"]),
    ("container_tags", CONTAINER_TAGS_SCHEMA, ["container_id"]),
]


def channels_schema(channels_format: str) -> T.StructType:
    """Return the `channels` table schema for the RLE or RAW format."""
    return CHANNELS_RLE_SCHEMA if channels_format.upper() == "RLE" else CHANNELS_RAW_SCHEMA
