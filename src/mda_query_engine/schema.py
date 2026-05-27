"""Schemas for data tables"""

import pyspark.sql.types as T

CONTAINER_TAGS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("key", T.StringType()),
        T.StructField("value", T.StringType()),
    ]
)

CONTAINER_METRICS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("start_dt", T.TimestampType()),
        T.StructField("stop_dt", T.TimestampType()),
        T.StructField("duration_ms", T.IntegerType()),
        T.StructField("num_channels", T.IntegerType()),
    ]
)

CHANNEL_TAGS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("key", T.StringType()),
        T.StructField("value", T.StringType()),
    ]
)

# Static reference table mapping ordinal channel integer values to human-readable labels.
# Single source of truth for label decoding (TSAL constants, SQL joins, UC tag application).
# Static per channel definition; not written per recording.
# Join on (channel_id, numeric_value) to decode any ordinal channel in SQL.
CHANNEL_VALUE_LABELS = T.StructType(
    [
        T.StructField("channel_id", T.LongType(), nullable=False),
        T.StructField("numeric_value", T.DoubleType(), nullable=False),
        T.StructField("label", T.StringType(), nullable=False),
    ]
)

CHANNEL_METRICS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("value_type", T.StringType()),
        T.StructField("sample_count", T.IntegerType()),
        T.StructField("nan_ratio", T.FloatType()),
        T.StructField("begin_s", T.FloatType()),
        T.StructField("end_s", T.FloatType()),
        T.StructField("duration_ms", T.IntegerType()),
        T.StructField("original_sample_count", T.IntegerType()),
        T.StructField("original_sr", T.FloatType()),
        T.StructField("min", T.FloatType()),
        T.StructField("max", T.FloatType()),
        T.StructField("mean", T.FloatType()),
        T.StructField("std", T.FloatType()),
        T.StructField("pz1", T.FloatType()),
        T.StructField("pz10", T.FloatType()),
        T.StructField("pz90", T.FloatType()),
        T.StructField("pz99", T.FloatType()),
    ]
)

CHANNELS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("tstart", T.LongType(), nullable=False),
        T.StructField("tend", T.LongType(), nullable=False),
        T.StructField("value", T.DoubleType()),
    ]
)

CHANNELS_SCHEMA_WITHOUT_RLE = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("timestamp", T.LongType(), nullable=False),
        T.StructField("value", T.DoubleType()),
    ]
)
