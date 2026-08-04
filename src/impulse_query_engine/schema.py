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

# Optional POI (point-of-interest) table. One row per occurrence (e.g. "AEB fired here").
# A POI is a point in time — no duration, no sample rate, no value that persists between
# entries. Under Option D, POI is a pure occurrence log: it always evaluates to a
# PointsInTime, and a signal's value *at* an occurrence comes from sampling the measured
# channel (``q.channel(...).where(q.poi(...))``), not from a POI column. Non-spine columns
# (``poi_type``, ``duration``, ``event_type``, …) are row-filterable via
# ``q.poi(...).having(q.poi_metric("duration") > 5)``. This schema mirrors the external
# ``tech_rds_dev.poi.poi`` table and is used for fixtures.
POI_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        # Occurrence time. This is PoiConfig.ts_column and MUST be a datetime / timestamp
        # column: the solver reads it directly as an absolute instant (unix_micros), with
        # no unit or origin assumptions.
        T.StructField("timestamp", T.TimestampType()),
        # Kind discriminator: q.poi(poi_type="aeb") filters on this.
        T.StructField("poi_type", T.StringType()),
        # Row-filterable spine columns (q.poi_metric(...) in a .having(...) predicate).
        T.StructField("duration", T.DoubleType()),
        T.StructField("event_type", T.StringType()),
    ]
)
