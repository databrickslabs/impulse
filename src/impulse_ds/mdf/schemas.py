"""Shared Spark schemas for MDF converter modules."""

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)


SIGNALS_SCHEMA = StructType([
    StructField("file_uri", StringType(), False),
    StructField("channel_id", IntegerType(), False),
    StructField("time", DoubleType(), False),
    StructField("value", DoubleType(), True),
])

METADATA_SCHEMA = StructType([
    StructField("file_uri", StringType(), False),
    StructField("channel_id", IntegerType(), False),
    StructField("group_idx", IntegerType(), False),
    StructField("channel_idx", IntegerType(), False),
    StructField("channel_name", StringType(), False),
    StructField("unit", StringType(), True),
    StructField("header_datetime", TimestampType(), True),
    StructField("md_comment", StringType(), True),
])
