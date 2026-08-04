from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

HISTOGRAM_FACT_SCHEMA = StructType(
    [
        StructField("container_id", IntegerType(), False),
        StructField("visual_id", IntegerType(), False),
        StructField("event_id", IntegerType(), True),
        StructField("bin_id", IntegerType(), False),
        StructField("hist_value", DoubleType(), False),
        StructField("lower_bound", DoubleType(), False),
        StructField("upper_bound", DoubleType(), False),
        StructField("bin_name", StringType(), False),
    ]
)

HISTOGRAM2D_FACT_SCHEMA = StructType(
    [
        StructField("container_id", IntegerType(), False),
        StructField("visual_id", IntegerType(), False),
        StructField("event_id", IntegerType(), True),
        StructField("x_bin_id", IntegerType(), False),
        StructField("y_bin_id", IntegerType(), False),
        StructField("hist_value", DoubleType(), False),
        StructField("x_lower_bound", DoubleType(), False),
        StructField("x_upper_bound", DoubleType(), False),
        StructField("y_lower_bound", DoubleType(), False),
        StructField("y_upper_bound", DoubleType(), False),
        StructField("x_bin_name", StringType(), False),
        StructField("y_bin_name", StringType(), False),
    ]
)

EVENT_INSTANCE_FACT_SCHEMA = StructType(
    [
        StructField("container_id", IntegerType(), False),
        StructField("event_instance_id", IntegerType(), False),
        StructField("event_id", IntegerType(), False),
        StructField("start_ts", LongType(), False),
        StructField("end_ts", LongType(), False),
    ]
)

STATS_AGGREGATOR_FACT_SCHEMA = StructType(
    [
        StructField("container_id", IntegerType(), False),
        StructField("visual_id", IntegerType(), False),
        StructField("channel_name", StringType(), False),
        StructField("event_id", IntegerType(), True),
        StructField("event_instance_id", LongType(), False),
        StructField("aggregation_label", StringType(), False),
        StructField("statistic_value", DoubleType(), False),
    ]
)

# Narrow, silver-shaped facts for calculated channels: one row per RLE sample
# interval. The channel's identity lives on ``calculated_channel_dimension`` (joined
# via ``channel_id``), so it is intentionally absent here. Field *types* are cosmetic
# — persistence projects by name only and the real container_id/channel_id types flow
# from the solved DataFrame.
CALCULATED_CHANNEL_FACT_SCHEMA = StructType(
    [
        StructField("container_id", IntegerType(), False),
        StructField("channel_id", LongType(), False),
        StructField("tstart", LongType(), False),
        StructField("tend", LongType(), False),
        StructField("value", DoubleType(), False),
    ]
)
