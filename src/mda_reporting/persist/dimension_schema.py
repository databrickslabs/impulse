from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

HISTOGRAM_DIMENSION_SCHEMA = StructType(
    [
        StructField("visual_id", IntegerType(), True),
        StructField("report_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("page_number", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("agg_type", StringType(), True),
        StructField("bins", ArrayType(DoubleType(), False), True),
        StructField("channel_name", StringType(), True),
        StructField("signal_expression", StringType(), True),
        StructField("weights_channel_name", StringType(), True),
        StructField("weights_expression", StringType(), True),
        StructField("values_unit", StringType(), True),
        StructField("bins_unit", StringType(), True),
        StructField("definition_hash", LongType(), True),
    ]
)

HISTOGRAM2D_DIMENSION_SCHEMA = StructType(
    [
        StructField("visual_id", IntegerType(), True),
        StructField("report_id", IntegerType(), True),
        StructField("page_number", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("agg_type", StringType(), True),
        StructField("x_bins", ArrayType(DoubleType(), False), True),
        StructField("y_bins", ArrayType(DoubleType(), False), True),
        StructField("x_channel_name", StringType(), True),
        StructField("x_signal_expression", StringType(), True),
        StructField("y_channel_name", StringType(), True),
        StructField("y_signal_expression", StringType(), True),
        StructField("weights_channel_name", StringType(), True),
        StructField("weights_expression", StringType(), True),
        StructField("values_unit", StringType(), True),
        StructField("x_bins_unit", StringType(), True),
        StructField("y_bins_unit", StringType(), True),
        StructField("definition_hash", LongType(), True),
    ]
)

EVENT_DIMENSION_SCHEMA = StructType(
    [
        StructField("event_id", IntegerType(), False),
        StructField("report_id", IntegerType(), False),
        StructField("event_type", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_description", StringType(), True),
        StructField("required_channels", ArrayType(StringType()), True),
        StructField("event_expression", StringType(), True),
        StructField("definition_hash", LongType(), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
    ]
)

STATS_AGGREGATOR_DIMENSION_SCHEMA = StructType(
    [
        StructField("visual_id", IntegerType(), False),
        StructField("report_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("page_number", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("agg_type", StringType(), True),
        StructField("statistics", ArrayType(StringType()), True),
        StructField("channel_names", ArrayType(StringType()), True),
        StructField("signal_expressions", ArrayType(StringType()), True),
        StructField("values_unit", StringType(), True),
        StructField("definition_hash", LongType(), True),
    ]
)
