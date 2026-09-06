from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def group_id_columns(secondary_grouping_key: str | None = None) -> list[str]:
    """Return the identity columns fact rows are grouped/anchored on.

    ``["container_id"]`` normally, plus the optional secondary grouping key when
    one is configured. Used as the id list for ``unpivot`` and for the explicit
    ``select`` steps in each aggregation/event explode chain, so the key survives
    as an output dimension.
    """
    cols = ["container_id"]
    if secondary_grouping_key:
        cols.append(secondary_grouping_key)
    return cols


def fact_field_names(schema: StructType, secondary_grouping_key: str | None = None) -> list[str]:
    """Return ``schema.fieldNames()`` plus the secondary grouping key when set.

    The static fact schemas intentionally omit the optional secondary grouping
    key (its presence and name are config-driven); this appends it to the
    final projection when one is configured so it is carried into the gold
    fact table alongside the schema's columns.
    """
    names = list(schema.fieldNames())
    if secondary_grouping_key and secondary_grouping_key not in names:
        names.append(secondary_grouping_key)
    return names


def fact_projection_columns(df, schema: StructType, secondary_grouping_key: str | None = None):
    """``schema.fieldNames()`` plus the secondary grouping key when set AND on *df*.

    Presence-aware variant of :func:`fact_field_names` for the persistence
    projection: the optional key is kept only when it is actually a column on the
    frame being written (the static schema never lists it), so both the
    incremental and full-write paths keep it via one shared rule.
    """
    names = list(schema.fieldNames())
    if (
        secondary_grouping_key
        and secondary_grouping_key in df.columns
        and secondary_grouping_key not in names
    ):
        names.append(secondary_grouping_key)
    return names


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
        StructField("event_instance_id", LongType(), False),
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
