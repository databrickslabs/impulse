"""Unit tests for DefinitionHashComparator."""

from pyspark.sql import Row

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.incremental.definition_hash_comparator import (
    DefinitionHashComparator,
)

# from tests.conftest import spark


def test_empty_events_list_returns_empty_tuples(spark):
    """Test that empty events list returns two empty lists."""
    comparator = DefinitionHashComparator(spark)

    changed, unchanged = comparator.group_events_by_hash_change(
        events=[], event_dimension_table="catalog.gold.event_dimension"
    )

    assert changed == []
    assert unchanged == []


def test_all_events_changed_when_table_does_not_exist(spark):
    """Test that all events are marked as changed when gold table doesn't exist."""
    comparator = DefinitionHashComparator(spark)
    expr = TimeSeriesSelector(None) > 0
    events = [
        BasicEvent(name="event_1", expr=expr),
        BasicEvent(name="event_2", expr=expr),
    ]

    # Use a non-existent table name
    changed, unchanged = comparator.group_events_by_hash_change(
        events=events,
        event_dimension_table="nonexistent_catalog.nonexistent_schema.nonexistent_table",
    )

    assert len(changed) == 2
    assert len(unchanged) == 0
    assert events[0] in changed
    assert events[1] in changed


def test_new_event_is_marked_as_changed(spark):
    """Test that a new event not in gold table is marked as changed."""
    comparator = DefinitionHashComparator(spark)
    expr = TimeSeriesSelector(None) > 0

    # Create an event
    new_event = BasicEvent(name="new_event", expr=expr)
    existing_event = BasicEvent(name="existing_event", expr=expr)

    # Create a mock dimension table with only the existing event
    dimension_data = [
        Row(
            event_id=existing_event.get_id(),
            definition_hash=existing_event.determine_definition_hash(),
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_event_dimension_new"
    )

    try:
        changed, unchanged = comparator.group_events_by_hash_change(
            events=[new_event, existing_event],
            event_dimension_table="spark_catalog.default.test_event_dimension_new",
        )

        assert new_event in changed
        assert existing_event in unchanged
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_event_dimension_new")


def test_unchanged_event_with_matching_hash(spark):
    """Test that event with matching hash is marked as unchanged."""
    comparator = DefinitionHashComparator(spark)
    expr = TimeSeriesSelector(None) > 0
    event = BasicEvent(name="test_event", expr=expr)

    # Create dimension table with matching hash
    dimension_data = [
        Row(
            event_id=event.get_id(),
            definition_hash=event.determine_definition_hash(),
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_event_dimension_match"
    )

    try:
        changed, unchanged = comparator.group_events_by_hash_change(
            events=[event],
            event_dimension_table="spark_catalog.default.test_event_dimension_match",
        )

        assert len(changed) == 0
        assert len(unchanged) == 1
        assert event in unchanged
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_event_dimension_match")


def test_changed_event_with_different_hash(spark):
    """Test that event with different hash is marked as changed."""
    comparator = DefinitionHashComparator(spark)
    expr = TimeSeriesSelector(None) > 0
    event = BasicEvent(name="test_event", expr=expr)

    # Create dimension table with different hash (simulating definition change)
    dimension_data = [
        Row(
            event_id=event.get_id(),
            definition_hash=12345678,  # Different from actual hash
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_event_dimension_diff"
    )

    try:
        changed, unchanged = comparator.group_events_by_hash_change(
            events=[event],
            event_dimension_table="spark_catalog.default.test_event_dimension_diff",
        )

        assert len(changed) == 1
        assert len(unchanged) == 0
        assert event in changed
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_event_dimension_diff")


def test_empty_aggregations_list_returns_empty_tuples(spark):
    """Test that empty aggregations list returns two empty lists."""
    comparator = DefinitionHashComparator(spark)

    changed, unchanged = comparator.group_aggregations_by_hash_change(
        aggregations=[], dimension_table="catalog.gold.histogram_dimension"
    )

    assert changed == []
    assert unchanged == []


def test_all_aggregations_changed_when_table_does_not_exist(spark):
    """Test that all aggregations are changed when gold table doesn't exist."""
    comparator = DefinitionHashComparator(spark)
    base_expr = TimeSeriesSelector(None)
    aggregations = [
        HistogramDuration(name="hist_1", base_expr=base_expr, bins=[0.0, 1.0, 2.0]),
        HistogramDuration(name="hist_2", base_expr=base_expr, bins=[0.0, 1.0, 2.0]),
    ]

    changed, unchanged = comparator.group_aggregations_by_hash_change(
        aggregations=aggregations,
        dimension_table="nonexistent_catalog.nonexistent_schema.nonexistent_table",
    )

    assert len(changed) == 2
    assert len(unchanged) == 0


def test_new_aggregation_is_marked_as_changed(spark):
    """Test that a new aggregation not in gold table is marked as changed."""
    comparator = DefinitionHashComparator(spark)
    base_expr = TimeSeriesSelector(None)

    new_hist = HistogramDuration(name="new_histogram", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
    existing_hist = HistogramDuration(
        name="existing_histogram", base_expr=base_expr, bins=[0.0, 1.0, 2.0]
    )

    # Create dimension table with only existing histogram
    dimension_data = [
        Row(
            visual_id=existing_hist.get_id(),
            definition_hash=existing_hist.determine_definition_hash(),
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_hist_dimension_new"
    )

    try:
        changed, unchanged = comparator.group_aggregations_by_hash_change(
            aggregations=[new_hist, existing_hist],
            dimension_table="spark_catalog.default.test_hist_dimension_new",
        )

        assert new_hist in changed
        assert existing_hist in unchanged
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_hist_dimension_new")


def test_unchanged_aggregation_with_matching_hash(spark):
    """Test that aggregation with matching hash is marked as unchanged."""
    comparator = DefinitionHashComparator(spark)
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="test_histogram", base_expr=base_expr, bins=[0.0, 50.0, 100.0])

    dimension_data = [
        Row(
            visual_id=hist.get_id(),
            definition_hash=hist.determine_definition_hash(),
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_hist_dimension_match"
    )

    try:
        changed, unchanged = comparator.group_aggregations_by_hash_change(
            aggregations=[hist],
            dimension_table="spark_catalog.default.test_hist_dimension_match",
        )

        assert len(changed) == 0
        assert len(unchanged) == 1
        assert hist in unchanged
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_hist_dimension_match")


def test_changed_aggregation_with_different_hash(spark):
    """Test that aggregation with different hash is marked as changed."""
    comparator = DefinitionHashComparator(spark)
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="test_histogram", base_expr=base_expr, bins=[0.0, 50.0, 100.0])

    # Create dimension table with different hash (simulating bins change)
    dimension_data = [
        Row(
            visual_id=hist.get_id(),
            definition_hash=99999999,  # Different from actual hash
        )
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_hist_dimension_diff"
    )

    try:
        changed, unchanged = comparator.group_aggregations_by_hash_change(
            aggregations=[hist],
            dimension_table="spark_catalog.default.test_hist_dimension_diff",
        )

        assert len(changed) == 1
        assert len(unchanged) == 0
        assert hist in changed
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_hist_dimension_diff")


def test_mixed_changed_and_unchanged_aggregations(spark):
    """Test correct classification of mixed changed and unchanged aggregations."""
    comparator = DefinitionHashComparator(spark)
    base_expr = TimeSeriesSelector(None)

    # Create multiple histograms
    hist_unchanged = HistogramDuration(
        name="hist_unchanged", base_expr=base_expr, bins=[0.0, 1.0, 2.0]
    )
    hist_changed = HistogramDuration(
        name="hist_changed", base_expr=base_expr, bins=[0.0, 1.0, 2.0]
    )
    hist_new = HistogramDuration(name="hist_new", base_expr=base_expr, bins=[0.0, 1.0, 2.0])

    # Create dimension table with:
    # - hist_unchanged: matching hash
    # - hist_changed: different hash
    # - hist_new: not present
    dimension_data = [
        Row(
            visual_id=hist_unchanged.get_id(),
            definition_hash=hist_unchanged.determine_definition_hash(),
        ),
        Row(
            visual_id=hist_changed.get_id(),
            definition_hash=11111111,  # Different hash
        ),
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_hist_dimension_mixed"
    )

    try:
        changed, unchanged = comparator.group_aggregations_by_hash_change(
            aggregations=[hist_unchanged, hist_changed, hist_new],
            dimension_table="spark_catalog.default.test_hist_dimension_mixed",
        )

        assert len(changed) == 2
        assert len(unchanged) == 1
        assert hist_unchanged in unchanged
        assert hist_changed in changed
        assert hist_new in changed
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_hist_dimension_mixed")


def test_table_exists_returns_true_for_existing_table(spark):
    """Test that _table_exists returns True for existing table."""
    comparator = DefinitionHashComparator(spark)
    result = comparator._table_exists("spark_catalog.silver.container_metrics")
    assert result is True


def test_table_exists_returns_false_for_nonexistent_table(spark):
    """Test that _table_exists returns False for non-existent table."""
    comparator = DefinitionHashComparator(spark)

    result = comparator._table_exists("nonexistent_catalog.nonexistent_schema.nonexistent_table")

    assert result is False


# ---------------------------------------------------------------------------
# Calculated channel variant (group_calculated_channels_by_hash_change)
# ---------------------------------------------------------------------------
def _calc_channel(name, factor=3.6, identity=None):
    from impulse_reporting.channels.calculated_channel import CalculatedChannel

    return CalculatedChannel(
        name=name,
        expr=TimeSeriesSelector(None) * factor,
        identity=identity or {"channel_name": name, "data_key": "CALC"},
    )


def test_all_calculated_channels_changed_when_table_does_not_exist(spark):
    """All channels are changed when the gold dimension table doesn't exist."""
    comparator = DefinitionHashComparator(spark)
    channels = [_calc_channel("speed_kmh"), _calc_channel("rpm_x2")]

    changed, unchanged = comparator.group_calculated_channels_by_hash_change(
        channels=channels,
        dimension_table="nonexistent_catalog.nonexistent_schema.nonexistent_table",
    )

    assert len(changed) == 2
    assert len(unchanged) == 0


def test_unchanged_calculated_channel_with_matching_hash(spark):
    """A channel whose stored hash matches is marked unchanged."""
    comparator = DefinitionHashComparator(spark)
    ch = _calc_channel("speed_kmh")

    dimension_data = [Row(channel_id=ch.get_id(), definition_hash=ch.determine_definition_hash())]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_calc_channel_dim_match"
    )

    try:
        changed, unchanged = comparator.group_calculated_channels_by_hash_change(
            channels=[ch],
            dimension_table="spark_catalog.default.test_calc_channel_dim_match",
        )
        assert changed == []
        assert unchanged == [ch]
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_calc_channel_dim_match")


def test_changed_calculated_channel_with_different_hash(spark):
    """A channel whose expression changed (different hash) is marked changed."""
    comparator = DefinitionHashComparator(spark)
    ch = _calc_channel("speed_kmh", factor=3.7)

    # Store a stale hash (as if the channel was previously defined with * 3.6).
    stale = _calc_channel("speed_kmh", factor=3.6)
    dimension_data = [
        Row(channel_id=stale.get_id(), definition_hash=stale.determine_definition_hash())
    ]
    spark.createDataFrame(dimension_data).write.mode("overwrite").saveAsTable(
        "spark_catalog.default.test_calc_channel_dim_changed"
    )

    try:
        changed, unchanged = comparator.group_calculated_channels_by_hash_change(
            channels=[ch],
            dimension_table="spark_catalog.default.test_calc_channel_dim_changed",
        )
        # Same identity → same channel_id as the stored row, but the expression
        # hash differs, so it must be classified as changed.
        assert ch.get_id() == stale.get_id()
        assert changed == [ch]
        assert unchanged == []
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.default.test_calc_channel_dim_changed")
