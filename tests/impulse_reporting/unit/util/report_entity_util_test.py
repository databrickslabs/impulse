import operator

from impulse_query_engine.analyze.metadata.metric_expression import MetricOp, MetricSelector
from impulse_query_engine.analyze.metadata.tag_expression import TagOp, TagSelector
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.config.config_parser import (
    CastType,
    Comparator,
    MetricFilter,
    TagFilter,
)
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.util.report_entity_util import ReportEntityUtil


def test_get_event_id_column_empty_events(spark):
    """Test get_event_id_column static method with empty events list."""
    events = []

    col_expr = ReportEntityUtil.get_event_id_column(events, "event_name")

    test_data = [("any_event",), ("another_event",)]
    df = spark.createDataFrame(test_data, ["event_name"])

    result_df = df.withColumn("event_id", col_expr)
    results = result_df.collect()

    # All events should map to None when events list is empty
    assert results[0]["event_id"] is None
    assert results[1]["event_id"] is None


def test_get_event_id_column(spark, basic_narrow_db):
    """Test get_event_id_column static method with several events"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event1 = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    event2 = BasicEvent(name="test_event_2", expr=eng_rpm < 1000)

    events = [event1, event2]

    col_expr = ReportEntityUtil.get_event_id_column(events, "event_name")

    test_data = [("test_event_1",), ("test_event_2",), ("unknown_event",)]
    df = spark.createDataFrame(test_data, ["event_name"])

    result_df = df.withColumn("event_id", col_expr)
    results = result_df.collect()

    assert results[0]["event_id"] == event1.get_id()
    assert results[1]["event_id"] == event2.get_id()
    assert results[2]["event_id"] is None


def test_get_event_id_column_with_aggregations(spark, basic_narrow_db):
    """Test get_event_id_column static method with list of aggregations."""

    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event1 = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    event2 = BasicEvent(name="test_event_2", expr=eng_rpm < 1000)

    hist1 = HistogramDuration(
        name="hist_1", base_expr=eng_rpm, bins=[0, 500, 1000, 1500], event=event1
    )
    hist2 = HistogramDuration(name="hist_2", base_expr=eng_rpm, bins=[0, 1000, 2000], event=event2)

    aggregations = [hist1, hist2]

    col_expr = ReportEntityUtil.get_event_id_column(aggregations, "hist_name")

    test_data = [("hist_1",), ("hist_2",), ("unknown_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("event_id", col_expr)
    results = result_df.collect()

    assert results[0]["event_id"] == hist1.get_event().get_id()
    assert results[1]["event_id"] == hist2.get_event().get_id()
    assert results[2]["event_id"] is None


def test_get_event_id_column_with_aggregations_no_event(spark, basic_narrow_db):
    """Test get_event_id_column static method with list of aggregations without events."""

    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")

    hist1 = HistogramDuration(name="hist_1", base_expr=eng_rpm, bins=[0, 500, 1000, 1500])
    hist2 = HistogramDuration(name="hist_2", base_expr=eng_rpm, bins=[0, 1000, 2000])

    aggregations = [hist1, hist2]

    col_expr = ReportEntityUtil.get_event_id_column(aggregations, "hist_name")

    test_data = [("hist_1",), ("hist_2",), ("unknown_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("event_id", col_expr)
    results = result_df.collect()

    assert results[0]["event_id"] is None
    assert results[1]["event_id"] is None
    assert results[2]["event_id"] is None


# --- generate_tag_filters tests ---


def test_generate_tag_filters_single(basic_narrow_db):
    """Test generate_tag_filters with a single tag filter."""
    query = basic_narrow_db.query
    groups = [[TagFilter(tag_name="uut_id", comparator=Comparator.EQ, value="AA080518")]]
    result = ReportEntityUtil.generate_tag_filters(query, groups)
    assert result is not None
    assert isinstance(result, TagOp)
    expected = TagOp(operator.eq, TagSelector("uut_id", cast_type="string"), "AA080518")
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_tag_filters_and_combined(basic_narrow_db):
    """Test generate_tag_filters with multiple AND-combined filters."""
    query = basic_narrow_db.query
    groups = [
        [
            TagFilter(tag_name="uut_id", comparator=Comparator.EQ, value="AA"),
            TagFilter(
                tag_name="container_id",
                comparator=Comparator.GE,
                value=100,
                cast_type=CastType.INT,
            ),
        ]
    ]
    result = ReportEntityUtil.generate_tag_filters(query, groups)
    assert result is not None

    name_cond = TagOp(operator.eq, TagSelector("uut_id", cast_type="string"), "AA")
    id_cond = TagOp(operator.ge, TagSelector("container_id", cast_type="int"), 100)
    expected = TagOp(operator.and_, name_cond, id_cond)
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_tag_filters_or_of_ands(basic_narrow_db):
    """Test generate_tag_filters with OR-of-ANDs structure."""
    query = basic_narrow_db.query
    groups = [
        [TagFilter(tag_name="uut_id", comparator=Comparator.EQ, value="AA")],
        [TagFilter(tag_name="uut_id", comparator=Comparator.EQ, value="BB")],
    ]
    result = ReportEntityUtil.generate_tag_filters(query, groups)
    assert result is not None

    cond_a = TagOp(operator.eq, TagSelector("uut_id", cast_type="string"), "AA")
    cond_b = TagOp(operator.eq, TagSelector("uut_id", cast_type="string"), "BB")
    expected = TagOp(operator.or_, cond_a, cond_b)
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_tag_filters_empty(basic_narrow_db):
    """Test generate_tag_filters returns None for empty groups."""
    result = ReportEntityUtil.generate_tag_filters(basic_narrow_db.query, [])
    assert result is None


def test_generate_tag_filters_all_comparators(basic_narrow_db):
    """Test generate_tag_filters with all comparator types."""
    query = basic_narrow_db.query
    for comp in Comparator:
        groups = [[TagFilter(tag_name="x", comparator=comp, value=1, cast_type=CastType.INT)]]
        result = ReportEntityUtil.generate_tag_filters(query, groups)
        assert result is not None


# --- generate_metric_filters tests ---


def test_generate_metric_filters_single(basic_narrow_db):
    """Test generate_metric_filters with a single metric filter."""
    query = basic_narrow_db.query
    groups = [[MetricFilter(column_name="uut_id", comparator=Comparator.EQ, value="AA")]]
    result = ReportEntityUtil.generate_metric_filters(query, groups)
    assert result is not None
    assert isinstance(result, MetricOp)
    expected = MetricOp(operator.eq, MetricSelector(key="uut_id"), "AA")
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_metric_filters_and_combined(basic_narrow_db):
    """Test generate_metric_filters with multiple AND-combined filters."""
    query = basic_narrow_db.query
    groups = [
        [
            MetricFilter(column_name="uut_id", comparator=Comparator.EQ, value="AA"),
            MetricFilter(
                column_name="start_ts",
                comparator=Comparator.GE,
                value="2025-01-01",
            ),
        ]
    ]
    result = ReportEntityUtil.generate_metric_filters(query, groups)
    assert result is not None

    name_cond = MetricOp(operator.eq, MetricSelector(key="uut_id"), "AA")
    ts_cond = MetricOp(operator.ge, MetricSelector(key="start_ts"), "2025-01-01")
    expected = MetricOp(operator.and_, name_cond, ts_cond)
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_metric_filters_or_of_ands(basic_narrow_db):
    """Test generate_metric_filters with OR-of-ANDs structure."""
    query = basic_narrow_db.query
    groups = [
        [MetricFilter(column_name="uut_id", comparator=Comparator.EQ, value="AA")],
        [MetricFilter(column_name="uut_id", comparator=Comparator.EQ, value="BB")],
    ]
    result = ReportEntityUtil.generate_metric_filters(query, groups)
    assert result is not None

    cond_a = MetricOp(operator.eq, MetricSelector(key="uut_id"), "AA")
    cond_b = MetricOp(operator.eq, MetricSelector(key="uut_id"), "BB")
    expected = MetricOp(operator.or_, cond_a, cond_b)
    assert str(result.get_selector_expr()) == str(expected.get_selector_expr())


def test_generate_metric_filters_empty(basic_narrow_db):
    """Test generate_metric_filters returns None for empty groups."""
    result = ReportEntityUtil.generate_metric_filters(basic_narrow_db.query, [])
    assert result is None


def test_generate_metric_filters_all_comparators(basic_narrow_db):
    """Test generate_metric_filters with all comparator types."""
    query = basic_narrow_db.query
    for comp in Comparator:
        groups = [[MetricFilter(column_name="x", comparator=comp, value=1)]]
        result = ReportEntityUtil.generate_metric_filters(query, groups)
        assert result is not None
