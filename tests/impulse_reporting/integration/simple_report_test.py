import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDistance,
    HistogramDuration,
)
from impulse_reporting.aggregations.histogram2d import (
    Histogram2DCustomWeights,
    Histogram2DDistance,
    Histogram2DDuration,
)
from impulse_reporting.config.config_parser import (
    Comparator,
    ContainerFilters,
    ImpulseConfig,
    MeasurementDimensions,
    MetricFilter,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig, TableConfig
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent


class ExtendedSource(Source):
    container_tags_table: str
    channel_tags_table: str


class ExtendedImpulseConfig(ImpulseConfig):
    source: ExtendedSource


def test_simple_report1(spark, setup_narrow_db):
    # Global report configuration

    extended_impulse_config = ExtendedImpulseConfig(
        source=ExtendedSource(
            container_tags_table="spark_catalog.silver_narrow_db.container_tags",
            channel_tags_table="spark_catalog.silver_narrow_db.channel_tags",
            container_metrics_table="spark_catalog.silver_narrow_db.container_metrics",
            channel_metrics_table="spark_catalog.silver_narrow_db.channel_metrics",
            channels_uri="spark_catalog.silver_narrow_db.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(column_name="container_id", comparator=Comparator.EQ, value=1),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2023-08-15T12:00:00.000Z",
                    ),
                    MetricFilter(
                        column_name="stop_dt",
                        comparator=Comparator.LE,
                        value="2023-08-15T13:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.DELTA_SOLVER),
        measurement_dimensions=[MeasurementDimensions.CONTAINER_ID],
    )

    my_report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(extended_impulse_config),
    )

    # Definition of relevant channels
    c1 = my_report.get_db().query.channel(seed="0")
    c2 = my_report.get_db().query.channel(seed="0")
    c3 = my_report.get_db().query.channel(seed="0")  # weights channel

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name, base_expr=c1, weights_expr=c3, bins=hist_custom_bins
    )
    my_first_page.add_aggregation(hist_custom)

    # Distance histogram
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name, base_expr=c1, weights_expr=c3, bins=hist_distance_bins
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist4)

    # # 2D histogram weighted by distance
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()
    visual_dfs = my_report.aggregation_dfs
    assert visual_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert visual_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert isinstance(my_report, Report)


def test_basic_narrow_report_no_uuts(spark):
    # Global report configuration
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
    )

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    c3 = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event = c2 > 1

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name, base_expr=c1, weights_expr=c3, bins=hist_custom_bins
    )
    my_first_page.add_aggregation(hist_custom)

    # TODO: activate this when diff function in SampleSeries implementation
    # # Distance histogram
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name, base_expr=c1, weights_expr=c3, bins=hist_distance_bins
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist4)

    # 2D histogram weighted by distance
    # TODO: Activate when diff function is merged
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    assert agg_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert agg_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert isinstance(my_report, Report)


def test_basic_narrow_report(spark):
    # Global report configuration
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    c3 = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event = c2 > 1

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name, base_expr=c1, weights_expr=c3, bins=hist_custom_bins
    )
    my_first_page.add_aggregation(hist_custom)

    # # Distance histogram
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name, base_expr=c1, weights_expr=c3, bins=hist_distance_bins
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist4)

    # 2D histogram weighted by distance
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    assert agg_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert agg_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert isinstance(my_report, Report)


def test_report_with_events(spark):
    # Global report configuration
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    c3 = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event = c2 > 1

    # Add event to report
    my_event = BasicEvent(name="veh_spd_event", expr=veh_spd_event, desc="Vehicle speed > 1 km/h")
    my_report.add_event(my_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, event=my_event, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, event=my_event, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram with event
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name,
        base_expr=c1,
        weights_expr=c3,
        bins=hist_custom_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist_custom)

    # # Distance histogram with event
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name,
        base_expr=c1,
        weights_expr=c3,
        bins=hist_distance_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights and event
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist4)

    # 2D histogram weighted by distance with event
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    agg_metadata_dfs = my_report.aggregation_metadata_dfs
    event_dfs = my_report.event_dfs
    event_metadata_dfs = my_report.event_metadata_dfs

    assert agg_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert agg_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0

    assert agg_metadata_dfs["HISTOGRAM"].count() > 0
    assert agg_metadata_dfs["HISTOGRAM2D"].count() > 0

    assert event_dfs["BASIC_EVENT"]["changed"].count() > 0
    assert event_metadata_dfs["BASIC_EVENT"].count() > 0


def test_persist_report(spark):
    # Global report configuration

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    c3 = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event = c2 > 1

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name, base_expr=c1, weights_expr=c3, bins=hist_custom_bins
    )
    my_first_page.add_aggregation(hist_custom)

    # TODO: activate this when diff function in SampleSeries implementation
    # # Distance histogram
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name, base_expr=c1, weights_expr=c3, bins=hist_distance_bins
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist4)

    # 2D histogram weighted by distance
    # TODO: Activate when diff function is merged
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()

    # Persist the report
    my_report.persist_results()

    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram2d_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_measurement_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram2d_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram_dimension")

    histogram_fact = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")
    histogram_2d_fact = spark.read.table("spark_catalog.gold.evaluation_histogram2d_fact")
    histogram_dimension = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")
    histogram2d_dimension = spark.read.table("spark_catalog.gold.evaluation_histogram2d_dimension")

    assert histogram_fact.filter(F.col("hist_value") > 0).count() > 0
    assert histogram_2d_fact.filter(F.col("hist_value") > 0).count() > 0
    assert spark.read.table("spark_catalog.gold.evaluation_measurement_dimension").count() > 0
    assert spark.read.table("spark_catalog.gold.evaluation_histogram2d_dimension").count() > 0

    # Verify each 1D histogram is persisted in dimension table
    assert histogram_dimension.filter(F.col("name") == "rpm_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "speed_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "custom_weights_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "distance_hist_p1").count() == 1

    # Verify each 2D histogram is persisted in dimension table
    assert histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_hist").count() == 1
    assert (
        histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_custom_weights_hist").count()
        == 1
    )
    assert histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_distance_hist").count() == 1


def test_persist_report_with_events(spark):
    """Persist report with BasicEvent and ContainerEvent; event tables must contain both."""
    # Global report configuration
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    c3 = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event = c2 > 1

    # Add BasicEvent and ContainerEvent to report
    my_event = BasicEvent(name="veh_spd_event", expr=veh_spd_event, desc="Vehicle speed > 1 km/h")
    my_report.add_event(my_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, event=my_event, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, event=my_event, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram with event
    hist_custom_name = "custom_weights_hist_p1"
    hist_custom_bins = [float(i) for i in range(0, 8000, 250)]
    hist_custom = HistogramCustomWeights(
        name=hist_custom_name,
        base_expr=c1,
        weights_expr=c3,
        bins=hist_custom_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist_custom)

    # TODO: activate this when diff function in SampleSeries implementation
    # # Distance histogram with event
    hist_distance_name = "distance_hist_p1"
    hist_distance_bins = [float(i) for i in range(0, 8000, 250)]
    hist_distance = HistogramDistance(
        name=hist_distance_name,
        base_expr=c1,
        weights_expr=c3,
        bins=hist_distance_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram
    hist2d_name = "rpm_vs_speed_hist"
    x_bins = [float(i) for i in range(0, 8000, 250)]
    y_bins = [float(i) for i in range(0, 300, 1)]
    hist3 = Histogram2DDuration(
        name=hist2d_name, x_expr=c1, y_expr=c2, x_bins=x_bins, y_bins=y_bins
    )
    my_first_page.add_aggregation(hist3)

    # 2D histogram with custom weights and event
    hist2d_custom_name = "rpm_vs_speed_custom_weights_hist"
    hist4 = Histogram2DCustomWeights(
        name=hist2d_custom_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist4)

    # 2D histogram weighted by distance with event
    # TODO: Activate when diff function is merged
    hist2d_distance_name = "rpm_vs_speed_distance_hist"
    hist5 = Histogram2DDistance(
        name=hist2d_distance_name,
        x_expr=c1,
        y_expr=c2,
        weights_expr=c3,
        x_bins=x_bins,
        y_bins=y_bins,
        event=my_event,
    )
    my_first_page.add_aggregation(hist5)

    # Determine content of all pages
    my_report.determine_report()

    agg_dfs = my_report.aggregation_dfs
    agg_metadata_dfs = my_report.aggregation_metadata_dfs
    event_dfs = my_report.event_dfs
    event_metadata_dfs = my_report.event_metadata_dfs

    assert agg_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert agg_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0

    assert agg_metadata_dfs["HISTOGRAM"].count() > 0
    assert agg_metadata_dfs["HISTOGRAM2D"].count() > 0

    assert event_dfs["BASIC_EVENT"]["changed"].count() > 0
    assert event_metadata_dfs["BASIC_EVENT"].count() > 0

    my_report.persist_results()

    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_instance_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_histogram2d_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_measurement_dimension")

    histogram_fact = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")
    histogram_2d_fact = spark.read.table("spark_catalog.gold.evaluation_histogram2d_fact")
    histogram_dimension = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")
    histogram2d_dimension = spark.read.table("spark_catalog.gold.evaluation_histogram2d_dimension")
    event_dimension = spark.read.table("spark_catalog.gold.evaluation_event_dimension")

    assert spark.read.table("spark_catalog.gold.evaluation_event_dimension").count() > 0
    assert spark.read.table("spark_catalog.gold.evaluation_event_instance_fact").count() > 0
    assert spark.read.table("spark_catalog.gold.evaluation_histogram_dimension").count() > 0
    assert histogram_fact.count() > 0
    assert histogram_2d_fact.filter(F.col("hist_value") > 0).count() > 0
    assert spark.read.table("spark_catalog.gold.evaluation_measurement_dimension").count() > 0

    # Verify each 1D histogram is persisted in dimension table
    assert histogram_dimension.filter(F.col("name") == "rpm_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "speed_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "custom_weights_hist_p1").count() == 1
    assert histogram_dimension.filter(F.col("name") == "distance_hist_p1").count() == 1

    # Verify each 2D histogram is persisted in dimension table
    assert histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_hist").count() == 1
    assert (
        histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_custom_weights_hist").count()
        == 1
    )
    assert histogram2d_dimension.filter(F.col("name") == "rpm_vs_speed_distance_hist").count() == 1

    # Verify event is persisted in dimension table
    assert event_dimension.filter(F.col("event_name") == "veh_spd_event").count() == 1


def test_simple_report_key_value_store(spark, key_value_store_db):
    """End-to-end report using KeyValueStoreSolver with EAV key-value-store data.

    Aligned with config_cs_bronze.json: uses entity_maps_to="container_id"
    and a single Vehicle Speed histogram (analogous to can_vehicle_speed).
    """

    # --- Global report configuration using KeyValueStoreSolver ---
    config = ImpulseConfig(
        source=Source(
            container_tags_table="spark_catalog.silver_key_value_store.container_tags",
            container_metrics_table="spark_catalog.silver_key_value_store.container_metrics",
            channel_metrics_table="spark_catalog.silver_key_value_store.channel_metrics",
            channels_uri="spark_catalog.silver_key_value_store.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
        query_engine=QueryEngine(
            solver=Solvers.KEY_VALUE_STORE_SOLVER,
            solver_config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
                container_metrics=TableConfig(
                    column_name_mapping={"project": "project_id"},
                ),
            ),
        ),
        measurement_dimensions=[MeasurementDimensions.CONTAINER_ID],
    )

    my_report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )

    # --- Definition of relevant channels (Vehicle Speed only) ---
    veh_spd = my_report.get_db().query.channel(channel_name="Vehicle Speed Sensor")
    veh_spd2 = my_report.get_db().query.channel(channel_name="Vehicle Speed Sensor")

    # --- Definition of 1st page ---
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Vehicle speed histogram (analogous to can_vehicle_speed)
    hist_name = "vehicle_speed_hist"
    hist_bins = [float(i) for i in range(0, 300, 10)]
    hist = HistogramDuration(
        name=hist_name,
        base_expr=veh_spd,
        bins=hist_bins,
        desc="Vehicle speed histogram",
        channel_name="Vehicle Speed Sensor",
    )
    my_first_page.add_aggregation(hist)

    # Vehicle speed 2D histogram
    hist2d_name = "vehicle_speed_2d_hist"
    hist2d_bins = [float(i) for i in range(0, 300, 10)]
    hist2d = Histogram2DDuration(
        name=hist2d_name,
        x_expr=veh_spd,
        y_expr=veh_spd2,
        x_bins=hist2d_bins,
        y_bins=hist2d_bins,
    )
    my_first_page.add_aggregation(hist2d)

    # --- Determine content of all pages ---
    my_report.determine_report()
    visual_dfs = my_report.aggregation_dfs
    assert visual_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert visual_dfs["HISTOGRAM2D"]["changed"].filter(F.col("hist_value") > 0).count() > 0
    assert isinstance(my_report, Report)
