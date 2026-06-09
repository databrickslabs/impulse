"""Integration test for the channel_mapping_resolution_dimension gold table.

Exercises the end-to-end flow: a Report configured with
``channel_mapping_table``, aggregations that use ``channel_with_alias``,
``determine_report()`` and ``persist_results()`` writing the new
``channel_mapping_resolution_dimension`` Delta table to the gold schema.
"""

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from tests.conftest import spark  # noqa: F401  pytest fixture
from tests.impulse_reporting.integration.test_helpers import (
    add_histograms_aggregations,
    create_alias_report,
)


def test_alias_report_writes_channel_mapping_resolution_dimension(
    spark, setup_key_value_store_alias_db
):
    report, channels = create_alias_report(spark, table_prefix="alias_int")
    add_histograms_aggregations(
        report,
        engine_rpm=channels["engine_speed"],
        vehicle_speed=channels["vehicle_speed"],
        weights=channels["weights"],
    )

    report.determine_report()
    report.persist_results()

    gold = spark.read.table("spark_catalog.gold.alias_int_channel_mapping_resolution_dimension")

    # Schema: exact column set as written by ChannelMappingResolutionDimension
    # + the _created_at meta column from the writer.
    assert set(gold.columns) == {
        "container_id",
        "channel_id",
        "channel_name",
        "data_key",
        "channel_alias",
        "priority",
        "_created_at",
    }

    rows = gold.collect()

    # Six resolutions: 3 containers x 2 aliases. Containers 1 and 2 carry
    # the (Engine RPM/TM) + (Vehicle Speed Sensor/TM) physical channels;
    # container 3 carries (EngSpd/ProjSpecREC_10Hz) + (Spd_Vhcl/ProjSpecREC_10Hz).
    resolutions = {
        (r.container_id, r.channel_id, r.channel_name, r.data_key, r.channel_alias) for r in rows
    }
    assert resolutions == {
        (1, 5, "Engine RPM", "TM", "engine_speed"),
        (1, 7, "Vehicle Speed Sensor", "TM", "vehicle_speed"),
        (2, 5, "Engine RPM", "TM", "engine_speed"),
        (2, 7, "Vehicle Speed Sensor", "TM", "vehicle_speed"),
        (3, 5, "EngSpd", "ProjSpecREC_10Hz", "engine_speed"),
        (3, 7, "Spd_Vhcl", "ProjSpecREC_10Hz", "vehicle_speed"),
    }

    # The dimension contract dedupes by (container_id, channel_alias);
    # each alias resolves to exactly one physical channel per container.
    assert len(rows) == len({(r.container_id, r.channel_alias) for r in rows}) == 6

    # The alias CSV leaves `priority` empty (NULL) for every mapping row;
    # those NULLs propagate verbatim through the resolution.
    assert all(r.priority is None for r in rows)

    # _created_at is stamped once via F.current_timestamp() inside
    # ReportEntityTransformer.add_meta_information, so all rows share it.
    created_ats = {r._created_at for r in rows}
    assert len(created_ats) == 1
    assert next(iter(created_ats)) is not None


def test_incremental_added_alias_resolves_over_all_containers(
    spark, setup_key_value_store_alias_db
):
    """A new alias introduced on an incremental run is a *changed* definition
    and must be resolved across ALL containers — not only the incrementally
    detected ones — mirroring how the fact tables are reprocessed.

    Scenario: run 1 (full) registers only ``engine_speed``. Run 2 runs
    incrementally with silver unchanged (so no containers are upserted) and
    adds a ``vehicle_speed`` aggregation. The new alias must still appear for
    every container in the resolution dimension.
    """
    prefix = "alias_inc"
    table = f"spark_catalog.gold.{prefix}_channel_mapping_resolution_dimension"
    rpm_bins = [float(i) for i in range(0, 8000, 250)]
    speed_bins = [float(i) for i in range(0, 300, 1)]

    # Start from a clean gold slate so run 1 is genuinely a first/full run
    # (the gold warehouse can persist across local test runs).
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold")
    for t in spark.sql("SHOW TABLES IN spark_catalog.gold").collect():
        if t.tableName.startswith(prefix):
            spark.sql(f"DROP TABLE IF EXISTS spark_catalog.gold.{t.tableName} PURGE")

    # --- Run 1 (gold absent -> full): only the engine_speed alias. ---
    report1, channels1 = create_alias_report(spark, table_prefix=prefix, incremental=True)
    page1 = Page(page_number=1)
    page1.add_aggregation(
        HistogramDuration(name="rpm_hist", base_expr=channels1["engine_speed"], bins=rpm_bins)
    )
    report1.add_page(page1)
    report1.determine_report()
    report1.persist_results()

    after_run1 = {(r.container_id, r.channel_alias) for r in spark.read.table(table).collect()}
    assert after_run1 == {
        (1, "engine_speed"),
        (2, "engine_speed"),
        (3, "engine_speed"),
    }

    # --- Run 2 (incremental, silver unchanged -> no containers upserted):
    # keep engine_speed (unchanged) and ADD vehicle_speed (changed/new). ---
    report2, channels2 = create_alias_report(spark, table_prefix=prefix, incremental=True)
    page2 = Page(page_number=1)
    page2.add_aggregation(
        HistogramDuration(name="rpm_hist", base_expr=channels2["engine_speed"], bins=rpm_bins)
    )
    page2.add_aggregation(
        HistogramDuration(name="speed_hist", base_expr=channels2["vehicle_speed"], bins=speed_bins)
    )
    report2.add_page(page2)
    report2.determine_report()
    report2.persist_results()

    gold = spark.read.table(table)
    after_run2 = {(r.container_id, r.channel_alias) for r in gold.collect()}

    # The newly-added vehicle_speed alias is resolved for ALL containers,
    # even though no containers were incrementally upserted; the engine_speed
    # rows written by run 1 are preserved by the upsert.
    assert after_run2 == {
        (1, "engine_speed"),
        (2, "engine_speed"),
        (3, "engine_speed"),
        (1, "vehicle_speed"),
        (2, "vehicle_speed"),
        (3, "vehicle_speed"),
    }

    # The MERGE saw no duplicate source rows for its key.
    assert (
        gold.count()
        == gold.dropDuplicates(["container_id", "channel_id", "channel_alias"]).count()
    )
