"""Integration test for the channel_mapping_resolution_dimension gold table.

Exercises the end-to-end flow: a Report configured with
``channel_mapping_table``, aggregations that use ``channel_with_alias``,
``determine_report()`` and ``persist_results()`` writing the new
``channel_mapping_resolution_dimension`` Delta table to the gold schema.
"""

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
    # + meta columns from ContainerDimensionWriter / persist pipeline.
    assert set(gold.columns) == {
        "container_id",
        "channel_id",
        "channel_name",
        "data_key",
        "channel_alias",
        "priority",
        "config_hash",
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

    # config_hash is deterministic per-config and applied uniformly to every
    # row in the dimension df via ContainerDimension._add_config_hash.
    config_hashes = {r.config_hash for r in rows}
    assert len(config_hashes) == 1
    assert next(iter(config_hashes)) is not None

    # _created_at is stamped once via F.current_timestamp() inside
    # ReportEntityTransformer.add_meta_information, so all rows share it.
    created_ats = {r._created_at for r in rows}
    assert len(created_ats) == 1
    assert next(iter(created_ats)) is not None
