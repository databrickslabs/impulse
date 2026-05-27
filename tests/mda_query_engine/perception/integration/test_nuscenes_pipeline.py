"""Pipeline integration tests — synthetic nuScenes data through the BYOD adapter.

Covers notebook 01 invariants (from tests/mda_query_engine/perception/integration/README.md):
  Step 2: container_tags / container_metrics / channel_tags populated, ≥1 container_id
  Step 3: channels populated; channel_metrics row count == distinct (container_id, channel_id)
  Step 4: perception_channels populated; all file_paths are UC Volume paths
  Step 5: object_tracks populated with ≥1 distinct detection_class

Invariants 5–9 (notebooks 02–04: TSAL events, lidar/camera detections, OpenLABEL, KPIs)
require the full event-detection pipeline and are covered by /deploy-e2e, not this tier.
"""


def _t(spark, ctx, table_name):
    return spark.read.table(f"{ctx['catalog']}.{ctx['schema']}.{table_name}")


class TestMetadataIngest:
    """Notebook 01, Step 2 — container_tags, container_metrics, channel_tags."""

    def test_container_tags_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "container_tags").count() > 0

    def test_container_metrics_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "container_metrics").count() > 0

    def test_channel_tags_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "channel_tags").count() > 0

    def test_at_least_one_distinct_container_id(self, spark, synthetic_nuscenes_bronze):
        df = _t(spark, synthetic_nuscenes_bronze, "container_tags")
        assert df.select("container_id").distinct().count() >= 1


class TestScalarChannels:
    """Notebook 01, Step 3 — channels and channel_metrics."""

    def test_channels_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "channels").count() > 0

    def test_channel_metrics_row_count_equals_distinct_channel_pairs(
        self, spark, synthetic_nuscenes_bronze
    ):
        channels = _t(spark, synthetic_nuscenes_bronze, "channels")
        metrics = _t(spark, synthetic_nuscenes_bronze, "channel_metrics")
        distinct_pairs = channels.select("container_id", "channel_id").distinct().count()
        assert metrics.count() == distinct_pairs


class TestPerceptionChannels:
    """Notebook 01, Step 4 — perception_channels."""

    def test_perception_channels_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "perception_channels").count() > 0

    def test_no_null_file_path(self, spark, synthetic_nuscenes_bronze):
        from pyspark.sql.functions import col

        df = _t(spark, synthetic_nuscenes_bronze, "perception_channels")
        assert df.filter(col("file_path").isNull()).count() == 0

    def test_all_file_paths_are_uc_volume_paths(self, spark, synthetic_nuscenes_bronze):
        rows = (
            _t(spark, synthetic_nuscenes_bronze, "perception_channels")
            .select("file_path")
            .collect()
        )
        assert len(rows) > 0
        bad = [r["file_path"] for r in rows if not r["file_path"].startswith("/Volumes/")]
        assert not bad, f"non-Volume paths in perception_channels: {bad}"


class TestObjectTracks:
    """Notebook 01, Step 5 — object_tracks."""

    def test_object_tracks_non_empty(self, spark, synthetic_nuscenes_bronze):
        assert _t(spark, synthetic_nuscenes_bronze, "object_tracks").count() > 0

    def test_at_least_one_distinct_detection_class(self, spark, synthetic_nuscenes_bronze):
        df = _t(spark, synthetic_nuscenes_bronze, "object_tracks")
        assert df.select("detection_class").distinct().count() >= 1
