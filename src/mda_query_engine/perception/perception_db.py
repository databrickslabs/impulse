"""PerceptionDB — access layer for the perception tables.

Internal reader used by ``MeasurementDB`` when constructed with a
``perception_config=``. Authors normally do not instantiate ``PerceptionDB``
directly; they pass a ``PerceptionDBConfig`` to ``MeasurementDB`` and call
``db.object_tracks(spark)`` / ``db.frame_embeddings(spark)`` / etc.

Table → schema mapping:
  perception_channels      mda_query_engine/perception/schema/silver.py
  channel_value_labels     mda_query_engine/schema.py (graduated under ADR-P14 Phase A)
  object_tracks            mda_query_engine/perception/schema/scenario.py
  frame_embeddings         mda_query_engine/perception/schema/scenario.py
  playlist_items           mda_reporting/persist/fact_schema.py (graduated under ADR-P14 Phase B)
  camera_object_detections mda_query_engine/perception/schema/annotation.py — populate only when a downstream consumer exists
  lidar_object_detections  annotation.py
  radar_object_detections  annotation.py
  lane_markings            annotation.py
  free_space               annotation.py
  predicted_trajectories   annotation.py
"""

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F


class PerceptionDBConfig:
    def __init__(
        self,
        perception_channels_table: str | None = None,
        channel_value_labels_table: str | None = None,
        object_tracks_table: str | None = None,
        frame_embeddings_table: str | None = None,
        perception_event_instance_objects_table: str | None = None,
        playlist_items_table: str | None = None,
        camera_object_detections_table: str | None = None,
        lidar_object_detections_table: str | None = None,
        radar_object_detections_table: str | None = None,
        lane_markings_table: str | None = None,
        free_space_table: str | None = None,
        predicted_trajectories_table: str | None = None,
        table_locations: str = "unity_catalog",
    ):
        self.perception_channels_table = perception_channels_table
        self.channel_value_labels_table = channel_value_labels_table
        self.object_tracks_table = object_tracks_table
        self.frame_embeddings_table = frame_embeddings_table
        self.perception_event_instance_objects_table = (
            perception_event_instance_objects_table
        )
        self.playlist_items_table = playlist_items_table
        self.camera_object_detections_table = camera_object_detections_table
        self.lidar_object_detections_table = lidar_object_detections_table
        self.radar_object_detections_table = radar_object_detections_table
        self.lane_markings_table = lane_markings_table
        self.free_space_table = free_space_table
        self.predicted_trajectories_table = predicted_trajectories_table
        self.table_locations = table_locations
        self._debug_tables: dict | None = None

    @staticmethod
    def for_unity_catalog(
        catalog_name: str,
        silver_schema: str = "silver",
        perception_schema: str = "perception_silver",
    ) -> "PerceptionDBConfig":
        s = f"{catalog_name}.{silver_schema}"
        p = f"{catalog_name}.{perception_schema}"
        return PerceptionDBConfig(
            perception_channels_table=f"{s}.perception_channels",
            channel_value_labels_table=f"{s}.channel_value_labels",
            object_tracks_table=f"{p}.object_tracks",
            frame_embeddings_table=f"{p}.frame_embeddings",
            perception_event_instance_objects_table=(
                f"{p}.perception_event_instance_objects"
            ),
            playlist_items_table=f"{p}.playlist_items",
            camera_object_detections_table=f"{p}.camera_object_detections",
            lidar_object_detections_table=f"{p}.lidar_object_detections",
            radar_object_detections_table=f"{p}.radar_object_detections",
            lane_markings_table=f"{p}.lane_markings",
            free_space_table=f"{p}.free_space",
            predicted_trajectories_table=f"{p}.predicted_trajectories",
            table_locations="unity_catalog",
        )

    @staticmethod
    def for_debug(debug_tables: dict) -> "PerceptionDBConfig":
        known = {
            "perception_channels",
            "channel_value_labels",
            "object_tracks",
            "frame_embeddings",
            "perception_event_instance_objects",
            "playlist_items",
            "camera_object_detections",
            "lidar_object_detections",
            "radar_object_detections",
            "lane_markings",
            "free_space",
            "predicted_trajectories",
        }
        cfg = PerceptionDBConfig(
            **{f"{k}_table": k if k in debug_tables else None for k in known},
            table_locations="debug",
        )
        cfg._debug_tables = debug_tables
        return cfg


class PerceptionDB:
    def __init__(self, config: PerceptionDBConfig):
        self.config = config

    def _read(self, spark, table_name: str | None) -> DataFrame:
        if table_name is None:
            raise ValueError("Table is not configured in PerceptionDBConfig")
        if self.config.table_locations == "unity_catalog":
            return spark.read.table(table_name)
        if self.config.table_locations == "debug":
            return self.config._debug_tables[table_name]
        return spark.read.format("delta").load(table_name)

    # ── Silver ──────────────────────────────────────────────────────────────

    def perception_channels(self, spark) -> DataFrame:
        return self._read(spark, self.config.perception_channels_table)

    def channel_value_labels(self, spark) -> DataFrame:
        return self._read(spark, self.config.channel_value_labels_table)

    # ── Scenario layer ───────────────────────────────────────────────────────

    def object_tracks(self, spark) -> DataFrame:
        return self._read(spark, self.config.object_tracks_table)

    def frame_embeddings(self, spark) -> DataFrame:
        return self._read(spark, self.config.frame_embeddings_table)

    def perception_event_instance_objects(self, spark) -> DataFrame:
        return self._read(spark, self.config.perception_event_instance_objects_table)

    def playlist_items(self, spark) -> DataFrame:
        return self._read(spark, self.config.playlist_items_table)

    # ── Annotation tables (populate only when a downstream consumer exists) ─

    def camera_object_detections(self, spark) -> DataFrame:
        return self._read(spark, self.config.camera_object_detections_table)

    def lidar_object_detections(self, spark) -> DataFrame:
        return self._read(spark, self.config.lidar_object_detections_table)

    def radar_object_detections(self, spark) -> DataFrame:
        return self._read(spark, self.config.radar_object_detections_table)

    def lane_markings(self, spark) -> DataFrame:
        return self._read(spark, self.config.lane_markings_table)

    def free_space(self, spark) -> DataFrame:
        return self._read(spark, self.config.free_space_table)

    def predicted_trajectories(self, spark) -> DataFrame:
        return self._read(spark, self.config.predicted_trajectories_table)


# ── Standalone helpers ──────────────────────────────────────────────────────


def frame_nearest_to(
    spark: SparkSession,
    perception_channels_table: str,
    container_id: int,
    channel_id: int,
    target_ts: int,
    *,
    df=None,
) -> Row:
    """Return the `perception_channels` row whose `timestamp` is closest to
    `target_ts` for the given `(container_id, channel_id)`.

    Used to pick the camera frame or LiDAR scan nearest to an event's midpoint
    when rendering a visualization.

    Pass ``df=`` to supply a pre-read DataFrame instead of reading the table
    from the catalog (useful in tests and notebook exploration).
    """
    source = df if df is not None else spark.read.table(perception_channels_table)
    return (
        source
        .filter((F.col("container_id") == container_id) & (F.col("channel_id") == channel_id))
        .withColumn("dt", F.abs(F.col("timestamp") - target_ts))
        .orderBy("dt")
        .first()
    )
