"""PerceptionDB — access layer for the perception tables.

Internal reader used by ``MeasurementDB`` when constructed with a
``perception_config=``. Authors normally do not instantiate ``PerceptionDB``
directly; they pass a ``PerceptionDBConfig`` to ``MeasurementDB`` and call
``db.object_tracks(spark)`` / ``db.perception_channels(spark)`` / etc.

Table → schema mapping:
  perception_channels               mda_query_engine/perception/schema/silver.py
  object_tracks                     mda_query_engine/perception/schema/scenario.py
  perception_event_instance_objects mda_query_engine/perception/schema/scenario.py
"""

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F


class PerceptionDBConfig:
    def __init__(
        self,
        perception_channels_table: str | None = None,
        object_tracks_table: str | None = None,
        perception_event_instance_objects_table: str | None = None,
        table_locations: str = "unity_catalog",
    ):
        self.perception_channels_table = perception_channels_table
        self.object_tracks_table = object_tracks_table
        self.perception_event_instance_objects_table = (
            perception_event_instance_objects_table
        )
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
            object_tracks_table=f"{p}.object_tracks",
            perception_event_instance_objects_table=(
                f"{p}.perception_event_instance_objects"
            ),
            table_locations="unity_catalog",
        )

    @staticmethod
    def for_debug(debug_tables: dict) -> "PerceptionDBConfig":
        known = {
            "perception_channels",
            "object_tracks",
            "perception_event_instance_objects",
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

    def perception_channels(self, spark) -> DataFrame:
        return self._read(spark, self.config.perception_channels_table)

    def object_tracks(self, spark) -> DataFrame:
        return self._read(spark, self.config.object_tracks_table)

    def perception_event_instance_objects(self, spark) -> DataFrame:
        return self._read(spark, self.config.perception_event_instance_objects_table)


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
