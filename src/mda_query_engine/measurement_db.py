from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

from mda_query_engine import __version__
from mda_query_engine.telemetry import verify_workspace_client
from .analyze.query.query_builder import QueryBuilder


class MeasurementDBConfig:
    def __init__(
        self,
        container_tags_table=None,
        container_metrics_table=None,
        channel_tags_table=None,
        channel_metrics_table=None,
        channels_uri=None,
        channel_mapping_table=None,
        table_locations: str = "external_locations",
    ):
        self.container_tags_table = container_tags_table
        self.container_metrics_table = container_metrics_table
        self.channel_tags_table = channel_tags_table
        self.channel_metrics_table = channel_metrics_table
        self.channels_uri = channels_uri
        self.channel_mapping_table = channel_mapping_table
        self.table_locations = table_locations
        self.debug_tables = None

    @staticmethod
    def for_unity_catalog(
        catalog_name: str,
        core_schema_name: str = "core",
        channel_mapping_table: str | None = None,
    ):
        return MeasurementDBConfig(
            container_tags_table=f"{catalog_name}.{core_schema_name}.container_tags",
            container_metrics_table=f"{catalog_name}.{core_schema_name}.container_metrics",
            channel_tags_table=f"{catalog_name}.{core_schema_name}.channel_tags",
            channel_metrics_table=f"{catalog_name}.{core_schema_name}.channel_metrics",
            channels_uri=f"{catalog_name}.{core_schema_name}.channels",
            channel_mapping_table=channel_mapping_table,
            table_locations="unity_catalog",
        )

    @staticmethod
    def for_debug(debug_tables):
        cfg = MeasurementDBConfig(
            container_tags_table="container_tags" if "container_tags" in debug_tables else None,
            container_metrics_table=(
                "container_metrics" if "container_metrics" in debug_tables else None
            ),
            channel_tags_table="channel_tags" if "channel_tags" in debug_tables else None,
            channel_metrics_table=(
                "channel_metrics" if "channel_metrics" in debug_tables else None
            ),
            channels_uri="channels" if "channels" in debug_tables else None,
            channel_mapping_table=(
                "channel_mapping" if "channel_mapping" in debug_tables else None
            ),
            table_locations="debug",
        )
        cfg.debug_tables = debug_tables
        return cfg


class MeasurementDB:
    def __init__(
        self,
        config: MeasurementDBConfig,
        ws: WorkspaceClient,
        perception_config: "PerceptionDBConfig | None" = None,
        event_instance_fact_table: str | None = None,
    ):
        self.config = config
        self.ws = verify_workspace_client(ws, "databricks-impulse", __version__)
        self._perception_config = perception_config
        if perception_config is not None:
            from mda_query_engine.perception.perception_db import PerceptionDB

            self._perception_db = PerceptionDB(perception_config)
        else:
            self._perception_db = None
        self._event_instance_fact_table = event_instance_fact_table

    @property
    def query(self):
        return QueryBuilder(db=self)

    @property
    def has_perception(self) -> bool:
        return self._perception_db is not None

    def _read_table(self, spark, table_name):
        if self.config.table_locations == "unity_catalog":
            return spark.read.table(table_name)
        elif self.config.table_locations == "debug":
            return self.config.debug_tables[table_name]
        return spark.read.format("delta").load(table_name)

    def container_tags(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.container_tags_table)

    def container_metrics(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.container_metrics_table)

    def channel_tags(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channel_tags_table)

    def channel_metrics(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channel_metrics_table)

    def channels(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channels_uri)

    def channel_mapping(self, spark) -> DataFrame:
        if self.config.channel_mapping_table is None:
            raise ValueError("channel_mapping_table is not configured")
        return self._read_table(spark, self.config.channel_mapping_table)

    def channel_uri(self):
        return self.config.channels_uri

    # ── Perception surface (gated on perception_config) ─────────────────────

    def _require_perception(self) -> "PerceptionDB":
        if self._perception_db is None:
            from mda_query_engine.perception.exceptions import PerceptionNotConfigured

            raise PerceptionNotConfigured(
                "MeasurementDB was constructed without perception_config; "
                "pass PerceptionDBConfig.for_unity_catalog(...) or .for_debug(...) "
                "to enable perception surfaces."
            )
        return self._perception_db

    def object_tracks(self, spark) -> DataFrame:
        return self._require_perception().object_tracks(spark)

    def frame_embeddings(self, spark) -> DataFrame:
        return self._require_perception().frame_embeddings(spark)

    def perception_channels(self, spark) -> DataFrame:
        return self._require_perception().perception_channels(spark)

    def channel_value_labels(self, spark) -> DataFrame:
        return self._require_perception().channel_value_labels(spark)

    def playlist_items(self, spark) -> DataFrame:
        return self._require_perception().playlist_items(spark)

    def perception_event_instance_objects(self, spark) -> DataFrame:
        return self._require_perception().perception_event_instance_objects(spark)

    def validate_sequence_of_events(self, sequence) -> None:
        """Validate a ``SequenceOfEvents`` for the per-object identity
        invariant before it is registered on a ``Report``.

        Mixed ``track_scope`` across the sequence's perception steps is
        ambiguous (some steps would form per-object windows, others per
        container) so this raises early instead of producing surprising
        results at solve time. Channel-only sequences and consistent
        sequences pass through unchanged.
        """
        self._require_perception()
        from mda_query_engine.perception.events.sequence_validation import (
            validate_sequence_of_events_track_scope,
        )

        validate_sequence_of_events_track_scope(sequence)

    def event_instance_fact(self, spark) -> DataFrame:
        """Core ``event_instance_fact`` LEFT JOIN'd to the perception side-car.

        ``object_id`` is populated for rows produced by track-scoped
        ``PerceptionEvent`` windows and NULL for every other row, including
        scalar ``BasicEvent`` rows and non-track-scoped perception rows.
        """
        self._require_perception()
        if self._event_instance_fact_table is None:
            raise ValueError(
                "MeasurementDB.event_instance_fact requires "
                "event_instance_fact_table to be configured."
            )
        if self.config.table_locations == "debug":
            fact_df = self.config.debug_tables[self._event_instance_fact_table]
        elif self.config.table_locations == "unity_catalog":
            fact_df = spark.read.table(self._event_instance_fact_table)
        else:
            fact_df = spark.read.format("delta").load(self._event_instance_fact_table)
        side_car_df = self.perception_event_instance_objects(spark)
        return fact_df.join(
            side_car_df,
            on=["container_id", "event_id", "event_instance_id"],
            how="left",
        )


class InMemoryMeasurementDB(MeasurementDB):
    @property
    def query(self):
        pass

    def add(self, ts, container_tags, measurement_tags):
        pass

    def container_tags(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.container_tags_table)

    def container_metrics(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.container_metrics_table)

    def channel_tags(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channel_tags_table)

    def channel_metrics(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channel_metrics_table)

    def channels(self, spark) -> DataFrame:
        return self._read_table(spark, self.config.channels_uri)

    def channel_uri(self):
        return self.config.channels_uri
