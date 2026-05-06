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
            container_tags_table="container_tags",
            container_metrics_table="container_metrics",
            channel_tags_table="channel_tags",
            channel_metrics_table="channel_metrics",
            channels_uri="channels",
            table_locations="debug",
        )
        cfg.debug_tables = debug_tables
        return cfg


class MeasurementDB:
    def __init__(self, config: MeasurementDBConfig, ws: WorkspaceClient):
        self.config = config
        self.ws = verify_workspace_client(ws, "databricks-impulse", __version__)

    @property
    def query(self):
        return QueryBuilder(db=self)

    def _read_table(self, spark, table_name):
        # if not DeltaTable.isDeltaTable(spark, table_name):
        #    raise Exception(f"Table not found: `{table_name}`")
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
