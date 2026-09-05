import warnings

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

from impulse_query_engine import __version__
from impulse_query_engine.telemetry import verify_workspace_client
from .analyze.query.query_builder import QueryBuilder


class MeasurementDBConfig:
    def __init__(
        self,
        container_tags_table=None,
        container_metrics_table=None,
        channel_tags_table=None,
        channel_metrics_table=None,
        channels_uri=None,
        poi_channels_uri=None,
        channel_mapping_table=None,
        unit_conversion_table=None,
        table_locations: str = "external_locations",
    ):
        self.container_tags_table = container_tags_table
        self.container_metrics_table = container_metrics_table
        self.channel_tags_table = channel_tags_table
        self.channel_metrics_table = channel_metrics_table
        self.channels_uri = channels_uri
        # Optional Points-in-Time (POI) channel-data table. ``None`` means no POI
        # channels are configured, so POI-unaware deployments are unchanged.
        self.poi_channels_uri = poi_channels_uri
        self.channel_mapping_table = channel_mapping_table
        self.unit_conversion_table = unit_conversion_table
        self.table_locations = table_locations
        self.debug_tables = None
        # URI -> pinned Delta version, populated once per run by
        # ``MeasurementDB.pin_versions``. Empty means "read latest" (unchanged
        # behavior). Never populated in debug mode.
        self.pinned_versions: dict[str, int] = {}

    @staticmethod
    def for_unity_catalog(
        catalog_name: str,
        core_schema_name: str = "core",
        channel_mapping_table: str | None = None,
        unit_conversion_table: str | None = None,
        poi_channels_uri: str | None = None,
    ):
        return MeasurementDBConfig(
            container_tags_table=f"{catalog_name}.{core_schema_name}.container_tags",
            container_metrics_table=f"{catalog_name}.{core_schema_name}.container_metrics",
            channel_tags_table=f"{catalog_name}.{core_schema_name}.channel_tags",
            channel_metrics_table=f"{catalog_name}.{core_schema_name}.channel_metrics",
            channels_uri=f"{catalog_name}.{core_schema_name}.channels",
            poi_channels_uri=poi_channels_uri,
            channel_mapping_table=channel_mapping_table,
            unit_conversion_table=unit_conversion_table,
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
            poi_channels_uri="poi_channels" if "poi_channels" in debug_tables else None,
            channel_mapping_table=(
                "channel_mapping" if "channel_mapping" in debug_tables else None
            ),
            unit_conversion_table=(
                "unit_conversion" if "unit_conversion" in debug_tables else None
            ),
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

    def _configured_table_uris(self) -> list[str]:
        """Return the URIs of every configured (non-``None``) silver table."""
        return [
            uri
            for uri in (
                self.config.container_tags_table,
                self.config.container_metrics_table,
                self.config.channel_tags_table,
                self.config.channel_metrics_table,
                self.config.channels_uri,
                self.config.poi_channels_uri,
                self.config.channel_mapping_table,
                self.config.unit_conversion_table,
            )
            if uri is not None
        ]

    @staticmethod
    def _current_delta_version(spark, table_locations: str, uri: str) -> int:
        """Resolve the latest committed Delta version of ``uri``.

        Handles UC (``catalog.schema.table``, resolved by ``forName`` with the
        full name) and path modes. Read-only: if the reference cannot be
        resolved (view, non-Delta, unknown table), the error propagates to
        :meth:`pin_versions`, which skips pinning that table so it keeps reading
        the latest version — never a wrong one.
        """
        from delta.tables import DeltaTable

        if table_locations == "unity_catalog":
            dt = DeltaTable.forName(spark, uri)
        else:  # path mode
            dt = DeltaTable.forPath(spark, uri)
        return int(dt.history(1).select("version").first()[0])

    def pin_versions(self, spark) -> None:
        """Pin every configured silver table to its current Delta version.

        Resolves each table's latest version once so that all lazily-evaluated
        reads in a run observe the same snapshot regardless of when they
        materialize (issue #87). Debug mode is exempt. Tables that cannot be
        time-traveled (views, non-Delta, unresolvable) are skipped with a
        warning and continue to read the latest version.
        """
        if self.config.table_locations == "debug":
            return
        pinned: dict[str, int] = {}
        for uri in self._configured_table_uris():
            try:
                pinned[uri] = self._current_delta_version(spark, self.config.table_locations, uri)
            except Exception as exc:  # noqa: BLE001 - graceful degradation per table
                warnings.warn(f"Could not pin Delta version for '{uri}': {exc}", stacklevel=2)
        self.config.pinned_versions = pinned

    def _read_table(self, spark, table_name):
        if self.config.table_locations == "debug":
            return self.config.debug_tables[table_name]
        reader = spark.read
        version = self.config.pinned_versions.get(table_name)
        if version is not None:
            reader = reader.option("versionAsOf", version)
        if self.config.table_locations == "unity_catalog":
            return reader.table(table_name)
        return reader.format("delta").load(table_name)

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

    def has_poi_channels(self) -> bool:
        """Whether a Points-in-Time (POI) channel-data table is configured."""
        return getattr(self.config, "poi_channels_uri", None) is not None

    def poi_channels(self, spark) -> DataFrame:
        """Read the Points-in-Time (POI) channel-data table.

        Parallel to :meth:`channels`. Raises if no ``poi_channels_uri`` is
        configured — callers should gate on :meth:`has_poi_channels` first.
        """
        if not self.has_poi_channels():
            raise ValueError("poi_channels_uri is not configured")
        return self._read_table(spark, self.config.poi_channels_uri)

    def channel_mapping(self, spark) -> DataFrame:
        if self.config.channel_mapping_table is None:
            raise ValueError("channel_mapping_table is not configured")
        return self._read_table(spark, self.config.channel_mapping_table)

    def unit_conversion(self, spark) -> DataFrame:
        if self.config.unit_conversion_table is None:
            raise ValueError("unit_conversion_table is not configured")
        return self._read_table(spark, self.config.unit_conversion_table)

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
