import json
import zlib
from typing import Any
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_reporting.aggregations.aggregation_types import AggregationType
from impulse_reporting.channels.channel_types import ChannelType
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    Solvers,
    DataType,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report_utils import (
    build_metadata_dfs,
    cleanup_temp_tables,
    collect_solvable_expressions,
    dispatch_aggregations,
    dispatch_calculated_channels,
    dispatch_events,
    group_selectables_by_type,
    merge_changed_unchanged,
    persist_dimensions_full,
    persist_dimensions_incremental,
    persist_facts_full,
    persist_facts_incremental,
    solve_expressions_batched,
    split_by_hash_change,
)
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.event import Event
from impulse_reporting.events.event_types import EventType
from impulse_reporting.incremental.container_detector import ContainerUpsertDetector
from impulse_reporting.incremental.definition_hash_comparator import (
    DefinitionHashComparator,
)
from impulse_reporting.meta.container_dimensions import (
    ChannelMappingResolutionDimension,
    ContainerDimension,
)
from impulse_reporting.persist.report_storage import (
    ReportEntityTransformer,
    Sink,
    SinkConfig,
    UnityCatalogSink,
    UnitySinkConfig,
    WriterFactory,
)
from impulse_reporting.util.report_entity_util import ReportEntityUtil
from impulse_query_engine.telemetry import log_telemetry, telemetry_logger


class Report:
    """Represents a report containing pages, events, and configurations for data processing and persistence."""

    def __init__(
        self,
        name: str,
        spark: SparkSession,
        workspace_client: WorkspaceClient,
        config: dict[str, Any] | None = None,
        config_path: str | None = None,
    ):
        """
        Initialize the Report object.

        Parameters
        ----------
        name : str
            Name of the report.
        spark : SparkSession
            Spark session to be used for data processing.
        workspace_client : WorkspaceClient
            Authenticated Databricks workspace client used for telemetry attribution.
        config : Optional[dict[str, Any]], optional
            Dictionary containing configuration parameters.
        config_path : Optional[str], optional
            Path to the JSON configuration file.
        Raises
        ------
        ValueError
            If neither config nor config_path is provided.
        DatabricksError
            If the workspace is not reachable.
        """
        self.name = name
        self.report_id = self.get_id()
        self.spark = spark

        self.pages = []
        self.events = []
        self.calculated_channels = []

        self.event_dfs = {}
        self.event_metadata_dfs = {}
        self.aggregation_dfs = {}
        self.aggregation_metadata_dfs = {}
        self.calculated_channel_dfs = {}
        self.calculated_channel_metadata_dfs = {}
        self.container_dimension_df = None
        self.channel_mapping_resolution_dimension_df = None
        self._is_incremental = None
        self._changed_channel_ids = {}

        if config:
            self.config = Report.load_config_from_dict(config)
        elif config_path:
            self.config = Report.load_config_from_file(config_path)
        else:
            raise ValueError("Either config or config_path must be provided")

        self.db = Report.create_measurement_db(self.config, workspace_client)
        self.ws = self.db.ws

        self.query: QueryBuilder = Report.create_query_builder(self.db, self.config)
        self.sink: Sink | None = (
            Report.create_sink(self.config) if self.config.unity_sink else None
        )

        self.solver = Report.create_solver(self.spark, self.config)
        log_telemetry(self.ws, "solver", self.config.query_engine.solver.name)
        log_telemetry(self.ws, "data_type", self.config.query_engine.data_type.value)

    @property
    def _has_sink(self) -> bool:
        return self.sink is not None

    def get_id(self) -> int:
        """
        Returns a unique identifier for the report.

        Returns
        -------
        int
            Unique positive 32-bit integer identifier for the report.
        """
        return zlib.crc32(self.name.encode()) & 0x7FFFFFFF  # Ensures positive 32-bit int

    def get_db(self) -> MeasurementDB:
        """
        Get the measurement database associated with this report.

        Returns
        -------
        MeasurementDB
            The measurement database instance.
        """
        return self.db

    def get_solver(self) -> QuerySolver:
        """
        Get the query solver associated with this report.

        Returns
        -------
        QuerySolver
            The query solver instance.
        """
        return self.solver

    @staticmethod
    def load_config_from_file(config_path: str) -> ImpulseConfig:
        """
        Load Impulse configuration from a JSON file.

        Parameters
        ----------
        config_path : str
            Path to the JSON configuration file.
        Returns
        -------
        UnitySinkConfig
            The loaded Unity sink configuration.
        """
        with open(config_path) as f:
            data = json.load(f)
        return ImpulseConfig.model_validate(data)

    @staticmethod
    def load_config_from_dict(config_info: dict[str, Any]) -> ImpulseConfig:
        """
        Load Impulse configuration from a dictionary.

        Parameters
        ----------
        config_info : dict of str to Any
            Dictionary containing configuration parameters.

        Returns
        -------
        ImpulseConfig
            The loaded Impulse configuration.
        """
        return ImpulseConfig.model_validate(config_info)

    @staticmethod
    def create_measurement_db(config: ImpulseConfig, ws: WorkspaceClient) -> MeasurementDB:
        """
        Create a measurement database based on the provided configuration.

        Maps the optional ``container_tags`` field from the Source config
        to the ``container_tags_table`` parameter expected by
        ``MeasurementDBConfig``.

        Parameters
        ----------
        config : ImpulseConfig
            The Impulse configuration.
        ws : WorkspaceClient
            Authenticated Databricks workspace client.

        Returns
        -------
        MeasurementDB
            The measurement database instance.
        """
        source_dict = dict(config.source)
        # Map config field name to MeasurementDBConfig parameter name
        if "container_tags" in source_dict:
            source_dict["container_tags_table"] = source_dict.pop("container_tags")
        measurement_db_config = MeasurementDBConfig(**source_dict, table_locations="unity_catalog")
        return MeasurementDB(config=measurement_db_config, ws=ws)

    @staticmethod
    def create_query_builder(db: MeasurementDB, config: ImpulseConfig) -> QueryBuilder:
        """
        Create a query builder based on the provided configuration and set container filters.

        Validates that tag filters are only used when a
        ``container_tags_table`` is configured in ``source``.  DefaultSolver
        supports tag and metric filters, but tag filters require the narrow
        ``container_tags`` table to be available.

        Parameters
        ----------
        db : MeasurementDB
            The measurement database instance.
        config : ImpulseConfig
            The Impulse configuration.

        Returns
        -------
        QueryBuilder
            The query builder instance with applied filters.

        Raises
        ------
        ValueError
            If tag filters are configured but ``source.container_tags_table``
            is not set.
        """
        query = db.query

        if config.container_filters is not None:
            has_tag_filters = len(config.container_filters.tag_filters) > 0

            if has_tag_filters and config.source.container_tags_table is None:
                raise ValueError(
                    "Tag filters require a container_tags_table to be configured "
                    "in `source`. Provide source.container_tags_table or remove "
                    "the tag filters."
                )

            tag_filter_expr = ReportEntityUtil.generate_tag_filters(
                query, config.container_filters.tag_filters
            )
            metric_filter_expr = ReportEntityUtil.generate_metric_filters(
                query, config.container_filters.metric_filters
            )
            query.where(tag_filter_expr, metric_filter_expr)

        return query

    @staticmethod
    def create_sink(config: ImpulseConfig) -> Sink:
        """
        Create a sink based on the provided configuration.

        Parameters
        ----------
        config : ImpulseConfig
            The Impulse configuration.

        Returns
        -------
        Sink
            The sink instance for report persistence.
        """
        return UnityCatalogSink(
            config=UnitySinkConfig(
                catalog_name=config.unity_sink.catalog,
                schema_name=config.unity_sink.schema,
                table_prefix=config.unity_sink.table_prefix,
            )
        )

    @staticmethod
    def create_solver(spark: SparkSession, config: ImpulseConfig) -> QuerySolver:
        """
        Create a query solver based on the provided configuration.
        Parameters
        ----------
        spark : SparkSession
            The Spark session to use for the solver.
        config : ImpulseConfig
            The configuration

        Returns
        -------
        QuerySolver
            An instance of the appropriate query solver based on the configuration.

        Raises
        ------
        ValueError
            If the solver type is unknown.
        """
        # DELTA_SOLVER and KEY_VALUE_STORE_SOLVER are deprecated aliases retained
        # for backward compatibility with existing report configs; all three
        # resolve to the unified DefaultSolver.
        match config.query_engine.solver:
            case Solvers.DEFAULT_SOLVER | Solvers.DELTA_SOLVER | Solvers.KEY_VALUE_STORE_SOLVER:
                return DefaultSolver(
                    spark,
                    config=config.query_engine.solver_config,
                    is_raw_data=config.query_engine.data_type is DataType.RAW,
                    drop_implausible_data=config.query_engine.drop_implausible_data,
                    raw_encoder=config.query_engine.raw_encoder,
                )
            case _:
                raise ValueError(
                    f"Unknown query engine solver: {config.query_engine.solver}. "
                    f"Supported: {Solvers.DEFAULT_SOLVER} (DELTA_SOLVER and "
                    f"KEY_VALUE_STORE_SOLVER are deprecated aliases)."
                )

    def get_sink_config(self) -> SinkConfig:
        """
        Get the current sink configuration.

        Returns
        -------
        SinkConfig
           The sink configuration associated with this report.

        Raises
        ------
        ValueError
            If no sink is configured (sinkless mode).
        """
        if not self._has_sink:
            raise ValueError("No sink configured. Cannot retrieve sink config in sinkless mode.")
        return self.sink.config

    def add_page(self, page: Page):
        """
        Add a page to the report.

        Parameters
        ----------
        page : Page
            The page to add.

        Returns
        -------
        None
        """
        self.pages.append(page)
        page.set_report_id(self.report_id)

    def add_event(self, event: Event):
        """
        Add an event to the report.

        Parameters
        ----------
        event : Event
            The event to add.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the event is a ContainerEvent and a ContainerEvent already exists in the report.
        """
        if isinstance(event, ContainerEvent) and any(
            isinstance(e, ContainerEvent) for e in self.events
        ):
            raise ValueError(
                "Only one ContainerEvent is allowed per report. "
                "A ContainerEvent has already been added to this report."
            )
        self.events.append(event)
        event.set_report_id(self.report_id)

    def get_events(self) -> list[Event]:
        """
        Get the list of events associated with the report.

        Returns
        -------
        list of Event
            List of events.
        """
        return self.events

    def get_events_dict(self) -> dict:
        """
        Get a dictionary of events part of the report keyed by event name.

        Returns
        -------
        dict
            Dictionary mapping event names to Event objects.
        """
        return {event.get_name(): event for event in self.events}

    def _group_events_by_type(self):
        """
        Group events by their type.

        Returns
        -------
        dict
            Dictionary mapping event type names to lists of events.
        """
        return group_selectables_by_type(self.events, EventType)

    def add_calculated_channel(self, channel):
        """
        Add a calculated channel to the report.

        Parameters
        ----------
        channel : CalculatedChannel
            The calculated channel to add.

        Returns
        -------
        None
        """
        self.calculated_channels.append(channel)
        channel.set_report_id(self.report_id)

    def get_calculated_channels(self) -> list:
        """
        Get the list of calculated channels associated with the report.

        Returns
        -------
        list of CalculatedChannel
            List of calculated channels.
        """
        return self.calculated_channels

    def _validate_unique_calculated_channels(self):
        """
        Reject calculated channels that share a canonical identity.

        ``channel_id`` is derived from the identity, so two channels with the
        same identity would collide on the fact-table merge key
        ``(container_id, channel_id, tstart)`` and overwrite each other
        non-deterministically. Fail fast, naming the colliding identity and the
        offending channel names.

        Raises
        ------
        ValueError
            If any canonical identity appears on more than one registered
            calculated channel.
        """
        names_by_identity: dict[str, list[str]] = {}
        for channel in self.calculated_channels:
            names_by_identity.setdefault(channel.canonical_identity(), []).append(
                channel.get_name()
            )

        duplicates = {
            identity: names for identity, names in names_by_identity.items() if len(names) > 1
        }
        if duplicates:
            details = "; ".join(
                f"identity [{identity}] used by channels {names}"
                for identity, names in duplicates.items()
            )
            raise ValueError(
                "Calculated channels must have unique identities — each identity maps "
                f"to one channel_id and one set of fact rows. Duplicates found: {details}."
            )

    def _group_aggregations_by_type(self):
        """
        Group aggregations by their type.

        Returns
        -------
        dict
            Dictionary mapping aggregation type names to lists of aggregations.
        """
        aggregations = [agg for page in self.pages for agg in page.aggregations]
        return group_selectables_by_type(aggregations, AggregationType)

    def _validate_aggregation_events(self) -> None:
        """
        Validate that all events used in aggregations are added to the report.

        Raises
        ------
        ValueError
            If an aggregation uses an event that was not added to the report via add_event().
        """
        registered_events = set(self.events)
        registered_event_names = {event.get_name() for event in self.events}

        missing_events = []

        for page in self.pages:
            for aggregation in page.aggregations:
                event = aggregation.get_event()
                if event is not None and event not in registered_events:
                    event_name = event.get_name()
                    if event_name not in registered_event_names:
                        missing_events.append(
                            f"Aggregation '{aggregation.get_name()}' uses event "
                            f"'{event_name}' which was not added to the report."
                        )

        if missing_events:
            error_message = (
                "The following events are used in aggregations but were not added "
                "to the report via add_event():\n"
                + "\n".join(f"  - {msg}" for msg in missing_events)
            )
            raise ValueError(error_message)

    @telemetry_logger("report", "persist_results")
    def persist_results(self, cleanup_temp_tables: bool | None = None):
        """
        Persist report results using appropriate strategy based on definition changes.

        Uses tracked state from determine_report() to decide persistence strategy:
        - Changed definitions: replaceWhere (atomic delete + insert)
        - Unchanged definitions: MERGE (upsert)

        Parameters
        ----------
        cleanup_temp_tables : bool, optional
            Whether to drop the batch-solving ``__impulse_temp_*`` tables from the
            sink schema after persistence completes successfully.
            - True/False: use this value, overriding the config flag.
            - None (default): fall back to ``config.unity_sink.cleanup_temp_tables``
              (which itself defaults to False).

        Returns
        -------
        None
        """
        if not self._has_sink:
            return

        # Use tracked state from determine_report
        changed_aggregation_ids = getattr(self, "_changed_aggregation_ids", {})
        changed_event_ids = getattr(self, "_changed_event_ids", {})
        changed_channel_ids = getattr(self, "_changed_channel_ids", {})

        if self._is_incremental:
            self._persist_incremental(
                changed_aggregation_ids, changed_event_ids, changed_channel_ids
            )
        else:
            self._persist_full()

        # Only drop the current run's temp tables once persistence has succeeded.
        if self._resolve_cleanup_temp_tables(cleanup_temp_tables):
            self._cleanup_temp_tables()

    def _resolve_cleanup_temp_tables(self, cleanup: bool | None) -> bool:
        """Resolve whether to drop temp tables: explicit arg wins, else config flag.

        ``self.config.unity_sink`` is guaranteed non-None when called, since
        ``persist_results`` returns early unless a sink is configured.
        """
        if cleanup is not None:
            return cleanup
        return bool(self.config.unity_sink.cleanup_temp_tables)

    def _persist_full(self):
        """
        Persist results using full overwrite strategy.

        Returns
        -------
        None
        """
        storage_factory = WriterFactory(self.sink)

        # aggregation fact + dimension tables — grouped by output table so shared
        # tables (e.g. StatsAggregator + PointValueAggregator → stats_aggregator_fact)
        # are written together.
        persist_facts_full(self.aggregation_dfs, AggregationType, storage_factory)
        persist_dimensions_full(self.aggregation_metadata_dfs, AggregationType, storage_factory)

        # event fact + dimension tables — grouped by output table to handle mixed
        # event types sharing a table.
        persist_facts_full(self.event_dfs, EventType, storage_factory)
        persist_dimensions_full(self.event_metadata_dfs, EventType, storage_factory)

        # calculated channel fact + dimension tables
        persist_facts_full(self.calculated_channel_dfs, ChannelType, storage_factory)
        persist_dimensions_full(self.calculated_channel_metadata_dfs, ChannelType, storage_factory)

        # persist measurement dimensions
        if self.container_dimension_df:
            writer = storage_factory.create_container_dimension_writer()
            uri = writer.get_output_uri()
            writer.write(self.container_dimension_df, uri=uri)

        # persist channel mapping resolution dimension
        if self.channel_mapping_resolution_dimension_df is not None:
            writer = storage_factory.create_channel_mapping_resolution_dimension_writer()
            uri = writer.get_output_uri()
            writer.write(self.channel_mapping_resolution_dimension_df, uri=uri)

    @telemetry_logger("report", "determine_report")
    def _persist_incremental(
        self,
        changed_aggregation_ids: dict[str, list[int]],
        changed_event_ids: dict[str, list[int]],
        changed_channel_ids: dict[str, list[int]] = None,
    ):
        """
        Persist results using incremental strategy.

        Uses MERGE for unchanged definitions and replaceWhere for changed definitions.

        Parameters
        ----------
        changed_aggregation_ids : dict[str, list[int]]
            Mapping of aggregation type to list of visual_ids with changed definitions.
        changed_event_ids : dict[str, list[int]]
            Mapping of event type to list of event_ids with changed definitions.
        changed_channel_ids : dict[str, list[int]], optional
            Mapping of channel type to list of channel_ids with changed definitions.

        Returns
        -------
        None
        """
        changed_channel_ids = changed_channel_ids or {}
        has_processed_containers = getattr(self, "_has_processed_containers", False)
        updated_container_ids = getattr(self, "_updated_container_ids", [])
        storage_factory = WriterFactory(self.sink)
        transformer = ReportEntityTransformer()

        def _transform(df, schema):
            return self._transform_for_persistence(df, schema, transformer)

        # Persist aggregation facts + dimensions. StatsAggregator and
        # PointValueAggregator share stats_aggregator_fact, so facts group by
        # table; merge keys are per-type (via _get_aggregation_merge_keys).
        persist_facts_incremental(
            self.aggregation_dfs,
            AggregationType,
            self.sink,
            _transform,
            id_column="visual_id",
            merge_keys=self._get_aggregation_merge_keys,
            changed_ids=changed_aggregation_ids,
            has_processed_containers=has_processed_containers,
            updated_container_ids=updated_container_ids,
        )
        persist_dimensions_incremental(
            self.aggregation_metadata_dfs,
            AggregationType,
            self.sink,
            _transform,
            merge_keys=["visual_id"],
        )

        # Persist event facts + dimensions. Mixed event types share
        # event_instance_fact, so facts group by table and union changed defs
        # before a single replaceWhere.
        persist_facts_incremental(
            self.event_dfs,
            EventType,
            self.sink,
            _transform,
            id_column="event_id",
            merge_keys=["container_id", "event_id", "event_instance_id"],
            changed_ids=changed_event_ids,
            has_processed_containers=has_processed_containers,
            updated_container_ids=updated_container_ids,
        )
        persist_dimensions_incremental(
            self.event_metadata_dfs,
            EventType,
            self.sink,
            _transform,
            merge_keys=["event_id"],
        )

        # Persist calculated channel facts + dimensions
        persist_facts_incremental(
            self.calculated_channel_dfs,
            ChannelType,
            self.sink,
            _transform,
            id_column="channel_id",
            merge_keys=["container_id", "channel_id", "tstart"],
            changed_ids=changed_channel_ids,
            has_processed_containers=has_processed_containers,
            updated_container_ids=updated_container_ids,
        )
        persist_dimensions_incremental(
            self.calculated_channel_metadata_dfs,
            ChannelType,
            self.sink,
            _transform,
            merge_keys=["channel_id"],
        )

        # Persist the measurement dimension LAST (as ``_persist_full`` does). It
        # holds the gold timestamp that container-update detection compares
        # against; the fact solves above are lazy, so writing it earlier would
        # bump that timestamp before they materialize and drop the containers
        # being reprocessed.
        if self.container_dimension_df:
            writer = storage_factory.create_container_dimension_writer()
            uri = writer.get_output_uri()
            # Add meta information and upsert directly (no schema transform needed)
            df_enriched = self.container_dimension_df.transform(transformer.add_meta_information)
            self.sink.upsert(df_enriched, uri, ["container_id"])

        # Persist channel mapping resolution dimension
        # (upsert by container_id, channel_id, channel_alias)
        if self.channel_mapping_resolution_dimension_df is not None:
            writer = storage_factory.create_channel_mapping_resolution_dimension_writer()
            uri = writer.get_output_uri()
            df_enriched = self.channel_mapping_resolution_dimension_df.transform(
                transformer.add_meta_information
            )
            solver_cfg = self.solver.config
            self.sink.upsert(
                df_enriched,
                uri,
                [
                    solver_cfg.container_id_col,
                    solver_cfg.channel_id_col,
                    solver_cfg.channel_alias_col,
                ],
            )

    def _transform_for_persistence(
        self,
        df: DataFrame,
        schema: StructType,
        transformer: "ReportEntityTransformer",
    ) -> DataFrame:
        """
        Transform DataFrame for persistence by selecting columns and adding metadata.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame to transform.
        schema : StructType
            Schema defining columns to select.
        transformer : ReportEntityTransformer
            Transformer instance for data transformation.

        Returns
        -------
        DataFrame
            Transformed DataFrame ready for persistence.
        """

        return df.transform(transformer.select_relevant_columns(schema)).transform(
            transformer.add_meta_information
        )

    def _get_aggregation_merge_keys(self, agg_type: AggregationType) -> list[str]:
        """
        Get merge keys for the given aggregation type.

        Parameters
        ----------
        agg_type : AggregationType
            The aggregation type.

        Returns
        -------
        list[str]
            List of column names to use as merge keys.
        """
        merge_keys_map = {
            AggregationType.HISTOGRAM: ["container_id", "visual_id", "bin_ID"],
            AggregationType.HISTOGRAM2D: [
                "container_id",
                "visual_id",
                "x_bin_ID",
                "y_bin_ID",
            ],
            AggregationType.STATS_AGGREGATOR: [
                "container_id",
                "visual_id",
                "aggregation_label",
                "event_instance_id",
                "channel_name",
            ],
            AggregationType.POINT_VALUE_AGGREGATOR: [
                "container_id",
                "visual_id",
                "aggregation_label",
                "event_instance_id",
                "channel_name",
            ],
        }
        return merge_keys_map.get(agg_type, ["container_id", "visual_id"])

    def _cleanup_temp_tables(self) -> None:
        """Drop leftover ``__impulse_temp_*`` Delta tables from previous runs.

        Only applies when a sink is configured; in sinkless mode this is a no-op.
        """
        if not self._has_sink:
            return

        cleanup_temp_tables(
            self.spark,
            self.config.unity_sink.catalog,
            self.config.unity_sink.schema,
        )

    def _solve_expressions_batched(
        self,
        expressions: list[TimeSeriesExpression],
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame | None:
        """Solve all expressions in configurable batches and return a joined wide DataFrame.

        Delegates to :func:`solve_expressions_batched` in ``report_utils``.
        """
        return solve_expressions_batched(
            spark=self.spark,
            expressions=expressions,
            query=self.query,
            solver=self.solver,
            batch_size=self.config.query_engine.batch_size,
            has_sink=self._has_sink,
            catalog=getattr(self.config, "unity_sink", None) and self.config.unity_sink.catalog,
            schema=getattr(self.config, "unity_sink", None) and self.config.unity_sink.schema,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

    @telemetry_logger("report", "determine_report")
    def determine_report(self, is_incremental: bool = None):
        """
        Determine and process events, aggregations, and container dimensions for the report.
        Results are accessible in the report's attributes.

        Supports incremental processing with definition-hash-based optimization:
        - Changed definitions trigger full reprocessing (all containers)
        - Unchanged definitions use incremental processing (only new/updated containers)

        Parameters
        ----------
        is_incremental : bool, optional
            Hint for processing mode. Overwritten by config when incremental
            config is provided.
            - True: Request incremental processing (if gold layer exists)
            - False: Force full processing
            - None: Use config value (default: full processing)

        Returns
        -------
        None
        """
        # Validate that every aggregation references a registered event
        self._validate_aggregation_events()

        # TODO: port unit-consistency sanity check from MDA Framework
        # (`mda_reporting/util/unit_sanity_check.py`). When a
        # `unit_conversion_table` is configured, walk all aggregation /
        # event expressions and emit a UserWarning for each aliased
        # selector whose source_unit differs from target_unit so the
        # caller knows to express formula constants in target units.

        # Clean up temp tables from previous runs
        self._cleanup_temp_tables()

        # Determine processing mode: config overrides signature, gold must exist
        self._is_incremental = self._resolve_is_incremental(is_incremental)

        # Detect containers to process (incremental mode only): new + updated.
        pre_filtered_containers_df = None
        if self._is_incremental:
            pre_filtered_containers_df = self._detect_upserted_containers()

        # Two signals for persistence:
        # - has_processed_containers (new + updated): gates whether a fact table is
        #   written (new containers must be inserted). Only a bool is needed, so
        #   probe emptiness with isEmpty() rather than collecting the whole id list.
        # - updated container ids: scopes the delete-by-source, since only
        #   containers that already have gold rows can have stale rows to prune.
        self._has_processed_containers = (
            pre_filtered_containers_df is not None and not pre_filtered_containers_df.isEmpty()
        )
        self._updated_container_ids = self._collect_container_ids(
            self._detect_updated_containers() if self._is_incremental else None
        )

        hash_comparator = DefinitionHashComparator(self.spark)

        # Group events and aggregations by type
        events_by_type = self._group_events_by_type()
        aggs_by_type = self._group_aggregations_by_type()

        # Split changed/unchanged definitions
        changed_events_by_type, unchanged_events_by_type, self._changed_event_ids = (
            split_by_hash_change(
                events_by_type, EventType, self.sink, self.spark, hash_comparator, kind="event"
            )
        )
        changed_aggs_by_type, unchanged_aggs_by_type, self._changed_aggregation_ids = (
            split_by_hash_change(
                aggs_by_type,
                AggregationType,
                self.sink,
                self.spark,
                hash_comparator,
                kind="aggregation",
            )
        )

        # Collect all solvable expressions (exclude ContainerEvent)
        all_changed_expressions = collect_solvable_expressions(
            changed_events_by_type, EventType, exclude_cls=ContainerEvent
        ) + collect_solvable_expressions(changed_aggs_by_type, AggregationType)
        all_unchanged_expressions = collect_solvable_expressions(
            unchanged_events_by_type, EventType, exclude_cls=ContainerEvent
        ) + collect_solvable_expressions(unchanged_aggs_by_type, AggregationType)

        # Centralized solve
        changed_solved_df = self._solve_expressions_batched(
            all_changed_expressions, pre_filtered_containers_df=None
        )
        unchanged_solved_df = self._solve_expressions_batched(
            all_unchanged_expressions, pre_filtered_containers_df=pre_filtered_containers_df
        )

        # Dispatch events
        changed_event_dfs = dispatch_events(
            self.spark,
            changed_events_by_type,
            EventType,
            changed_solved_df,
            self.query,
            self.solver,
            None,
            ContainerEvent,
        )
        unchanged_event_dfs = dispatch_events(
            self.spark,
            unchanged_events_by_type,
            EventType,
            unchanged_solved_df,
            self.query,
            self.solver,
            pre_filtered_containers_df,
            ContainerEvent,
        )

        # Merge event results into {type: {"changed": df, "unchanged": df}} and
        # build event dimensions from all (changed + unchanged) events.
        self.event_dfs = merge_changed_unchanged(changed_event_dfs, unchanged_event_dfs)
        self.event_metadata_dfs = build_metadata_dfs(events_by_type, EventType, self.spark)

        # Dispatch aggregations
        changed_agg_dfs = dispatch_aggregations(
            self.spark,
            changed_aggs_by_type,
            AggregationType,
            changed_solved_df,
        )
        unchanged_agg_dfs = dispatch_aggregations(
            self.spark,
            unchanged_aggs_by_type,
            AggregationType,
            unchanged_solved_df,
        )

        # Merge aggregation results and build aggregation dimensions from all.
        self.aggregation_dfs = merge_changed_unchanged(changed_agg_dfs, unchanged_agg_dfs)
        self.aggregation_metadata_dfs = build_metadata_dfs(
            aggs_by_type, AggregationType, self.spark
        )

        # Calculated channels: own narrow solve (not the wide solved_df).
        # Changed definitions recompute over all containers; unchanged ones over
        # the incrementally-detected subset.
        self._validate_unique_calculated_channels()
        channels_by_type = group_selectables_by_type(self.calculated_channels, ChannelType)
        changed_channels_by_type, unchanged_channels_by_type, self._changed_channel_ids = (
            split_by_hash_change(
                channels_by_type,
                ChannelType,
                self.sink,
                self.spark,
                hash_comparator,
                kind="channel",
            )
        )
        changed_channel_dfs = dispatch_calculated_channels(
            self.spark,
            changed_channels_by_type,
            ChannelType,
            self.query,
            self.solver,
            None,
        )
        unchanged_channel_dfs = dispatch_calculated_channels(
            self.spark,
            unchanged_channels_by_type,
            ChannelType,
            self.query,
            self.solver,
            pre_filtered_containers_df,
        )
        self.calculated_channel_dfs = merge_changed_unchanged(
            changed_channel_dfs, unchanged_channel_dfs
        )
        self.calculated_channel_metadata_dfs = build_metadata_dfs(
            channels_by_type, ChannelType, self.spark
        )

        # Determine container dimension
        self.container_dimension_df = ContainerDimension.get_dimension(
            spark=self.spark,
            query=self.query,
            solver=self.solver,
            config=self.config,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

        # Determine channel mapping resolution dimension.
        # Mirror the fact split: aliases from changed definitions resolve
        # over all containers, aliases only in unchanged definitions stay
        # scoped to the incrementally-detected containers.
        changed_aliased_selectors = TimeSeriesExpression.collect_selectors(
            all_changed_expressions,
            uses_alias=True,
        )
        unchanged_aliased_selectors = TimeSeriesExpression.collect_selectors(
            all_unchanged_expressions,
            uses_alias=True,
        )
        self.channel_mapping_resolution_dimension_df = (
            ChannelMappingResolutionDimension.get_dimension_for_scopes(
                spark=self.spark,
                query=self.query,
                solver=self.solver,
                changed_aliased_selectors=changed_aliased_selectors,
                unchanged_aliased_selectors=unchanged_aliased_selectors,
                pre_filtered_containers_df=pre_filtered_containers_df,
            )
        )

    def _resolve_is_incremental(self, is_incremental: bool = None) -> bool:
        """
        Resolve the processing mode considering signature, config, and gold layer.

        Priority order:
        1. Gold layer must exist for any incremental processing — no gold → FULL
        2. Config overrides the ``is_incremental`` signature when present
        3. ``enabled=True`` takes precedence over ``processing_mode``
        4. Signature parameter (``is_incremental``) used when no config exists
        5. Default (no config, no signature): FULL processing

        Parameters
        ----------
        is_incremental : bool, optional
            Hint from the caller. Overridden by config when incremental
            config is provided.

        Returns
        -------
        bool
            True for incremental processing, False for full processing.
        """
        # Rule 1: No gold layer → always FULL (nothing to compare against)
        if not self._gold_layer_exists():
            return False

        if not hasattr(self, "config") and is_incremental is not None:
            return is_incremental

        # Rule 2 & 3: Config overrides signature when provided
        has_incremental_config = (
            hasattr(self.config, "incremental") and self.config.incremental is not None
        )

        if has_incremental_config:
            # enabled=True → incremental, enabled=False → FULL (processing_mode is not checked)
            return bool(self.config.incremental.enabled)

        # No config: use signature parameter
        # Rule 4: is_incremental=True → incremental (gold exists)
        # Rule 5: is_incremental=None or False → FULL
        if is_incremental is None:
            return False

        return is_incremental

    def _gold_layer_exists(self) -> bool:
        """
        Check whether the gold layer measurement dimension table exists.

        Used by AUTO processing mode to decide between incremental and full
        processing on the first vs. subsequent runs.

        Returns
        -------
        bool
            True if the gold measurement dimension table exists.
        """
        if not self._has_sink:
            return False
        measurement_dim_table = self.sink.config.get_output_uri_measurement_dimensions_table()
        return self.spark.catalog.tableExists(measurement_dim_table)

    def _detect_upserted_containers(self) -> DataFrame | None:
        """
        Detect new and updated containers for incremental processing.

        Uses ``silver_last_modified_column`` and ``gold_last_modified_column``
        from the incremental config to parameterize the timestamp columns
        used for freshness comparison.  Falls back to ``"last_modified"``
        when no incremental config is present.

        Returns None if gold layer doesn't exist (triggers full processing)
        or if no sink is configured (sinkless mode).

        Returns
        -------
        DataFrame | None
            DataFrame containing containers to process, or None if gold table
            doesn't exist (indicating full processing is needed).
        """
        args = self._container_detection_args()
        if args is None:
            return None
        detector, silver_containers, measurement_dim_table, silver_col, gold_col = args
        return detector.detect_upserted_containers(
            silver_containers,
            measurement_dim_table,
            silver_last_modified_col=silver_col,
            gold_last_modified_col=gold_col,
        )

    def _detect_updated_containers(self) -> DataFrame | None:
        """Detect only UPDATED containers (present in gold, newer silver timestamp).

        Excludes new containers — see
        ``ContainerUpsertDetector.detect_updated_containers``. Used to scope the
        incremental delete-by-source. Returns None in sinkless mode or when the
        gold table doesn't exist.

        Returns
        -------
        DataFrame | None
            Updated containers, or None.
        """
        args = self._container_detection_args()
        if args is None:
            return None
        detector, silver_containers, measurement_dim_table, silver_col, gold_col = args
        return detector.detect_updated_containers(
            silver_containers,
            measurement_dim_table,
            silver_last_modified_col=silver_col,
            gold_last_modified_col=gold_col,
        )

    def _container_detection_args(self):
        """Shared inputs for container detection, or None in sinkless mode.

        Returns
        -------
        tuple | None
            ``(detector, silver_containers_df, measurement_dim_table, silver_col,
            gold_col)`` — the freshness column names come from the incremental
            config (default ``"last_modified"``). None when no sink is configured.
        """
        if not self._has_sink:
            return None
        detector = ContainerUpsertDetector(self.spark)
        silver_containers = self.db.container_metrics(self.spark)
        measurement_dim_table = self.sink.config.get_output_uri_measurement_dimensions_table()

        silver_col = "last_modified"
        gold_col = "last_modified"
        if hasattr(self.config, "incremental") and self.config.incremental is not None:
            silver_col = self.config.incremental.silver_last_modified_column
            gold_col = self.config.incremental.gold_last_modified_column

        return detector, silver_containers, measurement_dim_table, silver_col, gold_col

    @staticmethod
    def _collect_container_ids(containers_df: DataFrame | None) -> list:
        """Collect ``container_id`` values from a detected-containers DataFrame.

        Parameters
        ----------
        containers_df : DataFrame | None
            A detected-containers DataFrame (silver schema, includes
            ``container_id``), or None.

        Returns
        -------
        list
            Container id values (empty when None).
        """
        if containers_df is None:
            return []
        return [row["container_id"] for row in containers_df.select("container_id").collect()]
