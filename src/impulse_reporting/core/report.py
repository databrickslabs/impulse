import json
import zlib
from functools import reduce
from typing import Any
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_reporting.aggregations.aggregation_types import AggregationType
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    Solvers,
    DataType,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report_utils import (
    cleanup_temp_tables,
    collect_solvable_expressions,
    dispatch_aggregations,
    dispatch_events,
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
from impulse_reporting.meta.container_dimensions import ContainerDimension
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

        self.event_dfs = {}
        self.event_metadata_dfs = {}
        self.aggregation_dfs = {}
        self.aggregation_metadata_dfs = {}
        self.container_dimension_df = None
        self._is_incremental = None

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
        ``container_tags_table`` is configured in ``source``.  Both
        KeyValueStoreSolver and DeltaSolver support tag and metric filters,
        but tag filters require the narrow ``container_tags`` table to be
        available.

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
        match config.query_engine.solver:
            case Solvers.DELTA_SOLVER:
                return DeltaSolver(
                    spark,
                    config=config.query_engine.solver_config,
                    is_raw_data=config.query_engine.data_type is DataType.RAW,
                    drop_implausible_data=config.query_engine.drop_implausible_data,
                )
            case Solvers.KEY_VALUE_STORE_SOLVER:
                return KeyValueStoreSolver(
                    spark,
                    config=config.query_engine.solver_config,
                    is_raw_data=config.query_engine.data_type is DataType.RAW,
                    drop_implausible_data=config.query_engine.drop_implausible_data,
                )
            case _:
                raise ValueError(
                    f"Unknown query engine, we currently only support "
                    f"{Solvers.DELTA_SOLVER}, {Solvers.KEY_VALUE_STORE_SOLVER}"
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
        event_types = {event_type.name: [] for event_type in EventType}
        for event in self.events:
            for event_type in event_types.keys():
                if isinstance(event, EventType[event_type].value):
                    event_types[event_type].append(event)
                    break
        return event_types

    def _group_aggregations_by_type(self):
        """
        Group aggregations by their type.

        Returns
        -------
        dict
            Dictionary mapping aggregation type names to lists of aggregations.
        """
        agg_types = {agg_type.name: [] for agg_type in AggregationType}
        for page in self.pages:
            for aggregation in page.aggregations:
                for agg_type in agg_types.keys():
                    if isinstance(aggregation, AggregationType[agg_type].value):
                        agg_types[agg_type].append(aggregation)
                        break
        return agg_types

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
    def persist_results(self):
        """
        Persist report results using appropriate strategy based on definition changes.

        Uses tracked state from determine_report() to decide persistence strategy:
        - Changed definitions: replaceWhere (atomic delete + insert)
        - Unchanged definitions: MERGE (upsert)

        Returns
        -------
        None
        """
        if not self._has_sink:
            return

        # Use tracked state from determine_report
        changed_aggregation_ids = getattr(self, "_changed_aggregation_ids", {})
        changed_event_ids = getattr(self, "_changed_event_ids", {})

        if self._is_incremental:
            self._persist_incremental(changed_aggregation_ids, changed_event_ids)
        else:
            self._persist_full()

    def _persist_full(self):
        """
        Persist results using full overwrite strategy.

        Returns
        -------
        None
        """
        storage_factory = WriterFactory(self.sink)

        # aggregation fact tables
        for aggregation_type_str, aggregation_dfs in self.aggregation_dfs.items():
            aggregation_type = AggregationType[aggregation_type_str]
            writer = storage_factory.create_writer(aggregation_type)
            schema, uri = writer.extract_fact_schema_and_output_uri(aggregation_type)

            # Handle both dict format (from incremental mode) and DataFrame format
            if isinstance(aggregation_dfs, dict):
                dfs_to_combine = []
                if aggregation_dfs.get("changed") is not None:
                    dfs_to_combine.append(aggregation_dfs["changed"])
                if aggregation_dfs.get("unchanged") is not None:
                    dfs_to_combine.append(aggregation_dfs["unchanged"])
                if dfs_to_combine:
                    writer.write(dfs_to_combine, schema=schema, uri=uri)
            else:
                writer.write(aggregation_dfs, schema=schema, uri=uri)

        # aggregation dimension tables
        for (
            aggregation_type_str,
            aggregation_metadata_dfs,
        ) in self.aggregation_metadata_dfs.items():
            aggregation_type = AggregationType[aggregation_type_str]
            writer = storage_factory.create_writer(aggregation_type)
            schema, uri = writer.extract_metadata_schema_and_output_uri(aggregation_type)
            writer.write(aggregation_metadata_dfs, schema=schema, uri=uri)

        # event fact tables — group by output table to handle mixed event types
        event_fact_by_table = {}
        for event_type_str, event_dfs in self.event_dfs.items():
            table_name = EventType[event_type_str].get_fact_table_name()
            event_fact_by_table.setdefault(table_name, [])

            if isinstance(event_dfs, dict):
                if event_dfs.get("changed") is not None:
                    event_fact_by_table[table_name].append(event_dfs["changed"])
                if event_dfs.get("unchanged") is not None:
                    event_fact_by_table[table_name].append(event_dfs["unchanged"])
            else:
                event_fact_by_table[table_name].append(event_dfs)

        for table_name, event_dfs_list in event_fact_by_table.items():
            if not event_dfs_list:
                continue
            event_type = EventType.get_any_for_fact_table(table_name)
            writer = storage_factory.create_writer(event_type)
            schema, uri = writer.extract_fact_schema_and_output_uri(event_type)
            writer.write(event_dfs_list, schema=schema, uri=uri)

        # event dimension tables — group by output table to handle mixed event types
        event_dim_by_table = {}
        for event_type_str, event_metadata_dfs in self.event_metadata_dfs.items():
            table_name = EventType[event_type_str].get_dimension_table_name()
            event_dim_by_table.setdefault(table_name, [])
            event_dim_by_table[table_name].append(event_metadata_dfs)

        for table_name, event_meta_dfs_list in event_dim_by_table.items():
            if not event_meta_dfs_list:
                continue
            event_type = EventType.get_any_for_dimension_table(table_name)
            writer = storage_factory.create_writer(event_type)
            schema, uri = writer.extract_metadata_schema_and_output_uri(event_type)
            writer.write(event_meta_dfs_list, schema=schema, uri=uri)

        # persist measurement dimensions
        if self.container_dimension_df:
            writer = storage_factory.create_container_dimension_writer()
            uri = writer.get_output_uri()
            writer.write(self.container_dimension_df, uri=uri)

    @telemetry_logger("report", "determine_report")
    def _persist_incremental(
        self,
        changed_aggregation_ids: dict[str, list[int]],
        changed_event_ids: dict[str, list[int]],
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

        Returns
        -------
        None
        """
        storage_factory = WriterFactory(self.sink)
        transformer = ReportEntityTransformer()

        # Persist aggregation facts
        for aggregation_type_str, agg_data in self.aggregation_dfs.items():
            aggregation_type = AggregationType[aggregation_type_str]
            writer = storage_factory.create_writer(aggregation_type)
            schema, uri = writer.extract_fact_schema_and_output_uri(aggregation_type)
            merge_keys = self._get_aggregation_merge_keys(aggregation_type)

            if isinstance(agg_data, dict):
                # Structured format: {'changed': df, 'unchanged': df}
                changed_df = agg_data.get("changed")
                unchanged_df = agg_data.get("unchanged")

                # Changed definitions: replaceWhere (atomic)
                if changed_df is not None and aggregation_type_str in changed_aggregation_ids:
                    changed_ids = changed_aggregation_ids[aggregation_type_str]
                    # Transform and enrich the DataFrame before persisting
                    df_enriched = self._transform_for_persistence(changed_df, schema, transformer)
                    self.sink.replace_by_ids(
                        df=df_enriched,
                        uri=uri,
                        id_column="visual_id",
                        ids_to_replace=changed_ids,
                    )

                # Unchanged definitions: MERGE
                if unchanged_df is not None:
                    df_enriched = self._transform_for_persistence(
                        unchanged_df, schema, transformer
                    )
                    self.sink.upsert(df_enriched, uri, merge_keys)
            else:
                # Backward compatibility: single DataFrame - use MERGE
                df_enriched = self._transform_for_persistence(agg_data, schema, transformer)
                self.sink.upsert(df_enriched, uri, merge_keys)

        # Persist aggregation dimensions (always upsert by visual_id)
        for (
            aggregation_type_str,
            aggregation_metadata_df,
        ) in self.aggregation_metadata_dfs.items():
            aggregation_type = AggregationType[aggregation_type_str]
            writer = storage_factory.create_writer(aggregation_type)
            schema, uri = writer.extract_metadata_schema_and_output_uri(aggregation_type)
            df_enriched = self._transform_for_persistence(
                aggregation_metadata_df, schema, transformer
            )
            self.sink.upsert(df_enriched, uri, ["visual_id"])

        # Persist event facts — group by output table to handle mixed event types
        event_fact_changed_by_table: dict[str, list] = {}
        event_fact_unchanged_by_table: dict[str, list] = {}
        event_changed_ids_by_table: dict[str, list[int]] = {}
        for event_type_str, event_data in self.event_dfs.items():
            table_name = EventType[event_type_str].get_fact_table_name()

            if isinstance(event_data, dict):
                changed_df = event_data.get("changed")
                unchanged_df = event_data.get("unchanged")

                if changed_df is not None and event_type_str in changed_event_ids:
                    event_fact_changed_by_table.setdefault(table_name, []).append(changed_df)
                    event_changed_ids_by_table.setdefault(table_name, []).extend(
                        changed_event_ids[event_type_str]
                    )

                if unchanged_df is not None:
                    event_fact_unchanged_by_table.setdefault(table_name, []).append(unchanged_df)
            else:
                event_fact_unchanged_by_table.setdefault(table_name, []).append(event_data)

        for table_name in set(
            list(event_fact_changed_by_table.keys()) + list(event_fact_unchanged_by_table.keys())
        ):
            event_type = EventType.get_any_for_fact_table(table_name)
            writer = storage_factory.create_writer(event_type)
            schema, uri = writer.extract_fact_schema_and_output_uri(event_type)
            merge_keys = ["container_id", "event_id", "event_instance_id"]

            # Changed definitions: replaceWhere (atomic)
            changed_dfs = event_fact_changed_by_table.get(table_name, [])
            changed_ids = event_changed_ids_by_table.get(table_name, [])
            if changed_dfs and changed_ids:
                transformed = [
                    self._transform_for_persistence(cdf, schema, transformer)
                    for cdf in changed_dfs
                ]
                combined_df = reduce(lambda a, b: a.unionByName(b), transformed)
                self.sink.replace_by_ids(
                    df=combined_df,
                    uri=uri,
                    id_column="event_id",
                    ids_to_replace=changed_ids,
                )

            # Unchanged definitions: MERGE
            unchanged_dfs = event_fact_unchanged_by_table.get(table_name, [])
            for udf in unchanged_dfs:
                df_enriched = self._transform_for_persistence(udf, schema, transformer)
                self.sink.upsert(df_enriched, uri, merge_keys)

        # Persist event dimensions — group by output table to handle mixed event types
        event_dim_by_table: dict[str, list] = {}
        for event_type_str, event_metadata_df in self.event_metadata_dfs.items():
            table_name = EventType[event_type_str].get_dimension_table_name()
            event_dim_by_table.setdefault(table_name, []).append(event_metadata_df)

        for table_name, event_meta_dfs_list in event_dim_by_table.items():
            event_type = EventType.get_any_for_dimension_table(table_name)
            writer = storage_factory.create_writer(event_type)
            schema, uri = writer.extract_metadata_schema_and_output_uri(event_type)
            for mdf in event_meta_dfs_list:
                df_enriched = self._transform_for_persistence(mdf, schema, transformer)
                self.sink.upsert(df_enriched, uri, ["event_id"])

        # Persist measurement dimension (upsert by container_id)
        if self.container_dimension_df:
            writer = storage_factory.create_container_dimension_writer()
            uri = writer.get_output_uri()
            # Add meta information and upsert directly (no schema transform needed)
            df_enriched = self.container_dimension_df.transform(transformer.add_meta_information)
            self.sink.upsert(df_enriched, uri, ["container_id"])

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

        # Detect containers to process (incremental mode only)
        pre_filtered_containers_df = None
        if self._is_incremental:
            pre_filtered_containers_df = self._detect_upserted_containers()

        hash_comparator = DefinitionHashComparator(self.spark)

        # Group events and aggregations by type
        events_by_type = self._group_events_by_type()
        aggs_by_type = self._group_aggregations_by_type()

        # Split changed/unchanged definitions
        changed_events_by_type, unchanged_events_by_type, self._changed_event_ids = (
            split_by_hash_change(
                events_by_type, EventType, self.sink, self.spark, hash_comparator, is_event=True
            )
        )
        changed_aggs_by_type, unchanged_aggs_by_type, self._changed_aggregation_ids = (
            split_by_hash_change(
                aggs_by_type,
                AggregationType,
                self.sink,
                self.spark,
                hash_comparator,
                is_event=False,
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

        # Merge event results into {type: {"changed": df, "unchanged": df}}
        event_dfs = {}
        all_event_types = set(list(changed_event_dfs.keys()) + list(unchanged_event_dfs.keys()))
        for t in all_event_types:
            event_dfs[t] = {
                "changed": changed_event_dfs.get(t),
                "unchanged": unchanged_event_dfs.get(t),
            }

        # Metadata: merge from all events (changed + unchanged)
        event_metadata_dfs = {}
        for event_name, events_list in events_by_type.items():
            if not events_list:
                continue
            cls = EventType[event_name].value
            event_metadata_dfs[event_name] = cls.determine_metadata_df(self.spark, events_list)

        self.event_dfs = event_dfs
        self.event_metadata_dfs = event_metadata_dfs

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

        # Merge aggregation results
        aggregation_dfs = {}
        all_agg_types = set(list(changed_agg_dfs.keys()) + list(unchanged_agg_dfs.keys()))
        for t in all_agg_types:
            aggregation_dfs[t] = {
                "changed": changed_agg_dfs.get(t),
                "unchanged": unchanged_agg_dfs.get(t),
            }

        # Metadata: merge from all aggregations
        aggregation_metadata_dfs = {}
        for agg_name, agg_list in aggs_by_type.items():
            if not agg_list:
                continue
            cls = AggregationType[agg_name].value
            aggregation_metadata_dfs[agg_name] = cls.determine_metadata_df(self.spark, agg_list)

        self.aggregation_dfs = aggregation_dfs
        self.aggregation_metadata_dfs = aggregation_metadata_dfs

        # Determine container dimension
        self.container_dimension_df = ContainerDimension.get_dimension(
            spark=self.spark,
            query=self.query,
            solver=self.solver,
            config=self.config,
            pre_filtered_containers_df=pre_filtered_containers_df,
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
        if not self._has_sink:
            return None
        detector = ContainerUpsertDetector(self.spark)
        silver_containers = self.db.container_metrics(self.spark)
        measurement_dim_table = self.sink.config.get_output_uri_measurement_dimensions_table()

        # Retrieve configurable column names (default: "last_modified")
        silver_col = "last_modified"
        gold_col = "last_modified"
        if hasattr(self.config, "incremental") and self.config.incremental is not None:
            silver_col = self.config.incremental.silver_last_modified_column
            gold_col = self.config.incremental.gold_last_modified_column

        return detector.detect_upserted_containers(
            silver_containers,
            measurement_dim_table,
            silver_last_modified_col=silver_col,
            gold_last_modified_col=gold_col,
        )
