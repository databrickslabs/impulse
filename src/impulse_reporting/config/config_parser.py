import re
from datetime import datetime
from enum import Enum, StrEnum
from typing import Annotated

from pydantic import AfterValidator, BaseModel, field_validator, model_validator

from impulse_query_engine.analyze.query.solvers.registry import resolve_registration
from impulse_query_engine.analyze.query.solvers.solver_config import RawEncoder, SolverConfig
from impulse_reporting.channels.calculated_channel_kpis import DEFAULT_KPIS, KPI_BUILDERS


def is_valid_table_name(table_name: str) -> str:
    """
    Validate if a string is a valid Unity Catalog table name.

    Parameters
    ----------
    table_name : str
        The table name to validate. Should be in format 'catalog.schema.table'.

    Returns
    -------
    str
        The validated table name if valid.

    Raises
    ------
    ValueError
        If the table name does not match the required format or contains invalid characters.

    Notes
    -----
    Unity Catalog table names must:
    - Follow the format 'catalog.schema.table'
    - Each part can contain letters, numbers, hyphens and underscores
    - Each part cannot be empty
    """
    regex_valid_table_name = r"^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$"
    if re.fullmatch(regex_valid_table_name, table_name) is not None:
        return table_name
    else:
        raise ValueError(
            f"Invalid table name: {table_name}. Table names must be in the format 'catalog.schema.table'."
        )


def is_valid_unity_entity_name(entity_name: str) -> str:
    """
    Validate if a string is a valid Unity Catalog entity name.

    Parameters
    ----------
    entity_name : str
        The entity name to validate (catalog, schema, or table prefix).

    Returns
    -------
    str
        The validated entity name if valid.

    Raises
    ------
    ValueError
        If the entity name contains invalid characters.

    Notes
    -----
    Unity Catalog entity names must contain only letters, numbers, hyphens, and underscores.
    """
    regex_valid_entity_name = r"^[a-zA-Z0-9_-]+"
    if re.fullmatch(regex_valid_entity_name, entity_name) is not None:
        return entity_name
    else:
        raise ValueError(
            f"Invalid entity name: {entity_name}. Entity names must contain only letters, "
            f"numbers, hyphens, and underscores."
        )


DEFAULT_MEASUREMENT_DIMENSIONS = ["container_id", "start_ts", "stop_ts"]


class DataType(StrEnum):
    RAW = "RAW"
    RLE = "RLE"


class Solvers(StrEnum):
    """
    Names of the built-in solver types for the query engine.

    ``DEFAULT_SOLVER`` is the single, unified solver. ``DELTA_SOLVER`` and
    ``KEY_VALUE_STORE_SOLVER`` are **deprecated aliases** kept so that existing
    report configs continue to deserialize; both now resolve to the same
    ``DefaultSolver``. They will be removed in a future release.

    This is a :class:`~enum.StrEnum`: each member *is* its string value, so the
    ``query_engine.solver`` field is a plain ``str`` (accepting any registered
    solver name, including customer solvers) while existing comparisons against
    these members — ``qe.solver == Solvers.DEFAULT_SOLVER`` — keep working.

    Attributes
    ----------
    DEFAULT_SOLVER : str
    DELTA_SOLVER : str
        Deprecated alias for ``DEFAULT_SOLVER``.
    KEY_VALUE_STORE_SOLVER : str
        Deprecated alias for ``DEFAULT_SOLVER``.
    """

    DEFAULT_SOLVER = "DefaultSolver"
    DELTA_SOLVER = "DeltaSolver"
    KEY_VALUE_STORE_SOLVER = "KeyValueStoreSolver"


class Source(BaseModel):
    """
    Configuration for data source tables in Unity Catalog.

    Attributes
    ----------
    container_tags_table : str, optional
        Full Unity Catalog path to the container tags table (narrow/EAV format).
        Required when filtering by container tags. Omit for wide-only data
        models that carry container attributes as columns on
        ``container_metrics``. ``project_id`` scoping is independent of this
        field — it works in both narrow EAV and wide-only data models because
        it is applied to ``container_metrics`` (and ``channel_mapping`` if
        configured) regardless of whether ``container_tags_table`` is set.
    container_metrics_table : str
        Full Unity Catalog path to the container metrics table.
    channel_metrics_table : str
        Full Unity Catalog path to the channel metrics table.
    channels_uri : str
        Full Unity Catalog path to the channels data table.
    poi_channels_uri : str, optional
        Full Unity Catalog path to the Points-in-Time (POI) channel data table.
        Required only when the report selects POI channels via ``poi_channel()``;
        omit it for sample-only data models.
    channel_mapping_table : str, optional
        Full Unity Catalog path to the channel mapping table. Required when using
        ``channel_with_alias()`` for logical alias resolution.
    unit_conversion_table : str, optional
        Full Unity Catalog path to the unit conversion table. When set together
        with a ``channel_mapping_table`` whose rows carry ``source_unit`` and
        ``target_unit`` columns, the query engine converts time-series values
        from the source to the target unit during ``solve()``.

    Notes
    -----
    All table names must follow Unity Catalog naming conventions:
    'catalog.schema.table' format with valid characters only.
    """

    container_tags_table: Annotated[str, AfterValidator(is_valid_table_name)] | None = None
    channel_tags_table: Annotated[str, AfterValidator(is_valid_table_name)] | None = None
    container_metrics_table: Annotated[str, AfterValidator(is_valid_table_name)]
    channel_metrics_table: Annotated[str, AfterValidator(is_valid_table_name)]
    channels_uri: Annotated[str, AfterValidator(is_valid_table_name)]
    poi_channels_uri: Annotated[str, AfterValidator(is_valid_table_name)] | None = None
    channel_mapping_table: Annotated[str, AfterValidator(is_valid_table_name)] | None = None
    unit_conversion_table: Annotated[str, AfterValidator(is_valid_table_name)] | None = None


class UnitySink(BaseModel):
    """
    Configuration for data sink location in Unity Catalog.

    Attributes
    ----------
    catalog : str
        Target catalog name for output tables.
    schema : str
        Target schema name for output tables.
    table_prefix : str
        Prefix to use for generated output table names.
    cleanup_temp_tables : bool
        When ``True``, the intermediate ``__impulse_temp_*`` tables written to this
        sink during batch solving are dropped after ``persist_results()`` completes
        successfully. Defaults to ``False`` (temp tables are retained for inspection
        and only cleared at the start of the next report run).

    Notes
    -----
    All entity names must contain only letters, numbers, hyphens, and underscores.
    """

    catalog: Annotated[str, AfterValidator(is_valid_unity_entity_name)]
    schema: Annotated[str, AfterValidator(is_valid_unity_entity_name)]
    table_prefix: Annotated[
        str,
        AfterValidator(lambda v: v if v == "" else is_valid_unity_entity_name(v)),
    ]
    cleanup_temp_tables: bool = False


class Comparator(str, Enum):
    """
    Supported comparison operators for container filters.
    """

    EQ = "=="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="


class CastType(str, Enum):
    """
    Supported Spark cast types for tag value columns.
    """

    STRING = "string"
    INT = "int"
    DOUBLE = "double"
    TIMESTAMP = "timestamp"


class TagFilter(BaseModel):
    """
    A single tag-based filter applied on the container_tags_table (EAV).

    Attributes
    ----------
    tag_name : str
        The tag key / element_id to filter on.
    comparator : Comparator
        The comparison operator.
    value : str | int | float | datetime
        The expected value. Must match the cast_type: str for STRING,
        int for INT, int|float for DOUBLE, ISO-format string for TIMESTAMP
        (automatically parsed to datetime).
    cast_type : CastType
        Spark type to cast the tag value to before comparison.
    """

    tag_name: str
    comparator: Comparator
    value: str | int | float | datetime
    cast_type: CastType = CastType.STRING

    @model_validator(mode="after")
    def _validate_value_matches_cast_type(self) -> "TagFilter":
        v = self.value
        ct = self.cast_type

        if ct == CastType.STRING:
            if not isinstance(v, str):
                raise ValueError(
                    f"cast_type 'string' requires a str value, got {type(v).__name__}"
                )
        elif ct == CastType.INT:
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"cast_type 'int' requires an int value, got {type(v).__name__}")
        elif ct == CastType.DOUBLE:
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise ValueError(
                    f"cast_type 'double' requires a numeric value, got {type(v).__name__}"
                )
        elif ct == CastType.TIMESTAMP:
            if not isinstance(v, str):
                raise ValueError(
                    f"cast_type 'timestamp' requires an ISO-format string value, "
                    f"got {type(v).__name__}"
                )
            try:
                self.value = datetime.fromisoformat(v)
            except ValueError as err:
                raise ValueError(
                    f"cast_type 'timestamp' requires a valid ISO-format string, got '{v}'"
                ) from err

        return self


class MetricFilter(BaseModel):
    """
    A single metric-based filter applied on the container_metrics_table.

    Attributes
    ----------
    column_name : str
        The metric column to filter on.
    comparator : Comparator
        The comparison operator.
    value : str | int | float | datetime
        The expected value. When value_type is provided, must match accordingly.
    value_type : CastType, optional
        When provided, validates and/or converts the value to the expected type.
    """

    column_name: str
    comparator: Comparator
    value: str | int | float | datetime
    value_type: CastType | None = None

    @model_validator(mode="after")
    def _validate_value_matches_value_type(self) -> "MetricFilter":
        if self.value_type is None:
            return self

        v = self.value
        vt = self.value_type

        if vt == CastType.STRING:
            if not isinstance(v, str):
                raise ValueError(
                    f"value_type 'string' requires a str value, got {type(v).__name__}"
                )
        elif vt == CastType.INT:
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"value_type 'int' requires an int value, got {type(v).__name__}")
        elif vt == CastType.DOUBLE:
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise ValueError(
                    f"value_type 'double' requires a numeric value, got {type(v).__name__}"
                )
        elif vt == CastType.TIMESTAMP:
            if not isinstance(v, str):
                raise ValueError(
                    f"value_type 'timestamp' requires an ISO-format string value, "
                    f"got {type(v).__name__}"
                )
            try:
                self.value = datetime.fromisoformat(v)
            except ValueError as err:
                raise ValueError(
                    f"value_type 'timestamp' requires a valid ISO-format string, got '{v}'"
                ) from err

        return self


class ContainerFilters(BaseModel):
    """
    Container-level filters in disjunctive normal form (OR of ANDs).

    Each outer list element is a group of filters that are AND-combined.
    The resulting group expressions are then OR-combined.

    Attributes
    ----------
    tag_filters : list[list[TagFilter]]
        Tag-based filter groups (applied on container_tags_table).
    metric_filters : list[list[MetricFilter]]
        Metric-based filter groups (applied on container_metrics_table).
    """

    tag_filters: list[list[TagFilter]] = []
    metric_filters: list[list[MetricFilter]] = []


class QueryEngine(BaseModel):
    """
    Configuration for the query engine solver.

    Parameters
    ----------
    solver : Solvers, default=Solvers.DEFAULT_SOLVER
        The solver type to use for query execution.
    raw_encoder : RawEncoder, optional, default=None
        Encoder used to convert RAW point data into intervals.  ``RLE``
        collapses consecutive equal-valued samples into runs; ``INTERVAL``
        only derives ``tend`` and drops exact duplicates.  Only takes effect
        when ``data_type=RAW``; ignored for RLE input.  When omitted and
        ``data_type=RAW``, it is resolved to ``RLE`` at validation time;
        for RLE input the field stays ``None`` and is never consulted.
    solver_config : SolverConfig, optional
        Per-table column name mappings and filter configuration for
        the solver.  Use this when your silver-layer tables use
        non-default column names or when you need project/toolbox
        scoping.  Key sub-fields:

        - ``project_id`` (str): Top-level project filter value applied
          to container_tags, container_metrics, and channel_mapping
          tables when the corresponding columns exist after column
          renaming.
        - Per-table sections (``container_tags``, ``container_metrics``,
          ``channel_mapping``, ``channels``, etc.) each with
          ``column_name_mapping`` and ``filters`` dicts.

        When omitted, all default column names are used and no
        project/toolbox filtering is applied.

    Notes
    -----
    The default solver is ``Solvers.DEFAULT_SOLVER``.  It selects channels
    from a narrow EAV ``channel_tags`` table when ``source.channel_tags_table``
    is configured, and otherwise directly from columns on ``channel_metrics``.
    It operates either with a narrow EAV ``container_tags`` table or in a
    wide-only data model when ``source.container_tags_table`` is not
    configured.  (``DELTA_SOLVER`` and ``KEY_VALUE_STORE_SOLVER`` are
    deprecated aliases that resolve to the same solver.)

    - RLE channel data must contain 'container_id', 'channel_id', 'tstart', 'tend', 'value' columns
    - RAW channel data must contain 'container_id', 'channel_id', 'timestamp', 'value' columns
    """

    solver: str = Solvers.DEFAULT_SOLVER
    data_type: DataType = DataType.RLE
    drop_implausible_data: bool = False
    raw_encoder: RawEncoder | None = None
    solver_config: SolverConfig | None = None
    batch_size: int = 500

    @model_validator(mode="before")
    @classmethod
    def _validate_solver_config_for_solver(cls, data):
        """Validate ``solver_config`` through the selected solver's config class.

        A custom solver registered with a :class:`SolverConfig` subclass (via
        ``register_solver(name, MyConfig)``) can carry extra config fields.  By
        default Pydantic would parse ``solver_config`` as the base
        :class:`SolverConfig` and silently drop those fields.  Here we resolve
        the registered ``config_cls`` for ``solver`` and re-validate the raw
        ``solver_config`` dict through it, so extra/required fields are enforced
        at parse time and the subclass instance is preserved on the field.

        The solver must be registered when the config is parsed — i.e. the
        driver imported the customer's package before building the report.  An
        unknown name is rejected here (as a ``ValidationError`` listing the
        registered names), which surfaces a missing import early instead of
        silently accepting an unusable config.
        """
        if not isinstance(data, dict):
            return data
        name = str(data.get("solver", Solvers.DEFAULT_SOLVER))
        try:
            config_cls = resolve_registration(name).config_cls
        except KeyError as exc:
            # Re-raise as ValueError so Pydantic surfaces it as a ValidationError.
            raise ValueError(str(exc)) from exc
        raw_config = data.get("solver_config")
        if isinstance(raw_config, dict):
            data["solver_config"] = config_cls.model_validate(raw_config)
        return data

    @model_validator(mode="after")
    def validate_drop_implausible_data_requires_raw(self):
        """`drop_implausible_data=True` currently only takes effect with RAW data.

        The filter is applied inside the RAW -> interval conversion path by the
        selected ``raw_encoder`` (``RleEncoder`` / ``IntervalEncoder``).
        """
        if self.drop_implausible_data and self.data_type is not DataType.RAW:
            raise ValueError(
                "drop_implausible_data=True requires data_type=RAW. "
                "The implausible-data filter is only applied during the RAW -> RLE "
                "conversion path; RLE input is passed through unchanged."
            )
        return self

    @model_validator(mode="after")
    def default_raw_encoder_for_raw_data(self):
        """When ``data_type=RAW`` and ``raw_encoder`` is unset, default to RLE."""
        if self.data_type is DataType.RAW and self.raw_encoder is None:
            self.raw_encoder = RawEncoder.RLE
        return self


class IncrementalConfig(BaseModel):
    """
    Configuration for incremental processing behavior.

    Attributes
    ----------
    enabled : bool, default=False
        Whether incremental processing is enabled.
    silver_last_modified_column : str, default="timestamp"
        Column name in the silver layer used for freshness comparison.
    gold_last_modified_column : str, default="last_modified"
        Column name in the gold layer used for freshness comparison.
    Notes
    -----
    When `enabled` is False, all processing will be done in full mode
    regardless of other settings.
    """

    enabled: bool = False
    data_type: DataType = DataType.RLE
    drop_implausible_data: bool = False
    silver_last_modified_column: str = "timestamp"
    gold_last_modified_column: str = "_created_at"


class CalculatedChannels(BaseModel):
    """
    Configuration for calculated-channel outputs.

    Attributes
    ----------
    emit_channel_metrics : bool, default=False
        When True, also emit a ``calculated_channel_metrics`` gold table (silver
        ``channel_metrics`` shape) alongside the calculated-channel fact table, so
        the fact + metrics pair can serve as an Impulse silver source.
    attribute_columns : list of str, default=[]
        Calculated-channel attribute keys to surface as columns on the metrics
        table (e.g. ``["unit"]``). Empty (the default) → no attribute columns.
        Identity keys are always surfaced dynamically and win over an
        attribute key of the same name.
    kpis : list of str, default=["duration", "min", "max", "mean"]
        KPIs computed on the metrics table, one column per name. Each must be a
        registered KPI (see ``calculated_channel_kpis.KPI_BUILDERS``); an unknown
        name is rejected at validation. Duplicates are removed (order preserved).
    """

    emit_channel_metrics: bool = False
    attribute_columns: list[str] = []
    kpis: list[str] = list(DEFAULT_KPIS)

    @field_validator("attribute_columns", mode="after")
    @classmethod
    def _normalize_attribute_columns(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        normalized: list[str] = []
        for name in value:
            is_valid_unity_entity_name(name)
            if name not in seen:
                seen.add(name)
                normalized.append(name)
        return normalized

    @field_validator("kpis", mode="after")
    @classmethod
    def _normalize_kpis(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        normalized: list[str] = []
        for name in value:
            if name not in KPI_BUILDERS:
                valid = ", ".join(sorted(KPI_BUILDERS))
                raise ValueError(f"Unknown calculated-channel KPI: {name}. Valid KPIs: {valid}.")
            if name not in seen:
                seen.add(name)
                normalized.append(name)
        return normalized


class ImpulseConfig(BaseModel):
    """
     Main configuration model.

     Attributes
     ----------
     source : Source
         Configuration for input data sources.
     unity_sink : UnitySink
         Configuration for output data location.
     container_filters : ContainerFilters, optional
         Optional container-level filters (tag-based and/or metric-based).
     query_engine : QueryEngine, optional
         Optional query engine configuration. Defaults to Solvers.DEFAULT_SOLVER.
     incremental : IncrementalConfig, optional
         Optional incremental processing configuration. Defaults to IncrementalConfig().
     calculated_channels : CalculatedChannels, optional
         Optional calculated-channel output configuration (e.g. opting in to the
         ``calculated_channel_metrics`` table). Defaults to CalculatedChannels().
     measurement_dimensions : list of str, optional
         Column names to surface from ``container_metrics`` into the
         gold-layer ``measurement_dimension`` table. Names are matched
         **after** ``query_engine.solver_config.container_metrics.column_name_mapping``
         has been applied — i.e. these are the internal (post-mapping)
         column names, not the physical silver column names. If a silver
         table uses a physical name like ``my_measurement_id`` mapped to
         ``container_id``, list ``"container_id"`` here. Each listed name
         lands in the gold table verbatim, so the configured name is also
         the gold column name. Defaults to
         ``["container_id", "start_ts", "stop_ts"]``. The framework does not
         inject any column the user omits — keeping ``container_id`` in the
         list is recommended because it is the upsert key for incremental
         processing and the join key to event-fact tables, but the choice
         is the user's.
     Examples
     --------
    >>> config_data = {
     ...     "source": {
     ...         "container_metrics_table": "impulse_demo.silver.container_metric",
     ...         "channel_metrics_table": "impulse_demo.silver.channel_metric",
     ...         "channels_uri": "impulse_demo.silver.channel_data",
     ...         "channel_mapping_table": "impulse_demo.data_model.channel_mapping"
     ...     },
     ...     "unity_sink": {
     ...         "catalog": "impulse_demo",
     ...         "schema": "silver_refactored",
     ...         "table_prefix": "evaluation"
     ...     },
     ...     "container_filters": {
     ...         "tag_filters": [
     ...             [
     ...                 {"tag_name": "uut_id", "comparator": "==", "value": "AA080518", "cast_type": "string"}
     ...             ]
     ...         ],
     ...         "metric_filters": [
     ...             [
     ...                 {"column_name": "uut_id", "comparator": "==", "value": "AA080518"},
     ...                 {"column_name": "start_ts", "comparator": ">=", "value": "2025-04-27T05:20:54.000Z"}
     ...             ]
     ...         ]
     ...     },
     ...     "query_engine": {
     ...         "solver": "DefaultSolver",
     ...         "solver_config": {
     ...             "project_id": "my_project",
     ...             "container_tags": {
     ...                 "column_name_mapping": {"entity_id": "container_id"},
     ...                 "filters": {"parent_id": "my_parent_id"}
     ...             },
     ...             "container_metrics": {
     ...                 "column_name_mapping": {}
     ...             },
     ...             "channel_metrics": {
     ...                 "column_name_mapping": {}
     ...             },
     ...             "channel_mapping": {
     ...                 "column_name_mapping": {},
     ...                 "filters": {"toolbox_id": "my_toolbox"}
     ...             },
     ...             "channels": {
     ...                 "column_name_mapping": {}
     ...             }
     ...         }
     ...     }
     ... }
     >>> config = ImpulseConfig.model_validate(config_data)
    """

    source: Source
    unity_sink: UnitySink | None = None
    container_filters: ContainerFilters | None = None
    query_engine: QueryEngine = QueryEngine(solver=Solvers.DEFAULT_SOLVER)
    incremental: IncrementalConfig | None = None
    calculated_channels: CalculatedChannels = CalculatedChannels()

    measurement_dimensions: list[str] = list(DEFAULT_MEASUREMENT_DIMENSIONS)

    @field_validator("measurement_dimensions", mode="after")
    @classmethod
    def _normalize_measurement_dimensions(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        normalized: list[str] = []
        for name in value:
            is_valid_unity_entity_name(name)
            if name not in seen:
                seen.add(name)
                normalized.append(name)
        return normalized
