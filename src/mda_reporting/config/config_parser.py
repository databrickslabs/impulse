import re
from enum import Enum, StrEnum
from typing import Annotated

import pyspark.sql.functions as f
from pydantic import BaseModel, AfterValidator, model_validator
from pyspark.sql import Column
from datetime import datetime

from mda_query_engine.analyze.query.solvers import SolverConfig


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


class MeasurementDimensions(Enum):
    """
    Enumeration for available measurement dimensions information.
    Attributes
    ----------
    CONTAINER_ID : str
        Identifier for the container.
    UUT_ID : str
        Identifier for the unit under test (UUT).
    PROJECT_ID : str
        Identifier for the project.
    UUT_NAME : str
        Name of the unit under test (UUT). Currently not present in implementation.
    FILE_NAME : str
        Name of the file associated with the measurement.
    SOURCE_FILE_PATH : str
        Path to the source file containing the measurement data.
    START_TS : str
        Timestamp of the first data point in the measurement.
    STOP_TS : str
        Timestamp of the last data point in the measurement.
    ODO_START : str
        Starting odometer reading for the measurement. Currently not present in implementation.
    ODO_STOP : str
        Stopping odometer reading for the measurement. Currently not present in implementation.
    ENVIRONMENT : str
        Environment in which the measurement was taken either puma or datalogger.
    Notes
    -----
    The `get_column` method returns the corresponding Spark SQL column for each dimension.
    """

    CONTAINER_ID = "container_id"
    UUT_ID = "uut_id"
    PROJECT_ID = "project_id"  # todo not present currently
    VEHICLE_KEY = "vehicle_key"
    UUT_NAME = "uut_name"  # todo not present currently
    FILE_NAME = "file_name"
    SOURCE_FILE_PATH = "source_file_path"
    START_TS = "start_ts"
    STOP_TS = "stop_ts"
    ODO_START = "odo_start"  # todo not present currently
    ODO_STOP = "odo_stop"  # todo not present currently
    ENVIRONMENT = "environment"

    def get_column(self) -> Column:
        """
        Returns the corresponding Spark SQL column for the measurement dimension.
        The column names are mapped to their respective values based on the ER gold naming conventions.
        Returns
        -------
        pyspark.sql.Column
            The Spark SQL column corresponding to the measurement dimension.
        """
        measurement_dimensions_not_present_currently = [
            MeasurementDimensions.UUT_NAME,
            MeasurementDimensions.ODO_START,
            MeasurementDimensions.ODO_STOP,
        ]
        measurement_column = (
            f.lit("NOT_IMPLEMENTED")
            if self in measurement_dimensions_not_present_currently
            else f.column(self.value)
        )
        return measurement_column

    def map_gold_name_to_silver(self) -> str:
        """
        Maps the silver layer column name to the ER gold layer column name.

        Returns
        -------
        str
            The gold layer column name.
        """
        measurement_dimensions_er_gold_naming_map = {
            MeasurementDimensions.PROJECT_ID: "project",
            MeasurementDimensions.SOURCE_FILE_PATH: "file_path",
        }

        column_name = measurement_dimensions_er_gold_naming_map.get(self, self.value)
        return column_name


class DataType(StrEnum):
    RAW = "RAW"
    RLE = "RLE"


class Solvers(Enum):
    """
    Enumeration of available solver types for the query engine.

    Attributes
    ----------
    BASIC_NARROW_SOLVER : str
    DELTA_SOLVER : str
    KEY_VALUE_STORE_SOLVER : str
    """

    BASIC_NARROW_SOLVER = "BasicNarrowSolver"
    DELTA_SOLVER = "DeltaSolver"
    KEY_VALUE_STORE_SOLVER = "KeyValueStoreSolver"


class Source(BaseModel):
    """
    Configuration for data source tables in Unity Catalog.

    Attributes
    ----------
    container_metrics_table : str
        Full Unity Catalog path to the container metrics table.
    channel_metrics_table : str
        Full Unity Catalog path to the channel metrics table.
    channels_uri : str
        Full Unity Catalog path to the channels data table.

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

    Notes
    -----
    All entity names must contain only letters, numbers, hyphens, and underscores.
    """

    catalog: Annotated[str, AfterValidator(is_valid_unity_entity_name)]
    schema: Annotated[str, AfterValidator(is_valid_unity_entity_name)]
    table_prefix: Annotated[str, AfterValidator(is_valid_unity_entity_name)]


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
    solver : Solvers, default=Solvers.BASIC_NARROW_SOLVER
        The solver type to use for query execution.
    project_id : str, optional
        The project ID for KeyValueStoreSolver. Required when solver
        is KEY_VALUE_STORE_SOLVER.
    parent_id : str, optional
        The parent_id to filter entities by in concept_entities (e.g.
        ``"uut_concept"``).  Default: ``None`` (no parent_id filter).
    entity_maps_to : str, optional
        How ``entity_id`` in concept_entities maps to the
        container_metrics table.  ``"uut_id"`` (default) for
        1-to-many vehicle→files mapping, ``"container_id"`` for
        direct 1-to-1 file mapping.

    Notes
    -----
    The default solver is set to ``Solvers.BASIC_NARROW_SOLVER``.
    When using ``Solvers.KEY_VALUE_STORE_SOLVER``, ``project_id`` must
    be provided.
    - RLE channel data must contain 'container_id', 'channel_id', 'tstart', 'tend', 'value' columns
    - RAW channel data must contain 'container_id', 'channel_id', 'timestamp', 'value' columns
    """

    solver: Solvers = Solvers.BASIC_NARROW_SOLVER
    data_type: DataType = DataType.RLE
    drop_implausible_data: bool = False

    project_id: str | None = None
    parent_id: str | None = None
    entity_maps_to: str | None = "uut_id"
    solver_config: SolverConfig | None = None

    @model_validator(mode="after")
    def validate_project_id_for_key_value_store_solver(self):
        """Validate that project_id is provided when using KeyValueStoreSolver."""
        if self.solver == Solvers.KEY_VALUE_STORE_SOLVER and self.project_id is None:
            raise ValueError("project_id is required when using KeyValueStoreSolver")
        if self.entity_maps_to not in ("uut_id", "container_id"):
            raise ValueError(
                f"entity_maps_to must be 'uut_id' or 'container_id', "
                f"got '{self.entity_maps_to}'"
            )
        return self

    @model_validator(mode="after")
    def validate_drop_implausible_data_requires_raw(self):
        """`drop_implausible_data=True` currently only takes effect with RAW data.

        The filter is applied inside the RAW -> RLE conversion path in
        ``IntervalEncoder.prepare_channels_df``. RLE input short-circuits that
        path and the flag is silently ignored, so we reject the combination at
        config validation time.
        """
        if self.drop_implausible_data and self.data_type is not DataType.RAW:
            raise ValueError(
                "drop_implausible_data=True requires data_type=RAW. "
                "The implausible-data filter is only applied during the RAW -> RLE "
                "conversion path; RLE input is passed through unchanged."
            )
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
    drop_implausible_data: bool = False  # todo check this was added?
    silver_last_modified_column: str = "timestamp"
    gold_last_modified_column: str = "_created_at"


class MdaConfig(BaseModel):
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
         Optional query engine configuration. Defaults to Solvers.BASIC_NARROW_SOLVER.
     incremental : IncrementalConfig, optional
         Optional incremental processing configuration. Defaults to IncrementalConfig().
     measurement_dimensions : list of MeasurementDimensions, optional
         List of measurement dimensions to include in the configuration.
     Examples
     --------
    >>> config_data = {
     ...     "source": {
     ...         "container_metrics_table": "mda_demo.silver.container_metric",
     ...         "channel_metrics_table": "mda_demo.silver.channel_metric",
     ...         "channels_uri": "mda_demo.silver.channel_data"
     ...     },
     ...     "unity_sink": {
     ...         "catalog": "mda_demo",
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
     ...     }
     ... }
     >>> config = MdaConfig.model_validate(config_data)
    """

    source: Source
    unity_sink: UnitySink
    container_filters: ContainerFilters | None = None
    query_engine: QueryEngine = QueryEngine(solver=Solvers.BASIC_NARROW_SOLVER)
    incremental: IncrementalConfig | None = None

    measurement_dimensions: list[MeasurementDimensions] | None = [
        MeasurementDimensions.CONTAINER_ID,
        MeasurementDimensions.UUT_ID,
        MeasurementDimensions.FILE_NAME,
        MeasurementDimensions.SOURCE_FILE_PATH,
        MeasurementDimensions.START_TS,
        MeasurementDimensions.STOP_TS,
        MeasurementDimensions.PROJECT_ID,
        MeasurementDimensions.ENVIRONMENT,
    ]

    @model_validator(mode="after")
    def validate_container_tags_for_key_value_store_solver(self):
        """Validate that container_tags_table is provided when using KeyValueStoreSolver."""
        if (
            self.query_engine.solver == Solvers.KEY_VALUE_STORE_SOLVER
            and self.source.container_tags_table is None
        ):
            raise ValueError(
                "source.container_tags_table is required when using KeyValueStoreSolver"
            )
        return self
