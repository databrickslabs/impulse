import pyspark.sql.functions as f
import pytest
from pydantic import ValidationError

from mda_reporting.config.config_parser import (
    CastType,
    Comparator,
    ContainerFilters,
    IncrementalConfig,
    MdaConfig,
    MeasurementDimensions,
    MetricFilter,
    Solvers,
    TagFilter,
)

MDA_CONFIG_JSON = {
    "source": {
        "container_metrics_table": "avl_databricks_mvp.silver.container_metric",
        "channel_metrics_table": "avl_databricks_mvp.silver.channel_metric",
        "channels_uri": "avl_databricks_mvp.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "container_filters": {
        "metric_filters": [
            [
                {"column_name": "uut_id", "comparator": "==", "value": "123"},
                {
                    "column_name": "start_ts",
                    "comparator": ">=",
                    "value": "2025-04-27T05:20:54.000Z",
                },
            ]
        ]
    },
    "query_engine": {"solver": "BasicNarrowSolver"},
    "measurement_dimensions": [
        "uut_id",
        "file_name",
        "source_file_path",
        "start_ts",
        "stop_ts",
    ],
}


def test_mda_config_from_dict():
    """Test MdaConfig from a sample JSON-like dictionary."""
    config = MdaConfig.model_validate(MDA_CONFIG_JSON)
    assert config.source.container_metrics_table == "avl_databricks_mvp.silver.container_metric"
    assert config.unity_sink.catalog == "test_catalog"
    assert config.container_filters is not None
    assert len(config.container_filters.metric_filters) == 1
    assert len(config.container_filters.metric_filters[0]) == 2
    assert config.container_filters.metric_filters[0][0].column_name == "uut_id"
    assert config.container_filters.metric_filters[0][0].comparator == Comparator.EQ
    assert config.query_engine.solver == Solvers.BASIC_NARROW_SOLVER

    assert MeasurementDimensions.UUT_ID in config.measurement_dimensions
    assert MeasurementDimensions.FILE_NAME in config.measurement_dimensions
    assert MeasurementDimensions.SOURCE_FILE_PATH in config.measurement_dimensions
    assert MeasurementDimensions.START_TS in config.measurement_dimensions
    assert MeasurementDimensions.STOP_TS in config.measurement_dimensions


def test_mda_config_source_accepts_channel_mapping_table():
    """Test Source config accepts an optional channel_mapping_table."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["source"] = dict(MDA_CONFIG_JSON["source"])
    config_json["source"]["channel_mapping_table"] = "avl_meta.data_model.channel_mapping"
    config = MdaConfig.model_validate(config_json)
    assert config.source.channel_mapping_table == "avl_meta.data_model.channel_mapping"


def test_mda_config_source_rejects_invalid_channel_mapping_table():
    """Test Source config validates channel_mapping_table naming."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["source"] = dict(MDA_CONFIG_JSON["source"])
    config_json["source"]["channel_mapping_table"] = "invalid_table_name"
    with pytest.raises(ValidationError):
        MdaConfig.model_validate(config_json)


def test_mda_config_from_dict_no_query_engine_provided():
    """Test MdaConfig with no query engine provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json.pop("query_engine", None)
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.BASIC_NARROW_SOLVER


def test_mda_config_from_dict_no_measurement_dim_provided():
    """Test MdaConfig with no measurement dimension info provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json.pop("measurement_dimensions", None)
    config = MdaConfig.model_validate(config_json)

    assert MeasurementDimensions.CONTAINER_ID in config.measurement_dimensions
    assert MeasurementDimensions.UUT_ID in config.measurement_dimensions
    assert MeasurementDimensions.FILE_NAME in config.measurement_dimensions
    assert MeasurementDimensions.SOURCE_FILE_PATH in config.measurement_dimensions
    assert MeasurementDimensions.START_TS in config.measurement_dimensions
    assert MeasurementDimensions.STOP_TS in config.measurement_dimensions
    assert MeasurementDimensions.PROJECT_ID in config.measurement_dimensions
    assert MeasurementDimensions.ENVIRONMENT in config.measurement_dimensions


def test_mda_config_from_dict_wrong_measurement_dim_provided():
    """Test MdaConfig with wrong measurement dimension info provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json.update({"measurement_dimensions": ["wrong_dimension"]})

    with pytest.raises(ValidationError):
        config = MdaConfig.model_validate(config_json)


def test_mda_config_no_container_filters():
    """Test MdaConfig without container_filters field."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json.pop("container_filters")
    config = MdaConfig.model_validate(config_json)
    assert config.container_filters is None


def test_mda_config_empty_container_filters():
    """Test MdaConfig with empty container_filters."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["container_filters"] = {}
    config = MdaConfig.model_validate(config_json)
    assert config.container_filters is not None
    assert config.container_filters.tag_filters == []
    assert config.container_filters.metric_filters == []


def test_get_column():
    """Test the `get_column` method of `MeasurementDimensions`."""
    implemented = [
        MeasurementDimensions.CONTAINER_ID,
        MeasurementDimensions.PROJECT_ID,
        MeasurementDimensions.UUT_ID,
        MeasurementDimensions.FILE_NAME,
        MeasurementDimensions.SOURCE_FILE_PATH,
        MeasurementDimensions.START_TS,
        MeasurementDimensions.STOP_TS,
    ]
    not_implemented = [
        MeasurementDimensions.UUT_NAME,
        MeasurementDimensions.ODO_START,
        MeasurementDimensions.ODO_STOP,
    ]

    for dim in implemented:
        assert str(dim.get_column()) == str(f.col(dim.value))

    for dim in not_implemented:
        assert str(dim.get_column()) == str(f.lit("NOT_IMPLEMENTED"))


def test_map_gold_name_to_silver():
    """Test the `map_gold_name_to_silver` method of `MeasurementDimensions`."""
    expected_mappings = {
        MeasurementDimensions.CONTAINER_ID: MeasurementDimensions.CONTAINER_ID.value,
        MeasurementDimensions.UUT_ID: MeasurementDimensions.UUT_ID.value,
        MeasurementDimensions.PROJECT_ID: "project",
        MeasurementDimensions.UUT_NAME: MeasurementDimensions.UUT_NAME.value,
        MeasurementDimensions.FILE_NAME: MeasurementDimensions.FILE_NAME.value,
        MeasurementDimensions.SOURCE_FILE_PATH: "file_path",
        MeasurementDimensions.START_TS: "start_ts",
        MeasurementDimensions.STOP_TS: "stop_ts",
        MeasurementDimensions.ODO_START: MeasurementDimensions.ODO_START.value,
        MeasurementDimensions.ODO_STOP: MeasurementDimensions.ODO_STOP.value,
    }

    for dim, expected in expected_mappings.items():
        assert dim.map_gold_name_to_silver() == expected


def test_mda_config_key_value_store_solver_valid():
    """Test KeyValueStoreSolver config is accepted with project_id."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "test_project",
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [
                {
                    "tag_name": "uut_id",
                    "comparator": "==",
                    "value": "123",
                    "cast_type": "string",
                }
            ]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.KEY_VALUE_STORE_SOLVER


def test_mda_config_basic_narrow_solver_no_project_id():
    """Test BasicNarrowSolver config is valid without any solver_config."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "BasicNarrowSolver"}
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.BASIC_NARROW_SOLVER
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_none_by_default():
    """Test that solver_config defaults to None when not provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_dict():
    """Test that solver_config accepts a dictionary with per-table column mappings."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "test_project",
            "container_tags": {
                "column_name_mapping": {"project": "project_id"},
            },
            "channels": {
                "column_name_mapping": {
                    "measurement_id": "container_id",
                    "signal_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                },
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [
                {
                    "tag_name": "uut_id",
                    "comparator": "==",
                    "value": "123",
                    "cast_type": "string",
                }
            ]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is not None
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


def test_mda_config_solver_config_partial():
    """Test that solver_config accepts a partial dictionary (only some keys)."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "test_project",
            "channels": {
                "column_name_mapping": {"meas_id": "container_id"},
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [
                {
                    "tag_name": "uut_id",
                    "comparator": "==",
                    "value": "123",
                    "cast_type": "string",
                }
            ]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


def test_mda_config_key_value_store_solver_missing_project_id_empty_config():
    """KVS with empty solver_config (no project_id) raises ValidationError."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {},
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    with pytest.raises(ValidationError, match="project_id is required"):
        MdaConfig.model_validate(config_json)


def test_mda_config_key_value_store_solver_missing_container_tags_table():
    """KVS without container_tags_table in source raises ValidationError."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["source"] = {
        k: v for k, v in MDA_CONFIG_JSON["source"].items() if k != "container_tags_table"
    }
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {"project_id": "proj"},
    }
    with pytest.raises(ValidationError, match="container_tags_table is required"):
        MdaConfig.model_validate(config_json)


def test_mda_config_delta_solver_valid():
    """DeltaSolver is accepted without project_id or solver_config."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "DeltaSolver"}
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.DELTA_SOLVER
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_with_filters():
    """Per-table filters are parsed correctly in solver_config."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "proj",
            "container_tags": {
                "filters": {"environment": "production"},
            },
            "channels": {
                "column_name_mapping": {"meas_id": "container_id"},
                "filters": {"source": "live"},
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [
                {
                    "tag_name": "uut_id",
                    "comparator": "==",
                    "value": "123",
                    "cast_type": "string",
                }
            ]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    sc = config.query_engine.solver_config
    assert sc.container_tags.filters == {"environment": "production"}
    assert sc.channels.filters == {"source": "live"}
    assert sc.channels.column_name_mapping == {"meas_id": "container_id"}
    assert sc.container_metrics.filters == {}


# --- Container Filter Model Tests ---


def test_tag_filter_model():
    """Test TagFilter model parsing."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "uut_id",
            "comparator": "==",
            "value": "AA080518",
            "cast_type": "string",
        }
    )
    assert tf.tag_name == "uut_id"
    assert tf.comparator == Comparator.EQ
    assert tf.value == "AA080518"
    assert tf.cast_type == CastType.STRING


def test_tag_filter_default_cast_type():
    """Test TagFilter defaults to string cast_type."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "brand",
            "comparator": "!=",
            "value": "BMW",
        }
    )
    assert tf.cast_type == CastType.STRING


def test_metric_filter_model():
    """Test MetricFilter model parsing."""
    mf = MetricFilter.model_validate(
        {
            "column_name": "start_ts",
            "comparator": ">=",
            "value": "2025-04-27T05:00:00.000Z",
        }
    )
    assert mf.column_name == "start_ts"
    assert mf.comparator == Comparator.GE
    assert mf.value == "2025-04-27T05:00:00.000Z"


def test_metric_filter_no_value_type_accepts_any():
    """When value_type is omitted, any value type is accepted."""
    for value in ["text", 42, 3.14]:
        mf = MetricFilter.model_validate(
            {
                "column_name": "col",
                "comparator": "==",
                "value": value,
            }
        )
        assert mf.value_type is None
        assert mf.value == value


def test_metric_filter_value_type_string_valid():
    """value_type='string' with str value passes."""
    mf = MetricFilter.model_validate(
        {
            "column_name": "uut_id",
            "comparator": "==",
            "value": "AA080518",
            "value_type": "string",
        }
    )
    assert mf.value == "AA080518"
    assert isinstance(mf.value, str)


def test_metric_filter_value_type_string_rejects_int():
    """value_type='string' with int value raises ValidationError."""
    with pytest.raises(ValidationError, match="value_type 'string' requires a str value"):
        MetricFilter.model_validate(
            {
                "column_name": "uut_id",
                "comparator": "==",
                "value": 123,
                "value_type": "string",
            }
        )


def test_metric_filter_value_type_int_valid():
    """value_type='int' with int value passes."""
    mf = MetricFilter.model_validate(
        {
            "column_name": "count",
            "comparator": ">=",
            "value": 100,
            "value_type": "int",
        }
    )
    assert mf.value == 100
    assert isinstance(mf.value, int)


def test_metric_filter_value_type_int_rejects_str():
    """value_type='int' with str value raises ValidationError."""
    with pytest.raises(ValidationError, match="value_type 'int' requires an int value"):
        MetricFilter.model_validate(
            {
                "column_name": "count",
                "comparator": ">=",
                "value": "5",
                "value_type": "int",
            }
        )


def test_metric_filter_value_type_double_valid_float():
    """value_type='double' with float value passes."""
    mf = MetricFilter.model_validate(
        {
            "column_name": "threshold",
            "comparator": ">",
            "value": 3.14,
            "value_type": "double",
        }
    )
    assert mf.value == 3.14


def test_metric_filter_value_type_double_valid_int():
    """value_type='double' with int value passes (int is numeric)."""
    mf = MetricFilter.model_validate(
        {
            "column_name": "threshold",
            "comparator": ">",
            "value": 42,
            "value_type": "double",
        }
    )
    assert mf.value == 42


def test_metric_filter_value_type_double_rejects_str():
    """value_type='double' with str value raises ValidationError."""
    with pytest.raises(ValidationError, match="value_type 'double' requires a numeric value"):
        MetricFilter.model_validate(
            {
                "column_name": "threshold",
                "comparator": ">",
                "value": "3.14",
                "value_type": "double",
            }
        )


def test_metric_filter_value_type_timestamp_valid():
    """value_type='timestamp' with valid ISO string parses to datetime."""
    from datetime import datetime

    mf = MetricFilter.model_validate(
        {
            "column_name": "start_ts",
            "comparator": ">=",
            "value": "2025-04-27T05:00:00.000Z",
            "value_type": "timestamp",
        }
    )
    assert isinstance(mf.value, datetime)
    assert mf.value == datetime.fromisoformat("2025-04-27T05:00:00.000Z")


def test_metric_filter_value_type_timestamp_invalid_string():
    """value_type='timestamp' with non-ISO string raises ValidationError."""
    with pytest.raises(
        ValidationError,
        match="value_type 'timestamp' requires a valid ISO-format string",
    ):
        MetricFilter.model_validate(
            {
                "column_name": "start_ts",
                "comparator": ">=",
                "value": "not-a-timestamp",
                "value_type": "timestamp",
            }
        )


def test_container_filters_model():
    """Test ContainerFilters with both tag and metric filters (OR of ANDs)."""
    cf = ContainerFilters.model_validate(
        {
            "tag_filters": [
                [
                    {
                        "tag_name": "uut_id",
                        "comparator": "==",
                        "value": "AA",
                        "cast_type": "string",
                    },
                    {
                        "tag_name": "container_id",
                        "comparator": ">=",
                        "value": 100,
                        "cast_type": "int",
                    },
                ],
                [
                    {
                        "tag_name": "uut_id",
                        "comparator": "==",
                        "value": "BB",
                        "cast_type": "string",
                    },
                ],
            ],
            "metric_filters": [
                [
                    {
                        "column_name": "start_ts",
                        "comparator": ">=",
                        "value": "2025-01-01",
                    },
                ]
            ],
        }
    )
    assert len(cf.tag_filters) == 2
    assert len(cf.tag_filters[0]) == 2
    assert len(cf.tag_filters[1]) == 1
    assert len(cf.metric_filters) == 1
    assert cf.tag_filters[0][1].cast_type == CastType.INT


def test_invalid_comparator():
    """Test that an invalid comparator raises ValidationError."""
    with pytest.raises(ValidationError):
        MetricFilter.model_validate(
            {
                "column_name": "x",
                "comparator": "===",
                "value": 1,
            }
        )


def test_all_comparators():
    """Test all six comparators parse correctly."""
    for comp_str, comp_enum in [
        ("==", Comparator.EQ),
        ("!=", Comparator.NE),
        (">", Comparator.GT),
        (">=", Comparator.GE),
        ("<", Comparator.LT),
        ("<=", Comparator.LE),
    ]:
        mf = MetricFilter.model_validate(
            {
                "column_name": "col",
                "comparator": comp_str,
                "value": 42,
            }
        )
        assert mf.comparator == comp_enum


def test_all_cast_types():
    """Test all cast type enum values with matching value types."""
    from datetime import datetime

    for ct_str, ct_enum, value in [
        ("string", CastType.STRING, "v"),
        ("int", CastType.INT, 42),
        ("double", CastType.DOUBLE, 3.14),
        ("timestamp", CastType.TIMESTAMP, "2025-07-03T07:41:42"),
    ]:
        tf = TagFilter.model_validate(
            {
                "tag_name": "x",
                "comparator": "==",
                "value": value,
                "cast_type": ct_str,
            }
        )
        assert tf.cast_type == ct_enum


def test_tag_filter_string_value_valid():
    """cast_type=string with str value passes."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "uut_id",
            "comparator": "==",
            "value": "AA080518",
            "cast_type": "string",
        }
    )
    assert tf.value == "AA080518"
    assert isinstance(tf.value, str)


def test_tag_filter_string_value_rejects_int():
    """cast_type=string with int value raises ValidationError."""
    with pytest.raises(ValidationError, match="cast_type 'string' requires a str value"):
        TagFilter.model_validate(
            {
                "tag_name": "uut_id",
                "comparator": "==",
                "value": 123,
                "cast_type": "string",
            }
        )


def test_tag_filter_int_value_valid():
    """cast_type=int with int value passes."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "container_id",
            "comparator": ">=",
            "value": 100,
            "cast_type": "int",
        }
    )
    assert tf.value == 100
    assert isinstance(tf.value, int)


def test_tag_filter_int_value_rejects_str():
    """cast_type=int with str value raises ValidationError."""
    with pytest.raises(ValidationError, match="cast_type 'int' requires an int value"):
        TagFilter.model_validate(
            {
                "tag_name": "container_id",
                "comparator": ">=",
                "value": "100",
                "cast_type": "int",
            }
        )


def test_tag_filter_double_value_valid_float():
    """cast_type=double with float value passes."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "threshold",
            "comparator": ">",
            "value": 3.14,
            "cast_type": "double",
        }
    )
    assert tf.value == 3.14


def test_tag_filter_double_value_valid_int():
    """cast_type=double with int value passes (int is numeric)."""
    tf = TagFilter.model_validate(
        {
            "tag_name": "threshold",
            "comparator": ">",
            "value": 42,
            "cast_type": "double",
        }
    )
    assert tf.value == 42


def test_tag_filter_double_value_rejects_str():
    """cast_type=double with str value raises ValidationError."""
    with pytest.raises(ValidationError, match="cast_type 'double' requires a numeric value"):
        TagFilter.model_validate(
            {
                "tag_name": "threshold",
                "comparator": ">",
                "value": "3.14",
                "cast_type": "double",
            }
        )


def test_tag_filter_timestamp_value_valid():
    """cast_type=timestamp with valid ISO string parses to datetime."""
    from datetime import datetime

    tf = TagFilter.model_validate(
        {
            "tag_name": "start_ts",
            "comparator": ">=",
            "value": "2025-07-03T07:41:42.708000+00:00",
            "cast_type": "timestamp",
        }
    )
    assert isinstance(tf.value, datetime)
    assert tf.value == datetime.fromisoformat("2025-07-03T07:41:42.708000+00:00")


def test_tag_filter_timestamp_value_invalid_string():
    """cast_type=timestamp with non-ISO string raises ValidationError."""
    with pytest.raises(
        ValidationError,
        match="cast_type 'timestamp' requires a valid ISO-format string",
    ):
        TagFilter.model_validate(
            {
                "tag_name": "start_ts",
                "comparator": ">=",
                "value": "not-a-timestamp",
                "cast_type": "timestamp",
            }
        )


def test_tag_filter_timestamp_value_rejects_int():
    """cast_type=timestamp with int value raises ValidationError."""
    with pytest.raises(
        ValidationError, match="cast_type 'timestamp' requires an ISO-format string"
    ):
        TagFilter.model_validate(
            {
                "tag_name": "start_ts",
                "comparator": ">=",
                "value": 1234567890,
                "cast_type": "timestamp",
            }
        )


def test_mda_config_key_value_store_solver_valid():
    """Test KeyValueStoreSolver config with valid project_id in solver_config."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [{"tag_name": "uut_id", "comparator": "==", "value": "123", "cast_type": "string"}]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.KEY_VALUE_STORE_SOLVER
    assert config.query_engine.solver_config.project_id == "my_project"


def test_mda_config_key_value_store_solver_missing_project_id():
    """Test KeyValueStoreSolver config without project_id raises ValidationError."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "KeyValueStoreSolver"}
    with pytest.raises(ValidationError):
        MdaConfig.model_validate(config_json)


def test_mda_config_basic_narrow_solver_no_project_id():
    """Test BasicNarrowSolver config without project_id (backward compatible)."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "BasicNarrowSolver"}
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.BASIC_NARROW_SOLVER
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_none_by_default():
    """Test that solver_config defaults to None when not provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_dict():
    """Test that solver_config accepts a dictionary with per-table column mappings."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
            "container_tags": {
                "column_name_mapping": {"project": "project_id"},
            },
            "channels": {
                "column_name_mapping": {
                    "measurement_id": "container_id",
                    "signal_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                },
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [{"tag_name": "uut_id", "comparator": "==", "value": "123", "cast_type": "string"}]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is not None
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


def test_mda_config_solver_config_partial():
    """Test that solver_config accepts a partial dictionary (only some keys)."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
            "channels": {
                "column_name_mapping": {"meas_id": "container_id"},
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config_json["container_filters"] = {
        "tag_filters": [
            [{"tag_name": "uut_id", "comparator": "==", "value": "123", "cast_type": "string"}]
        ]
    }
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


def test_mda_config_key_value_store_solver_valid():
    """Test KeyValueStoreSolver config with valid project_id in solver_config."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.KEY_VALUE_STORE_SOLVER
    assert config.query_engine.solver_config.project_id == "my_project"


def test_mda_config_key_value_store_solver_missing_project_id():
    """Test KeyValueStoreSolver config without project_id raises ValidationError."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "KeyValueStoreSolver"}
    with pytest.raises(ValidationError):
        MdaConfig.model_validate(config_json)


def test_mda_config_basic_narrow_solver_no_project_id():
    """Test BasicNarrowSolver config without project_id (backward compatible)."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {"solver": "BasicNarrowSolver"}
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver == Solvers.BASIC_NARROW_SOLVER
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_none_by_default():
    """Test that solver_config defaults to None when not provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is None


def test_mda_config_solver_config_dict():
    """Test that solver_config accepts a dictionary with per-table column mappings."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
            "container_tags": {
                "column_name_mapping": {"project": "project_id"},
            },
            "channels": {
                "column_name_mapping": {
                    "measurement_id": "container_id",
                    "signal_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                },
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config is not None
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


def test_mda_config_solver_config_partial():
    """Test that solver_config accepts a partial dictionary (only some keys)."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["query_engine"] = {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "my_project",
            "channels": {
                "column_name_mapping": {"meas_id": "container_id"},
            },
        },
    }
    config_json["source"][
        "container_tags_table"
    ] = "spark_catalog.silver_key_value_store.container_tags"
    config = MdaConfig.model_validate(config_json)
    assert config.query_engine.solver_config.container_id_col == "container_id"
    assert config.query_engine.solver_config.tstart_col == "tstart"


# --- Incremental Configuration Tests ---


def test_incremental_config_default_values():
    """Test IncrementalConfig default values."""
    config = IncrementalConfig()
    assert config.enabled is False


def test_incremental_config_custom_values():
    """Test IncrementalConfig with custom values."""
    config = IncrementalConfig(enabled=True)
    assert config.enabled is True


def test_incremental_config_from_dict():
    """Test IncrementalConfig validation from dictionary."""
    config = IncrementalConfig.model_validate({"enabled": True})
    assert config.enabled is True


def test_mda_config_without_incremental():
    """Test MdaConfig without incremental configuration (default behavior)."""
    config = MdaConfig.model_validate(MDA_CONFIG_JSON)
    assert config.incremental is None


def test_mda_config_with_incremental():
    """Test MdaConfig with incremental configuration provided."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["incremental"] = {
        "enabled": True,
        "silver_last_modified_column": "timestamp",
        "gold_last_modified_column": "last_modified",
    }
    config = MdaConfig.model_validate(config_json)
    assert config.incremental is not None
    assert config.incremental.enabled is True


def test_mda_config_with_incremental_disabled():
    """Test MdaConfig with incremental explicitly disabled."""
    config_json = MDA_CONFIG_JSON.copy()
    config_json["incremental"] = {"enabled": False}
    config = MdaConfig.model_validate(config_json)
    assert config.incremental is not None
    assert config.incremental.enabled is False
