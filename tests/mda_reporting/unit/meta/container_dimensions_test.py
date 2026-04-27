import json
import os
from unittest.mock import create_autospec

import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import Row

from mda_reporting.config.config_parser import MdaConfig, MeasurementDimensions
from mda_reporting.core.report import Report
from mda_reporting.meta.container_dimensions import ContainerDimension
from tests.conftest import spark


def test_rename_dimension_cols(spark):
    # Get all MeasurementDimensions as column names
    silver_columns = [
        "uut_id",
        "container_id",
        "file_name",
        "data_key",
        "start_ts",
        "stop_ts",
        "project",
        "file_path",
    ]

    # Create schema with StringType for all columns
    schema = T.StructType([T.StructField(col, T.StringType(), True) for col in silver_columns])

    # Create empty DataFrame
    df = spark.createDataFrame([], schema)

    renamed_df = ContainerDimension._rename_dimension_cols(
        df, [dim for dim in MeasurementDimensions]
    )
    expected_column_names = [
        "container_id",
        "uut_id",
        "project_id",
        "file_name",
        "source_file_path",
        "start_ts",
        "stop_ts",
    ]

    assert len(renamed_df.columns) == len(expected_column_names)
    for column_name in renamed_df.columns:
        assert (
            column_name in expected_column_names
        ), f"Column {column_name} is not in the expected list of renamed columns."


def test_config_hashing(spark):
    base_path = os.path.dirname(os.path.abspath(__file__))

    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    with open(config_path) as f:
        config_dict = json.load(f)

    mda_config = MdaConfig(**config_dict)

    silver_columns = ["uut_id"]

    # Create schema with StringType for all columns
    schema = T.StructType([T.StructField(col, T.StringType(), True) for col in silver_columns])
    df = spark.createDataFrame([("test_vehicle",)], schema)
    result = df.transform(ContainerDimension._add_config_hash(mda_config))
    expected_result = [Row(uut_id="test_vehicle", config_hash=2005032600)]

    assert expected_result == result.collect()

    empty_df = spark.createDataFrame([], schema)
    result = empty_df.transform(ContainerDimension._add_config_hash(mda_config))
    assert result.count() == 0


def test_container_dimensions_default_col_order(spark):
    """Test if the default column order of the container dimensions is correct."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    with open(config_path) as f:
        config_dict = json.load(f)

    config_dict.pop("measurement_dimensions")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=config_dict,
    )

    # Definition of relevant channels
    dimensions_df = ContainerDimension.get_dimension(
        spark, my_report.query, my_report.solver, my_report.config
    )

    assert dimensions_df.columns[0] == MeasurementDimensions.CONTAINER_ID.value
    assert dimensions_df.columns[1] == MeasurementDimensions.UUT_ID.value
    assert dimensions_df.columns[2] == MeasurementDimensions.FILE_NAME.value
    assert dimensions_df.columns[3] == MeasurementDimensions.SOURCE_FILE_PATH.value
    assert dimensions_df.columns[4] == MeasurementDimensions.START_TS.value
    assert dimensions_df.columns[5] == MeasurementDimensions.STOP_TS.value
    assert dimensions_df.columns[6] == MeasurementDimensions.PROJECT_ID.value
    assert dimensions_df.columns[7] == MeasurementDimensions.ENVIRONMENT.value
