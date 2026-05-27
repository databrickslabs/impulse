import json
import os
from unittest.mock import create_autospec

import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import Row

from impulse_reporting.config.config_parser import ImpulseConfig
from impulse_reporting.core.report import Report
from impulse_reporting.meta.container_dimensions import ContainerDimension
from tests.conftest import spark


def test_get_dimension_raises_on_missing_silver_column(spark):
    """Missing silver columns must surface a clear error, not be silently dropped."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    with open(config_path) as f:
        config_dict = json.load(f)
    config_dict["measurement_dimensions"] = ["container_id", "definitely_not_in_silver"]

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=config_dict,
    )

    with pytest.raises(ValueError, match="definitely_not_in_silver"):
        ContainerDimension.get_dimension(
            spark, my_report.query, my_report.solver, my_report.config
        )


def test_user_list_is_respected_verbatim():
    """A user-supplied list passes through unchanged — no auto-injection of container_id."""
    config = ImpulseConfig.model_validate(
        {
            "source": {
                "container_metrics_table": "c.s.container_metrics",
                "channel_metrics_table": "c.s.channel_metrics",
                "channels_uri": "c.s.channels",
            },
            "measurement_dimensions": ["uut_id", "start_ts"],
        }
    )
    assert config.measurement_dimensions == ["uut_id", "start_ts"]


def test_measurement_dimensions_dedupes_preserving_order():
    config = ImpulseConfig.model_validate(
        {
            "source": {
                "container_metrics_table": "c.s.container_metrics",
                "channel_metrics_table": "c.s.channel_metrics",
                "channels_uri": "c.s.channels",
            },
            "measurement_dimensions": [
                "container_id",
                "uut_id",
                "uut_id",
                "start_ts",
                "container_id",
            ],
        }
    )
    assert config.measurement_dimensions == ["container_id", "uut_id", "start_ts"]


def test_config_hashing(spark):
    base_path = os.path.dirname(os.path.abspath(__file__))

    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    with open(config_path) as f:
        config_dict = json.load(f)

    impulse_config = ImpulseConfig(**config_dict)

    silver_columns = ["uut_id"]
    schema = T.StructType([T.StructField(col, T.StringType(), True) for col in silver_columns])

    df = spark.createDataFrame([("test_vehicle",)], schema)
    result = df.transform(ContainerDimension._add_config_hash(impulse_config))

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].uut_id == "test_vehicle"
    assert rows[0].config_hash is not None
    # Hashing must be deterministic for the same config.
    second_run = df.transform(ContainerDimension._add_config_hash(impulse_config)).collect()
    assert rows[0].config_hash == second_run[0].config_hash

    empty_df = spark.createDataFrame([], schema)
    assert empty_df.transform(ContainerDimension._add_config_hash(impulse_config)).count() == 0


def test_container_dimensions_default_col_order(spark):
    """Default measurement_dimensions surfaces only container_id, start_ts, stop_ts."""
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

    dimensions_df = ContainerDimension.get_dimension(
        spark, my_report.query, my_report.solver, my_report.config
    )

    # Default list + the always-added config_hash column.
    assert dimensions_df.columns == ["container_id", "start_ts", "stop_ts", "config_hash"]
