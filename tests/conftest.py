import os
from unittest.mock import create_autospec

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import pytest
from databricks.sdk import WorkspaceClient
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

import impulse_query_engine.schema as S
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = configure_spark_with_delta_pip(
        SparkSession.builder.master("local")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.databricks.delta.retentionDurationCheck.enabled ", "false")
        .config("spark.shuffle.partitions", 1)
    ).getOrCreate()
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_narrow_db")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_key_value_store")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_key_value_store_alias")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold")
    return spark


@pytest.fixture
def mock_workspace_client():
    """Return a mock WorkspaceClient for telemetry in tests."""
    return create_autospec(WorkspaceClient)


@pytest.fixture
def basic_narrow_db(spark, mock_workspace_client) -> MeasurementDB:
    """Return a basic narrow MeasurementDB instance with preloaded data."""
    tables = {}
    tables["container_metrics"] = spark.read.table("spark_catalog.silver.container_metrics")
    tables["channel_metrics"] = spark.read.table("spark_catalog.silver.channel_metrics")
    tables["channels"] = spark.read.table("spark_catalog.silver.channels")

    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture
def setup_narrow_db(spark):
    # delete all existing tables in silver_narrow_db schema
    silver_tables = spark.sql("SHOW TABLES IN spark_catalog.silver_narrow_db").collect()

    for table in silver_tables:
        table_name = table.tableName
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.silver_narrow_db.{table_name} PURGE")

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    container_tags = spark.createDataFrame(
        pd.read_csv(f"{base_path}/tests/unit/data/unit_test_csv/1_container_tags.csv"),
        schema=S.CONTAINER_TAGS,
    )
    container_metrics = spark.createDataFrame(
        pd.read_csv(
            f"{base_path}/tests/unit/data/unit_test_csv/1_container_metrics.csv",
            parse_dates=[1, 2],
        ),
        schema=S.CONTAINER_METRICS,
    )
    channel_tags = spark.createDataFrame(
        pd.read_csv(f"{base_path}/tests/unit/data/unit_test_csv/1_channel_tags.csv"),
        schema=S.CHANNEL_TAGS,
    )
    channel_metrics = spark.createDataFrame(
        pd.read_csv(f"{base_path}/tests/unit/data/unit_test_csv/1_channel_metrics.csv"),
        schema=S.CHANNEL_METRICS,
    )
    channels = spark.createDataFrame(
        pd.read_csv(
            f"{base_path}/tests/unit/data/unit_test_csv/1_channels.csv",
            dtype={
                "container_id": np.int64,
                "channel_id": np.int32,
                "tstart": np.longlong,
                "tend": np.longlong,
                "value": np.float64,
            },
        ),
        schema=S.CHANNELS_SCHEMA,
    )

    container_tags.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_narrow_db.container_tags"
    )
    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_narrow_db.container_metrics"
    )
    channel_tags.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_narrow_db.channel_tags"
    )
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_narrow_db.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_narrow_db.channels"
    )


@pytest.fixture(scope="session", autouse=True)
def setup_basic_db(spark):
    """Setup necessary silver tables."""

    # delete all existing tables in silver schema
    silver_tables = spark.sql("SHOW TABLES IN spark_catalog.silver").collect()

    for table in silver_tables:
        table_name = table.tableName
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.silver.{table_name} PURGE")

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    container_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/container_metrics.csv"
    channel_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_metrics.csv"
    channels_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv"

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}
    container_metrics = spark.read.options(**options).csv(container_metric_path)
    channel_metrics = spark.read.options(**options).csv(channel_metric_path)
    channels = spark.read.options(**options).csv(channels_path)

    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver.container_metrics"
    )
    container_metrics.where(f.col("container_id") == 1).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("spark_catalog.silver.container_metrics_inc_1")
    container_metrics.where(f.col("container_id").isin([1, 2])).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("spark_catalog.silver.container_metrics_inc_1_2")
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable("spark_catalog.silver.channels")


@pytest.fixture(scope="function", autouse=True)
def cleanup_gold(request, spark):
    """Drop all gold tables after each test function."""

    def remove_gold_layer():
        gold_tables = spark.sql("SHOW TABLES IN spark_catalog.gold").collect()

        for table in gold_tables:
            table_name = table.tableName
            spark.sql(f"DROP TABLE IF EXISTS spark_catalog.gold.{table_name} PURGE")

    request.addfinalizer(remove_gold_layer)


@pytest.fixture(scope="session", autouse=True)
def cleanup_schemas(request, spark):
    """Cleanup silver and gold schema once tests are finished."""

    def remove_test_dir():
        spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver_key_value_store CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver_key_value_store_alias CASCADE")
        spark.sql("DROP SCHEMA IF EXISTS spark_catalog.gold CASCADE")

    request.addfinalizer(remove_test_dir)


@pytest.fixture
def narrow_db(spark, setup_narrow_db, mock_workspace_client) -> MeasurementDB:
    """Return a narrow MeasurementDB instance with preloaded data."""
    debug_tables = {}

    debug_tables["container_tags"] = spark.read.table(
        "spark_catalog.silver_narrow_db.container_tags"
    )
    debug_tables["container_metrics"] = spark.read.table(
        "spark_catalog.silver_narrow_db.container_metrics"
    )
    debug_tables["channel_tags"] = spark.read.table("spark_catalog.silver_narrow_db.channel_tags")
    debug_tables["channel_metrics"] = spark.read.table(
        "spark_catalog.silver_narrow_db.channel_metrics"
    )
    debug_tables["channels"] = spark.read.table("spark_catalog.silver_narrow_db.channels")

    cfg = MeasurementDBConfig.for_debug(debug_tables)
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture(scope="session")
def setup_key_value_store_db(spark):
    """Setup key-value-store tables for testing."""

    # Delete all existing tables in silver_key_value_store schema
    silver_tables = spark.sql("SHOW TABLES IN spark_catalog.silver_key_value_store").collect()

    for table in silver_tables:
        table_name = table.tableName
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.silver_key_value_store.{table_name} PURGE")

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    # Load key-value-store container_tags (narrow/EAV format) - this is the new metadata table
    container_tags_path = f"{base_path}/tests/unit/data/key_value_store_csv/container_metrics.csv"
    # Load container_metrics from silver layer (wide format)
    container_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/container_metrics.csv"
    # Reuse channel data from basic_narrow_csv
    channel_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_metrics.csv"
    channels_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv"

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}
    container_tags = spark.read.options(**options).csv(container_tags_path)
    container_metrics = spark.read.options(**options).csv(container_metric_path)
    channel_metrics = spark.read.options(**options).csv(channel_metric_path)
    channels = spark.read.options(**options).csv(channels_path)

    container_tags.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store.container_tags"
    )
    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store.container_metrics"
    )
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store.channels"
    )


@pytest.fixture(scope="session")
def setup_key_value_store_alias_db(spark):
    """Setup key-value-store tables with channel alias data for testing."""

    silver_tables = spark.sql(
        "SHOW TABLES IN spark_catalog.silver_key_value_store_alias"
    ).collect()

    for table in silver_tables:
        table_name = table.tableName
        spark.sql(
            f"DROP TABLE IF EXISTS spark_catalog.silver_key_value_store_alias.{table_name} PURGE"
        )

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    container_tags_path = f"{base_path}/tests/unit/data/key_value_store_csv/container_metrics.csv"
    container_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/container_metrics.csv"
    channel_metric_path = (
        f"{base_path}/tests/unit/data/key_value_store_alias_csv/channel_metrics.csv"
    )
    channels_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv"
    channel_mapping_path = (
        f"{base_path}/tests/unit/data/key_value_store_alias_csv/channel_mapping.csv"
    )

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}
    container_tags = spark.read.options(**options).csv(container_tags_path)
    container_metrics = spark.read.options(**options).csv(container_metric_path)
    channel_metrics = spark.read.options(**options).csv(channel_metric_path)
    channels = spark.read.options(**options).csv(channels_path)
    channel_mapping = spark.read.options(**options).csv(channel_mapping_path)

    container_tags.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.container_tags"
    )
    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.container_metrics"
    )
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.channels"
    )
    channel_mapping.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.channel_mapping"
    )


@pytest.fixture
def key_value_store_db(spark, setup_key_value_store_db, mock_workspace_client) -> MeasurementDB:
    """Return a key-value-store MeasurementDB instance with preloaded data."""
    tables = {}
    tables["container_tags"] = spark.read.table(
        "spark_catalog.silver_key_value_store.container_tags"
    )
    tables["container_metrics"] = spark.read.table(
        "spark_catalog.silver_key_value_store.container_metrics"
    )
    tables["channel_metrics"] = spark.read.table(
        "spark_catalog.silver_key_value_store.channel_metrics"
    )
    tables["channels"] = spark.read.table("spark_catalog.silver_key_value_store.channels")

    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture
def key_value_store_alias_db(
    spark, setup_key_value_store_alias_db, mock_workspace_client
) -> MeasurementDB:
    """Return a key-value-store MeasurementDB with channel mapping configured."""
    tables = {}
    tables["container_tags"] = spark.read.table(
        "spark_catalog.silver_key_value_store_alias.container_tags"
    )
    tables["container_metrics"] = spark.read.table(
        "spark_catalog.silver_key_value_store_alias.container_metrics"
    )
    tables["channel_metrics"] = spark.read.table(
        "spark_catalog.silver_key_value_store_alias.channel_metrics"
    )
    tables["channels"] = spark.read.table("spark_catalog.silver_key_value_store_alias.channels")
    tables["channel_mapping"] = spark.read.table(
        "spark_catalog.silver_key_value_store_alias.channel_mapping"
    )

    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    return MeasurementDB(cfg, ws=mock_workspace_client)
