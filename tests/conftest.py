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
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_raw")
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


@pytest.fixture(scope="session")
def setup_raw_channels_db(spark):
    """Setup silver tables with a RAW-format ``channels`` table.

    ``channels`` holds raw ``(container_id, channel_id, timestamp, value)``
    point samples instead of pre-encoded intervals, so the two RAW encoders
    (``QueryEngineConfig.raw_encoder`` = ``RLE`` vs ``INTERVAL``) can be
    compared on the same signal. The RPM samples in ``channels.csv`` contain
    repeated consecutive values, so the RLE encoder genuinely merges runs
    while the INTERVAL encoder keeps every sample -- see
    ``raw_encoder_equivalence_test.py``. ``container_metrics`` and
    ``channel_metrics`` are reused from ``basic_narrow_csv``.
    """
    silver_tables = spark.sql("SHOW TABLES IN spark_catalog.silver_raw").collect()
    for table in silver_tables:
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.silver_raw.{table.tableName} PURGE")

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    container_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/container_metrics.csv"
    channel_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_metrics.csv"
    channels_path = f"{base_path}/tests/unit/data/raw_encoder_csv/channels.csv"

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}
    container_metrics = spark.read.options(**options).csv(container_metric_path)
    channel_metrics = spark.read.options(**options).csv(channel_metric_path)
    channels = spark.read.options(**options).csv(channels_path)

    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_raw.container_metrics"
    )
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_raw.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_raw.channels"
    )


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
        spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver_raw CASCADE")
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
    # Narrow/EAV channel_tags mirroring channel_metrics.channel_name. Only consumed
    # by the key_value_store_alias_with_channel_tags_db fixture (EAV + alias
    # coexistence); key_value_store_alias_db stays wide-only by omitting it.
    channel_tags_path = f"{base_path}/tests/unit/data/key_value_store_alias_csv/channel_tags.csv"

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}
    container_tags = spark.read.options(**options).csv(container_tags_path)
    container_metrics = spark.read.options(**options).csv(container_metric_path)
    channel_metrics = spark.read.options(**options).csv(channel_metric_path)
    channels = spark.read.options(**options).csv(channels_path)
    channel_mapping = spark.read.options(**options).csv(channel_mapping_path)
    channel_tags = spark.read.options(**options).csv(channel_tags_path)

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
    channel_tags.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver_key_value_store_alias.channel_tags"
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


@pytest.fixture
def key_value_store_alias_with_channel_tags_db(
    spark, setup_key_value_store_alias_db, mock_workspace_client
) -> MeasurementDB:
    """Alias fixture that ALSO carries an EAV ``channel_tags`` table.

    This is the only fixture with both a ``channel_tags`` table (EAV direct channel
    selection) and a ``channel_mapping`` table (alias resolution), exercising the two
    together. The ``channel_tags`` rows mirror ``channel_metrics.channel_name``, so a
    direct ``channel(channel_name="Engine RPM")`` resolves to containers {1, 2} while
    the alias ``engine_speed`` (via ``EngSpd`` in container 3) resolves to {1, 2, 3}.
    """
    schema = "spark_catalog.silver_key_value_store_alias"
    tables = {
        "container_tags": spark.read.table(f"{schema}.container_tags"),
        "container_metrics": spark.read.table(f"{schema}.container_metrics"),
        "channel_metrics": spark.read.table(f"{schema}.channel_metrics"),
        "channels": spark.read.table(f"{schema}.channels"),
        "channel_mapping": spark.read.table(f"{schema}.channel_mapping"),
        "channel_tags": spark.read.table(f"{schema}.channel_tags"),
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture(scope="session")
def unit_conversion_dataframes(spark):
    """Load unit-conversion test CSVs into cached in-memory DataFrames.

    Hands DataFrames directly to MeasurementDB (via ``for_debug``) instead of
    persisting them through Delta — the alias-style write-then-read fixture
    occasionally hit Delta ``ProtocolChangedException`` during macOS test
    runs.  Caching the DataFrames once per session keeps the data stable.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]

    container_tags_path = f"{base_path}/tests/unit/data/key_value_store_csv/container_metrics.csv"
    container_metric_path = f"{base_path}/tests/unit/data/basic_narrow_csv/container_metrics.csv"
    channel_metric_path = (
        f"{base_path}/tests/unit/data/key_value_store_unit_conversion_csv/channel_metrics.csv"
    )
    channels_path = f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv"
    channel_mapping_path = (
        f"{base_path}/tests/unit/data/key_value_store_unit_conversion_csv/channel_mapping.csv"
    )
    unit_conversion_path = (
        f"{base_path}/tests/unit/data/key_value_store_unit_conversion_csv/unit_conversion.csv"
    )

    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}

    def _load(path):
        df = spark.read.options(**options).csv(path).cache()
        df.count()
        return df

    return {
        "container_tags": _load(container_tags_path),
        "container_metrics": _load(container_metric_path),
        "channel_metrics": _load(channel_metric_path),
        "channels": _load(channels_path),
        "channel_mapping": _load(channel_mapping_path),
        "unit_conversion": _load(unit_conversion_path),
    }


@pytest.fixture
def key_value_store_unit_conversion_db(
    unit_conversion_dataframes, mock_workspace_client
) -> MeasurementDB:
    """Return a key-value-store MeasurementDB with unit conversion configured."""
    cfg = MeasurementDBConfig.for_debug(unit_conversion_dataframes)
    cfg.channel_mapping_table = "channel_mapping"
    cfg.unit_conversion_table = "unit_conversion"
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture
def key_value_store_unit_conversion_db_no_table(
    unit_conversion_dataframes, mock_workspace_client
) -> MeasurementDB:
    """Same data as ``key_value_store_unit_conversion_db`` but with
    ``unit_conversion_table=None`` to test the opt-out path."""
    tables = {k: v for k, v in unit_conversion_dataframes.items() if k != "unit_conversion"}
    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    # Explicitly leave unit_conversion_table = None
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture
def poi_integration_db(spark, mock_workspace_client) -> MeasurementDB:
    """MeasurementDB for POI-channel integration tests.

    Loads ``tests/unit/data/poi_integration_csv`` (EAV container_tags + wide
    container_metrics/channel_metrics/channels + channel_mapping + a ``poi``
    table). Timestamps are second-scale so POI ``timestamp_abs`` (cast to epoch
    seconds by Stage P) lands inside the channel sample intervals. Container 3
    has POI rows but no channel data — the POI-only container case.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    d = f"{base_path}/tests/unit/data/poi_integration_csv"
    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}

    def _load(name):
        return spark.read.options(**options).csv(f"{d}/{name}.csv")

    tables = {
        "container_tags": _load("container_tags"),
        "container_metrics": _load("container_metrics"),
        "channel_metrics": _load("channel_metrics"),
        "channels": _load("channels"),
        "channel_mapping": _load("channel_mapping"),
        # timestamp_abs parses to a proper TimestampType via inferSchema
        "poi": _load("poi"),
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    cfg.poi_table = "poi"
    return MeasurementDB(cfg, ws=mock_workspace_client)


@pytest.fixture
def poi_integration_eav_db(spark, mock_workspace_client) -> MeasurementDB:
    """POI fixture in **EAV channel mode** (``channel_tags_table`` set).

    Same POI/channel data as :func:`poi_integration_db`, but with an EAV
    ``channel_tags`` table added. That flips channel matching from wide mode
    (selectors applied to ``channel_metrics`` columns) to EAV mode (selectors
    resolved against ``channel_tags``, the channel-less-container drop happening in
    ``filter_channel_tags`` rather than ``filter_channel_metrics``). Container 3
    still has POI but no channel — the POI-only container must survive regardless
    of channel-matching mode.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    d = f"{base_path}/tests/unit/data/poi_integration_csv"
    options = {"header": "True", "delimiter": ",", "inferSchema": "True"}

    def _load(name):
        return spark.read.options(**options).csv(f"{d}/{name}.csv")

    tables = {
        "container_tags": _load("container_tags"),
        "container_metrics": _load("container_metrics"),
        "channel_metrics": _load("channel_metrics"),
        "channels": _load("channels"),
        "channel_mapping": _load("channel_mapping"),
        "channel_tags": _load("channel_tags"),  # ← enables EAV channel mode
        "poi": _load("poi"),
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    cfg.poi_table = "poi"
    return MeasurementDB(cfg, ws=mock_workspace_client)
