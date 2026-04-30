"""Unit tests for ContainerUpsertDetector."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mda_reporting.incremental.container_detector import ContainerUpsertDetector

# Schema for silver container_metrics
SILVER_CONTAINER_SCHEMA = StructType(
    [
        StructField("container_id", LongType(), False),
        StructField("file_name", StringType(), True),
        StructField("last_modified", TimestampType(), True),
    ]
)

# Schema for gold measurement_dimension
GOLD_MEASUREMENT_DIM_SCHEMA = StructType(
    [
        StructField("container_id", LongType(), False),
        StructField("last_modified", TimestampType(), True),
        StructField("_created_at", TimestampType(), True),
    ]
)

# Base timestamp for test data
BASE_TIMESTAMP = datetime(2025, 1, 1, 12, 0, 0)


@pytest.fixture
def cleanup_test_tables(spark):
    """Cleanup test tables after tests."""
    yield
    # Clean up gold test tables
    spark.sql("DROP TABLE IF EXISTS spark_catalog.gold.test_measurement_dimension PURGE")


def test_returns_none_when_gold_table_not_exists(spark):
    """Test that None is returned when gold table doesn't exist."""
    detector = ContainerUpsertDetector(spark)
    silver_df = spark.createDataFrame(
        [Row(container_id=1, file_name="test.dat", last_modified=datetime.now())],
        schema=SILVER_CONTAINER_SCHEMA,
    )

    result = detector.detect_upserted_containers(silver_df, "spark_catalog.gold.nonexistent_table")

    assert result is None


def test_detects_new_containers(spark, cleanup_test_tables):
    """Test detection of containers in silver but not in gold."""
    detector = ContainerUpsertDetector(spark)

    # Create silver data with 3 containers
    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=BASE_TIMESTAMP),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
        Row(container_id=3, file_name="file3.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    # Create gold data with only container 1
    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)
    gold_df.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.gold.test_measurement_dimension"
    )

    result = detector.detect_upserted_containers(
        silver_df, "spark_catalog.gold.test_measurement_dimension"
    )

    assert result is not None
    result_ids = [row.container_id for row in result.collect()]
    assert sorted(result_ids) == [2, 3]


def test_detects_updated_containers(spark, cleanup_test_tables):
    """Test detection of containers with newer last_modified in silver."""
    detector = ContainerUpsertDetector(spark)
    newer_timestamp = BASE_TIMESTAMP + timedelta(hours=1)

    # Create silver data with updated timestamp for container 1
    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=newer_timestamp),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    # Create gold data with older timestamps
    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
        Row(
            container_id=2,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)
    gold_df.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.gold.test_measurement_dimension"
    )

    result = detector.detect_upserted_containers(
        silver_df, "spark_catalog.gold.test_measurement_dimension"
    )

    assert result is not None
    result_ids = [row.container_id for row in result.collect()]
    assert result_ids == [1]


def test_detects_both_new_and_updated_containers(spark, cleanup_test_tables):
    """Test detection of both new and updated containers."""
    detector = ContainerUpsertDetector(spark)
    newer_timestamp = BASE_TIMESTAMP + timedelta(hours=1)

    # Container 1: updated (newer timestamp)
    # Container 2: unchanged
    # Container 3: new (not in gold)
    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=newer_timestamp),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
        Row(container_id=3, file_name="file3.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
        Row(
            container_id=2,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)
    gold_df.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.gold.test_measurement_dimension"
    )

    result = detector.detect_upserted_containers(
        silver_df, "spark_catalog.gold.test_measurement_dimension"
    )

    assert result is not None
    result_ids = [row.container_id for row in result.collect()]
    assert sorted(result_ids) == [1, 3]


def test_returns_empty_dataframe_when_no_changes(spark, cleanup_test_tables):
    """Test that empty DataFrame is returned when no containers need processing."""
    detector = ContainerUpsertDetector(spark)

    # Silver and gold have same data with same timestamps
    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=BASE_TIMESTAMP),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
        Row(
            container_id=2,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)
    gold_df.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.gold.test_measurement_dimension"
    )

    result = detector.detect_upserted_containers(
        silver_df, "spark_catalog.gold.test_measurement_dimension"
    )

    assert result is not None
    assert result.count() == 0


def test_preserves_silver_schema(spark, cleanup_test_tables):
    """Test that result preserves the silver DataFrame schema."""
    detector = ContainerUpsertDetector(spark)
    newer_timestamp = BASE_TIMESTAMP + timedelta(hours=1)

    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=newer_timestamp),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)
    gold_df.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.gold.test_measurement_dimension"
    )

    result = detector.detect_upserted_containers(
        silver_df, "spark_catalog.gold.test_measurement_dimension"
    )

    assert result is not None
    # Check that result has silver schema columns
    assert set(result.columns) == set(silver_df.columns)
    # Check specific columns
    assert "container_id" in result.columns
    assert "file_name" in result.columns
    assert "last_modified" in result.columns


def test_identify_new_containers_left_anti_join(spark):
    """Test that left anti-join correctly identifies new containers."""
    detector = ContainerUpsertDetector(spark)

    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=BASE_TIMESTAMP),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)

    result = detector._identify_new_containers(silver_df, gold_df)

    result_ids = [row.container_id for row in result.collect()]
    assert result_ids == [2]


def test_identify_updated_containers_timestamp_comparison(spark):
    """Test that timestamp comparison correctly identifies updated containers."""
    detector = ContainerUpsertDetector(spark)
    newer_timestamp = BASE_TIMESTAMP + timedelta(hours=1)

    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=newer_timestamp),
        Row(container_id=2, file_name="file2.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
        Row(
            container_id=2,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)

    result = detector._identify_updated_containers(silver_df, gold_df)

    result_ids = [row.container_id for row in result.collect()]
    assert result_ids == [1]


def test_identify_updated_containers_equal_timestamps_not_included(spark):
    """Test that containers with equal timestamps are not included."""
    detector = ContainerUpsertDetector(spark)

    silver_data = [
        Row(container_id=1, file_name="file1.dat", last_modified=BASE_TIMESTAMP),
    ]
    silver_df = spark.createDataFrame(silver_data, schema=SILVER_CONTAINER_SCHEMA)

    gold_data = [
        Row(
            container_id=1,
            last_modified=BASE_TIMESTAMP,
            _created_at=BASE_TIMESTAMP,
        ),
    ]
    gold_df = spark.createDataFrame(gold_data, schema=GOLD_MEASUREMENT_DIM_SCHEMA)

    result = detector._identify_updated_containers(silver_df, gold_df)

    assert result.count() == 0
