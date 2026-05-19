"""
Unit tests for histogram bin change scenario using incremental persist_results.

This test simulates the scenario described in the implementation plan:
- Histogram "speed_distribution" changes bins from [0, 50, 100] to [0, 25, 50, 75, 100]
- Old fact data structure is incompatible with new structure
- Uses replaceWhere (atomic) for changed definitions
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from impulse_reporting.persist.report_storage import UnityCatalogSink, UnitySinkConfig
from tests.conftest import spark

# Schema for histogram fact table
HISTOGRAM_FACT_SCHEMA = StructType(
    [
        StructField("container_id", LongType(), True),
        StructField("visual_id", IntegerType(), True),
        StructField("bin_ID", IntegerType(), True),
        StructField("bin_name", StringType(), True),
        StructField("hist_value", DoubleType(), True),
    ]
)


@pytest.fixture
def histogram_fact_table_name():
    return "spark_catalog.gold.test_histogram_fact_bin_change"


@pytest.fixture
def setup_old_histogram_data(spark, histogram_fact_table_name):
    """
    Set up old histogram data with bins [0, 50, 100].

    Old fact data in gold layer:
    | container_id | visual_id | bin_ID | bin_name | hist_value |
    |--------------|-----------|--------|----------|------------|
    | 1            | 12345     | 0      | 0-50     | 100.5      |
    | 1            | 12345     | 1      | 50-100   | 200.3      |
    | 2            | 12345     | 0      | 0-50     | 150.2      |
    | 2            | 12345     | 1      | 50-100   | 180.1      |
    | 1            | 67890     | 0      | 0-50     | 50.0       |
    | 1            | 67890     | 1      | 50-100   | 75.0       |
    """
    old_data = [
        Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=100.5),
        Row(
            container_id=1,
            visual_id=12345,
            bin_ID=1,
            bin_name="50-100",
            hist_value=200.3,
        ),
        Row(container_id=2, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=150.2),
        Row(
            container_id=2,
            visual_id=12345,
            bin_ID=1,
            bin_name="50-100",
            hist_value=180.1,
        ),
        # Another histogram (67890) that won't be changed
        Row(container_id=1, visual_id=67890, bin_ID=0, bin_name="0-50", hist_value=50.0),
        Row(
            container_id=1,
            visual_id=67890,
            bin_ID=1,
            bin_name="50-100",
            hist_value=75.0,
        ),
    ]

    df = spark.createDataFrame(old_data, schema=HISTOGRAM_FACT_SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(histogram_fact_table_name)

    yield histogram_fact_table_name

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {histogram_fact_table_name}")


def test_replace_by_ids_replaces_all_records_for_changed_histogram(
    spark, setup_old_histogram_data, histogram_fact_table_name
):
    """
    Test that replace_by_ids atomically replaces all records for a changed histogram.

    Scenario: Histogram bins changed from [0, 50, 100] to [0, 25, 50, 75, 100]
    - visual_id 12345 has changed bins (needs replaceWhere)
    - visual_id 67890 remains unchanged (should not be affected)
    """
    sink_config = UnitySinkConfig(
        catalog_name="spark_catalog",
        schema_name="gold",
        table_prefix="test",
    )
    sink = UnityCatalogSink(config=sink_config)

    # Verify old data exists
    old_data = spark.read.table(histogram_fact_table_name)
    assert old_data.count() == 6

    # New fact data with changed bins [0, 25, 50, 75, 100]
    new_data_changed = [
        Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=50.2),
        Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=50.3),
        Row(
            container_id=1,
            visual_id=12345,
            bin_ID=2,
            bin_name="50-75",
            hist_value=100.1,
        ),
        Row(
            container_id=1,
            visual_id=12345,
            bin_ID=3,
            bin_name="75-100",
            hist_value=100.2,
        ),
        Row(container_id=2, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=75.1),
        Row(container_id=2, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=75.1),
        Row(container_id=2, visual_id=12345, bin_ID=2, bin_name="50-75", hist_value=90.0),
        Row(
            container_id=2,
            visual_id=12345,
            bin_ID=3,
            bin_name="75-100",
            hist_value=90.1,
        ),
    ]

    new_df = spark.createDataFrame(new_data_changed, schema=HISTOGRAM_FACT_SCHEMA)

    # Execute replace_by_ids for changed histogram (visual_id=12345)
    sink.replace_by_ids(
        df=new_df,
        uri=histogram_fact_table_name,
        id_column="visual_id",
        ids_to_replace=[12345],
    )

    # Verify results
    result_df = spark.read.table(histogram_fact_table_name)

    # Total records: 8 (new for 12345) + 2 (unchanged for 67890) = 10
    assert result_df.count() == 10

    # Verify visual_id=12345 has new bin structure
    hist_12345 = result_df.filter("visual_id = 12345").orderBy("container_id", "bin_ID")
    hist_12345_data = hist_12345.collect()

    assert len(hist_12345_data) == 8
    # Check new bin names are present
    bin_names = {row.bin_name for row in hist_12345_data}
    assert bin_names == {"0-25", "25-50", "50-75", "75-100"}

    # Verify visual_id=67890 was NOT affected (still has old bins)
    hist_67890 = result_df.filter("visual_id = 67890").orderBy("container_id", "bin_ID")
    hist_67890_data = hist_67890.collect()

    assert len(hist_67890_data) == 2
    unchanged_bin_names = {row.bin_name for row in hist_67890_data}
    assert unchanged_bin_names == {"0-50", "50-100"}


def test_replace_by_ids_creates_table_if_not_exists(spark):
    """Test that replace_by_ids creates table if it doesn't exist."""
    table_name = "spark_catalog.gold.test_histogram_fact_new_create"

    try:
        # Make sure table doesn't exist
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        sink_config = UnitySinkConfig(
            catalog_name="spark_catalog",
            schema_name="gold",
            table_prefix="test",
        )
        sink = UnityCatalogSink(config=sink_config)

        new_data = [
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=0,
                bin_name="0-25",
                hist_value=50.2,
            ),
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=1,
                bin_name="25-50",
                hist_value=50.3,
            ),
        ]
        new_df = spark.createDataFrame(new_data, schema=HISTOGRAM_FACT_SCHEMA)

        # Should create table since it doesn't exist
        sink.replace_by_ids(
            df=new_df,
            uri=table_name,
            id_column="visual_id",
            ids_to_replace=[12345],
        )

        # Verify table was created with correct data
        result = spark.read.table(table_name)
        assert result.count() == 2

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_replace_by_ids_with_empty_ids_list_does_nothing(
    spark, setup_old_histogram_data, histogram_fact_table_name
):
    """Test that replace_by_ids with empty ids_to_replace does nothing."""
    sink_config = UnitySinkConfig(
        catalog_name="spark_catalog",
        schema_name="gold",
        table_prefix="test",
    )
    sink = UnityCatalogSink(config=sink_config)

    # Get initial count
    initial_count = spark.read.table(histogram_fact_table_name).count()

    # Empty DataFrame - shouldn't matter since ids_to_replace is empty
    empty_df = spark.createDataFrame([], schema=HISTOGRAM_FACT_SCHEMA)

    sink.replace_by_ids(
        df=empty_df,
        uri=histogram_fact_table_name,
        id_column="visual_id",
        ids_to_replace=[],  # Empty list
    )

    # Count should be unchanged
    final_count = spark.read.table(histogram_fact_table_name).count()
    assert initial_count == final_count


def test_replace_by_ids_with_multiple_visual_ids(spark):
    """Test replacing multiple histograms at once."""
    table_name = "spark_catalog.gold.test_histogram_fact_multi_replace"

    try:
        # Setup initial data with 3 histograms
        initial_data = [
            Row(
                container_id=1,
                visual_id=100,
                bin_ID=0,
                bin_name="old-0",
                hist_value=10.0,
            ),
            Row(
                container_id=1,
                visual_id=100,
                bin_ID=1,
                bin_name="old-1",
                hist_value=20.0,
            ),
            Row(
                container_id=1,
                visual_id=200,
                bin_ID=0,
                bin_name="old-0",
                hist_value=30.0,
            ),
            Row(
                container_id=1,
                visual_id=200,
                bin_ID=1,
                bin_name="old-1",
                hist_value=40.0,
            ),
            Row(
                container_id=1,
                visual_id=300,
                bin_ID=0,
                bin_name="keep-0",
                hist_value=50.0,
            ),
            Row(
                container_id=1,
                visual_id=300,
                bin_ID=1,
                bin_name="keep-1",
                hist_value=60.0,
            ),
        ]
        spark.createDataFrame(initial_data, schema=HISTOGRAM_FACT_SCHEMA).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(table_name)

        sink_config = UnitySinkConfig(
            catalog_name="spark_catalog",
            schema_name="gold",
            table_prefix="test",
        )
        sink = UnityCatalogSink(config=sink_config)

        # New data for visual_ids 100 and 200 (with more bins)
        new_data = [
            Row(
                container_id=1,
                visual_id=100,
                bin_ID=0,
                bin_name="new-0",
                hist_value=1.0,
            ),
            Row(
                container_id=1,
                visual_id=100,
                bin_ID=1,
                bin_name="new-1",
                hist_value=2.0,
            ),
            Row(
                container_id=1,
                visual_id=100,
                bin_ID=2,
                bin_name="new-2",
                hist_value=3.0,
            ),
            Row(
                container_id=1,
                visual_id=200,
                bin_ID=0,
                bin_name="new-0",
                hist_value=4.0,
            ),
            Row(
                container_id=1,
                visual_id=200,
                bin_ID=1,
                bin_name="new-1",
                hist_value=5.0,
            ),
            Row(
                container_id=1,
                visual_id=200,
                bin_ID=2,
                bin_name="new-2",
                hist_value=6.0,
            ),
        ]
        new_df = spark.createDataFrame(new_data, schema=HISTOGRAM_FACT_SCHEMA)

        # Replace both 100 and 200
        sink.replace_by_ids(
            df=new_df,
            uri=table_name,
            id_column="visual_id",
            ids_to_replace=[100, 200],
        )

        result = spark.read.table(table_name)

        # visual_id 100: 3 new records
        # visual_id 200: 3 new records
        # visual_id 300: 2 unchanged records
        # Total: 8 records
        assert result.count() == 8

        # Check visual_id 100 has new bins
        hist_100 = result.filter("visual_id = 100").collect()
        assert len(hist_100) == 3
        assert all("new-" in row.bin_name for row in hist_100)

        # Check visual_id 200 has new bins
        hist_200 = result.filter("visual_id = 200").collect()
        assert len(hist_200) == 3
        assert all("new-" in row.bin_name for row in hist_200)

        # Check visual_id 300 is unchanged
        hist_300 = result.filter("visual_id = 300").collect()
        assert len(hist_300) == 2
        assert all("keep-" in row.bin_name for row in hist_300)

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_upsert_for_unchanged_histogram_definitions(spark):
    """
    Test that upsert (MERGE) works correctly for unchanged histogram definitions.

    Scenario: Adding new containers to an existing histogram without changing bins.
    """
    # Use default database to avoid catalog parsing issues with DeltaTable.forName
    table_name = "test_histogram_fact_upsert"

    try:
        # Setup initial data
        initial_data = [
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=0,
                bin_name="0-50",
                hist_value=100.0,
            ),
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=1,
                bin_name="50-100",
                hist_value=200.0,
            ),
        ]
        spark.createDataFrame(initial_data, schema=HISTOGRAM_FACT_SCHEMA).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(table_name)

        sink_config = UnitySinkConfig(
            catalog_name="spark_catalog",
            schema_name="default",
            table_prefix="test",
        )
        sink = UnityCatalogSink(config=sink_config)

        # New data: update container 1 and add container 2
        new_data = [
            # Update: container 1, same bins, new values
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=0,
                bin_name="0-50",
                hist_value=150.0,
            ),
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=1,
                bin_name="50-100",
                hist_value=250.0,
            ),
            # Insert: new container 2
            Row(
                container_id=2,
                visual_id=12345,
                bin_ID=0,
                bin_name="0-50",
                hist_value=80.0,
            ),
            Row(
                container_id=2,
                visual_id=12345,
                bin_ID=1,
                bin_name="50-100",
                hist_value=120.0,
            ),
        ]
        new_df = spark.createDataFrame(new_data, schema=HISTOGRAM_FACT_SCHEMA)

        # Use upsert (MERGE) - same bins, same structure
        merge_keys = ["container_id", "visual_id", "bin_ID"]
        sink.upsert(new_df, table_name, merge_keys)

        result = spark.read.table(table_name)

        # Should have 4 records (2 updated + 2 inserted)
        assert result.count() == 4

        # Verify container 1 values were updated
        container_1 = result.filter("container_id = 1").orderBy("bin_ID").collect()
        assert container_1[0].hist_value == 150.0
        assert container_1[1].hist_value == 250.0

        # Verify container 2 was inserted
        container_2 = result.filter("container_id = 2").orderBy("bin_ID").collect()
        assert container_2[0].hist_value == 80.0
        assert container_2[1].hist_value == 120.0

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_combined_replace_and_upsert_scenario(spark):
    """
    Test the complete incremental processing scenario with both changed and unchanged definitions.

    Scenario:
    - Histogram 12345: Definition changed (bins changed) -> replaceWhere
    - Histogram 67890: Definition unchanged (new containers only) -> MERGE
    """
    # Use default database to avoid catalog parsing issues with DeltaTable.forName
    fact_table = "test_histogram_fact_combined"

    try:
        # Setup initial data
        initial_data = [
            # Histogram 12345 with old bins [0, 50, 100]
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=0,
                bin_name="0-50",
                hist_value=100.0,
            ),
            Row(
                container_id=1,
                visual_id=12345,
                bin_ID=1,
                bin_name="50-100",
                hist_value=200.0,
            ),
            # Histogram 67890 with bins [0, 50, 100] (won't change)
            Row(
                container_id=1,
                visual_id=67890,
                bin_ID=0,
                bin_name="0-50",
                hist_value=50.0,
            ),
            Row(
                container_id=1,
                visual_id=67890,
                bin_ID=1,
                bin_name="50-100",
                hist_value=75.0,
            ),
        ]
        spark.createDataFrame(initial_data, schema=HISTOGRAM_FACT_SCHEMA).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(fact_table)

        sink_config = UnitySinkConfig(
            catalog_name="spark_catalog",
            schema_name="default",
            table_prefix="test",
        )
        sink = UnityCatalogSink(config=sink_config)

        # Step 1: Replace changed definition (12345) with new bins [0, 25, 50, 75, 100]
        changed_df = spark.createDataFrame(
            [
                Row(
                    container_id=1,
                    visual_id=12345,
                    bin_ID=0,
                    bin_name="0-25",
                    hist_value=50.0,
                ),
                Row(
                    container_id=1,
                    visual_id=12345,
                    bin_ID=1,
                    bin_name="25-50",
                    hist_value=50.0,
                ),
                Row(
                    container_id=1,
                    visual_id=12345,
                    bin_ID=2,
                    bin_name="50-75",
                    hist_value=100.0,
                ),
                Row(
                    container_id=1,
                    visual_id=12345,
                    bin_ID=3,
                    bin_name="75-100",
                    hist_value=100.0,
                ),
            ],
            schema=HISTOGRAM_FACT_SCHEMA,
        )
        sink.replace_by_ids(
            df=changed_df,
            uri=fact_table,
            id_column="visual_id",
            ids_to_replace=[12345],
        )

        # Step 2: Upsert unchanged definition (67890) - add new container
        unchanged_df = spark.createDataFrame(
            [
                # Existing container 1 - update
                Row(
                    container_id=1,
                    visual_id=67890,
                    bin_ID=0,
                    bin_name="0-50",
                    hist_value=55.0,
                ),
                Row(
                    container_id=1,
                    visual_id=67890,
                    bin_ID=1,
                    bin_name="50-100",
                    hist_value=80.0,
                ),
                # New container 2 - insert
                Row(
                    container_id=2,
                    visual_id=67890,
                    bin_ID=0,
                    bin_name="0-50",
                    hist_value=40.0,
                ),
                Row(
                    container_id=2,
                    visual_id=67890,
                    bin_ID=1,
                    bin_name="50-100",
                    hist_value=60.0,
                ),
            ],
            schema=HISTOGRAM_FACT_SCHEMA,
        )
        merge_keys = ["container_id", "visual_id", "bin_ID"]
        sink.upsert(unchanged_df, fact_table, merge_keys)

        # Verify final state
        result = spark.read.table(fact_table)

        # Total records:
        # - 12345: 4 records (new bin structure)
        # - 67890: 4 records (2 updated + 2 new)
        # Total: 8
        assert result.count() == 8

        # Verify 12345 has new bin structure
        hist_12345 = result.filter("visual_id = 12345").collect()
        assert len(hist_12345) == 4
        bin_names_12345 = {row.bin_name for row in hist_12345}
        assert bin_names_12345 == {"0-25", "25-50", "50-75", "75-100"}

        # Verify 67890 has both containers with old bin structure
        hist_67890 = result.filter("visual_id = 67890").orderBy("container_id", "bin_ID").collect()
        assert len(hist_67890) == 4
        bin_names_67890 = {row.bin_name for row in hist_67890}
        assert bin_names_67890 == {"0-50", "50-100"}

        # Check container 1 was updated
        container_1_67890 = [r for r in hist_67890 if r.container_id == 1]
        assert container_1_67890[0].hist_value == 55.0  # Updated value

        # Check container 2 was inserted
        container_2_67890 = [r for r in hist_67890 if r.container_id == 2]
        assert len(container_2_67890) == 2

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fact_table}")
