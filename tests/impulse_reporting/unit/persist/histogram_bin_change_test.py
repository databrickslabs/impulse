"""
Sink-level tests for the incremental fact write (``merge_incremental``).

Exercises the single-MERGE incremental persist against real Delta tables:
- changed definitions (bin structure changes) rewrite an entity's rows;
- unchanged definitions upsert reprocessed containers;
- stale rows left by a shrunk container are deleted-by-source in the same
  transaction (the regression that motivated the unified MERGE);
- rows outside the delete scope are never touched.
"""

import pyspark.sql.functions as F
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

MERGE_KEYS = ["container_id", "visual_id", "bin_ID"]


def _sink(schema_name="gold"):
    return UnityCatalogSink(
        config=UnitySinkConfig(
            catalog_name="spark_catalog",
            schema_name=schema_name,
            table_prefix="test",
        )
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


def test_merge_incremental_changed_definition_replaces_bins(
    spark, setup_old_histogram_data, histogram_fact_table_name
):
    """A changed definition (new bin structure) rewrites all its rows in one MERGE.

    Scenario: Histogram bins changed from [0, 50, 100] to [0, 25, 50, 75, 100].
    - visual_id 12345 has changed bins → delete scope on visual_id.
    - visual_id 67890 remains unchanged → untouched (out of scope).
    """
    sink = _sink()

    old_data = spark.read.table(histogram_fact_table_name)
    assert old_data.count() == 6

    # New fact data with changed bins [0, 25, 50, 75, 100]
    new_data_changed = [
        Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=50.2),
        Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=50.3),
        Row(container_id=1, visual_id=12345, bin_ID=2, bin_name="50-75", hist_value=100.1),
        Row(container_id=1, visual_id=12345, bin_ID=3, bin_name="75-100", hist_value=100.2),
        Row(container_id=2, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=75.1),
        Row(container_id=2, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=75.1),
        Row(container_id=2, visual_id=12345, bin_ID=2, bin_name="50-75", hist_value=90.0),
        Row(container_id=2, visual_id=12345, bin_ID=3, bin_name="75-100", hist_value=90.1),
    ]
    new_df = spark.createDataFrame(new_data_changed, schema=HISTOGRAM_FACT_SCHEMA)

    sink.merge_incremental(
        new_df,
        histogram_fact_table_name,
        MERGE_KEYS,
        delete_conditions=[F.col("target.visual_id").isin([12345])],
    )

    result_df = spark.read.table(histogram_fact_table_name)

    # 8 (new for 12345) + 2 (unchanged for 67890) = 10
    assert result_df.count() == 10

    hist_12345 = result_df.filter("visual_id = 12345").collect()
    assert len(hist_12345) == 8
    assert {row.bin_name for row in hist_12345} == {"0-25", "25-50", "50-75", "75-100"}

    # visual_id 67890 is out of the delete scope → untouched.
    hist_67890 = result_df.filter("visual_id = 67890").collect()
    assert len(hist_67890) == 2
    assert {row.bin_name for row in hist_67890} == {"0-50", "50-100"}


def test_merge_incremental_creates_table_if_not_exists(spark):
    """merge_incremental creates the table when it doesn't exist yet (first write)."""
    table_name = "spark_catalog.gold.test_histogram_fact_new_create"

    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        sink = _sink()

        new_data = [
            Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=50.2),
            Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=50.3),
        ]
        new_df = spark.createDataFrame(new_data, schema=HISTOGRAM_FACT_SCHEMA)

        sink.merge_incremental(
            new_df,
            table_name,
            MERGE_KEYS,
            delete_conditions=[F.col("target.visual_id").isin([12345])],
        )

        result = spark.read.table(table_name)
        assert result.count() == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_merge_incremental_without_delete_conditions_is_pure_upsert(
    spark, setup_old_histogram_data, histogram_fact_table_name
):
    """With no delete_conditions, merge_incremental only updates/inserts — nothing deleted."""
    sink = _sink()
    initial_count = spark.read.table(histogram_fact_table_name).count()

    # Update one existing 12345 row; no delete scope.
    update_df = spark.createDataFrame(
        [Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=999.0)],
        schema=HISTOGRAM_FACT_SCHEMA,
    )
    sink.merge_incremental(update_df, histogram_fact_table_name, MERGE_KEYS, delete_conditions=[])

    result = spark.read.table(histogram_fact_table_name)
    # No rows added or removed — only the one matched row updated.
    assert result.count() == initial_count
    updated = result.filter("container_id = 1 AND visual_id = 12345 AND bin_ID = 0").collect()
    assert updated[0].hist_value == 999.0


def test_merge_incremental_shrink_deletes_stale_rows(spark):
    """Regression: a reprocessed container that now emits FEWER rows leaves no orphans.

    Container 1 shrinks from 3 bins to 1 under an unchanged definition; the two
    surplus rows must be deleted-by-source. Container 2 is not reprocessed and
    must be fully preserved.
    """
    table_name = "spark_catalog.gold.test_histogram_fact_shrink"
    try:
        initial = [
            Row(container_id=1, visual_id=100, bin_ID=0, bin_name="b0", hist_value=10.0),
            Row(container_id=1, visual_id=100, bin_ID=1, bin_name="b1", hist_value=11.0),
            Row(container_id=1, visual_id=100, bin_ID=2, bin_name="b2", hist_value=12.0),
            Row(container_id=2, visual_id=100, bin_ID=0, bin_name="b0", hist_value=20.0),
            Row(container_id=2, visual_id=100, bin_ID=1, bin_name="b1", hist_value=21.0),
            Row(container_id=2, visual_id=100, bin_ID=2, bin_name="b2", hist_value=22.0),
        ]
        spark.createDataFrame(initial, schema=HISTOGRAM_FACT_SCHEMA).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table_name)

        sink = _sink()

        # Reprocess only container 1; it now emits a single bin.
        shrunk = spark.createDataFrame(
            [Row(container_id=1, visual_id=100, bin_ID=0, bin_name="b0", hist_value=99.0)],
            schema=HISTOGRAM_FACT_SCHEMA,
        )
        sink.merge_incremental(
            shrunk,
            table_name,
            MERGE_KEYS,
            delete_conditions=[F.col("target.container_id").isin([1])],
        )

        result = spark.read.table(table_name)
        # Container 1: 3 stale rows → 1 (2 orphans deleted). Container 2: 3 preserved.
        c1 = result.filter("container_id = 1").collect()
        assert len(c1) == 1
        assert c1[0].hist_value == 99.0
        c2 = result.filter("container_id = 2").orderBy("bin_ID").collect()
        assert [r.hist_value for r in c2] == [20.0, 21.0, 22.0]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_merge_incremental_empty_source_deletes_reprocessed_rows(spark):
    """A reprocessed container that now yields ZERO rows still has its stale rows removed."""
    table_name = "spark_catalog.gold.test_histogram_fact_empty_source"
    try:
        initial = [
            Row(container_id=1, visual_id=100, bin_ID=0, bin_name="b0", hist_value=10.0),
            Row(container_id=1, visual_id=100, bin_ID=1, bin_name="b1", hist_value=11.0),
            Row(container_id=2, visual_id=100, bin_ID=0, bin_name="b0", hist_value=20.0),
        ]
        spark.createDataFrame(initial, schema=HISTOGRAM_FACT_SCHEMA).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table_name)

        sink = _sink()
        empty = spark.createDataFrame([], schema=HISTOGRAM_FACT_SCHEMA)
        sink.merge_incremental(
            empty,
            table_name,
            MERGE_KEYS,
            delete_conditions=[F.col("target.container_id").isin([1])],
        )

        result = spark.read.table(table_name)
        assert result.filter("container_id = 1").count() == 0
        assert result.filter("container_id = 2").count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_upsert_for_unchanged_histogram_definitions(spark):
    """
    Test that upsert (MERGE) works correctly for unchanged histogram definitions.

    ``upsert`` remains the primitive for dimension tables; it updates matched rows
    and inserts new ones (no delete-by-source).
    """
    # Use default database to avoid catalog parsing issues with DeltaTable.forName
    table_name = "test_histogram_fact_upsert"

    try:
        initial_data = [
            Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=100.0),
            Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="50-100", hist_value=200.0),
        ]
        spark.createDataFrame(initial_data, schema=HISTOGRAM_FACT_SCHEMA).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(table_name)

        sink = _sink(schema_name="default")

        # New data: update container 1 and add container 2
        new_data = [
            Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=150.0),
            Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="50-100", hist_value=250.0),
            Row(container_id=2, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=80.0),
            Row(container_id=2, visual_id=12345, bin_ID=1, bin_name="50-100", hist_value=120.0),
        ]
        new_df = spark.createDataFrame(new_data, schema=HISTOGRAM_FACT_SCHEMA)

        sink.upsert(new_df, table_name, MERGE_KEYS)

        result = spark.read.table(table_name)
        assert result.count() == 4

        container_1 = result.filter("container_id = 1").orderBy("bin_ID").collect()
        assert container_1[0].hist_value == 150.0
        assert container_1[1].hist_value == 250.0

        container_2 = result.filter("container_id = 2").orderBy("bin_ID").collect()
        assert container_2[0].hist_value == 80.0
        assert container_2[1].hist_value == 120.0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_merge_incremental_combined_changed_and_unchanged(spark):
    """One MERGE handles a changed definition and an unchanged, reprocessed one together.

    - Histogram 12345: definition changed (bins change) → scope by visual_id.
    - Histogram 67890: unchanged, container 1 reprocessed + container 2 added → scope
      by reprocessed container ids.
    Both scopes combine into a single ``merge_incremental`` over the shared table.
    """
    fact_table = "test_histogram_fact_combined"
    try:
        initial_data = [
            Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-50", hist_value=100.0),
            Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="50-100", hist_value=200.0),
            Row(container_id=1, visual_id=67890, bin_ID=0, bin_name="0-50", hist_value=50.0),
            Row(container_id=1, visual_id=67890, bin_ID=1, bin_name="50-100", hist_value=75.0),
        ]
        spark.createDataFrame(initial_data, schema=HISTOGRAM_FACT_SCHEMA).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(fact_table)

        sink = _sink(schema_name="default")

        # Changed 12345 (new bins over all containers) + unchanged 67890 (reprocessed
        # container 1, plus new container 2) unioned into a single source.
        combined = spark.createDataFrame(
            [
                Row(container_id=1, visual_id=12345, bin_ID=0, bin_name="0-25", hist_value=50.0),
                Row(container_id=1, visual_id=12345, bin_ID=1, bin_name="25-50", hist_value=50.0),
                Row(container_id=1, visual_id=12345, bin_ID=2, bin_name="50-75", hist_value=100.0),
                Row(
                    container_id=1, visual_id=12345, bin_ID=3, bin_name="75-100", hist_value=100.0
                ),
                Row(container_id=1, visual_id=67890, bin_ID=0, bin_name="0-50", hist_value=55.0),
                Row(container_id=1, visual_id=67890, bin_ID=1, bin_name="50-100", hist_value=80.0),
                Row(container_id=2, visual_id=67890, bin_ID=0, bin_name="0-50", hist_value=40.0),
                Row(container_id=2, visual_id=67890, bin_ID=1, bin_name="50-100", hist_value=60.0),
            ],
            schema=HISTOGRAM_FACT_SCHEMA,
        )
        sink.merge_incremental(
            combined,
            fact_table,
            MERGE_KEYS,
            delete_conditions=[
                F.col("target.container_id").isin([1]),
                F.col("target.visual_id").isin([12345]),
            ],
        )

        result = spark.read.table(fact_table)
        # 12345: 4 (new bins) + 67890: 4 (c1 updated, c2 inserted) = 8
        assert result.count() == 8

        hist_12345 = result.filter("visual_id = 12345").collect()
        assert len(hist_12345) == 4
        assert {row.bin_name for row in hist_12345} == {"0-25", "25-50", "50-75", "75-100"}

        hist_67890 = result.filter("visual_id = 67890").orderBy("container_id", "bin_ID").collect()
        assert len(hist_67890) == 4
        assert {row.bin_name for row in hist_67890} == {"0-50", "50-100"}
        container_1_67890 = [r for r in hist_67890 if r.container_id == 1]
        assert container_1_67890[0].hist_value == 55.0
        container_2_67890 = [r for r in hist_67890 if r.container_id == 2]
        assert len(container_2_67890) == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fact_table}")
