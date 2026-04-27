from unittest.mock import Mock

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from mda_reporting.aggregations.aggregation_types import AggregationType
from mda_reporting.events.event_types import EventType
from mda_reporting.persist.report_storage import (
    DefaultReportEntityWriter,
    ReportEntityTransformer,
    UnitySinkConfig,
    WriterFactory,
)


class TestUnitySinkConfig:
    """Test suite for UnitySinkConfig class."""

    def test_init(self):
        """Test UnitySinkConfig initialization."""
        config = UnitySinkConfig(
            catalog_name="test_catalog", schema_name="test_schema", table_prefix="test_prefix"
        )

        assert config.catalog_name == "test_catalog"
        assert config.schema_name == "test_schema"
        assert config.table_prefix == "test_prefix"

    def test_get_output_uri_fact_table(self):
        """Test fact table URI generation."""
        config = UnitySinkConfig(
            catalog_name="catalog", schema_name="schema", table_prefix="prefix"
        )

        mock_element = Mock()
        mock_element.get_fact_table_name.return_value = "fact_table"

        uri = config.get_output_uri_fact_table(mock_element)

        assert uri == "catalog.schema.prefix_fact_table"

    def test_get_output_uri_dimension_table(self):
        """Test dimension table URI generation."""
        config = UnitySinkConfig(
            catalog_name="catalog", schema_name="schema", table_prefix="prefix"
        )

        mock_element = Mock()
        mock_element.get_dimension_table_name.return_value = "dim_table"

        uri = config.get_output_uri_dimension_table(mock_element)

        assert uri == "catalog.schema.prefix_dim_table"


class TestReportEntityTransformer:
    """Test suite for ReportEntityTransformer class."""

    def test_concat_dataframes_single_df(self, spark):
        """Test concatenation with single DataFrame."""
        # Create a real DataFrame with test data
        data = [("Alice", 25), ("Bob", 30)]
        columns = ["name", "age"]
        df = spark.createDataFrame(data, columns)
        transformer = ReportEntityTransformer()

        result = transformer.concat_dataframes(df)

        assert result is df
        assert result.count() == 2
        assert result.columns == ["name", "age"]

    def test_concat_dataframes_multiple_dfs(self, spark):
        """Test concatenation with multiple DataFrames."""
        transformer = ReportEntityTransformer()

        # Create real DataFrames with test data
        data1 = [("Alice", 25), ("Bob", 30)]
        data2 = [("Charlie", 35), ("Diana", 28)]
        data3 = [("Eve", 32), ("Frank", 27)]
        columns = ["name", "age"]

        df1 = spark.createDataFrame(data1, columns)
        df2 = spark.createDataFrame(data2, columns)
        df3 = spark.createDataFrame(data3, columns)

        result = transformer.concat_dataframes([df1, df2, df3])

        # Verify the result contains all rows from all DataFrames
        assert result.count() == 6
        assert result.columns == ["name", "age"]

        # Collect data to verify all records are present
        result_data = [row.asDict() for row in result.collect()]
        expected_names = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"}
        actual_names = {row["name"] for row in result_data}

        assert actual_names == expected_names

    def test_select_relevant_columns(self, spark):
        """Test column selection based on schema."""
        transformer = ReportEntityTransformer()

        # Create a real DataFrame with test data
        data = [("Alice", 25, "Engineer", "Active"), ("Bob", 30, "Manager", "Inactive")]
        columns = ["name", "age", "job", "status"]
        df = spark.createDataFrame(data, columns)

        mock_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        result = df.transform(transformer.select_relevant_columns(mock_schema))

        # Verify only selected columns are present
        assert result.columns == ["name", "age"]
        assert result.count() == 2


class TestWriterFactory:
    """Test suite for WriterFactory class."""

    def test_write_report_entity(self, spark):
        factory = WriterFactory(Mock())
        writer = factory.create_writer(AggregationType.HISTOGRAM)

        assert writer is not None
        assert type(writer) is DefaultReportEntityWriter

        writer = factory.create_writer(EventType.BASIC_EVENT)

        assert writer is not None
        assert type(writer) is DefaultReportEntityWriter

    def test_create_writer_unsupported_type(self):
        """Test that create_writer raises ValueError for unsupported types."""
        factory = WriterFactory(Mock())

        with pytest.raises(ValueError, match="No writer found for element"):
            factory.create_writer("UNSUPPORTED_TYPE")
