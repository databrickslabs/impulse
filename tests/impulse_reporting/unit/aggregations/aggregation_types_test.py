import pytest
from pyspark.sql.types import StructType

from impulse_reporting.aggregations.aggregation_types import AggregationType


def test_histogram():
    """
    Test get_fact_table_name returns correct table name for HISTOGRAM type.

    Notes
    -----
    Verifies that the HISTOGRAM aggregation type returns the expected
    fact table name.
    """
    result = AggregationType.HISTOGRAM.get_fact_table_name()
    assert result == "histogram_fact"

    result = AggregationType.HISTOGRAM.get_dimension_table_name()
    assert result == "histogram_dimension"


def test_all_aggregation_type_methods_supported():
    """
    Test that all methods work for all AggregationType enum values.

    Notes
    -----
    Loops through all AggregationType enum values and verifies that
    get_fact_table_name(), get_dimension_table_name(), and get_schema()
    return valid values without raising ValueError. This ensures all enum
    values are properly supported in all match statements.

    Raises
    ------
    AssertionError
        If any enum value doesn't return valid results from any method.
    ValueError
        If any enum value is not supported in any match statement.
    """
    for aggregation_type in AggregationType:
        # Test get_fact_table_name()
        fact_table_name = aggregation_type.get_fact_table_name()
        assert isinstance(fact_table_name, str)
        assert len(fact_table_name) > 0

        # Test get_dimension_table_name()
        dimension_table_name = aggregation_type.get_dimension_table_name()
        assert isinstance(dimension_table_name, str)
        assert len(dimension_table_name) > 0

        # Test get_schema()
        schema = aggregation_type.get_fact_schema()
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0


def test_unsupported_aggregation_type():
    """
    Test that unsupported AggregationType raises ValueError.

    Notes
    -----
    This test creates a mock unsupported aggregation type and verifies
    that calling methods raises a ValueError.
    """

    class UnsupportedAggregationType:
        pass

    unsupported_type = UnsupportedAggregationType()
    with pytest.raises(ValueError):
        AggregationType.get_fact_table_name(unsupported_type)

    with pytest.raises(ValueError):
        AggregationType.get_fact_schema(unsupported_type)

    with pytest.raises(ValueError):
        AggregationType.get_dimension_table_name(unsupported_type)

    with pytest.raises(ValueError):
        AggregationType.get_dimension_schema(unsupported_type)
