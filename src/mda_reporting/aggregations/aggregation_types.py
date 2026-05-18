from enum import Enum

from pyspark.sql.types import StructType

from mda_reporting.aggregations.histogram import Histogram
from mda_reporting.aggregations.histogram2d import Histogram2D
from mda_reporting.aggregations.stats_aggregator import StatsAggregator
from mda_reporting.persist.dimension_schema import (
    HISTOGRAM2D_DIMENSION_SCHEMA,
    HISTOGRAM_DIMENSION_SCHEMA,
    STATS_AGGREGATOR_DIMENSION_SCHEMA,
)
from mda_reporting.persist.fact_schema import (
    HISTOGRAM2D_FACT_SCHEMA,
    HISTOGRAM_FACT_SCHEMA,
    STATS_AGGREGATOR_FACT_SCHEMA,
)


class AggregationType(Enum):
    """
    Enumeration of available aggregation types.

    Defines the supported aggregation types and their associated metadata
    including table names and schemas.

    Attributes
    ----------
    HISTOGRAM : Histogram
        Histogram aggregation type for bucketed data analysis.
    HISTOGRAM2D : HISTOGRAM2D
        Bi-dimensional Histogram aggregation type for bucketed data analysis.
    """

    HISTOGRAM = Histogram
    HISTOGRAM2D = Histogram2D
    STATS_AGGREGATOR = StatsAggregator

    def get_fact_table_name(self) -> str:
        """
        Get the fact table name for the aggregation type.

        Returns
        -------
        str
            The name of the fact table associated with this aggregation type.

        Raises
        ------
        ValueError
            If the aggregation type is not supported.
        """
        match self:
            case AggregationType.HISTOGRAM:
                return "histogram_fact"
            case AggregationType.HISTOGRAM2D:
                return "histogram2d_fact"
            case AggregationType.STATS_AGGREGATOR:
                return "stats_aggregator_fact"
            case _:
                raise ValueError(f"Unsupported aggregation type: {self}")

    def get_fact_schema(self) -> StructType:
        """
        Get the fact schema for the aggregation type.

        Returns
        -------
        StructType
            The PySpark schema structure for this aggregation type.

        Raises
        ------
        ValueError
            If the aggregation type is not supported.
        """
        match self:
            case AggregationType.HISTOGRAM:
                return HISTOGRAM_FACT_SCHEMA
            case AggregationType.HISTOGRAM2D:
                return HISTOGRAM2D_FACT_SCHEMA
            case AggregationType.STATS_AGGREGATOR:
                return STATS_AGGREGATOR_FACT_SCHEMA
            case _:
                raise ValueError(f"Unsupported event type: {self}")

    def get_dimension_table_name(self) -> str:
        """
        Get the dimension table name for the aggregation type.

        Returns
        -------
        str
            The name of the dimension table associated with this aggregation type.

        Raises
        ------
        ValueError
            If the aggregation type is not supported.
        """
        match self:
            case AggregationType.HISTOGRAM:
                return "histogram_dimension"
            case AggregationType.HISTOGRAM2D:
                return "histogram2d_dimension"
            case AggregationType.STATS_AGGREGATOR:
                return "stats_aggregator_dimension"
            case _:
                raise ValueError(f"Unsupported aggregation type: {self}")

    def get_dimension_schema(self) -> StructType:
        """
        Get the dimension schema for the aggregation type.
        Returns
        -------
        StructType
            The PySpark schema structure for this aggregation type.

        Raises
        ------
        ValueError
            If the aggregation type is not supported.
        """
        match self:
            case AggregationType.HISTOGRAM:
                return HISTOGRAM_DIMENSION_SCHEMA
            case AggregationType.HISTOGRAM2D:
                return HISTOGRAM2D_DIMENSION_SCHEMA
            case AggregationType.STATS_AGGREGATOR:
                return STATS_AGGREGATOR_DIMENSION_SCHEMA
            case _:
                raise ValueError(f"Unsupported aggregation type: {self}")
