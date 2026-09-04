from enum import Enum

from pyspark.sql.types import StructType

from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.persist.dimension_schema import CALCULATED_CHANNEL_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import CALCULATED_CHANNEL_FACT_SCHEMA


class ChannelType(Enum):
    """
    Enumeration of available calculated-channel types.

    Mirrors :class:`EventType` / :class:`AggregationType`: maps each enum member
    to its class and resolves the associated gold fact/dimension table names and
    schemas.

    Attributes
    ----------
    CALCULATED_CHANNEL : CalculatedChannel
        Derived-channel type computed via ``solve_calculated_channels``.
    """

    CALCULATED_CHANNEL = CalculatedChannel

    def get_fact_table_name(self) -> str:
        """
        Get the fact table name for the channel type.

        Returns
        -------
        str
            The name of the fact table associated with this channel type.

        Raises
        ------
        ValueError
            If the channel type is not supported.
        """
        match self:
            case ChannelType.CALCULATED_CHANNEL:
                return "calculated_channel_fact"
            case _:
                raise ValueError(f"Unsupported channel type: {self}")

    def get_fact_schema(self) -> StructType:
        """
        Get the fact schema for the channel type.

        Returns
        -------
        StructType
            The PySpark schema structure for this channel type.

        Raises
        ------
        ValueError
            If the channel type is not supported.
        """
        match self:
            case ChannelType.CALCULATED_CHANNEL:
                return CALCULATED_CHANNEL_FACT_SCHEMA
            case _:
                raise ValueError(f"Unsupported channel type: {self}")

    def get_dimension_table_name(self) -> str:
        """
        Get the dimension table name for the channel type.

        Returns
        -------
        str
            The name of the dimension table associated with this channel type.

        Raises
        ------
        ValueError
            If the channel type is not supported.
        """
        match self:
            case ChannelType.CALCULATED_CHANNEL:
                return "calculated_channel_dimension"
            case _:
                raise ValueError(f"Unsupported channel type: {self}")

    def get_metrics_table_name(self) -> str:
        """
        Get the (optional) channel-metrics table name for the channel type.

        This table mirrors the silver-layer ``channel_metrics`` table so the
        calculated-channel fact + metrics pair can serve as an Impulse silver
        source.  Unlike the fact/dimension tables it has **no** fixed schema
        constant: identity/attribute columns are derived dynamically per report
        (see :meth:`CalculatedChannel.determine_channel_metrics`).

        Returns
        -------
        str
            The name of the channel-metrics table associated with this channel type.

        Raises
        ------
        ValueError
            If the channel type is not supported.
        """
        match self:
            case ChannelType.CALCULATED_CHANNEL:
                return "calculated_channel_metrics"
            case _:
                raise ValueError(f"Unsupported channel type: {self}")

    def get_dimension_schema(self) -> StructType:
        """
        Get the dimension schema for the channel type.

        Returns
        -------
        StructType
            The PySpark schema structure for this channel type.

        Raises
        ------
        ValueError
            If the channel type is not supported.
        """
        match self:
            case ChannelType.CALCULATED_CHANNEL:
                return CALCULATED_CHANNEL_DIMENSION_SCHEMA
            case _:
                raise ValueError(f"Unsupported channel type: {self}")

    @classmethod
    def get_any_for_fact_table(cls, table_name: str) -> "ChannelType":
        """Return the first ChannelType whose fact table name matches.

        Parameters
        ----------
        table_name : str
            Fact table name to look up.

        Returns
        -------
        ChannelType

        Raises
        ------
        ValueError
            If no ChannelType matches the given table name.
        """
        for ct in cls:
            if ct.get_fact_table_name() == table_name:
                return ct
        raise ValueError(f"No ChannelType found for fact table: {table_name}")

    @classmethod
    def get_any_for_metrics_table(cls, table_name: str) -> "ChannelType":
        """Return the first ChannelType whose metrics table name matches.

        Parameters
        ----------
        table_name : str
            Metrics table name to look up.

        Returns
        -------
        ChannelType

        Raises
        ------
        ValueError
            If no ChannelType matches the given table name.
        """
        for ct in cls:
            if ct.get_metrics_table_name() == table_name:
                return ct
        raise ValueError(f"No ChannelType found for metrics table: {table_name}")

    @classmethod
    def get_any_for_dimension_table(cls, table_name: str) -> "ChannelType":
        """Return the first ChannelType whose dimension table name matches.

        Parameters
        ----------
        table_name : str
            Dimension table name to look up.

        Returns
        -------
        ChannelType

        Raises
        ------
        ValueError
            If no ChannelType matches the given table name.
        """
        for ct in cls:
            if ct.get_dimension_table_name() == table_name:
                return ct
        raise ValueError(f"No ChannelType found for dimension table: {table_name}")
