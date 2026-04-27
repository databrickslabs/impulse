from enum import Enum

from pyspark.sql.types import StructType

from mda_reporting.events.basic_event import BasicEvent
from mda_reporting.events.container_event import ContainerEvent
from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA
from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA


class EventType(Enum):
    """
    Enumeration of available event types.

    Defines the supported event types and their associated metadata
    including table names and schemas.

    Attributes
    ----------
    BASIC_EVENT : BasicEvent
        Basic event type for standard event processing.
    CONTAINER_EVENT : ContainerEvent
        Container event type spanning the full measurement container.
    """

    BASIC_EVENT = BasicEvent
    CONTAINER_EVENT = ContainerEvent

    def get_fact_table_name(self) -> str:
        """
        Get the fact table name for the event type.

        Returns
        -------
        str
            The name of the fact table associated with this event type.

        Raises
        ------
        ValueError
            If the event type is not supported.
        """
        match self:
            case EventType.BASIC_EVENT | EventType.CONTAINER_EVENT:
                return "event_instance_fact"
            case _:
                raise ValueError(f"Unsupported aggregation type: {self}")

    def get_fact_schema(self) -> StructType:
        """
        Get the fact schema for the event type.

        Returns
        -------
        StructType
            The PySpark schema structure for this event type.

        Raises
        ------
        ValueError
            If the event type is not supported.
        """
        match self:
            case EventType.BASIC_EVENT | EventType.CONTAINER_EVENT:
                return EVENT_INSTANCE_FACT_SCHEMA
            case _:
                raise ValueError(f"Unsupported event type: {self}")

    def get_dimension_table_name(self) -> str:
        """
        Get the dimension table name for the event type.

        Returns
        -------
        str
            The name of the dimension table associated with this event type.

        Raises
        ------
        ValueError
            If the event type is not supported.
        """
        match self:
            case EventType.BASIC_EVENT | EventType.CONTAINER_EVENT:
                return "event_dimension"
            case _:
                raise ValueError(f"Unsupported aggregation type: {self}")

    def get_dimension_schema(self) -> StructType:
        """
        Get the dimension schema for the event type.
        Returns
        -------
        StructType
            The PySpark schema structure for this event type.
        Raises
        ------
        ValueError
            If the event type is not supported.
        """
        match self:
            case EventType.BASIC_EVENT | EventType.CONTAINER_EVENT:
                return EVENT_DIMENSION_SCHEMA
            case _:
                raise ValueError(f"Unsupported event type: {self}")

    @classmethod
    def get_any_for_fact_table(cls, table_name: str) -> "EventType":
        """Return the first EventType whose fact table name matches.

        Parameters
        ----------
        table_name : str
            Fact table name to look up.

        Returns
        -------
        EventType

        Raises
        ------
        ValueError
            If no EventType matches the given table name.
        """
        for et in cls:
            if et.get_fact_table_name() == table_name:
                return et
        raise ValueError(f"No EventType found for fact table: {table_name}")

    @classmethod
    def get_any_for_dimension_table(cls, table_name: str) -> "EventType":
        """Return the first EventType whose dimension table name matches.

        Parameters
        ----------
        table_name : str
            Dimension table name to look up.

        Returns
        -------
        EventType

        Raises
        ------
        ValueError
            If no EventType matches the given table name.
        """
        for et in cls:
            if et.get_dimension_table_name() == table_name:
                return et
        raise ValueError(f"No EventType found for dimension table: {table_name}")
