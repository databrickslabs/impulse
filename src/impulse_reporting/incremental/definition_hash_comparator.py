"""Definition hash comparator for incremental processing."""

from pyspark.sql import SparkSession

from impulse_reporting.aggregations.aggregation import Aggregation
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.events.event import Event


class DefinitionHashComparator:
    """
    Compares current event/aggregation definition hashes against stored values
    in gold layer dimension tables to determine which need full reprocessing.

    This class is used during incremental processing to identify entities whose
    computation definition has changed, requiring full reprocessing of all
    containers for those entities.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for executing DataFrame operations.

    Examples
    --------
    >>> comparator = DefinitionHashComparator(spark)
    >>> changed, unchanged = comparator.group_events_by_hash_change(
    ...     events, "catalog.gold.event_dimension"
    ... )
    >>> # changed events need full reprocessing
    >>> # unchanged events can be processed incrementally
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the DefinitionHashComparator.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session for executing DataFrame operations.
        """
        self.spark = spark

    def _group_by_hash_change(
        self,
        items: list,
        dimension_table: str,
        id_column: str,
    ) -> tuple[list, list]:
        """
        Split entities into changed and unchanged by definition-hash comparison.

        Shared implementation behind :meth:`group_events_by_hash_change`,
        :meth:`group_aggregations_by_hash_change`, and
        :meth:`group_calculated_channels_by_hash_change` — the three differ only
        in the entity type and the id column keyed on in the gold dimension table.

        Compares each item's current ``determine_definition_hash()`` against the
        hash stored under its ``get_id()`` in *dimension_table*. Items whose hash
        differs, or that are absent from gold, are "changed" (need full
        reprocessing of all containers); the rest are "unchanged" (processed
        incrementally). When the gold table does not exist yet, everything is
        "changed".

        Parameters
        ----------
        items : list
            Current entity definitions to check (events, aggregations, or
            calculated channels).
        dimension_table : str
            URI of the gold layer dimension table.
        id_column : str
            Entity id column in the dimension table (e.g. ``"event_id"``,
            ``"visual_id"``, ``"channel_id"``).

        Returns
        -------
        tuple[list, list]
            ``(changed, unchanged)``.
        """
        if not self._table_exists(dimension_table):
            # No gold table exists - everything is "changed" (needs full processing)
            return (items, [])

        stored_hashes = (
            self.spark.read.table(dimension_table).select(id_column, "definition_hash").collect()
        )
        stored_hash_map = {row[id_column]: row.definition_hash for row in stored_hashes}

        changed: list = []
        unchanged: list = []
        for item in items:
            stored_hash = stored_hash_map.get(item.get_id())
            if stored_hash is None or stored_hash != item.determine_definition_hash():
                changed.append(item)
            else:
                unchanged.append(item)

        return (changed, unchanged)

    def group_events_by_hash_change(
        self,
        events: list[Event],
        event_dimension_table: str,
    ) -> tuple[list[Event], list[Event]]:
        """Split events into (changed, unchanged) by definition hash.

        Thin wrapper over :meth:`_group_by_hash_change` keyed on ``event_id``.
        """
        return self._group_by_hash_change(events, event_dimension_table, "event_id")

    def group_aggregations_by_hash_change(
        self,
        aggregations: list[Aggregation],
        dimension_table: str,
    ) -> tuple[list[Aggregation], list[Aggregation]]:
        """Split aggregations into (changed, unchanged) by definition hash.

        Thin wrapper over :meth:`_group_by_hash_change` keyed on ``visual_id``.
        """
        return self._group_by_hash_change(aggregations, dimension_table, "visual_id")

    def group_calculated_channels_by_hash_change(
        self,
        channels: list[CalculatedChannel],
        dimension_table: str,
    ) -> tuple[list[CalculatedChannel], list[CalculatedChannel]]:
        """Split calculated channels into (changed, unchanged) by definition hash.

        Thin wrapper over :meth:`_group_by_hash_change` keyed on ``channel_id``.
        """
        return self._group_by_hash_change(channels, dimension_table, "channel_id")

    def _table_exists(self, table_uri: str) -> bool:
        """
        Check if a table exists in the catalog.

        Parameters
        ----------
        table_uri : str
            Full table URI (e.g., "catalog.schema.table").

        Returns
        -------
        bool
            True if table exists, False otherwise.
        """
        try:
            return self.spark.catalog.tableExists(table_uri)
        except Exception:
            return False
