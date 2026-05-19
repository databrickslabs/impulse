"""Definition hash comparator for incremental processing."""

from pyspark.sql import SparkSession

from impulse_reporting.aggregations.aggregation import Aggregation
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

    def group_events_by_hash_change(
        self,
        events: list[Event],
        event_dimension_table: str,
    ) -> tuple[list[Event], list[Event]]:
        """
        Group events into changed and unchanged based on definition hash comparison.

        Compares the current definition hash of each event against the stored
        hash in the gold layer dimension table. Events with different hashes
        (or new events not in gold) are considered "changed" and require full
        reprocessing. Events with matching hashes are "unchanged" and can be
        processed incrementally.

        Parameters
        ----------
        events : List[Event]
            Current event definitions to check.
        event_dimension_table : str
            URI of the gold layer event dimension table
            (e.g., "catalog.gold.report_event_dimension").

        Returns
        -------
        Tuple[List[Event], List[Event]]
            A tuple of (changed_events, unchanged_events):
            - changed_events: Events with changed definitions that need full
              reprocessing of all containers.
            - unchanged_events: Events with unchanged definitions that can be
              processed incrementally.
        """

        if not self._table_exists(event_dimension_table):
            # No gold table exists - all events are "changed" (need full processing)
            return (events, [])

        # Load stored hashes from gold layer
        stored_hashes = (
            self.spark.read.table(event_dimension_table)
            .select("event_id", "definition_hash")
            .collect()
        )
        stored_hash_map = {row.event_id: row.definition_hash for row in stored_hashes}

        changed: list[Event] = []
        unchanged: list[Event] = []

        for event in events:
            event_id = event.get_id()
            current_hash = event.determine_definition_hash()
            stored_hash = stored_hash_map.get(event_id)

            if stored_hash is None or stored_hash != current_hash:
                # New event or definition changed
                changed.append(event)
            else:
                # Definition unchanged
                unchanged.append(event)

        return (changed, unchanged)

    def group_aggregations_by_hash_change(
        self,
        aggregations: list[Aggregation],
        dimension_table: str,
    ) -> tuple[list[Aggregation], list[Aggregation]]:
        """
        Group aggregations into changed and unchanged based on definition hash.

        Compares the current definition hash of each aggregation against the
        stored hash in the gold layer dimension table. Aggregations with
        different hashes (or new aggregations not in gold) are considered
        "changed" and require full reprocessing.

        Parameters
        ----------
        aggregations : List[Aggregation]
            Current aggregation definitions to check.
        dimension_table : str
            URI of the gold layer aggregation dimension table
            (e.g., "catalog.gold.report_histogram_dimension").

        Returns
        -------
        Tuple[List[Aggregation], List[Aggregation]]
            A tuple of (changed_aggregations, unchanged_aggregations):
            - changed_aggregations: Aggregations with changed definitions that
              need full reprocessing of all containers.
            - unchanged_aggregations: Aggregations with unchanged definitions
              that can be processed incrementally.
        """

        if not self._table_exists(dimension_table):
            # No gold table exists - all aggregations are "changed"
            return (aggregations, [])

        # Load stored hashes from gold layer
        stored_hashes = (
            self.spark.read.table(dimension_table).select("visual_id", "definition_hash").collect()
        )
        stored_hash_map = {row.visual_id: row.definition_hash for row in stored_hashes}

        changed: list[Aggregation] = []
        unchanged: list[Aggregation] = []

        for agg in aggregations:
            agg_id = agg.get_id()
            current_hash = agg.determine_definition_hash()
            stored_hash = stored_hash_map.get(agg_id)

            if stored_hash is None or stored_hash != current_hash:
                # New aggregation or definition changed
                changed.append(agg)
            else:
                # Definition unchanged
                unchanged.append(agg)

        return (changed, unchanged)

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
