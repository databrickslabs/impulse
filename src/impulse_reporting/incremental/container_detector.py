"""Container upsert detection for incremental processing."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


class ContainerUpsertDetector:
    """
    Detects new and updated containers by comparing silver and gold layers.

    This class compares the `container_metrics` table in the silver layer with
    the `measurement_dimension` table in the gold layer to identify containers
    that need processing.

    Returns None when gold layer doesn't exist (triggers full processing).

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for executing DataFrame operations.

    Examples
    --------
    >>> detector = ContainerUpsertDetector(spark)
    >>> silver_df = spark.read.table("catalog.silver.container_metrics")
    >>> upserted = detector.detect_upserted_containers(
    ...     silver_df, "catalog.gold.measurement_dimension"
    ... )
    >>> if upserted is None:
    ...     print("Full processing required")
    ... elif upserted.isEmpty():
    ...     print("No changes detected, skip processing")
    ... else:
    ...     print(f"Processing {upserted.count()} containers")
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the ContainerUpsertDetector.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session for executing DataFrame operations.
        """
        self.spark = spark

    def detect_upserted_containers(
        self,
        silver_containers_df: DataFrame,
        gold_measurement_dim_table: str,
        silver_last_modified_col: str = "last_modified",
        gold_last_modified_col: str = "last_modified",
    ) -> DataFrame | None:
        """
        Detect containers that need to be upserted using Spark DataFrame operations.

        Identifies both new containers (present in silver but not in gold) and
        updated containers (present in both with newer timestamp in silver).

        Parameters
        ----------
        silver_containers_df : DataFrame
            Container metrics from silver layer. Must contain ``container_id``.
        gold_measurement_dim_table : str
            URI of the gold measurement_dimension table (e.g.,
            "catalog.gold.measurement_dimension").
        silver_last_modified_col : str, optional
            Column name in the silver layer used for freshness comparison.
            Defaults to ``"last_modified"``.
        gold_last_modified_col : str, optional
            Column name in the gold layer used for freshness comparison.
            Defaults to ``"last_modified"``.

        Returns
        -------
        DataFrame | None
            DataFrame containing containers to process (with silver schema).
            Returns an empty DataFrame if no containers need processing.
            Returns None if gold table doesn't exist (first run, full processing needed).
        """
        # Check if gold table exists (first run check)
        if not self._table_exists(gold_measurement_dim_table):
            return None  # Signal: full processing needed

        gold_df = self.spark.read.table(gold_measurement_dim_table)

        new_containers = self._identify_new_containers(silver_containers_df, gold_df)
        updated_containers = self._identify_updated_containers(
            silver_containers_df,
            gold_df,
            silver_last_modified_col,
            gold_last_modified_col,
        )

        # Union both DataFrames and remove duplicates by container_id
        # Use dropDuplicates instead of distinct() to avoid set operation issues
        # with MAP type columns (e.g., channel_info)
        upserted = new_containers.unionByName(updated_containers).dropDuplicates(["container_id"])
        return upserted

    def _identify_new_containers(self, silver_df: DataFrame, gold_df: DataFrame) -> DataFrame:
        """
        Identify containers in silver but not in gold using left anti-join.

        Parameters
        ----------
        silver_df : DataFrame
            Container metrics from silver layer.
        gold_df : DataFrame
            Measurement dimension from gold layer.

        Returns
        -------
        DataFrame
            Containers that exist in silver but not in gold.
        """
        return silver_df.join(
            gold_df.select("container_id"),
            on="container_id",
            how="left_anti",
        )

    def _identify_updated_containers(
        self,
        silver_df: DataFrame,
        gold_df: DataFrame,
        silver_last_modified_col: str = "last_modified",
        gold_last_modified_col: str = "last_modified",
    ) -> DataFrame:
        """
        Identify containers updated since last processing using timestamp comparison.

        Uses an inner join with timestamp comparison to find containers where
        the silver timestamp column is newer than the gold timestamp column.
        If either column is missing from its respective DataFrame, returns an
        empty DataFrame (no updated containers detected).

        Parameters
        ----------
        silver_df : DataFrame
            Container metrics from silver layer.
        gold_df : DataFrame
            Measurement dimension from gold layer.
        silver_last_modified_col : str, optional
            Column name in silver used for freshness comparison.
        gold_last_modified_col : str, optional
            Column name in gold used for freshness comparison.

        Returns
        -------
        DataFrame
            Containers that have been updated (silver schema preserved).
        """
        # If either layer is missing the timestamp column, skip update detection
        if (
            silver_last_modified_col not in silver_df.columns
            or gold_last_modified_col not in gold_df.columns
        ):
            return silver_df.limit(0)

        # Preserve silver schema columns
        silver_cols = silver_df.columns

        return (
            silver_df.alias("s")
            .join(
                gold_df.alias("g").select("container_id", gold_last_modified_col),
                on="container_id",
                how="inner",
            )
            .where(col(f"s.{silver_last_modified_col}") > col(f"g.{gold_last_modified_col}"))
            .select([col(f"s.{c}") for c in silver_cols])  # Keep silver schema
        )

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
