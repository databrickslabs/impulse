from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from mda_reporting.aggregations.aggregation_types import AggregationType
from mda_reporting.events.event_types import EventType


@dataclass()
class SinkConfig(ABC):
    """
    Base class for sink configuration.

    Determines the interface of the child classes for configuring data sinks.
    Parameters
    ----------
    table_prefix : str
        Prefix to be used for table names.
    """

    table_prefix: str

    @abstractmethod
    def get_output_uri_fact_table(self, element: AggregationType | EventType) -> str:
        """
        Get the corresponding output URI for the fact table.

        Parameters
        ----------
        element : AggregationType | EventType
            The aggregation or event type to get the URI for.

        Returns
        -------
        str
            The output URI for the fact table.
        """
        pass

    @abstractmethod
    def get_output_uri_dimension_table(self, element: AggregationType | EventType) -> str:
        """
        Get the corresponding output URI for a dimension table.

        Parameters
        ----------
        element : AggregationType | EventType
            The aggregation or event type to get the URI for.

        Returns
        -------
        str
            The output URI for the dimension table.
        """
        pass

    @abstractmethod
    def get_output_uri_measurement_dimensions_table(self) -> str:
        """
        Get the output URI for the measurement dimensions table.

        Returns
        -------
        str
            The output URI for the measurement dimensions table.
        """
        pass


@dataclass()
class UnitySinkConfig(SinkConfig):
    """
    Configuration for Unity Catalog sink.

    Parameters
    ----------
    catalog_name : str
        Name of the Unity Catalog.
    schema_name : str
        Name of the schema within the catalog.
    table_prefix : str
        Prefix to be used for table names.
    """

    catalog_name: str
    schema_name: str

    def get_output_uri_fact_table(self, element: AggregationType | EventType) -> str:
        """
        Get the output URI for a fact table in Unity Catalog format.

        Parameters
        ----------
        element : AggregationType | EventType
            The aggregation or event type to get the URI for.

        Returns
        -------
        str
            The Unity Catalog URI for the fact table.
        """
        uri = f"{self.catalog_name}.{self.schema_name}.{self.table_prefix}_{element.get_fact_table_name()}"
        return uri

    def get_output_uri_dimension_table(self, element: AggregationType | EventType) -> str:
        """
        Get the output URI for the dimension table in Unity Catalog format.

        Parameters
        ----------
        element : AggregationType | EventType
            The aggregation or event type to get the URI for.

        Returns
        -------
        str
            The Unity Catalog URI for the dimension table.
        """
        uri = f"{self.catalog_name}.{self.schema_name}.{self.table_prefix}_{element.get_dimension_table_name()}"
        return uri

    def get_output_uri_measurement_dimensions_table(self) -> str:
        """
        Get the output URI for the measurement dimensions table in Unity Catalog format.
        Returns
        -------
        str
            The Unity Catalog URI for the measurement dimensions table.
        """
        uri = f"{self.catalog_name}.{self.schema_name}.{self.table_prefix}_measurement_dimension"
        return uri


class Sink(ABC):
    """
    Base class for sinks.

    Defines the interface for storing data to various destinations.
    """

    def __init__(self, config: SinkConfig):
        self.config = config

    @abstractmethod
    def store(self, df: DataFrame, uri: str):
        """
        Store a DataFrame to the specified URI.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to store.
        uri : str
            The destination URI where the DataFrame should be stored.

        Returns
        -------
        None
        """
        ...


class UnityCatalogSink(Sink):
    """
    Sink implementation for Unity Catalog.

    Stores data in Delta format to Unity Catalog tables.
    """

    def store(self, df: DataFrame, uri: str, overwrite_schema: bool = True):
        """
        Store the DataFrame to the specified table in Delta format.

        Overwrites existing data in the target table.

        Parameters
        ----------
        df : DataFrame
            The DataFrame to store.
        uri : str
            The Unity Catalog table URI where the DataFrame should be stored.
        overwrite_schema : bool, optional
            Whether to overwrite the schema of the target table, by default True.

        Returns
        -------
        None
        """
        df.write.mode("overwrite").format("delta").option(
            "overwriteSchema", overwrite_schema
        ).saveAsTable(uri)

    def upsert(
        self,
        df: DataFrame,
        uri: str,
        merge_keys: list[str],
        overwrite_schema: bool = True,
    ):
        """
        Upsert DataFrame to target table using Delta MERGE.

        Use for UNCHANGED definitions where merge keys align. Performs an
        update-or-insert operation based on the provided merge keys.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame with records to upsert.
        uri : str
            Target Unity Catalog table URI.
        merge_keys : list[str]
            Columns to use for matching records (e.g., ["container_id", "visual_id", "bin_ID"]).
        overwrite_schema : bool, optional
            Whether to overwrite the schema of the target table, by default True.

        Returns
        -------
        None
        """
        if not self._table_exists(df.sparkSession, uri):
            df.write.format("delta").option("overwriteSchema", overwrite_schema).saveAsTable(uri)
            return

        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

        target = self._resolve_delta_table(df.sparkSession, uri)
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def replace_by_ids(
        self,
        df: DataFrame,
        uri: str,
        id_column: str,
        ids_to_replace: list[int],
        overwrite_schema: bool = True,
    ):
        """
        Atomically replace all records for specified IDs using Delta Lake's replaceWhere.

        This is a single atomic transaction - no intermediate inconsistent state.
        Use for CHANGED definitions where old data structure is incompatible with new.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame with new records to insert.
        uri : str
            Target Unity Catalog table URI.
        id_column : str
            Column containing the entity ID (e.g., "visual_id" for aggregations, "event_id" for events).
        ids_to_replace : list[int]
            List of IDs whose records should be completely replaced.
        overwrite_schema : bool, optional
            Whether to overwrite the schema of the target table, by default True.

        Returns
        -------
        None
        """
        if not self._table_exists(df.sparkSession, uri):
            df.write.format("delta").option("overwriteSchema", overwrite_schema).saveAsTable(uri)
            return

        if not ids_to_replace:
            # No IDs to replace, nothing to do
            return

        # Build replaceWhere condition
        replace_condition = f"{id_column} IN ({','.join(map(str, ids_to_replace))})"

        # Atomic operation: deletes all matching rows and inserts new data in single transaction
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_condition)
            .option("overwriteSchema", overwrite_schema)
            .saveAsTable(uri)
        )

    def _table_exists(self, spark: SparkSession, uri: str) -> bool:
        """
        Check if a table exists in the catalog.

        Parameters
        ----------
        spark : SparkSession
            The Spark session to use for catalog operations.
        uri : str
            Full table URI (e.g., "catalog.schema.table").

        Returns
        -------
        bool
            True if table exists, False otherwise.
        """
        try:
            return spark.catalog.tableExists(uri)
        except Exception:
            return False

    @staticmethod
    def _resolve_delta_table(spark: SparkSession, uri: str):
        """
        Resolve a DeltaTable reference, handling three-part names.

        ``DeltaTable.forName`` in OSS Delta Lake may not support three-part
        names (``catalog.schema.table``).  When that happens, fall back to
        the two-part ``schema.table`` form (which resolves against the
        current/default catalog).

        Parameters
        ----------
        spark : SparkSession
            The Spark session to use.
        uri : str
            Full table URI (e.g., "catalog.schema.table" or "schema.table").

        Returns
        -------
        DeltaTable
            A DeltaTable reference for the given URI.
        """
        from delta.tables import DeltaTable

        try:
            return DeltaTable.forName(spark, uri)
        except Exception:
            parts = uri.split(".")
            if len(parts) == 3:
                return DeltaTable.forName(spark, f"{parts[1]}.{parts[2]}")
            raise


class ReportEntityTransformer:
    """
    Transformer class for report entities.

    Provides methods to transform DataFrames into gold layer data layout.

    Notes
    -----
    For the future we can create multiple transformers for different use cases.
    """

    @staticmethod
    def concat_dataframes(dfs: DataFrame | list[DataFrame]) -> DataFrame:
        """
        Concatenate a list of DataFrames into a single DataFrame.

        Parameters
        ----------
        dfs : DataFrame | list[DataFrame]
            A single DataFrame or list of DataFrames to concatenate.

        Returns
        -------
        DataFrame
            The concatenated DataFrame. If a single DataFrame is provided,
            returns it unchanged.
        """
        if isinstance(dfs, list):
            return reduce(lambda df1, df2: df1.union(df2), dfs)
        else:
            return dfs

    @staticmethod
    def select_relevant_columns(schema: StructType) -> Callable[..., "DataFrame"]:
        """
        Select relevant columns from the DataFrame based on the provided schema.

        Parameters
        ----------
        schema : StructType
            The schema defining which columns to select.

        Returns
        -------
        Callable[..., DataFrame]
            A function that takes a DataFrame and returns a DataFrame with
            only the columns specified in the schema.
        """

        def _(df: DataFrame) -> DataFrame:
            return df.select(*schema.fieldNames())

        return _

    @staticmethod
    def add_meta_information(df: DataFrame) -> DataFrame:
        """
        Add meta information to the DataFrame.

        Adds creation timestamp.

        Parameters
        ----------
        df : DataFrame
            The input DataFrame to enhance with metadata.

        Returns
        -------
        DataFrame
            The DataFrame with added metadata columns including '_created_at'
            timestamp.
        """
        return df.withColumn("_created_at", F.current_timestamp())


class ReportEntityWriter(ABC):
    """
    Base class for report entity writers.

    Defines the interface for writing report entities to various destinations.
    """

    @abstractmethod
    def write(self, df: DataFrame | list[DataFrame], schema: StructType, uri: str):
        """
        Write fact data to the sink.

        Parameters
        ----------
        df : DataFrame | list[DataFrame]
            The DataFrame(s) containing the fact data to write.
        schema : StructType
            The schema to apply to the data.
        uri : str
            The destination URI where the data should be written.

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def extract_fact_schema_and_output_uri(
        self, aggregation_type: AggregationType | EventType
    ) -> tuple[StructType, str]:
        """
        Extract fact schema and output URI for the given aggregation or event type.

        Parameters
        ----------
        aggregation_type : AggregationType | EventType
            The aggregation or event type to extract information for.

        Returns
        -------
        tuple[StructType, str]
            A tuple containing the fact schema and output URI.
        """
        pass

    @abstractmethod
    def extract_metadata_schema_and_output_uri(
        self, aggregation_type: AggregationType
    ) -> tuple[StructType, str]:
        pass


class DefaultReportEntityWriter(ReportEntityWriter):
    """
    Default implementation of ReportEntityWriter.

    Handles writing to sinks and adding meta information to DataFrames.

    Parameters
    ----------
    sink : Sink
        The sink to write data to.
    transformer : ReportEntityTransformer
        The transformer to apply to the data.
    """

    def __init__(self, sink: Sink, transformer: ReportEntityTransformer):
        self.sink = sink
        self.transformer = transformer

    def write(self, df: DataFrame | list[DataFrame], schema: StructType, uri: str):
        """
        Store the DataFrame to the specified URI after transforming it.

        Combines DataFrames, adds metadata, selects relevant columns,
        and stores to the sink.

        Parameters
        ----------
        df : DataFrame | list[DataFrame]
            The DataFrame(s) to write.
        schema : StructType
            The schema to apply to the data.
        uri : str
            The destination URI where the data should be stored.

        Returns
        -------
        None
        """
        df_combined = self.transformer.concat_dataframes(df)
        df_enriched = df_combined.transform(
            self.transformer.select_relevant_columns(schema)
        ).transform(self.transformer.add_meta_information)
        self.sink.store(df_enriched, uri)

    def extract_fact_schema_and_output_uri(
        self, entity_type: AggregationType | EventType
    ) -> tuple[StructType, str]:
        """
        Extract fact schema and output URI for the given aggregation type.

        Parameters
        ----------
        entity_type : AggregationType | EventType
            The aggregation or event type to extract information for.

        Returns
        -------
        tuple[StructType, str]
            A tuple containing the schema and fact table output URI.
        """
        schema = entity_type.get_fact_schema()
        uri = self.sink.config.get_output_uri_fact_table(entity_type)
        return schema, uri

    def extract_metadata_schema_and_output_uri(
        self, entity_type: AggregationType | EventType
    ) -> tuple[StructType, str]:
        """
        Extract metadata schema and output URI for the given aggregation type.

        Parameters
        ----------
        entity_type : AggregationType | EventType
            The aggregation type to extract information for.

        Returns
        -------
        tuple[StructType, str]
            A tuple containing the schema and metadata table output URI.
        """
        schema = entity_type.get_dimension_schema()
        uri = self.sink.config.get_output_uri_dimension_table(entity_type)
        return schema, uri


class ContainerDimensionWriter:
    """Writer for measurement dimensions."""

    def __init__(self, sink: Sink, transformer: ReportEntityTransformer):
        self.sink = sink
        self.transformer = transformer

    def write(self, df: DataFrame, uri: str):
        """
        Write measurement dimensions to the sink.
        Parameters
        ----------
        df : DataFrame
            The DataFrame containing measurement dimensions to write.
        uri : str
            The destination URI where the measurement dimensions should be stored.
        """
        df_enriched = df.transform(self.transformer.add_meta_information)
        self.sink.store(df_enriched, uri)

    def get_output_uri(self) -> str:
        """
        Get the output URI for the measurement dimensions table.
        Returns
        -------
        str
            The output URI for the measurement dimensions table.
        """
        return self.sink.config.get_output_uri_measurement_dimensions_table()


class WriterFactory:
    """
    Factory class to create report entity writers.

    Creates writers based on aggregation or event type.

    Parameters
    ----------
    sink : Sink
        The sink to use for created writers.
    """

    def __init__(self, sink: Sink):
        self.sink = sink
        self.sink_config: SinkConfig = sink.config
        self._default_transformer = ReportEntityTransformer()

    def create_writer(self, element: AggregationType | EventType) -> DefaultReportEntityWriter:
        """
        Get the appropriate writer for the given aggregation or event type.

        Parameters
        ----------
        element : AggregationType | EventType
            The aggregation or event type to create a writer for.

        Returns
        -------
        DefaultReportEntityWriter
            A writer configured for the given element type.

        Raises
        ------
        ValueError
            If the element type is not supported.

        """
        if isinstance(element, AggregationType):
            return DefaultReportEntityWriter(self.sink, self._default_transformer)
        elif isinstance(element, EventType):
            return DefaultReportEntityWriter(self.sink, self._default_transformer)
        else:
            error_msg = f"No writer found for element: {element}. Supported types are: AggregationType and EventType"
            raise ValueError(error_msg)

    def create_container_dimension_writer(self) -> ContainerDimensionWriter:
        """
        Create a writer for measurement dimensions.
        Returns
        -------
        ContainerDimensionWriter
            A writer configured for measurement dimensions.
        """
        return ContainerDimensionWriter(self.sink, self._default_transformer)
