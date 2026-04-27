import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from .basic_narrow_solver import BasicNarrowSolver
from .solver_config import SolverConfig


class KeyValueStoreSolver(BasicNarrowSolver):
    """
    Solver for querying container metadata from a narrow/EAV key-value-store table.

    This solver reads container tags from a narrow-format table where each
    attribute is stored as a separate row (entity_id, element_id, value) and
    pivots it to wide format for filtering. It then filters the container_metrics
    table by project_id and optionally joins with the key-value-store when
    MetricExpression filters are present.

    Column names used throughout the solver are driven by a
    :class:`SolverConfig` instance.  When no configuration is provided the
    ``DEFAULT_CONFIG`` dictionary is used.

    Parameters
    ----------
    spark : SparkSession
        Spark session used for query execution.
    project_id : str
        The project ID to filter entities by.
    parent_id : str, optional
        When provided, the tags table is filtered to rows matching this
        parent_id value.  When *None* (default) no parent_id filter is
        applied.
    config : str | dict | SolverConfig | None
        Optional configuration.  Accepts a path to a JSON file (``str``),
        a plain dictionary, or an already-constructed :class:`SolverConfig`.
        When *None* the class-level ``DEFAULT_CONFIG`` is used.
    """

    DEFAULT_CONFIG: dict = {
        "container_id_col": "container_id",
        "channel_id_cols": ["container_id", "channel_id"],
        "channel_data_mapping": {
            "tstart": "tstart",
            "tend": "tend",
            "value": "value",
        },
        "container_meta_data_mapping": {
            "project_id": "project_id",
        },
        "entity_id_col": "entity_id",
    }

    def __init__(self, spark, project_id: str, parent_id: str | None = None, config=None):
        """
        Initialize the KeyValueStoreSolver.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        project_id : str
            The project ID to filter entities by.
        parent_id : str, optional
            When provided, the tags table is filtered to rows matching
            this parent_id value.  When *None* (default) no parent_id
            filter is applied.
        config : str | dict | SolverConfig | None
            Optional solver configuration. If a ``str`` is given it is
            treated as a path to a JSON file.  If a ``dict`` is given it
            is converted via :meth:`SolverConfig.from_dict`.  If *None*,
            ``DEFAULT_CONFIG`` is used.
        """
        parsed_config = self._parse_config(config)
        super().__init__(spark, config=parsed_config)
        self.project_id = project_id
        self.parent_id = parent_id

    # ------------------------------------------------------------------
    # Config parsing
    # ------------------------------------------------------------------

    def _parse_config(self, config: None | dict | str | SolverConfig) -> SolverConfig:
        """
        Parse the provided config into a :class:`SolverConfig`.

        Parameters
        ----------
        config : None | dict | str | SolverConfig
            Raw configuration value.

        Returns
        -------
        SolverConfig
        """
        if config is None:
            return SolverConfig.from_dict(self.DEFAULT_CONFIG)
        if isinstance(config, SolverConfig):
            return config
        if isinstance(config, dict):
            return SolverConfig.from_dict(config)
        if isinstance(config, str):
            return SolverConfig.from_json(config)
        raise TypeError(f"config must be a str, dict, SolverConfig or None, got {type(config)}")

    # ------------------------------------------------------------------
    # Solver stages
    # ------------------------------------------------------------------

    def filter_container_tags(self, spark, query) -> DataFrame:
        """
        Filter container tags from the key-value-store table (narrow/EAV format).

        Reads the narrow-format key-value-store table, filters by project_id,
        and pivots to wide format if tag filters are present.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            The query object containing filters and db info.

        Returns
        -------
        DataFrame
            A DataFrame containing the filtered entity_ids aliased as container_id.
            If no tag filters are present, returns distinct entity_ids.
            Otherwise, returns pivoted data with filter expressions applied.
        """
        container_id_col = self.config.container_id_col
        project_id_col = self.config.project_id_col
        value_col = self.config.value_col
        entity_id_col = self.config.entity_id_col

        # Collect required element_ids from TagExpression filters
        filters = []
        required_elements = []
        for filt in query.filters:
            if isinstance(filt, TagExpression):
                filters.append(filt)
                required_elements.extend(filt.required_tags())
        required_elements = set(required_elements)

        # Read key-value-store table
        tags = query.db.container_tags(self.spark)
        tags = tags.where(F.col(project_id_col) == self.project_id)

        if self.parent_id is not None:
            tags = tags.where(F.col(self.config.parent_id_col) == self.parent_id)

        # If no tag filters, return distinct entity_ids as container_id
        if len(filters) == 0:
            return tags.select(F.col(entity_id_col).alias(container_id_col)).distinct()

        # Filter rows to only required element_ids
        tags = tags.where(F.col("element_id").isin(required_elements))

        # Pivot narrow to wide format
        tags = tags.groupBy(entity_id_col)
        tags = tags.pivot("element_id", list(required_elements)).agg(F.first(value_col))

        # Rename entity_id to container_id
        tags = tags.withColumnRenamed(entity_id_col, container_id_col)

        # Apply filter expressions
        expr = self._build_expr(filters)
        tags = tags.where(expr)

        return tags.select(container_id_col).distinct()

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:
        """
        Filter containers by project_id and optionally by key-value-store tags.

        Filters container_metrics by project_id. If MetricExpression filters
        are present, joins with key-value-store via entity_id to apply tag-level
        filtering.

        Parameters
        ----------
        spark : SparkSession
            Spark session used for query execution.
        query : QueryBuilder
            Query object containing filters and db info.
        container_df : pyspark.sql.DataFrame
            DataFrame containing container information (unused).
        pre_filtered_containers_df : pyspark.sql.DataFrame, optional
            DataFrame containing pre-filtered container information.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame containing filtered container metrics.
        """
        container_id_col = self.config.container_id_col

        # Read container_metrics, join with tags DataFrame
        # Use pre-filtered containers if provided (incremental mode)
        if pre_filtered_containers_df is not None:
            container_metrics = pre_filtered_containers_df
        else:
            container_metrics = query.db.container_metrics(spark)
        return container_metrics.join(
            container_df, how="inner", on=container_id_col
        ).dropDuplicates([self.config.container_id_col])
