"""Configuration for solver column mappings.

Provides Pydantic models that map silver-layer column names to the internal
column names used by the solver classes, making the solvers independent
of a specific data-layer naming convention.

Each input table has its own :class:`TableConfig` section with an optional
``column_name_mapping`` (physical column → internal name) and ``filters``
(internal column → equality value).

Solvers apply the ``column_name_mapping`` when reading a table to rename
physical columns to internal names.  All subsequent processing — including
filter application — uses the framework-internal column names exposed as
properties on :class:`SolverConfig`.
"""

import json

from pydantic import BaseModel


class TableConfig(BaseModel):
    """Per-table configuration for column renaming and equality filters.

    Attributes
    ----------
    column_name_mapping : dict[str, str]
        Mapping from physical column names on the table to internal
        names used by the solver.  An empty dict means no renaming
        (physical names already match internal names).
    filters : dict[str, str]
        Equality filters applied to the table **after** column renaming.
        Keys are internal column names; values are the literal values
        to match.
    """

    column_name_mapping: dict[str, str] = {}
    filters: dict[str, str] = {}


class SolverConfig(BaseModel):
    """Per-table configuration for solver column name mappings and filters.

    The framework uses a fixed set of internal column names (e.g.
    ``container_id``, ``channel_id``, ``tstart``, ``tend``, ``value``).
    When a silver-layer table uses different physical column names, the
    per-table ``column_name_mapping`` renames them to the internal names
    so that solver code can always reference the same constants.

    Attributes
    ----------
    project_id : str or None
        Optional project identifier applied as a filter on relevant tables
        (container_tags, channel_mapping) by solvers that support it.
    container_tags : TableConfig
        Column mappings and filters for the container tags (narrow/EAV) table.
    container_metrics : TableConfig
        Column mappings and filters for the container metrics table.
    channel_tags : TableConfig
        Column mappings and filters for the channel tags table.
    channel_metrics : TableConfig
        Column mappings and filters for the channel metrics table.
    channel_mapping : TableConfig
        Column mappings and filters for the channel mapping (alias) table.
    channels : TableConfig
        Column mappings and filters for the channel data table.
    """

    project_id: str | None = None

    container_tags: TableConfig = TableConfig()
    container_metrics: TableConfig = TableConfig()
    channel_tags: TableConfig = TableConfig()
    channel_metrics: TableConfig = TableConfig()
    channel_mapping: TableConfig = TableConfig()
    channels: TableConfig = TableConfig()

    # ------------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------------

    @classmethod
    def from_json(cls, json_path: str) -> "SolverConfig":
        """
        Load a SolverConfig from a JSON file.

        Parameters
        ----------
        json_path : str
            Path to the JSON configuration file.

        Returns
        -------
        SolverConfig
            A new SolverConfig instance populated from the file.
        """
        with open(json_path) as f:
            data = json.load(f)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> "SolverConfig":
        """
        Create a SolverConfig from a dictionary.

        This is a convenience alias for ``model_validate(data)``.

        Parameters
        ----------
        data : dict
            Dictionary with configuration keys.

        Returns
        -------
        SolverConfig
            A new SolverConfig instance populated from *data*.
        """
        return cls.model_validate(data)

    # ------------------------------------------------------------------
    # Framework-internal column names (constants)
    # ------------------------------------------------------------------

    @property
    def container_id_col(self) -> str:
        """Internal column name for the container identifier."""
        return "container_id"

    @property
    def channel_id_col(self) -> str:
        """Internal column name for the channel identifier."""
        return "channel_id"

    @property
    def channel_id_cols(self) -> list[str]:
        """Composite key ``[container_id, channel_id]``."""
        return [self.container_id_col, self.channel_id_col]

    @property
    def tstart_col(self) -> str:
        """Internal column name for the start timestamp."""
        return "tstart"

    @property
    def tend_col(self) -> str:
        """Internal column name for the end timestamp."""
        return "tend"

    @property
    def value_col(self) -> str:
        """Internal column name for the signal value on the channels table."""
        return "value"

    @property
    def tag_key_col(self) -> str:
        """Internal column name for the attribute key on the container_tags (EAV) table."""
        return "key"

    @property
    def tag_value_col(self) -> str:
        """Internal column name for the attribute value on the container_tags (EAV) table."""
        return "value"

    @property
    def alias_priority_col(self) -> str:
        """Internal column name for the alias priority on the channel_mapping table."""
        return "priority"

    @property
    def project_id_col(self) -> str:
        """Internal column name for the project identifier."""
        return "project_id"

    @property
    def parent_id_col(self) -> str:
        """Internal column name for the parent/scope identifier."""
        return "parent_id"

    @property
    def col_map(self) -> dict[str, str]:
        """Short-key → internal-column-name mapping for UDFs and caches."""
        return {
            "cid": self.container_id_col,
            "ch": self.channel_id_col,
            "ts": self.tstart_col,
            "te": self.tend_col,
            "val": self.value_col,
        }
