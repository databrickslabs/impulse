"""Configuration for solver column mappings.

Provides a Pydantic model that maps silver-layer column names to the internal
column names used by the solver classes, making the solvers independent
of a specific data-layer naming convention.
"""

import json

from pydantic import BaseModel


class SolverConfig(BaseModel):
    """
    Configuration for solver column name mappings.

    Attributes
    ----------
    container_id_col : str
        The column name used to identify a container (measurement).
    channel_id_cols : List[str]
        The column names that together uniquely identify a channel.
    channel_data_mapping : Dict[str, str]
        Mapping from internal (solver) column names to silver-layer column
        names for channel data.  Keys are the internal names
        (``"tstart"``, ``"tend"``, ``"value"``); values are the actual
        column names in the source table.
    container_meta_data_mapping : Dict[str, str]
        Mapping from internal (solver) column names to silver-layer column
        names for container metadata.  Keys are the internal names
        (``"project_id"``); values are the actual column names.
    entity_id_col : str
        The column name used to identify an entity in concept-entity /
        key-value-store tables (e.g. ``"entity_id"``).
    """

    container_id_col: str = "container_id"
    channel_id_cols: list[str] = ["container_id", "channel_id"]
    channel_data_mapping: dict[str, str] = {
        "tstart": "tstart",
        "tend": "tend",
        "value": "value",
    }
    container_meta_data_mapping: dict[str, str] = {
        "project_id": "project_id",
    }
    entity_id_col: str = "entity_id"
    parent_id_col: str = "parent_id"

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
        with open(json_path, "r") as f:
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
    # Convenience column-name helpers
    # ------------------------------------------------------------------

    @property
    def channel_id_col(self) -> str:
        """Return the simple channel-id column (last element of *channel_id_cols*)."""
        return self.channel_id_cols[-1]

    @property
    def tstart_col(self) -> str:
        """Silver-layer column name for *tstart*."""
        return self.channel_data_mapping.get("tstart", "tstart")

    @property
    def tend_col(self) -> str:
        """Silver-layer column name for *tend*."""
        return self.channel_data_mapping.get("tend", "tend")

    @property
    def value_col(self) -> str:
        """Silver-layer column name for *value*."""
        return self.channel_data_mapping.get("value", "value")

    @property
    def project_id_col(self) -> str:
        """Silver-layer column name for *project_id*."""
        return self.container_meta_data_mapping.get("project_id", "project_id")

    @property
    def col_map(self) -> dict[str, str]:
        """Short-key → actual-column-name mapping for UDFs and caches."""
        return {
            "cid": self.container_id_col,
            "ch": self.channel_id_col,
            "ts": self.tstart_col,
            "te": self.tend_col,
            "val": self.value_col,
        }
