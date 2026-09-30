from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

from impulse_query_engine import __version__
from impulse_query_engine.telemetry import verify_workspace_client

from .analyze.query.query_builder import QueryBuilder

if TYPE_CHECKING:
    from .measurement_db import MeasurementDBConfig


class AbstractMeasurementDB(ABC):
    """Read seam contract: returns silver-shaped frames for the solver.

    Implementations are selected by name via ``register_measurement_db`` and built as
    ``db_cls(config, ws)``. The returned frames must match the silver model the solver expects.
    """

    def __init__(self, config: "MeasurementDBConfig", ws: WorkspaceClient):
        self.config = config
        self.ws = verify_workspace_client(ws, "databricks-impulse", __version__)

    @property
    def query(self) -> QueryBuilder:
        return QueryBuilder(db=self)

    @abstractmethod
    def pin_versions(self, spark) -> None:
        """Pin every read table to one Delta snapshot for the run (called by ``Report``)."""

    @abstractmethod
    def container_tags(self, spark) -> DataFrame: ...

    @abstractmethod
    def container_metrics(self, spark) -> DataFrame: ...

    @abstractmethod
    def channel_tags(self, spark) -> DataFrame: ...

    @abstractmethod
    def channel_metrics(self, spark) -> DataFrame: ...

    @abstractmethod
    def channels(self, spark) -> DataFrame: ...

    @abstractmethod
    def has_poi_channels(self) -> bool: ...

    @abstractmethod
    def poi_channels(self, spark) -> DataFrame: ...

    @abstractmethod
    def channel_mapping(self, spark) -> DataFrame: ...

    @abstractmethod
    def unit_conversion(self, spark) -> DataFrame: ...

    @abstractmethod
    def channel_uri(self) -> str | None: ...
