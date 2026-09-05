"""Build context passed to :meth:`QuerySolver.from_config`.

The context carries everything a solver needs to construct itself, decoupled
from the reporting-layer configuration model (``impulse_reporting`` must not be
imported here — the query engine is the lower layer).  It is a frozen dataclass
rather than a Pydantic model because it holds a live ``SparkSession``.

It is also the single extension point for solver construction inputs: new
inputs are added as fields here rather than to every solver's ``from_config``
signature.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .solver_config import RawEncoder, SolverConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class SolverBuildContext:
    """Inputs used to build a :class:`QuerySolver` via ``from_config``.

    Attributes
    ----------
    spark : SparkSession
        Spark session used for query execution.
    solver_config : SolverConfig or None
        Optional per-table column mapping / filter configuration.  Custom
        solvers registered with a ``SolverConfig`` subclass receive that
        subclass instance here (validated at config-parse time).
    is_raw_data : bool
        Whether the input data is raw point data rather than RLE intervals.
    drop_implausible_data : bool
        Whether to drop data points marked as implausible before processing.
    raw_encoder : RawEncoder
        Which encoder converts RAW point data into intervals for solving.
    """

    spark: SparkSession
    solver_config: SolverConfig | None = None
    is_raw_data: bool = False
    drop_implausible_data: bool = False
    raw_encoder: RawEncoder = RawEncoder.RLE
