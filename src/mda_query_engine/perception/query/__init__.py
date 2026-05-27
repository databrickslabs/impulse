"""Query-side machinery that delivers ``object_tracks`` alongside
``channels`` rows into per-container UDFs."""

from .perception_solver import PerceptionSolver

__all__ = ["PerceptionSolver"]
