"""Name registry for query solvers, so a report config can select a solver by name.

A solver registers under a name with ``@register_solver(...)`` and is selected with
``query_engine.solver`` in the report config. Selection is by name only: the package that
registers a solver must be imported before the report config is parsed.
"""

from impulse_query_engine.registry import Registration, Registry

from .query_solver import QuerySolver
from .solver_config import SolverConfig

_REGISTRY: Registry[QuerySolver, SolverConfig] = Registry("solver", QuerySolver, SolverConfig)


def register_solver(
    name: str, config_cls: type[SolverConfig] = SolverConfig, *, aliases: tuple[str, ...] = ()
):
    """Register a ``QuerySolver`` subclass under *name* (and *aliases*), configured by *config_cls*.

    ``config_cls`` validates ``query_engine.solver_config`` when this solver is selected.
    Re-registering a name replaces the previous entry.
    """
    return _REGISTRY.register(name, config_cls, aliases=aliases)


def resolve_solver(name: str) -> Registration[QuerySolver, SolverConfig]:
    """Return the registration for *name*; raises ``KeyError`` listing the registered names."""
    return _REGISTRY.resolve(name)
