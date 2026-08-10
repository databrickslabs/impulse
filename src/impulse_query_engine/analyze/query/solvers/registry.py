"""Registry mapping a solver name to its solver class and config class.

The report config selects a solver by **name** (e.g. ``"solver": "DefaultSolver"``).
This registry resolves that name to the concrete :class:`QuerySolver` subclass to
instantiate **and** the :class:`SolverConfig` subclass to build for its
``solver_config`` block — so a customer can ship a solver that reshapes their raw
tables into the Impulse silver schema, plus a config subclass carrying extra
fields, without editing Impulse core.

Any concrete :class:`QuerySolver` subclass is **auto-registered under its class
name** at definition time (via ``QuerySolver.__init_subclass__`` →
:func:`auto_register_solver`), so a customer can select a solver by its class
name without any decorator.  The explicit :func:`register_solver` decorator is
still the way to add a *custom* name, deprecated *aliases*, or a
:class:`SolverConfig` subclass — and it takes precedence over (upgrades) the
auto-registered entry.

Built-in solvers self-register at import time (see the decorator on
``DefaultSolver``).  Customer solvers register themselves the same way from
their own package; importing that package runs the registration side effect.

Selection is by registered name only: there is no fully-qualified-class-path
import path, so a report config can never, on its own, cause Impulse to import
and execute an arbitrary class.  Which solvers exist is governed entirely by
what the driver imports.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from .query_solver import QuerySolver
from .solver_config import SolverConfig

_SolverT = TypeVar("_SolverT", bound=type[QuerySolver])


@dataclass(frozen=True)
class SolverRegistration:
    """A registered solver: the class to instantiate and its config class."""

    solver_cls: type[QuerySolver]
    config_cls: type[SolverConfig]


_REGISTRY: dict[str, SolverRegistration] = {}


def register_solver(
    name: str,
    config_cls: type[SolverConfig] = SolverConfig,
    *,
    aliases: tuple[str, ...] = (),
    overwrite: bool = False,
) -> Callable[[_SolverT], _SolverT]:
    """Class decorator registering the decorated solver under *name*.

    Usage::

        @register_solver("MySolver", MySolverConfig)
        class MySolver(DefaultSolver):
            ...

    Parameters
    ----------
    name : str
        The name used in the report config's ``query_engine.solver``.
    config_cls : type[SolverConfig], optional
        The :class:`SolverConfig` subclass to build for this solver's
        ``solver_config`` block.  Defaults to the base :class:`SolverConfig`
        (for solvers that need no extra config fields).
    aliases : tuple[str, ...], optional
        Additional names resolving to the same registration (e.g. deprecated
        solver names kept for backward compatibility).
    overwrite : bool, optional
        Allow replacing an existing registration.  Defaults to ``False``, which
        raises on a conflicting duplicate.

    Returns
    -------
    Callable
        A decorator that registers and returns the solver class unchanged.

    Raises
    ------
    TypeError
        If the decorated object is not a :class:`QuerySolver` subclass.
    ValueError
        If a name/alias is already registered to a different class and
        *overwrite* is ``False``.
    """

    def decorator(solver_cls: _SolverT) -> _SolverT:
        if not (isinstance(solver_cls, type) and issubclass(solver_cls, QuerySolver)):
            raise TypeError(f"register_solver expects a QuerySolver subclass, got {solver_cls!r}.")
        registration = SolverRegistration(solver_cls, config_cls)
        for key in (name, *aliases):
            existing = _REGISTRY.get(key)
            if existing is not None and existing.solver_cls is not solver_cls and not overwrite:
                raise ValueError(
                    f"Solver {key!r} is already registered to {existing.solver_cls.__name__}; "
                    f"pass overwrite=True to replace it."
                )
            _REGISTRY[key] = registration
        return solver_cls

    return decorator


def auto_register_solver(solver_cls: type[QuerySolver]) -> None:
    """Register *solver_cls* under its class name, if usable and not already named.

    Called by :meth:`QuerySolver.__init_subclass__` so that **every** concrete
    ``QuerySolver`` subclass is selectable by its class name without an explicit
    :func:`register_solver` decorator. Intentionally permissive/idempotent — it
    never raises and never overwrites, so it composes with the explicit decorator
    rather than fighting it:

    - **Abstract classes are skipped** — a subclass that hasn't implemented the
      abstract pipeline methods can't be instantiated, so it isn't offered.
    - **An existing registration for that name wins.** If the name is already
      taken (e.g. the class also carries an explicit ``@register_solver`` with a
      richer ``config_cls``/aliases, or a different class already claimed the
      name), the auto entry is **not** written. Explicit registration therefore
      always takes precedence, and a genuine name clash between two auto-only
      solvers keeps the first-defined one (no surprise overwrite).

    The auto entry always uses the base :class:`SolverConfig`; a solver needing a
    config subclass must use the explicit decorator.

    Parameters
    ----------
    solver_cls : type[QuerySolver]
        The subclass being defined.
    """
    if inspect.isabstract(solver_cls):
        return
    name = solver_cls.__name__
    if name in _REGISTRY:
        return
    _REGISTRY[name] = SolverRegistration(solver_cls, SolverConfig)


def is_registered(name: str) -> bool:
    """Return whether *name* resolves to a registered solver."""
    return name in _REGISTRY


def registered_names() -> list[str]:
    """Return the sorted list of registered solver names and aliases."""
    return sorted(_REGISTRY)


def resolve_registration(name: str) -> SolverRegistration:
    """Resolve a solver *name* to its :class:`SolverRegistration`.

    Parameters
    ----------
    name : str
        A registered solver name or alias.

    Returns
    -------
    SolverRegistration
        The resolved registration.

    Raises
    ------
    KeyError
        If *name* is not registered.  The message lists the known names so a
        missing driver-side import is easy to spot.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown solver {name!r}. Registered solvers: {registered_names()}. "
            f"Import the package that registers it before building the report."
        ) from None
