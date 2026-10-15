"""Name registries, so a report config can select an implementation by name.

An implementation registers under a name with a registry's ``register`` decorator and is selected
by that name in the report config. Selection is by name only: the package that registers an
implementation must be imported before the report config is parsed.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Registration[I, C]:
    impl_cls: type[I]
    config_cls: type[C]


class Registry[I, C]:
    """Maps names to an implementation class (a subclass of *base*) and its config class.

    *kind* names the implementations in error messages. When *config_base* is given, a config
    class must subclass it.
    """

    def __init__(self, kind: str, base: type[I], config_base: type[C] | None = None):
        self.kind = kind
        self.base = base
        self.config_base = config_base
        self._entries: dict[str, Registration[I, C]] = {}

    def register(self, name: str, config_cls: type[C], *, aliases: tuple[str, ...] = ()):
        """Register a subclass of ``base`` under *name* (and *aliases*), configured by *config_cls*.

        Re-registering a name replaces the previous entry.
        """
        if self.config_base is not None and not _is_subclass(config_cls, self.config_base):
            raise TypeError(f"{config_cls!r} must subclass {self.config_base.__name__}")

        def decorator[T: type](impl_cls: T) -> T:
            if not _is_subclass(impl_cls, self.base):
                raise TypeError(f"{impl_cls!r} must subclass {self.base.__name__}")
            registration = Registration(impl_cls, config_cls)
            for registered_name in (name, *aliases):
                self._entries[registered_name] = registration
            return impl_cls

        return decorator

    def resolve(self, name: str) -> Registration[I, C]:
        """Return the registration for *name*; raises ``KeyError`` listing the registered names."""
        try:
            return self._entries[name]
        except KeyError:
            raise KeyError(
                f"Unknown {self.kind} {name!r}; registered: {sorted(self._entries)}. "
                "Import the package that registers it before parsing the report config."
            ) from None


def _is_subclass(obj: object, base: type) -> bool:
    return isinstance(obj, type) and issubclass(obj, base)
