from collections.abc import Callable
import importlib


def load_module(module: str):
    return importlib.import_module(module)


def name_of(fn: Callable | type):
    return f"{fn.__module__}.{fn.__name__}"


def resolve_fn(desc: str):
    p = desc.split(".")
    module_name = ".".join(p[:-1])
    module = load_module(module_name)
    return getattr(module, p[-1])


def resolve_cls(cls: str):
    p = cls.split(".")
    module_name = ".".join(p[:-1])
    module = load_module(module_name)
    return getattr(module, p[-1])
