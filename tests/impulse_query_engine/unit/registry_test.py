# pylint: disable=missing-function-docstring
"""Tests for the generic name registry behind ``register_solver`` and ``register_measurement_db``."""

import pytest

from impulse_query_engine.registry import Registry


class Widget: ...


class WidgetConfig: ...


@pytest.fixture
def widgets() -> Registry[Widget, WidgetConfig]:
    # A local registry, so these tests don't touch the global solver / MeasurementDB registries.
    return Registry("widget", Widget, WidgetConfig)


def test_register_returns_class_and_last_registration_wins(widgets):
    class First(Widget): ...

    class Second(Widget): ...

    assert widgets.register("Custom", WidgetConfig)(First) is First
    widgets.register("Custom", WidgetConfig)(Second)
    assert widgets.resolve("Custom").impl_cls is Second


def test_register_with_config_class_and_aliases(widgets):
    class CustomConfig(WidgetConfig): ...

    class Custom(Widget): ...

    widgets.register("Custom", CustomConfig, aliases=("OldCustom",))(Custom)

    assert widgets.resolve("Custom").config_cls is CustomConfig
    assert widgets.resolve("OldCustom") == widgets.resolve("Custom")


@pytest.mark.parametrize("impl", [object, Widget(), "Widget"])
def test_register_rejects_non_subclass(widgets, impl):
    with pytest.raises(TypeError, match="must subclass Widget"):
        widgets.register("Bad", WidgetConfig)(impl)


@pytest.mark.parametrize("config_cls", [dict, WidgetConfig()])
def test_register_rejects_non_subclass_config_class(widgets, config_cls):
    with pytest.raises(TypeError, match="must subclass WidgetConfig"):
        widgets.register("Bad", config_cls)


def test_config_class_is_unchecked_without_config_base():
    registry = Registry("widget", Widget)

    registry.register("Custom", dict)(Widget)

    assert registry.resolve("Custom").config_cls is dict


def test_unknown_name_lists_registered_names(widgets):
    # The error names the unknown entry, lists the registered names, and tells the caller how to
    # fix it (import the registering package before parsing) -- all three are user guidance.
    widgets.register("B", WidgetConfig)(Widget)
    widgets.register("A", WidgetConfig)(Widget)

    with pytest.raises(
        KeyError,
        match=r"Unknown widget 'Missing'; registered: \['A', 'B'\]\. "
        r"Import the package that registers it",
    ):
        widgets.resolve("Missing")
