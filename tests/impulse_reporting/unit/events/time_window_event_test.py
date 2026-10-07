"""Unit tests for TimeWindowEvent."""

import pytest

from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    TimeWindowExpression,
)
from impulse_reporting.events.container_boundary_event import ContainerBoundaryEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.time_window_event import TimeWindowEvent


# ---------------------------------------------------------------------------
# Constructor / basic attributes
# ---------------------------------------------------------------------------
def test_init():
    event = TimeWindowEvent(name="w10", window_length=10000)
    assert event.name == "w10"
    assert event.window_length == 10000
    assert event.description is None
    assert isinstance(event.get_expression(), TimeWindowExpression)


def test_init_surfaces_window_length_attribute():
    event = TimeWindowEvent(name="w10", window_length=10000)
    assert event.attributes["window_length"] == "10000.0"


def test_window_length_normalized_across_int_and_float():
    # 10000 and 10000.0 are the same windows: event_dimension must not differ between them.
    a = TimeWindowEvent(name="w", window_length=10000)
    b = TimeWindowEvent(name="w", window_length=10000.0)
    assert isinstance(a.window_length, float)
    assert a.window_length == a.get_expression().window_length
    assert a.attributes == b.attributes
    assert a.as_dict() == b.as_dict()


def test_init_does_not_override_user_window_length_attribute():
    event = TimeWindowEvent(
        name="w10", window_length=10000, attributes={"window_length": "custom"}
    )
    assert event.attributes["window_length"] == "custom"


def test_is_container_boundary_event_but_not_container_event():
    # Routed via the filter pipeline like ContainerEvent, but a sibling (not a subclass), so
    # it gets its own (window-index) instance ids and is not limited to one per report.
    event = TimeWindowEvent(name="w", window_length=10)
    assert isinstance(event, ContainerBoundaryEvent)
    assert not isinstance(event, ContainerEvent)
    assert issubclass(ContainerEvent, ContainerBoundaryEvent)


@pytest.mark.parametrize("bad", [0, -1, -5.5, None, float("inf"), float("-inf"), float("nan")])
def test_non_positive_window_length_raises(bad):
    with pytest.raises(ValueError, match="strictly positive"):
        TimeWindowEvent(name="bad", window_length=bad)


# ---------------------------------------------------------------------------
# get_id / type string
# ---------------------------------------------------------------------------
def test_get_id_is_positive_int_and_deterministic():
    a = TimeWindowEvent(name="same", window_length=10)
    b = TimeWindowEvent(name="same", window_length=99)  # id keys on name only
    assert isinstance(a.get_id(), int) and a.get_id() > 0
    assert a.get_id() == b.get_id()


def test_event_type_str():
    assert TimeWindowEvent(name="w", window_length=10).get_event_type_str() == "TIME_WINDOW_EVENT"


# ---------------------------------------------------------------------------
# definition hash — must move with window_length, stable otherwise
# ---------------------------------------------------------------------------
def test_definition_hash_changes_with_window_length():
    a = TimeWindowEvent(name="w", window_length=10000)
    b = TimeWindowEvent(name="w", window_length=60000)
    assert a.determine_definition_hash() != b.determine_definition_hash()


def test_definition_hash_stable_across_desc_and_attributes():
    a = TimeWindowEvent(name="w", window_length=10000, desc="a", attributes={"k": "1"})
    b = TimeWindowEvent(name="w", window_length=10000, desc="b", attributes={"k": "2"})
    assert a.determine_definition_hash() == b.determine_definition_hash()


def test_definition_hash_stable_across_int_and_float_window_length():
    # 10000 and 10000.0 describe identical windows; the hash must not change between them
    # (otherwise an int/float re-run forces a spurious full recompute in incremental mode).
    a = TimeWindowEvent(name="w", window_length=10000)
    b = TimeWindowEvent(name="w", window_length=10000.0)
    assert a.determine_definition_hash() == b.determine_definition_hash()


def test_definition_hash_changes_with_epoch_unit():
    # epoch_unit decides the unit TIMESTAMP boundaries are tiled in, so flipping it must
    # force a full recompute. It reaches the hash through the expression string.
    unset = TimeWindowEvent(name="w", window_length=10000)
    cleared = TimeWindowEvent(name="w", window_length=10000)
    cleared.set_epoch_unit(None)
    s, ms = TimeWindowEvent(name="w", window_length=10000), TimeWindowEvent(
        name="w", window_length=10000
    )
    s.set_epoch_unit("s")
    ms.set_epoch_unit("ms")

    assert ms.epoch_unit == ms.get_expression().epoch_unit == "ms"
    assert "epoch_unit=ms" in ms.as_dict()["event_expression"]
    assert unset.determine_definition_hash() == cleared.determine_definition_hash()
    assert len({e.determine_definition_hash() for e in (unset, s, ms)}) == 3


# ---------------------------------------------------------------------------
# max_windows_per_container — guard rail, not part of the definition
# ---------------------------------------------------------------------------
def test_max_windows_per_container_default_and_override():
    assert TimeWindowEvent(name="w", window_length=10).max_windows_per_container == (
        MAX_WINDOWS_PER_CONTAINER
    )
    event = TimeWindowEvent(name="w", window_length=10, max_windows_per_container=5)
    assert event.max_windows_per_container == event.get_expression().max_windows == 5


def test_max_windows_per_container_excluded_from_hash():
    a = TimeWindowEvent(name="w", window_length=10)
    b = TimeWindowEvent(name="w", window_length=10, max_windows_per_container=5)
    assert a.determine_definition_hash() == b.determine_definition_hash()


@pytest.mark.parametrize("bad", [0, -1, 2.5, True, None])
def test_invalid_max_windows_per_container_raises(bad):
    with pytest.raises(ValueError, match="^max_windows_per_container must be a positive integer"):
        TimeWindowEvent(name="w", window_length=10, max_windows_per_container=bad)


# ---------------------------------------------------------------------------
# metadata dict shape
# ---------------------------------------------------------------------------
def test_as_dict_shape():
    event = TimeWindowEvent(
        name="w10", window_length=10000, desc="ten second windows", required_channels=["c1"]
    )
    d = event.as_dict()
    assert d["event_type"] == "TIME_WINDOW_EVENT"
    assert d["event_name"] == "w10"
    assert d["event_description"] == "ten second windows"
    assert d["required_channels"] == ["c1"]
    assert d["event_expression"] != "NA"
    assert d["attributes"]["window_length"] == "10000.0"
