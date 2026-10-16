"""Unit tests for TimeWindowEvent."""

import random
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest

from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    TimeWindowExpression,
    tile_windows,
)
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_reporting.events.container_boundary_event import ContainerBoundaryEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.time_window_event import (
    TimeWindowEvent,
    _explode_windows,
    _window_batches,
)
from tests.conftest import spark  # noqa: F401  (pytest fixture)


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


def test_definition_hash_changes_with_channel_time_frame():
    # The channel time frame decides where the windows lie, so changing the unit or the
    # origin must force a full recompute. It reaches the hash through the expression string.
    def event(unit=None, origin="epoch", container_unit=None) -> TimeWindowEvent:
        e = TimeWindowEvent(name="w", window_length=10000)
        e.set_channel_time(unit, origin, container_unit)
        return e

    unset = TimeWindowEvent(name="w", window_length=10000)
    s, ms, ms_relative = event("s"), event("ms"), event("ms", "container_start")
    ms_from_s = event("ms", container_unit="s")

    assert ms_relative.get_expression().channel_time_unit == "ms"
    assert ms_relative.get_expression().channel_time_origin == "container_start"
    assert "channel_time_origin=container_start" in ms_relative.as_dict()["event_expression"]
    # The defaults keep today's hash.
    assert unset.determine_definition_hash() == event().determine_definition_hash()
    assert ms_from_s.get_expression().container_time_unit == "s"
    hashes = {e.determine_definition_hash() for e in (unset, s, ms, ms_relative, ms_from_s)}
    assert len(hashes) == 5


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


# ---------------------------------------------------------------------------
# _explode_windows: tile_windows on the event fact side (one row per window)
# ---------------------------------------------------------------------------
# Output columns of _window_batches for the test frames.
_COLUMNS = ["k", "event_name", "start_ts", "end_ts"]


def _exploded(spark, rows, windows, ts_type="long", id_type="int"):  # noqa: F811
    """_explode_windows over (k, start_ts, stop_ts) rows, as {(k, event_name): windows}."""
    # One partition, so all rows reach the function in one Arrow batch.
    df = spark.createDataFrame(
        rows, f"k {id_type}, start_ts {ts_type}, stop_ts {ts_type}"
    ).coalesce(1)
    out = _explode_windows(
        df, id_col="k", start_col="start_ts", stop_col="stop_ts", windows=windows
    )
    grouped = {}
    for r in out.collect():
        grouped.setdefault((r.k, r.event_name), []).append([r.start_ts, r.end_ts])
    return out, grouped


def test_explode_windows_edge_cases(spark):  # noqa: F811
    rows = [
        (0, 0, 100),  # exact multiple -> 10 windows
        (1, 0, 105),  # short final window clamped to stop
        (2, 1000, 1010),  # span == W -> 1 window
        (3, 1000, 1001),  # span < W -> 1 clamped window
        (4, 50, 50),  # stop == start -> none
        (5, 60, 50),  # stop < start -> none
        (6, None, 50),  # null bound -> none
        (7, 0, None),
    ]
    out, w = _exploded(spark, rows, [("tw", 10.0, MAX_WINDOWS_PER_CONTAINER)])

    assert out.schema.simpleString() == (
        "struct<k:int,event_name:string,start_ts:double,end_ts:double>"
    )
    assert w[(0, "tw")] == [[float(s), float(s + 10)] for s in range(0, 100, 10)]
    assert w[(1, "tw")][-1] == [100.0, 105.0] and len(w[(1, "tw")]) == 11
    assert w[(2, "tw")] == [[1000.0, 1010.0]]
    assert w[(3, "tw")] == [[1000.0, 1001.0]]
    assert set(w) == {(k, "tw") for k in range(4)}


def test_explode_windows_non_finite_bounds_yield_no_windows(spark):  # noqa: F811
    nan, inf = float("nan"), float("inf")
    rows = [(0, 0.0, nan), (1, nan, 10.0), (2, nan, nan), (3, 0.0, inf), (4, -inf, 10.0)]
    out, _ = _exploded(spark, rows, [("tw", 4.0, MAX_WINDOWS_PER_CONTAINER)], ts_type="double")
    assert out.count() == 0


def test_explode_windows_without_any_window_yields_no_rows(spark):  # noqa: F811
    rows = [(0, None, None), (1, 50, 50)]
    out, _ = _exploded(spark, rows, [("tw", 10.0, MAX_WINDOWS_PER_CONTAINER)])
    assert out.count() == 0


def test_explode_windows_raises_beyond_max_windows(spark):  # noqa: F811
    _, ok = _exploded(spark, [(0, 0, 100)], [("tw", 10.0, 10)])
    assert len(ok[(0, "tw")]) == 10
    with pytest.raises(Exception, match="10 windows of length 10.0 .* exceed max_windows=9"):
        _exploded(spark, [(0, 0, 100)], [("tw", 10.0, 9)])


def test_explode_windows_several_events_in_one_pass(spark):  # noqa: F811
    rows = [(0, 0, 105), (1, 1000, 1030)]
    events = [("ten", 10.0, 100), ("seven", 7.5, 100)]
    _, w = _exploded(spark, rows, events)

    assert set(w) == {(k, name) for k, _, _ in rows for name, _, _ in events}
    for k, start, stop in rows:
        for name, length, _ in events:
            expected = [[s, e] for s, e in zip(*tile_windows(start, stop, length), strict=True)]
            assert w[(k, name)] == expected


@pytest.mark.parametrize(
    "id_type, ids", [("int", [7, 8]), ("bigint", [2**40, 2**40 + 1]), ("string", ["c-a", "c-b"])]
)
def test_explode_windows_keeps_container_id_type(spark, id_type, ids):  # noqa: F811
    rows = [(ids[0], 0, 30), (ids[1], 100, 120)]
    out, w = _exploded(spark, rows, [("tw", 10.0, 100)], id_type=id_type)

    assert out.schema["k"].dataType.simpleString() == id_type
    assert w == {
        (ids[0], "tw"): [[0.0, 10.0], [10.0, 20.0], [20.0, 30.0]],
        (ids[1], "tw"): [[100.0, 110.0], [110.0, 120.0]],
    }


def _bounds_batch(ids, starts, stops) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(ids, pa.string()), pa.array(starts, pa.int64()), pa.array(stops, pa.int64())],
        names=["k", "start_ts", "stop_ts"],
    )


def test_window_batches_flush_between_events_and_per_input_batch():
    # Each container gives 4 "a" windows and 2 "b" windows.
    events = [("a", 10.0, 100), ("b", 20.0, 100)]
    first = _bounds_batch(["c0", "c1", "c2"], [0, 100, 200], [40, 140, 240])
    second = _bounds_batch(["c3"], [300], [340])

    def run(batch_windows):
        return list(
            _window_batches(
                iter([first, second]),
                "k",
                "start_ts",
                "stop_ts",
                events,
                _COLUMNS,
                batch_windows,
            )
        )

    batches = run(batch_windows=9)
    # 6 windows after c0, 10 after c1's "a" -> flush, before c1's "b"; c1's "b" and c2
    # flushed at the end of the first input batch; c3 alone from the second one.
    assert [b.num_rows for b in batches] == [10, 8, 6]
    pairs = [
        set(zip(b.column("k").to_pylist(), b.column("event_name").to_pylist(), strict=True))
        for b in batches
    ]
    assert pairs == [
        {("c0", "a"), ("c0", "b"), ("c1", "a")},
        {("c1", "b"), ("c2", "a"), ("c2", "b")},
        {("c3", "a"), ("c3", "b")},
    ]
    assert batches[0].schema.names == ["k", "event_name", "start_ts", "end_ts"]
    assert batches[0].schema.field("k").type == pa.string()
    assert batches[0].column("event_name").to_pylist()[:6] == ["a"] * 4 + ["b"] * 2

    # Many events per container stay bounded: each flush adds at most one event's windows.
    many = [(f"e{i}", 10.0, 100) for i in range(5)]
    sizes = [
        b.num_rows
        for b in _window_batches(
            iter([first]), "k", "start_ts", "stop_ts", many, _COLUMNS, batch_windows=6
        )
    ]
    assert sizes == [8] * 7 + [4]

    # Batching only splits the stream; the rows are those of a single batch per input batch.
    unbatched = pa.Table.from_batches(run(batch_windows=10**9))
    assert pa.Table.from_batches(batches).equals(unbatched)


def _as_list(windows) -> list[tuple[float, float]]:
    """Windows as ordered (start, end) pairs."""
    return [(float(s), float(e)) for s, e in windows]


def _count_mismatch_case(window_length: float) -> tuple[int, int]:
    """Find ns-epoch (start, stop) whose int64 and double spans yield different counts.

    This is exactly the case where exact int64 subtraction and double subtraction disagree
    on the number of windows, so tile_windows must convert to float first on every path.
    """
    base = 1_700_000_000_000_000_000
    for start in range(base, base + 512):
        for k in (1, 3, 5, 7):
            for d in range(-300, 1):
                stop = start + int(k * window_length) + d
                exact = int(np.ceil((stop - start) / window_length))
                rounded = int(np.ceil((float(stop) - float(start)) / window_length))
                if exact != rounded:
                    return start, stop
    raise AssertionError("no int64/double count-mismatch case found")


def _solve_windows(start, stop, window_length: float) -> list[tuple[float, float]]:
    """Windows of the solve: TimeWindowExpression.build on float64 container metrics."""
    config = SolverConfig()
    cache = SimpleNamespace(
        container_metrics={
            config.window_start_col: np.float64(start),
            config.window_stop_col: np.float64(stop),
        }
    )
    return _as_list(TimeWindowExpression(window_length).build(cache).get_data())


def test_event_fact_and_solve_windows_identical_across_input_dtypes(spark):  # noqa: F811
    """Both sides call tile_windows, but receive the bounds differently: the event fact as
    Python ints from the Arrow long columns, the solve as float64 (its container metrics are
    nulled on most rows). For ns epochs beyond 2^53, including a span where int64 and double
    subtraction disagree on the count, both must produce the same windows in the same order,
    also with a null bound in the same Arrow batch."""
    rnd = random.Random(7)
    window_length = 1_000_000_007.0
    cases = []
    for _ in range(60):
        start = rnd.randint(1_600_000_000_000_000_000, 1_800_000_000_000_000_000)
        cases.append(
            (start, start + int(rnd.randint(1, 50) * window_length) + rnd.randint(-600, 600))
        )
    cases.append(_count_mismatch_case(window_length))
    rows = [(k, s, e) for k, (s, e) in enumerate(cases)]
    windows = [("tw", window_length, MAX_WINDOWS_PER_CONTAINER)]

    _, without_nulls = _exploded(spark, rows, windows)
    _, with_null = _exploded(spark, [*rows, (-1, None, None)], windows)

    mismatches = []
    for k, (start, stop) in enumerate(cases):
        solve = _solve_windows(start, stop, window_length)
        assert solve, f"case {k} produced no windows"
        if not (_as_list(without_nulls[(k, "tw")]) == _as_list(with_null[(k, "tw")]) == solve):
            mismatches.append((k, start, stop))
    assert not mismatches, f"event fact / solve window mismatch: {mismatches[:5]}"
