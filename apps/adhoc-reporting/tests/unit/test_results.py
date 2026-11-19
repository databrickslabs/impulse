from types import SimpleNamespace

from server.results import stats_table


def fact(container, channel, label, value, event_id=None, instance="h", total=0):
    return {
        "container_id": str(container),
        "event_id": event_id,
        "event_instance_id": instance,
        "channel": channel,
        "label": label,
        "value": str(value),
        "total": str(total),
    }


def test_whole_series_lists_each_recording_as_written():
    agg = SimpleNamespace(name="s", kind="statistics", values_unit="", statistics=["min", "max", "mean"])
    facts = [fact(c, ch, label, c * 10 + i, instance=f"h{c}", total=12) for c in (1, 2) for ch in ("RPM", "Temp") for i, label in enumerate(("mean", "max", "min"))]
    table = stats_table(agg, facts, [])
    assert table["scoped"] is False and table["event"] is None and table["more"] is False
    assert table["columns"] == ["min", "max", "mean"]
    assert [(r["recording"], r["instance"], r["signal"]) for r in table["rows"]] == [(1, None, "RPM"), (1, None, "Temp"), (2, None, "RPM"), (2, None, "Temp")]
    assert table["rows"][2]["values"] == {"mean": 20.0, "max": 21.0, "min": 22.0}


def test_event_instances_are_numbered_in_time_order():
    agg = SimpleNamespace(name="s", kind="statistics", values_unit="", statistics=["max"])
    facts = [fact(1, "RPM", "max", 5, "7", "a"), fact(1, "RPM", "max", 9, "7", "b"), fact(2, "RPM", "max", 3, "7", "c")]
    instances = [
        {"event_id": "7", "event_instance_id": "b", "event_name": "rpm_band", "ordinal": "1"},
        {"event_id": "7", "event_instance_id": "a", "event_name": "rpm_band", "ordinal": "2"},
        {"event_id": "7", "event_instance_id": "c", "event_name": "rpm_band", "ordinal": "1"},
    ]
    table = stats_table(agg, facts, instances)
    assert table["scoped"] is True and table["event"] == "rpm_band"
    assert [(r["recording"], r["instance"], r["values"]["max"]) for r in table["rows"]] == [(1, 1, 9.0), (1, 2, 5.0), (2, 1, 3.0)]


def test_point_values_put_signals_in_columns():
    agg = SimpleNamespace(name="p", kind="point_values", values_unit="km/h")
    facts = [fact(1, "Speed", "value", 50, "3", "x", total=3), fact(1, "RPM", "value", 2000, "3", "x", total=3), fact(1, "Speed", "value", 60, "3", "y", total=3)]
    instances = [
        {"event_id": "3", "event_instance_id": "x", "event_name": "milestones", "ordinal": "1"},
        {"event_id": "3", "event_instance_id": "y", "event_name": "milestones", "ordinal": "2"},
    ]
    table = stats_table(agg, facts, instances)
    assert table["columns"] == ["Speed", "RPM"]
    assert [(r["instance"], r["signal"], r["values"]) for r in table["rows"]] == [(1, None, {"Speed": 50.0, "RPM": 2000.0}), (2, None, {"Speed": 60.0})]


def test_truncation_is_flagged():
    agg = SimpleNamespace(name="s", kind="statistics", values_unit="", statistics=["max"])
    assert stats_table(agg, [fact(1, "RPM", "max", 1, total=5000)], [])["more"] is True


def test_string_container_ids_are_kept():
    agg = SimpleNamespace(name="s", kind="statistics", statistics=["max"], values_unit="")
    facts = [fact("MCH-000586_2025-06-01", "VIB", "max", 4.0, total=2), fact("MCH-000579_2025-06-01", "VIB", "max", 9.0, total=2)]
    table = stats_table(agg, facts, [])
    assert [r["recording"] for r in table["rows"]] == ["MCH-000579_2025-06-01", "MCH-000586_2025-06-01"]
