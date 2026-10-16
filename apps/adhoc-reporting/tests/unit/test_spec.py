import pytest
from pydantic import ValidationError

from server.spec import ReportSpec, analyze
from tests.fixtures import demo


def messages(spec: ReportSpec) -> list[str]:
    return [i.message for i in analyze(spec).issues]


def test_dump_uses_schema_alias():
    spec = ReportSpec.model_validate({"name": "oil_temp", "source": {"catalog": "c", "schema": "s"}})
    assert spec.dump()["source"] == {"catalog": "c", "schema": "s"}


@pytest.mark.parametrize("name", ["Oil", "1abc", "oil-temp", "", "a" * 64])
def test_rejects_invalid_report_names(name):
    with pytest.raises(ValidationError):
        ReportSpec(name=name)


@pytest.mark.parametrize("name", ["report", "lambda", "match", "ev_x", "Engine"])
def test_rejects_reserved_signal_names(name):
    with pytest.raises(ValidationError):
        ReportSpec.model_validate({"name": "r", "signals": [{"kind": "channel", "name": name, "channel": {"channel_name": "x"}}]})


def test_demo_fixture_is_valid():
    analysis = analyze(demo.spec())
    assert analysis.issues == []
    assert analysis.signal_order[-2:] == ["avg_temp", "distance_km"]
    assert analysis.code["ev_rpm_band"] == "(eng_rpm > 2000) & (eng_rpm < 5000)"


def test_expression_order_follows_dependencies():
    spec = demo.spec()
    spec.signals.reverse()
    analysis = analyze(spec)
    assert analysis.issues == []
    assert analysis.signal_order.index("avg_temp") > analysis.signal_order.index("amb_temp")


def test_cycle_and_broken_dependency():
    data = demo.spec().dump()
    data["signals"] += [
        {"kind": "expression", "name": "a", "expr": "b + 1"},
        {"kind": "expression", "name": "b", "expr": "a + 1"},
        {"kind": "expression", "name": "bad", "expr": "eng_rpm > 1"},
        {"kind": "expression", "name": "uses_bad", "expr": "bad * 2"},
    ]
    found = messages(ReportSpec.model_validate(data))
    assert any("a depends on itself" in m for m in found)
    assert any("bad must be a signal" in m for m in found)
    assert any("uses_bad uses bad, which has an error" in m for m in found)


def test_event_types_are_enforced():
    spec = demo.spec()
    spec.events[0].expr = "eng_rpm.rising_edges()"
    spec.events[2].expr = "eng_rpm > 1"
    found = messages(spec)
    assert any("rpm_band needs a condition" in m for m in found)
    assert any("milestones needs points in time" in m for m in found)


def test_aggregation_references_and_scopes():
    spec = demo.spec()
    spec.aggregations[0].event = "milestones"
    spec.aggregations[0].bins = [0, 10, 5]
    spec.aggregations[3].signals = ["eng_rpm", "missing"]
    spec.aggregations[5].event = "rpm_band"
    spec.aggregations[1].distance = ""
    found = messages(spec)
    assert any("rpm_hist: event milestones marks points in time" in m for m in found)
    assert any("rpm_hist: bin edges must be strictly increasing" in m for m in found)
    assert any("recording_stats: signal missing does not exist" in m for m in found)
    assert any("values_at_milestones: values are sampled at points in time" in m for m in found)
    assert any("speed_by_distance: choose a distance signal" in m for m in found)


def test_duplicates_and_missing_selection():
    spec = demo.spec()
    spec.signals.append(spec.signals[0].model_copy())
    spec.aggregations.append(spec.aggregations[0].model_copy())
    spec.uuts = []
    spec.window.start = "yesterday"
    found = messages(spec)
    assert "Signal eng_rpm is defined twice" in found
    assert "Aggregation name rpm_hist is already used" in found
    assert "Select at least one unit under test" in found
    assert "The window start is not a valid date and time" in found


def test_window_in_data_format():
    spec = demo.spec()
    spec.window.start, spec.window.stop = "2026-01-09T16:50:39.574Z", "2026-01-09 16:52:32.296"
    assert not [m for m in messages(spec) if "window" in m]
    spec.window.stop = "2026-01-09 16:50:00"
    assert "The window must start before it stops" in messages(spec)


def test_non_finite_bins():
    spec = demo.spec()
    spec.aggregations[0].bins = [0, float("inf")]
    assert any("finite" in m for m in messages(spec))
