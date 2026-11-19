import os
import uuid

import pytest

from server import catalog, datamodel, sql, store
from server.errors import ApiError
from server.spec import ReportSpec, Window

CATALOG = os.environ.get("IMPULSE_DEMO_CATALOG", "")
SCHEMA = os.environ.get("IMPULSE_DEMO_SCHEMA", "")


def test_sql_runs_as_user(live_user):
    rows = sql.query(live_user, "SELECT current_user() AS me, :x AS echo", {"x": "ok"})
    assert rows == [{"me": live_user.email, "echo": "ok"}]


def test_catalog_browsing(live_user):
    assert CATALOG in catalog.catalogs(live_user)
    assert SCHEMA in catalog.schemas(live_user, CATALOG)


def test_resolve_demo_schema(live_user):
    found = datamodel.resolve(live_user, datamodel.DataModel(), CATALOG, SCHEMA)
    assert found.issues == []
    assert found.data_type == "RAW"
    assert found.uut_source == "column"


@pytest.mark.parametrize("uut_source", ["column", "tag"])
def test_uuts_demo(live_user, uut_source):
    model = datamodel.DataModel(uut={"source": uut_source})
    found = datamodel.resolve(live_user, model, CATALOG, SCHEMA)
    rows = datamodel.uuts(live_user, model, found)
    assert rows and all(r["containers"] > 0 and r["first"] and r["last"] for r in rows)


@pytest.mark.parametrize("uut_source", ["column", "tag"])
def test_channels_demo(live_user, uut_source):
    model = datamodel.DataModel(uut={"source": uut_source})
    found = datamodel.resolve(live_user, model, CATALOG, SCHEMA)
    rows = datamodel.channels(live_user, model, found, ["Seat_Leon"], Window())
    by_name = {r["channel"]["channel_name"]: r for r in rows}
    assert set(by_name) == {"Engine RPM", "Vehicle Speed Sensor", "Ambient Air Temperature", "Intake Air Temperature", "DTC", "DTC_count"}
    rpm = by_name["Engine RPM"]
    assert rpm["unit"] == "RPM" and rpm["containers"] == 3 and rpm["min"] is not None and rpm["max"] > rpm["min"]


def test_channels_respect_window(live_user):
    model = datamodel.DataModel()
    found = datamodel.resolve(live_user, model, CATALOG, SCHEMA)
    assert datamodel.channels(live_user, model, found, ["Seat_Leon"], Window(start="2030-01-01T00:00:00")) == []
    assert datamodel.channels(live_user, model, found, ["no_such_uut"], Window()) == []


def test_missing_table_reported(live_user):
    model = datamodel.DataModel(tables={"channels": "does_not_exist"})
    found = datamodel.resolve(live_user, model, CATALOG, SCHEMA)
    assert any("does_not_exist" in issue for issue in found.issues)


def test_store_round_trip(live_user):
    name = f"zz_test_{uuid.uuid4().hex[:8]}"
    created = store.create(live_user, ReportSpec(name=name, description="live test"))
    try:
        assert created.revision == 1
        loaded = store.get(live_user, name)
        loaded.description = "changed"
        assert store.save(live_user, loaded, expected_revision=1).revision == 2
        with pytest.raises(ApiError) as conflict:
            store.save(live_user, loaded, expected_revision=1)
        assert conflict.value.status == 409
        assert name in [r["name"] for r in store.list_reports(live_user)]
    finally:
        store.delete(live_user, name)
    with pytest.raises(ApiError):
        store.get(live_user, name)


def test_results_of_demo_run(live_user):
    from server import results
    from tests.fixtures import demo

    found = results.results(live_user, demo.spec(catalog=CATALOG, schema=SCHEMA), datamodel.DataModel())
    if not found["available"]:
        pytest.skip("run make smoke-persist once to create the demo gold tables")
    assert found["recordings"] == 3
    by_name = {a["name"]: a for a in found["aggregations"]}
    assert sum(b["value"] for b in by_name["rpm_hist"]["bins"]) == pytest.approx(
        next(e["seconds"] for e in found["events"] if e["name"] == "rpm_band"), rel=1e-6
    )
    assert by_name["rpm_vs_speed"]["cells"] and by_name["recording_stats"]["rows"] and by_name["values_at_milestones"]["rows"]


def test_preflight_reports_missing_access(live_user, monkeypatch):
    from server import codegen, runs
    from tests.fixtures import demo

    monkeypatch.setattr(runs, "runner", lambda: live_user.client)
    monkeypatch.setattr(runs, "runner_identity", lambda: (live_user.email, live_user.email))
    model = datamodel.DataModel()
    config = codegen.config(demo.spec(catalog=CATALOG, schema=SCHEMA), datamodel.resolve(live_user, model, CATALOG, SCHEMA), model)
    assert runs.preflight(live_user, config)["ok"]
    config["source"]["channels_uri"] = f"{CATALOG}.{SCHEMA}.does_not_exist"
    checked = runs.preflight(live_user, config)
    assert not checked["ok"]
    assert [p["privilege"] for p in checked["user"]] == ["SELECT"]
    assert "GRANT USE CATALOG" in checked["grants"]
