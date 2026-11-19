import pytest

from server import runs
from server.errors import ApiError
from server.runs import grant_script

CONFIG = {"source": {"containers": "c.silver.container_metrics"}, "unity_sink": {"catalog": "c", "schema": "gold"}}
GRANTS = [
    "GRANT USE CATALOG ON CATALOG `c` TO `app-sp`;",
    "GRANT USE SCHEMA, SELECT ON SCHEMA `c`.`silver` TO `app-sp`;",
    "GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `c`.`gold` TO `app-sp`;",
]


@pytest.fixture
def app_principal(monkeypatch):
    monkeypatch.setattr(runs, "runner_identity", lambda: ("app-sp", "Impulse App"))
    monkeypatch.setattr(runs, "runner", lambda: None)


def deny_catalog_grant(monkeypatch, executed):
    def query(user, statement):
        executed.append(statement)
        if statement == GRANTS[0]:
            raise ApiError(403, "missing_privilege", "User does not have MANAGE on Catalog 'c'")

    monkeypatch.setattr(runs.sql, "query", query)


def test_grant_script_covers_source_and_sink():
    tables = ["c1.silver.container_metrics", "c1.silver.channels", "c2.raw.channel_tags"]
    script = grant_script("app-sp", tables, {"catalog": "c1", "schema": "gold"})
    assert script.splitlines() == [
        "GRANT USE CATALOG ON CATALOG `c1` TO `app-sp`;",
        "GRANT USE CATALOG ON CATALOG `c2` TO `app-sp`;",
        "GRANT USE SCHEMA, SELECT ON SCHEMA `c1`.`silver` TO `app-sp`;",
        "GRANT USE SCHEMA, SELECT ON SCHEMA `c2`.`raw` TO `app-sp`;",
        "GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `c1`.`gold` TO `app-sp`;",
    ]


def test_grant_script_merges_shared_schema():
    script = grant_script("sp", ["c.s.container_metrics"], {"catalog": "c", "schema": "s"})
    assert script.splitlines() == [
        "GRANT USE CATALOG ON CATALOG `c` TO `sp`;",
        "GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `c`.`s` TO `sp`;",
    ]


def test_grant_runs_the_generated_statements_as_the_user(monkeypatch, app_principal):
    executed = []
    user = object()
    monkeypatch.setattr(runs.sql, "query", lambda principal, statement: executed.append((principal, statement)))

    runs.grant(user, CONFIG)

    assert executed == [(user, statement) for statement in GRANTS]


def test_grant_continues_when_the_app_already_has_the_denied_privilege(monkeypatch, app_principal):
    executed = []
    deny_catalog_grant(monkeypatch, executed)
    monkeypatch.setattr(runs, "_checks", lambda principal, tables, sink: [])

    runs.grant(object(), CONFIG)

    assert executed == GRANTS


def test_grant_reports_the_denial_when_the_app_still_lacks_access(monkeypatch, app_principal):
    executed = []
    deny_catalog_grant(monkeypatch, executed)
    missing = [{"object": "c.gold", "privilege": "CREATE TABLE", "message": "PERMISSION_DENIED"}]
    monkeypatch.setattr(runs, "_checks", lambda principal, tables, sink: missing)

    with pytest.raises(ApiError, match="MANAGE on Catalog"):
        runs.grant(object(), CONFIG)
    assert executed == GRANTS
