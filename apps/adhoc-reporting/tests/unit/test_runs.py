from server.runs import grant_script


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
