from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound
from pydantic import ValidationError

from server.datamodel import DataModel, Source, parse, reference, resolve, uuts_query
from server.user import User
from tests.fixtures import demo

TABLES = {
    "container_metrics": "c.s.container_metrics",
    "channel_metrics": "c.s.channel_metrics",
    "channels": "c.s.channels",
    "container_tags": "c.s.container_tags",
    "channel_tags": "c.s.channel_tags",
}


def source(uut_source: str) -> Source:
    return Source(
        catalog="c",
        schema="s",
        tables=TABLES,
        columns={},
        data_type="RAW",
        uut_source=uut_source,
        uut_key="vehicle_key",
        issues=[],
    )


def user_without(tables: set[str], columns: set[str] = frozenset()) -> User:
    def get(full_name: str) -> SimpleNamespace:
        table = full_name.split(".")[-1]
        if table in tables:
            raise NotFound(f"Table {full_name} does not exist")
        return SimpleNamespace(columns=[SimpleNamespace(name=c) for c in demo.COLUMNS[table] if c not in columns])

    return User("user@example.com", SimpleNamespace(tables=SimpleNamespace(get=get)))


def test_physical_uses_inverse_mapping():
    model = DataModel(solver_config={"container_metrics": {"column_name_mapping": {"rec_id": "container_id"}}})
    assert model.physical("container_metrics", "container_id") == "rec_id"
    assert model.physical("container_metrics", "start_dt") == "start_dt"


def test_resolve_without_tag_tables():
    found = resolve(user_without({"container_tags", "channel_tags"}), DataModel(), "c", "wide")
    assert found.issues == []
    assert (found.uut_source, found.uut_key) == ("column", "vehicle_key")
    assert found.tables["container_tags"] is None and found.tables["channel_tags"] is None


def test_missing_uut_id_names_the_setting():
    found = resolve(user_without({"container_tags"}, {"vehicle_key"}), DataModel(), "c", "no_uut")
    assert found.uut_source is None
    assert found.issues == [
        "No unit under test id: container_metrics has no vehicle_key column and there is no container_tags table. "
        "Set uut.key on the Data model page."
    ]


def test_uuts_by_column():
    statement, params = uuts_query(DataModel(), source("column"))
    assert "CAST(cm.`vehicle_key` AS STRING) AS id" in statement
    assert "FROM `c`.`s`.`container_metrics` cm" in statement
    assert params == {}


def test_uuts_by_tag_applies_mapping_and_filters():
    model = DataModel(
        solver_config={"container_tags": {"column_name_mapping": {"entity_id": "container_id"}, "filters": {"parent_id": "p1"}}}
    )
    statement, params = uuts_query(model, source("tag"))
    assert "t.`entity_id`" in statement
    assert "t.`value` IS NOT NULL" in statement
    assert "t.`parent_id` = :container_tags_f0" in statement
    assert params == {"uut_key": "vehicle_key", "container_tags_f0": "p1"}


def test_unknown_config_key_rejected():
    with pytest.raises(ValidationError):
        DataModel.model_validate({"tablez": {}})
    with pytest.raises(ValidationError):
        DataModel.model_validate({"tables": {"poi_channels": "poi_channels"}})


def test_reference_documents_every_setting():
    rows = {r["key"]: r for r in reference()}
    assert all(r["description"] for r in rows.values())
    assert {"tables.channels", "tables.channel_tags", "uut.source", "uut.key", "time_columns.start", "time_columns.stop"} <= rows.keys()
    assert not {"tables", "uut", "time_columns"} & rows.keys()
    assert rows["channel_identity"]["default"] == "[channel_name]"
    assert rows["data_type"]["default"] == "empty"
    assert rows["time_unit"]["default"] == "us"
    assert rows["solver_config"]["example"].startswith("solver_config:")
    assert rows["tables.channels"]["example"] is None
    for r in rows.values():
        if r["example"]:
            parse(r["example"])
