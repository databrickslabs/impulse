import hashlib
import json
import re

import pytest

from server.agent import skills, tools
from server.agent.loop import MAX_TOOL_RESULT

APP = [s for s in skills.catalog().values() if s.layer == "app"]
UPSTREAM = skills.LAYERS[1][1]
TOOL_LIKE = re.compile(r"`((?:add|remove|set|list|get|search|run|validate|preview|goto|load|read|create|open)_[a-z_]+)`")
SKILL_LIKE = re.compile(r"`(impulse(?:-[a-z]+)*)`")
CLI_ERA = ("Makefile", "databricks bundle", "Bash", "Read tool", "Edit tool", "nameda", "01_signal_definitions", "mapping table", "v0.1.2", "%run")


def test_both_layers_present():
    names = set(skills.catalog())
    assert {"impulse-app", "impulse-app-data", "impulse-app-signals", "impulse-app-aggregations", "impulse-app-run"} <= names
    assert len([s for s in skills.catalog().values() if s.layer == "impulse"]) == 10


@pytest.mark.parametrize("skill", APP, ids=lambda s: s.name)
def test_app_skill_frontmatter_and_references(skill):
    text = (skill.folder / "SKILL.md").read_text()
    meta = skills.frontmatter(text)
    assert meta["name"] == skill.folder.name
    assert len(skill.description) > 40
    for name in TOOL_LIKE.findall(text):
        assert name in tools.TOOLS, f"{skill.name} mentions unknown tool {name}"
    for name in SKILL_LIKE.findall(text):
        assert name in skills.catalog(), f"{skill.name} mentions unknown skill {name}"
    for term in CLI_ERA:
        assert term not in text, f"{skill.name} contains {term}"


def test_entry_skill_lists_every_tool():
    text = (skills.catalog()["impulse-app"].folder / "SKILL.md").read_text()
    missing = [name for name in tools.TOOLS if f"`{name}`" not in text]
    assert missing == []


def test_upstream_copy_matches_manifest():
    lines = (UPSTREAM / "MANIFEST").read_text().splitlines()
    assert lines[0].startswith("tag v")
    expected = dict(reversed(line.split("  ", 1)) for line in lines[1:])
    actual = {
        p.relative_to(UPSTREAM).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in UPSTREAM.rglob("*")
        if p.is_file() and p.name != "MANIFEST"
    }
    assert actual == expected


def test_manifest_tag_matches_lock():
    from server.settings import settings

    assert (UPSTREAM / "MANIFEST").read_text().splitlines()[0] == f"tag v{settings.framework['version']}"


def test_every_skill_fits_one_tool_result():
    for skill in skills.catalog().values():
        assert len(json.dumps(skills.load(skill.name))) <= MAX_TOOL_RESULT, skill.name
        for file in skill.references:
            assert len(json.dumps(skills.reference(skill.name, file))) <= MAX_TOOL_RESULT, f"{skill.name}/{file}"


def test_reference_loading_is_confined():
    loaded = skills.load("impulse-app-signals")
    assert "references/automotive_domains.md" in loaded["references"]
    assert "Drehzahl" in skills.reference("impulse-app-signals", "references/automotive_domains.md")["content"]
    with pytest.raises(KeyError):
        skills.reference("impulse-app-signals", "../../impulse/impulse/SKILL.md")
