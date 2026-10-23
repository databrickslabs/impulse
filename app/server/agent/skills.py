import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from server.settings import ROOT

LAYERS = (("app", ROOT / "skills" / "app"), ("impulse", ROOT / "skills" / "impulse"))
FRONTMATTER = re.compile(r"^---\n(.*?)\n---\n", re.S)


@dataclass(frozen=True)
class Skill:
    name: str
    description: str
    layer: str
    folder: Path

    @property
    def references(self) -> list[str]:
        return sorted(p.relative_to(self.folder).as_posix() for p in self.folder.rglob("*.md") if p.name != "SKILL.md")


def frontmatter(text: str) -> dict:
    match = FRONTMATTER.match(text)
    return (yaml.safe_load(match.group(1)) or {}) if match else {}


@lru_cache(maxsize=1)
def catalog() -> dict[str, Skill]:
    found: dict[str, Skill] = {}
    for layer, root in LAYERS:
        for path in sorted(root.glob("*/SKILL.md")):
            meta = frontmatter(path.read_text())
            name = meta.get("name") or path.parent.name
            found[name] = Skill(name, " ".join(str(meta.get("description", "")).split()), layer, path.parent)
    return found


def index() -> str:
    lines = []
    for layer, title in (("app", "App skills, for operating this app"), ("impulse", "Impulse framework skills, for framework semantics")):
        lines.append(f"{title}:")
        lines += [f"- {s.name}: {s.description}" for s in catalog().values() if s.layer == layer]
    return "\n".join(lines)


def load(name: str) -> dict:
    skill = catalog().get(name)
    if skill is None:
        raise KeyError(f"Unknown skill {name}. Available: {', '.join(catalog())}")
    text = (skill.folder / "SKILL.md").read_text()
    return {"name": name, "content": FRONTMATTER.sub("", text, count=1), "references": skill.references}


def reference(name: str, file: str) -> dict:
    skill = catalog().get(name)
    if skill is None:
        raise KeyError(f"Unknown skill {name}")
    if file not in skill.references:
        raise KeyError(f"{name} has no reference {file}. Available: {', '.join(skill.references) or 'none'}")
    return {"name": name, "file": file, "content": (skill.folder / file).read_text()}
