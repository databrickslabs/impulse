import argparse
import hashlib
import re
import shutil
import subprocess
import tempfile
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO = "https://github.com/databrickslabs/impulse"
TARGET = ROOT / "skills" / "impulse"


def manifest(folder: Path) -> str:
    lines = []
    for path in sorted(p for p in folder.rglob("*") if p.is_file() and p.name != "MANIFEST"):
        lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(folder).as_posix()}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tag")
    args = parser.parse_args()
    if not re.fullmatch(r"v\d+\.\d+\.\d+", args.tag):
        raise SystemExit("tag must look like v1.2.3")
    version = args.tag[1:]
    lock_path = ROOT / "impulse.lock"
    lock = tomllib.loads(lock_path.read_text())
    previous = f"v{lock['version']}"

    with tempfile.TemporaryDirectory() as tmp:
        checkout = Path(tmp) / "impulse"
        subprocess.run(["git", "clone", "--quiet", "--filter=blob:none", REPO, str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", args.tag], check=True)
        if (checkout / "VERSION").read_text().strip() != version:
            raise SystemExit(f"VERSION file at {args.tag} does not say {version}")
        shutil.rmtree(TARGET, ignore_errors=True)
        TARGET.mkdir(parents=True)
        for skill in sorted((checkout / "skills").iterdir()):
            if skill.is_dir() and (skill / "SKILL.md").exists():
                shutil.copytree(skill, TARGET / skill.name)
        (TARGET / "MANIFEST").write_text(f"tag {args.tag}\n" + manifest(TARGET))
        if previous != args.tag:
            log = subprocess.run(
                ["git", "-C", str(checkout), "log", "--oneline", f"{previous}..{args.tag}"], check=True, capture_output=True, text=True
            ).stdout
            print(f"Upstream changes {previous}..{args.tag}:\n{log}")

    wheel = f"{REPO}/releases/download/{args.tag}/databricks_impulse-{version}-py3-none-any.whl"
    with urllib.request.urlopen(urllib.request.Request(wheel, method="HEAD")) as response:
        if response.status != 200:
            raise SystemExit(f"release wheel not found: {wheel}")
    text = lock_path.read_text()
    text = re.sub(r'^version = ".*"$', f'version = "{version}"', text, flags=re.M)
    text = re.sub(r'^wheel = ".*"$', f'wheel = "{wheel}"', text, flags=re.M)
    lock_path.write_text(text)
    print(f"impulse.lock now pins {version}; skills/impulse synced from {args.tag}")


if __name__ == "__main__":
    main()
