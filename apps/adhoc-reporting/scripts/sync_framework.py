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


def clone(tag: str, checkout: Path) -> None:
    subprocess.run(["git", "clone", "--quiet", "--filter=blob:none", REPO, str(checkout)], check=True)
    subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", tag], check=True)


def copy_skills(checkout: Path, target: Path) -> None:
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    for skill in sorted((checkout / "skills").iterdir()):
        if skill.is_dir() and (skill / "SKILL.md").exists():
            shutil.copytree(skill, target / skill.name)


def check(tag: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        checkout = Path(tmp) / "impulse"
        clone(tag, checkout)
        copy_skills(checkout, Path(tmp) / "skills")
        expected = manifest(Path(tmp) / "skills")
    if manifest(TARGET) != expected:
        raise SystemExit(
            f"skills/impulse differs from skills/ at {tag}. It is a generated copy: edit the framework skills in the "
            f"repository's root skills/ folder, and run this script with {tag} to restore the copy."
        )
    print(f"skills/impulse matches skills/ at {tag}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tag", nargs="?", help="release to pin, like v1.2.3")
    parser.add_argument("--check", action="store_true", help="only check that skills/impulse matches skills/ at the pinned release")
    args = parser.parse_args()
    lock_path = ROOT / "impulse.lock"
    lock = tomllib.loads(lock_path.read_text())
    if args.check:
        if args.tag:
            parser.error("--check takes no tag, it checks the pinned release")
        check(f"v{lock['version']}")
        return
    if not re.fullmatch(r"v\d+\.\d+\.\d+", args.tag or ""):
        raise SystemExit("tag must look like v1.2.3")
    version = args.tag[1:]
    previous = f"v{lock['version']}"

    with tempfile.TemporaryDirectory() as tmp:
        checkout = Path(tmp) / "impulse"
        clone(args.tag, checkout)
        if (checkout / "VERSION").read_text().strip() != version:
            raise SystemExit(f"VERSION file at {args.tag} does not say {version}")
        copy_skills(checkout, TARGET)
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
