# -*- coding: utf-8 -*-
"""CI-only tools: source checks, DEV marker, portable validation, release manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "rosreestr_parser_app.py"
DEFAULTS = ROOT / "config.example.json"
CURRENT = ROOT / "latest.json"
REPO = "https://github.com/serbo26-lab/rosreestr-parser"

TITLE_RE = re.compile(r'(?m)^(APP_TITLE[ \t]*=[ \t]*)"([^"\r\n]+)"[ \t]*$')
CODE_RE = re.compile(r"(?m)^APP_VERSION_CODE[ \t]*=[ \t]*(\d+)[ \t]*$")
TAG_RE = re.compile(r"^v\d+\.\d+(?:\.\d+)?$")
MANDATORY = (
    "RosreestrParser.exe", "config.json", "docs/wiki_oss.md",
    "docs/wiki_snt.md", "template_burmistr.xlsx",
    "template_roskvartal.xlsx", "template_snt.xlsx", "template_full_info.xlsx",
)


def fail(message: str) -> None:
    raise SystemExit("ERROR: " + message)


def app_version() -> tuple[str, int]:
    source = SRC.read_text(encoding="utf-8-sig")
    title, code = TITLE_RE.search(source), CODE_RE.search(source)
    if not title or not code:
        fail("APP_TITLE or APP_VERSION_CODE missing")
    return title.group(2), int(code.group(1))


def version_from_tag(tag: str) -> str:
    if not TAG_RE.fullmatch(tag):
        fail("Tag must look like v1.4 or v1.4.1")
    return "V" + tag[1:]


def check_source() -> None:
    compile(SRC.read_text(encoding="utf-8-sig"), str(SRC), "exec")
    config = json.loads(DEFAULTS.read_text(encoding="utf-8-sig"))
    if not isinstance(config, dict) or not CURRENT.is_file():
        fail("Missing config defaults or update manifest")
    app_version()
    print("OK: source, syntax and defaults")


def check_release(tag: str) -> None:
    version = version_from_tag(tag)
    title, code = app_version()
    if not re.search(rf"(?<!\w){re.escape(version)}(?![\w.])", title):
        fail(f"APP_TITLE must contain {version}: actual {title}")
    old = json.loads(CURRENT.read_text(encoding="utf-8-sig"))
    old_code = int(old.get("version_code", 0))
    if code <= old_code:
        fail(f"APP_VERSION_CODE={code} must be greater than published {old_code}")
    print(f"OK: release {tag}, version_code={code}, old={old_code}")


def stamp_dev(sha: str) -> None:
    if not re.fullmatch(r"[a-f0-9]{40}", sha.lower()):
        fail("Invalid commit SHA")
    source = SRC.read_text(encoding="utf-8-sig")
    updated, matches = TITLE_RE.subn(
        lambda m: f'{m.group(1)}"{m.group(2)} DEV {sha[:7]}"', source, count=1
    )
    if matches != 1 or updated == source:
        fail("Could not mark DEV build")
    compile(updated, str(SRC), "exec")
    SRC.write_text(updated, encoding="utf-8")
    config = json.loads(DEFAULTS.read_text(encoding="utf-8-sig"))
    config["update_manifest_url"] = ""
    config["update_channel"] = "dev"
    DEFAULTS.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"OK: DEV label added ({sha[:7]}), stable updater disabled")


def verify_zip(archive: Path, sha_file: Path) -> None:
    if not archive.is_file() or not sha_file.is_file():
        fail("ZIP or SHA256 file missing")
    with archive.open("rb") as stream:
        real = hashlib.file_digest(stream, "sha256").hexdigest()
    checksum = sha_file.read_text(encoding="utf-8-sig").strip().split()[0].lower()
    if not re.fullmatch(r"[0-9a-f]{64}", checksum) or checksum != real:
        fail(f"SHA256 mismatch: {checksum} vs {real}")
    with zipfile.ZipFile(archive) as z:
        bad = z.testzip()
        if bad:
            fail(f"Damaged ZIP entry: {bad}")
        names = z.namelist()
        if len(set(names)) != len(names):
            fail("Duplicate archive paths")
        missing = sorted(set(MANDATORY) - set(names))
        if missing:
            fail(f"Missing required portable files: {missing}")
        for name in names:
            path = name.replace("\\", "/")
            if path.startswith("/") or ".." in path.split("/"):
                fail(f"Unsafe path in ZIP: {name}")
            if path.startswith("_internal/"):
                continue
            parts = path.lower().strip("/").split("/")
            if any(x in {"__pycache__", "release_assets", "output", "state",
                         "profiles", "profile_rosreestr", "updates", "build",
                         ".venv", "src"} for x in parts):
                fail(f"Forbidden local folder in ZIP: {name}")
            if parts[-1] in {"readme.md", "license", "config.example.json"} or re.search(
                r"(?i)(\.bak[^/]*$|\.py[co]$)", path
            ):
                fail(f"Forbidden file in ZIP: {name}")
        actual_config = json.loads(z.read("config.json").decode("utf-8-sig"))
        clean_config = json.loads(DEFAULTS.read_text(encoding="utf-8-sig"))
        if actual_config != clean_config:
            fail("ZIP config does not match config.example.json")
    print(f"OK: {archive}, bytes={archive.stat().st_size}, sha256={real}")


def prepare_notes(tag: str, notes: str, target: Path) -> None:
    version_from_tag(tag)
    lines = [line.strip() for line in notes.splitlines() if line.strip()]
    if not lines:
        fail("Release notes cannot be empty")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: notes at {target}")


def make_manifest(tag: str, archive: Path, notes_file: Path) -> None:
    display = version_from_tag(tag)
    check_release(tag)
    if not archive.is_file() or not notes_file.is_file():
        fail("Release ZIP or notes missing")
    content = json.loads(CURRENT.read_text(encoding="utf-8-sig"))
    content.update(
        latest_version=display,
        version_code=app_version()[1],
        channel="stable",
        file_name="rosreestr_parser.zip",
        download_url=f"{REPO}/releases/download/{tag}/rosreestr_parser.zip",
        release_url=f"{REPO}/releases/tag/{tag}",
        sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        notes=[line.strip() for line in
               notes_file.read_text(encoding="utf-8").splitlines() if line.strip()],
    )
    CURRENT.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"OK: latest.json -> {display}")


def main() -> None:
    cli = argparse.ArgumentParser()
    sub = cli.add_subparsers(dest="command", required=True)
    sub.add_parser("check-source")
    release = sub.add_parser("check-release")
    release.add_argument("--tag", required=True)
    dev = sub.add_parser("stamp-dev")
    dev.add_argument("--sha", required=True)
    verify = sub.add_parser("verify-zip")
    verify.add_argument("--zip", required=True, type=Path)
    verify.add_argument("--sha-file", required=True, type=Path)
    notes = sub.add_parser("prepare-notes")
    notes.add_argument("--tag", required=True)
    notes.add_argument("--notes", required=True)
    notes.add_argument("--file", required=True, type=Path)
    manifest = sub.add_parser("make-manifest")
    manifest.add_argument("--tag", required=True)
    manifest.add_argument("--zip", required=True, type=Path)
    manifest.add_argument("--notes-file", required=True, type=Path)
    args = cli.parse_args()
    if args.command == "check-source":
        check_source()
    elif args.command == "check-release":
        check_release(args.tag)
    elif args.command == "stamp-dev":
        stamp_dev(args.sha)
    elif args.command == "verify-zip":
        verify_zip(args.zip, args.sha_file)
    elif args.command == "prepare-notes":
        prepare_notes(args.tag, args.notes, args.file)
    elif args.command == "make-manifest":
        make_manifest(args.tag, args.zip, args.notes_file)


if __name__ == "__main__":
    main()
