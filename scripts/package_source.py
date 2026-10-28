#!/usr/bin/env python3
"""Build an auditable, deterministic source-only ZIP from an explicit allowlist."""
import argparse
import hashlib
import json
from itertools import chain
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = {"Cargo.toml", "Cargo.lock", "README.md", "THIRD_PARTY.md", ".gitignore"}
DIRECTORIES = {"src", "sbe_schemas", "tests", "fixtures", "examples", "scripts", "docs", "evidence", "third_party"}
ALLOWED_SUFFIXES = {".rs", ".toml", ".lock", ".md", ".yaml", ".xml", ".jsonl", ".json", ".py", ".log", ".txt"}
INTERNAL_DOCUMENTS = {"docs/PLAN.md", "docs/PROGRESS.md", "docs/REVIEW_BRIEF.md", "docs/REVIEW_REQUEST.md", "docs/REVIEW_RESULTS.md", "docs/UPSTREAM_README.md", "docs/CHALLENGE_MAP.md"}
HOST_INVENTORIES = {"resources-before.json", "resources-during.json", "resources-after.json", "resources-post-exit.json"}


def selected():
    files = []
    candidates = chain((ROOT / name for name in ROOT_FILES), *( (ROOT / directory).rglob("*") for directory in sorted(DIRECTORIES)))
    for path in candidates:
        relative = path.relative_to(ROOT)
        if relative.as_posix() in INTERNAL_DOCUMENTS or path.name in HOST_INVENTORIES:
            continue
        if relative.parts[0] == "evidence" and relative.as_posix() not in {"evidence/VERIFICATION.md", "evidence/submission-results.json"}:
            continue
        if relative.parts[0] not in DIRECTORIES and str(relative) not in ROOT_FILES:
            continue
        if any(part.startswith(".") for part in relative.parts) and str(relative) != ".gitignore":
            continue
        if any(part in {"__pycache__", "node_modules", "target", ".git"} for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"source allowlist contains symlink: {relative}")
        if not path.is_file():
            continue
        if path.suffix not in ALLOWED_SUFFIXES and str(relative) not in ROOT_FILES:
            raise ValueError(f"unexpected allowlisted file type: {relative}")
        if path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError(f"oversized source artifact: {relative}")
        if any(part in {"__pycache__", "node_modules", "target", ".git"} for part in relative.parts):
            continue
        files.append(path)
    return sorted(files, key=lambda path: path.relative_to(ROOT).as_posix())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    files = selected()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    manifest = []
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in files:
            name = "rust-orderbook-collector/" + path.relative_to(ROOT).as_posix()
            data = path.read_bytes()
            info = zipfile.ZipInfo(name, date_time=(2026, 10, 27, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
            manifest.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    args.output.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    args.output.with_suffix(".sha256").write_text(f"{digest}  {args.output.name}\n")
    with zipfile.ZipFile(args.output) as archive:
        assert archive.testzip() is None
        assert len(archive.namelist()) == len(files)
    print(json.dumps({"archive": str(args.output), "files": len(files), "bytes": args.output.stat().st_size, "sha256": digest}))


if __name__ == "__main__":
    main()
