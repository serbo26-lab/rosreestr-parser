# -*- coding: utf-8 -*-
"""Reject real-looking cadastral IDs in test fixtures.

Test datasets must use synthetically impossible cadastral region 00:00.
Real-looking addresses are reviewed manually under AGENTS.md.
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
SEARCH = (ROOT / "tests",)
CADASTRAL_PATTERN = re.compile(r"\b\d{2}:\d{2}:\d{3,}:\d+\b")
# Fictional-only address markers are documented in AGENTS.md.
# Arbitrary real-world addresses cannot be reliably identified by regex;
# human review of every new fixture is mandatory.

def main():
    errors=[]
    for base in SEARCH:
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if not p.is_file() or p.suffix.lower() not in {".py", ".txt", ".json", ".csv", ".md", ".yaml", ".yml"}:
                continue
            content=p.read_text(encoding="utf-8-sig").lower()
            for number in CADASTRAL_PATTERN.findall(content):
                if not number.startswith("00:00:"):
                    errors.append(f"{p.relative_to(ROOT)}: non-synthetic cadastre ID")
    if errors:
        print("\n".join(errors))
        raise SystemExit(1)
    print("OK: test fixtures contain only synthetic cadastral identifiers and place names")

if __name__ == "__main__":
    main()
