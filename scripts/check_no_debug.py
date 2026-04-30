"""Pre-commit hook + standalone script that catches debug-leftover code
in tracked files before it can land on `main`.

Why this exists
---------------
Commit ``aca3fd3`` removed 17 lines of debug code from
``site/weekly-trends.qmd`` that wrote to ``/tmp/`` during render. That
code was added during a debugging session and survived to ``main``
because no automated check looked for it. This script is the loud check
that should have been there.

What it catches
---------------
Patterns that almost always indicate forgotten debug code:

- ``breakpoint()`` (PEP 553) — pdb entry; never wanted in production
- ``import pdb`` / ``import ipdb`` — debugger imports
- ``open("/tmp/...`` or ``Path("/tmp/...`` writes (the actual leak that
  shipped in weekly-trends.qmd)
- ``console.log(`` in ``.qmd`` files (JavaScript debug output)
- ``print("DEBUG`` / ``# TODO: REMOVE`` / ``# XXX``

What it doesn't catch
---------------------
- Print statements (frequently used legitimately in render-time chunks)
- Real pdb.set_trace() (less common than breakpoint() now)
- Embedded JS console.warn / console.error (these are sometimes
  intentional in production)

The lint is pattern-based and tolerates one explicit allow comment per
match: ``# debug-allow`` on the same line tells the script to skip that
line (rare, but useful if a /tmp/ write is genuinely intended).

Usage
-----
As a CLI from the repo root::

    python scripts/check_no_debug.py            # check tracked files
    python scripts/check_no_debug.py --staged   # check git-staged files only
    python scripts/check_no_debug.py file1 file2  # check specific files

Exit code 1 on any match; 0 on clean.

To wire as a pre-commit hook (after running ``pip install pre-commit`` and
``pre-commit install``), the hook is declared in ``.pre-commit-config.yaml``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


# Pattern definitions. Each tuple is (regex, human-readable description,
# file-extension predicate). The predicate decides whether the pattern
# applies to a given file path.
ALL_PY_QMD = lambda p: p.suffix in {".py", ".qmd"}  # noqa: E731
ALL_QMD = lambda p: p.suffix == ".qmd"  # noqa: E731
ALL_PY = lambda p: p.suffix == ".py"  # noqa: E731

PATTERNS: list[tuple[re.Pattern, str, callable]] = [
    (re.compile(r"\bbreakpoint\s*\("), "breakpoint() call", ALL_PY_QMD),
    (re.compile(r"^\s*import\s+(pdb|ipdb)\b"), "pdb/ipdb import", ALL_PY_QMD),
    (re.compile(r"^\s*from\s+(pdb|ipdb)\s+import"), "pdb/ipdb import", ALL_PY_QMD),
    (re.compile(r"\b(pdb|ipdb)\.set_trace\s*\("), "pdb.set_trace()", ALL_PY_QMD),
    (re.compile(r'open\s*\(\s*[\'"]/tmp/'), "/tmp/ write", ALL_PY_QMD),
    (re.compile(r'Path\s*\(\s*[\'"]/tmp/'), "/tmp/ Path write", ALL_PY_QMD),
    (re.compile(r'\.write_html\s*\(\s*[\'"]/tmp/'), "/tmp/ write_html", ALL_PY_QMD),
    (re.compile(r"console\.log\s*\("), "console.log() in .qmd", ALL_QMD),
    (re.compile(r'#\s*XXX\b', re.IGNORECASE), "# XXX comment", ALL_PY_QMD),
    (re.compile(r'#\s*TODO:\s*REMOVE\b', re.IGNORECASE), "# TODO: REMOVE", ALL_PY_QMD),
]


# A line containing this allow-comment skips the lint for that line.
# Both Python (#) and JS (//) comment forms are recognized so the same
# escape hatch works in .py and .qmd files (which often have embedded JS).
ALLOW_RE = re.compile(r"(#|//)\s*debug-allow\b", re.IGNORECASE)


def _git_tracked_files() -> list[Path]:
    """Return all git-tracked files that the lint applies to."""
    try:
        out = subprocess.check_output(
            ["git", "ls-files"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [Path(p) for p in out.splitlines() if p]


def _git_staged_files() -> list[Path]:
    """Return only files staged for commit (the pre-commit-hook view)."""
    try:
        out = subprocess.check_output(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [Path(p) for p in out.splitlines() if p]


def check_file(path: Path) -> list[tuple[int, str, str]]:
    """Return a list of (line_number, pattern_description, line_text) hits."""
    if not path.exists() or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    hits: list[tuple[int, str, str]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if ALLOW_RE.search(line):
            continue
        for pattern, desc, applies in PATTERNS:
            if applies(path) and pattern.search(line):
                hits.append((line_no, desc, line.strip()))
                break  # one hit per line is enough
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files", nargs="*", help="Specific files to check (default: all tracked files)"
    )
    parser.add_argument(
        "--staged", action="store_true",
        help="Check only files staged for commit (pre-commit hook mode)",
    )
    args = parser.parse_args(argv)

    if args.files:
        candidates = [Path(p) for p in args.files]
    elif args.staged:
        candidates = _git_staged_files()
    else:
        candidates = _git_tracked_files()

    # Apply the file-extension filter
    relevant = [p for p in candidates if p.suffix in {".py", ".qmd"}]
    if not relevant:
        return 0

    total_hits = 0
    for path in relevant:
        hits = check_file(path)
        for line_no, desc, line in hits:
            print(f"{path}:{line_no}: {desc}: {line}")
            total_hits += 1

    if total_hits:
        print(
            f"\ncheck_no_debug: {total_hits} debug-leftover hit(s) found.\n"
            "If a hit is intentional (rare), append `# debug-allow` to the\n"
            "end of the line and re-run."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
