"""Generate analyst-friendly download artifacts (CSV + XLSX) alongside each
committed Parquet snapshot in ``data/processed/``, then bundle the whole
folder into the rendered site so download links work on the deployed site.

Why this exists
---------------
Parquet is the canonical format for the platform — small, typed, fast to
read in Python / R / DuckDB. But many of the platform's downstream readers
(producers, journalists, Extension peers without a Python environment)
want a file that opens directly in Excel, Numbers, LibreOffice, or a text
editor. This script generates those formats fresh on every site build,
right next to each ``*.parquet``.

The script also handles the bundling of ``data/processed/`` into the
rendered ``_site/`` so that the download links on chart pages and the
data catalog (which point to ``../data/processed/<file>``) actually
resolve on the deployed GitHub Pages site.

Two phases
----------
This script runs twice during a Quarto build:

- ``--phase pre``: generate CSV (and XLSX for small files) next to each
  ``*.parquet``. Runs before ``quarto render``, so the chart pages
  rendering Python that reads from data/processed/ has access if it
  needs it.
- ``--phase post``: copy all of ``data/processed/`` into
  ``site/_site/data/processed/`` so the deployed site serves the files.
  Runs after ``quarto render`` (the output dir exists by then).

Outputs
-------
For every ``data/processed/*.parquet``:
- A matching ``*.csv`` (always — universally readable)
- A matching ``*.xlsx`` (only when the parquet is < 500 KB, because XLSX
  becomes unwieldy at large row counts and Excel itself caps at ~1.05M
  rows). The LRP corpus is the main file that exceeds the threshold.

Generated files are gitignored (see ``.gitignore``) — they are produced
fresh on every build.

Usage
-----
Called by Quarto's ``pre-render`` and ``post-render`` hooks (see
``site/_quarto.yml``). To run manually::

    python scripts/prepare_downloads.py --phase pre   # generate CSV/XLSX
    python scripts/prepare_downloads.py --phase post  # copy to _site/

Idempotent — safe to rerun.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"
SITE_OUTPUT = REPO_ROOT / "site" / "_site" / "data" / "processed"
XLSX_SIZE_LIMIT_BYTES = 500 * 1024  # 500 KB — files above this are CSV-only

# Filename patterns that should be copied into the deployed site (anything
# else in data/processed/ is internal — e.g. vintage-stamped intermediate
# parquets that aren't directly linked from the site).
SITE_BUNDLE_GLOBS = ("*.parquet", "*.csv", "*.xlsx", "*MANIFEST.json")


# ---------------------------------------------------------------------------
# Phase 1 — generate CSV / XLSX from each parquet
# ---------------------------------------------------------------------------

def _convert_one(parquet_path: Path) -> tuple[bool, bool]:
    """Convert a single parquet to CSV (+ XLSX if small enough).

    Returns (csv_written, xlsx_written) for the per-file summary line.
    """
    df = pd.read_parquet(parquet_path)
    stem = parquet_path.with_suffix("")  # e.g. .../clovis_latest

    # CSV — always. UTF-8, no index column, with headers.
    csv_path = stem.with_suffix(".csv")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    csv_written = True

    # XLSX — only for small files. Excel struggles past ~1M rows and the
    # files balloon for wide schemas; CSV remains the lingua franca.
    xlsx_written = False
    if parquet_path.stat().st_size < XLSX_SIZE_LIMIT_BYTES:
        xlsx_path = stem.with_suffix(".xlsx")
        try:
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            xlsx_written = True
        except ImportError:
            # openpyxl not installed in this environment; CSV still ships.
            print(f"  (skipped XLSX for {parquet_path.name}: openpyxl not available)")
    return csv_written, xlsx_written


def run_pre() -> int:
    if not PROCESSED.exists():
        print(f"ERROR: {PROCESSED} does not exist; nothing to convert.")
        return 1

    parquets = sorted(PROCESSED.glob("*.parquet"))
    if not parquets:
        print(f"INFO: No parquets found under {PROCESSED}; nothing to convert.")
        return 0

    print(f"prepare_downloads (pre): scanning {PROCESSED}")
    n_csv = 0
    n_xlsx = 0
    for pq in parquets:
        csv_ok, xlsx_ok = _convert_one(pq)
        if csv_ok:
            n_csv += 1
        if xlsx_ok:
            n_xlsx += 1
        size_kb = pq.stat().st_size / 1024
        marks = []
        if csv_ok:
            marks.append("CSV")
        if xlsx_ok:
            marks.append("XLSX")
        print(f"  {pq.name:<55s} {size_kb:>8.1f} KB  →  {' + '.join(marks)}")

    print(f"prepare_downloads (pre): wrote {n_csv} CSVs, {n_xlsx} XLSXs.")
    return 0


# ---------------------------------------------------------------------------
# Phase 2 — copy data/processed/* into _site/data/processed/
# ---------------------------------------------------------------------------

def run_post() -> int:
    if not PROCESSED.exists():
        print(f"ERROR: {PROCESSED} does not exist; nothing to copy.")
        return 1
    if not SITE_OUTPUT.parent.parent.exists():
        # _site/ doesn't exist yet — render must have failed. Don't crash.
        print(f"WARNING: {SITE_OUTPUT.parent.parent} does not exist; skipping copy.")
        return 0

    SITE_OUTPUT.mkdir(parents=True, exist_ok=True)
    print(f"prepare_downloads (post): bundling into {SITE_OUTPUT}")
    n_copied = 0
    for pattern in SITE_BUNDLE_GLOBS:
        for src in sorted(PROCESSED.glob(pattern)):
            dst = SITE_OUTPUT / src.name
            shutil.copy2(src, dst)
            n_copied += 1

    print(f"prepare_downloads (post): copied {n_copied} files.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase", choices=("pre", "post"), default="pre",
        help="pre: generate CSV/XLSX. post: bundle into _site/.",
    )
    args = parser.parse_args()
    if args.phase == "pre":
        return run_pre()
    else:
        return run_post()


if __name__ == "__main__":
    sys.exit(main())
