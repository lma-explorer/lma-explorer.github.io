"""Bulk-ingest the Era B (Oct 2017–Apr 2019) Clovis CV_LS750 TXT corpus.

Walks ``data/raw/clovis_historical/mmn/CV_LS750*.TXT``, parses each via
``era_b_txt.parse``, concatenates into one DataFrame, runs the §7 validator
(see ``validate.py``), and writes a vintage-stamped Parquet snapshot under
``data/processed/`` matching the convention used by the live MARS snapshot
(``pipelines/clovis/snapshot.py``).

Outputs (in ``data/processed/``):

    clovis_historical_era_b_<vintage>.parquet   # the historical block
    clovis_historical_era_b_latest.parquet      # convenience copy
    clovis_historical_MANIFEST.json             # append-only

The MANIFEST entry uses ``slug = "AMS_1781_HISTORICAL"`` and
``coverage = "<first_auction_date>..<last_auction_date>"`` per PLAN_4.1 §10b.
The historical block is read-only after ingest; if the parser is later
corrected, the ingest is re-run with a new vintage and the prior file is
preserved on disk for SHA-256 reproducibility.

Validator behavior on FAIL: aborts before writing the parquet. Rerun with
``--allow-fail`` to write anyway (intended for forensic inspection only;
do not commit a failed batch).

Usage:
    python -m pipelines.clovis_historical.ingest_era_b
    python -m pipelines.clovis_historical.ingest_era_b --vintage 2026-04-25
    python -m pipelines.clovis_historical.ingest_era_b --csv-fallback   # if pyarrow missing
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pipelines.clovis_historical.era_b_txt import parse  # type: ignore
from pipelines.clovis_historical.validate import validate_batch  # type: ignore

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis_historical" / "mmn"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "clovis_historical_MANIFEST.json"

CANONICAL_COLS = [
    "auction_date", "commodity", "class", "frame", "muscle_grade",
    "weight_break_low", "weight_break_high",
    "avg_weight", "avg_weight_min", "avg_weight_max",
    "head_count", "price_low", "price_high", "price_avg",
    "receipts", "vintage",
    "breed", "annotation",
]


def collect_rows(raw_dir: Path, vintage: date) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """Parse every CV_LS750*.TXT in raw_dir. Returns (df, [(filename, row_count)])."""
    files = sorted(raw_dir.glob("CV_LS750*.TXT"))
    if not files:
        raise SystemExit(f"No CV_LS750*.TXT files found under {raw_dir}. "
                         "Run download_era_b.py first.")
    all_rows: list[dict] = []
    per_file: list[tuple[str, int]] = []
    fallback_used: list[str] = []
    for p in files:
        text = p.read_text(encoding="utf-8", errors="replace")
        # Sniff whether the prose auction-date marker is present; if not,
        # we'll be silently using the filename fallback. Track it so the
        # ingest output flags which weeks landed via fallback.
        from pipelines.clovis_historical.era_b_txt import RE_AUCTION_DATE  # type: ignore
        if not RE_AUCTION_DATE.search(text):
            fallback_used.append(p.name)
        rows = parse(text, vintage=vintage, filename=p.name)
        per_file.append((p.name, len(rows)))
        all_rows.extend(r.to_dict() for r in rows)
    if fallback_used:
        print(f"\nWARNING: {len(fallback_used)} file(s) used filename-derived auction date "
              "(prose marker not matched):")
        for fn in fallback_used:
            print(f"  - {fn}")
        print("These rows are still ingested; the filename date is reliable for weekly aggregation.\n")

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df, per_file
    # Reorder to canonical column order
    df = df[CANONICAL_COLS]
    # Type discipline
    int_cols = ["weight_break_low", "weight_break_high", "head_count", "receipts"]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df, per_file


def write_outputs(df: pd.DataFrame, vintage: date, csv_fallback: bool) -> dict:
    """Write the snapshot file(s) and return a manifest *entry* dict.

    Schema matches ``pipelines/clovis/snapshot.py``'s entry shape so the
    chart pages' ``data.qmd`` MANIFEST reader works on both. We add a
    handful of historical-block-specific fields at the entry level
    (era, format_source, auction_week_count, coverage) — the live
    reader ignores unknown keys.
    """
    import hashlib
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    suffix = "csv" if csv_fallback else "parquet"
    snap_path = PROCESSED_DIR / f"clovis_historical_era_b_{vintage.isoformat()}.{suffix}"
    latest_path = PROCESSED_DIR / f"clovis_historical_era_b_latest.{suffix}"

    if csv_fallback:
        df.to_csv(snap_path, index=False)
        df.to_csv(latest_path, index=False)
    else:
        df.to_parquet(snap_path, index=False)
        df.to_parquet(latest_path, index=False)

    auction_dates = pd.to_datetime(df["auction_date"]).dt.date
    coverage = f"{auction_dates.min().isoformat()}..{auction_dates.max().isoformat()}"
    sha256 = hashlib.sha256(snap_path.read_bytes()).hexdigest()

    return {
        # Canonical fields — match the live MARS manifest entry shape
        "vintage": vintage.isoformat(),
        "file": str(snap_path.relative_to(REPO_ROOT)),
        "sha256": sha256,
        "rows": int(len(df)),
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
        # Historical-block extras — ignored by the live reader, surfaced
        # by anything that knows about Era B
        "era": "B",
        "format_source": "MMN per-slug archive (CV_LS750 fixed-width TXT)",
        "latest_path": str(latest_path.relative_to(REPO_ROOT)),
        "auction_week_count": int(auction_dates.nunique()),
        "coverage": coverage,
        "schema_columns": CANONICAL_COLS,
    }


def append_manifest(entry: dict) -> None:
    """Append-only manifest in the same shape as ``clovis_MANIFEST.json``:
    ``{"slug": "...", "entries": [...]}``. Re-running with the same
    vintage replaces the prior entry rather than duplicating."""
    if MANIFEST_PATH.exists():
        try:
            manifest = json.loads(MANIFEST_PATH.read_text())
        except json.JSONDecodeError:
            manifest = {}
        # Migrate from old top-level-list format if present
        if isinstance(manifest, list):
            manifest = {"slug": "AMS_1781_HISTORICAL", "entries": []}
    else:
        manifest = {"slug": "AMS_1781_HISTORICAL", "entries": []}
    if "entries" not in manifest:
        manifest["entries"] = []
    if "slug" not in manifest:
        manifest["slug"] = "AMS_1781_HISTORICAL"

    # Replace any prior entry with the same vintage; otherwise append.
    manifest["entries"] = [
        e for e in manifest["entries"] if e.get("vintage") != entry["vintage"]
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--vintage", help="ISO date YYYY-MM-DD (default: today)")
    ap.add_argument("--allow-fail", action="store_true",
                    help="Write outputs even if validator FAILs (forensic only)")
    ap.add_argument("--csv-fallback", action="store_true",
                    help="Write CSV instead of Parquet if pyarrow is missing")
    args = ap.parse_args(argv)

    vintage = (
        date.fromisoformat(args.vintage) if args.vintage else date.today()
    )

    print(f"Ingesting Era B from {RAW_DIR} (vintage={vintage})")
    df, per_file = collect_rows(RAW_DIR, vintage)
    print(f"Parsed {len(df)} rows from {len(per_file)} files")
    if len(per_file) <= 8:
        for fn, n in per_file:
            print(f"  {fn}: {n} rows")

    print("\n=== Running validator ===")
    report = validate_batch(df, era="B")
    report.print_summary()

    if not report.passed and not args.allow_fail:
        print("\nValidator FAIL — aborting. Re-run with --allow-fail for forensic write.")
        return 3

    print("\n=== Writing outputs ===")
    entry = write_outputs(df, vintage, args.csv_fallback)
    print(json.dumps(entry, indent=2))

    append_manifest(entry)
    print(f"\nMANIFEST appended: {MANIFEST_PATH}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
