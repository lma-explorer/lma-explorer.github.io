"""One-time ingestion of CME GF feeder-cattle daily settle prices.

Reads the user's local Excel workbook of historical feeder-cattle futures
settles (path from ``CLOVIS_FUTURES_XLSX`` env var) and writes a vintage-
stamped, SHA-256-stamped Parquet snapshot under ``data/processed/``.

The source workbook is third-party-compiled from CME's public daily
settlements. Per the platform's data-licensing posture (see
``LICENSE-DATA.md``), third-party compilations of public-domain data
are not redistributed; this pipeline keeps the workbook local
(gitignored) and writes only the *derived* settle parquet, which
contains the same numeric values that CME publishes daily.

A future Phase-2 follow-on can replace the Excel-bootstrap path with a
direct CME daily-settles pull, refreshable on schedule. For now, this
ingest runs once when the user updates their workbook.

Output:

    data/processed/cme_feeders_<vintage>.parquet   # this run's snapshot
    data/processed/cme_feeders_latest.parquet      # convenience copy
    data/processed/cme_feeders_MANIFEST.json       # append-only

Schema (long-format, one row per (date, contract_month)):

    date            date           daily settle date
    contract_month  string         "JAN" / "MAR" / "APR" / "MAY" /
                                   "AUG" / "SEP" / "OCT" / "NOV" /
                                   "NEARBY"
    settle          float          $/cwt
    vintage         date           when this snapshot was first written

The ``NEARBY`` rows duplicate the settle of whichever traded contract
was nearest expiry on that date — it's a derived view exposed as a
synthetic contract for downstream simplicity (the basis derivation
queries by ``contract_month``). The redundancy is small (~9,500 days
× 1 extra row each); the convenience is real.

Usage:
    export CLOVIS_FUTURES_XLSX="/path/to/feederfutures.xlsx"
    python -m pipelines.cme_feeders.ingest
    python -m pipelines.cme_feeders.ingest --csv-fallback   # pyarrow missing
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

ENV_VAR = "CLOVIS_FUTURES_XLSX"
SLUG = "CME_GF_FEEDERS"

# Sheet A column layout (after skipping the 4 header rows).
SHEET_A_COLS = {
    "date": 0,
    "JAN": 2, "MAR": 3, "APR": 4, "MAY": 5,
    "AUG": 6, "SEP": 7, "OCT": 8, "NOV": 9,
    "NEARBY": 10,
}
SHEET_A_DATA_FIRST_ROW = 4

CONTRACT_MONTHS = ["JAN", "MAR", "APR", "MAY", "AUG", "SEP", "OCT", "NOV", "NEARBY"]

# Sanity bounds — settles outside this band are treated as data errors
# in the source workbook and dropped (with a warning).
MIN_PLAUSIBLE_SETTLE = 30.0   # historical low ~$50; floor at $30 for safety
MAX_PLAUSIBLE_SETTLE = 600.0  # 2025-2026 highs around $400; ceiling at $600

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "cme_feeders_MANIFEST.json"


def _vintage_tag() -> date:
    return date.today()


def _resolve_source() -> Path:
    val = os.environ.get(ENV_VAR)
    if not val:
        print(
            f"ERROR: {ENV_VAR} env var not set.\n"
            "Set it to your local feeder-cattle settle Excel:\n"
            f"    export {ENV_VAR}='.../Data_REDACTED/feederfutures.xlsx'",
            file=sys.stderr,
        )
        sys.exit(2)
    p = Path(val).expanduser()
    if not p.exists():
        print(f"ERROR: {ENV_VAR} points at {p}, which does not exist.",
              file=sys.stderr)
        sys.exit(2)
    return p


def read_source(path: Path) -> pd.DataFrame:
    """Read sheet A as a wide DataFrame (one row per trading day, one
    column per contract-month settle plus ``NEARBY``)."""
    raw = pd.read_excel(path, sheet_name="A", header=None,
                        skiprows=SHEET_A_DATA_FIRST_ROW)
    cleaned = pd.DataFrame({
        name: raw[idx]
        for name, idx in SHEET_A_COLS.items()
    })
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce").dt.date
    cleaned = cleaned[cleaned["date"].notna()].copy()
    for c in CONTRACT_MONTHS:
        cleaned[c] = pd.to_numeric(cleaned[c], errors="coerce")
    return cleaned.sort_values("date").reset_index(drop=True)


def to_long_format(wide: pd.DataFrame, vintage: date) -> pd.DataFrame:
    """Pivot wide → long, drop NaN settles, attach vintage."""
    melted = wide.melt(
        id_vars=["date"],
        value_vars=CONTRACT_MONTHS,
        var_name="contract_month",
        value_name="settle",
    )
    melted = melted[melted["settle"].notna()].copy()
    melted["settle"] = melted["settle"].astype(float)

    # Sanity bounds — log out-of-bounds rows then drop
    oob = melted[
        (melted["settle"] < MIN_PLAUSIBLE_SETTLE)
        | (melted["settle"] > MAX_PLAUSIBLE_SETTLE)
    ]
    if not oob.empty:
        print(f"[ingest] {len(oob)} settle rows outside "
              f"[{MIN_PLAUSIBLE_SETTLE}, {MAX_PLAUSIBLE_SETTLE}] $/cwt — dropped:",
              file=sys.stderr)
        for _, r in oob.head(5).iterrows():
            print(f"           {r['date']} {r['contract_month']:>6} = ${r['settle']:.2f}",
                  file=sys.stderr)
        if len(oob) > 5:
            print(f"           ... and {len(oob) - 5} more", file=sys.stderr)
        melted = melted[
            (melted["settle"] >= MIN_PLAUSIBLE_SETTLE)
            & (melted["settle"] <= MAX_PLAUSIBLE_SETTLE)
        ]

    melted["vintage"] = vintage
    melted = melted.sort_values(["date", "contract_month"]).reset_index(drop=True)
    return melted[["date", "contract_month", "settle", "vintage"]]


def write_outputs(df: pd.DataFrame, vintage: date, csv_fallback: bool) -> dict:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    suffix = "csv" if csv_fallback else "parquet"
    snap_path = PROCESSED_DIR / f"cme_feeders_{vintage.isoformat()}.{suffix}"
    latest_path = PROCESSED_DIR / f"cme_feeders_latest.{suffix}"

    if csv_fallback:
        df.to_csv(snap_path, index=False)
        df.to_csv(latest_path, index=False)
    else:
        df.to_parquet(snap_path, index=False)
        df.to_parquet(latest_path, index=False)

    sha256 = hashlib.sha256(snap_path.read_bytes()).hexdigest()
    date_min = df["date"].min()
    date_max = df["date"].max()
    coverage = f"{date_min.isoformat()}..{date_max.isoformat()}"

    return {
        # Match the live MARS / BLS manifest entry shape
        "vintage": vintage.isoformat(),
        "file": str(snap_path.relative_to(REPO_ROOT)),
        "sha256": sha256,
        "rows": int(len(df)),
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
        # CME-specific extras
        "source_kind": "third-party-compiled CME daily settle workbook",
        "latest_path": str(latest_path.relative_to(REPO_ROOT)),
        "coverage": coverage,
        "contract_months": sorted(df["contract_month"].unique().tolist()),
        "trading_days": int(df["date"].nunique()),
    }


def append_manifest(entry: dict) -> None:
    if MANIFEST_PATH.exists():
        try:
            manifest = json.loads(MANIFEST_PATH.read_text())
        except json.JSONDecodeError:
            manifest = {}
    else:
        manifest = {}
    if not isinstance(manifest, dict):
        manifest = {}
    if "slug" not in manifest:
        manifest["slug"] = SLUG
    if "entries" not in manifest:
        manifest["entries"] = []
    # Replace any prior entry with the same vintage; otherwise append
    manifest["entries"] = [
        e for e in manifest["entries"] if e.get("vintage") != entry["vintage"]
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--vintage", help="ISO date YYYY-MM-DD (default: today)")
    ap.add_argument("--csv-fallback", action="store_true",
                    help="Write CSV instead of Parquet (sandbox / no-pyarrow)")
    args = ap.parse_args(argv)

    vintage = (
        date.fromisoformat(args.vintage) if args.vintage else _vintage_tag()
    )

    src = _resolve_source()
    print(f"Reading {src}")
    wide = read_source(src)
    print(f"Read {len(wide):,} trading days, "
          f"{wide['date'].min()} → {wide['date'].max()}")

    long_df = to_long_format(wide, vintage)
    print(f"Long-format rows: {len(long_df):,} "
          f"({long_df['date'].nunique()} dates × "
          f"{long_df['contract_month'].nunique()} contract months, "
          f"NaN settles dropped)")

    entry = write_outputs(long_df, vintage, args.csv_fallback)
    print("\n=== Manifest entry ===")
    print(json.dumps(entry, indent=2))

    append_manifest(entry)
    print(f"\nMANIFEST appended: {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
