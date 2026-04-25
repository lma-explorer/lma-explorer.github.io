"""Write the validated MARS AMS_1781 response to a vintage-stamped Parquet snapshot.

Reads the most recent raw payload in ``data/raw/clovis/`` and writes three
artifacts:

    data/processed/clovis_weekly_<YYYY-MM-DD>.parquet   # this pull's snapshot
    data/processed/clovis_latest.parquet                # convenience copy
    data/processed/clovis_MANIFEST.json                 # append-only record

``data/processed/clovis_release_basis_2025.parquet`` is never touched by this
module; it is a one-time release artifact pinned to the Clovis vintage current
at Phase 1 release (December 2025 basis), so historical chart values stay
reproducible after future refreshes.

Long-format schema (preserves MARS's native 50-lb binning; downstream code
aggregates to the 100-lb bins used on the chart):

    auction_date      : date        (MARS report_date, MM/DD/YYYY parsed)
    commodity         : string      ("Feeder Cattle")
    class             : string      ("Steers" / "Heifers" / "Bulls")
    frame             : string      ("Medium and Large" / "Medium" / "Large" / ...)
    muscle_grade      : string      ("1" / "1-2" / "2" / ...)
    weight_break_low  : Int32 (NaN) lower 50-lb bin edge (lbs)
    weight_break_high : Int32 (NaN) upper 50-lb bin edge (lbs)
    avg_weight        : float (NaN) observed average weight (lbs)
    avg_weight_min    : float (NaN)
    avg_weight_max    : float (NaN)
    head_count        : Int32       number of head in this observation
    price_low         : float       avg_price_min, $/cwt
    price_high        : float       avg_price_max, $/cwt
    price_avg         : float       avg_price,     $/cwt
    receipts          : Int32 (NaN) total auction receipts that week
    vintage           : date        when this snapshot was first written

We filter to ``commodity == "Feeder Cattle"`` and ``price_unit == "Per Cwt"``.
Replacement Cattle, Slaughter Cattle, and head-priced breeding stock stay in
the raw JSON for the record but do not land in the processed parquet — the
chart pages use feeder $/cwt prices only.

Usage:
    python -m pipelines.clovis.snapshot
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "clovis_MANIFEST.json"

RELEASE_BASIS_NAME = "clovis_release_basis_2025.parquet"  # never written here
LATEST_NAME = "clovis_latest.parquet"

# The pipeline narrows the raw payload to feeder-cattle, per-cwt observations —
# the chart pages' empirical core. Other commodities (Replacement, Slaughter) and
# other price units (Per Unit, Per Head, Per Family) are preserved in the raw
# JSON but not carried into the processed snapshot.
KEEP_COMMODITY = "Feeder Cattle"
KEEP_PRICE_UNIT = "Per Cwt"
KEEP_CLASSES = {"Steers", "Heifers", "Bulls"}

# Matches validate.py. Rows with an ``avg_price`` outside this window are
# treated as USDA-AMS data-entry errors in the source archive and dropped
# from the snapshot; the validator reports them as warnings first.
MIN_PLAUSIBLE_PRICE = 20.0
MAX_PLAUSIBLE_PRICE = 800.0


def _vintage_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_raw() -> dict[str, Any]:
    candidates = sorted(RAW_DIR.glob("AMS_1781_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw MARS payloads in {RAW_DIR}")
    with candidates[-1].open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        return None


def _payload_to_dataframe(payload: Any, vintage_tag: str) -> pd.DataFrame:
    """Convert a MARS AMS_1781 response to the long-format DataFrame above."""
    if isinstance(payload, dict):
        rows = payload.get("results") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []

    vintage_d = date.fromisoformat(vintage_tag)
    records: list[dict[str, Any]] = []
    for r in rows:
        if r.get("commodity") != KEEP_COMMODITY:
            continue
        if r.get("price_unit") != KEEP_PRICE_UNIT:
            continue
        if r.get("class") not in KEEP_CLASSES:
            continue
        ad = _parse_date(r.get("report_date"))
        if ad is None:
            continue
        # Drop any row whose headline avg_price is outside plausible bounds —
        # USDA-AMS archive has known one-off data-entry errors (e.g. a
        # Heifers row on 2021-09-01 with avg_price_min=1.44). The validator
        # warns on these; the snapshot excludes them.
        try:
            ap = float(r.get("avg_price"))
        except (TypeError, ValueError):
            continue
        if not (MIN_PLAUSIBLE_PRICE <= ap <= MAX_PLAUSIBLE_PRICE):
            continue
        records.append(
            {
                "auction_date": ad,
                "commodity": r.get("commodity"),
                "class": r.get("class"),
                "frame": r.get("frame"),
                "muscle_grade": r.get("muscle_grade"),
                "weight_break_low": r.get("weight_break_low"),
                "weight_break_high": r.get("weight_break_high"),
                "avg_weight": r.get("avg_weight"),
                "avg_weight_min": r.get("avg_weight_min"),
                "avg_weight_max": r.get("avg_weight_max"),
                "head_count": r.get("head_count"),
                "price_low": r.get("avg_price_min"),
                "price_high": r.get("avg_price_max"),
                "price_avg": r.get("avg_price"),
                "receipts": r.get("receipts"),
                "vintage": vintage_d,
            }
        )

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df

    # Sort deterministically: by auction date, class, and weight bin (low first, null last).
    df = df.sort_values(
        by=[
            "auction_date",
            "class",
            "frame",
            "muscle_grade",
            "weight_break_low",
            "weight_break_high",
        ],
        na_position="last",
    ).reset_index(drop=True)

    # Dtype hygiene: dates as proper datetime64[ns], nullable ints for counts.
    df["auction_date"] = pd.to_datetime(df["auction_date"])
    df["vintage"] = pd.to_datetime(df["vintage"])
    for col in (
        "weight_break_low",
        "weight_break_high",
        "head_count",
        "receipts",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
    for col in (
        "avg_weight",
        "avg_weight_min",
        "avg_weight_max",
        "price_low",
        "price_high",
        "price_avg",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    return df


def _write_manifest_entry(
    vintage_tag: str, snapshot_path: Path, row_count: int
) -> None:
    entry = {
        "vintage": vintage_tag,
        "file": snapshot_path.relative_to(REPO_ROOT).as_posix(),
        "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
        "rows": row_count,
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"slug": "AMS_1781", "entries": []}
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage_tag
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a validated Clovis (AMS_1781) vintage snapshot.",
    )
    parser.parse_args(argv)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    payload = _load_raw()
    vintage = _vintage_tag()

    df = _payload_to_dataframe(payload, vintage)
    if df.empty:
        print(
            "[snapshot] no Feeder Cattle $/cwt rows extracted from payload",
            file=sys.stderr,
        )
        return 1

    snapshot_path = PROCESSED_DIR / f"clovis_weekly_{vintage}.parquet"
    latest_path = PROCESSED_DIR / LATEST_NAME

    df.to_parquet(snapshot_path, index=False)
    df.to_parquet(latest_path, index=False)
    _write_manifest_entry(vintage, snapshot_path, len(df))

    print(f"[snapshot] wrote {snapshot_path} ({len(df)} rows)")
    print(f"[snapshot] wrote {latest_path}")
    print(f"[snapshot] updated {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
