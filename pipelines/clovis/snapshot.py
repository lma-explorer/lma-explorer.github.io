"""Write the validated MARS AMS_1781 response to vintage-stamped Parquet snapshots.

Reads the most recent raw payload in ``data/raw/clovis/`` and writes
TWO parallel artifact sets — one for feeder cattle (the chart-shelf core),
one for slaughter cattle (cull-cow prices for the drought-destocking tool).

Feeder outputs (the chart-shelf and basis pipeline read these):

    data/processed/clovis_weekly_<YYYY-MM-DD>.parquet   # vintage snapshot
    data/processed/clovis_latest.parquet                # convenience copy
    data/processed/clovis_MANIFEST.json                 # append-only record

Slaughter outputs (the drought-destocking tool reads these):

    data/processed/clovis_slaughter_<YYYY-MM-DD>.parquet   # vintage snapshot
    data/processed/clovis_slaughter_latest.parquet         # convenience copy
    data/processed/clovis_slaughter_MANIFEST.json          # append-only record

``data/processed/clovis_release_basis_2025.parquet`` is never touched by this
module; it is a one-time release artifact pinned to the Clovis vintage current
at Phase 1 release (December 2025 basis), so historical chart values stay
reproducible after future refreshes.

Feeder long-format schema (preserves MARS's native 50-lb binning; downstream
code aggregates to the 100-lb bins used on the chart):

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

Slaughter long-format schema (no weight binning — slaughter cows are priced
per actual avg_weight; grade is carried in the quality_grade_name field
rather than the feeder-style frame/muscle_grade split):

    auction_date         : date     (MARS report_date, MM/DD/YYYY parsed)
    commodity            : string   ("Slaughter Cattle")
    class                : string   ("Cows" / "Bulls" / "Dairy Cows")
    quality_grade_name   : string   ("Boner 80-85%" / "Breaker 75-80%" /
                                     "Lean 85-90%" / "Premium White 65-75%")
    dressing             : string   ("Average" / "High" / "Low" / "Very Low")
    lot_desc             : string   ("None" / "Return to Feed" / ...)
    avg_weight           : float    observed average weight (lbs)
    avg_weight_min       : float
    avg_weight_max       : float
    head_count           : Int32    number of head
    price_low            : float    avg_price_min, $/cwt
    price_high           : float    avg_price_max, $/cwt
    price_avg            : float    avg_price,     $/cwt
    receipts             : Int32 (NaN) total auction receipts that week
    vintage              : date     when this snapshot was first written

Filters:
- Feeder snapshot: ``commodity == "Feeder Cattle"``, ``price_unit == "Per Cwt"``,
  class in {Steers, Heifers, Bulls}.
- Slaughter snapshot: ``commodity == "Slaughter Cattle"``, ``price_unit == "Per Cwt"``,
  class in {Cows, Bulls, Dairy Cows}.
- Replacement Cattle and head-priced breeding stock stay in the raw JSON for
  the record but do not land in either processed parquet.

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

# Feeder-cattle filter parameters — the chart-shelf core (price-weight,
# seasonality, weekly-trends, basis, sell-now-compare).
KEEP_COMMODITY = "Feeder Cattle"
KEEP_PRICE_UNIT = "Per Cwt"
KEEP_CLASSES = {"Steers", "Heifers", "Bulls"}

# Slaughter-cattle filter parameters (added 2026-05-12 for the drought-
# destocking tool's cull-cow data path). The drought tool primarily needs
# the Cows class; Bulls and Dairy Cows are also kept for future tools at
# negligible storage cost. Other commodities (Replacement Cattle) and
# other price units (Per Unit, Per Head, Per Family) remain dropped.
KEEP_COMMODITY_SLAUGHTER = "Slaughter Cattle"
KEEP_CLASSES_SLAUGHTER = {"Cows", "Bulls", "Dairy Cows"}
SLAUGHTER_LATEST_NAME = "clovis_slaughter_latest.parquet"
SLAUGHTER_MANIFEST_PATH = PROCESSED_DIR / "clovis_slaughter_MANIFEST.json"
SLAUGHTER_VINTAGE_TEMPLATE = "clovis_slaughter_{vintage}.parquet"

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


def _payload_to_slaughter_dataframe(payload: Any, vintage_tag: str) -> pd.DataFrame:
    """Convert a MARS AMS_1781 response to the slaughter long-format DataFrame.

    Parallel to ``_payload_to_dataframe`` but for the Slaughter Cattle
    commodity. Schema differs: no weight binning (cull cows are priced per
    actual ``avg_weight``); grade is carried in ``quality_grade_name`` rather
    than the feeder-style ``frame`` / ``muscle_grade`` split; ``dressing``
    and ``lot_desc`` are preserved as additional descriptors.
    """
    if isinstance(payload, dict):
        rows = payload.get("results") or []
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []

    vintage_d = date.fromisoformat(vintage_tag)
    records: list[dict[str, Any]] = []
    for r in rows:
        if r.get("commodity") != KEEP_COMMODITY_SLAUGHTER:
            continue
        if r.get("price_unit") != KEEP_PRICE_UNIT:
            continue
        if r.get("class") not in KEEP_CLASSES_SLAUGHTER:
            continue
        ad = _parse_date(r.get("report_date"))
        if ad is None:
            continue
        # Same plausible-price bounds as feeder ([$20, $800]/cwt) — wide
        # enough to catch the typical $50-$300/cwt slaughter-cow range plus
        # higher-priced bulls and premium dairy cows, narrow enough to drop
        # one-off data-entry errors. See _payload_to_dataframe for context.
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
                "quality_grade_name": r.get("quality_grade_name"),
                "dressing": r.get("dressing"),
                "lot_desc": r.get("lot_desc"),
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

    # Sort deterministically: by auction date, class, grade, dressing.
    df = df.sort_values(
        by=["auction_date", "class", "quality_grade_name", "dressing"],
        na_position="last",
    ).reset_index(drop=True)

    # Dtype hygiene: dates as proper datetime64[ns], nullable ints for counts.
    df["auction_date"] = pd.to_datetime(df["auction_date"])
    df["vintage"] = pd.to_datetime(df["vintage"])
    for col in ("head_count", "receipts"):
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
    manifest_path: Path,
    slug: str,
    vintage_tag: str,
    snapshot_path: Path,
    row_count: int,
) -> None:
    """Append-or-replace a vintage entry in the given manifest file."""
    entry = {
        "vintage": vintage_tag,
        "file": snapshot_path.relative_to(REPO_ROOT).as_posix(),
        "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
        "rows": row_count,
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"slug": slug, "entries": []}
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage_tag
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a validated Clovis (AMS_1781) vintage snapshot.",
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Skip the post-snapshot cleaned-weekly aggregate step. "
        "Use when re-running the snapshot in isolation; the cleaner is "
        "normally invoked as the final step so the cleaned-weekly artifact "
        "stays in lockstep with the per-pen snapshot.",
    )
    args = parser.parse_args(argv)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    payload = _load_raw()
    vintage = _vintage_tag()

    # ----- Feeder cattle snapshot (the chart-shelf core) -----
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
    _write_manifest_entry(MANIFEST_PATH, "AMS_1781", vintage, snapshot_path, len(df))

    print(f"[snapshot] wrote {snapshot_path} ({len(df)} rows)")
    print(f"[snapshot] wrote {latest_path}")
    print(f"[snapshot] updated {MANIFEST_PATH}")

    # ----- Slaughter cattle snapshot (for the drought-destocking tool) -----
    df_slaughter = _payload_to_slaughter_dataframe(payload, vintage)
    if df_slaughter.empty:
        print(
            "[snapshot] no Slaughter Cattle $/cwt rows in payload — "
            "feeder snapshot still written.",
            file=sys.stderr,
        )
    else:
        slaughter_snapshot_path = PROCESSED_DIR / SLAUGHTER_VINTAGE_TEMPLATE.format(
            vintage=vintage
        )
        slaughter_latest_path = PROCESSED_DIR / SLAUGHTER_LATEST_NAME
        df_slaughter.to_parquet(slaughter_snapshot_path, index=False)
        df_slaughter.to_parquet(slaughter_latest_path, index=False)
        _write_manifest_entry(
            SLAUGHTER_MANIFEST_PATH,
            "AMS_1781_slaughter",
            vintage,
            slaughter_snapshot_path,
            len(df_slaughter),
        )
        print(
            f"[snapshot] wrote {slaughter_snapshot_path} "
            f"({len(df_slaughter)} rows)"
        )
        print(f"[snapshot] wrote {slaughter_latest_path}")
        print(f"[snapshot] updated {SLAUGHTER_MANIFEST_PATH}")

    # Refresh the cleaned-weekly aggregate so the chart pages and the
    # methodology page see consistent inputs after every per-pen snapshot.
    # Imported lazily to avoid pulling numpy when only the per-pen step is
    # needed (tests, debugging).
    if not args.skip_clean:
        from pipelines.clovis import clean as _clean

        rc = _clean.main([])
        if rc != 0:
            print(
                "[snapshot] cleaner returned non-zero; per-pen snapshot still "
                "written. Re-run `python -m pipelines.clovis.clean` after "
                "diagnosing.",
                file=sys.stderr,
            )
            return rc

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
