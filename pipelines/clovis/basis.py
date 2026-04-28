"""Derive cash−futures values per Clovis auction-week × weight bin × class
× contract month. The output ``basis`` field is named for compactness, but
only the (class=Steers, weight_break_low=700) rows constitute strict basis
under the CME GF Feeder Cattle futures contract specification (700–799 lb
Medium-and-Large #1 Steers per the cmegroup.com contract page). Other
(class, weight_break) combinations are weight-class cash−futures spreads,
not directly hedgeable basis. See site/methodology/basis.qmd "What the
chart actually shows" for the formal distinction; the chart page surfaces
the strict-basis vs. spread distinction explicitly to a producer audience.

Joins:
  - Clovis combined parquet (Era B + MARS, via pipelines.clovis.load)
  - CME GF feeder-cattle settle parquet (data/raw/cme/cme_feeders_latest.parquet,
    gitignored — the platform does NOT redistribute raw CME settles).

For each Clovis observation (auction_date × class × frame × muscle_grade ×
weight_break_low/high), aggregates lots within the same (auction_date, class,
weight_bin_100lb, frame, muscle_grade) to a single cash price (head-weighted
where head_count is available, simple mean otherwise) and joins to the daily
settle for {MAY, NOV, NEARBY} contracts on the auction date (with prior-
trading-day fallback up to 5 days). The ``settle`` value is used in memory
to compute ``basis = cash − settle`` and is intentionally NOT written to the
output parquet — only the derived ``basis`` statistic is committed under
CC-BY-4.0; raw CME settles are licensed proprietarily by CME and are not
redistributed by this repository.

Output (long-format, ~16k rows for the current 8.6-year span):

    auction_date     date
    class            string ("Steers" / "Heifers" / "Bulls")
    frame            string
    muscle_grade     string
    weight_break_low  Int32
    weight_break_high Int32
    price_avg_cash   float   ($/cwt, Clovis weighted-avg over lots in the bin)
    n_lots           Int32   (count of Clovis lots aggregated into this row)
    contract_month   string  ("MAY" / "NOV" / "NEARBY")
    basis            float   ($/cwt, = price_avg_cash − settle, with `settle`
                             observed in memory and NOT persisted in this output)
                             NOTE: strict hedging basis only for (class=Steers,
                             weight_break_low=700). Other rows are weight-class
                             cash−futures spreads — see methodology/basis.qmd.
    vintage          date

Output paths:

    data/processed/clovis_basis_<vintage>.parquet
    data/processed/clovis_basis_latest.parquet
    data/processed/clovis_basis_MANIFEST.json

The chart (sub-task 4.2d) reads the latest parquet and aggregates at
render time — per-(week-of-year × class × bin × contract), giving
average + IQR + min/max bands across the available years.

Usage:
    python -m pipelines.clovis.basis
    python -m pipelines.clovis.basis --csv-fallback
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED = REPO_ROOT / "data" / "processed"
# CME settles are read from a gitignored local-only directory. The platform
# does not redistribute raw CME settles (see LICENSE-DATA.md); the basis
# pipeline reads them in-memory, computes basis = cash − settle, and writes
# only the derived basis statistic to data/processed/.
CME_RAW_DIR = REPO_ROOT / "data" / "raw" / "cme"
CME_LATEST_PARQUET = CME_RAW_DIR / "cme_feeders_latest.parquet"
CME_LATEST_CSV = CME_RAW_DIR / "cme_feeders_latest.csv"
MANIFEST_PATH = PROCESSED / "clovis_basis_MANIFEST.json"

CONTRACTS = ["MAY", "NOV", "NEARBY"]
SETTLE_FALLBACK_DAYS = 5  # auctions on weekends/holidays back-fill to the
                           # most recent trading day within this window

SLUG = "CLOVIS_BASIS"


def _vintage_tag() -> date:
    return date.today()


def _read_cme_settles() -> pd.DataFrame:
    """Read the CME feeder-cattle daily-settle long-format parquet/CSV.
    Returns columns: date, contract_month, settle."""
    if CME_LATEST_PARQUET.exists():
        try:
            df = pd.read_parquet(CME_LATEST_PARQUET)
        except Exception:
            df = None
        else:
            return df[["date", "contract_month", "settle"]]
    if CME_LATEST_CSV.exists():
        df = pd.read_csv(CME_LATEST_CSV)
        return df[["date", "contract_month", "settle"]]
    raise FileNotFoundError(
        "No CME settles parquet/CSV found. Run "
        "pipelines.cme_feeders.ingest first."
    )


def _try_read(parquet: Path, csv_fallback: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """Try parquet first, fall back to CSV if pyarrow is unavailable.
    Returns None if neither yields a DataFrame."""
    if parquet.exists():
        try:
            return pd.read_parquet(parquet)
        except Exception:
            pass  # likely missing pyarrow — try CSV next
    if csv_fallback is not None and csv_fallback.exists():
        return pd.read_csv(csv_fallback)
    return None


def _read_clovis_combined() -> pd.DataFrame:
    """Read the combined Clovis series (Era B + MARS-era). Each era reads
    parquet first and falls back to CSV if pyarrow is unavailable —
    matters for sandbox dev environments; production runs read parquet."""
    parts: list[pd.DataFrame] = []
    era_b = _try_read(
        PROCESSED / "clovis_historical_era_b_latest.parquet",
        PROCESSED / "clovis_historical_era_b_latest.csv",
    )
    if era_b is not None:
        parts.append(era_b)
    era_a = _try_read(
        PROCESSED / "clovis_latest.parquet",
        PROCESSED / "clovis_latest.csv",
    )
    if era_a is not None:
        parts.append(era_a)
    if not parts:
        raise FileNotFoundError(
            "No Clovis parquet/CSV found. Run pipelines/clovis_historical/"
            "ingest_era_b.py and/or pipelines/clovis/snapshot.py first."
        )
    # Drop empty frames before concat to silence the pandas FutureWarning
    # about all-NA columns affecting result dtypes.
    parts = [p for p in parts if not p.empty]
    if len(parts) == 1:
        return parts[0]
    return pd.concat(parts, ignore_index=True, sort=False)


def _to_100lb_bin(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror the chart pages' bin derivation: 100-lb bins from
    weight_break_low (or avg_weight as fallback)."""
    out = df.copy()
    raw_w = pd.to_numeric(out["weight_break_low"], errors="coerce")
    raw_w = raw_w.fillna(pd.to_numeric(out["avg_weight"], errors="coerce"))
    out = out[raw_w.notna()].copy()
    raw_w = raw_w[raw_w.notna()]
    bin_lo = ((raw_w // 100) * 100).astype(int)
    out["bin_lo"] = bin_lo
    out["bin_hi"] = bin_lo + 100
    out = out[(bin_lo >= 300) & (bin_lo < 1000)].copy()
    return out


def aggregate_clovis_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Clovis lot-level rows to one row per (auction_date,
    class, frame, muscle_grade, bin_lo, bin_hi). Cash price is
    head-weighted when ``head_count`` is present, simple mean otherwise.

    Filters out rows whose auction_date or price_avg is null. Keeps all
    classes and grades — the chart filters at render time, so we
    preserve breadth here.
    """
    df = df.copy()
    df["auction_date"] = pd.to_datetime(df["auction_date"]).dt.date
    df["price_avg"] = pd.to_numeric(df["price_avg"], errors="coerce")
    df = df[df["auction_date"].notna() & df["price_avg"].notna()]

    binned = _to_100lb_bin(df)
    if "head_count" in binned.columns:
        binned["head_count"] = pd.to_numeric(binned["head_count"], errors="coerce").fillna(1)
    else:
        binned["head_count"] = 1

    # Head-weighted mean: sum(price * head) / sum(head)
    binned["price_x_head"] = binned["price_avg"] * binned["head_count"]
    grouped = (
        binned.groupby(
            ["auction_date", "class", "frame", "muscle_grade", "bin_lo", "bin_hi"],
            as_index=False,
        )
        .agg(
            price_x_head_sum=("price_x_head", "sum"),
            head_total=("head_count", "sum"),
            n_lots=("price_avg", "count"),
        )
    )
    grouped["price_avg_cash"] = grouped["price_x_head_sum"] / grouped["head_total"]
    grouped = grouped.drop(columns=["price_x_head_sum"])
    grouped = grouped.rename(columns={
        "bin_lo": "weight_break_low",
        "bin_hi": "weight_break_high",
    })
    return grouped


def lookup_settles(weekly_clovis: pd.DataFrame, cme: pd.DataFrame) -> pd.DataFrame:
    """For each unique auction_date in weekly_clovis, find the settle for
    each contract in CONTRACTS, with prior-trading-day fallback. Returns
    a long DataFrame: auction_date, contract_month, settle, settle_date.

    Implementation: for each contract, build a Series indexed by date
    (only days where that contract has a settle), and use as_of-style
    lookup via reindex+ffill within the fallback window.
    """
    cme = cme.copy()
    cme["date"] = pd.to_datetime(cme["date"]).dt.date

    auction_dates = sorted(set(weekly_clovis["auction_date"].tolist()))
    rows: list[dict] = []
    for contract in CONTRACTS:
        sub = cme[cme["contract_month"] == contract].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("date").set_index("date")
        # Build date-aligned settle series
        settle_series = sub["settle"]
        for ad in auction_dates:
            # Walk back up to SETTLE_FALLBACK_DAYS for the most recent
            # trading day with a settle for this contract.
            settle_val = None
            settle_date = None
            for delta in range(0, SETTLE_FALLBACK_DAYS + 1):
                probe = ad - pd.Timedelta(days=delta).to_pytimedelta()
                # convert pd.Timedelta-style probe back to date
                if hasattr(probe, "date"):
                    probe = probe.date()
                if probe in settle_series.index:
                    val = settle_series.loc[probe]
                    if pd.notna(val):
                        settle_val = float(val)
                        settle_date = probe
                        break
            if settle_val is not None:
                rows.append({
                    "auction_date": ad,
                    "contract_month": contract,
                    "settle": settle_val,
                    "settle_date": settle_date,
                })
    return pd.DataFrame(rows)


def derive_basis(weekly_clovis: pd.DataFrame, settles: pd.DataFrame,
                 vintage: date) -> pd.DataFrame:
    """Cross-join weekly_clovis × settles on auction_date, compute
    basis = price_avg_cash − settle. Long format.

    The ``settle`` and ``settle_date`` columns are used IN MEMORY to compute
    the derived ``basis`` statistic and are then dropped before returning —
    raw CME settles are not redistributed by this repository. See the module
    docstring for the licensing posture.
    """
    joined = weekly_clovis.merge(settles, on="auction_date", how="left")
    joined["basis"] = joined["price_avg_cash"] - joined["settle"]
    joined["vintage"] = vintage
    # Output schema deliberately excludes `settle` and `settle_date` — those
    # are CME-licensed values used only in memory to derive basis. Only the
    # derived `basis` value is persisted.
    cols = [
        "auction_date", "class", "frame", "muscle_grade",
        "weight_break_low", "weight_break_high",
        "price_avg_cash", "n_lots",
        "contract_month", "basis",
        "vintage",
    ]
    return joined[cols].sort_values(
        ["auction_date", "class", "weight_break_low", "contract_month"]
    ).reset_index(drop=True)


def write_outputs(df: pd.DataFrame, vintage: date, csv_fallback: bool) -> dict:
    PROCESSED.mkdir(parents=True, exist_ok=True)
    suffix = "csv" if csv_fallback else "parquet"
    snap_path = PROCESSED / f"clovis_basis_{vintage.isoformat()}.{suffix}"
    latest_path = PROCESSED / f"clovis_basis_latest.{suffix}"
    if csv_fallback:
        df.to_csv(snap_path, index=False)
        df.to_csv(latest_path, index=False)
    else:
        df.to_parquet(snap_path, index=False)
        df.to_parquet(latest_path, index=False)

    sha256 = hashlib.sha256(snap_path.read_bytes()).hexdigest()
    auction_dates = pd.to_datetime(df["auction_date"]).dt.date
    coverage = f"{auction_dates.min().isoformat()}..{auction_dates.max().isoformat()}"

    rows_with_basis = int(df["basis"].notna().sum())
    rows_total = int(len(df))

    return {
        "vintage": vintage.isoformat(),
        "file": str(snap_path.relative_to(REPO_ROOT)),
        "sha256": sha256,
        "rows": rows_total,
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
        # Basis-specific extras
        "latest_path": str(latest_path.relative_to(REPO_ROOT)),
        "rows_with_basis": rows_with_basis,
        "rows_missing_settle": rows_total - rows_with_basis,
        "auction_week_count": int(auction_dates.nunique()),
        "coverage": coverage,
        "contract_months": sorted(df["contract_month"].dropna().unique().tolist()),
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
    manifest["entries"] = [
        e for e in manifest["entries"] if e.get("vintage") != entry["vintage"]
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--vintage", help="ISO date YYYY-MM-DD (default: today)")
    ap.add_argument("--csv-fallback", action="store_true")
    args = ap.parse_args(argv)

    vintage = (
        date.fromisoformat(args.vintage) if args.vintage else _vintage_tag()
    )

    print("Reading Clovis combined data ...")
    raw = _read_clovis_combined()
    print(f"  {len(raw):,} lot-level rows from Clovis")

    print("\nReading CME feeder settles ...")
    cme = _read_cme_settles()
    print(f"  {len(cme):,} settle rows ({cme['date'].min()} → {cme['date'].max()}, "
          f"{cme['contract_month'].nunique()} contract months)")

    print("\nAggregating Clovis lots → one cash price per (week × class × bin × frame × grade) ...")
    weekly = aggregate_clovis_weekly(raw)
    print(f"  {len(weekly):,} weekly aggregated rows "
          f"({weekly['auction_date'].nunique()} distinct weeks)")

    print(f"\nLooking up settles for contracts {CONTRACTS} (with up to "
          f"{SETTLE_FALLBACK_DAYS}-day prior-trading-day fallback) ...")
    settles = lookup_settles(weekly, cme)
    print(f"  {len(settles):,} (auction_date, contract) settle rows resolved")

    print("\nDeriving basis = price_avg_cash − settle ...")
    basis = derive_basis(weekly, settles, vintage)
    print(f"  {len(basis):,} basis rows")
    rows_with_basis = int(basis["basis"].notna().sum())
    print(f"  {rows_with_basis:,} have non-null basis "
          f"({len(basis) - rows_with_basis:,} missing settle for some contract)")

    # Sanity: print basis range per contract
    print("\nBasis range per contract:")
    for c in CONTRACTS:
        s = basis[basis["contract_month"] == c]["basis"].dropna()
        if not s.empty:
            print(f"  {c:>6}: {s.min():>+8.2f} → {s.max():>+8.2f} $/cwt   "
                  f"(median {s.median():>+6.2f}, n={len(s):,})")

    print("\n=== Writing outputs ===")
    entry = write_outputs(basis, vintage, args.csv_fallback)
    print(json.dumps(entry, indent=2))
    append_manifest(entry)
    print(f"\nMANIFEST appended: {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
