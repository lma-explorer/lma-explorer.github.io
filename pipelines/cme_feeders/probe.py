"""Sub-task 4.2a probe: verify the futures Excel can be joined to the
Clovis parquet to produce sensible basis numbers.

Reads the user's local CME GF feeder-cattle settle file (path from the
``CLOVIS_FUTURES_XLSX`` environment variable) and the live Clovis combined
parquet. Picks ~10 representative auction dates spread across the Era B
+ MARS span, looks up the corresponding daily settle for each contract
month (with prior-trading-day fallback for weekend auctions), computes
basis for May / November / Nearby, and prints a sanity-check table.

This is read-only diagnostic — does not write any artifact. Once the
basis numbers look reasonable to a domain expert eye, the full
ingestion pipeline (``ingest.py``) and basis derivation
(``../clovis/basis.py``) get built on top.

Usage:
    export CLOVIS_FUTURES_XLSX="/path/to/feederfutures.xlsx"
    python -m pipelines.cme_feeders.probe
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ENV_VAR = "CLOVIS_FUTURES_XLSX"

# Sheet A column layout (after skipping the 4 header rows):
#   0=date, 1=":" separator, 2=JAN, 3=MAR, 4=APR, 5=MAY, 6=AUG, 7=SEP,
#   8=OCT, 9=NOV, 10=Nearby
SHEET_A_COLS = {
    "date": 0,
    "JAN": 2, "MAR": 3, "APR": 4, "MAY": 5,
    "AUG": 6, "SEP": 7, "OCT": 8, "NOV": 9,
    "Nearby": 10,
}
SHEET_A_DATA_FIRST_ROW = 4  # zero-indexed

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED = REPO_ROOT / "data" / "processed"
ERA_B_PARQUET = PROCESSED / "clovis_historical_era_b_latest.parquet"
ERA_B_CSV = PROCESSED / "clovis_historical_era_b_latest.csv"
ERA_A_PARQUET = PROCESSED / "clovis_latest.parquet"


def get_futures_path() -> Optional[Path]:
    val = os.environ.get(ENV_VAR)
    if not val:
        return None
    p = Path(val).expanduser()
    return p if p.exists() else None


def read_futures_daily(path: Path) -> pd.DataFrame:
    """Read sheet A as a tidy DataFrame indexed by date.

    Returns columns: date, JAN, MAR, APR, MAY, AUG, SEP, OCT, NOV, Nearby
    (all settle prices, $/cwt). Drops the placeholder ":" column and the
    rows past the last actual trading day (the file pads forward with
    blank dates).
    """
    raw = pd.read_excel(path, sheet_name="A", header=None,
                        skiprows=SHEET_A_DATA_FIRST_ROW)
    # Build a clean DataFrame with named columns
    cleaned = pd.DataFrame({
        name: raw[idx]
        for name, idx in SHEET_A_COLS.items()
    })
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned = cleaned[cleaned["date"].notna()].copy()
    # Cast contract columns to numeric
    contract_cols = [c for c in cleaned.columns if c != "date"]
    for c in contract_cols:
        cleaned[c] = pd.to_numeric(cleaned[c], errors="coerce")
    cleaned = cleaned.sort_values("date").reset_index(drop=True)
    return cleaned


def read_clovis_combined() -> pd.DataFrame:
    """Read the combined Clovis series (Era B + MARS-era).

    Mirrors ``pipelines.clovis.load.load_clovis_combined`` but tolerates
    sandbox environments without pyarrow by falling back to the Era B
    CSV. Production runs on the user's local machine read both parquets
    and union them.
    """
    parts: list[pd.DataFrame] = []
    if ERA_B_PARQUET.exists():
        try:
            parts.append(pd.read_parquet(ERA_B_PARQUET))
        except Exception:
            pass
    elif ERA_B_CSV.exists():
        parts.append(pd.read_csv(ERA_B_CSV))

    if ERA_A_PARQUET.exists():
        try:
            parts.append(pd.read_parquet(ERA_A_PARQUET))
        except Exception:
            pass

    if not parts:
        raise FileNotFoundError(
            "No Clovis parquet/CSV found. Run pipelines/clovis_historical/"
            "ingest_era_b.py and/or pipelines/clovis/snapshot.py first."
        )
    if len(parts) == 1:
        return parts[0]
    # Concatenate, normalize auction_date, dedupe per the same key the
    # chart pages use.
    combined = pd.concat(parts, ignore_index=True, sort=False)
    combined["auction_date"] = pd.to_datetime(combined["auction_date"])
    DEDUPE_KEY = ["auction_date", "class", "frame", "muscle_grade",
                  "weight_break_low", "weight_break_high"]
    if "vintage" in combined.columns:
        combined["vintage"] = pd.to_datetime(combined["vintage"])
        combined = combined.sort_values("vintage", ascending=False)
    combined = combined.drop_duplicates(subset=DEDUPE_KEY, keep="first")
    return combined.sort_values("auction_date").reset_index(drop=True)


def lookup_settle(futures_df: pd.DataFrame, target_date: pd.Timestamp,
                  contract: str) -> Optional[float]:
    """Return the settle for ``contract`` on ``target_date``. If the
    target date isn't a trading day, falls back to the nearest prior
    trading day (within 5 days)."""
    for delta in range(0, 6):
        probe_date = target_date - pd.Timedelta(days=delta)
        match = futures_df[futures_df["date"] == probe_date]
        if not match.empty:
            val = match[contract].iloc[0]
            if pd.notna(val):
                return float(val)
    return None


def main() -> int:
    fut_path = get_futures_path()
    if fut_path is None:
        print(f"ERROR: {ENV_VAR} unset or file missing.\n"
              "Set it: export "
              "CLOVIS_FUTURES_XLSX='/path/to/feederfutures.xlsx'",
              file=sys.stderr)
        return 2

    print(f"Reading futures from: {fut_path}")
    fut = read_futures_daily(fut_path)
    print(f"Futures: {len(fut)} trading days, "
          f"{fut['date'].min().date()} → {fut['date'].max().date()}")

    print("\nReading Clovis combined parquet/CSV ...")
    clovis = read_clovis_combined()
    clovis["auction_date"] = pd.to_datetime(clovis["auction_date"])
    print(f"Clovis: {len(clovis):,} rows, "
          f"{clovis['auction_date'].min().date()} → "
          f"{clovis['auction_date'].max().date()}")

    # Pick representative auction dates — one per year roughly, plus the
    # most recent auction. Filter to grade-1 M&L Steers 600-699 lb (a
    # typical producer-decision weight) for the sanity-check.
    chart_filter = clovis[
        (clovis["class"] == "Steers")
        & (clovis["frame"] == "Medium and Large")
        & (clovis["muscle_grade"] == "1")
    ].copy()
    weight_col = pd.to_numeric(chart_filter["weight_break_low"], errors="coerce")
    weight_col = weight_col.fillna(pd.to_numeric(chart_filter["avg_weight"], errors="coerce"))
    chart_filter = chart_filter[(weight_col >= 600) & (weight_col < 700)]
    print(f"\nFiltered to grade-1 M&L Steers 600-699 lb: {len(chart_filter):,} rows")

    # One sample auction per year
    chart_filter["year"] = chart_filter["auction_date"].dt.year
    samples = (
        chart_filter
        .groupby("year")
        .first()
        .reset_index()
        .sort_values("auction_date")
    )
    print(f"\nSampling {len(samples)} auction dates (one per year)\n")

    # Print the basis table
    print(f"{'auction_date':<12} {'cash':>8} {'May':>8} {'b_May':>8} "
          f"{'Nov':>8} {'b_Nov':>8} {'Nearby':>8} {'b_Near':>8}")
    print("-" * 80)
    for _, row in samples.iterrows():
        d = row["auction_date"]
        cash = float(row["price_avg"])
        s_may = lookup_settle(fut, d, "MAY")
        s_nov = lookup_settle(fut, d, "NOV")
        s_near = lookup_settle(fut, d, "Nearby")
        b_may = (cash - s_may) if s_may is not None else None
        b_nov = (cash - s_nov) if s_nov is not None else None
        b_near = (cash - s_near) if s_near is not None else None

        def _fmt(v: Optional[float]) -> str:
            return f"{v:>8.2f}" if v is not None else "      —"

        print(f"{d.date()!s:<12} {cash:>8.2f} {_fmt(s_may)} {_fmt(b_may)} "
              f"{_fmt(s_nov)} {_fmt(b_nov)} {_fmt(s_near)} {_fmt(b_near)}")

    # Sanity stats
    print("\n=== Sanity ===")
    print(f"Cash range: ${chart_filter['price_avg'].min():.2f} → "
          f"${chart_filter['price_avg'].max():.2f}/cwt")
    fut_in_window = fut[
        (fut["date"] >= chart_filter["auction_date"].min())
        & (fut["date"] <= chart_filter["auction_date"].max())
    ]
    if not fut_in_window.empty:
        print(f"May settle range in window: "
              f"${fut_in_window['MAY'].min():.2f} → "
              f"${fut_in_window['MAY'].max():.2f}/cwt")
        print(f"Nov settle range in window: "
              f"${fut_in_window['NOV'].min():.2f} → "
              f"${fut_in_window['NOV'].max():.2f}/cwt")

    return 0


if __name__ == "__main__":
    sys.exit(main())
