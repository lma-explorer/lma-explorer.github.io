"""Aggregate per-pen Clovis observations to a cleaned weekly series.

Reads the per-pen artifacts written by ``snapshot.py`` (live MARS-era) and
the one-time historical Era B block, aggregates pens to a weekly series in
100-lb weight classes (``300-399`` … ``800-899`` lb) using a head-count-
weighted mean of ``price_avg``, deflates each weekly observation to the
December-2025 base month using CPI-U All Items NSA, and applies a symmetric
rolling-median ratio test to replace obvious data-reporting spikes with a
local median.

Outputs (one parquet, one CSV log, one manifest entry):

    data/processed/clovis_weekly_cleaned_<YYYY-MM-DD>.parquet
    data/processed/clovis_weekly_cleaned_latest.parquet
    data/processed/clovis_spike_log_<YYYY-MM-DD>.csv
    data/processed/clovis_weekly_cleaned_MANIFEST.json   (append-only)

Cleaned-weekly schema (tidy long, one row per (date, sex, weight_class)):

    auction_date     : datetime64[ns]
    sex              : string  ("Steers" / "Heifers")
    weight_class     : string  ("300-399" … "800-899")
    head_count       : Int64   total head in the weekly aggregate
    n_pens           : Int32   number of pens contributing to the aggregate
    price_nominal    : float   head-count-weighted mean of pen ``price_avg`` ($/cwt)
    price_real       : float   ``price_nominal`` deflated to Dec-2025 dollars
    cpi_at_obs       : float   CPI-U value at the observation month
    spike_replaced   : bool    True if this row was a spike-replacement
    direction        : string  "low" / "high" / "" — only set if replaced
    source_eras      : string  comma-joined source-era set ("MARS" / "EraB" / "MARS,EraB")
    vintage          : datetime64[ns]

Spike replacement (the same rule used by the paper-figures pipeline):

    - 13-week centered rolling median per (sex, weight_class)
    - Flag if abs(ratio - 1) puts the row outside [ratio_lo, ratio_hi]
      (i.e. ratio <= 0.60 or ratio >= 1/0.60 ≈ 1.67), AND
    - the absolute deviation from the local median exceeds ``min_abs`` ($50)
    - Flagged ``price_real`` is replaced with the local median; ``price_nominal``
      is recomputed via ``price_real * (cpi_at_obs / CPI_BASE)`` so the two
      series stay internally consistent
    - Every replacement is appended to ``clovis_spike_log_<vintage>.csv``

The cleaner runs symmetrically on the high and low side; an empty spike log
is itself a deliverable (publishes "no anomalies in this vintage").

Usage:
    python -m pipelines.clovis.clean
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"

ERA_A_PATH = PROCESSED_DIR / "clovis_latest.parquet"
ERA_B_PATH = PROCESSED_DIR / "clovis_historical_era_b_latest.parquet"
CPI_PATH = PROCESSED_DIR / "cpi_latest.parquet"

CLEANED_LATEST_NAME = "clovis_weekly_cleaned_latest.parquet"
CLEANED_VINTAGE_TEMPLATE = "clovis_weekly_cleaned_{vintage}.parquet"
SPIKE_LOG_TEMPLATE = "clovis_spike_log_{vintage}.csv"
CLEANED_MANIFEST_PATH = PROCESSED_DIR / "clovis_weekly_cleaned_MANIFEST.json"

# Filter to the analytical core used on the chart pages.
KEEP_COMMODITY = "Feeder Cattle"
KEEP_FRAME = "Medium and Large"
KEEP_MUSCLE_GRADE = "1"
KEEP_CLASSES = ("Steers", "Heifers")

# 100-lb weight ladder used by the chart pages.
WEIGHT_LADDER = (
    "300-399",
    "400-499",
    "500-599",
    "600-699",
    "700-799",
    "800-899",
)

# Deflation basis: December 2025 — pinned so that recomputed real-dollar
# numbers stay stable across BLS revisions until a deliberate basis change.
DEFLATION_BASIS_PERIOD = "2025-12"


@dataclass(frozen=True)
class CleanConfig:
    """Spike-detection thresholds. Held as a dataclass so tests can override."""

    rolling_window_weeks: int = 13
    rolling_min_periods: int = 5
    ratio_lo: float = 0.60
    ratio_hi: float = 1.0 / 0.60  # ≈ 1.67, symmetric to ratio_lo
    min_abs_dev: float = 50.0


CFG = CleanConfig()


# --------------------------------------------------------------------------- #
# Bin assignment                                                              #
# --------------------------------------------------------------------------- #


def assign_100lb_bin(low: float | None, avg: float | None) -> str | None:
    """Snap a per-pen row to a 100-lb bin label like ``'400-499'``.

    Prefer ``weight_break_low`` (the AMS-published 50-lb bin floor); fall back
    to ``avg_weight`` when the bin is missing (some MARS rows lack
    ``weight_break_low``). Returns ``None`` for pens outside the 300-899 lb
    analytical window.
    """
    w: float | None = None
    if low is not None and not (isinstance(low, float) and math.isnan(low)):
        try:
            w = float(low)
        except (TypeError, ValueError):
            w = None
    if w is None and avg is not None and not (isinstance(avg, float) and math.isnan(avg)):
        try:
            w = float(avg)
        except (TypeError, ValueError):
            w = None
    if w is None or math.isnan(w):
        return None
    lo = int(math.floor(w / 100.0) * 100)
    if lo < 300 or lo > 800:
        return None
    return f"{lo}-{lo + 99}"


# --------------------------------------------------------------------------- #
# Aggregation                                                                 #
# --------------------------------------------------------------------------- #


def aggregate_weekly(pens: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-pen rows to a weekly head-count-weighted DataFrame.

    Returns a tidy-long DataFrame with columns:
    ``auction_date, sex, weight_class, head_count, n_pens, price_nominal,
    source_eras``.
    """
    if pens.empty:
        return pd.DataFrame(
            columns=[
                "auction_date",
                "sex",
                "weight_class",
                "head_count",
                "n_pens",
                "price_nominal",
                "source_eras",
            ]
        )

    # Apply the analytical filter (M&L 1, Steers/Heifers, Feeder Cattle, $/cwt).
    df = pens.loc[
        (pens["commodity"].astype(str) == KEEP_COMMODITY)
        & (pens["class"].isin(KEEP_CLASSES))
        & (pens["frame"].astype(str).str.strip() == KEEP_FRAME)
        & (pens["muscle_grade"].astype(str).str.strip() == KEEP_MUSCLE_GRADE)
    ].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "auction_date",
                "sex",
                "weight_class",
                "head_count",
                "n_pens",
                "price_nominal",
                "source_eras",
            ]
        )

    df["weight_class"] = df.apply(
        lambda r: assign_100lb_bin(r.get("weight_break_low"), r.get("avg_weight")),
        axis=1,
    )
    df = df.dropna(subset=["weight_class", "price_avg"])
    df = df.loc[df["price_avg"] > 0].copy()

    # head_count missing or non-positive → fall back to weight 1 (treat as one
    # head). Keeps the aggregate well-defined; logged elsewhere if needed.
    hc = pd.to_numeric(df["head_count"], errors="coerce").fillna(1.0)
    hc = hc.where(hc > 0, 1.0)
    df["_hc"] = hc
    df["_wp"] = df["price_avg"] * hc

    grp = df.groupby(["auction_date", "class", "weight_class"], as_index=False).agg(
        sum_wp=("_wp", "sum"),
        sum_hc=("_hc", "sum"),
        n_pens=("price_avg", "size"),
        source_eras=("_source_era", lambda s: ",".join(sorted(set(s)))),
    )
    grp["price_nominal"] = grp["sum_wp"] / grp["sum_hc"]
    grp = grp.rename(columns={"class": "sex", "sum_hc": "head_count"})
    grp["head_count"] = grp["head_count"].round().astype("Int64")
    grp["n_pens"] = grp["n_pens"].astype("Int32")
    return grp[
        [
            "auction_date",
            "sex",
            "weight_class",
            "head_count",
            "n_pens",
            "price_nominal",
            "source_eras",
        ]
    ]


# --------------------------------------------------------------------------- #
# CPI deflation                                                               #
# --------------------------------------------------------------------------- #


def load_cpi_map() -> tuple[dict[str, float], float, str]:
    """Read CPI-U from ``cpi_latest.parquet`` and return a {YYYY-MM: cpi_u}
    map plus the basis CPI value for ``DEFLATION_BASIS_PERIOD``.
    """
    cpi = pd.read_parquet(CPI_PATH)
    cpi = cpi.copy()
    cpi["period"] = pd.to_datetime(cpi["period"]).dt.strftime("%Y-%m")
    cpi_map = dict(zip(cpi["period"], cpi["cpi_u"].astype(float)))
    if DEFLATION_BASIS_PERIOD not in cpi_map:
        raise ValueError(
            f"basis period {DEFLATION_BASIS_PERIOD} not present in CPI series; "
            f"latest available is {max(cpi_map)!r}"
        )
    return cpi_map, float(cpi_map[DEFLATION_BASIS_PERIOD]), DEFLATION_BASIS_PERIOD


def deflate(weekly: pd.DataFrame, cpi_map: dict[str, float], cpi_base: float) -> pd.DataFrame:
    """Add ``cpi_at_obs`` and ``price_real`` columns. Drops rows whose
    observation month has no published CPI yet.
    """
    out = weekly.copy()
    out["_yyyymm"] = pd.to_datetime(out["auction_date"]).dt.strftime("%Y-%m")
    out["cpi_at_obs"] = out["_yyyymm"].map(cpi_map)
    n_missing = int(out["cpi_at_obs"].isna().sum())
    if n_missing:
        print(
            f"[clean] dropping {n_missing} weekly row(s) past latest CPI month — "
            f"no deflator yet for those weeks.",
            file=sys.stderr,
        )
    out = out.dropna(subset=["cpi_at_obs"]).copy()
    out["price_real"] = out["price_nominal"] * (cpi_base / out["cpi_at_obs"])
    return out.drop(columns=["_yyyymm"])


# --------------------------------------------------------------------------- #
# Spike replacement                                                           #
# --------------------------------------------------------------------------- #


def replace_spikes(
    weekly_real: pd.DataFrame, cpi_base: float, cfg: CleanConfig = CFG
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Symmetric rolling-median ratio test on ``price_real``.

    For each (sex, weight_class) group, computes a centered rolling-median of
    ``price_real`` over a 13-week window. Flags any row whose ratio to that
    median is at or beyond [ratio_lo, ratio_hi] AND whose absolute deviation
    from the median exceeds ``min_abs_dev`` ($50). Replaces the flagged row's
    ``price_real`` with the local median and recomputes ``price_nominal`` so
    the two series stay internally consistent.

    Returns
    -------
    cleaned_df, spike_log
        ``cleaned_df`` mirrors ``weekly_real`` schema with two added columns
        (``spike_replaced``, ``direction``). ``spike_log`` is one row per
        replacement with the before-and-after numbers.
    """
    out = weekly_real.copy().sort_values(
        ["sex", "weight_class", "auction_date"]
    ).reset_index(drop=True)

    out["spike_replaced"] = False
    out["direction"] = ""

    log_rows: list[dict] = []

    for (sex, wc), idxs in out.groupby(["sex", "weight_class"]).groups.items():
        idxs = list(idxs)
        sub = out.loc[idxs, "price_real"]
        med = sub.rolling(
            cfg.rolling_window_weeks,
            center=True,
            min_periods=cfg.rolling_min_periods,
        ).median()
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = sub / med
        diff = (sub - med).abs()
        mask = (
            ((ratio <= cfg.ratio_lo) | (ratio >= cfg.ratio_hi))
            & (diff > cfg.min_abs_dev)
        ).fillna(False)

        for pos_idx, do_replace in zip(idxs, mask):
            if not do_replace:
                continue
            local_med = float(med.loc[pos_idx])
            orig_real = float(out.at[pos_idx, "price_real"])
            orig_nom = float(out.at[pos_idx, "price_nominal"])
            cpi_obs = float(out.at[pos_idx, "cpi_at_obs"])
            new_nom = local_med * (cpi_obs / cpi_base)
            direction = "low" if orig_real / local_med <= cfg.ratio_lo else "high"

            log_rows.append(
                {
                    "auction_date": out.at[pos_idx, "auction_date"],
                    "sex": sex,
                    "weight_class": wc,
                    "direction": direction,
                    "price_real_orig": round(orig_real, 4),
                    "price_real_replaced": round(local_med, 4),
                    "price_nominal_orig": round(orig_nom, 4),
                    "price_nominal_replaced": round(new_nom, 4),
                    "local_median_real": round(local_med, 4),
                    "ratio": round(orig_real / local_med, 4),
                    "cpi_at_obs": cpi_obs,
                }
            )

            out.at[pos_idx, "price_real"] = local_med
            out.at[pos_idx, "price_nominal"] = new_nom
            out.at[pos_idx, "spike_replaced"] = True
            out.at[pos_idx, "direction"] = direction

    log_df = pd.DataFrame(
        log_rows,
        columns=[
            "auction_date",
            "sex",
            "weight_class",
            "direction",
            "price_real_orig",
            "price_real_replaced",
            "price_nominal_orig",
            "price_nominal_replaced",
            "local_median_real",
            "ratio",
            "cpi_at_obs",
        ],
    )
    return out, log_df


# --------------------------------------------------------------------------- #
# Vintage stamping + manifest                                                 #
# --------------------------------------------------------------------------- #


def _vintage_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _write_manifest_entry(
    vintage_tag: str,
    snapshot_path: Path,
    spike_log_path: Path,
    row_count: int,
    spike_count: int,
    cpi_base: float,
) -> None:
    entry = {
        "vintage": vintage_tag,
        "file": snapshot_path.relative_to(REPO_ROOT).as_posix(),
        "spike_log": spike_log_path.relative_to(REPO_ROOT).as_posix(),
        "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
        "rows": row_count,
        "spikes_replaced": spike_count,
        "deflation_basis": DEFLATION_BASIS_PERIOD,
        "cpi_base": round(cpi_base, 4),
        "rolling_window_weeks": CFG.rolling_window_weeks,
        "ratio_lo": CFG.ratio_lo,
        "ratio_hi": round(CFG.ratio_hi, 4),
        "min_abs_dev": CFG.min_abs_dev,
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if CLEANED_MANIFEST_PATH.exists():
        with CLEANED_MANIFEST_PATH.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"slug": "clovis_weekly_cleaned", "entries": []}
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage_tag
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with CLEANED_MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


# --------------------------------------------------------------------------- #
# Source-aware loaders                                                        #
# --------------------------------------------------------------------------- #


def _load_per_pen_with_era_label() -> pd.DataFrame:
    """Read both per-pen sources and stamp each row with its source era.

    Era-window precedence here matches ``load.py``: most-recent-vintage-wins
    on the dedupe key. We tag rows with their source era so the cleaned
    weekly artifact's ``source_eras`` column is honest about each cell's
    provenance.
    """
    frames: list[pd.DataFrame] = []
    if ERA_A_PATH.exists():
        a = pd.read_parquet(ERA_A_PATH)
        a["_source_era"] = "MARS"
        frames.append(a)
    if ERA_B_PATH.exists():
        b = pd.read_parquet(ERA_B_PATH)
        b["_source_era"] = "EraB"
        frames.append(b)
    if not frames:
        raise FileNotFoundError(
            f"neither {ERA_A_PATH} nor {ERA_B_PATH} exists; nothing to clean"
        )
    df = pd.concat(frames, ignore_index=True, sort=False)
    df["auction_date"] = pd.to_datetime(df["auction_date"])
    return df


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate per-pen Clovis observations to a cleaned weekly series."
    )
    parser.parse_args(argv)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    pens = _load_per_pen_with_era_label()
    weekly = aggregate_weekly(pens)
    if weekly.empty:
        print(
            "[clean] no weekly rows produced after filter+aggregate; "
            "skipping cleaned-artifact write.",
            file=sys.stderr,
        )
        return 1

    cpi_map, cpi_base, _ = load_cpi_map()
    weekly_real = deflate(weekly, cpi_map, cpi_base)

    cleaned, spike_log = replace_spikes(weekly_real, cpi_base)

    vintage = _vintage_tag()
    vintage_d = pd.Timestamp(vintage)
    cleaned["vintage"] = vintage_d

    snapshot_path = PROCESSED_DIR / CLEANED_VINTAGE_TEMPLATE.format(vintage=vintage)
    latest_path = PROCESSED_DIR / CLEANED_LATEST_NAME
    spike_log_path = PROCESSED_DIR / SPIKE_LOG_TEMPLATE.format(vintage=vintage)

    cleaned.to_parquet(snapshot_path, index=False)
    cleaned.to_parquet(latest_path, index=False)
    spike_log.to_csv(spike_log_path, index=False)
    _write_manifest_entry(
        vintage,
        snapshot_path,
        spike_log_path,
        row_count=len(cleaned),
        spike_count=int(cleaned["spike_replaced"].sum()),
        cpi_base=cpi_base,
    )

    print(f"[clean] wrote {snapshot_path} ({len(cleaned)} weekly rows)")
    print(f"[clean] wrote {latest_path}")
    print(
        f"[clean] wrote {spike_log_path} "
        f"({len(spike_log)} replacement(s))"
    )
    print(f"[clean] updated {CLEANED_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
