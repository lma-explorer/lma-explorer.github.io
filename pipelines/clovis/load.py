"""Combined Clovis time-series loader.

Reads the live MARS-era parquet (``clovis_latest.parquet``) and the
one-time historical Era B block (``clovis_historical_era_b_latest.parquet``)
and returns one unioned DataFrame with most-recent-vintage-wins dedupe per
PLAN_4.1 §10b.

Used by the chart pages (``site/price-weight.qmd``,
``site/seasonality.qmd``, ``site/data.qmd``) so they all read from one
canonical loader rather than each replicating the union logic.

Era windows after 9.7a verification:

- Era B TXT (one-time, read-only): 2017-10-04 → 2019-04-10
- Era A MARS (rolling, weekly):    2019-04-17 → present

The era windows are disjoint, so the dedupe is a defensive belt-and-
suspenders — a future correction to either pipeline could in principle
emit a row for the other era's window, and the dedupe ensures the most
recent ``vintage`` wins.

The function is deliberately tolerant of missing files: if Era B is
absent (e.g. a development run before the historical block is checked
out) it returns Era A alone; if Era A is absent (e.g. a transient state
before the first weekly run after a fresh clone) it returns Era B
alone; if both are absent the caller's existing fallback path kicks
in (synthetic stand-in for the chart pages).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

# Dedupe key per PLAN_4.1 §10b — uniquely identifies a single weight-class
# observation within an auction.
DEDUPE_KEY = [
    "auction_date",
    "class",
    "frame",
    "muscle_grade",
    "weight_break_low",
    "weight_break_high",
]


@dataclass
class LoadResult:
    """Returned by :func:`load_clovis_combined`. The chart code uses
    ``df`` for plotting and ``data_source_label`` for the page caption."""

    df: pd.DataFrame
    data_source_label: str
    era_a_rows: int
    era_b_rows: int
    era_a_present: bool
    era_b_present: bool

    @property
    def is_combined(self) -> bool:
        return self.era_a_present and self.era_b_present


def _read_if_exists(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def load_clovis_combined(processed_dir: Path) -> Optional[LoadResult]:
    """Load Era A + Era B, union, dedupe. Returns ``None`` only when both
    files are missing — the caller can then fall back to a synthetic
    placeholder.

    Parameters
    ----------
    processed_dir
        Path to ``data/processed/``. Always passed in (rather than
        computed via ``__file__``) because chart pages run from the
        ``site/`` directory and resolve the path relative to themselves.
    """
    era_a_path = processed_dir / "clovis_latest.parquet"
    era_b_path = processed_dir / "clovis_historical_era_b_latest.parquet"

    df_a = _read_if_exists(era_a_path)
    df_b = _read_if_exists(era_b_path)

    if df_a is None and df_b is None:
        return None

    # Normalize types for the dedupe key. Both eras get auction_date as
    # pandas datetime64 (not python date) so downstream code that calls
    # ``.dt.to_period("M")`` and friends keeps working — Era B's parquet
    # comes back from pyarrow as object-dtype python ``date``, the live
    # MARS parquet comes back as ``datetime64[ns]``; coerce both to the
    # same nanosecond precision. Weight breaks become nullable Int64 so
    # groupby/dedupe behaves consistently when MARS writes plain int and
    # Era B writes Int64-or-NaN.
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["auction_date"] = pd.to_datetime(df["auction_date"])
        for col in ("weight_break_low", "weight_break_high"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        # Don't error on missing optional cols (frame/muscle_grade
        # will be there for both eras; breed/annotation only for Era B).
        return df

    if df_a is not None and df_b is not None:
        df_a_n = _normalize(df_a)
        df_b_n = _normalize(df_b)
        # Outer concat — Era B has extra cols (breed, annotation) that
        # Era A doesn't. Concat fills missing cols with NaN, which is fine.
        combined = pd.concat([df_b_n, df_a_n], ignore_index=True, sort=False)
        # Most-recent-vintage-wins: sort vintage descending, drop_duplicates
        # keeps the first occurrence per key, which is the most recent.
        if "vintage" in combined.columns:
            combined["vintage"] = pd.to_datetime(combined["vintage"]).dt.date
            combined = combined.sort_values("vintage", ascending=False)
        combined = combined.drop_duplicates(subset=DEDUPE_KEY, keep="first")
        # Restore chronological order for downstream consumers that expect it.
        combined = combined.sort_values("auction_date").reset_index(drop=True)
        return LoadResult(
            df=combined,
            data_source_label=(
                "Combined: Era B (Oct 2017–Apr 2019, USDA-AMS Market News archive) "
                "+ Era A (Apr 2019–present, USDA-AMS MARS API)"
            ),
            era_a_rows=len(df_a),
            era_b_rows=len(df_b),
            era_a_present=True,
            era_b_present=True,
        )

    if df_b is not None:
        return LoadResult(
            df=_normalize(df_b),
            data_source_label="Era B only (Oct 2017–Apr 2019, USDA-AMS Market News archive)",
            era_a_rows=0,
            era_b_rows=len(df_b),
            era_a_present=False,
            era_b_present=True,
        )

    assert df_a is not None
    return LoadResult(
        df=_normalize(df_a),
        data_source_label="USDA-AMS MARS feed (AMS_1781)",
        era_a_rows=len(df_a),
        era_b_rows=0,
        era_a_present=True,
        era_b_present=False,
    )
