"""Read an external reference Excel workbook into long-format DataFrames
for the validator's parser-consistency check.

This reader is **only** invoked when the user has configured
``CLOVIS_REFERENCE_XLSX`` in their environment to point at a local
Excel workbook that compiles the same underlying USDA-AMS reports
(public-domain) the platform parses. The validator uses this as a
DIY peer review on its own parser: if an independent extraction of
the same source bytes lands in the same neighborhood as our extraction,
the parser is correctly reading the columns. If not, something's wrong.

The workbook itself is never tracked in the public repo and the
file path is taken exclusively from the environment — no hardcoded
fallback. Anyone re-running locally provides their own reference
extract; absence of the env var skips the check silently.

The platform's published series uses USDA's lot-weighted weekly
averages (the values AMS itself publishes); third-party compilations
sometimes use simple averages of the price range or other conventions.
Either approach is academically defensible — the platform's choice is
documented on the public methodology page. The consistency check
exists to validate the parser's extraction, not to claim alignment
with any particular methodology.

Expected workbook layout (sheet ``A1``, Feeder Steers + Feeder Heifers
Medium and Large 1, weekly, 100-lb bins):

- Rows 0-3: workbook metadata
- Row 4: class headers — col 1 "Feeder Steers - Medium & Large 1",
  col 8 "Feeder Heifers - Medium & Large 1"
- Row 5: weight-bin labels — col 1-7 are Steers bins ("3-400" through
  "9-1000"), col 8-13 are Heifers bins ("3-400" through "8-900")
- Row 6: separator
- Row 7+: data — col 0 week-ending date, cols 1-7 Steers prices,
  cols 8-13 Heifers prices, all $/cwt.

Other sheet conventions in the same workbook (B/B1/B2 grade 1-2,
C/C1 grade 2, AA/AB/AC bulls, O/Q/R/S Holstein/Dairy variants) are
out of scope — the chart filter currently shows only grade 1.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

ENV_VAR = "CLOVIS_REFERENCE_XLSX"

# Sheet A1 layout (Steers cols 1-7, Heifers cols 8-13). 100-lb bins.
SHEET_A1_LAYOUT = {
    "Steers": {
        "(300, 400)": 1, "(400, 500)": 2, "(500, 600)": 3,
        "(600, 700)": 4, "(700, 800)": 5, "(800, 900)": 6,
        "(900, 1000)": 7,
    },
    "Heifers": {
        "(300, 400)": 8, "(400, 500)": 9, "(500, 600)": 10,
        "(600, 700)": 11, "(700, 800)": 12, "(800, 900)": 13,
    },
}

DATA_FIRST_ROW = 7  # zero-indexed row where the time series begins


@dataclass
class ReferenceLong:
    """Long-format reference observations for one (sheet, frame, grade) bundle."""

    df: pd.DataFrame  # cols: auction_date, class, weight_break_low, weight_break_high, price_avg
    sheet: str
    frame: str
    muscle_grade: str

    @property
    def date_range(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        d = self.df["auction_date"].dropna()
        return d.min(), d.max()


def get_reference_path() -> Optional[Path]:
    """Return the configured reference workbook path, or None if unset.

    Reads ``CLOVIS_REFERENCE_XLSX`` from the environment. The validator
    uses this to decide whether to run or skip the consistency check.
    """
    val = os.environ.get(ENV_VAR)
    if not val:
        return None
    p = Path(val).expanduser()
    return p if p.exists() else None


def read_reference_grade_1(workbook_path: Path) -> ReferenceLong:
    """Read sheet A1 (Steers + Heifers M&L 1, weekly, 100-lb bins) into long format.

    Returns a ReferenceLong with columns: auction_date (week-ending date),
    class, weight_break_low, weight_break_high, price_avg ($/cwt).
    """
    raw = pd.read_excel(workbook_path, sheet_name="A1", header=None,
                        skiprows=DATA_FIRST_ROW)
    raw = raw.dropna(how="all")
    raw[0] = pd.to_datetime(raw[0], errors="coerce")
    raw = raw[raw[0].notna()].copy()

    rows: list[dict] = []
    for cls_name, bins in SHEET_A1_LAYOUT.items():
        for bin_label, col_idx in bins.items():
            lo, hi = eval(bin_label)  # safe: hard-coded above
            prices = pd.to_numeric(raw[col_idx], errors="coerce")
            for date, price in zip(raw[0], prices):
                if pd.isna(price):
                    continue
                rows.append({
                    "auction_date": date,
                    "class": cls_name,
                    "weight_break_low": lo,
                    "weight_break_high": hi,
                    "price_avg": float(price),
                })

    long_df = pd.DataFrame(rows)
    return ReferenceLong(df=long_df, sheet="A1", frame="Medium and Large", muscle_grade="1")


def annual_median_table(long_df: pd.DataFrame, year_col_name: str = "year") -> pd.DataFrame:
    """Per (year, class, weight_bin) median price_avg.

    Used both for the platform's Era B side and the reference side of
    the consistency comparison. Median (rather than mean) tracks the
    chart's price-weight figures and is robust to outlier weeks.
    """
    df = long_df.copy()
    df[year_col_name] = pd.to_datetime(df["auction_date"]).dt.year
    grouped = (
        df.groupby([year_col_name, "class", "weight_break_low", "weight_break_high"],
                   as_index=False)
          .agg(price_median=("price_avg", "median"),
               n_obs=("price_avg", "size"))
    )
    return grouped


def era_b_to_100lb_bin(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse Era B's native bins to 100-lb bins matching the chart's
    aggregation logic (see ``site/price-weight.qmd``'s data-shape chunk).

    Returns rows with: auction_date, class, weight_break_low, weight_break_high,
    price_avg — same shape as ReferenceLong.df, ready for annual_median_table.
    """
    out = df[df["class"].isin(["Steers", "Heifers"])
             & (df["frame"] == "Medium and Large")
             & (df["muscle_grade"] == "1")].copy()
    raw_weight = pd.to_numeric(out["weight_break_low"], errors="coerce")
    raw_weight = raw_weight.fillna(pd.to_numeric(out["avg_weight"], errors="coerce"))
    out = out[raw_weight.notna()].copy()
    raw_weight = raw_weight[raw_weight.notna()]
    bin_lo = ((raw_weight // 100) * 100).astype(int)
    out["weight_break_low"] = bin_lo
    out["weight_break_high"] = bin_lo + 100
    out = out[(bin_lo >= 300) & (bin_lo < 1000)]
    out["price_avg"] = pd.to_numeric(out["price_avg"], errors="coerce")
    return out[["auction_date", "class", "weight_break_low",
                "weight_break_high", "price_avg"]]
