"""LRP-vs-cash backtest aggregations.

Companion to ``pipelines/lrp/load.py``. Where ``load.py`` powers the
descriptive LRP explorer (``site/lrp.qmd``), this module powers the
comparative backtest page (``site/lrp-backtest.qmd``).

The core statistic
------------------
For each LRP endorsement, the per-cwt LRP advantage over the no-LRP
alternative is::

    lrp_advantage_per_cwt = (indemnity_amount - producer_premium_amount)
                            / total_weight_cwt

**Why cash cancels out.** Consider a producer who will sell at cash
on (or near) the LRP end_date regardless of whether they bought LRP.

- LRP path: revenue = cash_at_end + indemnity_amount
            cost    = producer_premium_amount
            net     = cash_at_end + indemnity - premium
- No-LRP path:   net  = cash_at_end

The difference is::

    LRP_net − Cash_net = indemnity_amount − producer_premium_amount

Per cwt: ``(indemnity − producer_premium) / total_weight_cwt``.

The producer's actual cash price (Clovis or any other auction) drops out
of the algebra. This is what makes the backtest mathematically clean and
robust against basis risk, lot variability, and end-week cash volatility:
none of those affect the LRP-vs-no-LRP delta.

What the formula does *not* capture
-----------------------------------
- **Opportunity cost of capital.** The producer pays the premium up
  front; the indemnity (if any) is paid at end_date. Discounting is
  ignored — endorsement lengths are 13–52 weeks, so the effect is small
  but non-zero for very long endorsements.
- **Basis-risk surprise.** RMA's `actual_end_value` is a CME-futures-
  derived ending value; the producer's cash market may diverge. If a
  producer expected LRP to cover a *cash* drop and the cash market dropped
  while futures held, LRP wouldn't have paid out. The advantage formula
  measures the LRP product's actual payment behavior, not the producer's
  perceived risk.
- **Selection effects.** Producers buy LRP when they expect a downturn.
  An honest historical backtest treats every endorsement as an i.i.d.
  observation, which slightly understates the conditional value to the
  producers who actually self-selected into the program.

These are caveats for the methodology page, not flaws in the formula.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pipelines.lrp.load import (
    DEFAULT_STATE_ABBR,
    DEFAULT_YEAR_WINDOW,
    YEAR_WINDOWS,
    _backtest_subset,
    apply_year_window,
)
from pipelines.lrp.parse import (
    FEEDER_CATTLE_TYPE_CODES_BACKTEST,
)


# Coverage-level bin edges (5-percentage-point ranges).
#
# Why range-bin and not snap-to-menu: RMA does NOT require LRP-Feeder Cattle
# producers to select a coverage level from a fixed menu. Producers choose a
# specific coverage_price in dollars per cwt, and the corpus stores
# coverage_level_pct as the derived ratio (coverage_price / expected_end_value).
# Empirically (TX 2022-2026), only ~44% of endorsements land on round 5%
# values; the remaining 56% are spread continuously across [0.70, 1.00).
# Snapping continuous choices to a discrete menu would erase real signal.
# Range-binning preserves producer choice while pooling enough endorsements
# per bar for the mean to be statistically stable. See
# methodology/lrp-backtest.qmd for the full discussion.
#
# Bins are left-closed, right-open: 0.70 → [0.70, 0.75); 0.95 → [0.95, 1.00);
# 1.00 → [1.00, ∞). The bin's lower edge serves as its identifier (so
# downstream chart code can format "70-75%" labels uniformly).
COVERAGE_BIN_EDGES = [0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00, np.inf]
COVERAGE_BIN_LOWER_EDGES = [0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]


# --------------------------------------------------------------------------
# Per-endorsement statistic
# --------------------------------------------------------------------------


def compute_advantage(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with a ``lrp_advantage_per_cwt`` column.

    Rows where ``total_weight_cwt`` is missing or zero are dropped — they
    represent malformed endorsements that we can't normalize per-cwt
    against. In practice this affects <0.01% of the corpus.

    The returned DataFrame is *not* otherwise filtered. Use
    :func:`backtest_subset` to apply the standard chart-page filter.
    """
    work = df[df["total_weight_cwt"].notna() & (df["total_weight_cwt"] > 0)].copy()
    work["lrp_advantage_per_cwt"] = (
        (work["indemnity_amount"] - work["producer_premium_amount"])
        / work["total_weight_cwt"]
    )
    return work


# --------------------------------------------------------------------------
# Filtered subset for the chart page
# --------------------------------------------------------------------------


def backtest_subset(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
) -> pd.DataFrame:
    """Apply the standard backtest filter and the per-cwt advantage column.

    Returns a DataFrame containing one row per LRP endorsement in the
    selected (state, year-window) slice, restricted to type codes 809-812
    (Steers Weight 1+2 and Heifers Weight 1+2 — the four conventional
    feeder type codes the descriptive LRP explorer also uses), with
    ``lrp_advantage_per_cwt`` populated.
    """
    sub = _backtest_subset(df, state_abbr=state_abbr, year_window=year_window)
    return compute_advantage(sub)


# --------------------------------------------------------------------------
# Aggregation by coverage-level percent
# --------------------------------------------------------------------------


def aggregate_by_coverage(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
) -> pd.DataFrame:
    """Group the backtest subset by coverage_level_pct (binned to 0.01),
    return per-bin counts, mean advantage, and the share of endorsements
    that paid an indemnity.

    Output schema:
        coverage_level_pct : float (e.g., 0.95)
        n_endorsements     : int
        mean_advantage_per_cwt : float ($/cwt; positive = LRP beat cash)
        median_advantage_per_cwt : float
        indemnified_share  : float (0..1)
        total_head         : int
        total_weight_cwt   : float
    """
    sub = backtest_subset(df, state_abbr=state_abbr, year_window=year_window)
    if sub.empty:
        return pd.DataFrame(
            columns=[
                "coverage_level_pct",
                "n_endorsements",
                "mean_advantage_per_cwt",
                "median_advantage_per_cwt",
                "indemnified_share",
                "total_head",
                "total_weight_cwt",
            ]
        )

    # Drop endorsements with no coverage level OR coverage below 0.70.
    # The product floor is 70% per RMA documentation; values below that
    # are typically corrupt early-year rows. Combined with the NaN drop,
    # this affects <0.5% of the corpus.
    sub = sub[
        sub["coverage_level_pct"].notna()
        & (sub["coverage_level_pct"] >= 0.70)
    ].copy()
    if sub.empty:
        return pd.DataFrame(
            columns=[
                "coverage_level_pct",
                "n_endorsements",
                "mean_advantage_per_cwt",
                "median_advantage_per_cwt",
                "indemnified_share",
                "total_head",
                "total_weight_cwt",
            ]
        )
    # Bin to 5-percentage-point ranges. Lower-edge as the bin identifier
    # so downstream chart code can format "70-75%", "75-80%", etc.
    # uniformly. left-closed, right-open: 0.70 binds [0.70, 0.75).
    sub["coverage_bin"] = pd.cut(
        sub["coverage_level_pct"],
        bins=COVERAGE_BIN_EDGES,
        labels=COVERAGE_BIN_LOWER_EDGES,
        include_lowest=True,
        right=False,
    ).astype(float)

    grouped = sub.groupby("coverage_bin", as_index=False).agg(
        n_endorsements=("lrp_advantage_per_cwt", "size"),
        mean_advantage_per_cwt=("lrp_advantage_per_cwt", "mean"),
        median_advantage_per_cwt=("lrp_advantage_per_cwt", "median"),
        n_indemnified=("indemnity_amount", lambda s: (s > 0).sum()),
        total_head=("n_head", "sum"),
        total_weight_cwt=("total_weight_cwt", "sum"),
    )
    grouped["indemnified_share"] = (
        grouped["n_indemnified"] / grouped["n_endorsements"]
    ).fillna(0.0)
    grouped = grouped.rename(columns={"coverage_bin": "coverage_level_pct"})
    grouped = grouped[
        [
            "coverage_level_pct",
            "n_endorsements",
            "mean_advantage_per_cwt",
            "median_advantage_per_cwt",
            "indemnified_share",
            "total_head",
            "total_weight_cwt",
        ]
    ].sort_values("coverage_level_pct").reset_index(drop=True)
    return grouped


# --------------------------------------------------------------------------
# Annual aggregate (for a "trend over years" view, optional)
# --------------------------------------------------------------------------


def aggregate_by_year(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
) -> pd.DataFrame:
    """Group the backtest subset by reinsurance_year. Same statistics as
    aggregate_by_coverage but indexed by year. Useful for the secondary
    'how has LRP advantage moved year-by-year?' chart if added later."""
    sub = backtest_subset(df, state_abbr=state_abbr, year_window=year_window)
    if sub.empty:
        return pd.DataFrame()
    grouped = sub.groupby("reinsurance_year", as_index=False).agg(
        n_endorsements=("lrp_advantage_per_cwt", "size"),
        mean_advantage_per_cwt=("lrp_advantage_per_cwt", "mean"),
        median_advantage_per_cwt=("lrp_advantage_per_cwt", "median"),
        n_indemnified=("indemnity_amount", lambda s: (s > 0).sum()),
    )
    grouped["indemnified_share"] = (
        grouped["n_indemnified"] / grouped["n_endorsements"]
    ).fillna(0.0)
    return grouped.sort_values("reinsurance_year").reset_index(drop=True)


# --------------------------------------------------------------------------
# Sample-size summary (for in-page "AZ has N endorsements at 95% coverage")
# --------------------------------------------------------------------------


def sample_summary(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
) -> dict:
    """Return a small dict summarizing the slice for an inline disclosure
    line on the chart page."""
    sub = backtest_subset(df, state_abbr=state_abbr, year_window=year_window)
    return {
        "n_endorsements": int(len(sub)),
        "n_indemnified": int((sub["indemnity_amount"] > 0).sum()) if not sub.empty else 0,
        "year_min": int(sub["reinsurance_year"].min()) if not sub.empty else 0,
        "year_max": int(sub["reinsurance_year"].max()) if not sub.empty else 0,
        "state_label": "National (all states)" if state_abbr in (None, "All") else state_abbr,
        "indemnified_share": float((sub["indemnity_amount"] > 0).mean()) if not sub.empty else 0.0,
        "mean_advantage_per_cwt": float(sub["lrp_advantage_per_cwt"].mean()) if not sub.empty else 0.0,
    }
