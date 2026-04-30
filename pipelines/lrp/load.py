"""Render-time loader and aggregate views for the LRP chart page.

Reads ``data/processed/lrp_latest.parquet`` once and exposes a few
aggregate-view functions that ``site/lrp.qmd`` consumes. No separate
aggregate parquets are written to disk — the corpus is small enough
(~210k rows, ~7.7 MB) that aggregation at render time is cheap, and
keeping the source-of-truth single (one parquet, not 1+N) avoids a
fan-out of MANIFEST entries to maintain.

The aggregations are organized around four groups of LRP information,
mirroring the structure of the chart page:

    1. PARTICIPATION / USE — endorsements_earning_premium,
       endorsements_indemnified, n_head, total_weight_cwt,
       liability_amount.
    2. COST / SUBSIDY — total_premium_amount, producer_premium_amount,
       subsidy_amount, cost_per_cwt (mean), rate (mean).
    3. COVERAGE DESIGN — endorsement_length (mean weeks), coverage_price
       (mean), expected_end_value (mean), coverage_level_pct (mean).
       Sales effective and end dates are at the row level, not aggregated
       here — surfaced in the corpus row count and date range.
    4. OUTCOME / PAYOUT — indemnity_amount, indemnified_share (=
       endorsements_indemnified / endorsements_earning_premium),
       indemnity_to_producer_premium_ratio, subsidy_share, producer_share
       of total premium.

Used by ``site/lrp.qmd``. Mirrors the design of ``pipelines/clovis/load.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from pipelines.lrp.parse import (
    FEEDER_CATTLE_TYPE_CODES_BACKTEST,
    FEEDER_CATTLE_COMMODITY_CODE,
    LRP_PLAN_CODE,
)

# State-level rollup default lower bound. Pre-2021 RMA reporting was
# mostly national-aggregate (state_abbr == "XX"); the choropleth therefore
# defaults to 2021+. See pipelines/lrp/README.md "Schema-evolution findings"
# and the project_lrp_schema_evolution.md memory note.
CHOROPLETH_DEFAULT_MIN_YEAR = 2021

# Year-window strings used by the chart's button row. Mirrors the chart-shelf
# convention used by price-weight, seasonality, weekly-trends, basis,
# sell-now-compare. Keys are the button labels; values are dicts with
# ``min_year`` (int, inclusive) and an optional ``max_year`` (int, inclusive).
# The actual numeric bounds are computed at call-time relative to the
# corpus's most recent year, so "last_5" stays current as new years land.
YEAR_WINDOWS = ("all", "last_5", "last_3", "latest")
DEFAULT_YEAR_WINDOW = "all"

# Producer-reference state — Arizona. Defaulted in dropdowns. See
# project_user_state_arizona.md memory note.
DEFAULT_STATE_ABBR = "AZ"


@dataclass
class LoadResult:
    """Returned by :func:`load_corpus`. The chart code uses ``df`` for
    aggregation and ``data_source_label`` for the page caption."""

    df: pd.DataFrame
    data_source_label: str
    rows: int
    file_present: bool


def _read_if_exists(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_parquet(path)


def load_corpus(processed_dir: Path) -> Optional[LoadResult]:
    """Load the full LRP corpus from ``lrp_latest.parquet``.

    Returns ``None`` if the parquet is missing — the caller can then
    fall back to a synthetic stand-in (matching the Clovis loader pattern).

    Parameters
    ----------
    processed_dir
        Path to ``data/processed/``. Always passed in (rather than
        computed via ``__file__``) because ``site/lrp.qmd`` runs from the
        ``site/`` directory and resolves the path relative to itself.
    """
    path = processed_dir / "lrp_latest.parquet"
    df = _read_if_exists(path)
    if df is None:
        return None
    return LoadResult(
        df=df,
        data_source_label=(
            "USDA RMA Summary of Business — Livestock Risk Protection "
            "(pubfs-rma annual zips, 2003-present)"
        ),
        rows=len(df),
        file_present=True,
    )


# --------------------------------------------------------------------------
# Year-window and state-list helpers
# --------------------------------------------------------------------------


def _year_bounds(df: pd.DataFrame, window: str) -> tuple[int, int]:
    """Return ``(min_year, max_year)`` (inclusive) for a YEAR_WINDOWS string.

    ``current_year`` is taken as the maximum reinsurance_year present in the
    corpus, NOT today's calendar year — keeps the chart honest if RMA hasn't
    yet posted the current year's data.
    """
    if window not in YEAR_WINDOWS:
        raise ValueError(f"window must be one of {YEAR_WINDOWS}; got {window!r}")
    years = df["reinsurance_year"].dropna()
    if years.empty:
        return (0, 0)
    current = int(years.max())
    floor = int(years.min())
    if window == "all":
        return (floor, current)
    if window == "last_5":
        return (max(current - 4, floor), current)
    if window == "last_3":
        return (max(current - 2, floor), current)
    if window == "latest":
        return (current, current)
    raise AssertionError(f"unreachable: {window}")


def apply_year_window(df: pd.DataFrame, window: str) -> pd.DataFrame:
    """Filter ``df`` to rows in the named year window. Pure read; no copy
    unless the filter actually trims rows."""
    lo, hi = _year_bounds(df, window)
    return df[(df["reinsurance_year"] >= lo) & (df["reinsurance_year"] <= hi)]


def available_states(df: pd.DataFrame) -> list[str]:
    """Return the list of state abbreviations present in the corpus, sorted.
    ``DEFAULT_STATE_ABBR`` (Arizona) is moved to the front when present;
    ``XX`` (national-aggregate placeholder) is excluded. The dropdown also
    presents an ``All`` option above this list — that's a chart-page
    concern, not a loader concern, so it isn't returned here.
    """
    seen = set(df["state_abbr"].dropna().unique().tolist())
    seen.discard("XX")
    out = sorted(seen)
    if DEFAULT_STATE_ABBR in seen:
        out = [DEFAULT_STATE_ABBR] + [s for s in out if s != DEFAULT_STATE_ABBR]
    return out


def available_years(df: pd.DataFrame) -> list[int]:
    """Return the sorted list of reinsurance years present in the corpus."""
    years = df["reinsurance_year"].dropna().astype(int).unique().tolist()
    return sorted(years)


# --------------------------------------------------------------------------
# Common subset filter (used by all aggregate functions)
# --------------------------------------------------------------------------


def _backtest_subset(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
    include_xx: bool = False,
) -> pd.DataFrame:
    """Apply the chart-page's standard filter: backtest type-code subset
    (809-812: Steers/Heifers Weight 1+2), optional state filter, year-window
    filter, and the ``XX`` exclusion that defaults on for charts that render
    on a state map.
    """
    sub = df[df["type_code"].isin(FEEDER_CATTLE_TYPE_CODES_BACKTEST)]
    sub = apply_year_window(sub, year_window)
    if state_abbr is not None and state_abbr != "All":
        sub = sub[sub["state_abbr"] == state_abbr]
    elif not include_xx:
        sub = sub[sub["state_abbr"] != "XX"]
    return sub.copy()


# --------------------------------------------------------------------------
# Core aggregations — yearly, state, county
# --------------------------------------------------------------------------


def yearly_summary(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year_window: str = DEFAULT_YEAR_WINDOW,
) -> pd.DataFrame:
    """Per-year aggregate within the backtest subset.

    Returns one row per reinsurance year, with columns covering all four
    LRP information groups and the four derived ratios. If ``state_abbr``
    is None, the rollup is national-aggregate (XX excluded — see below);
    if a state code is given, the rollup is state-specific. ``"All"`` is
    treated as the national-aggregate request.

    XX-row policy: when ``state_abbr is None``, XX (RMA's national-only
    aggregate placeholder) is INCLUDED in the year totals — these rows
    represent real LRP volume reported without state attribution. This is
    different from the choropleth's exclusion (the map can't render XX).

    Returns columns:
        reinsurance_year                   : int
        n_endorsements_earning             : int
        n_endorsements_indemn              : int
        n_head                             : int
        total_weight_cwt                   : float
        liability_amount                   : int ($)
        producer_premium_amount            : int ($)
        subsidy_amount                     : int ($)
        total_premium_amount               : int ($)
        indemnity_amount                   : int ($, may be negative)
        avg_endorsement_length_weeks       : float
        avg_coverage_price                 : float ($/cwt, weighted by n_head)
        avg_expected_end_value             : float ($/cwt, weighted by n_head)
        avg_coverage_level_pct             : float (weighted by n_head)
        avg_cost_per_cwt                   : float ($/cwt, weighted by n_head)
        indemnified_share                  : float (n_indemn / n_earning)
        indemnity_to_producer_premium_rate : float (indemn / producer_prem)
        subsidy_share                      : float (subsidy / total_premium)
        producer_share_of_premium          : float (producer / total_premium)
    """
    sub = df[df["type_code"].isin(FEEDER_CATTLE_TYPE_CODES_BACKTEST)]
    sub = apply_year_window(sub, year_window)
    if state_abbr is not None and state_abbr != "All":
        sub = sub[sub["state_abbr"] == state_abbr]
    sub = sub.copy()

    # Determine the year-range bounds for padding: the full window range,
    # not the (possibly state-narrowed) range. This ensures a state with
    # sparse historical participation still shows the full year axis on
    # the time-series chart (with zero bars for years it didn't participate).
    _full_lo, _full_hi = _year_bounds(df, year_window)

    if sub.empty:
        # Even with no data, return zero-filled rows so the chart's x-axis
        # spans the chosen window.
        empty_padded = pd.DataFrame({"reinsurance_year": list(range(_full_lo, _full_hi + 1))})
        for col in _yearly_summary_columns():
            if col == "reinsurance_year":
                continue
            empty_padded[col] = 0
        return empty_padded

    grouped = sub.groupby("reinsurance_year", dropna=True)

    # Sums of the integer money / count columns
    out = grouped.agg(
        n_endorsements_earning=("n_endorsements_earning", "sum"),
        n_endorsements_indemn=("n_endorsements_indemn", "sum"),
        n_head=("n_head", "sum"),
        total_weight_cwt=("total_weight_cwt", "sum"),
        liability_amount=("liability_amount", "sum"),
        producer_premium_amount=("producer_premium_amount", "sum"),
        subsidy_amount=("subsidy_amount", "sum"),
        total_premium_amount=("total_premium_amount", "sum"),
        indemnity_amount=("indemnity_amount", "sum"),
    ).reset_index()

    # Head-weighted means for the per-cwt and rate-style fields
    weighted_means = sub.groupby("reinsurance_year").apply(
        lambda g: pd.Series(_head_weighted_means(g))
    ).reset_index()
    out = out.merge(weighted_means, on="reinsurance_year", how="left")

    # Pad missing years in the window with zero rows so the chart's x-axis
    # spans the full window even for states that started participating late.
    # AZ (the producer-reference state) doesn't have meaningful pre-2022 data
    # for example; padding makes that visible as flat zeros rather than
    # collapsing the axis.
    full_years = pd.DataFrame({"reinsurance_year": list(range(_full_lo, _full_hi + 1))})
    out = full_years.merge(out, on="reinsurance_year", how="left")

    # Defensive: dtype hygiene + zero-fill for ratios that divide by counts.
    # NaN comes from two sources: years padded above where the state had no
    # participation, and the original groupby's missing-cell defaults.
    for col in (
        "n_endorsements_earning", "n_endorsements_indemn",
        "n_head", "liability_amount",
        "producer_premium_amount", "subsidy_amount",
        "total_premium_amount", "indemnity_amount",
    ):
        out[col] = out[col].fillna(0).astype("int64")

    # The head-weighted mean columns also need fillna for padded years.
    for col in (
        "avg_endorsement_length_weeks", "avg_coverage_price",
        "avg_expected_end_value", "avg_coverage_level_pct",
        "avg_cost_per_cwt",
    ):
        if col in out.columns:
            out[col] = out[col].fillna(0.0)

    # Derived ratios — guard against divide-by-zero
    out["indemnified_share"] = _safe_div(
        out["n_endorsements_indemn"], out["n_endorsements_earning"]
    )
    out["indemnity_to_producer_premium_rate"] = _safe_div(
        out["indemnity_amount"], out["producer_premium_amount"]
    )
    out["subsidy_share"] = _safe_div(
        out["subsidy_amount"], out["total_premium_amount"]
    )
    out["producer_share_of_premium"] = _safe_div(
        out["producer_premium_amount"], out["total_premium_amount"]
    )

    return out.sort_values("reinsurance_year").reset_index(drop=True)


def state_summary(
    df: pd.DataFrame,
    *,
    year_window: str = "last_5",
) -> pd.DataFrame:
    """Per-state aggregate within the backtest subset, year-bounded.

    Used to drive the state-level choropleth + benchmark table. Excludes
    rows where ``state_abbr == "XX"`` — those don't render on a state map.

    Default year window is ``last_5`` (recency-weighted view of where the
    program is active, which is what the choropleth communicates best).
    """
    sub = _backtest_subset(df, state_abbr=None, year_window=year_window)
    if sub.empty:
        return pd.DataFrame(columns=_state_summary_columns())

    grouped = sub.groupby("state_abbr", dropna=True)
    out = grouped.agg(
        n_endorsements_earning=("n_endorsements_earning", "sum"),
        n_endorsements_indemn=("n_endorsements_indemn", "sum"),
        n_head=("n_head", "sum"),
        total_weight_cwt=("total_weight_cwt", "sum"),
        liability_amount=("liability_amount", "sum"),
        producer_premium_amount=("producer_premium_amount", "sum"),
        subsidy_amount=("subsidy_amount", "sum"),
        total_premium_amount=("total_premium_amount", "sum"),
        indemnity_amount=("indemnity_amount", "sum"),
    ).reset_index()

    for col in (
        "n_endorsements_earning", "n_endorsements_indemn",
        "n_head", "liability_amount",
        "producer_premium_amount", "subsidy_amount",
        "total_premium_amount", "indemnity_amount",
    ):
        out[col] = out[col].fillna(0).astype("int64")

    out["indemnified_share"] = _safe_div(
        out["n_endorsements_indemn"], out["n_endorsements_earning"]
    )
    out["indemnity_to_producer_premium_rate"] = _safe_div(
        out["indemnity_amount"], out["producer_premium_amount"]
    )
    out["subsidy_share"] = _safe_div(
        out["subsidy_amount"], out["total_premium_amount"]
    )

    return out.sort_values("producer_premium_amount", ascending=False).reset_index(drop=True)


def county_summary(
    df: pd.DataFrame,
    *,
    state_abbr: str,
    year_window: str = "last_5",
) -> pd.DataFrame:
    """Per-county aggregate for ONE state, within the backtest subset.

    Returns one row per (county_fips, county_name) within ``state_abbr``.
    Used for the county-level choropleth drill-down. The FIPS code is the
    5-digit identifier (``state_fips`` + ``county_fips``) Plotly's bundled
    geojson keys on.
    """
    sub = _backtest_subset(
        df, state_abbr=state_abbr, year_window=year_window, include_xx=False
    )
    if sub.empty:
        return pd.DataFrame(
            columns=["state_abbr", "state_fips", "county_fips", "fips_5",
                     "county_name", "n_endorsements_earning",
                     "n_head", "producer_premium_amount", "indemnity_amount"]
        )

    sub = sub.copy()
    sub["fips_5"] = sub["state_fips"].astype("string").str.zfill(2) + \
        sub["county_fips"].astype("string").str.zfill(3)

    grouped = sub.groupby(
        ["state_abbr", "state_fips", "county_fips", "fips_5", "county_name"],
        dropna=False,
    )
    out = grouped.agg(
        n_endorsements_earning=("n_endorsements_earning", "sum"),
        n_endorsements_indemn=("n_endorsements_indemn", "sum"),
        n_head=("n_head", "sum"),
        producer_premium_amount=("producer_premium_amount", "sum"),
        indemnity_amount=("indemnity_amount", "sum"),
    ).reset_index()

    for col in (
        "n_endorsements_earning", "n_endorsements_indemn",
        "n_head", "producer_premium_amount", "indemnity_amount",
    ):
        out[col] = out[col].fillna(0).astype("int64")

    return out.sort_values("producer_premium_amount", ascending=False).reset_index(drop=True)


def summary_metrics(
    df: pd.DataFrame,
    *,
    state_abbr: Optional[str] = None,
    year: Optional[int] = None,
) -> dict:
    """Return a single-(state, year) summary as a dict of values.

    Used by the chart page's at-a-glance metrics block. If ``year`` is
    None, defaults to the most recent year in the corpus's backtest subset.
    If ``state_abbr`` is None, returns national-aggregate (XX excluded).

    Returns a dict with keys covering all four groups and the derived ratios.
    """
    sub = df[df["type_code"].isin(FEEDER_CATTLE_TYPE_CODES_BACKTEST)]
    if state_abbr is not None and state_abbr != "All":
        sub = sub[sub["state_abbr"] == state_abbr]
    else:
        sub = sub[sub["state_abbr"] != "XX"]
    if year is None:
        years = sub["reinsurance_year"].dropna()
        year = int(years.max()) if not years.empty else 0
    sub = sub[sub["reinsurance_year"] == year]

    if sub.empty:
        return {"state_abbr": state_abbr or "All", "year": year, "rows": 0}

    n_earning = int(sub["n_endorsements_earning"].fillna(0).sum())
    n_indemn = int(sub["n_endorsements_indemn"].fillna(0).sum())
    n_head = int(sub["n_head"].fillna(0).sum())
    total_weight = float(sub["total_weight_cwt"].fillna(0).sum())
    liability = int(sub["liability_amount"].fillna(0).sum())
    producer_prem = int(sub["producer_premium_amount"].fillna(0).sum())
    subsidy = int(sub["subsidy_amount"].fillna(0).sum())
    total_prem = int(sub["total_premium_amount"].fillna(0).sum())
    indemnity = int(sub["indemnity_amount"].fillna(0).sum())

    means = _head_weighted_means(sub)

    return {
        "state_abbr": state_abbr or "All",
        "year": year,
        "rows": int(len(sub)),
        # Group 1: Participation / use
        "n_endorsements_earning": n_earning,
        "n_endorsements_indemn": n_indemn,
        "n_head": n_head,
        "total_weight_cwt": total_weight,
        "liability_amount": liability,
        # Group 2: Cost / subsidy
        "producer_premium_amount": producer_prem,
        "subsidy_amount": subsidy,
        "total_premium_amount": total_prem,
        "avg_cost_per_cwt": means.get("avg_cost_per_cwt", float("nan")),
        # Group 3: Coverage design
        "avg_endorsement_length_weeks": means.get("avg_endorsement_length_weeks", float("nan")),
        "avg_coverage_price": means.get("avg_coverage_price", float("nan")),
        "avg_expected_end_value": means.get("avg_expected_end_value", float("nan")),
        "avg_coverage_level_pct": means.get("avg_coverage_level_pct", float("nan")),
        # Group 4: Outcome / payout
        "indemnity_amount": indemnity,
        "indemnified_share": (n_indemn / n_earning) if n_earning > 0 else 0.0,
        "indemnity_to_producer_premium_rate": (indemnity / producer_prem) if producer_prem > 0 else 0.0,
        "subsidy_share": (subsidy / total_prem) if total_prem > 0 else 0.0,
        "producer_share_of_premium": (producer_prem / total_prem) if total_prem > 0 else 0.0,
    }


# --------------------------------------------------------------------------
# Internal helpers
# --------------------------------------------------------------------------


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Element-wise division returning 0.0 where the denominator is 0 or NA."""
    n = pd.to_numeric(numer, errors="coerce").fillna(0).astype("float64")
    d = pd.to_numeric(denom, errors="coerce").fillna(0).astype("float64")
    out = pd.Series(0.0, index=n.index, dtype="float64")
    mask = d != 0
    out.loc[mask] = n[mask] / d[mask]
    return out


def _head_weighted_means(sub: pd.DataFrame) -> dict:
    """Compute head-weighted means for the per-cwt and rate-style columns.

    Per-row weights = ``n_head`` (defaults to 1 where missing). Where the
    sum of weights is zero, returns simple mean. Returns a dict keyed by the
    output column name.
    """
    if sub.empty:
        return {}

    w = pd.to_numeric(sub["n_head"], errors="coerce").fillna(0).astype("float64")
    total_w = float(w.sum())

    def _mean(col: str) -> float:
        s = pd.to_numeric(sub[col], errors="coerce")
        if total_w > 0:
            num = float((s.fillna(0) * w).sum())
            return num / total_w
        return float(s.mean()) if not s.empty else float("nan")

    return {
        "avg_endorsement_length_weeks": _mean("length_weeks"),
        "avg_coverage_price": _mean("coverage_price"),
        "avg_expected_end_value": _mean("expected_end_value"),
        "avg_coverage_level_pct": _mean("coverage_level_pct"),
        "avg_cost_per_cwt": _mean("cost_per_cwt"),
    }


def _yearly_summary_columns() -> list[str]:
    return [
        "reinsurance_year",
        "n_endorsements_earning", "n_endorsements_indemn",
        "n_head", "total_weight_cwt", "liability_amount",
        "producer_premium_amount", "subsidy_amount",
        "total_premium_amount", "indemnity_amount",
        "avg_endorsement_length_weeks", "avg_coverage_price",
        "avg_expected_end_value", "avg_coverage_level_pct",
        "avg_cost_per_cwt",
        "indemnified_share", "indemnity_to_producer_premium_rate",
        "subsidy_share", "producer_share_of_premium",
    ]


def _state_summary_columns() -> list[str]:
    return [
        "state_abbr",
        "n_endorsements_earning", "n_endorsements_indemn",
        "n_head", "total_weight_cwt", "liability_amount",
        "producer_premium_amount", "subsidy_amount",
        "total_premium_amount", "indemnity_amount",
        "indemnified_share", "indemnity_to_producer_premium_rate",
        "subsidy_share",
    ]
