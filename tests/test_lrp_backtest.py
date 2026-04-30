"""Tests for pipelines/lrp/backtest.py — the LRP-vs-cash advantage math.

These tests verify the core formula and the chart-page filter behavior
without reading any parquet from disk. The point: if `compute_advantage`
or `aggregate_by_coverage` ever silently changes its math, these tests
catch it before a chart page renders the wrong number.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.lrp.backtest import (
    aggregate_by_coverage,
    aggregate_by_year,
    compute_advantage,
    sample_summary,
)


def _synthetic_corpus() -> pd.DataFrame:
    """Build a tiny corpus that exercises every code path:
    - Multiple states, type codes, coverage levels, years
    - Endorsements with and without indemnity
    - One row with bad (zero) total_weight_cwt that should be dropped
    - One row outside the type-code subset that should be filtered out
    """
    rows = [
        # AZ, 2024, type 809 (Steers W1), 95% coverage, indemnity > premium
        {"state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "809",
         "coverage_level_pct": 0.95, "total_weight_cwt": 100.0,
         "producer_premium_amount": 1500, "indemnity_amount": 3000, "n_head": 100,
         "commodity_code": "0801", "plan_code": "81"},
        # AZ, 2024, type 810, 95%, indemnity < premium
        {"state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "810",
         "coverage_level_pct": 0.95, "total_weight_cwt": 200.0,
         "producer_premium_amount": 4000, "indemnity_amount": 0, "n_head": 200,
         "commodity_code": "0801", "plan_code": "81"},
        # AZ, 2024, type 811 (Heifers), 90%, exact break-even
        {"state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "811",
         "coverage_level_pct": 0.90, "total_weight_cwt": 150.0,
         "producer_premium_amount": 1500, "indemnity_amount": 1500, "n_head": 150,
         "commodity_code": "0801", "plan_code": "81"},
        # TX, 2024 — different state for filter test
        {"state_abbr": "TX", "reinsurance_year": 2024, "type_code": "809",
         "coverage_level_pct": 0.95, "total_weight_cwt": 100.0,
         "producer_premium_amount": 1500, "indemnity_amount": 3000, "n_head": 100,
         "commodity_code": "0801", "plan_code": "81"},
        # AZ, 2020, older year — for year-window test
        {"state_abbr": "AZ", "reinsurance_year": 2020, "type_code": "809",
         "coverage_level_pct": 0.80, "total_weight_cwt": 100.0,
         "producer_premium_amount": 1000, "indemnity_amount": 500, "n_head": 100,
         "commodity_code": "0801", "plan_code": "81"},
        # Bad row: zero weight should be dropped
        {"state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "809",
         "coverage_level_pct": 0.95, "total_weight_cwt": 0.0,
         "producer_premium_amount": 100, "indemnity_amount": 0, "n_head": 0,
         "commodity_code": "0801", "plan_code": "81"},
        # Out-of-subset type code (823 = Unborn Calves) should not appear in aggregate
        {"state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "823",
         "coverage_level_pct": 0.95, "total_weight_cwt": 100.0,
         "producer_premium_amount": 1000, "indemnity_amount": 500, "n_head": 100,
         "commodity_code": "0801", "plan_code": "81"},
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# compute_advantage
# ---------------------------------------------------------------------------


def test_compute_advantage_formula_is_indemnity_minus_premium_per_cwt() -> None:
    df = _synthetic_corpus()
    result = compute_advantage(df)
    az_809_2024 = result[
        (result["state_abbr"] == "AZ")
        & (result["type_code"] == "809")
        & (result["reinsurance_year"] == 2024)
        & (result["total_weight_cwt"] == 100.0)
    ]
    # (3000 - 1500) / 100 == 15.0
    assert az_809_2024["lrp_advantage_per_cwt"].iloc[0] == pytest.approx(15.0)


def test_compute_advantage_drops_zero_weight_rows() -> None:
    df = _synthetic_corpus()
    result = compute_advantage(df)
    # The bad zero-weight row must not appear, regardless of state/year
    assert (result["total_weight_cwt"] > 0).all()


def test_compute_advantage_handles_break_even() -> None:
    df = _synthetic_corpus()
    result = compute_advantage(df)
    az_811 = result[
        (result["state_abbr"] == "AZ")
        & (result["type_code"] == "811")
    ]
    # (1500 - 1500) / 150 == 0
    assert az_811["lrp_advantage_per_cwt"].iloc[0] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# aggregate_by_coverage — filter + group correctness
# ---------------------------------------------------------------------------


def test_aggregate_by_coverage_filters_by_state() -> None:
    df = _synthetic_corpus()
    az = aggregate_by_coverage(df, state_abbr="AZ", year_window="all")
    tx = aggregate_by_coverage(df, state_abbr="TX", year_window="all")
    # AZ has rows at 80%, 90%, 95% (after type-code subset, dropping 823 + zero weight)
    assert set(az["coverage_level_pct"].tolist()) == {0.80, 0.90, 0.95}
    # TX has only one row at 95%
    assert tx["coverage_level_pct"].tolist() == [0.95]
    assert tx["n_endorsements"].iloc[0] == 1


def test_aggregate_by_coverage_excludes_type_823() -> None:
    df = _synthetic_corpus()
    az = aggregate_by_coverage(df, state_abbr="AZ", year_window="all")
    # Type 823 row at 95% would have advantage=(500-1000)/100=-5.0 if included.
    # AZ-95% should average only the {809-rows for 2024} which is just the
    # (3000-1500)/100=15 and the (0-4000)/200=-20 → mean = -2.5
    az_95 = az[az["coverage_level_pct"] == 0.95]
    assert az_95["mean_advantage_per_cwt"].iloc[0] == pytest.approx(-2.5)
    # And the 823 row's weight (100) must not show up in total_weight_cwt for AZ-95%
    # AZ-95% includes 100 (809) + 200 (810) = 300
    assert az_95["total_weight_cwt"].iloc[0] == pytest.approx(300.0)


def test_aggregate_by_coverage_indemnified_share() -> None:
    df = _synthetic_corpus()
    az = aggregate_by_coverage(df, state_abbr="AZ", year_window="all")
    # AZ-95%: 1 of 2 endorsements has indemnity > 0 → 0.5
    az_95 = az[az["coverage_level_pct"] == 0.95]
    assert az_95["indemnified_share"].iloc[0] == pytest.approx(0.5)


def test_aggregate_by_coverage_empty_returns_zero_row_frame() -> None:
    # State that doesn't exist in corpus — should return empty (not crash)
    df = _synthetic_corpus()
    result = aggregate_by_coverage(df, state_abbr="ZZ", year_window="all")
    assert result.empty
    assert "coverage_level_pct" in result.columns


def test_aggregate_by_coverage_bins_off_menu_values_into_range() -> None:
    """RMA stores coverage_level_pct as a continuous derived ratio
    (coverage_price / expected_end_value), not a fixed-menu choice. A
    value like 0.8807 should land in the 0.85-bucket (lower edge), not
    snap to 0.90. Verify the range-binning behavior."""
    df = _synthetic_corpus()
    # Inject one off-menu value: 0.8807 should land in the [0.85, 0.90) bin
    off_menu = pd.DataFrame([{
        "state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "809",
        "coverage_level_pct": 0.8807, "total_weight_cwt": 100.0,
        "producer_premium_amount": 1500, "indemnity_amount": 0, "n_head": 100,
        "commodity_code": "0801", "plan_code": "81",
    }])
    df_with_off_menu = pd.concat([df, off_menu], ignore_index=True)
    result = aggregate_by_coverage(df_with_off_menu, state_abbr="AZ", year_window="all")
    # The 0.8807 endorsement should appear in the 0.85 bucket, NOT in 0.90
    bins = set(result["coverage_level_pct"].tolist())
    assert 0.85 in bins, "0.8807 should land in [0.85, 0.90) bin (lower edge 0.85)"
    # The original AZ 0.90 endorsement (one of the synthetic rows) should still
    # be in 0.90; the 0.8807 row should NOT have been pooled into it.


def test_aggregate_by_coverage_drops_below_70_percent() -> None:
    """RMA's product floor is 70%. Endorsements below should be dropped."""
    df = _synthetic_corpus()
    too_low = pd.DataFrame([{
        "state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "809",
        "coverage_level_pct": 0.50, "total_weight_cwt": 100.0,
        "producer_premium_amount": 1500, "indemnity_amount": 0, "n_head": 100,
        "commodity_code": "0801", "plan_code": "81",
    }])
    df_with_low = pd.concat([df, too_low], ignore_index=True)
    result = aggregate_by_coverage(df_with_low, state_abbr="AZ", year_window="all")
    bins = set(result["coverage_level_pct"].tolist())
    # 0.50 should not produce a bin; only the standard ones
    assert 0.50 not in bins
    assert all(b >= 0.70 for b in bins)


def test_aggregate_by_coverage_drops_nan_coverage_level() -> None:
    """The production LRP corpus has NaN coverage_level_pct on a small
    fraction of rows (~0.01% — typically early-year rows where RMA's
    reporting was incomplete). The .astype(int) bin step would crash on
    these. Verify we drop them without crashing."""
    df = _synthetic_corpus()
    # Inject one NaN-coverage row in the AZ slice
    nan_row = pd.DataFrame([{
        "state_abbr": "AZ", "reinsurance_year": 2024, "type_code": "809",
        "coverage_level_pct": float("nan"), "total_weight_cwt": 100.0,
        "producer_premium_amount": 1500, "indemnity_amount": 0, "n_head": 100,
        "commodity_code": "0801", "plan_code": "81",
    }])
    df_with_nan = pd.concat([df, nan_row], ignore_index=True)
    # Should not crash; the NaN row should be silently excluded
    result = aggregate_by_coverage(df_with_nan, state_abbr="AZ", year_window="all")
    assert not result.empty
    # Coverage bins should still be {0.80, 0.90, 0.95} — no NaN bin
    assert set(result["coverage_level_pct"].tolist()) == {0.80, 0.90, 0.95}


# ---------------------------------------------------------------------------
# aggregate_by_year
# ---------------------------------------------------------------------------


def test_aggregate_by_year_groups_correctly() -> None:
    df = _synthetic_corpus()
    az = aggregate_by_year(df, state_abbr="AZ", year_window="all")
    # AZ has 4 rows in subset: 3 in 2024 (809,810,811) + 1 in 2020 (809)
    # Bad-weight row dropped, type 823 filtered out
    assert set(az["reinsurance_year"].tolist()) == {2020, 2024}
    az_2024 = az[az["reinsurance_year"] == 2024]
    assert az_2024["n_endorsements"].iloc[0] == 3


# ---------------------------------------------------------------------------
# sample_summary
# ---------------------------------------------------------------------------


def test_sample_summary_counts_correctly() -> None:
    df = _synthetic_corpus()
    s = sample_summary(df, state_abbr="AZ", year_window="all")
    # 4 valid AZ endorsements in the type-code subset
    assert s["n_endorsements"] == 4
    assert s["state_label"] == "AZ"
    assert s["year_min"] == 2020
    assert s["year_max"] == 2024


def test_sample_summary_handles_empty_slice_without_crash() -> None:
    df = _synthetic_corpus()
    s = sample_summary(df, state_abbr="ZZ", year_window="all")
    assert s["n_endorsements"] == 0
    assert s["mean_advantage_per_cwt"] == 0.0


# ---------------------------------------------------------------------------
# Suppression flag and 95% CI columns (V2 — sample-size disclosure)
# ---------------------------------------------------------------------------


def test_aggregate_by_coverage_output_schema_includes_suppression_and_ci() -> None:
    """V2 schema: aggregate_by_coverage must return suppressed flag and CI bounds.

    The chart page's filter logic reads these columns; if they go missing
    a render would crash. This test guards the schema contract.
    """
    df = _synthetic_corpus()
    result = aggregate_by_coverage(df, state_abbr="AZ", year_window="all")
    expected_cols = {
        "coverage_level_pct",
        "n_endorsements",
        "mean_advantage_per_cwt",
        "median_advantage_per_cwt",
        "std_advantage_per_cwt",
        "sem_advantage_per_cwt",
        "ci95_lower",
        "ci95_upper",
        "indemnified_share",
        "total_head",
        "total_weight_cwt",
        "suppressed",
    }
    assert expected_cols.issubset(set(result.columns)), (
        f"Output schema is missing required columns. "
        f"Expected at least {expected_cols}, got {set(result.columns)}"
    )


def test_aggregate_by_coverage_suppresses_bins_below_threshold() -> None:
    """Synthetic corpus has tiny n per bin; all bins should be flagged
    suppressed=True since each bin has n < MIN_BIN_N (=30)."""
    df = _synthetic_corpus()
    result = aggregate_by_coverage(df, state_abbr="AZ", year_window="all")
    # Every AZ bin in the synthetic corpus has n < 30, so all should be suppressed.
    assert (result["suppressed"] == True).all(), (
        f"Expected all rows suppressed for the synthetic corpus, but found "
        f"{result[~result['suppressed']]['coverage_level_pct'].tolist()} unsuppressed."
    )


def test_aggregate_by_coverage_does_not_suppress_when_n_at_or_above_threshold() -> None:
    """Build a corpus with exactly MIN_BIN_N (=30) rows in one bin and verify
    suppressed is False at the boundary."""
    from pipelines.lrp.backtest import MIN_BIN_N

    rows = []
    for i in range(MIN_BIN_N):
        rows.append({
            "state_abbr": "TX", "reinsurance_year": 2024, "type_code": "810",
            "coverage_level_pct": 0.95, "total_weight_cwt": 100.0,
            "producer_premium_amount": 1500, "indemnity_amount": 1500 + i * 10,
            "n_head": 100, "commodity_code": "0801", "plan_code": "81",
        })
    df = pd.DataFrame(rows)
    result = aggregate_by_coverage(df, state_abbr="TX", year_window="all")
    tx_95 = result[result["coverage_level_pct"] == 0.95]
    assert tx_95["n_endorsements"].iloc[0] == MIN_BIN_N
    assert tx_95["suppressed"].iloc[0] == False, (
        "Bin with exactly MIN_BIN_N rows should not be suppressed (boundary check)."
    )


def test_aggregate_by_coverage_ci_brackets_the_mean() -> None:
    """For any bin with n >= 2, the 95% CI must bracket the mean.
    For bins with n < 2 the CI may be NaN (sem undefined)."""
    from pipelines.lrp.backtest import MIN_BIN_N
    import math

    # Build a bin with 30 rows so the CI is computable.
    rows = []
    for i in range(MIN_BIN_N):
        rows.append({
            "state_abbr": "TX", "reinsurance_year": 2024, "type_code": "810",
            "coverage_level_pct": 0.95, "total_weight_cwt": 100.0,
            "producer_premium_amount": 1500, "indemnity_amount": 1500 + i * 10,
            "n_head": 100, "commodity_code": "0801", "plan_code": "81",
        })
    df = pd.DataFrame(rows)
    result = aggregate_by_coverage(df, state_abbr="TX", year_window="all")
    tx_95 = result[result["coverage_level_pct"] == 0.95].iloc[0]
    mean = tx_95["mean_advantage_per_cwt"]
    lo = tx_95["ci95_lower"]
    hi = tx_95["ci95_upper"]
    assert lo <= mean <= hi, (
        f"95% CI [{lo}, {hi}] does not bracket the mean ({mean})."
    )
    # Sanity: CI should be approximately mean ± 1.96 * std/sqrt(n)
    sem = tx_95["sem_advantage_per_cwt"]
    assert math.isclose(hi - mean, 1.96 * sem, rel_tol=1e-6)
    assert math.isclose(mean - lo, 1.96 * sem, rel_tol=1e-6)
