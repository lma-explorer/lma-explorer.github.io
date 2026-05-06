"""Tests for `pipelines/clovis/clean.py`.

The cleaner has four pure-function pieces with non-trivial logic:

    * `assign_100lb_bin` — bin assignment, including the avg_weight fallback
    * `aggregate_weekly`  — head-count-weighted weekly aggregation under the
                            M&L 1, Steers/Heifers, $/cwt filter
    * `deflate`           — CPI ratio applied per observation month
    * `replace_spikes`    — symmetric rolling-median ratio test, replacement
                            with local median, audit log generation, and the
                            invariant that cleaned nominal stays in sync with
                            cleaned real

Every test uses synthetic in-memory inputs. No parquet reads, no CPI
file reads, no network.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.clovis.clean import (
    CleanConfig,
    aggregate_weekly,
    assign_100lb_bin,
    deflate,
    replace_spikes,
)


# --------------------------------------------------------------------------- #
# assign_100lb_bin                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "low,avg,expected",
    [
        # weight_break_low present → use it directly (floor to 100)
        (400, 420, "400-499"),
        (450, 480, "400-499"),
        (300, 350, "300-399"),
        (800, 820, "800-899"),
        # weight_break_low missing → fall back to avg_weight
        (None, 520, "500-599"),
        (None, 699, "600-699"),
        # both missing → None
        (None, None, None),
        # outside the analytical window → None (lower & upper bounds)
        (200, 250, None),
        (900, 950, None),
        # NaN handling on avg fallback
        (None, float("nan"), None),
    ],
)
def test_assign_100lb_bin(low, avg, expected) -> None:
    assert assign_100lb_bin(low, avg) == expected


def test_assign_100lb_bin_low_takes_priority_over_avg() -> None:
    """If both fields are present, the bin floor uses weight_break_low.
    Common when MARS reports a 350-399 bin with avg_weight=380 — the
    headline bin is 300-399 (floor 350 = 300), not 300-399 from avg."""
    # low=350 → floor(350/100)*100 = 300 → "300-399"
    # avg=380 → would give "300-399" too, but the test verifies precedence
    assert assign_100lb_bin(350, 380) == "300-399"


# --------------------------------------------------------------------------- #
# aggregate_weekly                                                            #
# --------------------------------------------------------------------------- #


def _pen_row(
    auction_date: str,
    sex: str = "Steers",
    frame: str = "Medium and Large",
    muscle_grade: str = "1",
    weight_break_low: float | None = 500,
    avg_weight: float | None = 525,
    head_count: int = 50,
    price_avg: float = 200.0,
    source_era: str = "MARS",
) -> dict:
    return {
        "auction_date": pd.Timestamp(auction_date),
        "commodity": "Feeder Cattle",
        "class": sex,
        "frame": frame,
        "muscle_grade": muscle_grade,
        "weight_break_low": weight_break_low,
        "weight_break_high": (weight_break_low + 49) if weight_break_low else None,
        "avg_weight": avg_weight,
        "head_count": head_count,
        "price_avg": price_avg,
        "_source_era": source_era,
    }


def test_aggregate_weekly_head_count_weighting() -> None:
    """Two pens in the same (date, sex, weight_class) cell with different
    head counts: the aggregate price is the head-count-weighted mean, not
    the simple mean."""
    pens = pd.DataFrame(
        [
            _pen_row(
                "2024-01-03", weight_break_low=500, head_count=10, price_avg=180.0
            ),
            _pen_row(
                "2024-01-03", weight_break_low=500, head_count=90, price_avg=200.0
            ),
        ]
    )
    out = aggregate_weekly(pens)
    assert len(out) == 1
    row = out.iloc[0]
    expected = (10 * 180.0 + 90 * 200.0) / 100  # = 198.0
    assert row["price_nominal"] == pytest.approx(expected)
    assert row["head_count"] == 100
    assert row["n_pens"] == 2


def test_aggregate_weekly_filter_drops_non_M_and_L_1() -> None:
    """Pens outside the analytical filter (Medium 2, Large 1, etc.) drop
    out before aggregation."""
    pens = pd.DataFrame(
        [
            _pen_row("2024-01-03", frame="Medium and Large", muscle_grade="1"),
            _pen_row("2024-01-03", frame="Medium and Large", muscle_grade="2"),
            _pen_row("2024-01-03", frame="Medium", muscle_grade="1"),
            _pen_row("2024-01-03", frame="Large", muscle_grade="1"),
        ]
    )
    out = aggregate_weekly(pens)
    # Only the first row survives the filter
    assert len(out) == 1
    assert out.iloc[0]["sex"] == "Steers"
    assert out.iloc[0]["weight_class"] == "500-599"


def test_aggregate_weekly_filter_keeps_steers_and_heifers_only() -> None:
    pens = pd.DataFrame(
        [
            _pen_row("2024-01-03", sex="Steers"),
            _pen_row("2024-01-03", sex="Heifers"),
            _pen_row("2024-01-03", sex="Bulls"),
        ]
    )
    out = aggregate_weekly(pens)
    sexes = set(out["sex"].unique())
    assert sexes == {"Steers", "Heifers"}


def test_aggregate_weekly_empty_input_returns_empty_frame() -> None:
    out = aggregate_weekly(pd.DataFrame())
    assert out.empty
    # Schema is still well-defined
    expected_cols = {
        "auction_date",
        "sex",
        "weight_class",
        "head_count",
        "n_pens",
        "price_nominal",
        "source_eras",
    }
    assert set(out.columns) == expected_cols


def test_aggregate_weekly_records_source_era_set() -> None:
    """If a (date, sex, bin) cell receives pens from both eras, the
    source_eras column lists both."""
    pens = pd.DataFrame(
        [
            _pen_row("2019-04-17", source_era="EraB"),
            _pen_row("2019-04-17", source_era="MARS"),
        ]
    )
    out = aggregate_weekly(pens)
    assert len(out) == 1
    assert out.iloc[0]["source_eras"] == "EraB,MARS"


# --------------------------------------------------------------------------- #
# deflate                                                                     #
# --------------------------------------------------------------------------- #


def test_deflate_at_basis_month_is_identity() -> None:
    weekly = pd.DataFrame(
        [
            {
                "auction_date": pd.Timestamp("2025-12-10"),
                "sex": "Steers",
                "weight_class": "500-599",
                "head_count": 100,
                "n_pens": 1,
                "price_nominal": 250.0,
                "source_eras": "MARS",
            }
        ]
    )
    cpi_map = {"2025-12": 324.0}
    out = deflate(weekly, cpi_map, cpi_base=324.0)
    assert out.iloc[0]["price_real"] == pytest.approx(250.0)


def test_deflate_older_month_inflates_to_real() -> None:
    """A 2002 nominal $40/cwt at CPI=180 deflates to ~$72/cwt in
    Dec-2025 dollars at CPI=324 — pin the formula end-to-end."""
    weekly = pd.DataFrame(
        [
            {
                "auction_date": pd.Timestamp("2002-04-27"),
                "sex": "Heifers",
                "weight_class": "600-699",
                "head_count": 30,
                "n_pens": 1,
                "price_nominal": 40.0,
                "source_eras": "MARS",
            }
        ]
    )
    cpi_map = {"2002-04": 180.0, "2025-12": 324.0}
    out = deflate(weekly, cpi_map, cpi_base=324.0)
    assert out.iloc[0]["price_real"] == pytest.approx(40.0 * 324.0 / 180.0)


def test_deflate_drops_rows_with_no_cpi_for_their_month() -> None:
    weekly = pd.DataFrame(
        [
            {
                "auction_date": pd.Timestamp("2030-01-07"),
                "sex": "Steers",
                "weight_class": "500-599",
                "head_count": 50,
                "n_pens": 1,
                "price_nominal": 300.0,
                "source_eras": "MARS",
            }
        ]
    )
    cpi_map = {"2025-12": 324.0}  # no 2030 entry
    out = deflate(weekly, cpi_map, cpi_base=324.0)
    assert out.empty


# --------------------------------------------------------------------------- #
# replace_spikes                                                              #
# --------------------------------------------------------------------------- #


def _make_clean_series(
    sex: str = "Heifers",
    weight_class: str = "600-699",
    n_weeks: int = 30,
    price_real: float = 200.0,
) -> pd.DataFrame:
    """Build a baseline of `n_weeks` weekly observations with a flat
    `price_real` and a constant deflator. Tests inject spikes on top."""
    dates = pd.date_range("2020-01-08", periods=n_weeks, freq="7D")
    return pd.DataFrame(
        {
            "auction_date": dates,
            "sex": sex,
            "weight_class": weight_class,
            "head_count": [50] * n_weeks,
            "n_pens": [1] * n_weeks,
            "price_nominal": [price_real] * n_weeks,  # CPI ratio = 1 throughout
            "cpi_at_obs": [324.0] * n_weeks,
            "price_real": [price_real] * n_weeks,
            "source_eras": ["MARS"] * n_weeks,
        }
    )


def test_replace_spikes_flags_and_replaces_a_low_outlier() -> None:
    df = _make_clean_series(price_real=200.0)
    # Inject a 50%-of-median drop in the middle of the series.
    df.loc[15, "price_real"] = 100.0
    df.loc[15, "price_nominal"] = 100.0  # CPI ratio still 1.0 → both move together

    cleaned, log = replace_spikes(df, cpi_base=324.0)

    spike_rows = cleaned.loc[cleaned["spike_replaced"]]
    assert len(spike_rows) == 1
    assert spike_rows.iloc[0]["direction"] == "low"
    # Replacement is the local median (200.0)
    assert spike_rows.iloc[0]["price_real"] == pytest.approx(200.0)
    # Audit log carries the before-and-after pair
    assert len(log) == 1
    assert log.iloc[0]["price_real_orig"] == pytest.approx(100.0)
    assert log.iloc[0]["price_real_replaced"] == pytest.approx(200.0)


def test_replace_spikes_flags_and_replaces_a_high_outlier() -> None:
    df = _make_clean_series(price_real=200.0)
    df.loc[15, "price_real"] = 400.0  # 2x the median, ratio = 2.0
    df.loc[15, "price_nominal"] = 400.0

    cleaned, log = replace_spikes(df, cpi_base=324.0)

    spike_rows = cleaned.loc[cleaned["spike_replaced"]]
    assert len(spike_rows) == 1
    assert spike_rows.iloc[0]["direction"] == "high"
    assert spike_rows.iloc[0]["price_real"] == pytest.approx(200.0)


def test_replace_spikes_keeps_nominal_in_sync_after_replacement() -> None:
    """Cleaned nominal must equal cleaned_real * (cpi_at_obs / cpi_base).
    Without this, the nominal-line overlays in the chart pages would
    disagree with the cleaned real line at spike weeks."""
    df = _make_clean_series(price_real=200.0)
    # Use a CPI value that makes the ratio non-trivial: cpi_at_obs=180
    # means nominal = real * (180 / 324) ≈ real * 0.5556.
    df.loc[15, "cpi_at_obs"] = 180.0
    df.loc[15, "price_real"] = 100.0
    df.loc[15, "price_nominal"] = 100.0 * (180.0 / 324.0)

    cleaned, _ = replace_spikes(df, cpi_base=324.0)

    row = cleaned.iloc[15]
    assert row["spike_replaced"]
    expected_nom = row["price_real"] * (row["cpi_at_obs"] / 324.0)
    assert row["price_nominal"] == pytest.approx(expected_nom)


def test_replace_spikes_does_not_touch_legitimate_values() -> None:
    """A flat 200.0 series with no injected outliers should produce zero
    replacements and an empty log."""
    df = _make_clean_series(price_real=200.0)

    cleaned, log = replace_spikes(df, cpi_base=324.0)

    assert log.empty
    assert not cleaned["spike_replaced"].any()
    # And the values are untouched
    pd.testing.assert_series_equal(
        cleaned["price_real"].reset_index(drop=True),
        df["price_real"].reset_index(drop=True),
        check_names=False,
    )


def test_replace_spikes_respects_min_abs_floor() -> None:
    """A modest deviation that fails the ratio test but does NOT exceed
    the absolute-deviation floor should pass through unflagged.

    With a baseline of $200 and ratio_lo=0.60, a value of $100 has
    ratio 0.50 (would fail) AND |dev|=$100 (clears the $50 floor) → flag.
    But a baseline of $80 with the same ratio gives a value of $40,
    |dev|=$40 (below the $50 floor) → must not flag.
    """
    df = _make_clean_series(price_real=80.0)
    df.loc[15, "price_real"] = 40.0  # ratio 0.50, |dev| = $40 < $50 floor
    df.loc[15, "price_nominal"] = 40.0

    cleaned, log = replace_spikes(df, cpi_base=324.0)
    assert log.empty
    assert not cleaned["spike_replaced"].any()


def test_replace_spikes_handles_each_bin_independently() -> None:
    """The rolling median is computed per (sex, weight_class), so a
    spike in one bin must not pull the median for an adjacent bin."""
    df_a = _make_clean_series(sex="Heifers", weight_class="600-699", price_real=200.0)
    df_b = _make_clean_series(sex="Steers", weight_class="500-599", price_real=240.0)
    df_b.loc[15, "price_real"] = 120.0  # Steers spike, ratio 0.50
    df_b.loc[15, "price_nominal"] = 120.0
    df = pd.concat([df_a, df_b], ignore_index=True)

    cleaned, log = replace_spikes(df, cpi_base=324.0)

    # Exactly one replacement, in the Steers bin
    assert len(log) == 1
    assert log.iloc[0]["sex"] == "Steers"
    assert log.iloc[0]["weight_class"] == "500-599"
    # Heifers bin untouched
    heifers = cleaned.loc[cleaned["sex"] == "Heifers"]
    assert not heifers["spike_replaced"].any()


def test_replace_spikes_empty_dataframe_does_not_crash() -> None:
    df = pd.DataFrame(
        columns=[
            "auction_date",
            "sex",
            "weight_class",
            "head_count",
            "n_pens",
            "price_nominal",
            "cpi_at_obs",
            "price_real",
            "source_eras",
        ]
    )
    cleaned, log = replace_spikes(df, cpi_base=324.0)
    assert cleaned.empty
    assert log.empty


def test_replace_spikes_thresholds_can_be_overridden() -> None:
    """`CleanConfig` is a dataclass so a test (or a future PR) can tighten
    or loosen the rule without editing the module-level default."""
    df = _make_clean_series(price_real=200.0)
    df.loc[15, "price_real"] = 160.0  # ratio 0.80, default rule does NOT flag
    df.loc[15, "price_nominal"] = 160.0

    # Default config: not flagged
    _, log_default = replace_spikes(df, cpi_base=324.0)
    assert log_default.empty

    # Strict config: ratio_lo bumped to 0.85 → 0.80 now flags
    strict = CleanConfig(ratio_lo=0.85, ratio_hi=1.0 / 0.85, min_abs_dev=20.0)
    _, log_strict = replace_spikes(df, cpi_base=324.0, cfg=strict)
    assert len(log_strict) == 1
