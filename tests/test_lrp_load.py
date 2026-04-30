"""Tests for pipelines/lrp/load.py — year-window helpers and state list.

These verify the chart-page filter behavior on synthetic data. If
year-window math drifts (e.g., "Last 5" suddenly means "Last 6"), every
chart page that uses these helpers would silently render a different
slice — these tests catch that.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.lrp.load import (
    DEFAULT_STATE_ABBR,
    apply_year_window,
    available_states,
    available_years,
)


def _df_with_years(years: list[int], states: list[str] | None = None) -> pd.DataFrame:
    """Cross-product of years × states for a minimal corpus."""
    if states is None:
        states = [DEFAULT_STATE_ABBR]
    rows = []
    for y in years:
        for s in states:
            rows.append({"reinsurance_year": y, "state_abbr": s})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# apply_year_window
# ---------------------------------------------------------------------------


def test_apply_year_window_all_keeps_everything() -> None:
    df = _df_with_years([2018, 2020, 2022, 2024, 2026])
    result = apply_year_window(df, "all")
    assert sorted(result["reinsurance_year"].unique()) == [2018, 2020, 2022, 2024, 2026]


def test_apply_year_window_latest_keeps_only_max_year() -> None:
    df = _df_with_years([2018, 2020, 2022, 2024, 2026])
    result = apply_year_window(df, "latest")
    assert result["reinsurance_year"].unique().tolist() == [2026]


def test_apply_year_window_last_3_is_inclusive_3_years() -> None:
    df = _df_with_years([2020, 2021, 2022, 2023, 2024, 2025, 2026])
    result = apply_year_window(df, "last_3")
    # max=2026, last_3 = max-2 .. max = [2024, 2025, 2026]
    assert sorted(result["reinsurance_year"].unique()) == [2024, 2025, 2026]


def test_apply_year_window_last_5_is_inclusive_5_years() -> None:
    df = _df_with_years([2020, 2021, 2022, 2023, 2024, 2025, 2026])
    result = apply_year_window(df, "last_5")
    # max=2026, last_5 = max-4 .. max = [2022..2026]
    assert sorted(result["reinsurance_year"].unique()) == [2022, 2023, 2024, 2025, 2026]


def test_apply_year_window_invalid_string_raises() -> None:
    df = _df_with_years([2024])
    with pytest.raises(ValueError):
        apply_year_window(df, "this_is_not_a_window")


# ---------------------------------------------------------------------------
# available_states / available_years
# ---------------------------------------------------------------------------


def test_available_states_excludes_xx_placeholder() -> None:
    df = _df_with_years([2024], states=["AZ", "TX", "XX", "NM"])
    states = available_states(df)
    assert "XX" not in states


def test_available_states_puts_default_first() -> None:
    df = _df_with_years([2024], states=["AZ", "CA", "NM", "TX"])
    states = available_states(df)
    # AZ (the platform's default reference state) should be first
    assert states[0] == DEFAULT_STATE_ABBR
    # The rest are alphabetical
    assert states[1:] == sorted(["CA", "NM", "TX"])


def test_available_years_returns_sorted_unique() -> None:
    df = _df_with_years([2024, 2020, 2024, 2018, 2020])
    years = available_years(df)
    assert years == [2018, 2020, 2024]
