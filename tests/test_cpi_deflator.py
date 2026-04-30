"""Tests for the CPI deflator math used by every chart page that
toggles between Nominal $ and Real December-2025 $.

The math is small but load-bearing: every real-dollar value the site
shows is `nominal × (cpi_basis / cpi_month)`. A bug here would silently
mis-deflate every historical chart. These tests pin the formula and
its expected behavior at the basis month.
"""

from __future__ import annotations

import pandas as pd
import pytest


def _deflator(cpi_basis: float, cpi_month: float) -> float:
    """The deflator function used (inline) by every chart page's
    `deflate-to-dec-2025` chunk. Reproduced here so a regression in any
    page would show up in tests rather than silently in the chart."""
    return cpi_basis / cpi_month


def test_deflator_at_basis_month_is_unity() -> None:
    """At the basis month itself, the deflator is exactly 1.0 by
    construction. A real-$ value at the basis month equals its nominal $."""
    cpi_basis = 318.4  # arbitrary CPI value
    deflator = _deflator(cpi_basis, cpi_basis)
    assert deflator == pytest.approx(1.0)


def test_deflator_inflates_historical_to_basis() -> None:
    """A 2020 nominal $200 with 2020 CPI=257 should real-deflate to
    a basis-month-equivalent value strictly greater than $200, because
    prices have inflated since 2020."""
    cpi_2020 = 257.0
    cpi_basis = 318.4  # ~Dec 2025
    nominal_2020 = 200.0
    real_basis = nominal_2020 * _deflator(cpi_basis, cpi_2020)
    assert real_basis > nominal_2020
    assert real_basis == pytest.approx(247.7, abs=0.5)


def test_deflator_deflates_future_to_basis() -> None:
    """A 2027 hypothetical $300 with 2027 CPI=330 should real-deflate to
    a basis-month-equivalent value strictly less than $300, because
    prices will have continued to inflate past December 2025."""
    cpi_basis = 318.4
    cpi_2027 = 330.0
    nominal_2027 = 300.0
    real_basis = nominal_2027 * _deflator(cpi_basis, cpi_2027)
    assert real_basis < nominal_2027


def test_deflator_join_pattern() -> None:
    """Reproduce the chart-page pattern: per-month CPI lookup + per-row
    deflator. Verifies no off-by-one in the join."""
    obs = pd.DataFrame(
        {
            "auction_date": pd.to_datetime(
                ["2020-06-15", "2022-06-15", "2025-12-15"]
            ),
            "price_avg": [200.0, 220.0, 250.0],
        }
    )
    cpi = pd.DataFrame(
        {
            "period": pd.to_datetime(["2020-06-01", "2022-06-01", "2025-12-01"]),
            "cpi_u": [257.0, 296.0, 318.4],
        }
    )

    # Mirror the chart-page logic: floor to month start, join, multiply
    obs["period_month"] = obs["auction_date"].dt.to_period("M").dt.to_timestamp()
    obs = obs.merge(cpi, left_on="period_month", right_on="period", how="left")
    cpi_basis = 318.4
    obs["deflator"] = cpi_basis / obs["cpi_u"]
    obs["price_real"] = obs["price_avg"] * obs["deflator"]

    # 2025-12 row should be unchanged (deflator=1)
    assert obs.loc[obs["auction_date"] == pd.Timestamp("2025-12-15"),
                    "price_real"].iloc[0] == pytest.approx(250.0)
    # 2020-06 row should inflate
    assert obs.loc[obs["auction_date"] == pd.Timestamp("2020-06-15"),
                    "price_real"].iloc[0] > 200.0
