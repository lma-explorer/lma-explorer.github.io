"""Tests for the slaughter-cattle path in pipelines/clovis/snapshot.py.

Parallel to the existing feeder coverage. The slaughter function has the
same shape as `_payload_to_dataframe` but operates on a different commodity
and class set with a different output schema (no frame/muscle_grade/
weight_break columns; quality_grade_name + dressing + lot_desc instead).

All tests use synthetic in-memory payloads. No parquet reads, no network.
"""

from __future__ import annotations

import pandas as pd

from pipelines.clovis.snapshot import (
    MAX_PLAUSIBLE_PRICE,
    MIN_PLAUSIBLE_PRICE,
    _payload_to_slaughter_dataframe,
)


# --- Synthetic row helpers --------------------------------------------------


def _slaughter_row(
    auction_date: str = "04/22/2026",
    class_: str = "Cows",
    quality_grade: str = "Boner 80-85%",
    avg_price: float = 167.6,
    **overrides,
) -> dict:
    """Build a synthetic slaughter row matching the AMS_1781 MARS schema."""
    row = {
        "commodity": "Slaughter Cattle",
        "price_unit": "Per Cwt",
        "class": class_,
        "quality_grade_name": quality_grade,
        "dressing": "Average",
        "lot_desc": "None",
        "report_date": auction_date,
        "avg_weight": 1180.0,
        "avg_weight_min": 965.0,
        "avg_weight_max": 1870.0,
        "head_count": 13,
        "avg_price": avg_price,
        "avg_price_min": 161.0,
        "avg_price_max": 171.0,
        "receipts": 81,
    }
    row.update(overrides)
    return row


def _feeder_row(**overrides) -> dict:
    """A synthetic feeder row — should be EXCLUDED from slaughter output."""
    row = {
        "commodity": "Feeder Cattle",
        "price_unit": "Per Cwt",
        "class": "Steers",
        "frame": "Medium and Large",
        "muscle_grade": "1",
        "weight_break_low": 500,
        "weight_break_high": 550,
        "report_date": "04/22/2026",
        "head_count": 50,
        "avg_price": 280.0,
        "avg_price_min": 270.0,
        "avg_price_max": 290.0,
    }
    row.update(overrides)
    return row


# --- Filter correctness -----------------------------------------------------


def test_keeps_cows_bulls_dairy():
    payload = {
        "results": [
            _slaughter_row(class_="Cows"),
            _slaughter_row(class_="Bulls"),
            _slaughter_row(class_="Dairy Cows"),
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 3
    assert set(df["class"]) == {"Cows", "Bulls", "Dairy Cows"}


def test_drops_feeder_cattle():
    payload = {"results": [_feeder_row(), _slaughter_row()]}
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1
    assert (df["commodity"] == "Slaughter Cattle").all()


def test_drops_replacement_cattle():
    payload = {
        "results": [
            _slaughter_row(commodity="Replacement Cattle"),  # dropped
            _slaughter_row(),  # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1


def test_drops_non_per_cwt():
    payload = {
        "results": [
            _slaughter_row(price_unit="Per Head"),  # dropped
            _slaughter_row(),  # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1


def test_drops_out_of_band_classes():
    """Slaughter rows in classes not in KEEP_CLASSES_SLAUGHTER are dropped."""
    payload = {
        "results": [
            _slaughter_row(class_="Heifers"),  # dropped (not in slaughter set)
            _slaughter_row(class_="Calves"),   # dropped
            _slaughter_row(class_="Cows"),     # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1
    assert df["class"].iloc[0] == "Cows"


# --- Plausible-price bounds -------------------------------------------------


def test_drops_price_too_low():
    payload = {
        "results": [
            _slaughter_row(avg_price=MIN_PLAUSIBLE_PRICE - 1),  # dropped
            _slaughter_row(avg_price=50.0),                      # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1
    assert df["price_avg"].iloc[0] == 50.0


def test_drops_price_too_high():
    payload = {
        "results": [
            _slaughter_row(avg_price=MAX_PLAUSIBLE_PRICE + 1),  # dropped
            _slaughter_row(avg_price=300.0),                     # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1


def test_drops_non_numeric_price():
    payload = {
        "results": [
            _slaughter_row(avg_price=None),    # dropped (TypeError on float())
            _slaughter_row(avg_price="N/A"),   # dropped (ValueError on float())
            _slaughter_row(avg_price=200.0),   # kept
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert len(df) == 1


# --- Output schema ----------------------------------------------------------


def test_column_set():
    df = _payload_to_slaughter_dataframe({"results": [_slaughter_row()]}, "2026-05-12")
    expected = {
        "auction_date", "commodity", "class",
        "quality_grade_name", "dressing", "lot_desc",
        "avg_weight", "avg_weight_min", "avg_weight_max",
        "head_count",
        "price_low", "price_high", "price_avg",
        "receipts", "vintage",
    }
    assert set(df.columns) == expected


def test_no_feeder_fields_leak():
    """Slaughter schema must NOT have frame, muscle_grade, weight_break_*."""
    df = _payload_to_slaughter_dataframe({"results": [_slaughter_row()]}, "2026-05-12")
    for absent in ("frame", "muscle_grade", "weight_break_low", "weight_break_high"):
        assert absent not in df.columns, f"unexpected feeder field {absent!r}"


def test_dtypes():
    df = _payload_to_slaughter_dataframe({"results": [_slaughter_row()]}, "2026-05-12")
    assert df["auction_date"].dtype.name.startswith("datetime64")
    assert df["vintage"].dtype.name.startswith("datetime64")
    assert df["head_count"].dtype.name == "Int32"
    assert df["receipts"].dtype.name == "Int32"
    for f in ("avg_weight", "price_avg", "price_low", "price_high"):
        assert df[f].dtype.name == "float64"


# --- Sort order -------------------------------------------------------------


def test_sort_order():
    """Output sorted by (auction_date, class, quality_grade_name, dressing)."""
    payload = {
        "results": [
            _slaughter_row(auction_date="04/22/2026", class_="Cows",  quality_grade="Lean 85-90%"),
            _slaughter_row(auction_date="04/15/2026", class_="Cows",  quality_grade="Boner 80-85%"),
            _slaughter_row(auction_date="04/22/2026", class_="Cows",  quality_grade="Boner 80-85%"),
            _slaughter_row(auction_date="04/22/2026", class_="Bulls", quality_grade="Boner 80-85%"),
        ]
    }
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")

    # Dates ascending
    assert df["auction_date"].tolist() == [
        pd.Timestamp("2026-04-15"),
        pd.Timestamp("2026-04-22"),
        pd.Timestamp("2026-04-22"),
        pd.Timestamp("2026-04-22"),
    ]
    # Within same date: class alphabetical (Bulls before Cows)
    same_day = df[df["auction_date"] == pd.Timestamp("2026-04-22")]
    assert same_day["class"].tolist() == ["Bulls", "Cows", "Cows"]
    # Within Cows on same day: Boner before Lean
    cows_same_day = same_day[same_day["class"] == "Cows"]
    assert cows_same_day["quality_grade_name"].tolist() == ["Boner 80-85%", "Lean 85-90%"]


# --- Empty / degenerate inputs ---------------------------------------------


def test_empty_results():
    df = _payload_to_slaughter_dataframe({"results": []}, "2026-05-12")
    assert df.empty


def test_missing_results_key():
    df = _payload_to_slaughter_dataframe({}, "2026-05-12")
    assert df.empty


def test_payload_with_only_feeder_rows():
    """Mixed payload that happens to have no slaughter rows -> empty DF."""
    payload = {"results": [_feeder_row(), _feeder_row()]}
    df = _payload_to_slaughter_dataframe(payload, "2026-05-12")
    assert df.empty