"""Tests for pipelines/clovis/validate.py's prior-snapshot file selection.

Regression coverage for the production failure observed on 2026-05-14
(clovis_refresh.yml run #6): `_latest_snapshot()` must pick the per-pen
vintage file (``clovis_weekly_<YYYY-MM-DD>.parquet``) and NOT the cleaned-
weekly aggregate (``clovis_weekly_cleaned_*.parquet``), even though both
files share the ``clovis_weekly_`` prefix.

The original failure mode: the loose ``startswith("clovis_weekly_")`` filter
matched both file families. By string sort, ``clovis_weekly_cleaned_latest.parquet``
sorts last (``'c'`` > ``'2'``), so ``candidates[-1]`` returned the cleaner's
output. That file has a CPI-truncated date range and a different schema, which
made ``check_continuity`` see spurious "new" old dates and raise a false-positive
AssertionError. The fix is a strict regex matching only the per-pen vintage
filename pattern.

These tests use synthetic empty files in pytest's tmp_path (no actual parquet
contents needed — ``_latest_snapshot()`` only inspects filenames). The module-
level ``PROCESSED_DIR`` is monkey-patched per-test.
"""

from __future__ import annotations

from pipelines.clovis import validate as v


# --- regression coverage for the production failure -------------------------


def test_latest_snapshot_picks_per_pen_not_cleaned(tmp_path, monkeypatch):
    """The bug: cleaned-weekly files sort last and were wrongly picked.
    The fix: strict regex matches only ``clovis_weekly_<YYYY-MM-DD>.parquet``.
    """
    files = [
        "clovis_weekly_2026-04-23.parquet",
        "clovis_weekly_2026-04-30.parquet",
        "clovis_weekly_2026-05-08.parquet",
        "clovis_weekly_cleaned_2026-05-08.parquet",  # WAS wrongly picked pre-fix
        "clovis_weekly_cleaned_latest.parquet",      # WAS wrongly picked pre-fix
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)

    result = v._latest_snapshot()
    assert result is not None
    assert result.name == "clovis_weekly_2026-05-08.parquet", (
        f"Expected per-pen vintage, got {result.name!r}. "
        "Regression: a cleaned-weekly file was probably wrongly selected."
    )


def test_latest_snapshot_returns_none_when_only_cleaned_files_present(
    tmp_path, monkeypatch
):
    """Cleaned files alone must NOT be returned as the per-pen baseline."""
    files = [
        "clovis_weekly_cleaned_latest.parquet",
        "clovis_weekly_cleaned_2026-05-08.parquet",
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    assert v._latest_snapshot() is None


def test_latest_snapshot_ignores_unrelated_prefixes(tmp_path, monkeypatch):
    """Other artifact filenames in data/processed/ must not be picked up.

    Notably: ``clovis_slaughter_*`` (added 2026-05-14 in 4.drought-b),
    ``clovis_latest`` (the convenience alias), ``clovis_historical_*``,
    ``cpi_*``, ``lrp_*``, and the release-basis file.
    """
    files = [
        "clovis_latest.parquet",
        "clovis_slaughter_latest.parquet",
        "clovis_slaughter_2026-05-21.parquet",
        "clovis_historical_era_b_latest.parquet",
        "clovis_release_basis_2025.parquet",
        "cpi_latest.parquet",
        "lrp_latest.parquet",
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    assert v._latest_snapshot() is None


# --- basic existence and ordering coverage ---------------------------------


def test_latest_snapshot_returns_none_when_dir_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    assert v._latest_snapshot() is None


def test_latest_snapshot_returns_none_when_processed_dir_missing(
    tmp_path, monkeypatch
):
    missing = tmp_path / "nonexistent"
    monkeypatch.setattr(v, "PROCESSED_DIR", missing)
    assert v._latest_snapshot() is None


def test_latest_snapshot_picks_most_recent_by_date(tmp_path, monkeypatch):
    """Among multiple per-pen vintages, the most recent date wins (lex sort
    on YYYY-MM-DD == chronological sort).
    """
    files = [
        "clovis_weekly_2026-04-30.parquet",
        "clovis_weekly_2026-05-08.parquet",
        "clovis_weekly_2026-04-23.parquet",  # creation order irrelevant
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    result = v._latest_snapshot()
    assert result.name == "clovis_weekly_2026-05-08.parquet"


def test_latest_snapshot_excludes_non_parquet_extensions(tmp_path, monkeypatch):
    """Files matching the date pattern but with non-parquet extensions are
    excluded by the regex's literal ``\\.parquet$`` anchor.
    """
    files = [
        "clovis_weekly_2026-05-08.parquet",
        "clovis_weekly_2026-05-15.json",  # not parquet
        "clovis_weekly_2026-05-15.csv",   # not parquet
        "clovis_weekly_2026-05-15.parquet.bak",  # not exactly .parquet
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    result = v._latest_snapshot()
    assert result.name == "clovis_weekly_2026-05-08.parquet"


def test_latest_snapshot_excludes_malformed_vintage_strings(
    tmp_path, monkeypatch
):
    """The vintage portion must be a strict YYYY-MM-DD; partial dates,
    extra suffixes, or non-numeric components do not match.
    """
    files = [
        "clovis_weekly_2026-05-08.parquet",           # valid; must win
        "clovis_weekly_2026-05.parquet",              # missing day
        "clovis_weekly_2026-05-08-extra.parquet",     # extra suffix
        "clovis_weekly_latest.parquet",               # not a date
        "clovis_weekly_release_basis_2025.parquet",   # not a date
    ]
    for fname in files:
        (tmp_path / fname).write_bytes(b"")
    monkeypatch.setattr(v, "PROCESSED_DIR", tmp_path)
    result = v._latest_snapshot()
    assert result.name == "clovis_weekly_2026-05-08.parquet"
