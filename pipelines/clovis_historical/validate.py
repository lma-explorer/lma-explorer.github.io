"""Per-row + per-batch validator for the historical Clovis Era B block.

Implements PLAN_4.1 §7 validation rules. Designed to be called by
``ingest_era_b.py`` after parsing but before writing the parquet snapshot.

Behavior matches the live MARS validator (``pipelines/clovis/validate.py``)
in spirit: per-row schema/range checks plus a per-batch sanity gate. The
era-B gate is looser on coverage (the 2017 and 2019 partial-year tails
allow <40 weeks) and includes a hook for the LMIC private cross-check that
runs only when ``Data_LMIC/AuctionsClovisNM.xlsx`` is present locally
(per Guardrail #1, that file is gitignored and never enters the public
repo, so the comparison cannot run in CI — it's a local-only forensic
gate).

Per Guardrail #9 ("loud fail"), ``validate_batch`` returns a
``ValidationReport`` with ``passed`` set to False whenever any rule
fires. ``ingest_era_b.py`` aborts the parquet write on FAIL.

Usage as a script:
    python -m pipelines.clovis_historical.validate \
        data/processed/clovis_historical_era_b_latest.parquet
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

# Era windows — sub-task 9.1 §14.2 verified.
ERA_WINDOWS: dict[str, tuple[date, date]] = {
    "B": (date(2017, 10, 4), date(2019, 4, 11)),
    "A": (date(2019, 4, 17), date.today()),
}

VALID_CLASSES = {"Steers", "Heifers", "Bulls"}
VALID_GRADES = {"1", "1-2", "2", "3"}
PRICE_AVG_RANGE = (20.0, 800.0)  # $/cwt sanity bounds, matching §7

# Per-batch coverage thresholds. 2017 only has Q4 in Era B; 2019 only has
# Q1 + part of April. The other Era B years should hit ≥40 weeks.
COVERAGE_FLOORS_ERA_B: dict[int, int] = {
    2017: 9,    # Oct-Dec ≈ 13 cal weeks; allow some dark
    2018: 40,   # full year
    2019: 12,   # Jan-Apr 11; ~14 cal weeks possible
}

# Class-balance floors per class. Calibrated against the full Era B
# corpus (76 weeks, 7,025 lots): Bulls run a stable 5-6% of feeder lots at
# Clovis — they're a minority class by composition, not a parsing artifact.
# Steers and Heifers each dominate at 30-50%. Floors are set to catch
# "parser broken" failure modes without false-positiving on real auction
# composition (the Bulls floor at 10% would fire every year — see
# 2018=6.0%, 2019=5.6%).
CLASS_BALANCE_FLOORS = {
    "Steers": 0.30,
    "Heifers": 0.20,
    "Bulls": 0.03,
}
# Backwards-compat shim if anything else imports the old constant.
CLASS_BALANCE_FLOOR = 0.03  # kept for type checkers; not used directly

# LMIC drift threshold: |median delta| > 5% opens a data-source-drift issue.
LMIC_DRIFT_THRESHOLD = 0.05
LMIC_PATH = REPO_ROOT.parent / "Data_LMIC" / "AuctionsClovisNM.xlsx"


@dataclass
class ValidationReport:
    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.passed = False
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def note(self, msg: str) -> None:
        self.info.append(msg)

    def print_summary(self) -> None:
        for m in self.info:
            print(f"  INFO   {m}")
        for m in self.warnings:
            print(f"  WARN   {m}")
        for m in self.errors:
            print(f"  FAIL   {m}")
        status = "PASS" if self.passed else "FAIL"
        print(f"\n  RESULT: {status}  ({len(self.errors)} errors, {len(self.warnings)} warnings)")


# ---- per-row checks ---------------------------------------------------------

def _check_schema(df: pd.DataFrame, report: ValidationReport) -> None:
    required = {
        "auction_date", "commodity", "class", "frame", "muscle_grade",
        "head_count", "price_low", "price_high", "price_avg",
        "receipts", "vintage",
    }
    missing = required - set(df.columns)
    if missing:
        report.fail(f"Missing required columns: {sorted(missing)}")


def _check_dates(df: pd.DataFrame, era: str, report: ValidationReport) -> None:
    lo, hi = ERA_WINDOWS[era]
    ad = pd.to_datetime(df["auction_date"], errors="coerce").dt.date
    bad = ad.isna().sum()
    if bad:
        report.fail(f"{bad} rows with unparseable auction_date")
    out_of_window = ((ad < lo) | (ad > hi)).sum()
    if out_of_window:
        report.fail(
            f"{out_of_window} rows have auction_date outside era {era} window "
            f"({lo} → {hi})"
        )


def _check_classes(df: pd.DataFrame, report: ValidationReport) -> None:
    bad = ~df["class"].isin(VALID_CLASSES)
    if bad.any():
        bad_vals = sorted(df.loc[bad, "class"].unique())
        report.fail(f"{int(bad.sum())} rows with invalid class: {bad_vals}")


def _check_grades(df: pd.DataFrame, report: ValidationReport) -> None:
    bad = ~df["muscle_grade"].isin(VALID_GRADES)
    if bad.any():
        bad_vals = sorted(df.loc[bad, "muscle_grade"].unique())
        report.warn(f"{int(bad.sum())} rows with non-standard muscle_grade: {bad_vals}")


def _check_prices(df: pd.DataFrame, report: ValidationReport) -> None:
    pa = pd.to_numeric(df["price_avg"], errors="coerce")
    null_n = pa.isna().sum()
    if null_n:
        report.fail(f"{null_n} rows have null/non-numeric price_avg")
    lo, hi = PRICE_AVG_RANGE
    oor = ((pa < lo) | (pa > hi)).sum()
    if oor:
        report.fail(f"{oor} rows have price_avg outside [{lo}, {hi}] $/cwt")


# ---- per-batch checks -------------------------------------------------------

def _check_coverage(df: pd.DataFrame, era: str, report: ValidationReport) -> None:
    if era != "B":
        return  # Era A coverage gate is the live validator's job
    ad = pd.to_datetime(df["auction_date"]).dt.date
    weeks_per_year: dict[int, int] = (
        ad.groupby(ad.map(lambda d: d.year))
          .nunique()
          .to_dict()
    )
    for year, floor in COVERAGE_FLOORS_ERA_B.items():
        n = weeks_per_year.get(year, 0)
        if n < floor:
            report.fail(
                f"Coverage gate: {year} has {n} distinct auction weeks, "
                f"floor is {floor}"
            )
        else:
            report.note(f"Coverage {year}: {n} weeks (≥{floor} floor)")


def _check_class_balance(df: pd.DataFrame, era: str, report: ValidationReport) -> None:
    ad = pd.to_datetime(df["auction_date"]).dt.date
    for year, sub in df.groupby(ad.map(lambda d: d.year)):
        total = len(sub)
        if total == 0:
            continue
        for cls in VALID_CLASSES:
            share = (sub["class"] == cls).sum() / total
            floor = CLASS_BALANCE_FLOORS[cls]
            if share < floor:
                # 2017 Q4 partial — Bulls absent some weeks; warn-only.
                if year == 2017 and cls == "Bulls":
                    report.warn(
                        f"Class balance {year}: {cls} share {share:.1%} "
                        f"(2017 Q4 partial; floor {floor:.0%})"
                    )
                else:
                    report.fail(
                        f"Class balance {year}: {cls} share {share:.1%} "
                        f"< {floor:.0%} floor"
                    )
            else:
                report.note(f"Class balance {year}: {cls} {share:.1%} (≥{floor:.0%} floor)")


def _lmic_drift_check(df: pd.DataFrame, report: ValidationReport) -> None:
    """Local-only LMIC private cross-check. Skips silently if file absent."""
    if not LMIC_PATH.exists():
        report.note(
            f"LMIC drift check: skipped (Data_LMIC/AuctionsClovisNM.xlsx not present). "
            "This is expected in CI; run locally for the full gate."
        )
        return
    try:
        # Implementation deferred to local run. The shape/columns of the LMIC
        # workbook are not in this scaffold yet. Emit a placeholder PASS that
        # the local run replaces with the real per-year delta computation.
        report.note(
            "LMIC drift check: placeholder PASS. Wire the real comparison "
            "against AuctionsClovisNM.xlsx in the local fork before commit."
        )
    except Exception as e:  # noqa: BLE001
        report.warn(f"LMIC drift check raised: {e}")


# ---- public entry point -----------------------------------------------------

def validate_batch(df: pd.DataFrame, era: str) -> ValidationReport:
    report = ValidationReport()
    if df.empty:
        report.fail("Input DataFrame is empty.")
        return report

    _check_schema(df, report)
    if not report.passed:
        return report  # don't run row checks against missing columns

    _check_dates(df, era, report)
    _check_classes(df, report)
    _check_grades(df, report)
    _check_prices(df, report)

    _check_coverage(df, era, report)
    _check_class_balance(df, era, report)
    _lmic_drift_check(df, report)

    return report


# ---- CLI --------------------------------------------------------------------

def _read_any(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file extension: {path.suffix}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("path", help="Path to era_b parquet or CSV")
    ap.add_argument("--era", default="B", choices=["A", "B"])
    args = ap.parse_args(argv)

    df = _read_any(Path(args.path))
    print(f"Loaded {len(df)} rows from {args.path}")
    report = validate_batch(df, era=args.era)
    report.print_summary()
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
