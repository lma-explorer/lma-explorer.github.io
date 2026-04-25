"""Per-row + per-batch validator for the historical Clovis Era B block.

Implements PLAN_4.1 §7 validation rules. Designed to be called by
``ingest_era_b.py`` after parsing but before writing the parquet snapshot.

Behavior matches the live MARS validator (``pipelines/clovis/validate.py``)
in spirit: per-row schema/range checks plus a per-batch sanity gate. The
era-B gate is looser on coverage (the 2017 and 2019 partial-year tails
allow <40 weeks) and includes an optional parser-consistency check
against an external Excel reference compiled from the same USDA-AMS
public-domain reports. The reference workbook path is read from the
``CLOVIS_REFERENCE_XLSX`` environment variable; the check skips
silently when the env var is unset.

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

# Parser-consistency thresholds — calibrated 2026-04-25 against the full
# Era B corpus comparing the platform's parse to an independent extraction
# of the same USDA-AMS reports. Across 3 yrs × 13 (class, bin) cells
# (39 comparison cells), natural deltas were mean 1.59%, median 1.21%,
# max 6.14%, with 2/39 cells > 5%. Differences track the choice of
# aggregation convention (USDA's lot-weighted average vs alternatives
# like simple-average-of-range), not parser correctness — but a real
# parser bug would systematically blow these out. Thresholds catch
# the bug case while accepting natural method-of-aggregation noise.
PARSER_CONSISTENCY_MEAN_FAIL = 0.05    # parser-broken signal
PARSER_CONSISTENCY_MAX_FAIL = 0.25     # one-cell extreme outlier
PARSER_CONSISTENCY_MEAN_WARN = 0.03    # heads-up; investigate
PARSER_CONSISTENCY_MAX_WARN = 0.10     # one-cell heads-up
PARSER_CONSISTENCY_LOG = REPO_ROOT / "data" / "raw" / "clovis_historical" / "parser_consistency_log.csv"


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


def _parser_consistency_check(df: pd.DataFrame, report: ValidationReport) -> None:
    """Optional parser-consistency check against an external Excel reference
    compiled independently from the same USDA-AMS public-domain reports.

    Reads the workbook path from the ``CLOVIS_REFERENCE_XLSX`` environment
    variable. If the env var is unset or the file is missing, the check
    skips silently — the platform's other validator gates (per-row schema
    and range checks; per-batch coverage and class-balance) still run, and
    the parquet still ships. The parser-consistency check is opt-in.

    Method: compute annual median ``price_avg`` per (year, class,
    weight_bin) for both the platform's Era B parse (filtered to the
    chart's grade=1 population — Steers/Heifers, Medium and Large 1)
    and the reference workbook's matching sheet (A1, weekly, 100-lb bins).
    Express deltas as percentages of the reference value. Strong, broad
    drift across many cells indicates a parser bug; tight agreement is
    a DIY peer review on the platform's parser.

    Empirically (calibration run 2026-04-25) the natural deltas were
    1-2% mean across cells, with thin small-bin cells (300-400 lb)
    running a few percent higher due to sample-size noise. The
    ``PARSER_CONSISTENCY_*`` thresholds accommodate that.

    Writes the full delta table to ``data/raw/clovis_historical/
    parser_consistency_log.csv`` (gitignored) for forensic review.
    """
    try:
        # Imported lazily so the validator has no hard openpyxl dependency
        # in the per-row code path.
        from pipelines.clovis_historical.reference_reader import (  # type: ignore
            ENV_VAR, get_reference_path, read_reference_grade_1,
            annual_median_table, era_b_to_100lb_bin,
        )
    except ImportError as e:
        report.warn(f"Parser-consistency check skipped — could not import reader: {e}")
        return

    ref_path = get_reference_path()
    if ref_path is None:
        report.note(
            f"Parser-consistency check: skipped ({ENV_VAR} env var unset or "
            "the configured workbook is not on disk). This is expected in "
            "CI / clean-clone environments; set the env var locally for the gate."
        )
        return

    try:
        ref_long = read_reference_grade_1(ref_path)
        era_b_long = era_b_to_100lb_bin(df)

        if era_b_long.empty:
            report.note("Parser-consistency check: no grade=1 / 100-lb-bin rows in Era B; skipped.")
            return
        ad = pd.to_datetime(era_b_long["auction_date"])
        lo, hi = ad.min(), ad.max()
        ref_window = ref_long.df[
            (ref_long.df["auction_date"] >= lo) & (ref_long.df["auction_date"] <= hi)
        ]

        ref_med = annual_median_table(ref_window).rename(
            columns={"price_median": "ref_median", "n_obs": "ref_n"})
        era_b_med = annual_median_table(era_b_long).rename(
            columns={"price_median": "platform_median", "n_obs": "platform_n"})
        merged = ref_med.merge(
            era_b_med,
            on=["year", "class", "weight_break_low", "weight_break_high"],
            how="inner",
        )
        if merged.empty:
            report.note("Parser-consistency check: no overlapping (year, class, bin) cells; skipped.")
            return

        merged["delta_pct"] = ((merged["platform_median"] - merged["ref_median"])
                                / merged["ref_median"] * 100)
        merged["abs_delta_pct"] = merged["delta_pct"].abs()
        merged = merged.sort_values(["year", "class", "weight_break_low"]).reset_index(drop=True)

        # Persist the full table (gitignored) for forensic inspection
        PARSER_CONSISTENCY_LOG.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(PARSER_CONSISTENCY_LOG, index=False)

        mean_abs = merged["abs_delta_pct"].mean()
        med_abs = merged["abs_delta_pct"].median()
        max_abs = merged["abs_delta_pct"].max()
        n_cells = len(merged)
        n_over_5 = int((merged["abs_delta_pct"] > 5).sum())
        n_over_10 = int((merged["abs_delta_pct"] > 10).sum())
        worst = merged.nlargest(3, "abs_delta_pct")[
            ["year", "class", "weight_break_low", "weight_break_high",
             "ref_median", "platform_median", "delta_pct"]
        ]

        mean_frac = mean_abs / 100
        max_frac = max_abs / 100
        if mean_frac > PARSER_CONSISTENCY_MEAN_FAIL:
            report.fail(
                f"Parser-consistency FAIL: mean |delta| {mean_abs:.2f}% across {n_cells} cells "
                f"> {PARSER_CONSISTENCY_MEAN_FAIL * 100:.0f}% threshold. "
                f"Worst cells:\n{worst.to_string(index=False)}\nFull table: {PARSER_CONSISTENCY_LOG}"
            )
        elif max_frac > PARSER_CONSISTENCY_MAX_FAIL:
            report.fail(
                f"Parser-consistency FAIL: max-cell |delta| {max_abs:.2f}% "
                f"> {PARSER_CONSISTENCY_MAX_FAIL * 100:.0f}% threshold. "
                f"Worst cells:\n{worst.to_string(index=False)}\nFull table: {PARSER_CONSISTENCY_LOG}"
            )
        elif (mean_frac > PARSER_CONSISTENCY_MEAN_WARN
              or max_frac > PARSER_CONSISTENCY_MAX_WARN):
            report.warn(
                f"Parser-consistency WARN: mean |delta| {mean_abs:.2f}%, max {max_abs:.2f}%, "
                f"{n_over_5}/{n_cells} cells > 5% — within tolerance but inspect. "
                f"Full table: {PARSER_CONSISTENCY_LOG}"
            )
        else:
            report.note(
                f"Parser-consistency PASS: mean |delta| {mean_abs:.2f}%, median {med_abs:.2f}%, "
                f"max {max_abs:.2f}% across {n_cells} cells. "
                f"({n_over_5}/{n_cells} cells > 5%, {n_over_10}/{n_cells} > 10%). "
                f"Full table: {PARSER_CONSISTENCY_LOG}"
            )
    except Exception as e:  # noqa: BLE001
        report.warn(f"Parser-consistency check raised: {type(e).__name__}: {e}")


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
    _parser_consistency_check(df, report)

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
