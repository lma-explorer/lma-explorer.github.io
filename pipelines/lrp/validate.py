"""Validate the LRP corpus snapshot before publication.

Three independent checks run in sequence; any failure causes the validator
to exit non-zero so a future scheduled workflow can open a data-source-drift
issue and NOT promote a new snapshot to ``lrp_latest.parquet``.

    1. Schema-hash check: a stable hash derived from the canonical
       ``COLUMNS`` list in ``pipelines/lrp/parse.py`` must equal the hash
       committed in ``expected_schema.sha256``. Catches silent RMA schema
       additions, reorderings, or rename events.

    2. Value sanity: per-cwt prices within plausible bounds, coverage
       level within [0, 1], endorsement length in the documented set,
       date columns parse cleanly, integer counts non-negative
       (with ``indemnity_amount`` allowed to be negative).

    3. Year coverage: every reinsurance_year in the parquet sits within
       [``HISTORY_FLOOR_YEAR``, current year], and no year present in
       the raw-zip directory is missing from the parquet.

Reads ``data/processed/lrp_latest.parquet`` only; does not parse raw
zips itself (parse.py validates zip structure on read). Writes nothing
unless ``--rebaseline`` is passed.

Usage:
    python -m pipelines.lrp.validate                    # run all checks
    python -m pipelines.lrp.validate --rebaseline       # regenerate schema hash

The --rebaseline flag is a deliberate, reviewed action. Run it only after
inspecting the change in the COLUMNS list, deciding the new schema is
acceptable, and updating any downstream consumers (parse.py docstring,
snapshot.py docstring, pipelines/lrp/README.md schema table). NEVER
rebaseline to make a failing pipeline green.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipelines.lrp.parse import COLUMNS, FEEDER_CATTLE_TYPE_CODES_BACKTEST

# ---------------------------------------------------------------------------
# Plausibility bounds
# ---------------------------------------------------------------------------

# Coverage price and expected end value are quoted in $/cwt for the
# backtest-subset type codes {809, 810, 811, 812} (born Steers/Heifers
# Weight 1+2). The widest historical range in our 24-year corpus is
# roughly $25 (2003 cycle trough) to $310 (2024 cycle peak). The
# plausibility window is intentionally wider than observed — a real future
# value at $400 would be flagged but not rejected; a stray $5 or $1500
# is the actual concern.
#
# IMPORTANT: these bounds apply ONLY to the backtest subset. Type 823
# (Unborn Calves, new in 2026) uses $/head pricing with values around
# $1,000-$1,700, which is correct for that product but would violate
# the $/cwt bounds. The validator scopes the dollar-bound checks to
# FEEDER_CATTLE_TYPE_CODES_BACKTEST = {809, 810, 811, 812} only.
MIN_PLAUSIBLE_DOLLARS_PER_CWT = 20.0
MAX_PLAUSIBLE_DOLLARS_PER_CWT = 800.0

# Cost per cwt is the producer's premium per cwt of insured liability.
# It's a fraction of the coverage price, typically 2-7% — observed range
# in the corpus is roughly $0.10 (2003 deep-OTM endorsements) to $40
# (2024 high-coverage). Same backtest-subset scope as the prices above.
MIN_PLAUSIBLE_PREMIUM_PER_CWT = 0.01
MAX_PLAUSIBLE_PREMIUM_PER_CWT = 100.0

# RMA-documented endorsement lengths (weeks). Any other value implies an
# upstream schema change.
VALID_ENDORSEMENT_LENGTHS = frozenset({13, 17, 21, 26, 30, 34, 39, 43, 47, 52})

# Reinsurance-year floor (the earliest year RMA has on pubfs).
HISTORY_FLOOR_YEAR = 2003

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
RAW_DIR = REPO_ROOT / "data" / "raw" / "lrp"
LATEST_PARQUET_PATH = PROCESSED_DIR / "lrp_latest.parquet"
EXPECTED_SCHEMA_PATH = Path(__file__).resolve().parent / "expected_schema.sha256"


# ---------------------------------------------------------------------------
# Schema hash
# ---------------------------------------------------------------------------


def schema_hash() -> str:
    """Compute a stable SHA-256 hash of the canonical COLUMNS list.

    The hash covers ``(index, source_name, name, dtype)`` tuples in order;
    any reorder, rename, or dtype change shifts the hash. Stable across
    Python versions (no dict ordering, no pickle).
    """
    h = hashlib.sha256()
    for c in COLUMNS:
        # Pipe-delimited line per column — pipe is also our source-data
        # delimiter, so this naming is a small mnemonic. Fields cannot
        # contain pipes or newlines (tested by the assertion below).
        line = f"{c.index}|{c.source_name}|{c.name}|{c.dtype}\n"
        assert "|" not in c.source_name and "\n" not in c.source_name, (
            f"COLUMN source_name contains forbidden char: {c.source_name!r}"
        )
        h.update(line.encode("utf-8"))
    return h.hexdigest()


def read_baseline_hash() -> str:
    """Read the committed expected_schema.sha256 baseline.

    Treats lines starting with '#' as comments. Returns the first
    non-comment, non-empty line (stripped). Raises if the file is
    malformed or missing.
    """
    if not EXPECTED_SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"{EXPECTED_SCHEMA_PATH} missing; run --rebaseline to create it."
        )
    text = EXPECTED_SCHEMA_PATH.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#"):
            return line
    raise RuntimeError(f"No hash line found in {EXPECTED_SCHEMA_PATH}")


def write_baseline_hash(hash_value: str) -> None:
    """Rebaseline expected_schema.sha256 with a new SHA-256 hash."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content = (
        "# LRP COLUMNS schema-hash baseline.\n"
        "#\n"
        f"# Last rebaselined: {timestamp} (UTC)\n"
        "#\n"
        "# Regenerated via `python -m pipelines.lrp.validate --rebaseline`.\n"
        "# Compared at validate-time against the live hash of the COLUMNS\n"
        "# list in pipelines/lrp/parse.py. A mismatch causes the validator\n"
        "# to exit non-zero (the pipeline's signal that the upstream RMA\n"
        "# schema may have drifted and downstream consumers need a review\n"
        "# pass before promoting a new snapshot).\n"
        f"{hash_value}\n"
    )
    EXPECTED_SCHEMA_PATH.write_text(content, encoding="utf-8")


def check_schema_hash() -> list[str]:
    """Compare the live COLUMNS hash against the committed baseline.

    Returns a list of issue strings; empty list means no issues. The
    hash itself is not surfaced unless there's a mismatch.
    """
    live = schema_hash()
    try:
        baseline = read_baseline_hash()
    except (FileNotFoundError, RuntimeError) as exc:
        return [f"schema-hash baseline unreadable: {exc}"]
    if live == baseline:
        return []
    return [
        "SCHEMA HASH MISMATCH",
        f"  baseline: {baseline}",
        f"  live:     {live}",
        "  If the COLUMNS change is intentional, re-run with --rebaseline",
        "  (after reviewing downstream consumers in 4.LRP-c / 4.LRP-d).",
    ]


# ---------------------------------------------------------------------------
# Value sanity
# ---------------------------------------------------------------------------


def check_value_sanity(df: pd.DataFrame) -> list[str]:
    """Return a list of issue strings; empty list means clean.

    Scoping rule:
        - Dollar-bound checks (coverage_price, expected_end_value, cost_per_cwt)
          apply ONLY to the backtest subset {809, 810, 811, 812}, because other
          type codes can use different pricing units (notably 823 = Unborn
          Calves, which prices per head not per cwt).
        - All other checks (dates, integers, coverage level, endorsement
          length, snapshot filter) apply to the full DataFrame, since those
          checks are unit-independent.
    """
    issues: list[str] = []
    n = len(df)
    if n == 0:
        return ["DataFrame is empty (no rows to validate)"]

    backtest_mask = df["type_code"].isin(FEEDER_CATTLE_TYPE_CODES_BACKTEST)
    df_backtest = df.loc[backtest_mask]

    # 1. Coverage price and expected end value plausibility ($/cwt) — backtest subset only
    for col in ("coverage_price", "expected_end_value"):
        s = df_backtest[col].dropna()
        below = int((s < MIN_PLAUSIBLE_DOLLARS_PER_CWT).sum())
        above = int((s > MAX_PLAUSIBLE_DOLLARS_PER_CWT).sum())
        if below or above:
            issues.append(
                f"{col}: {below} below ${MIN_PLAUSIBLE_DOLLARS_PER_CWT:.0f}, "
                f"{above} above ${MAX_PLAUSIBLE_DOLLARS_PER_CWT:.0f} "
                f"(of {len(s):,} non-null in backtest subset)"
            )

    # 2. Cost per cwt plausibility (premium $/cwt) — backtest subset only
    s = df_backtest["cost_per_cwt"].dropna()
    below = int((s < MIN_PLAUSIBLE_PREMIUM_PER_CWT).sum())
    above = int((s > MAX_PLAUSIBLE_PREMIUM_PER_CWT).sum())
    if below or above:
        issues.append(
            f"cost_per_cwt: {below} below ${MIN_PLAUSIBLE_PREMIUM_PER_CWT}, "
            f"{above} above ${MAX_PLAUSIBLE_PREMIUM_PER_CWT} "
            f"(of {len(s):,} non-null in backtest subset)"
        )

    # 3. Coverage level percent within [0, 1]
    s = df["coverage_level_pct"].dropna()
    out_of_range = int(((s < 0) | (s > 1)).sum())
    if out_of_range:
        issues.append(
            f"coverage_level_pct: {out_of_range} values outside [0, 1] "
            f"(of {len(s):,} non-null)"
        )

    # 4. Endorsement length must be in the documented set
    s = df["length_weeks"].dropna()
    invalid = int((~s.isin(VALID_ENDORSEMENT_LENGTHS)).sum())
    if invalid:
        unique_invalid = sorted(int(x) for x in s[~s.isin(VALID_ENDORSEMENT_LENGTHS)].unique())
        issues.append(
            f"length_weeks: {invalid} values outside "
            f"{sorted(VALID_ENDORSEMENT_LENGTHS)} (saw {unique_invalid[:8]}...)"
        )

    # 5. Date columns must parse cleanly (no NaT after the snapshot filter)
    for col in ("effective_date", "end_date"):
        nat = int(df[col].isna().sum())
        if nat:
            issues.append(f"{col}: {nat} NaT values (failed date parse)")

    # 6. End date must follow effective date (sanity on coverage logic)
    both_present = df[df["effective_date"].notna() & df["end_date"].notna()]
    if not both_present.empty:
        backwards = int((both_present["end_date"] < both_present["effective_date"]).sum())
        if backwards:
            issues.append(
                f"end_date < effective_date: {backwards} rows "
                f"(coverage period would be negative)"
            )

    # 7. Non-negative integer columns. ``indemnity_amount`` is signed and
    # is excluded from this check (negatives represent loss-cap reversals
    # in some edge cases per RMA documentation).
    nonneg_int_cols = (
        "n_endorsements_earning",
        "n_endorsements_indemn",
        "n_head",
        "subsidy_amount",
        "total_premium_amount",
        "producer_premium_amount",
        "liability_amount",
        "total_weight_cwt",
    )
    for col in nonneg_int_cols:
        s = df[col].dropna()
        neg = int((s < 0).sum())
        if neg:
            issues.append(
                f"{col}: {neg} negative values (should be non-negative; "
                f"of {len(s):,} non-null)"
            )

    # 8. Snapshot filter must have been applied: only commodity 0801 + plan 81
    bad_commodity = int((df["commodity_code"] != "0801").sum())
    bad_plan = int((df["plan_code"] != "81").sum())
    if bad_commodity:
        issues.append(
            f"commodity_code: {bad_commodity} rows are not '0801' "
            "(snapshot filter not applied?)"
        )
    if bad_plan:
        issues.append(
            f"plan_code: {bad_plan} rows are not '81' "
            "(snapshot filter not applied?)"
        )

    return issues


# ---------------------------------------------------------------------------
# Year coverage
# ---------------------------------------------------------------------------


def _current_reinsurance_year() -> int:
    return datetime.now(timezone.utc).year


def _years_present_in_raw() -> set[int]:
    """Return the set of reinsurance years for which a raw zip exists.

    Looks for any ``lrp_<YYYY>_*.zip`` under ``data/raw/lrp/`` regardless
    of vintage — the validate step is concerned with years, not vintages.
    """
    years: set[int] = set()
    if not RAW_DIR.exists():
        return years
    for p in RAW_DIR.glob("lrp_*.zip"):
        parts = p.name.split("_")
        if len(parts) >= 2 and parts[1].isdigit():
            years.add(int(parts[1]))
    return years


def check_year_coverage(df: pd.DataFrame) -> list[str]:
    """Year sanity: parquet years are in range, and all raw years are represented."""
    issues: list[str] = []
    if df.empty:
        return ["DataFrame is empty (no rows to check year coverage)"]

    parquet_years = set(int(y) for y in df["reinsurance_year"].dropna().unique())
    current_year = _current_reinsurance_year()

    # 1. Every parquet year must be in [HISTORY_FLOOR_YEAR, current_year]
    out_of_range = sorted(
        y for y in parquet_years if y < HISTORY_FLOOR_YEAR or y > current_year
    )
    if out_of_range:
        issues.append(
            f"reinsurance_year: {len(out_of_range)} years outside "
            f"[{HISTORY_FLOOR_YEAR}, {current_year}]: {out_of_range[:8]}..."
        )

    # 2. Every raw-zip year must be represented in the parquet (catches a
    # snapshot run that silently dropped a year — e.g. a parse failure
    # that was swallowed somewhere upstream).
    raw_years = _years_present_in_raw()
    missing_from_parquet = sorted(raw_years - parquet_years)
    if missing_from_parquet:
        issues.append(
            f"reinsurance_year: {len(missing_from_parquet)} years on disk "
            f"but missing from parquet: {missing_from_parquet}"
        )

    return issues


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _run_all_checks() -> int:
    print("[validate] checking schema hash...", file=sys.stderr)
    schema_issues = check_schema_hash()
    if schema_issues:
        for line in schema_issues:
            print(f"  {line}", file=sys.stderr)
        return 1
    print(f"[validate]   schema hash OK: {schema_hash()[:16]}...", file=sys.stderr)

    if not LATEST_PARQUET_PATH.exists():
        print(
            f"[validate] {LATEST_PARQUET_PATH.name} missing; run snapshot first",
            file=sys.stderr,
        )
        return 1

    print(f"[validate] reading {LATEST_PARQUET_PATH.name}...", file=sys.stderr)
    df = pd.read_parquet(LATEST_PARQUET_PATH)
    print(
        f"[validate]   loaded {len(df):,} rows × {len(df.columns)} cols",
        file=sys.stderr,
    )

    print("[validate] checking value sanity...", file=sys.stderr)
    sanity_issues = check_value_sanity(df)

    print("[validate] checking year coverage...", file=sys.stderr)
    coverage_issues = check_year_coverage(df)

    all_issues = sanity_issues + coverage_issues
    if all_issues:
        print("[validate] ISSUES FOUND:", file=sys.stderr)
        for line in all_issues:
            print(f"  - {line}", file=sys.stderr)
        return 1

    print("[validate] all checks passed", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate freshly-snapshotted LRP data before publication.",
    )
    parser.add_argument(
        "--rebaseline",
        action="store_true",
        help=(
            "Regenerate expected_schema.sha256 from the current COLUMNS list. "
            "Deliberate action — only run after reviewing schema change."
        ),
    )
    args = parser.parse_args(argv)

    if args.rebaseline:
        live = schema_hash()
        try:
            old = read_baseline_hash()
        except (FileNotFoundError, RuntimeError):
            old = "(none)"
        if old == live:
            print(
                f"[validate] no rebaseline needed; baseline already matches "
                f"({live[:16]}...)",
                file=sys.stderr,
            )
            return 0
        write_baseline_hash(live)
        print(
            f"[validate] rebaselined expected_schema.sha256\n"
            f"           old: {old[:16]}{'...' if len(old) > 16 else ''}\n"
            f"           new: {live[:16]}...",
            file=sys.stderr,
        )
        return 0

    return _run_all_checks()


if __name__ == "__main__":
    raise SystemExit(main())
