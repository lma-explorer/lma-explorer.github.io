"""Write the validated LRP corpus to a vintage-stamped Parquet snapshot.

Reads all annual zips currently under ``data/raw/lrp/`` (parsed via
``pipelines.lrp.parse``), concatenates them into one tidy long-format
DataFrame, and writes:

    data/processed/lrp_premiums_<YYYY-MM-DD>.parquet   # this pull's snapshot
    data/processed/lrp_latest.parquet                  # convenience copy
    data/processed/lrp_MANIFEST.json                   # append-only audit record

Long-format schema (matches ``pipelines/lrp/parse.py:COLUMNS``):

    reinsurance_year         : Int16
    commodity_year           : Int16
    state_fips               : string
    state_abbr               : string      ("NM", "TX", ...)
    county_fips              : string
    county_name              : string
    commodity_code           : string      ("0801" for feeder cattle)
    commodity_name           : string
    plan_code                : string      ("81" for LRP)
    plan_name                : string
    type_code                : string      ("810" Steers / "820" Heifers / "830" combined)
    type_name                : string
    practice_code            : string
    practice_name            : string
    effective_date           : date
    length_weeks             : Int16       (one of 13, 17, 21, 26, 30, 34, 39, 43, 47, 52)
    coverage_price           : float64     ($/cwt)
    expected_end_value       : float64     ($/cwt)
    coverage_level_pct       : float64
    rate                     : float64
    cost_per_cwt             : float64     ($/cwt — premium per cwt)
    end_date                 : date
    n_endorsements_earning   : Int64
    n_endorsements_indemn    : Int64
    n_head                   : Int32
    total_weight_cwt         : float64
    subsidy_amount           : Int64       ($)
    total_premium_amount     : Int64       ($)
    producer_premium_amount  : Int64       ($)
    liability_amount         : Int64       ($)
    indemnity_amount         : Int64       ($, may be negative)
    vintage                  : date        (when this snapshot was first written)

The pipeline applies one filter at snapshot time: rows where
``commodity_code == "0801"`` AND ``plan_code == "81"``. Other livestock
commodities (Fed Cattle 0802, Swine 0815) and non-LRP plans are preserved
in the raw zips but do not land in the processed parquet — the platform's
focus is feeder-cattle LRP.

Within feeder cattle (commodity 0801) the parquet retains all 11 type
codes that appear historically: Steers Weight 1/2 (809/810), Heifers
Weight 1/2 (811/812), Brahman Weight 1/2 (813/814), Dairy Weight 1/2
(815/816), and the Unborn variants (817/818/819). Type-code narrowing
to the analytically-comparable subset {809, 810, 811, 812} happens at
backtest time in ``4.LRP-c``, not here.

Usage:
    python -m pipelines.lrp.snapshot

Status:
    Scaffold only. Constants and output-path conventions are final.
    Implementation raises NotImplementedError until 4.LRP-b.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "lrp"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "lrp_MANIFEST.json"

LATEST_NAME = "lrp_latest.parquet"

# Snapshot-time filter narrows to feeder-cattle LRP only. Other commodities
# (Fed Cattle 0802, Swine 0815) and other plans are dropped here.
#
# Type-code selection is INTENTIONALLY NOT applied at snapshot time. Within
# commodity 0801 there are 11 type codes covering Steers Weight 1/2, Heifers
# Weight 1/2, Brahman, Dairy, and Unborn variants (see parse.py:_TYPE_CODES_FEEDER
# for the full list). The 4.LRP-c backtest narrows to the analytically-relevant
# subset {809, 810, 811, 812} when joining with Clovis cash; the snapshot keeps
# the full feeder-cattle corpus so 4.LRP-d's volume/state visualizations can
# show the complete LRP picture, not just the backtested slice.
KEEP_COMMODITY_CODE = "0801"
KEEP_PLAN_CODE = "81"


def _vintage_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main(argv: list[str] | None = None) -> int:
    _ = argparse.ArgumentParser(
        description="Snapshot validated LRP data to vintage-stamped parquet.",
    ).parse_args(argv)

    raise NotImplementedError(
        "pipelines.lrp.snapshot is a scaffold. Implementation lands in 4.LRP-b."
    )


if __name__ == "__main__":
    raise SystemExit(main())
