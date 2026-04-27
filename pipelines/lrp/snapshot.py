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
    type_code                : string      (809-819 within feeder cattle)
    type_name                : string
    practice_code            : string
    practice_name            : string
    effective_date           : datetime64[ns]
    length_weeks             : Int16
    coverage_price           : float64     ($/cwt)
    expected_end_value       : float64     ($/cwt)
    coverage_level_pct       : float64
    rate                     : float64
    cost_per_cwt             : float64     ($/cwt — premium per cwt)
    end_date                 : datetime64[ns]
    n_endorsements_earning   : Int64
    n_endorsements_indemn    : Int64
    n_head                   : Int32
    total_weight_cwt         : float64
    subsidy_amount           : Int64       ($)
    total_premium_amount     : Int64       ($)
    producer_premium_amount  : Int64       ($)
    liability_amount         : Int64       ($)
    indemnity_amount         : Int64       ($, may be negative)
    vintage                  : datetime64[ns] (when this snapshot was first written)

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
    python -m pipelines.lrp.snapshot                  # use today's vintage
    python -m pipelines.lrp.snapshot --vintage 2026-04-27   # override (testing)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipelines.lrp.parse import parse_lrp_txt

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "lrp"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "lrp_MANIFEST.json"

LATEST_NAME = "lrp_latest.parquet"
SNAPSHOT_NAME_PATTERN = "lrp_premiums_{vintage}.parquet"

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

# Filename pattern: lrp_<YEAR>_<VINTAGE>.zip. Capture the year and the vintage.
# Example: lrp_2024_2026-04-27.zip → year=2024, vintage=2026-04-27.
_FILENAME_RE = re.compile(r"^lrp_(\d{4})_(\d{4}-\d{2}-\d{2})\.zip$")


def _vintage_tag() -> str:
    """Return a YYYY-MM-DD tag for the current UTC publication moment."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _list_raw_zips_for_vintage(vintage: str) -> list[tuple[int, Path]]:
    """Find all zips matching ``lrp_<YYYY>_<vintage>.zip``.

    Returns a list of (reinsurance_year, path) tuples sorted by year. Files
    not matching the pattern (or with a different vintage) are silently
    skipped — the user is expected to pick a single vintage worth of zips
    via ``--vintage`` if multiple coexist on disk.
    """
    out: list[tuple[int, Path]] = []
    if not RAW_DIR.exists():
        return out
    for p in sorted(RAW_DIR.iterdir()):
        m = _FILENAME_RE.match(p.name)
        if not m:
            continue
        year = int(m.group(1))
        file_vintage = m.group(2)
        if file_vintage != vintage:
            continue
        out.append((year, p))
    return sorted(out, key=lambda t: t[0])


def _build_snapshot_dataframe(
    zips: list[tuple[int, Path]], vintage: str
) -> tuple[pd.DataFrame, list[dict]]:
    """Parse each zip, filter, concat, return (combined_df, per_year_breakdown).

    ``breakdown`` is a list of per-year dicts capturing rows kept, unique
    state count (excluding XX), XX rows, NM rows, and feeder type-code
    distribution. These are persisted in the MANIFEST so 4.LRP-d can build
    a "data window starts here" annotation without re-deriving from the
    parquet.
    """
    frames: list[pd.DataFrame] = []
    breakdown: list[dict] = []
    vintage_ts = pd.Timestamp(vintage)

    for year, zp in zips:
        print(f"[snapshot] parsing {zp.name}", file=sys.stderr)
        df_year = parse_lrp_txt(zp)

        # Apply snapshot filter: feeder cattle (0801) under LRP plan (81).
        mask = (df_year["commodity_code"] == KEEP_COMMODITY_CODE) & (
            df_year["plan_code"] == KEEP_PLAN_CODE
        )
        df_year = df_year.loc[mask].copy()

        # Capture per-year breakdown stats BEFORE adding the vintage column,
        # so the breakdown reflects the parquet's actual feeder-cattle
        # coverage rather than including the synthetic vintage timestamp.
        type_code_counts = (
            df_year["type_code"].value_counts().to_dict() if not df_year.empty else {}
        )
        breakdown.append(
            {
                "reinsurance_year": year,
                "rows": int(len(df_year)),
                "unique_states_excl_xx": int(
                    df_year.loc[df_year["state_abbr"] != "XX", "state_abbr"].nunique()
                ),
                "rows_xx": int((df_year["state_abbr"] == "XX").sum()),
                "rows_nm": int((df_year["state_abbr"] == "NM").sum()),
                "type_code_counts": {str(k): int(v) for k, v in type_code_counts.items()},
            }
        )

        if not df_year.empty:
            df_year["vintage"] = vintage_ts
            frames.append(df_year)

    if not frames:
        return pd.DataFrame(), breakdown

    out = pd.concat(frames, ignore_index=True)

    # Sort deterministically so byte-identical content yields a byte-identical
    # parquet (critical for the SHA recorded in the MANIFEST). Sort key is
    # the natural reading order: year ascending, then within year by sale
    # date, state, county, type, length, coverage price.
    out = out.sort_values(
        by=[
            "reinsurance_year",
            "effective_date",
            "state_abbr",
            "county_fips",
            "type_code",
            "length_weeks",
            "coverage_price",
        ],
        na_position="last",
        kind="mergesort",  # stable sort
    ).reset_index(drop=True)

    return out, breakdown


def _write_manifest_entry(
    vintage: str,
    snapshot_path: Path,
    row_count: int,
    breakdown: list[dict],
) -> None:
    """Append (or replace) the entry for this vintage in lrp_MANIFEST.json.

    The manifest is keyed by vintage; re-running snapshot for the same
    vintage replaces the prior entry rather than duplicating it.
    """
    entry = {
        "vintage": vintage,
        "file": snapshot_path.relative_to(REPO_ROOT).as_posix(),
        "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
        "rows": row_count,
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "annual_breakdown": breakdown,
    }
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"slug": "LRP_PUBFS", "entries": []}
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Snapshot validated LRP data to vintage-stamped parquet.",
    )
    parser.add_argument(
        "--vintage",
        type=str,
        default=None,
        help=(
            "Use this vintage tag (YYYY-MM-DD) instead of today's UTC date. "
            "Required when re-snapshotting an older raw pull, or in tests."
        ),
    )
    args = parser.parse_args(argv)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    vintage = args.vintage or _vintage_tag()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", vintage):
        parser.error(f"--vintage must be YYYY-MM-DD; got {vintage!r}")

    zips = _list_raw_zips_for_vintage(vintage)
    if not zips:
        print(
            f"[snapshot] no zips found for vintage {vintage} in {RAW_DIR}",
            file=sys.stderr,
        )
        return 1
    print(
        f"[snapshot] reading {len(zips)} zip(s) for vintage {vintage} "
        f"(years {zips[0][0]}-{zips[-1][0]})",
        file=sys.stderr,
    )

    df, breakdown = _build_snapshot_dataframe(zips, vintage)

    if df.empty:
        print(
            "[snapshot] no feeder-cattle LRP rows after filter; nothing to write",
            file=sys.stderr,
        )
        return 1

    snapshot_path = PROCESSED_DIR / SNAPSHOT_NAME_PATTERN.format(vintage=vintage)
    latest_path = PROCESSED_DIR / LATEST_NAME

    df.to_parquet(snapshot_path, index=False)
    df.to_parquet(latest_path, index=False)
    _write_manifest_entry(vintage, snapshot_path, len(df), breakdown)

    # Final summary for the operator.
    nm_rows = int((df["state_abbr"] == "NM").sum())
    xx_rows = int((df["state_abbr"] == "XX").sum())
    n_states = int(df.loc[df["state_abbr"] != "XX", "state_abbr"].nunique())
    type_code_counts = df["type_code"].value_counts().head(8).to_dict()

    print(f"[snapshot] wrote {snapshot_path.name} ({len(df):,} rows)")
    print(f"[snapshot] wrote {latest_path.name}")
    print(f"[snapshot] updated {MANIFEST_PATH.name} with {len(breakdown)} per-year entries")
    print(f"[snapshot] coverage: {len(zips)} years; "
          f"{n_states} states (excluding XX); "
          f"{nm_rows:,} NM rows; {xx_rows:,} XX (national-aggregate) rows")
    print(f"[snapshot] top type codes (within 0801): "
          + ", ".join(f"{k}={v:,}" for k, v in type_code_counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
