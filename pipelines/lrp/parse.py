"""Extract one ``lrp_<YYYY>.zip`` and parse the pipe-delimited TXT inside.

The bulk-zip files have **no header row**. Column names come from the RMA
documentation in ``LRP_Summary_of_Business_All_Years.docx`` and are
reproduced as the ``COLUMNS`` constant below. Maintain this list in lock-step
with any future RMA schema change — the validator's schema-hash check
(see ``pipelines/lrp/validate.py``) keys off this list.

The 31-column schema (from RMA docs, transcribed M1.3):

    Source #  | Source name                  | Python name              | Dtype
    ----------+------------------------------+--------------------------+----------
       1      | Reinsurance Year             | reinsurance_year         | Int16
       2      | Commodity Year               | commodity_year           | Int16
       3      | Location State Code          | state_fips               | string
       4      | Location State Abbreviation  | state_abbr               | string
       5      | Location County Code         | county_fips              | string
       6      | Location County Name         | county_name              | string
       7      | Commodity Code               | commodity_code           | string
       8      | Commodity Name               | commodity_name           | string
       9      | Insurance Plan Code          | plan_code                | string
      10      | Insurance Plan Name          | plan_name                | string
      11      | Type Code                    | type_code                | string
      12      | Type Code Name               | type_name                | string
      13      | Practice Code                | practice_code            | string
      14      | Practice Code Name           | practice_name            | string
      15      | Sales Effective Date         | effective_date           | date
      16      | Endorsement Length           | length_weeks             | Int16
      17      | Coverage Price               | coverage_price           | float64
      18      | Expected End Value           | expected_end_value       | float64
      19      | Coverage Level Percent       | coverage_level_pct       | float64
      20      | Rate                         | rate                     | float64
      21      | Cost Per Cwt                 | cost_per_cwt             | float64
      22      | End Date                     | end_date                 | date
      23      | Endorsements Earning Premium | n_endorsements_earning   | Int64
      24      | Endorsements Indemnified     | n_endorsements_indemn    | Int64
      25      | Net Number of Head           | n_head                   | Int32
      26      | Total Weight                 | total_weight_cwt         | float64
      27      | Subsidy Amount               | subsidy_amount           | Int64
      28      | Total Premium Amount         | total_premium_amount     | Int64
      29      | Producer Premium Amount      | producer_premium_amount  | Int64
      30      | Liability Amount             | liability_amount         | Int64
      31      | Indemnity Amount             | indemnity_amount         | Int64    # may be negative

Backtest-relevant columns (the seven that drive 4.LRP-c) are:
    effective_date, coverage_price, expected_end_value, cost_per_cwt,
    end_date, liability_amount, indemnity_amount.

Geographic-scope columns (the four that drive 4.LRP-d's choropleth candidate)
are:
    state_fips, state_abbr, county_fips, county_name.

Product-scope columns for filtering to feeder-cattle steers/heifers are:
    commodity_code (== "0801"), plan_code (== "81"), type_code
    (810 = STEERS, 820 = HEIFERS, 830 = STEERS / HEIFERS combined).

Parsing rules:
    - Empty string ("") between two pipes is treated as NA, not as 0.
    - String columns are stripped of trailing whitespace (the source uses
      fixed-width-padded strings, e.g. "All Other Counties            ").
    - Integer columns use pandas' nullable Int16/Int32/Int64 so suppressed
      values stay as <NA> rather than forcing float upcasting.
    - Date columns parse YYYY-MM-DD; malformed dates become NaT.
    - The Indemnity Amount column ("S9(10)") tolerates negatives.

Smoke-test entry point:
    python -m pipelines.lrp.parse data/raw/lrp/lrp_2003_<vintage>.zip

Prints the parsed DataFrame's shape, dtypes, head, and a few key
value-distributions for quick sanity-checking.
"""

from __future__ import annotations

import argparse
import csv
import sys
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import NamedTuple

import pandas as pd


class _Col(NamedTuple):
    """One row of the 31-column schema mapping.

    ``index`` is 0-based for use with pandas.read_csv(usecols=...) /
    iloc-based access. ``source_name`` is the RMA-doc field name. ``name``
    is the Python-friendly snake_case rename used in the parquet. ``dtype``
    is the target pandas dtype after parsing.
    """

    index: int
    source_name: str
    name: str
    dtype: str


# The 31-column schema. ORDER MATTERS — the bulk-zip TXT has no header,
# so column index is the only way fields are identified.
COLUMNS: list[_Col] = [
    _Col(0, "Reinsurance Year", "reinsurance_year", "Int16"),
    _Col(1, "Commodity Year", "commodity_year", "Int16"),
    _Col(2, "Location State Code", "state_fips", "string"),
    _Col(3, "Location State Abbreviation", "state_abbr", "string"),
    _Col(4, "Location County Code", "county_fips", "string"),
    _Col(5, "Location County Name", "county_name", "string"),
    _Col(6, "Commodity Code", "commodity_code", "string"),
    _Col(7, "Commodity Name", "commodity_name", "string"),
    _Col(8, "Insurance Plan Code", "plan_code", "string"),
    _Col(9, "Insurance Plan Name", "plan_name", "string"),
    _Col(10, "Type Code", "type_code", "string"),
    _Col(11, "Type Code Name", "type_name", "string"),
    _Col(12, "Practice Code", "practice_code", "string"),
    _Col(13, "Practice Code Name", "practice_name", "string"),
    _Col(14, "Sales Effective Date", "effective_date", "date"),
    _Col(15, "Endorsement Length", "length_weeks", "Int16"),
    _Col(16, "Coverage Price", "coverage_price", "float64"),
    _Col(17, "Expected End Value", "expected_end_value", "float64"),
    _Col(18, "Coverage Level Percent", "coverage_level_pct", "float64"),
    _Col(19, "Rate", "rate", "float64"),
    _Col(20, "Cost Per Cwt", "cost_per_cwt", "float64"),
    _Col(21, "End Date", "end_date", "date"),
    _Col(22, "Endorsements Earning Premium", "n_endorsements_earning", "Int64"),
    _Col(23, "Endorsements Indemnified", "n_endorsements_indemn", "Int64"),
    _Col(24, "Net Number of Head", "n_head", "Int32"),
    _Col(25, "Total Weight", "total_weight_cwt", "float64"),
    _Col(26, "Subsidy Amount", "subsidy_amount", "Int64"),
    _Col(27, "Total Premium Amount", "total_premium_amount", "Int64"),
    _Col(28, "Producer Premium Amount", "producer_premium_amount", "Int64"),
    _Col(29, "Liability Amount", "liability_amount", "Int64"),
    _Col(30, "Indemnity Amount", "indemnity_amount", "Int64"),
]

assert len(COLUMNS) == 31, "LRP schema is 31 columns; revisit RMA docs if this fires."

# Commodity / plan filters used by the snapshot pipeline. Confirmed by
# diagnostic sweep across all 24 backfilled years (2003-2026):
#   commodity 0801 = "Feeder Cattle"  (the platform's scope)
#   commodity 0802 = "Fed Cattle"     (different market, dropped at snapshot)
#   commodity 0815 = "Swine"          (different market, dropped at snapshot)
FEEDER_CATTLE_COMMODITY_CODE = "0801"
LRP_PLAN_CODE = "81"

# Within feeder cattle (commodity 0801), the type-code taxonomy is:
#   809 = Steers Weight 1
#   810 = Steers Weight 2
#   811 = Heifers Weight 1
#   812 = Heifers Weight 2
#   813 = Brahman Weight 1   (rare, regional)
#   814 = Brahman Weight 2   (rare, regional)
#   815 = Dairy Weight 1     (different production system)
#   816 = Dairy Weight 2     (different production system)
#   817 = Unborn Steers & Heifers   (forward contract on calves)
#   818 = Unborn Brahman              (forward contract on calves)
#   819 = Unborn Dairy                 (forward contract on calves)
# Earlier years (2003-2020) use a subset of these; 813/814/817/818/819 are
# the new-since-2021 expansion. Type-code 997 ("NO PRACTICE SPECIFIED" /
# "NO TYPE SPECIFIED") appears in commodity 0815 (Swine) records, never
# inside 0801 in modern data, but the parser does not assume this.
FEEDER_CATTLE_TYPE_CODES_ALL = frozenset(
    {"809", "810", "811", "812", "813", "814", "815", "816", "817", "818", "819"}
)

# The analytically-comparable subset for the Clovis-cash backtest (4.LRP-c).
# These four type codes are conventional non-Brahman, non-dairy, born-cattle
# feeder classes that map to Clovis auction lots' Steers / Heifers x weight
# bins. Brahman, Dairy, and Unborn are excluded because they don't have a
# clean Clovis-cash counterpart.
FEEDER_CATTLE_TYPE_CODES_BACKTEST = frozenset({"809", "810", "811", "812"})

# Convenience views over COLUMNS used during dtype coercion.
_NAMES: list[str] = [c.name for c in COLUMNS]
_INT_COLS: list[_Col] = [c for c in COLUMNS if c.dtype.startswith("Int")]
_FLOAT_COLS: list[_Col] = [c for c in COLUMNS if c.dtype == "float64"]
_DATE_COLS: list[_Col] = [c for c in COLUMNS if c.dtype == "date"]
_STRING_COLS: list[_Col] = [c for c in COLUMNS if c.dtype == "string"]


def parse_lrp_txt(zip_path: str | Path) -> pd.DataFrame:
    """Extract a pubfs-rma LRP zip and parse its TXT into a tidy DataFrame.

    The zip is expected to contain exactly one file (``lrp_<YYYY>.txt``)
    whose contents are pipe-delimited, no header, 31 columns. The returned
    DataFrame has the COLUMNS-defined Python names and dtypes, with date
    columns parsed and integer columns nullable (Int16/Int32/Int64) so
    suppressed values can be NA without forcing float upcasting.

    Raises:
        FileNotFoundError: ``zip_path`` does not exist.
        zipfile.BadZipFile: the file is not a valid zip.
        RuntimeError: the zip contains zero or multiple inner files, or
            the parsed DataFrame is empty / has the wrong column count.
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        inner = zf.namelist()
        if len(inner) != 1:
            raise RuntimeError(
                f"Expected exactly one file inside {zip_path.name}, "
                f"found {len(inner)}: {inner!r}"
            )
        inner_name = inner[0]
        with zf.open(inner_name) as raw:
            # Read everything as string first; coerce dtypes after stripping
            # whitespace. Reading-then-coercing is more robust than passing
            # `dtype=` to read_csv because pandas' inference for nullable
            # ints + suppression markers ("") + decimal-leading-period
            # ("0.018060" written as ".018060") is fragile.
            text = TextIOWrapper(raw, encoding="utf-8", newline="")
            df = pd.read_csv(
                text,
                sep="|",
                header=None,
                names=_NAMES,
                dtype=str,
                keep_default_na=False,
                na_values=[""],
                quoting=csv.QUOTE_NONE,
                engine="c",
            )

    if df.empty:
        raise RuntimeError(f"Parsed DataFrame from {zip_path.name} is empty.")
    if len(df.columns) != len(COLUMNS):
        raise RuntimeError(
            f"Parsed DataFrame has {len(df.columns)} columns; expected "
            f"{len(COLUMNS)}. File may have a schema mismatch."
        )

    # Strip whitespace from string columns. The source uses fixed-width
    # padding (e.g., "All Other Counties            "); without strip the
    # parquet would carry trailing spaces forever.
    for col in _STRING_COLS:
        # df[col.name] is currently 'object' since we read everything as str.
        # Strip then convert to pandas StringDtype.
        df[col.name] = df[col.name].str.strip().astype("string")

    # Coerce numeric columns. ``errors='coerce'`` turns un-parsable values
    # into NaN, then .astype(nullable-int) turns NaN into <NA>.
    for col in _INT_COLS:
        df[col.name] = pd.to_numeric(df[col.name], errors="coerce").astype(col.dtype)

    for col in _FLOAT_COLS:
        df[col.name] = pd.to_numeric(df[col.name], errors="coerce").astype("float64")

    # Parse dates. format='%Y-%m-%d' enforces the documented format; any
    # malformed date becomes NaT. We keep dates as datetime64[ns] (not .dt.date)
    # so they round-trip through parquet cleanly via pyarrow.
    for col in _DATE_COLS:
        df[col.name] = pd.to_datetime(
            df[col.name], format="%Y-%m-%d", errors="coerce"
        )

    return df


# --------------------------------------------------------------------------
# Smoke-test CLI: parse one zip and print summary stats. Intentionally a
# library-only module + a small main() so a developer can sanity-check the
# parser on any single zip without writing a notebook cell.
# --------------------------------------------------------------------------


def _summary(df: pd.DataFrame) -> str:
    """Compose a human-readable summary of one parsed DataFrame."""
    lines = [
        f"shape: {df.shape}",
        "",
        "dtypes:",
        df.dtypes.to_string(),
        "",
        "head:",
        df.head(3).to_string(),
        "",
        "key value distributions:",
        f"  reinsurance_year unique: {sorted(df['reinsurance_year'].dropna().unique().tolist())}",
        f"  commodity_code value counts (top 5):",
        df["commodity_code"].value_counts().head(5).to_string(),
        "",
        f"  type_code value counts (top 10):",
        df["type_code"].value_counts().head(10).to_string(),
        "",
        f"  state_abbr unique count: {df['state_abbr'].nunique()}",
        f"  state_abbr top 10 by row count:",
        df["state_abbr"].value_counts().head(10).to_string(),
        "",
        f"  effective_date min/max: "
        f"{df['effective_date'].min()} / {df['effective_date'].max()}",
        f"  end_date min/max: "
        f"{df['end_date'].min()} / {df['end_date'].max()}",
        "",
        f"  coverage_price min/median/max: "
        f"{df['coverage_price'].min():.2f} / "
        f"{df['coverage_price'].median():.2f} / "
        f"{df['coverage_price'].max():.2f}",
        f"  indemnity_amount nonzero count: "
        f"{(df['indemnity_amount'] != 0).sum()}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test parse.parse_lrp_txt() against one zip and print summary stats.",
    )
    parser.add_argument(
        "zip_path",
        type=Path,
        help="Path to a single lrp_<YYYY>_<vintage>.zip under data/raw/lrp/.",
    )
    args = parser.parse_args(argv)

    df = parse_lrp_txt(args.zip_path)
    print(_summary(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
