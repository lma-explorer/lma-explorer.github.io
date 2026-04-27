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

Status:
    Scaffold only. The COLUMNS constant is final and ready to use.
    parse_lrp_txt() is intentionally NotImplementedError until 4.LRP-b.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple


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

# Type-code values that designate feeder cattle classes the platform cares
# about for the Clovis-cash backtest. Other type codes (e.g., fed cattle,
# swine, lamb) are filtered out at parse time.
FEEDER_CATTLE_TYPE_CODES = {
    "810",  # STEERS
    "820",  # HEIFERS
    "830",  # STEERS / HEIFERS combined
}

FEEDER_CATTLE_COMMODITY_CODE = "0801"
LRP_PLAN_CODE = "81"


def parse_lrp_txt(zip_path: Path) -> "object":  # pd.DataFrame at runtime
    """Extract a pubfs-rma LRP zip and parse its TXT into a tidy DataFrame.

    The zip is expected to contain exactly one file (``lrp_<YYYY>.txt``)
    whose contents are pipe-delimited, no header, 31 columns. The returned
    DataFrame has the COLUMNS-defined Python names and dtypes, with date
    columns parsed and integer columns nullable (Int16/Int32/Int64) so
    suppressed values can be NA without forcing float upcasting.

    Status: NotImplementedError until 4.LRP-b.
    """
    raise NotImplementedError(
        "parse_lrp_txt() is a 4.LRP-b deliverable. "
        "See pipelines/lrp/README.md for the build sequence."
    )
