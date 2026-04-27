"""Fetch USDA RMA Livestock Risk Protection (LRP) annual summary-of-business files.

Source:
    https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/
    livestock_and_dairy_participation/

The RMA publishes one zip per reinsurance year, named ``lrp_<YYYY>.zip``.
Each zip contains a single pipe-delimited TXT file with no header row,
holding all LRP endorsements for that reinsurance year. The series begins
with reinsurance year 2003 and currently extends through 2026.

The 31-column schema is documented in ``LRP_Summary_of_Business_All_Years.docx``
on the same RMA directory and reproduced (Python-friendly form) in
``pipelines/lrp/parse.py:COLUMNS``.

Usage:
    python -m pipelines.lrp.ingest                   # routine: latest year only
    python -m pipelines.lrp.ingest --backfill        # initial: full history (2003-present)
    python -m pipelines.lrp.ingest --year 2024       # one specific year (debugging)

Writes raw zips to:
    data/raw/lrp/lrp_<YYYY>_<vintage>.zip

where <vintage> is the current UTC date (YYYY-MM-DD). The vintage is the
*publication* moment the zip was first downloaded under, not the reinsurance year.

Status:
    Scaffold only. The fetch logic is intentionally a NotImplementedError
    until 4.LRP-b is implemented. The constants (URL pattern, year bounds,
    output paths) are correct and ready to use.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

# RMA pubfs base URL. The zips live one level below this and are named
# ``lrp_<YYYY>.zip``. RMA's HTTPS host serves them directly; no auth required.
PUBFS_BASE_URL = (
    "https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/"
    "Summary_of_Business/livestock_and_dairy_participation"
)
ZIP_NAME_PATTERN = "lrp_{year}.zip"

# Series bounds. Earliest available is reinsurance year 2003 per the directory
# listing (probe finding, M1.3 / matrix v0.3). Latest is the current
# reinsurance year, defaulted via _current_reinsurance_year().
HISTORY_FLOOR_YEAR = 2003

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "lrp"


def _vintage_tag() -> str:
    """Return a YYYY-MM-DD tag for the current UTC publication moment."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _current_reinsurance_year() -> int:
    """Return the current reinsurance year.

    RMA's reinsurance year aligns with the calendar year for LRP. The
    "current" year is therefore today's calendar year — RMA may not have
    published a complete file for it yet, but the ingest will fetch
    whatever partial-year data is currently posted.
    """
    return datetime.now(timezone.utc).year


def fetch_year(year: int) -> Path:
    """Download one ``lrp_<year>.zip`` and write it under data/raw/lrp/.

    Returns the local path. Raises if the download fails or the file is
    suspiciously small (< 1 KB suggests an HTML error page rather than a
    real zip).

    Status: NotImplementedError until 4.LRP-b.
    """
    raise NotImplementedError(
        "fetch_year() is a 4.LRP-b deliverable. "
        "See pipelines/lrp/README.md for the build sequence."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch RMA LRP annual summary-of-business zips.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch the full available history (2003-present) instead of the latest year only.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Fetch one specific reinsurance year (debugging convenience).",
    )
    _ = parser.parse_args(argv)

    raise NotImplementedError(
        "pipelines.lrp.ingest is a scaffold. Implementation lands in 4.LRP-b."
    )


if __name__ == "__main__":
    raise SystemExit(main())
