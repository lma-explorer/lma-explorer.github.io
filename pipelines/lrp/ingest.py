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
    python -m pipelines.lrp.ingest --backfill --force  # ignore existing-vintage cache

Writes raw zips to:
    data/raw/lrp/lrp_<YYYY>_<vintage>.zip

where <vintage> is the current UTC date (YYYY-MM-DD). The vintage is the
*publication* moment the zip was first downloaded under, not the reinsurance year.

Caching behavior:
    A backfill or routine pull will skip any year whose zip already exists
    under data/raw/lrp/ for the current vintage AND validates as a real zip.
    This makes the 24-year backfill resumable across runs without repeated
    network traffic. Pass --force to override.
"""

from __future__ import annotations

import argparse
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

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

# Politeness delay between successive year fetches in backfill mode. RMA's
# pubfs is a static file server and we never expect to be rate-limited, but
# courtesy and obvious-non-abuse are cheap.
BACKFILL_DELAY_SECONDS = 1.0

# Sanity floor for downloaded content. A real LRP zip is at least ~18 KB
# (the 2003 zip, our smallest year). Anything smaller than 1 KB is almost
# certainly an HTML error page from a proxy or a misrouted CDN response.
MIN_PLAUSIBLE_BYTES = 1024

# A pubfs HTTPS GET sometimes returns a server-template HTML page when the
# requested file is missing. We send a User-Agent so the response is
# attributable; the value is informational only.
USER_AGENT = (
    "lma-explorer/1.0 (+https://github.com/lma-explorer/lma-explorer.github.io)"
)

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


def _zip_url(year: int) -> str:
    """Build the canonical pubfs-rma URL for one reinsurance year."""
    return f"{PUBFS_BASE_URL}/{ZIP_NAME_PATTERN.format(year=year)}"


def _out_path(year: int, vintage: str) -> Path:
    """Return the on-disk path the zip will be written to."""
    return RAW_DIR / f"lrp_{year}_{vintage}.zip"


def _get_with_retry(url: str) -> bytes:
    """GET binary content from pubfs-rma with retries on transient errors.

    5xx and connection errors retry with linear backoff. 4xx is surfaced
    immediately — for a static-file URL it almost always means the URL is
    wrong (e.g., year out of range), and a retry will not fix that.
    """
    last_exc: Exception | None = None
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"pubfs-rma server error {resp.status_code}")
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            sleep_seconds = RETRY_BACKOFF_SECONDS * attempt
            print(
                f"[ingest] attempt {attempt} failed: {exc}; retrying in {sleep_seconds}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
    assert last_exc is not None
    raise last_exc


def fetch_year(year: int, *, force: bool = False) -> Path:
    """Download one ``lrp_<year>.zip`` and write it under ``data/raw/lrp/``.

    Returns the local path. If a zip for this (year, vintage) already exists
    on disk and validates as a real zip, the function returns immediately
    without issuing a network request. Pass ``force=True`` to override.

    Raises:
        ValueError: ``year`` is outside the supported range.
        RuntimeError: download succeeded but the content is too small to be
            a real zip, or is not a valid zip.
        requests.RequestException: the underlying GET ultimately failed.
    """
    if year < HISTORY_FLOOR_YEAR or year > _current_reinsurance_year():
        raise ValueError(
            f"Year {year} is outside the supported range "
            f"[{HISTORY_FLOOR_YEAR}, {_current_reinsurance_year()}]."
        )

    vintage = _vintage_tag()
    out_path = _out_path(year, vintage)

    # Resumability: skip if a same-vintage zip is already on disk and valid.
    if not force and out_path.exists():
        if zipfile.is_zipfile(out_path):
            print(
                f"[ingest] skip {year} — already on disk at {out_path.name} "
                f"({out_path.stat().st_size:,} bytes)",
                file=sys.stderr,
            )
            return out_path
        # Stale / corrupt prior write — drop it and re-fetch.
        out_path.unlink(missing_ok=True)

    url = _zip_url(year)
    print(f"[ingest] GET {url}", file=sys.stderr)
    content = _get_with_retry(url)

    if len(content) < MIN_PLAUSIBLE_BYTES:
        raise RuntimeError(
            f"Downloaded content for year {year} is only {len(content)} bytes; "
            f"expected at least {MIN_PLAUSIBLE_BYTES}. Likely an error page rather "
            f"than a zip. URL was {url}."
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(content)

    if not zipfile.is_zipfile(out_path):
        # Roll back the bad write so a subsequent run does not see a stale
        # corrupt file under data/raw/lrp/.
        out_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Downloaded content for year {year} ({len(content)} bytes) is not "
            f"a valid zip. URL was {url}."
        )

    print(f"[ingest] wrote {out_path.name} ({len(content):,} bytes)", file=sys.stderr)
    return out_path


def fetch_years(years: list[int], *, force: bool = False) -> tuple[list[Path], list[tuple[int, Exception]]]:
    """Fetch a list of years sequentially, with politeness delay.

    Returns ``(succeeded_paths, failed_year_exceptions)``. The function does
    not raise on individual failures — the caller decides whether one
    failed year warrants stopping the whole backfill.
    """
    succeeded: list[Path] = []
    failures: list[tuple[int, Exception]] = []
    for i, year in enumerate(years):
        try:
            succeeded.append(fetch_year(year, force=force))
        except Exception as exc:  # noqa: BLE001 — report-and-continue is intentional
            failures.append((year, exc))
            print(f"[ingest] year {year} failed: {exc}", file=sys.stderr)
        if i < len(years) - 1:
            time.sleep(BACKFILL_DELAY_SECONDS)
    return succeeded, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch RMA LRP annual summary-of-business zips.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=(
            "Fetch the full available history "
            f"({HISTORY_FLOOR_YEAR}-present) instead of the latest year only."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Fetch one specific reinsurance year (debugging convenience).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Re-download even if a same-vintage zip is already on disk. "
            "Default is to skip cached zips."
        ),
    )
    args = parser.parse_args(argv)

    if args.year is not None and args.backfill:
        parser.error("--year and --backfill are mutually exclusive")

    if args.backfill:
        years = list(range(HISTORY_FLOOR_YEAR, _current_reinsurance_year() + 1))
    elif args.year is not None:
        years = [args.year]
    else:
        years = [_current_reinsurance_year()]

    succeeded, failures = fetch_years(years, force=args.force)

    print(
        f"[ingest] fetched {len(succeeded)} of {len(years)} years"
        + (f"; {len(failures)} failed" if failures else ""),
        file=sys.stderr,
    )
    if failures:
        for year, exc in failures:
            print(f"[ingest]   FAILED {year}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
