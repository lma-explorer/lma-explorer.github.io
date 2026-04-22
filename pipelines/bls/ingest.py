"""Fetch the BLS CPI-U, All Items, U.S. City Average, NSA (series CUUR0000SA0).

Usage:
    python -m pipelines.bls.ingest              # routine pull: trailing year only
    python -m pipelines.bls.ingest --backfill   # initial backfill: 1985 -> current

Environment:
    BLS_API_KEY   v2 API key (free, registered via email form).

Writes raw JSON to:
    data/raw/bls/CUUR0000SA0_<vintage>.json

where <vintage> is the current UTC date in YYYY-MM format. The "vintage" is
the BLS *publication* month the data was first seen under, not the data
month itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BLS_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SERIES_ID = "CUUR0000SA0"
EARLIEST_YEAR = 1985
BACKFILL_CHUNK_YEARS = 19  # BLS caps a single query at 20 years; 19 keeps a 1-yr safety margin.
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "bls"


def _vintage_tag() -> str:
    """Return the YYYY-MM tag for the current BLS publication month."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _current_year() -> int:
    return datetime.now(timezone.utc).year


def _post_with_retry(payload: dict[str, Any]) -> dict[str, Any]:
    """POST to the BLS API, retrying on transient HTTP errors.

    We retry on 5xx and connection errors. A 4xx is usually a permanent
    configuration problem (bad key, bad series ID) and is surfaced immediately.
    """
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                BLS_ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"BLS server error {resp.status_code}")
            resp.raise_for_status()
            body = resp.json()
            # BLS returns HTTP 200 even on logical failure; check the status field.
            if body.get("status") != "REQUEST_SUCCEEDED":
                msgs = body.get("message") or ["<no message>"]
                raise RuntimeError(f"BLS logical failure: {msgs}")
            return body
        except (requests.RequestException, RuntimeError) as exc:
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


def fetch_cpi_u(start_year: int, end_year: int) -> dict[str, Any]:
    """Fetch a single chunk of the CPI-U series.

    BLS caps a request at 20 years; callers that need a longer span should
    chunk and merge at the caller level (see ``fetch_backfill``).
    """
    if end_year - start_year + 1 > 20:
        raise ValueError(
            f"BLS caps a single request at 20 years; got {start_year}-{end_year}"
        )
    api_key = os.environ.get("BLS_API_KEY")
    if not api_key:
        raise RuntimeError(
            "BLS_API_KEY not set. Register for a free v2 key at "
            "https://www.bls.gov/developers/ and expose it as an environment "
            "variable or a GitHub Actions secret."
        )
    payload = {
        "seriesid": [SERIES_ID],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key,
    }
    return _post_with_retry(payload)


def fetch_backfill() -> list[dict[str, Any]]:
    """Fetch the full series, chunking across BLS's 20-year per-query cap.

    Returns a list of raw API bodies, one per chunk. Downstream code is
    responsible for merging them.
    """
    bodies: list[dict[str, Any]] = []
    start = EARLIEST_YEAR
    end_cap = _current_year()
    while start <= end_cap:
        end = min(start + BACKFILL_CHUNK_YEARS, end_cap)
        print(f"[ingest] backfill chunk {start}-{end}", file=sys.stderr)
        bodies.append(fetch_cpi_u(start, end))
        start = end + 1
    return bodies


def fetch_trailing_year() -> dict[str, Any]:
    """Fetch the trailing year of the series (the routine monthly pull)."""
    current = _current_year()
    return fetch_cpi_u(current - 1, current)


def _write_raw(body: dict[str, Any] | list[dict[str, Any]]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    vintage = _vintage_tag()
    out_path = RAW_DIR / f"{SERIES_ID}_{vintage}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(body, f, indent=2, sort_keys=True)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch BLS CPI-U data.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch the full series from 1985 to today (multi-chunk).",
    )
    args = parser.parse_args(argv)

    if args.backfill:
        body: Any = fetch_backfill()
    else:
        body = fetch_trailing_year()

    out_path = _write_raw(body)
    print(f"[ingest] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
