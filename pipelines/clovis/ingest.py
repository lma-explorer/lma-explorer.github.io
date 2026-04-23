"""Fetch the USDA-AMS MARS report AMS_1781 (Clovis Livestock Auction).

Usage:
    python -m pipelines.clovis.ingest              # routine: latest report only
    python -m pipelines.clovis.ingest --backfill   # initial: full MARS history

Environment:
    AMS_MARS_API_KEY   MARS API key. Free registration at
                       https://mymarketnews.ams.usda.gov -> My Profile.
                       The key is used as the HTTP Basic Auth username; the
                       password half is empty. See MARS docs:
                       https://mymarketnews.ams.usda.gov/mars-api/getting-started

Writes raw JSON to:
    data/raw/clovis/AMS_1781_<vintage>.json

where <vintage> is the current UTC date (YYYY-MM-DD). The vintage is the
*publication* moment the data was first seen under, not the auction date.
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

# MARS API endpoint for fetching a single report by slug_id. The v1.1 path is
# the current stable documented endpoint per basic-instructions as of 2026-04.
# When MARS moves to v2/v3 this constant is the only thing we change.
MARS_ENDPOINT = "https://marsapi.ams.usda.gov/services/v1.1/reports/{slug_id}"
SLUG_ID = "1781"
SLUG_NAME = "AMS_1781"

# MARS caps an unauthenticated request at 5,000 rows and an authenticated
# request at 100,000. The full Clovis history through the MARS window is well
# under that ceiling, so a single call is sufficient; we keep a sanity cap
# for defensive purposes.
MAX_ROWS_PER_REQUEST = 100_000
REQUEST_TIMEOUT_SECONDS = 45
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis"


def _vintage_tag() -> str:
    """Return a YYYY-MM-DD tag for the current UTC publication moment."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _auth() -> tuple[str, str]:
    """Return the (key, empty_password) tuple for MARS HTTP Basic Auth."""
    key = os.environ.get("AMS_MARS_API_KEY")
    if not key:
        raise RuntimeError(
            "AMS_MARS_API_KEY not set. Register for a free key at "
            "https://mymarketnews.ams.usda.gov (My Profile) and expose it as "
            "an environment variable or a GitHub Actions secret."
        )
    return key, ""


def _get_with_retry(url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
    """GET against MARS with retries on transient errors.

    5xx and connection errors retry with linear backoff. 4xx is surfaced
    immediately — it almost always means an auth or query-shape problem
    the retry won't fix.
    """
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                auth=_auth(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"MARS server error {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
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


def fetch_clovis(backfill: bool = False) -> dict[str, Any]:
    """Fetch the Clovis report body from MARS.

    For a routine pull we request the most recent observation only, by not
    passing a date-range filter. For a backfill we request the full MARS
    history, which MARS returns paginated — the exact pagination query-string
    syntax is filled in once the response shape is confirmed from a live
    call (see TODO below).
    """
    url = MARS_ENDPOINT.format(slug_id=SLUG_ID)
    params: dict[str, str] = {}
    if backfill:
        # TODO once the live MARS response is inspected: add the correct
        # date-range filter. MARS v1.1 accepts a `q` parameter with field
        # filters (e.g. `q=report_begin_date>=01/01/2017`), but the exact
        # field name for the Clovis report's auction date is schema-dependent
        # and gets confirmed from the first real response.
        pass
    return _get_with_retry(url, params=params or None)


def _write_raw(body: dict[str, Any]) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    vintage = _vintage_tag()
    out_path = RAW_DIR / f"{SLUG_NAME}_{vintage}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(body, f, indent=2, sort_keys=True)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch USDA-AMS Clovis Livestock Auction report (AMS_1781).",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Request the full MARS-available history (rather than the latest report only).",
    )
    args = parser.parse_args(argv)

    body = fetch_clovis(backfill=args.backfill)
    out_path = _write_raw(body)
    print(f"[ingest] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
