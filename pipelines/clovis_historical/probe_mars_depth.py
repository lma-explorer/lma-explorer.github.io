"""Probe the live MARS API to find out how deep the AMS_1781 history goes.

Sub-task 9.7a in PLAN_4.1: closes open question §10.4. Determines whether
the live ``GET /services/v1.1/reports/1781`` returns observations all the
way back to April 17, 2019 (the Era B/A handoff date documented in §14.2)
or only the last few years. The answer dictates whether sub-task 9.8
(site read-path wiring) needs to handle a 2-segment time series
(MARS API + Era B TXT) or a 3-segment series with an Era A PDF backfill
in the middle.

This script makes ONE authenticated API call, summarizes coverage by year,
and prints a depth verdict. Does not write to ``data/raw/`` — that's
``pipelines/clovis/ingest.py``'s job. Read-only diagnostic.

Auth: same ``AMS_MARS_API_KEY`` env var the live pipeline uses.

Usage:
    AMS_MARS_API_KEY=... python -m pipelines.clovis_historical.probe_mars_depth

Output:
    - Total record count returned by MARS for slug 1781
    - Year histogram of report_begin_date
    - Earliest and latest report_begin_date
    - Verdict: COVERED / GAP / UNKNOWN, with the gap span if applicable

Known unknowns this probe also surfaces:
    - The exact JSON shape of the v1.1 response (the live pipeline has a
      TODO about confirming field names). The probe prints the top-level
      keys and a sample record so future code-paths can reference it.
    - Whether the 5,000-record cap (unauth) or 100,000-record cap (auth)
      is being hit, which would indicate a paginated response that this
      probe doesn't yet walk.
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import date

import requests

MARS_ENDPOINT = "https://marsapi.ams.usda.gov/services/v1.1/reports/1781"
ERA_A_FLOOR = date(2019, 4, 17)  # First MMN PDF after the Era B/A handoff


def _auth() -> tuple[str, str]:
    key = os.environ.get("AMS_MARS_API_KEY")
    if not key:
        print(
            "ERROR: AMS_MARS_API_KEY not set in environment.\n"
            "Register at https://mymarketnews.ams.usda.gov -> My Profile, "
            "then export the key:\n"
            "    export AMS_MARS_API_KEY='<your-key>'",
            file=sys.stderr,
        )
        sys.exit(2)
    return key, ""


def _extract_records(body) -> list[dict]:
    """The v1.1 response is one of: a top-level list, or a dict with
    'results' / 'data' / 'records'. Be permissive."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("results", "data", "records", "items"):
            if key in body and isinstance(body[key], list):
                return body[key]
    return []


def _date_field(rec: dict) -> str | None:
    """Find the auction-date field. MARS reports tend to use one of these
    field names depending on slug; we try them in order."""
    for key in (
        "report_begin_date",
        "report_end_date",
        "auction_date",
        "report_date",
        "publication_date",
    ):
        v = rec.get(key)
        if v:
            return str(v)
    return None


def _parse_date(s: str) -> date | None:
    # MARS dates are typically MM/DD/YYYY or YYYY-MM-DD. Try both.
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            from datetime import datetime
            return datetime.strptime(s.split("T")[0].split(" ")[0], fmt).date()
        except ValueError:
            continue
    return None


def main() -> int:
    auth = _auth()
    print(f"GET {MARS_ENDPOINT}")
    try:
        resp = requests.get(MARS_ENDPOINT, auth=auth, timeout=60)
    except requests.RequestException as e:
        print(f"FAIL: request error: {e}", file=sys.stderr)
        return 3
    print(f"  HTTP {resp.status_code} | {len(resp.content)} bytes | {resp.elapsed.total_seconds():.2f}s")
    if resp.status_code != 200:
        print(f"FAIL: non-200 response\n--- body (first 600 chars) ---\n{resp.text[:600]}",
              file=sys.stderr)
        return 4

    try:
        body = resp.json()
    except ValueError as e:
        print(f"FAIL: response not JSON: {e}", file=sys.stderr)
        return 5

    # Document response shape
    print("\n=== Response shape ===")
    if isinstance(body, dict):
        print(f"top-level keys: {sorted(body.keys())}")
    else:
        print(f"top-level: {type(body).__name__}")

    records = _extract_records(body)
    print(f"records: {len(records)}")
    if not records:
        print(
            "FAIL: no records extracted from response. Response shape may have changed; "
            "inspect raw body and update _extract_records.",
            file=sys.stderr,
        )
        # Print a snippet for diagnostics
        import json
        print(json.dumps(body, indent=2)[:1500], file=sys.stderr)
        return 6

    # Sample record — show the first one
    print("\n=== Sample record (first) ===")
    sample = records[0]
    if isinstance(sample, dict):
        for k in sorted(sample.keys())[:20]:
            v = sample[k]
            disp = repr(v)[:80]
            print(f"  {k}: {disp}")

    # Date-field detection
    date_strs = [_date_field(r) for r in records if isinstance(r, dict)]
    date_strs = [d for d in date_strs if d]
    parsed = [_parse_date(d) for d in date_strs]
    parsed = [d for d in parsed if d]
    if not parsed:
        print("FAIL: could not extract dates from any record (all tried fields empty/unparseable).")
        return 7

    min_d, max_d = min(parsed), max(parsed)
    year_hist = Counter(d.year for d in parsed)

    print(f"\n=== Coverage ===")
    print(f"records with parseable date: {len(parsed)} / {len(records)}")
    print(f"earliest date: {min_d}")
    print(f"latest   date: {max_d}")
    print(f"year histogram:")
    for y in sorted(year_hist):
        print(f"  {y}: {year_hist[y]}")

    # Cap signal — if we got exactly 5000 or 100000, there's likely more
    if len(records) in (5000, 100000):
        print(
            f"\nWARNING: record count is exactly {len(records)} — that's the "
            "row cap; pagination may be needed to see further history."
        )

    # Verdict
    print("\n=== Verdict for sub-task 9.8 read-path planning ===")
    if min_d <= ERA_A_FLOOR:
        gap_days = (ERA_A_FLOOR - min_d).days
        print(
            f"COVERED: MARS API returns history back to {min_d} "
            f"(≤ {ERA_A_FLOOR}, the Era A floor). 9.8 is a 2-segment join: "
            "MARS API + Era B TXT. No PDF backfill needed."
        )
        if gap_days < 0:
            print(f"  (MARS depth is {-gap_days} days deeper than Era A floor.)")
    else:
        gap_days = (min_d - ERA_A_FLOOR).days
        gap_weeks = gap_days // 7
        print(
            f"GAP: MARS API earliest is {min_d} (after {ERA_A_FLOOR} Era A floor). "
            f"Gap = {gap_days} days (~{gap_weeks} weeks) of Era A PDFs need backfilling. "
            "9.8 is a 3-segment join: MARS API (recent) + Era A PDF backfill (gap) + "
            "Era B TXT (historical)."
        )
        print(
            "  Action: extend download_era_b.py with --era A and a PDF parser, OR "
            "use MARS report_begin_date filter to refresh the API call. The PDFs "
            "and the API both exist in MMN; choose whichever is faster."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
