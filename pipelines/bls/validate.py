"""Validate a freshly-pulled BLS CPI response before it is snapshotted.

Three independent checks run in sequence; any one of them failing causes the
pipeline to exit non-zero so the scheduled workflow opens a data-source-drift
issue and does not commit a new vintage.

    1. Schema-hash check: a hash derived from the set of field names at three
       levels of the response (root, series, data rows) must equal the hash
       committed in ``expected_schema.sha256``. This catches the overwhelming
       majority of BLS structural changes. It does NOT check field types or
       value formats -- those are caught by check 2.

    2. Month-over-month sanity: no MoM absolute change may exceed 5 percent.
       CPI has not moved that fast even in recent historical data; a value
       beyond that almost certainly indicates a unit error or a decimal shift.

    3. Continuity: the new vintage may extend the last committed vintage by
       at most one new period, and may not rewrite history more than two
       periods back (BLS occasionally revises the most recent month or two).

This module reads the most recent raw file under data/raw/bls/ and the most
recent committed parquet under data/processed/cpi_*.parquet (skipping the
article-basis file). It writes nothing.

Usage:
    python -m pipelines.bls.validate                 # validate latest raw
    python -m pipelines.bls.validate --rebaseline    # regenerate schema hash

The --rebaseline flag is a deliberate, reviewed action: run it only after you
have read the raw JSON, decided that the new schema shape is acceptable, and
propagated any downstream code changes. Never rebaseline to make a failing
pipeline green.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "bls"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SCHEMA_HASH_PATH = Path(__file__).with_name("expected_schema.sha256")

MAX_MOM_ABSOLUTE_CHANGE = 0.05  # 5 percent
MAX_HISTORY_REVISION_DEPTH = 2  # periods

# Values BLS uses to mark a cell as suppressed, preliminary-but-unpublished,
# or otherwise not available. Treated as missing (NaN) rather than as
# pipeline failures — a single suppressed cell in a 40-year backfill should
# not kill the refresh. If BLS ever starts returning these for the most
# recent month systematically, the continuity check downstream catches it.
SUPPRESSION_MARKERS = {"", "-", "(NA)", "N/A", "(X)", "NA"}


# --------------------------------------------------------------------------- #
# Schema hashing                                                              #
# --------------------------------------------------------------------------- #


def _key_signature(body: dict[str, Any]) -> str:
    """Build a deterministic, order-independent signature of the response shape.

    Hashes the sorted key set at three levels: the top-level body, the first
    series object, and the first data row. This is coarse by design -- we
    want renamed fields, added fields, and removed fields to trigger, but
    we do not want every new footnote to trigger.
    """
    root_keys = sorted(body.keys())

    results = body.get("Results") or {}
    series = (results.get("series") or [])
    series_keys: list[str] = []
    data_keys: list[str] = []
    if series:
        series_keys = sorted(series[0].keys())
        data = series[0].get("data") or []
        if data:
            data_keys = sorted(data[0].keys())

    signature = json.dumps(
        {
            "root": root_keys,
            "series": series_keys,
            "data": data_keys,
        },
        sort_keys=True,
    )
    return signature


def _hash_signature(signature: str) -> str:
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def compute_schema_hash(body: dict[str, Any]) -> str:
    return _hash_signature(_key_signature(body))


def load_expected_hash() -> str | None:
    if not SCHEMA_HASH_PATH.exists():
        return None
    raw = SCHEMA_HASH_PATH.read_text(encoding="utf-8").strip()
    # Tolerate an inline comment after the hash.
    match = re.match(r"[0-9a-f]{64}", raw)
    return match.group(0) if match else None


def write_expected_hash(new_hash: str) -> None:
    SCHEMA_HASH_PATH.write_text(
        f"{new_hash}\n"
        "# sha256 of sorted root/series/data key sets; regenerate deliberately.\n",
        encoding="utf-8",
    )


# --------------------------------------------------------------------------- #
# Value checks                                                                #
# --------------------------------------------------------------------------- #


def _iter_data_rows(body: dict[str, Any]) -> Iterable[dict[str, Any]]:
    results = body.get("Results") or {}
    series = (results.get("series") or [])
    if not series:
        return []
    return series[0].get("data") or []


def _period_to_month_index(year: str, period: str) -> int:
    """Convert ("2026", "M03") to a monotonic month index for ordering."""
    return int(year) * 12 + int(period[1:])


def check_mom_sanity(body: dict[str, Any]) -> list[str]:
    """Return a list of human-readable error messages; empty means OK.

    Suppressed / non-published cells (values in SUPPRESSION_MARKERS) are
    skipped with a stderr warning; MoM comparison resumes at the next
    numeric cell. Truly non-numeric garbage (an unexpected string that
    isn't a known suppression marker) still fails loud.
    """
    rows = sorted(
        _iter_data_rows(body),
        key=lambda r: _period_to_month_index(r["year"], r["period"]),
    )
    errors: list[str] = []
    prev_value: float | None = None
    prev_label: str | None = None
    for row in rows:
        # Skip annual / semiannual aggregate rows; we only care about monthly.
        period = row.get("period", "")
        if not period.startswith("M"):
            continue
        raw_value = row.get("value")
        raw_str = "" if raw_value is None else str(raw_value).strip()
        label = f"{row.get('year')}-{period}"
        if raw_str in SUPPRESSION_MARKERS:
            # Gap in the series. Reset the MoM anchor so a suppressed month
            # doesn't leak a spurious comparison across the gap.
            print(
                f"[validate] WARN skipping suppressed cell at {label}: {raw_value!r}",
                file=sys.stderr,
            )
            prev_value = None
            prev_label = None
            continue
        try:
            value = float(raw_str)
        except ValueError:
            errors.append(f"non-numeric value at {label}: {raw_value!r}")
            prev_value = None
            prev_label = None
            continue
        if prev_value is not None:
            change = (value - prev_value) / prev_value
            if abs(change) > MAX_MOM_ABSOLUTE_CHANGE:
                errors.append(
                    f"MoM change {change:+.3%} from {prev_label}={prev_value} "
                    f"to {label}={value} exceeds {MAX_MOM_ABSOLUTE_CHANGE:.0%}"
                )
        prev_value = value
        prev_label = label
    return errors


# --------------------------------------------------------------------------- #
# Continuity check against the last committed vintage                         #
# --------------------------------------------------------------------------- #


_PARQUET_VINTAGE_RE = re.compile(r"^cpi_(\d{4}-\d{2})\.parquet$")


def _latest_committed_vintage() -> Path | None:
    """Return the newest cpi_YYYY-MM.parquet (excluding the release-basis file)."""
    if not PROCESSED_DIR.exists():
        return None
    candidates = [
        p
        for p in PROCESSED_DIR.iterdir()
        if _PARQUET_VINTAGE_RE.match(p.name) is not None
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.name)
    return candidates[-1]


def check_continuity(body: dict[str, Any]) -> list[str]:
    """Ensure the incoming vintage doesn't silently rewrite history.

    Returns error messages. No previous vintage on disk means no check (first run).
    """
    prev_path = _latest_committed_vintage()
    if prev_path is None:
        return []

    try:
        import pandas as pd  # imported lazily so the schema check can run without pandas
    except ImportError:
        return ["pandas not installed; cannot run continuity check"]

    prev_df = pd.read_parquet(prev_path)
    prev_map = {
        (int(p.year), int(p.month)): float(v)
        for p, v in zip(prev_df["period"], prev_df["cpi_u"])
    }

    errors: list[str] = []
    # Skip suppressed cells when building the continuity map — they're handled
    # in check_mom_sanity. A suppressed value can't be compared against a prior
    # vintage's numeric value meaningfully.
    new_rows: list[tuple[int, int, float]] = []
    for r in _iter_data_rows(body):
        period = str(r.get("period", ""))
        if not period.startswith("M"):
            continue
        raw = r.get("value")
        raw_str = "" if raw is None else str(raw).strip()
        if raw_str in SUPPRESSION_MARKERS:
            continue
        try:
            new_rows.append((int(r["year"]), int(period[1:]), float(raw_str)))
        except ValueError:
            # Silently ignore here; check_mom_sanity already flagged it.
            continue
    new_map = {(y, m): v for y, m, v in new_rows}

    if not new_map:
        return ["no monthly rows in response"]

    max_prev = max(prev_map)
    max_new = max(new_map)
    prev_index = max_prev[0] * 12 + max_prev[1]
    new_index = max_new[0] * 12 + max_new[1]
    periods_added = new_index - prev_index
    if periods_added < 0:
        errors.append(
            f"new vintage ends at {max_new} which is before previous vintage end {max_prev}"
        )
    elif periods_added > 1:
        errors.append(
            f"new vintage adds {periods_added} months at the end; expected at most 1"
        )

    # Allow revisions only in the last MAX_HISTORY_REVISION_DEPTH months of the prior vintage.
    revision_cutoff_index = prev_index - MAX_HISTORY_REVISION_DEPTH
    for (y, m), v in new_map.items():
        idx = y * 12 + m
        if idx > prev_index:
            continue  # new month
        if idx <= revision_cutoff_index:
            prev_v = prev_map.get((y, m))
            if prev_v is None:
                errors.append(f"new vintage introduces historical month {y}-{m:02d} not in prior")
            elif abs(prev_v - v) > 1e-6:
                errors.append(
                    f"historical revision beyond allowed depth: {y}-{m:02d} "
                    f"was {prev_v}, now {v}"
                )
    return errors


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #


def _latest_raw() -> Path:
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"no raw directory at {RAW_DIR}")
    candidates = sorted(RAW_DIR.glob("CUUR0000SA0_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw BLS payloads in {RAW_DIR}")
    return candidates[-1]


def _load_raw() -> dict[str, Any]:
    path = _latest_raw()
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    # Backfill runs produce a list of bodies; collapse into a single view for hashing.
    # For the value / continuity checks we merge the data rows.
    if isinstance(payload, list):
        merged = dict(payload[0])
        merged_series = dict(merged["Results"]["series"][0])
        all_rows: list[dict[str, Any]] = []
        for body in payload:
            all_rows.extend(body["Results"]["series"][0].get("data") or [])
        # De-duplicate by (year, period), keeping the latest occurrence.
        dedup: dict[tuple[str, str], dict[str, Any]] = {}
        for r in all_rows:
            dedup[(r["year"], r["period"])] = r
        merged_series["data"] = list(dedup.values())
        merged["Results"] = {"series": [merged_series]}
        return merged
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate latest BLS raw payload.")
    parser.add_argument(
        "--rebaseline",
        action="store_true",
        help="Regenerate expected_schema.sha256 from the latest raw payload.",
    )
    args = parser.parse_args(argv)

    body = _load_raw()

    actual_hash = compute_schema_hash(body)

    if args.rebaseline:
        write_expected_hash(actual_hash)
        print(f"[validate] rebaselined schema hash -> {actual_hash}")
        return 0

    expected = load_expected_hash()
    errors: list[str] = []

    if expected is None:
        # First run: accept current shape and write it.
        write_expected_hash(actual_hash)
        print(
            f"[validate] no baseline found; recorded initial schema hash {actual_hash}"
        )
    elif expected != actual_hash:
        errors.append(
            f"schema hash mismatch: expected {expected}, got {actual_hash}. "
            "Inspect raw JSON and rebaseline deliberately if appropriate."
        )

    errors.extend(check_mom_sanity(body))
    errors.extend(check_continuity(body))

    if errors:
        print("[validate] FAIL", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("[validate] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
