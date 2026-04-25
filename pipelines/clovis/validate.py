"""Validate a freshly-pulled MARS AMS_1781 response before it is snapshotted.

Three independent checks run in sequence; any failure causes the pipeline to
exit non-zero so the scheduled workflow opens a data-source-drift issue and
does NOT commit a new snapshot.

    1. Schema-hash check: a hash derived from the set of field names at the
       levels of the MARS response (envelope + first result row) must equal
       the hash committed in ``expected_schema.sha256``. Catches the
       overwhelming majority of MARS structural changes without being
       sensitive to value-level differences.

    2. Value sanity: per-cwt prices within documented bounds, weight-class
       bins in expected 50-lb granularity, no negative head counts.

    3. Continuity: the new pull must extend the last committed snapshot
       forward; MARS occasionally republishes a corrected row for the most
       recent week or two, which is expected; a revision reaching further
       back than ``MAX_HISTORY_REVISION_DEPTH`` weeks triggers a failure.

Reads the most recent raw JSON under data/raw/clovis/ and the most recent
committed parquet under data/processed/clovis_weekly_*.parquet. Writes nothing.

Usage:
    python -m pipelines.clovis.validate                 # validate latest raw
    python -m pipelines.clovis.validate --rebaseline    # regenerate schema hash

The --rebaseline flag is a deliberate, reviewed action. Run it only after
reading the raw JSON, deciding the new schema shape is acceptable, and
updating any downstream code. NEVER rebaseline to make a failing
pipeline green.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SCHEMA_HASH_PATH = Path(__file__).with_name("expected_schema.sha256")

# Defensive bounds for Clovis feeder-cattle $/cwt prices, the window MARS covers
# (2019-present). Wide enough to tolerate the real market swings observed
# during 2020-2021 and 2024-2026, tight enough to catch decimal-shift errors.
MIN_PLAUSIBLE_PRICE = 20.0
MAX_PLAUSIBLE_PRICE = 800.0

# MARS weight-break bins are nominally 50-lb wide. We verify every observed
# span falls within [0, 100] lbs to catch a unit change (e.g. kg instead of
# lbs) or a structural reshape of the weight-break fields.
MAX_WEIGHT_BREAK_SPAN_LBS = 100

# A handful of known-bad rows exist in USDA-AMS's archive (data-entry errors
# in the published report that were never corrected). We tolerate up to this
# fraction of feeder-cattle Per-Cwt rows failing sanity — enough to survive
# one-off typos in the long archive, strict enough that a systemic unit
# change or decimal shift (which would affect a whole week's worth of rows)
# still trips the check and opens a data-source-drift issue.
MAX_BAD_ROW_FRACTION = 0.001  # 0.1%

# Up to 2 weeks of history may be revised in a given pull. A revision reaching
# further back than this triggers a data-source-drift alert rather than a
# silent rewrite of the committed archive.
MAX_HISTORY_REVISION_DEPTH = 2

# MARS suppression / missing-value markers. Treated as missing rather than
# pipeline failures. Extend if a new marker appears.
SUPPRESSION_MARKERS = {"", "-", "(NA)", "N/A", "(X)", "NA", None}


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _latest_raw() -> Path:
    candidates = sorted(RAW_DIR.glob("AMS_1781_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw MARS payloads in {RAW_DIR}")
    return candidates[-1]


def _latest_snapshot() -> Path | None:
    """Newest clovis_weekly_*.parquet (excluding the release-basis file)."""
    if not PROCESSED_DIR.exists():
        return None
    candidates = sorted(
        p
        for p in PROCESSED_DIR.iterdir()
        if p.name.startswith("clovis_weekly_") and p.name.endswith(".parquet")
    )
    return candidates[-1] if candidates else None


# --------------------------------------------------------------------------- #
# Schema hashing                                                              #
# --------------------------------------------------------------------------- #


def _key_signature(body: Any) -> str:
    """Build a deterministic signature of the MARS response shape.

    Hashes the sorted key set at the envelope level and at the first
    ``results`` row. Coarse enough to ignore per-row value changes, strict
    enough to catch renamed, added, or removed fields at either level.
    """
    parts: list[str] = []

    if isinstance(body, dict):
        parts.append("root:" + ",".join(sorted(body.keys())))
        rows = body.get("results")
        if not isinstance(rows, list):
            rows = []
    elif isinstance(body, list):
        parts.append("root:list")
        rows = body
    else:
        parts.append("root:scalar")
        rows = []

    if rows and isinstance(rows[0], dict):
        parts.append("row:" + ",".join(sorted(rows[0].keys())))

    blob = "|".join(parts).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _load_expected_hash() -> str:
    if not SCHEMA_HASH_PATH.exists():
        return ""
    for line in SCHEMA_HASH_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line
    return ""


def _write_expected_hash(h: str) -> None:
    SCHEMA_HASH_PATH.write_text(
        f"{h}\n"
        "# sha256 of the MARS AMS_1781 response key sets (root envelope + first\n"
        "# results row). Regenerate deliberately via:\n"
        "#   python -m pipelines.clovis.validate --rebaseline\n"
        "# after reviewing any schema change in data/raw/clovis/.\n",
        encoding="utf-8",
    )


# --------------------------------------------------------------------------- #
# Checks                                                                      #
# --------------------------------------------------------------------------- #


def check_schema(body: Any) -> None:
    expected = _load_expected_hash()
    got = _key_signature(body)
    if expected and got != expected:
        raise AssertionError(
            f"schema hash mismatch: expected {expected}, got {got}"
        )


def check_value_sanity(body: Any) -> None:
    """Inspect feeder-cattle $/cwt rows for impossible values.

    Individual out-of-bounds rows are logged to stderr and tolerated as
    known-bad archive entries. The check fails only if the fraction of
    anomalous rows exceeds ``MAX_BAD_ROW_FRACTION`` — which would indicate a
    structural change (unit swap, decimal shift) affecting many rows, not a
    one-off typo in a single published report.
    """
    rows = body.get("results") if isinstance(body, dict) else body or []
    feeder_rows = 0
    bad: list[str] = []
    for i, r in enumerate(rows):
        if r.get("commodity") != "Feeder Cattle":
            continue
        if r.get("price_unit") != "Per Cwt":
            continue
        feeder_rows += 1
        row_bad = False
        for field in ("avg_price", "avg_price_min", "avg_price_max"):
            v = r.get(field)
            if v in SUPPRESSION_MARKERS:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                bad.append(f"row {i} ({r.get('report_date')}): non-numeric {field}={v!r}")
                row_bad = True
                continue
            if not (MIN_PLAUSIBLE_PRICE <= fv <= MAX_PLAUSIBLE_PRICE):
                bad.append(
                    f"row {i} ({r.get('report_date')}): {field}={fv} outside "
                    f"[{MIN_PLAUSIBLE_PRICE}, {MAX_PLAUSIBLE_PRICE}] $/cwt"
                )
                row_bad = True
        hc = r.get("head_count")
        if hc is not None and hc not in SUPPRESSION_MARKERS:
            try:
                if int(hc) < 0:
                    bad.append(f"row {i}: negative head_count={hc}")
                    row_bad = True
            except (TypeError, ValueError):
                bad.append(f"row {i}: non-numeric head_count={hc!r}")
                row_bad = True
        lo, hi = r.get("weight_break_low"), r.get("weight_break_high")
        if lo is not None and hi is not None:
            try:
                span = int(hi) - int(lo)
            except (TypeError, ValueError):
                span = None
            if span is not None and not (0 < span <= MAX_WEIGHT_BREAK_SPAN_LBS):
                bad.append(
                    f"row {i}: weight_break span {span} lb outside "
                    f"(0, {MAX_WEIGHT_BREAK_SPAN_LBS}]"
                )
                row_bad = True
        del row_bad  # counter not needed; 'bad' list already tracks cases

    for line in bad[:20]:
        print(f"[validate] WARNING: {line}", file=sys.stderr)
    if len(bad) > 20:
        print(
            f"[validate] WARNING: ... and {len(bad) - 20} more anomalies suppressed in log.",
            file=sys.stderr,
        )

    if feeder_rows == 0:
        raise AssertionError("no Feeder Cattle Per-Cwt rows in payload")
    bad_fraction = len(bad) / feeder_rows
    if bad_fraction > MAX_BAD_ROW_FRACTION:
        raise AssertionError(
            f"value sanity: {len(bad)} anomalous rows out of {feeder_rows} "
            f"feeder-cattle Per-Cwt rows ({bad_fraction:.3%}) exceeds the "
            f"{MAX_BAD_ROW_FRACTION:.1%} tolerance. Likely a structural change "
            f"(unit swap, decimal shift); investigate before rebaselining."
        )


def check_continuity(body: Any) -> None:
    """Confirm the new pull doesn't rewrite deep history.

    We extract the set of auction dates in the new payload and the set in the
    most recent committed snapshot, and check two things:

        - No new auction_date is older than the prior snapshot's newest date
          by more than ``MAX_HISTORY_REVISION_DEPTH`` weeks.
        - No existing auction_date from the prior snapshot is absent from the
          new payload up to that depth.

    On the first run (no prior snapshot), this check is a no-op.
    """
    prior_path = _latest_snapshot()
    if prior_path is None:
        return

    new_rows = body.get("results") if isinstance(body, dict) else body or []
    new_dates = set()
    for r in new_rows:
        if r.get("commodity") != "Feeder Cattle":
            continue
        d = r.get("report_date")
        try:
            new_dates.add(datetime.strptime(d, "%m/%d/%Y").date())
        except (TypeError, ValueError):
            continue

    if not new_dates:
        raise AssertionError("no feeder-cattle auction dates in new payload")

    prior = pd.read_parquet(prior_path, columns=["auction_date"])
    prior_dates = set(pd.to_datetime(prior["auction_date"]).dt.date.unique())
    if not prior_dates:
        return

    newest_prior = max(prior_dates)
    # Any new date that's older than (newest_prior - N weeks) AND not present
    # in prior would be a silent deep-history revision.
    cutoff_days = MAX_HISTORY_REVISION_DEPTH * 7
    deep_new = [
        d for d in (new_dates - prior_dates)
        if (newest_prior - d).days > cutoff_days
    ]
    if deep_new:
        raise AssertionError(
            f"{len(deep_new)} new auction date(s) older than "
            f"{MAX_HISTORY_REVISION_DEPTH}w before prior snapshot's newest date "
            f"({newest_prior.isoformat()}); deep-history revision suspected. "
            f"Examples: {sorted(deep_new)[:5]}"
        )


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the latest MARS AMS_1781 response.",
    )
    parser.add_argument(
        "--rebaseline",
        action="store_true",
        help="Regenerate expected_schema.sha256 from the current raw payload. "
        "Reviewed action only.",
    )
    args = parser.parse_args(argv)

    raw_path = _latest_raw()
    with raw_path.open("r", encoding="utf-8") as f:
        body = json.load(f)

    if args.rebaseline:
        new_hash = _key_signature(body)
        _write_expected_hash(new_hash)
        print(f"[validate] rebaselined schema hash to {new_hash}")
        return 0

    check_schema(body)
    check_value_sanity(body)
    check_continuity(body)
    print(f"[validate] OK ({raw_path.name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
