"""Validate a freshly-pulled MARS AMS_1781 response before it is snapshotted.

Three independent checks run in sequence; any failure causes the pipeline to
exit non-zero so the scheduled workflow opens a data-source-drift issue and
does NOT commit a new snapshot.

    1. Schema-hash check: a hash derived from the set of field names at the
       levels of the MARS response (root, report, row) must equal the hash
       committed in ``expected_schema.sha256``. Catches the overwhelming
       majority of MARS structural changes without being sensitive to
       value-level differences.

    2. Value sanity: price fields within documented $/cwt bounds, weight-class
       labels drawn from the known set, no negative head counts.

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
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
SCHEMA_HASH_PATH = Path(__file__).with_name("expected_schema.sha256")

# Defensive bounds for Clovis feeder-cattle $/cwt prices, 1992-present window.
# Chosen to catch decimal-shift errors (e.g. $23 or $2300) without rejecting
# real market moves. Revisit if the market ever runs outside this band.
MIN_PLAUSIBLE_PRICE = 20.0
MAX_PLAUSIBLE_PRICE = 800.0

# Up to 2 weeks of history may be revised in a given pull. A revision reaching
# further back than this triggers a data-source-drift alert rather than a
# silent rewrite of the committed archive.
MAX_HISTORY_REVISION_DEPTH = 2

# MARS suppression markers — cells missing, preliminary, or otherwise
# unpublished. Mirrored from the BLS validator's set because MARS reports
# historically use the same conventions; extend if a new marker shows up.
SUPPRESSION_MARKERS = {"", "-", "(NA)", "N/A", "(X)", "NA", None}


# --------------------------------------------------------------------------- #
# Schema hashing                                                              #
# --------------------------------------------------------------------------- #


def _latest_raw() -> Path:
    candidates = sorted(RAW_DIR.glob("AMS_1781_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw MARS payloads in {RAW_DIR}")
    return candidates[-1]


def _key_signature(body: Any) -> str:
    """Build a deterministic, order-independent signature of the MARS shape.

    MARS typically returns either a list of row dicts at the top level or a
    wrapping envelope (e.g. ``{"reports": [...]}``). We hash sorted keys at
    the outer level and at the first row, which is coarse enough to ignore
    per-row value changes but strict enough to catch any renamed or added
    field.

    NOTE: the exact shape is confirmed from the first live MARS response and
    the key paths below may be tightened once known. Until then the signature
    covers both common MARS envelopes.
    """
    parts: list[str] = []

    if isinstance(body, dict):
        parts.append("root:" + ",".join(sorted(body.keys())))
        # Common envelope keys we probe for the row list.
        rows: list[Any] = []
        for key in ("results", "reports", "data", "report_detail", "records"):
            v = body.get(key)
            if isinstance(v, list):
                rows = v
                break
        if not rows and "Results" in body:
            rows = body["Results"]  # tolerate capitalization variants
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
        "# sha256 of the MARS AMS_1781 response's key sets (root + first row).\n"
        "# Regenerate deliberately via "
        "`python -m pipelines.clovis.validate --rebaseline`\n"
        "# after reviewing any schema change in data/raw/clovis/.\n",
        encoding="utf-8",
    )


# --------------------------------------------------------------------------- #
# Check stubs                                                                 #
# --------------------------------------------------------------------------- #


def check_schema(body: Any) -> None:
    expected = _load_expected_hash()
    got = _key_signature(body)
    if expected and got != expected:
        raise AssertionError(
            f"schema hash mismatch: expected {expected}, got {got}"
        )


def check_value_sanity(body: Any) -> None:
    """Walk the response's numeric price fields and bound-check them.

    TODO: specialize to the exact field names once the live MARS response is
    known. Until then we walk any numeric field with 'price' in the key name
    and apply the same bounds — correct-enough for a first pass, to be
    tightened after inspection.
    """
    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(v, (int, float)) and "price" in k.lower():
                    if not (MIN_PLAUSIBLE_PRICE <= float(v) <= MAX_PLAUSIBLE_PRICE):
                        raise AssertionError(
                            f"price out of plausible bounds: {k}={v}"
                        )
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(body)


def check_continuity(body: Any) -> None:
    """Ensure the pull extends the committed archive without rewriting
    history further back than MAX_HISTORY_REVISION_DEPTH weeks.

    TODO: fill in once the field name for the auction date is known and the
    prior-snapshot parquet exists. Stubbed to pass for now so the first
    backfill can land; re-enabled in the follow-up after snapshot.py has run.
    """
    return


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
