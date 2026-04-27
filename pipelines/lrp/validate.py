"""Validate parsed LRP data before it is snapshotted.

Three independent checks run in sequence; any failure causes the pipeline
to exit non-zero so a future scheduled workflow can open a data-source-drift
issue and NOT commit a new snapshot.

    1. Schema-hash check: a hash derived from the COLUMNS list in
       ``pipelines/lrp/parse.py`` must equal the hash committed in
       ``expected_schema.sha256``. Catches silent RMA schema additions or
       reorderings.

    2. Value sanity: per-cwt prices within documented bounds, dates parse
       to real dates, the indemnity_amount column tolerates negatives,
       integer counts non-negative.

    3. Continuity: a freshly parsed file's row count is within an
       order-of-magnitude of the expected count for the matching reinsurance
       year (catches truncated downloads).

Reads parsed DataFrame inputs only; does not download anything itself.
Writes nothing.

Usage:
    python -m pipelines.lrp.validate                    # validate latest pull
    python -m pipelines.lrp.validate --rebaseline       # regenerate schema hash

The --rebaseline flag is a deliberate, reviewed action. Run it only after
inspecting the change in the COLUMNS list, deciding the new schema is
acceptable, and updating any downstream consumers. NEVER rebaseline to make
a failing pipeline green.

Status:
    Scaffold only. The plausibility bounds, hash function, and CLI entry
    point are sketched but raise NotImplementedError until 4.LRP-b.
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

# Plausibility bounds for $/cwt fields. LRP coverage prices and expected
# end values move with the cattle cycle but should sit comfortably inside
# this window for any historical reinsurance year. Any value outside is
# flagged for review (warnings first, hard failures only on egregious
# violations).
MIN_PLAUSIBLE_DOLLARS_PER_CWT = 20.0
MAX_PLAUSIBLE_DOLLARS_PER_CWT = 800.0

# Endorsement length must be one of the documented periods (weeks).
VALID_ENDORSEMENT_LENGTHS = {13, 17, 21, 26, 30, 34, 39, 43, 47, 52}

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_SCHEMA_PATH = Path(__file__).resolve().parent / "expected_schema.sha256"


def schema_hash() -> str:
    """Compute a stable hash of the canonical COLUMNS list.

    The hash is over (index, source_name, name, dtype) tuples in order;
    any reordering, rename, or dtype change shifts the hash. This is the
    value the validator compares against ``expected_schema.sha256``.

    Status: NotImplementedError until 4.LRP-b.
    """
    raise NotImplementedError(
        "schema_hash() is a 4.LRP-b deliverable. "
        "See pipelines/lrp/README.md for the build sequence."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate freshly-parsed LRP data before snapshotting.",
    )
    parser.add_argument(
        "--rebaseline",
        action="store_true",
        help=(
            "Regenerate expected_schema.sha256 from the current COLUMNS list. "
            "Deliberate action — only after reviewing schema change."
        ),
    )
    _ = parser.parse_args(argv)

    raise NotImplementedError(
        "pipelines.lrp.validate is a scaffold. Implementation lands in 4.LRP-b."
    )


if __name__ == "__main__":
    raise SystemExit(main())
