"""Write the validated MARS AMS_1781 response to a vintage-stamped Parquet snapshot.

Reads the most recent raw payload in ``data/raw/clovis/`` and writes three
artifacts:

    data/processed/clovis_weekly_<YYYY-MM-DD>.parquet   # this pull's snapshot
    data/processed/clovis_latest.parquet                # convenience copy
    data/processed/clovis_MANIFEST.json                 # append-only record

``data/processed/clovis_article_basis_2025.parquet`` is never touched by this
module; it is a one-time release artifact pinned to the Clovis vintage current
when the forthcoming Extension article is finalized (December 2025 basis).

The long-format schema we target (finalized once the MARS response shape is
confirmed — see TODO in ``_payload_to_dataframe``):

    auction_date   : date      # the auction the observation came from
    class          : string    # 'steer' / 'heifer' (or 'mixed' if MARS reports it)
    weight_low     : int       # lower bound of weight-class bin, lbs
    weight_high    : int       # upper bound of weight-class bin, lbs
    price_low      : float     # $/cwt, range low
    price_high     : float     # $/cwt, range high
    price_weighted : float     # $/cwt, weighted average
    head_count     : int       # number of head in this observation
    vintage        : date      # when this snapshot was first written

Usage:
    python -m pipelines.clovis.snapshot
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw" / "clovis"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "clovis_MANIFEST.json"

ARTICLE_BASIS_NAME = "clovis_article_basis_2025.parquet"  # never written here
LATEST_NAME = "clovis_latest.parquet"


def _vintage_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_raw() -> dict[str, Any]:
    candidates = sorted(RAW_DIR.glob("AMS_1781_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw MARS payloads in {RAW_DIR}")
    with candidates[-1].open("r", encoding="utf-8") as f:
        return json.load(f)


def _payload_to_dataframe(payload: Any, vintage_tag: str) -> pd.DataFrame:
    """Convert a MARS AMS_1781 response to the long-format DataFrame above.

    TODO: fill in once the live MARS response shape is inspected. The current
    body is deliberately minimal — it returns an empty frame with the right
    columns so the rest of the pipeline (manifest, latest-copy, downstream
    validate) can exercise end-to-end before the parser is complete.
    """
    vintage_first = date.fromisoformat(vintage_tag)
    columns = [
        "auction_date",
        "class",
        "weight_low",
        "weight_high",
        "price_low",
        "price_high",
        "price_weighted",
        "head_count",
        "vintage",
    ]
    df = pd.DataFrame(columns=columns)
    df["vintage"] = pd.to_datetime([vintage_first] * len(df))
    return df


def _write_manifest_entry(
    vintage_tag: str, snapshot_path: Path, row_count: int
) -> None:
    entry = {
        "vintage": vintage_tag,
        "file": snapshot_path.relative_to(REPO_ROOT).as_posix(),
        "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
        "rows": row_count,
        "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"slug": "AMS_1781", "entries": []}
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage_tag
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a validated Clovis (AMS_1781) vintage snapshot.",
    )
    parser.parse_args(argv)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    payload = _load_raw()
    vintage = _vintage_tag()

    df = _payload_to_dataframe(payload, vintage)
    if df.empty:
        print(
            "[snapshot] WARNING: parser returned an empty frame — this is expected "
            "until _payload_to_dataframe is finalized against a real MARS response.",
            file=sys.stderr,
        )

    snapshot_path = PROCESSED_DIR / f"clovis_weekly_{vintage}.parquet"
    latest_path = PROCESSED_DIR / LATEST_NAME

    df.to_parquet(snapshot_path, index=False)
    df.to_parquet(latest_path, index=False)
    _write_manifest_entry(vintage, snapshot_path, len(df))

    print(f"[snapshot] wrote {snapshot_path} ({len(df)} rows)")
    print(f"[snapshot] wrote {latest_path}")
    print(f"[snapshot] updated {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
