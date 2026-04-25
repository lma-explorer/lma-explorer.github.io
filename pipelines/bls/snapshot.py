"""Write the validated BLS CPI response to a vintage-stamped Parquet snapshot.

Reads the most recent raw payload in ``data/raw/bls/``, merges it with the
prior vintage to carry forward revision counts, and writes three artifacts:

    data/processed/cpi_<YYYY-MM>.parquet   # vintage snapshot (this month's file)
    data/processed/cpi_latest.parquet      # convenience copy of the newest vintage
    data/processed/MANIFEST.json           # append-only record of vintages

The revision_count column is the number of times BLS has republished a given
month. It is 0 on a month's first appearance and increments by 1 each time
the snapshot finds the value has changed from the previous vintage's record
for that month.

``data/processed/cpi_release_basis_2025.parquet`` is never touched by this
module; it is a one-time release artifact pinned to the CPI vintage current
at Phase 1 release (December 2025 basis), so historical chart values stay
reproducible after later BLS revisions.

Usage:
    python -m pipelines.bls.snapshot
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
RAW_DIR = REPO_ROOT / "data" / "raw" / "bls"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED_DIR / "MANIFEST.json"

RELEASE_BASIS_NAME = "cpi_release_basis_2025.parquet"  # never written by the pipeline


def _vintage_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _load_raw() -> dict[str, Any]:
    candidates = sorted(RAW_DIR.glob("CUUR0000SA0_*.json"))
    if not candidates:
        raise FileNotFoundError(f"no raw BLS payloads in {RAW_DIR}")
    path = candidates[-1]
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        # Backfill: merge chunks by (year, period).
        dedup: dict[tuple[str, str], dict[str, Any]] = {}
        for body in payload:
            rows = body["Results"]["series"][0].get("data") or []
            for r in rows:
                dedup[(r["year"], r["period"])] = r
        merged = dict(payload[0])
        merged_series = dict(merged["Results"]["series"][0])
        merged_series["data"] = list(dedup.values())
        merged["Results"] = {"series": [merged_series]}
        return merged
    return payload


def _prior_snapshot() -> pd.DataFrame | None:
    """Newest cpi_YYYY-MM.parquet (excluding the release-basis file), if any."""
    if not PROCESSED_DIR.exists():
        return None
    candidates = sorted(
        p
        for p in PROCESSED_DIR.iterdir()
        if p.name.startswith("cpi_")
        and p.name.endswith(".parquet")
        and p.name != RELEASE_BASIS_NAME
        and p.name != "cpi_latest.parquet"
        # "cpi_YYYY-MM.parquet" -> 10 char stem after 'cpi_'
    )
    if not candidates:
        return None
    return pd.read_parquet(candidates[-1])


def _payload_to_dataframe(payload: dict[str, Any], vintage_tag: str) -> pd.DataFrame:
    series = payload["Results"]["series"][0]
    rows = series.get("data") or []
    records = []
    vintage_first_of_month = date.fromisoformat(f"{vintage_tag}-01")
    for r in rows:
        period = r.get("period", "")
        if not period.startswith("M"):
            continue  # skip semiannual/annual aggregates
        year = int(r["year"])
        month = int(period[1:])
        try:
            value = float(r["value"])
        except (TypeError, ValueError):
            continue
        records.append(
            {
                "period": date(year, month, 1),
                "cpi_u": value,
                "vintage": vintage_first_of_month,
                "revision_count": 0,
            }
        )
    df = pd.DataFrame.from_records(records).sort_values("period").reset_index(drop=True)
    df["period"] = pd.to_datetime(df["period"])
    df["vintage"] = pd.to_datetime(df["vintage"])
    return df


def _apply_revision_counts(new_df: pd.DataFrame, prior_df: pd.DataFrame | None) -> pd.DataFrame:
    if prior_df is None or prior_df.empty:
        return new_df
    prior_map: dict[pd.Timestamp, tuple[float, int]] = {
        p: (float(v), int(c))
        for p, v, c in zip(
            prior_df["period"], prior_df["cpi_u"], prior_df["revision_count"]
        )
    }
    updated_counts = []
    for period, value in zip(new_df["period"], new_df["cpi_u"]):
        prior = prior_map.get(period)
        if prior is None:
            updated_counts.append(0)  # brand-new month
        else:
            prior_value, prior_count = prior
            if abs(prior_value - value) > 1e-6:
                updated_counts.append(prior_count + 1)
            else:
                updated_counts.append(prior_count)
    new_df = new_df.copy()
    new_df["revision_count"] = updated_counts
    return new_df


def _write_manifest_entry(vintage_tag: str, snapshot_path: Path, row_count: int) -> None:
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
        manifest = {"series": "CUUR0000SA0", "entries": []}
    # Replace an existing entry for the same vintage (so a manual rerun doesn't dupe).
    manifest["entries"] = [
        e for e in manifest.get("entries", []) if e.get("vintage") != vintage_tag
    ]
    manifest["entries"].append(entry)
    manifest["entries"].sort(key=lambda e: e["vintage"])
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a validated BLS CPI vintage snapshot to Parquet."
    )
    parser.parse_args(argv)  # no flags; argparse handles --help cleanly.

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    payload = _load_raw()
    vintage = _vintage_tag()

    df_new = _payload_to_dataframe(payload, vintage)
    if df_new.empty:
        print("[snapshot] no monthly rows extracted from payload", file=sys.stderr)
        return 1

    df_prior = _prior_snapshot()
    df_final = _apply_revision_counts(df_new, df_prior)

    vintage_path = PROCESSED_DIR / f"cpi_{vintage}.parquet"
    latest_path = PROCESSED_DIR / "cpi_latest.parquet"

    df_final.to_parquet(vintage_path, index=False)
    df_final.to_parquet(latest_path, index=False)
    _write_manifest_entry(vintage, vintage_path, len(df_final))

    print(f"[snapshot] wrote {vintage_path} ({len(df_final)} rows)")
    print(f"[snapshot] wrote {latest_path}")
    print(f"[snapshot] updated {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
