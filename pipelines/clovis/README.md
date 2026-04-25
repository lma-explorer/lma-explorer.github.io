# `pipelines/clovis/` — USDA-AMS Clovis auction ingestion

This folder is the companion to `pipelines/bls/`: a three-file pipeline that pulls the USDA-AMS *Clovis Livestock Auction - Clovis, NM* report (slug **AMS_1781**, slug_id **1781**) and writes a vintage-stamped Parquet snapshot the rest of the platform consumes.

Per `GOVERNANCE.md`'s Automation Over Features commitment, this pipeline covers the weekly feeder-cattle weighted-average report only. Adding other Clovis reports (slaughter/replacement, special sales), other auctions, or derived indices waits until this one is running cleanly on schedule and serving the price–weight chart on the live site.

## Why USDA-AMS directly

This pipeline pulls the Clovis report from USDA-AMS Market News' own publication path — the MARS API for current data, the per-slug archive for older releases. Both are public-domain (17 U.S.C. § 105) and freely redistributable, which lets the platform commit derived snapshots to a public repository without the licensing complications that follow third-party compilations of the same underlying reports. Any third-party compilation has its own editorial work and licensing terms; the platform avoids those by going to the source.

MARS reliably covers **April 2019 → present** for structured weekly data. The October 2017 → April 2019 portion lives in USDA-AMS Market News' per-slug archive as fixed-width TXT reports; that block is reconstructed once via the companion `pipelines/clovis_historical/` package and read alongside the live MARS series via `pipelines/clovis/load.py`.

## Files

- **`ingest.py`** — single-function fetch of the MARS API, Clovis weighted-average report (slug `AMS_1781`). Handles the initial backfill (covers the full MARS window back to its earliest available date) and the routine weekly pull. Writes raw JSON to `data/raw/clovis/` for debugging; that folder is gitignored.
- **`validate.py`** — schema-hash check against `expected_schema.sha256`, plus sanity bounds (per-weight-class week-over-week swings, continuity with prior vintage). Mirrors the BLS validator structure and tolerates the same suppression markers.
- **`snapshot.py`** — writes `data/processed/clovis_weekly_<YYYY-MM-DD>.parquet`, refreshes `clovis_latest.parquet`, appends to `data/processed/clovis_MANIFEST.json`.
- **`expected_schema.sha256`** — one-line baseline hash of MARS's response structure. Regenerated deliberately via `python -m pipelines.clovis.validate --rebaseline` after reviewing any schema change; never silently.

## Running locally

```bash
pip install -r ../requirements.txt
export AMS_MARS_API_KEY=<your key>  # free registration at https://mymarketnews.ams.usda.gov
python -m pipelines.clovis.ingest --backfill
python -m pipelines.clovis.validate
python -m pipelines.clovis.snapshot
```

For the scheduled unattended weekly pull, see `.github/workflows/clovis_refresh.yml`.

## When MARS changes something

Every change — renamed field, new section, altered date format, changed unit — should surface as a schema-hash mismatch in `validate.py`. The workflow opens a `data-source-drift` issue and stops committing. When investigating:

1. Inspect the raw JSON at `data/raw/clovis/AMS_1781_<vintage>.json`.
2. Decide whether the change is cosmetic (update the expected schema) or substantive (patch downstream code before bumping the hash).
3. Regenerate the hash with `python -m pipelines.clovis.validate --rebaseline` only after review.

## Historical reconstruction (separate workstream)

The MARS API covers April 2019 → present. October 2017 → April 2019 is reconstructed once from USDA-AMS Market News' per-slug archive (a sibling pipeline at `pipelines/clovis_historical/`); both eras are then unioned by the chart code via `pipelines/clovis/load.py`. Pre-October 2017 history is not present in any public-domain weekly archive currently reachable; if a deeper machine-readable archive becomes available from USDA-AMS, it lands here as a one-time block alongside the existing eras.

## What lives here vs. where

- Raw API payloads: `data/raw/clovis/` (gitignored).
- Validated, normalized snapshots: `data/processed/clovis_weekly_<YYYY-MM-DD>.parquet` and `data/processed/clovis_latest.parquet` (committed).
- The release-basis Clovis file (a Phase 1 release artifact pinned to a fixed vintage): `data/processed/clovis_release_basis_2025.parquet` (committed once at Phase 1 release, never overwritten by the pipeline; lets historical chart values stay reproducible after future refreshes).
- Manifest: `data/processed/clovis_MANIFEST.json` (committed).
