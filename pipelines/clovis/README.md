# `pipelines/clovis/` — USDA-AMS Clovis auction ingestion

This folder is the companion to `pipelines/bls/`: a three-file pipeline that pulls the USDA-AMS *Clovis Livestock Auction - Clovis, NM* report (slug **AMS_1781**, slug_id **1781**) and writes vintage-stamped Parquet snapshots the rest of the platform consumes.

Per `GOVERNANCE.md`'s Automation Over Features commitment, this pipeline covers the weekly feeder-cattle weighted-average report plus the slaughter-cattle subset (added 2026-05-12 for the Tier 2 drought-destocking tool). Other Clovis reports (replacement cattle, special sales), other auctions, and derived indices remain out of scope for now; they would be added as separate pipelines once the existing artifacts are running cleanly on schedule.

## Why USDA-AMS directly

This pipeline pulls the Clovis report from USDA-AMS Market News' own publication path — the MARS API for current data, the per-slug archive for older releases. Both are public-domain (17 U.S.C. § 105) and freely redistributable, which lets the platform commit derived snapshots to a public repository without the licensing complications that follow third-party compilations of the same underlying reports. Any third-party compilation has its own editorial work and licensing terms; the platform avoids those by going to the source.

MARS reliably covers **April 2019 → present** for structured weekly data. The October 2017 → April 2019 portion lives in USDA-AMS Market News' per-slug archive as fixed-width TXT reports; that block is reconstructed once via the companion `pipelines/clovis_historical/` package and read alongside the live MARS series via `pipelines/clovis/load.py`.

## Files

- **`ingest.py`** — single-function fetch of the MARS API, Clovis weighted-average report (slug `AMS_1781`). Handles the initial backfill (covers the full MARS window back to its earliest available date) and the routine weekly pull. Writes raw JSON to `data/raw/clovis/` for debugging; that folder is gitignored.
- **`validate.py`** — schema-hash check against `expected_schema.sha256`, plus sanity bounds (per-weight-class week-over-week swings, continuity with prior vintage). Mirrors the BLS validator structure and tolerates the same suppression markers.
- **`snapshot.py`** — writes vintage-stamped feeder and slaughter parquets, refreshes the `*_latest` aliases, and appends to the corresponding manifests. Specifically: `clovis_weekly_<YYYY-MM-DD>.parquet` / `clovis_latest.parquet` / `clovis_MANIFEST.json` for feeder cattle (the chart-shelf core), and `clovis_slaughter_<YYYY-MM-DD>.parquet` / `clovis_slaughter_latest.parquet` / `clovis_slaughter_MANIFEST.json` for slaughter cattle (cull-cow prices for the drought-destocking tool, added 2026-05-12).
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
- Validated, normalized feeder snapshots: `data/processed/clovis_weekly_<YYYY-MM-DD>.parquet` and `data/processed/clovis_latest.parquet` (committed).
- Validated, normalized slaughter snapshots: `data/processed/clovis_slaughter_<YYYY-MM-DD>.parquet` and `data/processed/clovis_slaughter_latest.parquet` (committed; added 2026-05-12 for the drought-destocking tool).
- The release-basis Clovis file (a Phase 1 release artifact pinned to a fixed vintage): `data/processed/clovis_release_basis_2025.parquet` (committed once at Phase 1 release, never overwritten by the pipeline; lets historical chart values stay reproducible after future refreshes).
- Manifests: `data/processed/clovis_MANIFEST.json` (feeder) and `data/processed/clovis_slaughter_MANIFEST.json` (slaughter) — both committed.
