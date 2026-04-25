# `pipelines/bls/` — BLS CPI-U ingestion

This folder is a three-file pipeline that pulls the BLS CPI-U, All Items, U.S. City Average, Not Seasonally Adjusted series (`CUUR0000SA0`) and writes it out as a vintage-stamped Parquet snapshot the rest of the platform can consume.

The pipeline is intentionally small. Per `GOVERNANCE.md`'s Automation Over Features commitment, we don't add a new BLS series, a new deflator, or a new derived index here until the existing one is running cleanly on schedule, failing loudly when BLS changes something, and serving the single hero chart on the live site.

## Files

- **`ingest.py`** — single-function fetch of the BLS public API. Handles the initial backfill chunking (the API caps a single query at 20 years) and the routine monthly pull. Writes the raw response JSON to `data/raw/bls/` for debugging; that folder is gitignored.
- **`validate.py`** — runs three checks before any downstream code touches the response: a schema-hash check against `expected_schema.sha256`, a month-over-month sanity bound (>5% MoM rejects the vintage), and a continuity check (must extend the previous vintage by at most one month; must not silently rewrite more than the most recent two months of history).
- **`snapshot.py`** — writes the validated response out to `data/processed/cpi_<YYYY-MM>.parquet`, updates `cpi_latest.parquet`, and appends to `MANIFEST.json`.
- **`expected_schema.sha256`** — one-line file holding the sha256 hash of the expected response structure. Regenerated deliberately when BLS changes their schema and we've vetted the change; never regenerated silently.

## Running locally

```bash
pip install -r ../requirements.txt
export BLS_API_KEY=<your key>  # free registration at https://www.bls.gov/developers/
python -m pipelines.bls.ingest --backfill
python -m pipelines.bls.validate
python -m pipelines.bls.snapshot
```

For the scheduled, unattended monthly pull that runs in CI, see `.github/workflows/bls_cpi_refresh.yml`. That workflow does not pass `--backfill` — it requests the trailing year only.

## When BLS changes something

Every change — new field, renamed key, new periodicity, changed value encoding — should surface as a schema-hash mismatch in `validate.py`. The workflow will open a `data-source-drift` issue and stop committing. That is the correct behavior. When investigating:

1. Inspect the raw JSON at `data/raw/bls/CUUR0000SA0_<vintage>.json`.
2. Decide whether the change is cosmetic (add it to the expected schema) or substantive (patch downstream code before bumping the hash).
3. Regenerate the hash with `python -m pipelines.bls.validate --rebaseline` only after the review is complete.

## What lives here vs. where

- Raw API payloads: `data/raw/bls/` (gitignored).
- Validated, normalized snapshots: `data/processed/cpi_<YYYY-MM>.parquet` and `data/processed/cpi_latest.parquet` (committed).
- The frozen release-basis CPI (a Phase 1 release artifact pinned to the December 2025 vintage): `data/processed/cpi_release_basis_2025.parquet` (committed once at Phase 1 release, never overwritten by the pipeline; lets historical chart values stay reproducible after BLS revisions).
- Manifest: `data/processed/MANIFEST.json` (committed).
