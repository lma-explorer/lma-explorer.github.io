# `pipelines/lrp/` — USDA RMA Livestock Risk Protection ingestion

Status: **scaffold (M2 kickoff, 2026-04-27)**. No real implementation yet — every module raises `NotImplementedError`. The structure, constants, and 31-column schema mapping are final and ready for `4.LRP-b` to fill in.

## What this pipeline does

Ingests the public **Summary of Business** files for USDA RMA's Livestock Risk Protection (LRP) program — every endorsement sold under LRP, by reinsurance year, from 2003 through the current year. The data feeds the `4.LRP-c` backtest (LRP outcomes vs. local Clovis cash realizations) and the `4.LRP-d` chart page that ships the backtest visually.

## Source

RMA pubfs HTTPS host:

```
https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/
  Summary_of_Business/livestock_and_dairy_participation/lrp_<YYYY>.zip
```

Each annual zip contains a single pipe-delimited TXT (`lrp_<YYYY>.txt`) with no header row. The 31-column schema is documented in `LRP_Summary_of_Business_All_Years.docx` on the same RMA directory; it is reproduced as the canonical Python `COLUMNS` list in [`parse.py`](parse.py).

## Refresh cadence

**Annual.** RMA updates the per-year zip periodically (the directory listing showed RMA bumping the 2026 file as recently as 2026-04-27). For now this pipeline runs on demand via `workflow_dispatch`; once 4.LRP-d is live we may add a monthly cron to pick up RMA's mid-year updates to the current reinsurance-year zip.

## History scope

**2003 → present (full).** 24 annual zips total. Earlier history is not available — 2003 is RMA's documented floor for the LRP series.

## Build sequence

| Sub-task | Deliverable | Dependency |
|---|---|---|
| **4.LRP-a** | Data-sourcing probe (matrix v0.3 entry) | done (M1.3) |
| **4.LRP-b** | This pipeline's full implementation: `ingest.py` downloads zips, `parse.py` parses TXT, `validate.py` runs schema/value/continuity checks, `snapshot.py` writes parquet+MANIFEST | `4.LRP-a` |
| **4.LRP-c** | The actual backtest: simulate "buy LRP at week W with coverage price C, hold to end-date E, realize indemnity vs. premium" against local Clovis cash. Produces a per-(year, length, type, coverage-level) table. | `4.LRP-b` |
| **4.LRP-d** | Chart page `site/lrp.qmd` + methodology page `site/methodology/lrp.qmd`. Includes time-series of premium/indemnity/net outcome. **Candidate for state-level Plotly choropleth** showing LRP concentration by `state_abbr` (queued, sized 3-4 hrs, contingent on the data story being legible after 4.LRP-c). | `4.LRP-c` |

Total estimated effort for M2: **~25-35 hrs** (matrix v0.3, revised after the M1.3 probe).

## Storage layout

```
data/raw/lrp/
  lrp_2003_<vintage>.zip          # downloaded zip, vintage-stamped
  lrp_2004_<vintage>.zip
  ...
  lrp_2026_<vintage>.zip

data/processed/
  lrp_premiums_<vintage>.parquet  # 23-year tidy parquet, vintage-stamped
  lrp_latest.parquet              # convenience copy (= most recent vintage)
  lrp_MANIFEST.json               # append-only audit trail
```

The vintage tag is the UTC date the snapshot was first written, not the reinsurance year. A single 23-year parquet per vintage matches the Clovis pipeline's pattern (one parquet per vintage, even though Clovis is weekly cadence and LRP is annual).

## Schema

31 columns, no header. Full mapping (source-name → Python-name → dtype) lives in [`parse.py:COLUMNS`](parse.py). The seven backtest-driving columns are:

- `effective_date` (col 15) — endorsement sale date
- `coverage_price` (col 17) — $/cwt
- `expected_end_value` (col 18) — $/cwt
- `cost_per_cwt` (col 21) — premium $/cwt
- `end_date` (col 22) — coverage expiry
- `liability_amount` (col 30) — $ insured
- `indemnity_amount` (col 31) — $ paid out (may be negative)

The four geographic-scope columns that drive the choropleth candidate at 4.LRP-d are: `state_fips`, `state_abbr`, `county_fips`, `county_name`.

## Filters applied at snapshot time

The processed parquet is narrowed to feeder-cattle LRP endorsements only:

- `commodity_code == "0801"` (FEEDER CATTLE)
- `plan_code == "81"` (LIVESTOCK RISK PROTECTION)
- `type_code in {"810", "820", "830"}` (Steers / Heifers / combined)

Other commodities (fed cattle, swine, lamb), other plans, and other product types are preserved in the raw zips for the record but not carried into the processed parquet — the platform's backtest is feeder-cattle steers/heifers under LRP only.

## Public-attribution posture

This pipeline reads only public-domain RMA data published under [17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105). RMA documentation links are linked-out, not redistributed. See repo-root `LICENSE-DATA.md` for the canonical posture.

## Why bulk zips, not RIRS

RMA also publishes a web app called RIRS that exposes a query interface to the same underlying data, but only from July 2021 forward. The bulk zips reach back to 2003, are documented schema-stable, and avoid the rate-limit and session-token complexity of the web-app interface. The probe (M1.3, matrix v0.3) confirmed the bulk zips as the primary ingestion path; RIRS remains useful for ad-hoc lookups but is not part of the pipeline.

## Future work flagged here

- **`4.LRP-d` choropleth candidate**: state-level LRP concentration map driven by `producer_premium_amount` summed by `state_abbr`. Sized 3-4 hrs. Decision deferred until the backtest data story is legible.
- **`v2.lrp-county-choropleth`**: county-level granularity using topojson, contingent on state-level being well-received. Adds non-trivial CI weight (topojson or geopandas), so explicitly v2.
- **Monthly refresh cron**: not added at scaffold time. Decision deferred until 4.LRP-d is live and we can see how often RMA bumps the current-year zip in practice.
