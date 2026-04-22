# Phase 1 Plan — BLS CPI Pipeline + Price-Weight Explorer

**Phase**: 1 (Prototype)
**Effort estimate (per strategy doc Table 4)**: 2–3 effort weeks
**Companion**: `Article_drafts/Interactive_Platform_Strategy_2026.docx` Section 5 + Section 4.2
**Exit criteria** (verbatim from strategy doc):
1. Scheduled Action runs without manual intervention
2. One figure publicly visible at `https://lma-explorer.github.io`
3. Prototype URL can be shared with pilot reviewers

This document breaks Phase 1 into the concrete work items that need to land. It is intended to be read alongside `DECISIONS_LOG.md` and the strategy doc; nothing here contradicts either.

---

## 1. Component A — BLS CPI ingestion pipeline

### 1.1 Series choice

- **Series ID**: `CUUR0000SA0`
- **Title**: CPI-U, All Items, U.S. City Average, Not Seasonally Adjusted
- **Why this series**: it is the standard deflator used in agricultural economics (and in the forthcoming Extension article, which deflates to December 2025 dollars), it has the longest continuous history, and it is published mid-month for the prior month with rare revisions. NSA is preferred to SA for deflating because seasonal-adjustment patterns differ across goods baskets and the deflator should reflect the actual price level a producer faced in a given month.
- **Single-series-only** for Phase 1; multi-series additions (e.g., CPI for food at home, regional CPI) are explicit Phase 2+ candidates.

### 1.2 API access

- **Endpoint**: `https://api.bls.gov/publicAPI/v2/timeseries/data/`
- **Auth**: free v2 API key, requested via a one-time email registration form. Stored as a GitHub Actions secret named `BLS_API_KEY`. Never committed.
- **Rate limit**: 500 queries/day, 50 series/request, 20 years/query. The pipeline uses ~1 query/month and is nowhere near the limit. The 20-year cap matters for the initial backfill, which spans more than 20 years; the backfill code chunks by decade.

### 1.3 Pipeline structure

Three Python files under `pipelines/bls/`, matching the common pattern that other pipelines will reuse:

**`ingest.py`** — fetches the raw API response.
- Single function `fetch_cpi_u(start_year, end_year) -> dict` that wraps the BLS POST request, retries on transient HTTP errors, and returns the parsed JSON.
- Initial backfill calls in two chunks (e.g., 1985–2004, 2005–2026); routine monthly runs request only the most recent year.
- Writes the raw JSON to `data/raw/bls/CUUR0000SA0_<vintage>.json` for debugging. The `data/raw/` directory is gitignored.

**`validate.py`** — checks the response before any downstream code touches it.
- **Schema hash check**: hash the sorted set of top-level keys + the structure of `Results.series[0]`. Compare to the committed `pipelines/bls/expected_schema.sha256`. On mismatch, raise — do not silently coerce. This is the strategy doc Section 7 Risk 1 mitigation, applied to BLS rather than AMS but the same logic.
- **Value sanity**: reject the new vintage if any month-over-month CPI change exceeds 5% in absolute value (CPI has not moved that fast even in pandemic-era data; this catches gross unit/scale errors).
- **Continuity check**: the new vintage must extend the previous vintage by at most one new month and must not silently revise more than the most recent two months (BLS does occasionally revise prior months; this allows for that without allowing rewrite of distant history).

**`snapshot.py`** — writes the validated, normalized Parquet file.
- Output path: `data/processed/cpi_<YYYY-MM>.parquet` where `<YYYY-MM>` is the vintage month (the BLS publication month, not the data month).
- Schema (Parquet columns):
  - `period` (date, first-of-month)
  - `cpi_u` (float, series value)
  - `vintage` (date, the publication date this row was first reported under)
  - `revision_count` (int, 0 for first appearance, +1 each time BLS revises)
- Also writes `data/processed/cpi_latest.parquet` as a copy of the newest vintage (for convenience; not the source of truth).
- Updates `data/processed/MANIFEST.json` with vintage, sha256, and row count.

### 1.4 Base-month pinning (article reproducibility)

Per strategy doc Section 4.2 ("the repository pins a base-month CPI value as a release artifact so that every figure in the forthcoming Extension article remains reproducible even after CPI is revised"):

- A separate file `data/processed/cpi_article_basis_2025.parquet` is committed once and never overwritten by the pipeline. It contains the CPI vintage that was current at the time the forthcoming article is finalized (December 2025 basis).
- New interactive figures choose, in their own source code, between `cpi_article_basis_2025.parquet` (for reproducing article figures) and `cpi_latest.parquet` (for live deflation). The choice is explicit in each `.qmd` file's first code cell, never hidden in a framework default.
- A short note on the methodology page explains the choice and why both vintages exist.

### 1.5 Scheduled refresh workflow

`.github/workflows/bls_cpi_refresh.yml` — cron-scheduled GitHub Action.

- **Schedule**: `0 14 16 * *` (16th of each month at 14:00 UTC, which is 9:00 AM US Central — a few hours after the typical mid-month BLS release; gives the release time to land).
- **Job**: checkout, set up Python, run `python pipelines/bls/ingest.py && python pipelines/bls/validate.py && python pipelines/bls/snapshot.py`. On success, commit the new Parquet vintage and updated manifest using a bot account. On failure, open a `data-source-drift` issue auto-populated from the failure trace and **do not commit anything** — last-known-good stays live.
- **Manual trigger**: `workflow_dispatch` is enabled so we can re-run after a fix without waiting for the next cron tick.
- **Concurrency**: `concurrency: bls-cpi-refresh` so two runs can't race each other.

### 1.6 Phase 1 BLS pipeline checklist (against GOVERNANCE.md)

- [x] Scheduled-run definition: `bls_cpi_refresh.yml` defined above.
- [x] Failure detection: workflow opens a `data-source-drift` issue on red.
- [x] Last-known-good fallback: workflow does not commit on validation failure; published site keeps showing the prior vintage.
- [x] Vintage stamp: every figure that uses CPI carries a "CPI vintage: YYYY-MM" line in the figure caption.
- [x] Methodology page: `site/methodology/cpi.qmd` explains the deflation choice and base-month pinning.
- [x] License compliance: BLS data is U.S. public domain; CC-BY-4.0 notice on the derived Parquet is communicated via `LICENSE-DATA.md`.

---

## 2. Component B — Price-Weight Explorer (hero chart)

### 2.1 Data source for Phase 1

- **Source**: archived Clovis, NM feeder-cattle auction data already used in the forthcoming Extension article; full span January 2000 through December 2025 (26 years). **Not** live AMS — that pipeline lands in Phase 2. Phase 1 reads from a static, committed Parquet snapshot.
- **Open ask to user**: the actual Clovis snapshot file. We need to know its current location and format to convert it into the Parquet shape the page expects. Working assumption: it lives in the user's local files and will be shared this session or in the next. Until then, the page can render against a small synthetic stand-in so the layout work is unblocked.
- **Snapshot file (target name)**: `data/processed/clovis_archive_2000_2025.parquet` (previously `clovis_archive_2025.parquet`; renamed to reflect the 2000–2025 coverage span)
- **Expected schema** (subject to confirmation when the actual file is shared):
  - `report_date` (date)
  - `weight_class` (categorical: "200–300 lb", "300–400 lb", …)
  - `sex` (categorical: "Steer", "Heifer")
  - `head_count` (int)
  - `price_low_cwt` (float, $/cwt)
  - `price_mean_cwt` (float, $/cwt)
  - `price_high_cwt` (float, $/cwt)

### 2.2 Page layout

`site/price-weight.qmd` — single Quarto page.

- **Headline**: short, producer-facing ("What did feeder cattle bring at Clovis last year, by weight?").
- **Controls** (Observable JS inputs):
  - Weight class: multi-select; defaults to all classes.
  - Sex: Steer / Heifer / Both. Default Both.
  - Year range: slider, 2015–2025 (whatever the snapshot covers). Default last 3 years.
  - Inflation adjustment: toggle; off by default. When on, deflates to January 2026 dollars using the article-basis CPI.
- **Chart**: per weight class, show the min–mean–max band over the selected years. X axis: weight class. Y axis: $/cwt. Hover tooltip shows the underlying head count.
- **Below the chart**:
  - "Data through" vintage stamp.
  - Methodology link.
  - Download buttons: Parquet, CSV.
  - Caveat block: "Clovis is one auction in one region; not representative of national prices. See methodology for context."
- **Mobile**: chart re-flows to vertical orientation below 600 px viewport.

### 2.3 Interactive chart tech

- **Recommendation**: Plotly via Quarto's native Plotly support.
- **Why Plotly over Observable JS**: simpler from a Python code cell; the chart definition lives in the same `.qmd` file as the data load; mobile rendering is acceptable out of the box; Quarto handles the JS bundling.
- **Why not Observable JS for Phase 1**: Observable cells are a stronger choice for showcase pages later, but they pull in a heavier client-side runtime that's not justified by Phase 1's single chart. Phase 2 can re-evaluate per-page.
- **Why not D3 directly**: no — too much custom code for a prototype hero chart.

### 2.4 Phase 1 hero chart checklist (against GOVERNANCE.md)

- [x] Scheduled-run definition: site rebuilds on every push and weekly via `site_build.yml` (separate workflow, not specific to this chart).
- [x] Failure detection: site build failure surfaces as red CI on the default branch and via GitHub Actions failure notifications.
- [x] Last-known-good fallback: GitHub Pages keeps serving the previous successful build until a new one succeeds.
- [x] Vintage stamp: shown on the page.
- [x] Methodology page: `site/methodology/index.qmd` exists and is linked from this page.
- [x] License compliance: archived Clovis data is derived from public AMS reports; CC-BY-4.0 derived snapshot is fine.

---

## 3. Site infrastructure (the rest of Phase 1)

### 3.1 `site/_quarto.yml`

Project-level Quarto config. Highlights:

- `project.type: website`
- `project.output-dir: _site`
- `website.title: "Livestock Marketing Alternatives Explorer"`
- `website.repo-url: https://github.com/lma-explorer/lma-explorer.github.io`
- `website.repo-actions: [edit, source, issue]` — every page gets "Edit on GitHub" and "Report an issue" links, supporting the Extension-peer audience.
- `website.navbar`: Home / Price–weight / Methodology / Data / About.
- `format.html.theme: cosmo` (Bootstrap 5, mobile-friendly out of the box).
- `format.html.toc: true` for methodology pages, false for landing pages.

### 3.2 `site/index.qmd`

Phase 1 home page is intentionally minimal:

- One-paragraph "what this is."
- Three audience cards (Producers / Peers / Researchers), each with one entry-point link.
- "Featured figure" embed of the price–weight explorer.
- "Last refreshed" stamp pulled from the manifest.

### 3.3 `site_build.yml` workflow

`.github/workflows/site_build.yml`:

- Triggered on push to `main`, on the BLS pipeline workflow's success, and on a weekly cron tick.
- Sets up Quarto, renders the site, deploys to GitHub Pages via `actions/deploy-pages`.
- On render failure, opens an issue tagged `site-build-failure`.

### 3.4 GitHub Pages enablement

- Pages source: GitHub Actions (not branch).
- Custom domain: not in Phase 1 (see `DECISIONS_LOG.md` Decision 2).

---

## 4. Phase 1 work breakdown

The work items below map roughly to commits / PRs. Order matters for some (the BLS pipeline must precede figures that use it); others are parallelizable.

| #  | Work item                                                               | Blocks       | Notes                                                            |
|----|-------------------------------------------------------------------------|--------------|------------------------------------------------------------------|
| 1  | Initial commit: scaffold from `Repo_Scaffold/` folder                   | all          | Push README, LICENSE, GOVERNANCE, CODEOWNERS, etc.               |
| 2  | Set up Quarto site skeleton (`_quarto.yml`, `index.qmd` stub)           | 5, 6         | Verify Pages deploys an empty site                               |
| 3  | Write BLS API key → secret; add `expected_schema.sha256` placeholder    | 4            | One-time, manual                                                 |
| 4  | Implement `pipelines/bls/{ingest,validate,snapshot}.py`                 | 7            | Backfill on first run                                            |
| 5  | Add `bls_cpi_refresh.yml` workflow with monthly schedule                | 7            | Test via `workflow_dispatch` first                               |
| 6  | Add `site_build.yml` workflow                                           | 8            | Quarto render + Pages deploy                                     |
| 7  | Commit the article-basis CPI snapshot as a release artifact             | 9            | Generated once from the first BLS pipeline run                   |
| 8  | Place archived Clovis Parquet snapshot in `data/processed/`             | 9            | **Needs file from user**                                         |
| 9  | Build `site/price-weight.qmd` against the Clovis snapshot               | 10           | Plotly chart + Observable inputs                                 |
| 10 | Methodology pages: `site/methodology/{index,cpi}.qmd`                   | 11           | Required by checklist                                            |
| 11 | First publish to `https://lma-explorer.github.io`                       | reviewers    | Verify mobile rendering before sharing                           |
| 12 | Recruit pilot reviewers (1 producer + 1 Extension peer minimum)         | exit         | See `DECISIONS_LOG.md` Decision 5                                |

---

## 5. Open questions for the next working session

1. **GitHub org handle confirmation.** Working assumption is `@lma-explorer`. Required before item #1 above.
2. **Archived Clovis snapshot.** Required for items #8 and #9. Where does the file live, and what is its current format (CSV, Excel, R data file)?
3. **BLS API key registration.** A one-time email-form sign-up. Either you or the co-maintainer should hold it, and it becomes a repository secret.
4. **Bot identity for auto-commits.** The pipeline workflow needs a GitHub identity to commit refreshed Parquet files. Use the default `github-actions[bot]` (zero setup) or create a dedicated `lma-explorer-bot` account (more readable in commit history).
5. **Co-maintainer engagement timing.** When does the colleague move from "informally validated" to "formally signed on"? Ideally before item #11 ships and the first external link is created.
6. **Additional data the user mentioned.** User noted other data they've been working on; helpful to know what it is and how it might fit into Phase 1 vs. Phase 2.

---

## 6. Phase 1 done definition

Phase 1 is "done" when, in this order:

- The repo exists at `github.com/lma-explorer/lma-explorer.github.io` (or whatever org handle replaces the working assumption).
- The BLS CPI pipeline has run successfully at least once on schedule (not just on `workflow_dispatch`) and committed its first vintage.
- The price-weight explorer page is live at `https://lma-explorer.github.io/price-weight/` and renders correctly on mobile.
- A pilot URL has been shared with at least one producer and at least one Extension peer, and their feedback is captured in a tracking issue (does not need to be acted on yet).
- `DECISIONS_LOG.md` is updated to fill in the placeholders (org handle, co-maintainer, pilot reviewers).

Then we move to Phase 2.
