# Repository Tree (intended structure)

This file documents the full directory structure the repo should have once the initial commit is pushed. The scaffold folder on disk contains only the files that have content today (README, LICENSE, GOVERNANCE, etc.); the empty subdirectories below are created by `git init` / first commit rather than duplicated onto your desktop.

```
lma-explorer.github.io/
├── README.md                             ← top-level orientation (three audiences)
├── LICENSE                               ← MIT (code)
├── LICENSE-DATA.md                       ← CC-BY-4.0 (docs + derived data)
├── GOVERNANCE.md                         ← automation-over-features commitment
├── CODEOWNERS                            ← review routing (placeholders for now)
├── CITATION.cff                          ← machine-readable citation
├── DECISIONS_LOG.md                      ← Phase 0 decisions + rationale
├── PHASE1_PLAN.md                        ← Phase 1 design (BLS CPI + price-weight)
├── .gitignore
│
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.yml
│   │   ├── data_source_drift.yml
│   │   └── feature_request.yml
│   └── workflows/                        ← created in Phase 1
│       ├── bls_cpi_refresh.yml           ← monthly BLS CPI pipeline (Phase 1)
│       ├── site_build.yml                ← Quarto render + Pages deploy (Phase 1)
│       └── zenodo_deposit.yml            ← annual DOI deposit (Phase 2)
│
├── site/                                 ← Quarto source
│   ├── _quarto.yml                       ← site config (Phase 1)
│   ├── index.qmd                         ← home page (Phase 1 stub)
│   ├── price-weight.qmd                  ← hero chart (Phase 1)
│   ├── seasonality.qmd                   ← weekly seasonality (Phase 2)
│   ├── channels.qmd                      ← marketing channels table (Phase 2)
│   ├── calculator.qmd                    ← slide + shrink (Phase 3)
│   └── methodology/
│       ├── index.qmd
│       ├── cpi.qmd                       ← CPI deflation (Phase 1)
│       ├── ams.qmd                       ← AMS ingestion (Phase 2)
│       └── nass.qmd                      ← NASS ingestion (Phase 2)
│
├── pipelines/
│   ├── bls/                              ← Phase 1
│   │   ├── ingest.py                     ← fetch CUUR0000SA0 from BLS v2 API
│   │   ├── validate.py                   ← schema + value sanity checks
│   │   └── snapshot.py                   ← write vintage-stamped Parquet
│   ├── ams/                              ← Phase 2
│   │   ├── ingest.py
│   │   ├── validate.py
│   │   └── snapshot.py
│   ├── nass/                             ← Phase 2
│   │   ├── ingest.py
│   │   ├── validate.py
│   │   └── snapshot.py
│   ├── cme/                              ← link-out helper, no ingestion
│   │   └── links.py                      ← resolve CME public-page URLs
│   └── common/
│       ├── schema_hash.py                ← strategy-doc Risk 1 mitigation
│       └── manifest.py                   ← pipeline manifest writer
│
├── data/
│   ├── processed/
│   │   ├── cpi_YYYY-MM.parquet           ← BLS CPI vintages (Phase 1)
│   │   ├── ams_clovis_YYYY-WW.parquet    ← AMS feeder reports (Phase 2)
│   │   └── nass_cattle_inventory_YYYY-MM.parquet  ← NASS (Phase 2)
│   └── raw/                              ← gitignored, CI-only
│
├── R/                                    ← reusable R helpers
├── py/                                   ← reusable Python helpers
│
└── docs/
    ├── Interactive_Platform_Strategy_2026.pdf    ← archived strategy
    ├── Article_MarketAlternatives_2025.pdf       ← archived Extension article (committed on release)
    ├── annual-review-YYYY.md                     ← created each January
    └── changelog.md
```

## What's in the scaffold folder today

The `Repo_Scaffold/` folder on your desktop contains just the text files — no empty subdirectories. When you initialize the GitHub repo, the missing folders will be created by the first commits that add content to them, so you don't need to clutter your local filesystem with empty placeholder directories.

Files ready to commit in the first push:

- `README.md`
- `LICENSE`
- `LICENSE-DATA.md`
- `GOVERNANCE.md`
- `CODEOWNERS`
- `CITATION.cff`
- `DECISIONS_LOG.md`
- `.gitignore`
- `.github/ISSUE_TEMPLATE/bug_report.yml`
- `.github/ISSUE_TEMPLATE/data_source_drift.yml`
- `.github/ISSUE_TEMPLATE/feature_request.yml`

`PHASE1_PLAN.md` is the next file (coming in this session).
