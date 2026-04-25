# Livestock Marketing Alternatives Explorer

An interactive, continuously-updated explorer of public-domain weekly feeder-cattle price data for the four-corners region (AZ, NM, UT, CO). This repository is the code, data pipelines, and published website (`https://lma-explorer.github.io/`) for the platform.

A Sall & Tronstad extension article analyzing the same dataset is in preparation. The platform develops its own methodology and is not bound to any particular published comparison; see [`site/methodology/`](site/methodology/) for design rationales.

**Status**: Phase 1 (Prototype). Price-weight explorer and weekly seasonality charts are live, reading from a combined 2017-10 → present Clovis weekly series.

## Who this is for

The same repository serves three audiences through different entry points.

**Producers and ranchers.** Use the rendered site (this URL, once deployed) for plain-language comparisons of marketing channels, weekly seasonality, and price–weight views of the Clovis, NM feeder-cattle auction. No login. Mobile-friendly.

**Extension peers, instructors, and graduate students.** Read the methodology pages for how each figure is computed. Fork this repository and adapt it for a different state or auction. Every chart renders from committed code; nothing is hand-tuned.

**Researchers and journalists.** Download the versioned Parquet and CSV snapshots in `/data/processed/`. Cite the annual Zenodo release DOI (first release: Phase 2 exit). All derived data carries a CC-BY-4.0 notice; the underlying sources (USDA-AMS, BLS, USDA NASS) are U.S. public domain.

## Data sources

Three public data families are ingested directly and redistributed as cleaned Parquet snapshots:

- **USDA-AMS LMR** — feeder- and fed-cattle auction reports via the MARS API (sale-day refresh for Clovis, weekly national summaries). Public domain.
- **BLS CPI-U** — series `CUUR0000SA0`, used as the deflator for all real-price figures. Public domain.
- **USDA NASS QuickStats** — cattle inventory and slaughter. Public domain.

One additional source is **link-out only** and is *not* mirrored in this repository:

- **CME feeder (GF) and live-cattle (LE) futures settlement** — proprietary; the platform links to cmegroup.com for live settlement rather than caching or redistributing. See `GOVERNANCE.md` for the data-licensing posture.

## Repository layout

```
/site/                Quarto source (.qmd) for the rendered website
/pipelines/           One Python module per public data source
  ams/                USDA-AMS ingestion (Phase 2)
  bls/                BLS CPI ingestion (Phase 1 — first pipeline)
  nass/               USDA NASS ingestion (Phase 2)
  cme/                Thin link-out helper (no redistribution)
/data/processed/      Committed Parquet snapshots, time-stamped by vintage
/data/raw/            Raw API responses cached for debugging (gitignored)
/R/, /py/             Reusable analysis helpers
/docs/                Methodology PDFs, Extension article PDF (on release), citation metadata
/.github/workflows/   Scheduled refresh, site build, annual Zenodo deposit
```

See `/docs/` for the underlying roadmap and strategy document.

## Licensing

- **Code** (Python, R, Quarto templates, workflows): MIT. See `LICENSE`.
- **Documentation and derived data** (README, methodology, Parquet/CSV snapshots, rendered figures): CC-BY-4.0. See `LICENSE-DATA.md`.

The underlying USDA-AMS, BLS, and USDA NASS data is U.S. public domain and legally carries no attribution requirement. The CC-BY-4.0 notice on derived data is a request for attribution to downstream users, not a constraint inherited from the sources.

## Maintenance philosophy

This project optimizes for **automation over features**. Every user-visible addition must ship with monitoring, failure detection, and a last-known-good fallback in the same pull request. See `GOVERNANCE.md` for the pre-merge checklist.

## Citation

See `CITATION.cff`. Once the first Zenodo deposit is live (planned for Phase 2 exit), a versioned DOI will be the preferred citation target.

## Contributing and contact

External contributions are welcome. See `GOVERNANCE.md` for the issue-and-PR process. Data-source drift (AMS slug changes, NASS suppression edge cases, BLS schema changes) should be filed using the "Data source drift" issue template.
