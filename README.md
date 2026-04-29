# Livestock Marketing Alternatives Explorer

An interactive, continuously-updated explorer of public-domain weekly feeder-cattle price data for the four-corners region (AZ, NM, UT, CO). This repository is the code, data pipelines, and published website (`https://lma-explorer.github.io/`) for the platform.

A Sall & Tronstad extension article analyzing the same dataset is in preparation. The platform develops its own methodology and is not bound to any particular published comparison; see [`site/methodology/`](site/methodology/) for design rationales.

**Status**: active prototype. Live chart pages: price–weight, weekly seasonality, weekly trends, basis, sell-now compare (Clovis, NM feeder-cattle auction), and **LRP** (USDA RMA Summary of Business, all states, AZ default). Live data feeds: USDA-AMS Clovis (sale-week), BLS CPI-U (monthly), USDA RMA (annual). Additional regional auctions (Willcox AZ, Utah feeders) are roadmap items.

## Who this is for

The same repository serves three audiences through different entry points.

**Producers and ranchers.** Use the rendered site (this URL, once deployed) for plain-language comparisons of marketing channels, weekly seasonality, and price–weight views of the Clovis, NM feeder-cattle auction. No login. Mobile-friendly.

**Extension peers, instructors, and graduate students.** Read the methodology pages for how each figure is computed. Fork this repository and adapt it for a different state or auction. Every chart renders from committed code; nothing is hand-tuned.

**Researchers and journalists.** Download the versioned Parquet and CSV snapshots in `/data/processed/`. Cite the annual Zenodo release DOI (planned; first release will land once the platform stabilizes — see GOVERNANCE.md). All derived data carries a CC-BY-4.0 notice; the underlying sources (USDA-AMS, BLS, USDA NASS) are U.S. public domain.

## Data sources

Three public data families are ingested directly and redistributed as cleaned Parquet snapshots under CC-BY-4.0:

- **USDA-AMS Market News** — feeder-cattle auction reports for Clovis (NM) via the MARS API, weekly (sale-week refresh). Historical Era B reconstruction (Oct 2017 – Apr 2019) from the AMS per-slug archive. Public domain.
- **BLS CPI-U** — series `CUUR0000SA0`, used as the deflator for all real-price figures. Public domain.
- **USDA RMA Livestock Risk Protection** — annual Summary of Business zips for feeder-cattle LRP endorsements, 2003-present. Both pipeline infrastructure and the public [LRP explorer page](https://lma-explorer.github.io/lrp.html) are live on `main`. Public domain.

One additional source is **derivative-only**:

- **CME GF feeder-cattle settles** — proprietary. The basis pipeline reads CME settles from a local source (`data/raw/cme/`, gitignored) and writes only the derived `basis = cash − settle` statistic to `data/processed/`. Raw CME settles are not committed or redistributed by this repository. Downstream users wanting raw settles must license them directly from CME DataMine. See `LICENSE-DATA.md` for the full posture.

## Repository layout

```
/site/                         Quarto source (.qmd) for the rendered website
  index.qmd                    home page
  price-weight.qmd             weight-class price-explorer chart
  seasonality.qmd              week-of-year overlay across years
  weekly-trends.qmd            calendar timeline of auction weeks
  basis.qmd                    cash − CME settle by week-of-year
  sell-now-compare.qmd         this-week vs same-week historical band
  one-pager.qmd                producer reference card (typst → PDF)
  data.qmd                     versioned data catalog
  about.qmd                    maintainer and engagement page
  methodology/                 per-pipeline methodology pages
/pipelines/                    one Python package per ingested data source
  bls/                         CPI-U monthly refresh
  clovis/                      USDA-AMS MARS-era weekly refresh + basis derivation
  clovis_historical/           one-time Era B reconstruction (Oct 2017 – Apr 2019)
  cme_feeders/                 (placeholder; see LICENSE-DATA.md re: redistribution)
  lrp/                         USDA RMA LRP annual zips (corpus on disk; chart at /lrp.html)
/data/processed/               committed Parquet snapshots, time-stamped by vintage
/data/raw/                     proprietary inputs and raw API responses (gitignored)
/.github/workflows/            scheduled refresh, site build, future Zenodo deposit
ROADMAP_TREE.md                forward-looking intended structure (separate from current layout above)
```

The forward-looking structure (which Phase-2 pipelines, what files would land where, what additional folders would be created) is documented separately in `ROADMAP_TREE.md`. This README's layout reflects what's on `main` today.

## Licensing

- **Code** (Python, R, Quarto templates, workflows): MIT. See `LICENSE`.
- **Documentation and derived data** (README, methodology, Parquet/CSV snapshots, rendered figures): CC-BY-4.0. See `LICENSE-DATA.md`.

The underlying USDA-AMS, BLS, and USDA NASS data is U.S. public domain and legally carries no attribution requirement. The CC-BY-4.0 notice on derived data is a request for attribution to downstream users, not a constraint inherited from the sources.

## Maintenance philosophy

This project optimizes for **automation over features**. Every user-visible addition must ship with monitoring, failure detection, and a last-known-good fallback in the same pull request. See `GOVERNANCE.md` for the pre-merge checklist.

## Citation

See `CITATION.cff`. Once the first Zenodo deposit is live (planned for once the platform stabilizes — see GOVERNANCE.md for the roadmap), a versioned DOI will be the preferred citation target.

## Contributing and contact

External contributions are welcome. See `GOVERNANCE.md` for the issue-and-PR process. Data-source drift (AMS slug changes, NASS suppression edge cases, BLS schema changes) should be filed using the "Data source drift" issue template.
