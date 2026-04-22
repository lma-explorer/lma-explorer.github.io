# Data and Documentation License

The non-code contents of this repository — documentation, methodology, derived data snapshots, and rendered figures — are licensed under the **Creative Commons Attribution 4.0 International License (CC-BY-4.0)**.

Full license text: https://creativecommons.org/licenses/by/4.0/legalcode

## What CC-BY-4.0 applies to

- Markdown and Quarto documentation in `/site/` and the repository root (`README.md`, `GOVERNANCE.md`, `DECISIONS_LOG.md`, etc.)
- Methodology PDFs and, on release, the archived Extension article *Market Alternatives and Price Slide Considerations for Selling Feeder Cattle* (Sall & Tronstad, in preparation) in `/docs/`
- Derived Parquet and CSV snapshots under `/data/processed/`
- Rendered chart images and static HTML figures produced by the site build

## What is *not* covered by this license

- **Code** in the repository (Python modules, R scripts, Quarto code cells, GitHub Actions workflows) — licensed under MIT. See `LICENSE`.
- **Upstream source data** — USDA-AMS, BLS, and USDA NASS data are U.S. government works and are in the public domain (17 U.S.C. § 105). This repository's CC-BY-4.0 applies only to the *derived* versions — the cleaned, normalized, snapshotted, and documented form in which they are published here.
- **CME settlement data** — proprietary; not hosted in this repository. Links only.

## Attribution expected

When using documentation, figures, or derived data from this repository in a downstream work, please cite:

> Livestock Marketing Alternatives Explorer. [Year of snapshot]. [URL]. Licensed under CC-BY-4.0.

Once an annual Zenodo deposit is live (planned for Phase 2 exit), the DOI should be preferred over the repository URL for durable citation. See `CITATION.cff`.

## Attribution expected for upstream sources (public-domain)

Even though the underlying U.S. government data has no legal attribution requirement, downstream users are asked to also credit the primary source:

- USDA-AMS: "Source: U.S. Department of Agriculture, Agricultural Marketing Service."
- BLS: "Source: U.S. Bureau of Labor Statistics."
- USDA NASS: "Source: USDA NASS QuickStats."
