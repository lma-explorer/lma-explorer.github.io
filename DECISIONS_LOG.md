# Phase 0 Decisions Log

**Project**: Livestock Marketing Alternatives Explorer
**Phase**: 0 — Governance
**Date closed**: 2026-04-21
**Companion**: `Article_drafts/Interactive_Platform_Strategy_2026.docx` (Section 8)

This log captures the six Phase 0 decisions called out in the strategy document, along with rationale and any deferred sub-decisions. Updates to this file should be append-only — amend, don't rewrite, so the reasoning behind each choice survives.

---

## Decision 1 — Institutional attribution

**Chosen**: Personal / independent.

The platform is maintained by the primary author and an expected co-maintainer as individuals. Extension offices (UArizona and potentially NMSU) are expected to link *to* the platform rather than host or own it.

**Rationale**: The strategy doc references both UArizona (through the forthcoming article) and NMSU (through the Clovis auction and institutional-policy risk row). Rather than force a premature institutional alignment or expose the project to single-institution policy shifts around GitHub Actions / Pages, the platform is kept portable. Extension offices retain full linking and citation privileges. This matches Risk 6 in strategy doc Section 7 ("Institutional GitHub policies restrict Actions or Pages use") and its mitigation ("keep the design portable").

**What this implies**: CODEOWNERS and the README masthead name individuals, not institutions. Attribution lines in derived data acknowledge the underlying public-domain sources (USDA-AMS, BLS, USDA NASS) but not a host institution for the platform itself.

---

## Decision 2 — Platform name and URL

**Chosen name**: Livestock Marketing Alternatives Explorer.

**URL strategy**: Start on `lma-explorer.github.io` (GitHub Pages org-pages URL). Migrate to a custom domain at Phase 2 MVP exit, keeping the `.github.io` URL live as a redirect for one year post-migration so that any external Extension links that land in print materials still resolve.

**GitHub org handle (confirmed 2026-04-22)**: `@lma-explorer`. Org was created on GitHub as a Free personal-account-owned org on 2026-04-22. A single repo under that org, named exactly `lma-explorer.github.io`, serves as both code home and published site (GitHub Pages org-pages convention).

**Rationale**: The working name mirrors the article title, which is the single strongest signal producers will follow when searching for the article. A long name is acceptable for the page title; the URL shortens it to `lma-explorer` for brevity. GitHub Pages org-pages give the cleanest canonical URL (`lma-explorer.github.io`, no trailing repo slug).

**What this implies**: Repo must be named exactly `lma-explorer.github.io`. Custom-domain candidates to evaluate at Phase 2 exit: `livestockmarketing.org`, `cattlemarkets.org`, `lma-explorer.org`.

---

## Decision 3 — License pair

**Chosen**: CC-BY-4.0 for documentation and derived data; MIT for code.

**Rationale**: Strategy doc's recommendation. Simplest possible combination — each license has a plain-English summary and is widely understood by both researchers and Extension peers. MIT is the permissive default for small, non-patent-sensitive Python/R projects; CC-BY-4.0 on derived data communicates an attribution expectation even though the underlying USDA/BLS/NASS data is public domain and legally has no attribution requirement.

**What this implies**: Two license artifacts — `LICENSE` (MIT, for code) and `LICENSE-DATA.md` (CC-BY-4.0 notice, explicitly flagged as applying to docs, derived CSVs/Parquet, and rendered figures). Methodology PDFs and Zenodo deposits inherit CC-BY-4.0.

---

## Decision 4 — First hero chart (Phase 1)

**Chosen**: Price-weight explorer (Article Figures 1/2).

**Rationale**: Strategy doc front-runner. Gives producers an immediately useful view (weight class × sex × year range with min/mean/max bands) and exercises the Parquet → interactive-chart → download path that every other figure on the site will reuse. Uses an archived copy of the Clovis data for Phase 1 so that the hero chart is not blocked on the AMS ingestion pipeline (which lands in Phase 2).

**Alternatives considered and declined**:

- Weekly seasonality (Article Fig 3/4): high producer utility but needs year-overlay UX that's heavier to design up front. Moved to Phase 2.
- Inflation-adjusted price series: tighter coupling to the BLS CPI pipeline that Phase 1 ships, but less immediate "wow" for producers landing on the home page. Moved to Phase 2 as a secondary figure.

**What this implies**: Phase 1 deliverables are (a) the BLS CPI pipeline (feeds the deflator) and (b) the price-weight explorer page reading from an archived Clovis Parquet snapshot.

---

## Decision 5 — Pilot reviewers and co-maintainer

**Chosen**: Defer both until Phase 1 exit.

**Rationale**: The co-maintainer has informally validated the idea but has not formally signed on. Pilot reviewer recruitment (2–4 producers + 2 Extension peers) is more useful against a working prototype than an empty repo. Placeholder entries in CODEOWNERS and GOVERNANCE.md keep the scaffolding unblocked.

**Placeholders to fill by Phase 1 exit**:

- Co-maintainer: name + GitHub handle → CODEOWNERS
- 2–4 producer reviewers: names + preferred contact channel
- 2 Extension peer reviewers: names + institutional affiliation

---

## Decision 6 — Automation over features commitment

**Chosen**: Yes — written into `GOVERNANCE.md`.

**Rationale**: Strategy doc Section 8 final bullet: "every feature must have a monitoring and failure-recovery design before it ships, even if it means fewer features overall. This is the single decision that most determines whether the platform survives Phase 4." This is captured as a binding repository policy rather than an aspiration, with a specific pre-merge checklist.

**What this implies**: Every pull request that adds a user-visible feature (ingestion source, chart, calculator, page) must include, in the same PR, a scheduled-run definition, a failure-detection path (GitHub Issue auto-open on CI red), and a last-known-good fallback. See `GOVERNANCE.md` section "Feature shipping checklist."

---

## Decisions already locked in before Phase 0

These were settled in the strategy-document session and are restated here for completeness:

- **Audiences**: producers + Extension peers + researchers/journalists, served through layered entry points on a single repo.
- **Stack**: Quarto + GitHub Pages; GitHub Actions for scheduled pipelines; Parquet snapshots; annual Zenodo deposit; optional Streamlit/Shiny "lab" subdomain later.
- **Data sources**: USDA-AMS LMR (MARS API), BLS CPI v2, USDA NASS QuickStats. CME GF/LE futures are link-out only, never mirrored.
- **Hosting model**: Co-owned GitHub organization; Extension offices link to the platform, do not host it.

---

## Open decisions punted out of Phase 0

These surfaced during Phase 0 but do not block moving to Phase 1:

- **Custom domain** — defer to Phase 2 MVP exit.
- **CODEOWNERS handles** — placeholder until Phase 1 exit.
- **Pilot reviewer list** — recruit during Phase 1.
- **"Lab" subdomain stack** (Streamlit vs. Shiny-for-Python) — not needed until Phase 3.
- ~~GitHub org handle final confirmation~~ — resolved 2026-04-22; org `@lma-explorer` created on GitHub.

---

## Decision 7 — Companion article status and coupling (added 2026-04-22)

**Context**: On 2026-04-22 the user placed `Article_MarketAlternatives_Slides_Final_2025.docx` into `Article_drafts/` and clarified that it is a draft — not final, not published — but that platform work should continue unblocked.

**Chosen**: The platform treats the companion article as *forthcoming* in all public copy (README, site, CITATION.cff). No version of the platform site is shared outside the pilot reviewer list until the article has a public URL, so no external link lands against a vaporware reference. Internal Phase 1 work proceeds as planned.

**Corrections locked in**:

- **Real article title** used in scaffold: *Market Alternatives and Price Slide Considerations for Selling Feeder Cattle* (Sall & Tronstad, in preparation). The earlier working name *Comparison of Livestock Marketing Alternatives* was in fact the title of the 1994 Tronstad predecessor article in the REDACTED publication, not the forthcoming article. Predecessor is now cited explicitly.
- **Co-author**: Russell Tronstad (UA REDACTED Extension service) is now named in platform copy and pending only a GitHub-handle placeholder in `CODEOWNERS`. Supporting evidence: the forthcoming article's reference list cites "Tronstad, 1994" as its predecessor, and Tronstad is a co-author on three prior Sall publications (2019 beef alliance paper; 2023 alfalfa export and feed-price papers). The working assumption is confirmed; the formal sign-on (co-maintainer handle) remains a Phase 1 exit item per Decision 5.
- **Deflation basis**: the article deflates to **December 2025 dollars**. Pipeline file is renamed accordingly: `cpi_article_basis_2026.parquet` → `cpi_article_basis_2025.parquet`. Every methodology reference updated.
- **Clovis data span**: January 2000 through December 2025 (26 years). Earlier stub in `site/price-weight.qmd` said 2015–2025 and is corrected.
- **Folder rename**: workspace subfolder `Artcle_drafts/` → `Article_drafts/` (typo fixed upstream).

**What this implies**:

- Public launch timing is now coupled to the article's release. The platform can run in pilot-reviewer mode against the draft, but external Extension linking to the platform should wait for the article's release so the companion relationship is symmetric.
- A single new placeholder in the scaffold: once the article has a DOI or a permanent Extension-site URL, update the README, `site/about.qmd`, `site/price-weight.qmd`, and `CITATION.cff` references to link to the article directly.
- If the article's title changes before release, repeat this sweep. Ground truth is `Article_drafts/Article_MarketAlternatives_Slides_Final_2025.docx`'s cover page (currently line 1–3 of the docx).
