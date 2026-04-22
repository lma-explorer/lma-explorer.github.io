# Governance

This document defines who is responsible for what, how changes get into the repository, and the binding policies that protect the platform's long-term viability. It is written into the repository so that it survives changes in personnel.

## Maintainers

- **Primary maintainer**: TBD-PRIMARY (placeholder until Phase 1 exit; see `DECISIONS_LOG.md`, Decision 5).
- **Co-maintainer**: TBD-COMAINTAINER (placeholder until Phase 1 exit).

Both maintainers are individuals, not institutions. The platform is intentionally institution-independent so that policy changes at any single Extension office cannot strand the project. See `DECISIONS_LOG.md` Decision 1.

## Decision authority

| Decision type                                | Authority                          | Forum                          |
|---------------------------------------------|-------------------------------------|--------------------------------|
| Routine bug fixes, documentation typos      | Either maintainer                   | Single PR review               |
| New feature or new ingestion source         | Both maintainers must approve       | PR review + checklist below    |
| License changes                             | Primary maintainer + 30-day notice  | Issue thread, public notice    |
| Adding a third maintainer                   | Both current maintainers            | Public issue, 14-day comment   |
| Sunsetting a feature                        | Either maintainer                   | Public issue, 30-day notice    |
| Emergency takedown (legal, data correction) | Either maintainer                   | Acted on, then publicly logged |

## The Automation Over Features Commitment

This is the load-bearing policy of the project. Per the strategy document Section 8, maintenance burden is the single largest threat to the platform's survival past Phase 4. The following commitment is binding:

> **Every user-visible feature must ship with a monitoring path, a failure-detection path, and a last-known-good fallback. Pull requests that add features without these are not eligible to merge, even if the feature itself is excellent.**

Practically, this means a few things:

1. We accept fewer features in exchange for features that don't rot. A backlog item that has no automation story is not ready to be worked on yet.
2. When a scheduled job breaks, the system tells us — we don't find out from a user.
3. When a scheduled job breaks, what's already published stays correct (last-known-good), even if it's now stale; the site does not start showing partial or wrong data while we fix the break.
4. Manual annual reviews are budgeted; ad-hoc manual maintenance is treated as a sign that something needs to be either automated or sunset.

## Feature shipping checklist

Any pull request that adds or changes a user-visible feature (a new ingestion source, a new chart, a new calculator, a new page) must include, in the same PR:

- [ ] **Scheduled-run definition.** A GitHub Actions workflow file or addition to an existing workflow that runs the new code on the cadence appropriate to the underlying data (sale-day, weekly, monthly, etc.). No "I'll add the schedule later" — schedule and code ship together.
- [ ] **Failure detection.** The workflow must open a GitHub Issue (using the `data-source-drift` template or an equivalent) when it fails. Silent failures are forbidden. Where the upstream data has a parseable schema, a schema-hash check that alerts on change is required (see strategy doc Section 7, Risk 1).
- [ ] **Last-known-good fallback.** When the workflow fails, the previously published data and rendered figures must remain live and unchanged. PRs that overwrite published artifacts mid-failure are rejected.
- [ ] **Vintage stamp.** Any rendered figure or downloadable file derived from refreshed data must carry a visible vintage timestamp ("Data through: YYYY-MM-DD") and a link to the methodology page. Stale data is acceptable; lying about freshness is not.
- [ ] **Methodology page or update.** New ingestion sources or new figure types require a corresponding methodology section; PRs that change the substantive computation behind an existing figure require a methodology update in the same PR.
- [ ] **License compliance check.** New data sources must be confirmed redistributable under CC-BY-4.0 (or moved to link-out only). The PR description states the license posture explicitly.

PRs missing any item above are blocked from merge. The checklist is enforced by maintainer review, not (yet) by automation; promoting any of these items to a CI check is itself a welcome contribution.

## Issue and pull request etiquette

- **Issues** for bugs, data drift, or feature requests use the templates in `.github/ISSUE_TEMPLATE/`. Issues filed without a template will be triaged but may be slower to act on.
- **Pull requests** should reference the issue they close (`Closes #N`) and check off the feature shipping checklist above where applicable.
- **Discussion** of larger directional questions (e.g., "should we add a new state's auction data?") happens in GitHub Discussions before a PR is opened, not in a PR's review thread.

## Acceptable use

- **Forking and adapting**: explicitly encouraged. The MIT + CC-BY-4.0 license pair is chosen specifically so Extension peers in other states can fork the repository and adapt it without legal friction.
- **Mirroring derived data**: permitted with attribution. Please pin to a specific Zenodo DOI when mirroring so the vintage is unambiguous.
- **Redistributing CME data**: not permitted from this repository because we do not host CME data (link-out only). Downstream users wanting bulk CME data must license it directly from CME DataMine.
- **Scraping**: this repository does not scrape any source. All ingestion uses documented APIs with registered API keys. Forks should preserve this posture.

## Data retention

- **Processed data** (`/data/processed/`): committed to the repository indefinitely. Old vintages are retained as part of the git history; the latest of each cadence is also kept as a `latest` symlink for convenience.
- **Raw API responses** (`/data/raw/`): gitignored. Cached locally for debugging; retained only in CI artifacts (default 90-day GitHub retention). Not redistributed.
- **CME-derived figures**: any figure computed from publicly posted (lagged) CME numbers is regenerated from scratch each run; no CME numbers are persisted to disk.

## Compliance posture

- USDA-AMS, BLS, and USDA NASS data are public-domain U.S. government works (17 U.S.C. § 105) and may be republished with attribution. The CC-BY-4.0 notice on derived data is a request, not a legal restriction inherited from the source.
- CME settlement data is proprietary. The platform does not mirror, cache, or redistribute CME settlement series. See `LICENSE-DATA.md` for the full data-licensing posture.

## Annual review

Once per year (target: each January), both maintainers conduct a written review covering: (a) which features are healthy; (b) which features need attention or sunset; (c) whether any new data source is worth adding; (d) whether any aspect of this governance document needs to change. The review is committed to `/docs/annual-review-YYYY.md`.

## Changing this document

Changes to `GOVERNANCE.md` require primary-maintainer approval and a 14-day public comment window via a dedicated issue. The current version of this document always lives at the repository root.
