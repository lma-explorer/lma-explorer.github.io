# Security policy

The Livestock Marketing Alternatives Explorer is a public, open-source research platform served by GitHub Pages. The threat model is correspondingly narrow: there is no user account system, no authenticated surface, no payment flow, no PII intake, and no privileged backend. The deployed site is a static set of HTML, CSS, JavaScript, and pre-rendered data artifacts.

That said, the project does run scheduled GitHub Actions workflows that ingest data from public APIs (USDA-AMS Market News, BLS, USDA RMA) and publish derived snapshots back to the repository. Issues affecting that pipeline — for example, a vulnerability that could allow a third party to write malicious data into the published snapshots, or to take over the workflow's GitHub-issued token — fall within scope.

## Supported versions

This project does not maintain multiple parallel release branches. **Only the `main` branch is supported.** Security fixes land on `main` and are picked up on the next deploy. Previously published snapshots in `data/processed/` remain available for reproducibility, but receive no security backports.

The version reported in `pyproject.toml` and `CITATION.cff` reflects the current state of `main`. There is no support for older tagged versions; users who need a frozen reference vintage should pin to a specific commit SHA in their citations.

## Reporting a vulnerability

If you believe you have found a security vulnerability in this project, please report it privately rather than opening a public issue.

Preferred channel: open a [private security advisory](https://github.com/lma-explorer/lma-explorer.github.io/security/advisories/new) on the repository. GitHub will route it to the maintainer without public visibility.

If you cannot use GitHub's private advisory flow, please email the maintainer at the address listed on the maintainer's GitHub profile and include `[lma-explorer security]` in the subject line.

Please include in your report:

- A description of the vulnerability and its impact.
- Steps to reproduce, including any URLs, payloads, or workflow runs involved.
- Whether the issue is already public; if so, where.
- Any suggested mitigation.

## What to expect

The maintainer will acknowledge receipt within **7 calendar days** and provide an initial assessment within **14 calendar days** of receipt. Depending on severity:

- **High severity** (active exploitation, ability to inject malicious data into published snapshots, ability to take over the deploy workflow): the maintainer will work to land a fix on `main` and trigger a redeploy as soon as practical, and will publish a coordinated advisory once the fix is live.
- **Medium severity** (theoretical exploit paths, hardening opportunities): scheduled into the next regular development cycle; advisory published when the fix lands.
- **Low severity** (informational, low-impact): may be addressed via a regular pull request without a separate advisory.

This is a research-grade prototype maintained by a single person; turnaround timelines are best-effort, not contractual.

## Out of scope

The following are not in scope for this policy:

- Issues affecting GitHub.com itself, GitHub Pages hosting, or the GitHub Actions runner. Report those to GitHub via [their security policy](https://github.com/security).
- Issues affecting third-party Python packages or GitHub Actions used by the project. Report those upstream; if the project's pinning leaves users exposed, that is in scope, but the underlying CVE is not.
- Disagreements with methodological choices documented in `site/methodology/`. Those belong in regular issues or pull requests.
- Privacy concerns about repository-level traffic visible to the maintainer via GitHub's Insights tab. The `GOVERNANCE.md` privacy section describes what the maintainer does and does not see.

## Public disclosure

After a fix has landed and the deploy is live, the maintainer will publish an advisory describing the issue, its impact, the affected commits, and the fix. The reporter will be credited unless they prefer otherwise.

---

*Last reviewed: 2026-05-04. Flagged in the 2026-05-04 independent audit (item #9).*
