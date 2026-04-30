# Contributing

Thanks for considering a contribution. The platform is small and single-maintained, so contributions are welcome but the bar for changes that affect what producers see is intentionally high — we'd rather ship five well-tested figures than fifty drifting ones.

If you spot a bad number, a confusing chart, a broken link, or a methodology choice you disagree with, please open an issue. If you want to propose a change, read the rest of this document first.

## Three ways to engage

1. **Found an error or have a small fix?** Open an [issue](https://github.com/lma-explorer/lma-explorer.github.io/issues/new/choose) using the most appropriate template (`bug_report`, `data_source_drift`, or `feature_request`). Issues without a template will still be triaged but may take longer.

2. **Want to suggest a methodology change or a new chart page?** Open an issue first — *before* writing the code — describing the question the chart answers and the data sources involved. Methodology decisions tend to be load-bearing across multiple pages, and a 30-minute conversation in an issue can save days of rework.

3. **Want to send a fix as a pull request?** Read [`GOVERNANCE.md`](GOVERNANCE.md) — particularly the **feature shipping checklist** — and see [Pull request workflow](#pull-request-workflow) below.

## Where decisions happen

- **Bug reports, data drift, individual chart adjustments**: GitHub Issues using the templates in `.github/ISSUE_TEMPLATE/`.
- **Larger directional questions** ("should we add a new state's auction?", "should we change the deflation basis month?"): the GOVERNANCE doc says these go in GitHub Discussions, but the Discussions tab is not yet enabled on this repository. For now, open a regular Issue with the `discussion` label and use it as the forum. If/when Discussions is enabled, that section of GOVERNANCE will be updated.
- **PR review**: the maintainer reviews directly. CODEOWNERS routes review requests automatically.

## Pull request workflow

### Before opening a PR

1. **Read [`GOVERNANCE.md`](GOVERNANCE.md)** — especially the *Automation Over Features* commitment and the *feature shipping checklist*. Most PR rejections are about checklist items missing, not about the code itself.
2. **For new features**, open an issue first to align on scope.
3. **Run the test suite locally** before opening the PR — see [Local development setup](#local-development-setup).

### What every PR must include

The full checklist lives in `GOVERNANCE.md`. Headline items:

- [ ] **A scheduled refresh** if the PR adds a new ingestion source. PRs that add ingestion code without a corresponding workflow file are not eligible to merge.
- [ ] **Failure detection** — the workflow opens a GitHub Issue (using `data-source-drift` template) on failure. Silent failures are forbidden.
- [ ] **Last-known-good fallback** — a failed run never overwrites a published artifact.
- [ ] **A vintage stamp** on every figure or download derived from refreshed data.
- [ ] **A methodology page or update** — new charts and substantive methodology changes both require methodology pages.
- [ ] **License compliance** — new data sources are confirmed redistributable under CC-BY-4.0 (or restructured to link-out only).

### What the maintainer will check

- That the test suite passes (`pytest tests/` — run by CI).
- That `ruff check .` is clean.
- That `python3 scripts/check_no_debug.py` finds no debug-leftover code.
- That the methodology page (if added) answers the six standard questions (see [What a methodology page contains](https://github.com/lma-explorer/lma-explorer.github.io/blob/main/site/methodology/index.qmd#what-a-methodology-page-contains)).
- That the PR closes an issue (`Closes #N`) where applicable.

## Local development setup

Clone the repo and install dependencies:

```bash
git clone https://github.com/lma-explorer/lma-explorer.github.io.git
cd lma-explorer.github.io
pip install -r pipelines/requirements.txt
```

For development tooling (linter, test runner, pre-commit):

```bash
pip install ruff pytest pre-commit
pre-commit install
```

After `pre-commit install`, the debug-detect hook + ruff + standard hygiene checks run on every commit and block the commit if any check fails. Run manually any time:

```bash
pre-commit run --all-files
```

To run the test suite:

```bash
pytest tests/ -v
```

Tests use synthetic data (no parquet reads, no API calls), so they're fast. The full suite finishes in under 5 seconds.

To render the site locally:

```bash
cd site
quarto render
```

Quarto will run two pre/post-render hooks (see `_quarto.yml`'s `pre-render` and `post-render` keys):

1. **Pre-render**: generates CSV (and XLSX for small files) from each `data/processed/*.parquet`.
2. **Post-render**: copies `data/processed/` into `site/_site/data/processed/` so the deployed site's download links resolve.

To preview the rendered site locally:

```bash
cd site/_site
python3 -m http.server 8000
# Visit http://localhost:8000
```

Plotly figures need an HTTP server (CDN-loaded JS won't work over `file://`).

## Adapting the platform for a different region or auction

The MIT license + CC-BY-4.0 license pair is chosen specifically so Extension peers in other states can fork the repository and adapt it without legal friction. A few notes if you're going down that road:

- **Pipelines are independent**: each `pipelines/<source>/` is self-contained. Adding a new auction is mostly cloning `pipelines/clovis/` and adapting the slug/URL. The chart-page code reads the resulting parquet via the `load_<source>_combined` pattern.
- **Methodology pages are per-pipeline**: forks should write their own methodology pages reflecting their data choices, even if the chart code is unchanged.
- **The platform's license posture (CC-BY for derived data, no CME redistribution) is region-agnostic** — fork it as-is unless your region has different licensing constraints.

If you're forking, please:

1. Change `site-url` in `_quarto.yml` to your domain.
2. Update `CITATION.cff` with your authorship.
3. Reach out via an Issue if you'd like to be linked from a "see also" section here — we'd love to grow the network.

## Code of conduct

Be kind. Disagreements about methodology are welcome and useful; ad-hominem attacks are not. The maintainer reserves the right to lock or close conversations that aren't productive.

## Questions

If something here isn't clear, open an issue with the `documentation` label.
