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

- `commodity_code == "0801"` (Feeder Cattle)
- `plan_code == "81"` (Livestock Risk Protection)

Other commodities (Fed Cattle 0802, Swine 0815) and non-LRP plans are preserved in the raw zips for the record but not carried into the processed parquet.

**Type-code narrowing is intentionally NOT applied at snapshot time.** Within commodity 0801 the type-code taxonomy expanded over the 24-year corpus (see schema-evolution findings below). The full feeder-cattle slate is kept in the parquet so 4.LRP-d's volume-by-state visualizations can show the complete LRP picture. The 4.LRP-c backtest narrows to the analytically-comparable subset `{"809", "810", "811", "812"}` (Steers Weight 1/2, Heifers Weight 1/2) — those are the type codes that map cleanly to Clovis auction lots. Dairy (815/816), Brahman (813/814), and Unborn (817/818/819) are excluded from the backtest because they don't have a clean Clovis-cash counterpart.

## Schema-evolution findings (24-year backfill, 2003-2026)

These are empirical findings from parsing every year. Captured here so 4.LRP-c (backtest scope) and 4.LRP-d (chart page scope) make informed decisions rather than re-derive these.

**Total feeder-cattle LRP corpus**: 210,375 rows across 24 years.

**Volume explosion in 2021.** Annual feeder-cattle row counts:

```
2003:     31     2009:    611     2015:  2,549     2021:  6,395
2004:    362     2010:  1,227     2016:  1,715     2022: 12,892
2005:  1,308     2011:  2,444     2017:  2,252     2023: 28,511
2006:  1,590     2012:  2,098     2018:  1,264     2024: 37,493
2007:    752     2013:  1,386     2019:  1,017     2025: 54,363
2008:  1,157     2014:  3,505     2020:    814     2026: 44,639 (partial)
```

The 2021+ surge tracks the [2018 Farm Bill](https://www.usda.gov/media/blog/2019/02/26/2018-farm-bill-and-livestock-risk-protection-program) increase to LRP premium subsidies, which made the program substantially more attractive to producers. Any 4.LRP-c historical backtest spanning 2003-2020 will be data-thin; the heaviest, most decision-relevant data is 2021+.

**State-level granularity began in 2004 but stayed sparse until 2021.** State counts and `XX` (national-aggregate) counts:

| Year | Unique states (excl. XX) | Rows in `XX` |
|---|---|---|
| 2003 | 0 | 566 (100%) |
| 2004 | 7 | 544 |
| 2010 | 14 | 558 |
| 2015 | 20 | 1,048 |
| 2020 | 16 | 534 |
| 2021 | 28 | 2,782 |
| 2024 | 37 | 10,252 |
| 2026 | 39 | 11,488 |

For 4.LRP-d's state-level choropleth, the practical data window is **2021+**. Earlier years are mostly `XX`, which doesn't render usefully on a state-level map.

**NM (Clovis state) data window.** NM rows by year:

```
2003-2010: 0 in every year
2011: 11        2017: 0         2023: 501
2012: 0         2018: 0         2024: 735
2013: 13        2019: 0         2025: 729
2014: 19        2020: 0         2026: 613 (partial)
2015: 14        2021: 88
2016: 10        2022: 268
```

For an NM-specific Clovis-cash backtest, the meaningful data window is **2021+** (~2,900 NM-specific rows total). A more permissive cut (2011+) adds another ~80 NM rows but spans years where coverage was sporadic.

**Type-code taxonomy expansion.** Within feeder cattle (commodity 0801):

- 2003 had **only one** feeder cattle type code: `810` (then named "STEERS").
- 2005 added Steers Weight 1 vs. Weight 2 split (codes 809/810).
- 2005-2020 stable set: `{809, 810, 811, 812, 815, 816, 820, 997}` (8 codes).
- 2021 added: 814 (Brahman Weight 2), 817 (Unborn Steers & Heifers), 821.
- 2024 added: 818 (Unborn Brahman), 819 (Unborn Dairy).
- 2026 added: 823.

Type names also drifted (e.g., 810 went from "STEERS" in 2003 to "Steers Weight 2" in 2024). The platform stores the type name as-published per row; do not assume a stable mapping across years.

**Coverage prices reflect the cattle cycle.** 2003 coverage prices were $25-$82/cwt (median $36); 2024 was $62-$310/cwt (median $238). Real-vs-nominal deflation will be relevant if the chart wants cycle-comparable views — see the BLS pipeline's CPI-deflation pattern for precedent.

## Public-attribution posture

This pipeline reads only public-domain RMA data published under [17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105). RMA documentation links are linked-out, not redistributed. See repo-root `LICENSE-DATA.md` for the canonical posture.

## Why bulk zips, not RIRS

RMA also publishes a web app called RIRS that exposes a query interface to the same underlying data, but only from July 2021 forward. The bulk zips reach back to 2003, are documented schema-stable, and avoid the rate-limit and session-token complexity of the web-app interface. The probe (M1.3, matrix v0.3) confirmed the bulk zips as the primary ingestion path; RIRS remains useful for ad-hoc lookups but is not part of the pipeline.

## Future work flagged here

- **`4.LRP-d` choropleth candidate**: state-level LRP concentration map driven by `producer_premium_amount` summed by `state_abbr`. Sized 3-4 hrs. Decision deferred until the backtest data story is legible.
- **`v2.lrp-county-choropleth`**: county-level granularity using topojson, contingent on state-level being well-received. Adds non-trivial CI weight (topojson or geopandas), so explicitly v2.
- **Monthly refresh cron**: not added at scaffold time. Decision deferred until 4.LRP-d is live and we can see how often RMA bumps the current-year zip in practice.
