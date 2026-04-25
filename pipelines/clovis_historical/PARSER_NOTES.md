# Era B TXT parser — locked invariants (sub-task 9.2 pilot, 2026-04-24)

This file documents the parser's assumptions and the four pilot files that
exercise them. Treat it as the regression baseline for sub-task 9.3 (full
ingest of all 76 Era B weeks).

## Pilot files

| Filename | Auction date | Source URL | Notes |
|---|---|---|---|
| `CV_LS75020171005.TXT` | 2017-10-04 | `mymarketnews.ams.usda.gov/.../1781/2017-10-05/780646/...` | Earliest report in MMN. Includes Holstein Steers Large 3 block (only 2 Holstein lots in the corpus). City header "Clovis". |
| `CV_LS75020180118.TXT` | 2018-01-17 | `mymarketnews.ams.usda.gov/.../1781/2018-01-18/687384/...` | Mid-Era-B reference. 95 lots, full M&L 1/1-2/2 grades. |
| `CV_LS75020180719.TXT` | 2018-07-18 | `mymarketnews.ams.usda.gov/.../1781/2018-07-19/557988/...` | Late-summer 2018; first appearance of Bulls section in pilot. Header line widths differ from Jan 2018. |
| `CV_LS75020190327.TXT` | 2019-03-27 | `mymarketnews.ams.usda.gov/.../1781/2019-03-27/450037/...` | Two weeks before Era B/A handoff (last TXT is 2019-04-11). City header "Amarillo, TX" — a TX-based reporter handled this NM Clovis sale. Same-day publication (Wed not Thu). |

Pilot output: `data/processed/clovis_historical_era_b_pilot.csv` (327 rows, 18
columns). Production runs write `clovis_historical_era_b_<vintage>.parquet`
via pandas+pyarrow (same engine the live MARS snapshot already uses).

## Row-count audit (all four match manual section-by-section count)

| File | Parsed | Manual total |
|---|---|---|
| 2017-10-04 | 77 | 17 + 18 + 5 + 2 + 15 + 15 + 5 = 77 ✓ |
| 2018-01-17 | 95 | 22 + 18 + 11 + 21 + 15 + 8 = 95 ✓ |
| 2018-07-18 | 102 | 20 + 19 + 6 + 25 + 12 + 9 + 3 + 4 + 4 = 102 ✓ |
| 2019-03-27 | 53 | 15 + 9 + 12 + 9 + 3 + 3 + 2 = 53 ✓ |

## Format invariants the parser depends on

1. **Auction date marker** — Two known variants, both handled:
   - 2017-present (MMN era): `Feeder Cattle Weighted Average Report for MM/DD/YYYY`
   - Pre-2017 (Wayback Mann Library era): `Feeder Cattle Weighted Average for M/DD/YY` — no "Report", 2-digit year
   The regex tolerates both: `Feeder Cattle Weighted Average\s+(?:Report\s+)?for\s+(\d{1,2})/(\d{1,2})/(\d{2,4})`. 2-digit years are mapped to 20YY (safe — the auction predates 2000-format reports). The published date in the city-header line is sometimes the auction date and sometimes the publication date (Wed-vs-Thu varies by reporter); we trust the "Report for ..." line for the canonical auction date.
2. **Receipts marker** — `Receipts:  N    Last Week:  N    Year Ago:  N` (varied whitespace and decimal commas tolerated).
3. **Class header** — `Feeder [Holstein ]<Steers|Heifers|Bulls> <Medium and Large|Large> <1|1-2|2|3>`. Holstein appears as a prefix only when present (2 lots in the pilot, both 2017-10-04). Frame "Large" appears for Holstein Large 3 only; "Medium and Large" for everything else. Grade is one of `1`, `1-2`, `2`, `3`.
4. **Column header** — `Head   Wt Range   Avg Wt    Price Range   Avg Price` (whitespace-tolerant, used as a "skip this line" anchor — we do not parse against fixed column positions because widths drift across reporters/years).
5. **Data row pattern** — `<head> <wtrange> <avgwt> <pricerange> <avgprice> [annotation]`, where `wtrange`/`pricerange` are either single numbers or hyphenated `low-high`. Annotation is free text after the avg price.
6. **Section terminator** — first occurrence of `Slaughter Cows:`, `Slaughter Bulls:`, `Replacement Cows:`, `Cow/Calf Pairs:`, `Holstein:` (the slaughter-Holstein block in the 2017 file), `Source:`, or any line beginning with `# ` (our own provenance comments).

## Output schema (matches `pipelines/clovis/snapshot.py` long-format)

| Column | Type | Notes |
|---|---|---|
| `auction_date` | date | from "Report for MM/DD/YYYY" |
| `commodity` | string | always `"Feeder Cattle"` |
| `class` | string | `"Steers"` / `"Heifers"` / `"Bulls"` |
| `frame` | string | `"Medium and Large"` (≈99%) or `"Large"` (Holstein) |
| `muscle_grade` | string | `"1"` / `"1-2"` / `"2"` / `"3"` |
| `weight_break_low` | int | from "Wt Range" low end (single value → low=high) |
| `weight_break_high` | int | from "Wt Range" high end |
| `avg_weight` | float | from "Avg Wt" |
| `avg_weight_min` | NaN | not reported in TXT — always null in Era B |
| `avg_weight_max` | NaN | same |
| `head_count` | int | from "Head" |
| `price_low` | float | from "Price Range" low end (single value → low=high) |
| `price_high` | float | from "Price Range" high end |
| `price_avg` | float | from "Avg Price" |
| `receipts` | int | from "Receipts:" header (auction-level, copied to every row) |
| `vintage` | date | ingest run date |
| `breed` | string | `"Beef"` (default) or `"Holstein"` |
| `annotation` | string | lot attribute: `""` / `"Value Enhanced"` / `"Calves"` / `"Thin"` / `"VA Calves"` / `"Guaranteed Open"` / `"New Crop"` / `"Fleshy"` / `"Full"` (and rare others) |

The 16 canonical columns match `snapshot.py` exactly. `breed` and `annotation` are extras kept for fidelity; downstream chart code can ignore them or use them as filter axes (the Era A pipeline calls these `lot_qualifier` in MARS terms).

## Annotation distribution in the pilot (327 lots)

| n | annotation |
|---|---|
| 206 | (none — these are the rows the price-weight chart consumes) |
| 61 | Value Enhanced |
| 23 | Calves |
| 17 | Thin |
| 10 | VA Calves |
| 7 | Guaranteed Open |
| 1 | New Crop |
| 1 | Fleshy |
| 1 | Full |

When 9.3 ingests all 76 weeks, the annotation set may grow (e.g. "Yearlings", "Value Added"). The validator (sub-task 9.6) should warn — not fail — on novel annotation values, since they're not data-quality issues.

## Known fidelity gaps vs. the live MARS schema

- `avg_weight_min` / `avg_weight_max` are **always null** in Era B — the TXT format does not break out per-lot weight min/max; only the single `Avg Wt` is reported. Downstream code that paints shaded bands on the price-weight chart already handles nulls by collapsing to the avg.
- Some lots in the source TXT show a single-value `Wt Range` like `285` instead of a `low-high` range. We populate `weight_break_low == weight_break_high` for those; chart code already handles that case for MARS data.
- `vintage` semantics differ from Era A: a one-time historical-block ingest stamps every row with the same vintage. If a parser fix later corrects rows, the new run gets a new vintage and the MANIFEST appends; the prior file stays for SHA-256 reproducibility (per Phase-0 commitment, see DECISIONS_LOG Decision 6).

## What 9.3 still has to confirm

1. Whether any 2017-10/11/12 reports introduce an additional class label (e.g. "Yearling Steers") not present in the pilot. The regex falls back gracefully (the section is silently skipped and counted) but a coverage warning should fire.
2. Whether weight-range rows with decimal weights (e.g. `434.5`) ever appear. The regex tolerates them.
3. Whether the 2022 anomaly (31 weeks vs. 44–50 typical, per `mmn_inventory.csv`) is auction-cancellation noise or a parser-relevant format change. Inspect the 31 files when 9.3 runs.

## Late-2018 / early-2019 prose variant (documented 2026-04-25)

A 16-file cluster covering Oct 2018 → Mar 2019 uses a different prose template — the standard `Feeder Cattle Weighted Average Report for MM/DD/YYYY` line is **absent**. Example header from `CV_LS75020181004.TXT`:

```
CV_LS750
Clovis, NM     Friday, October 5, 2018     USDA-NM Dept. of Ag Market News

Clovis Livestock Auction - Clovis, NM - Wednesday, October 3 & 4, 2018

Receipts:  3797      Week Ago:  3135    Year Ago:  2243

Notice:  Due to increased receipts Clovis Livestock Auction will be having 2-day
```

The driver was an operational change at Clovis: the auction went from 1-day Wednesday sales to **2-day Wednesday-Thursday sales** during the high-receipts run, with reports published on Friday. The reporter swapped the standard "Report for ..." template for the more descriptive "Clovis Livestock Auction - Clovis, NM - <Day>, <Month> <day1> & <day2>, <year>" line.

Files in the cluster (all auto-handled via filename fallback):

- 2018: 10/04, 10/11, 10/17, 10/24, 10/25, 11/01, 11/08, 11/15, 11/29, 12/06, 12/13, 12/20
- 2019: 01/10, 02/08, 02/12, 03/07

**Decision: do not extend the regex to parse the variant.** The filename
fallback already produces the correct auction date (the filename uses the
last auction day, which is the right anchor for weekly bins). Trying to
regex-parse long-form prose with month names, day-pairs (e.g. "October
3 & 4"), and ordinal commas would add complexity for cosmetic gain. The
ingest's `WARNING:` block surfaces the cluster transparently; future
re-runs of `ingest_era_b.py` will list the same 16 files until/unless
USDA-AMS retroactively replaces those files with standard-template
versions.

If we later want to *retire* the WARNING (cleaner ingest output), the
right move is to add a second regex that anchors on the city header
line — `Clovis Livestock Auction.*<day-name>, <month-name>\s+\d+(?:\s*&\s*\d+)?,?\s+\d{4}` —
plus a small `MONTHS = {...}` lookup. That's a 30-minute change with
zero data impact.

## Sub-task 9.4a (Wayback) compatibility — verified

The 2010 Wayback CV_LS750 sample (`data/raw/clovis_historical/wayback/cv_ls750_20100129.txt`) parses against the **same regex set** as the MMN files, after the auction-date regex was relaxed (see invariant 1). Result on that sample:

- 88 rows from 10 class blocks (Steers/Heifers/Bulls × M&L 1/1-2/2/3, where present)
- `auction_date = 2010-01-27` ✓ (matches source "1/27/10")
- `receipts = 2385` ✓ (matches source "Receipts: 2385")
- Manual section count matches: 19+12+10+3+14+13+8+4+4+1 = 88 ✓

This means 9.4a, when activated, reuses `era_b_txt.py` unchanged and only adds the LMIC cross-check + chart-overlay UX work — no parser fork.
