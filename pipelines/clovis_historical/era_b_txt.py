"""Era B parser — fixed-width USDA-AMS Clovis Livestock Auction TXT reports.

Era B spans October 5, 2017 → April 11, 2019. Format is the legacy plain-text
"CV_LS750{YYYYMMDD}.TXT" report served from the MMN per-slug archive at
``mymarketnews.ams.usda.gov/filerepo/sites/default/files/1781/<auction_date>/<node_id>/...``.

Schema parity: produces the same long-format columns documented in the
``pipelines.clovis.snapshot`` module docstring, so Era A (MARS) and Era B (TXT)
parquet files can be unioned at read time without reconciliation.

    auction_date      : date         (parsed from "Report for MM/DD/YYYY")
    commodity         : string       ("Feeder Cattle")
    class             : string       ("Steers" / "Heifers" / "Bulls")
    frame             : string       ("Medium and Large" / "Large")
    muscle_grade      : string       ("1" / "1-2" / "2" / "3")
    weight_break_low  : Int32 (NaN)
    weight_break_high : Int32 (NaN)
    avg_weight        : float
    avg_weight_min    : float (NaN)  — not reported in TXT, always NaN here
    avg_weight_max    : float (NaN)  — same
    head_count        : Int32
    price_low         : float
    price_high        : float
    price_avg         : float
    receipts          : Int32 (NaN)
    vintage           : date         (ingest date)

Two extra columns kept alongside the canonical 16 (downstream ignores unknowns):

    breed             : string       ("Beef" default; "Holstein" for Holstein header)
    annotation        : string       ("Calves" / "Value Enhanced" / "Thin" / ... or empty)

The 2017-10-05 report includes one Holstein Steer block that the chart code
filters out at render time (frame == "Medium and Large" excludes it); we keep
it here for fidelity. ``annotation`` carries lot attributes ("Calves",
"Value Enhanced", etc.) so chart code can mirror the existing Era A filter
(where `lot_qualifier` is the equivalent MARS field).

Stops parsing at the first ``Slaughter Cows:`` line — the chart pages use
feeder $/cwt prices only, per ``pipelines/clovis/snapshot.py`` docstring.
Replacement Cattle, Slaughter Cattle, Cow/Calf Pairs are intentionally not
emitted.

Usage:
    python -m pipelines.clovis_historical.era_b_txt parse <path> [<path>...]
    python -m pipelines.clovis_historical.era_b_txt pilot     # runs the 4-week pilot

The parser is regex-based, not column-position-based, because column widths
drift across reporters/years (e.g. 2018-01-18 vs. 2018-07-19 use different
header line widths).
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

# ---- regexes ----------------------------------------------------------------

# Auction date: "Feeder Cattle Weighted Average Report for 01/24/2018"
# Also handles the older Wayback-era format: "Feeder Cattle Weighted Average for 1/27/10"
# (no "Report", 2-digit year — see Wayback sample 2010-01-30 in
# data/raw/clovis_historical/wayback/cv_ls750_20100129.txt).
RE_AUCTION_DATE = re.compile(
    r"Feeder Cattle Weighted Average\s+(?:Report\s+)?for\s+(\d{1,2})/(\d{1,2})/(\d{2,4})"
)

# Receipts: "Receipts:  2595       Last Week:  4278       Year Ago:  2137"
RE_RECEIPTS = re.compile(r"^\s*Receipts:\s*([\d,]+)", re.MULTILINE)

# Class header — captures species (Steers/Heifers/Bulls), optional Holstein
# breed marker, frame ("Medium and Large" or "Large"), and grade ("1"/"1-2"/"2"/"3").
RE_CLASS_HEADER = re.compile(
    r"""^\s*Feeder
        \s+(?P<breed>Holstein\s+)?       # optional 'Holstein '
        (?P<species>Steers|Heifers|Bulls)
        \s+(?P<frame>Medium\ and\ Large|Large)
        \s+(?P<grade>1-2|1|2|3)
        \s*$
    """,
    re.MULTILINE | re.VERBOSE,
)

# Column header line: " Head   Wt Range   Avg Wt    Price Range   Avg Price"
RE_COLHEADER = re.compile(r"^\s*Head\s+Wt Range\s+Avg Wt\s+Price Range\s+Avg Price")

# Data row:
#   <head> <wtrange> <avgwt> <pricerange> <avgprice> [<annotation>]
# Where wtrange and pricerange may be either a single number or `low-high`.
# Avg wt is integer; prices are dollar-cents.
# Annotation is free text after the avg price, possibly empty.
RE_DATA_ROW = re.compile(
    r"""^
        \s*(?P<head>\d+)
        \s+(?P<wt>(?:\d+(?:\.\d+)?)(?:-\d+(?:\.\d+)?)?)
        \s+(?P<avgwt>\d+(?:\.\d+)?)
        \s+(?P<price>(?:\d+\.\d+)(?:-\d+\.\d+)?)
        \s+(?P<avgprice>\d+\.\d+)
        \s*(?P<annot>.*)$
    """,
    re.VERBOSE,
)

# Hard stop signals — when feeder section ends.
RE_END_SECTIONS = re.compile(
    r"""^\s*(?:
        Slaughter\s+Cows:|
        Slaughter\s+Bulls:|
        Replacement\s+Cows:|
        Cow/Calf\s+Pairs:|
        Holstein\s*:|              # "Holstein:  Slaughter Cows:" in 2017 report
        Source:|
        \#\s                       # our own provenance comment lines
    )""",
    re.VERBOSE,
)


# ---- data class -------------------------------------------------------------

@dataclass
class Row:
    auction_date: date
    commodity: str
    cls: str               # "class" is reserved
    frame: str
    muscle_grade: str
    weight_break_low: int | None
    weight_break_high: int | None
    avg_weight: float | None
    avg_weight_min: float | None
    avg_weight_max: float | None
    head_count: int
    price_low: float
    price_high: float
    price_avg: float
    receipts: int | None
    vintage: date
    breed: str = "Beef"
    annotation: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["class"] = d.pop("cls")
        for k, v in list(d.items()):
            if isinstance(v, date):
                d[k] = v.isoformat()
        return d


# ---- helpers ----------------------------------------------------------------

def _parse_range(token: str) -> tuple[float | None, float | None]:
    """'300-345' -> (300.0, 345.0); '285' -> (285.0, 285.0); '' -> (None, None)."""
    if not token:
        return (None, None)
    if "-" in token:
        a, b = token.split("-", 1)
        return (float(a), float(b))
    v = float(token)
    return (v, v)


# Filename pattern: CV_LS750{YYYYMMDD}.TXT — always carries the report date.
# Used as a bulletproof fallback when prose variants of the auction-date
# header line slip past RE_AUCTION_DATE. The report-date in the filename is
# the publication date, which is the same as the auction date on same-day
# reports (most of 2019+) and one day after the auction otherwise (typical
# Wed sale, Thu report). The chart code aggregates to weekly bins so a 1-day
# offset is below the resolution of every downstream figure; we still prefer
# the prose-derived auction date when available, and only fall back when the
# prose match fails.
RE_FILENAME_DATE = re.compile(r"CV_LS750(\d{4})(\d{2})(\d{2})", re.IGNORECASE)


def _parse_auction_date(text: str, filename: str | None = None) -> date:
    """Extract auction date from the report prose; fall back to filename."""
    m = RE_AUCTION_DATE.search(text)
    if m:
        mo, dy, yr = m.group(1), m.group(2), m.group(3)
        yr = int(yr)
        if yr < 100:
            yr += 2000
        return date(yr, int(mo), int(dy))

    if filename:
        fm = RE_FILENAME_DATE.search(filename)
        if fm:
            return date(int(fm.group(1)), int(fm.group(2)), int(fm.group(3)))

    raise ValueError(
        "Could not find 'Feeder Cattle Weighted Average Report for ...' line, "
        "and no filename hint provided for fallback. "
        "Pass the filename to parse() to enable the fallback path."
    )


def _parse_receipts(text: str) -> int | None:
    m = RE_RECEIPTS.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


# ---- main parser ------------------------------------------------------------

def parse(text: str, vintage: date | None = None, filename: str | None = None) -> list[Row]:
    """Parse one Era B TXT report into Rows.

    Stops at the first 'Slaughter Cows:' / 'Replacement' / 'Source:' line.
    Skips Holstein blocks if you want feeder-beef-only — but we keep them with
    breed='Holstein' so the downstream filter has fidelity.

    Pass ``filename`` (the source basename, e.g. 'CV_LS75020180118.TXT') to
    enable the filename-derived auction-date fallback when the report's
    prose marker is missing or in a variant form.
    """
    if vintage is None:
        vintage = date.today()

    auction_date = _parse_auction_date(text, filename=filename)
    receipts = _parse_receipts(text)

    rows: list[Row] = []
    in_class = False
    cur_cls = cur_frame = cur_grade = cur_breed = ""

    # Walk line by line. Track state.
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]

        if RE_END_SECTIONS.match(line):
            break  # feeder section over

        m = RE_CLASS_HEADER.match(line)
        if m:
            cur_cls = m.group("species")
            cur_frame = m.group("frame")
            cur_grade = m.group("grade")
            cur_breed = "Holstein" if m.group("breed") else "Beef"
            in_class = True
            i += 1
            # Expect column-header line right after; tolerate a blank between.
            while i < len(lines) and not RE_COLHEADER.match(lines[i]) and not lines[i].strip():
                i += 1
            if i < len(lines) and RE_COLHEADER.match(lines[i]):
                i += 1
            continue

        if in_class:
            stripped = line.strip()
            if not stripped:
                in_class = False
                i += 1
                continue
            mr = RE_DATA_ROW.match(line)
            if mr:
                wt_lo, wt_hi = _parse_range(mr.group("wt"))
                pr_lo, pr_hi = _parse_range(mr.group("price"))
                annot = mr.group("annot").strip()
                rows.append(
                    Row(
                        auction_date=auction_date,
                        commodity="Feeder Cattle",
                        cls=cur_cls,
                        frame=cur_frame,
                        muscle_grade=cur_grade,
                        weight_break_low=int(wt_lo) if wt_lo is not None else None,
                        weight_break_high=int(wt_hi) if wt_hi is not None else None,
                        avg_weight=float(mr.group("avgwt")),
                        avg_weight_min=None,
                        avg_weight_max=None,
                        head_count=int(mr.group("head")),
                        price_low=pr_lo if pr_lo is not None else 0.0,
                        price_high=pr_hi if pr_hi is not None else 0.0,
                        price_avg=float(mr.group("avgprice")),
                        receipts=receipts,
                        vintage=vintage,
                        breed=cur_breed,
                        annotation=annot,
                    )
                )
            else:
                # Non-data line inside a class block → treat as section break;
                # the next iteration will re-match a class header if there is one.
                in_class = False
        i += 1

    return rows


# ---- pilot ------------------------------------------------------------------

def _pilot_paths() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    base = repo_root / "data" / "raw" / "clovis_historical" / "mmn"
    return [
        base / "CV_LS75020171005.TXT",
        base / "CV_LS75020180118.TXT",
        base / "CV_LS75020180719.TXT",
        base / "CV_LS75020190327.TXT",
    ]


def run_pilot() -> None:
    """Parse the four pilot files and print summary + sanity rows."""
    import json

    vintage = date.today()
    grand_total = 0
    summary_lines: list[str] = []

    for p in _pilot_paths():
        text = p.read_text(encoding="utf-8", errors="replace")
        rows = parse(text, vintage=vintage)
        grand_total += len(rows)
        if not rows:
            summary_lines.append(f"{p.name:>26}: 0 rows (PARSE FAILURE?)")
            continue

        # Stats
        n = len(rows)
        receipts = rows[0].receipts
        ad = rows[0].auction_date
        breeds = sorted({r.breed for r in rows})
        grades = sorted({r.muscle_grade for r in rows})
        species = sorted({r.cls for r in rows})
        annotated = sum(1 for r in rows if r.annotation)

        summary_lines.append(
            f"{p.name:>26}: {n:>3} rows | auction_date={ad} receipts={receipts} | "
            f"species={species} grades={grades} breeds={breeds} | "
            f"{annotated} annotated lots"
        )

        # Show 2 sample rows from this file
        for r in rows[:2]:
            print(f"  [{p.name}] {json.dumps(r.to_dict(), default=str)}")

    print("\n=== PILOT SUMMARY ===")
    for s in summary_lines:
        print(s)
    print(f"\nTotal rows across 4 files: {grand_total}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "pilot":
        run_pilot()
    elif len(sys.argv) > 2 and sys.argv[1] == "parse":
        for arg in sys.argv[2:]:
            text = Path(arg).read_text(encoding="utf-8", errors="replace")
            rows = parse(text)
            print(f"{arg}: {len(rows)} rows")
            for r in rows[:3]:
                print(f"  {r.to_dict()}")
    else:
        print(__doc__)
        sys.exit(0)
