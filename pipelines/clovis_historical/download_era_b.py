"""Download all Era B (Oct 2017–Apr 2019) Clovis CV_LS750 TXT reports to disk.

Reads ``data/raw/clovis_historical/mmn/mmn_inventory.csv`` (built by sub-task
9.1, see PLAN_4.1 §14), filters to ``era == 'B'`` rows, and downloads each
TXT to the same directory if not already present. Idempotent.

Uses the ``requests`` library with browser-shaped headers because MMN's
Akamai WAF fingerprints curl/urllib TLS and refuses to serve the
``/filerepo/...`` paths to non-browser stacks. ``requests`` ships a
different TLS handshake that the WAF accepts.

Defaults to ``--workers 1`` because the WAF will blackhole the source IP
for hours after heavy concurrent retries (verified the hard way 2026-04-25).
A single worker takes ~5-10 min for the 72 missing files; the friction-
free path.

Usage:
    python -m pipelines.clovis_historical.download_era_b

Optional flags:
    --era A          download Era A PDFs instead (warning: 317 files, ~150 MB)
    --workers N      concurrent download threads (default 1; raise cautiously)
    --force          re-download even if file exists
    --inventory PATH alternative inventory CSV path
    --delay SECONDS  pause between fetches (default 0.5; politeness)
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INVENTORY = REPO_ROOT / "data" / "raw" / "clovis_historical" / "mmn" / "mmn_inventory.csv"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Referer": "https://mymarketnews.ams.usda.gov/viewReport/1781",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


def make_session() -> "requests.Session":
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


def fetch_one(rec: dict, out_dir: Path, force: bool, session: "requests.Session", delay: float) -> tuple[str, int, str | None]:
    """Download one report. Returns (filename, bytes, error_or_none)."""
    fn = rec["filename"]
    target = out_dir / fn
    if target.exists() and not force:
        return (fn, target.stat().st_size, None)  # skipped

    url = rec["document_url"]
    last_err = None
    for attempt in range(3):
        try:
            r = session.get(url, timeout=(10, 30))  # (connect, read)
            r.raise_for_status()
            target.write_bytes(r.content)
            if delay:
                time.sleep(delay)
            return (fn, len(r.content), None)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(2 ** attempt)  # 1, 2, 4 s backoff
    return (fn, 0, last_err)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--era", default="B", choices=["A", "B"], help="which era to download")
    ap.add_argument("--workers", type=int, default=1, help="concurrent threads (default 1)")
    ap.add_argument("--delay", type=float, default=0.5, help="seconds between fetches per worker")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--inventory", default=str(DEFAULT_INVENTORY))
    args = ap.parse_args(argv)

    inv_path = Path(args.inventory)
    if not inv_path.exists():
        print(f"ERROR: inventory not found at {inv_path}", file=sys.stderr)
        return 2

    rows = [r for r in csv.DictReader(inv_path.open()) if r["era"] == args.era]
    out_dir = inv_path.parent

    # Pre-compute skip vs. todo so the progress display is honest
    todo = [r for r in rows if not (out_dir / r["filename"]).exists() or args.force]
    skipped = len(rows) - len(todo)
    print(f"Era {args.era}: {len(rows)} files in inventory; "
          f"{skipped} already on disk (skipped); {len(todo)} to fetch.")
    if not todo:
        print("Nothing to do.")
        return 0

    print(f"Workers: {args.workers}; per-fetch delay: {args.delay}s; out_dir: {out_dir}")
    session = make_session()

    t0 = time.time()
    ok = err = 0
    errors: list[tuple[str, str]] = []

    def _worker_session():
        # Each worker gets its own session for thread safety, but they all
        # share BROWSER_HEADERS via a fresh make_session().
        return make_session()

    if args.workers == 1:
        # Sequential — easier to read progress and gentler on the WAF.
        for i, rec in enumerate(todo, 1):
            fn, sz, e = fetch_one(rec, out_dir, args.force, session, args.delay)
            if e is None:
                ok += 1
                print(f"  [{i:>3}/{len(todo)}] OK   {fn}  ({sz:>5} bytes, t={time.time()-t0:.0f}s)")
            else:
                err += 1
                errors.append((fn, e))
                print(f"  [{i:>3}/{len(todo)}] FAIL {fn}  {e}")
    else:
        # Concurrent path — careful: WAF can blackhole the source IP.
        sessions = [_worker_session() for _ in range(args.workers)]
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {
                ex.submit(fetch_one, r, out_dir, args.force,
                          sessions[i % len(sessions)], args.delay): r
                for i, r in enumerate(todo)
            }
            for i, f in enumerate(as_completed(futs), 1):
                fn, sz, e = f.result()
                if e is None:
                    ok += 1
                    print(f"  [{i:>3}/{len(todo)}] OK   {fn}  ({sz:>5} bytes)")
                else:
                    err += 1
                    errors.append((fn, e))
                    print(f"  [{i:>3}/{len(todo)}] FAIL {fn}  {e}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s: {ok} ok, {err} errors.")
    if errors:
        print("Errors:")
        for fn, e in errors[:10]:
            print(f"  {fn}: {e}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
