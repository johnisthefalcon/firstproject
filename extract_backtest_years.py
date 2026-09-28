#!/usr/bin/env python3
"""
extract_backtest_years.py — pull the last N years of price data out of the
long-term archives into a small, self-contained file for backtesting,
instead of loading the whole 10-year archive every time.

Reads BOTH:
    /home/john/projects/firstproject/price/longterm_<X>price10y.csv   (> RETENTION_DAYS old)
    /home/john/projects/firstproject/<X>price.csv                    (live, last RETENTION_DAYS)

and merges them so the extract is continuous right up to today, not just
whatever's in the archive — archive_prices.py deliberately keeps the last
35 days out of the archive, so the archive alone would leave a gap at the
recent end.

Writes one file per market to:
    /home/john/projects/firstproject/price/backtest_<X>price_last<N>y.csv

Market → filename-letter mapping (same as archive_prices.py):
    F = FTSE, D = DAX, C = CAC, N = Nikkei, HS = Hang Seng

USAGE
-----
    python3 extract_backtest_years.py 3                 # last 3 years, all 5 markets
    python3 extract_backtest_years.py 5 --markets F,D    # last 5 years, FTSE and DAX only
    python3 extract_backtest_years.py 1 --dry-run        # show what would happen, write nothing
    python3 extract_backtest_years.py 2 --output-dir /home/john/backtests

Output files are sorted chronologically and deduped by timestamp (if the
same timestamp somehow appears in both the archive and the live file, the
live file's row wins, since it's the more recently-written copy).
"""

import argparse
import csv
import os
from datetime import datetime, timezone

BASE_DIR    = "/home/john/projects/firstproject"
ARCHIVE_DIR = os.path.join(BASE_DIR, "price")

MARKETS = [
    # (letter, live_filename,   archive_filename)
    ("F",  "Fprice.csv",  "longterm_Fprice10y.csv"),
    ("D",  "Dprice.csv",  "longterm_Dprice10y.csv"),
    ("C",  "Cprice.csv",  "longterm_Cprice10y.csv"),
    ("N",  "Nprice.csv",  "longterm_Nprice10y.csv"),
    ("HS", "HSprice.csv", "longterm_HSprice10y.csv"),
]

TS_FORMATS = (
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%d-%m-%YT%H:%M:%S", "%d-%m-%YT%H:%M",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
)


def _parse_ts(ts_str: str):
    ts_str = ts_str.strip()
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None


def _row_ok(ts, cols):
    if ts is None or len(cols) < 4:
        return False
    try:
        for v in cols[:4]:
            float(v)
    except (TypeError, ValueError):
        return False
    return True


def _read_rows(path):
    """Returns {timestamp_str: [o,h,l,c]}. Later rows in the file win on a
    duplicate timestamp, consistent with the rest of this toolset."""
    rows = {}
    if not os.path.exists(path):
        return rows
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            ts_str = row[0]
            ts = _parse_ts(ts_str)
            cols = row[1:5]
            if _row_ok(ts, cols):
                rows[ts_str.strip()] = cols
    return rows


def _years_ago(n_years: int) -> datetime:
    """Calendar-accurate 'N years before today', true UTC. Falls back a day
    for a Feb-29 cutoff landing on a non-leap year."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        return now.replace(year=now.year - n_years)
    except ValueError:
        # today is Feb 29 and (today.year - n_years) isn't a leap year
        return now.replace(month=2, day=28, year=now.year - n_years)


def extract_market(letter, live_name, archive_name, years, output_dir, dry_run):
    live_path    = os.path.join(BASE_DIR, live_name)
    archive_path = os.path.join(ARCHIVE_DIR, archive_name)

    print(f"\n=== {letter} ({live_name}) ===")

    archive_rows = _read_rows(archive_path)
    live_rows    = _read_rows(live_path)

    if not archive_rows and not live_rows:
        print(f"    no data found in either {archive_path} or {live_path} — skipping")
        return

    merged = {**archive_rows, **live_rows}   # live wins on any overlap
    cutoff = _years_ago(years)

    kept = {ts_str: cols for ts_str, cols in merged.items()
            if (_parse_ts(ts_str) or datetime.min) >= cutoff}

    print(f"    archive rows: {len(archive_rows)}   live rows: {len(live_rows)}   "
         f"merged total: {len(merged)}")
    print(f"    cutoff ({years}y back): {cutoff:%Y-%m-%d %H:%M} UTC")
    print(f"    rows in last {years} year(s): {len(kept)}")

    if not kept:
        print(f"    nothing in range — not writing an output file")
        return

    out_name = f"backtest_{letter}price_last{years}y.csv"
    out_path = os.path.join(output_dir, out_name)

    ordered = sorted(kept.items(), key=lambda kv: _parse_ts(kv[0]) or datetime.min)
    print(f"    date range in output: {ordered[0][0]} → {ordered[-1][0]}")

    if dry_run:
        print(f"    [dry-run] would write {len(ordered)} rows to {out_path}")
        return

    os.makedirs(output_dir, exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        for ts_str, cols in ordered:
            writer.writerow([ts_str] + cols)
    os.replace(tmp, out_path)
    print(f"    wrote {len(ordered)} rows to {out_path}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("years", type=int, help="how many years back to extract, e.g. 3")
    p.add_argument("--markets", type=str, default=None,
                   help="comma-separated letters to limit to, e.g. F,D (default: all 5)")
    p.add_argument("--output-dir", type=str, default=ARCHIVE_DIR,
                   help=f"where to write the extracted files (default: {ARCHIVE_DIR})")
    p.add_argument("--dry-run", action="store_true",
                   help="show what would happen, write nothing")
    args = p.parse_args()

    if args.years <= 0:
        raise SystemExit("years must be a positive integer")

    wanted = set(args.markets.upper().split(",")) if args.markets else None
    if args.dry_run:
        print("*** DRY RUN — no files will be written ***")

    for letter, live_name, archive_name in MARKETS:
        if wanted and letter not in wanted:
            continue
        extract_market(letter, live_name, archive_name, args.years,
                       args.output_dir, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
