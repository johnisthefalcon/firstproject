#!/usr/bin/env python3
"""
archive_day_info.py

Run this every 5 minutes (alongside the trading cron) to snapshot each
market's *_day_info.txt file into a running archive log.

Why: _write_day_info() OVERWRITES the day_info file in place on every
tick, so at any given moment you can only see the LATEST state. When
chasing a bug (a bad quadrant lock, a stale RSI series, a sizing that
doesn't match the log, etc.) you need to see how that file evolved over
the day, not just its current contents. This script solves that by
appending a timestamped copy of each day_info file to a per-market
archive every time it runs.

Usage:
    python3 archive_day_info.py
    python3 archive_day_info.py --project-dir /home/john/projects/firstproject
    python3 archive_day_info.py --no-dedup

Cron (every 5 minutes):
    */5 * * * * /usr/bin/python3 /home/john/projects/firstproject/archive_day_info.py >> /home/john/projects/firstproject/archive_cron.log 2>&1
"""

import argparse
import os
import sys
from datetime import datetime

# Market name -> day_info filename, matching MarketConfig.day_info_path
# in framework.py. Adjust here if a path or market set ever changes.
MARKETS = {
    "FTSE":     "FTSE_day_info.txt",
    "DAX":      "DAX_day_info.txt",
    "CAC":      "CAC_day_info.txt",
    "NIKKEI":   "NIKKEI_day_info.txt",
    "HANGSENG": "HANGSENG_day_info.txt",
}

DEFAULT_PROJECT_DIR = "/home/john/projects/firstproject"
ARCHIVE_SUFFIX = "_day_info_archive.log"
SEPARATOR = "=" * 70


def read_file(path):
    """Return the file's content, or None if it doesn't exist / can't be read."""
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return None
    except OSError as e:
        return f"__ERROR__:{e}"


def last_snapshot_body(archive_path):
    """
    Return the content of the most recent snapshot already in the archive
    (everything after its last separator block), so we can dedup against
    it. Returns None if the archive doesn't exist or has no snapshots yet.
    """
    if not os.path.exists(archive_path):
        return None
    try:
        with open(archive_path, "r") as f:
            content = f.read()
    except OSError:
        return None
    if SEPARATOR not in content:
        return None
    # Each snapshot block is: SEPARATOR, header line(s), blank line, body.
    # Split on the separator and take the last chunk; the body is
    # everything after the first blank line in that chunk.
    last_block = content.split(SEPARATOR)[-1]
    parts = last_block.split("\n\n", 1)
    return parts[1] if len(parts) == 2 else last_block


def archive_one(market, day_info_path, archive_path, dedup, now_str):
    body = read_file(day_info_path)

    if body is None:
        status = "MISSING"
        snapshot_body = f"(day_info file not found: {day_info_path})\n"
    elif body.startswith("__ERROR__:"):
        status = "ERROR"
        snapshot_body = f"(could not read day_info file: {body[len('__ERROR__:'):]})\n"
    else:
        status = "OK"
        snapshot_body = body if body.endswith("\n") else body + "\n"

    if dedup and status == "OK":
        prev = last_snapshot_body(archive_path)
        if prev is not None and prev == snapshot_body:
            print(f"[{now_str}] {market}: unchanged, skipped")
            return

    header = f"{SEPARATOR}\n[{now_str}] {market}  ({status}: {day_info_path})\n"
    with open(archive_path, "a") as f:
        f.write(header + "\n" + snapshot_body)

    print(f"[{now_str}] {market}: appended ({status})")


def main():
    ap = argparse.ArgumentParser(description="Archive day_info snapshots for bug tracking.")
    ap.add_argument("--project-dir", default=DEFAULT_PROJECT_DIR,
                     help=f"Directory containing the *_day_info.txt files (default: {DEFAULT_PROJECT_DIR})")
    ap.add_argument("--archive-dir", default=None,
                     help="Directory to write *_day_info_archive.log files into (default: same as --project-dir)")
    ap.add_argument("--no-dedup", action="store_true",
                     help="Append every run even if the day_info content hasn't changed since the last snapshot")
    args = ap.parse_args()

    archive_dir = args.archive_dir or args.project_dir
    os.makedirs(archive_dir, exist_ok=True)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for market, filename in MARKETS.items():
        day_info_path = os.path.join(args.project_dir, filename)
        archive_path = os.path.join(archive_dir, market + ARCHIVE_SUFFIX)
        try:
            archive_one(market, day_info_path, archive_path, not args.no_dedup, now_str)
        except Exception as e:
            # Never let one market's failure stop the others, and never let
            # this script crash the cron job silently.
            print(f"[{now_str}] {market}: FAILED to archive ({e})", file=sys.stderr)


if __name__ == "__main__":
    main()
