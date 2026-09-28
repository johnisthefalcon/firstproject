#!/usr/bin/env python3
"""
run_backtest.py — true tick-by-tick backtest of framework.py, writing a CSV of trades.

Runs the ACTUAL framework.py code (run_sequential/run_market), tick by tick,
through every real 5-min price bar — not a reimplementation. See the README
in this package for why that distinction matters.

USAGE:
    python3 run_backtest.py --start 2026-08-13 --end 2026-08-21 --out trades.csv

REQUIRED FILES (same folder as this script, unless you pass --framework / --data-dir):
    framework.py          your actual trading framework
    Fprice.csv             FTSE 5-min bars   (date,open,high,low,close — no header)
    Dprice.csv              DAX  5-min bars
    Cprice.csv              CAC  5-min bars
    Nprice.csv              Nikkei 5-min bars
    HSprice.csv             HangSeng 5-min bars

    Each CSV row: 2026-08-13T09:05:00,10768.1,10772.0,10765.0,10771.5
    (this is the same format streamer.py already writes to these files —
    just point --data-dir at wherever your real Cprice.csv etc. live, or
    copy them alongside this script.)

WHAT IT DOES:
    - Seeds ~150 days of real history per market so MA20/RSI/D1-D3 are
      genuine from tick one.
    - Adds a short warm-up period before --start so day_info files exist
      the way they always do in a running production system (a market in
      blackout has no day_info file on a truly cold start — see README).
    - Steps through every 5-min bar from warm-up start to --end, calling
      the real run_sequential() each tick, with IG/Telegram/the system
      clock faked out (paper fills at the bar close, no network calls).
    - Writes every closed trade from --start onward to the output CSV.

RUNTIME: roughly 1.5-4 minutes of computation per trading day, depending
on your machine — a month takes a while. This is real, not a bug: it's
literally re-running the production code path against history. There is
no --fast flag; see the README for why the accuracy/speed tradeoff is
deliberate.

DEPENDENCIES: pip install pandas
"""
import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta

import tick_backtest as tb


def find_prev_weekday(d):
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--start', required=True, help='First recorded date, YYYY-MM-DD')
    ap.add_argument('--end', required=True, help='Last date (exclusive), YYYY-MM-DD')
    ap.add_argument('--framework', default='./framework.py', help='Path to framework.py')
    ap.add_argument('--data-dir', default='.', help='Directory containing Fprice.csv, Dprice.csv, Cprice.csv, Nprice.csv, HSprice.csv')
    ap.add_argument('--out', default='trades.csv', help='Output CSV path')
    ap.add_argument('--scratch', default='./tickbt_scratch', help='Working directory for day_info/lock/price scratch files')
    ap.add_argument('--lookback-days', type=int, default=150, help='Calendar days of real history to seed for MA20/RSI/D1-D3')
    ap.add_argument('--warmup-days', type=int, default=1, help='Weekdays to run before --start purely so day_info files exist')
    args = ap.parse_args()

    tb.FRAMEWORK_SRC = args.framework
    tb.SCRATCH = args.scratch

    if not os.path.exists(args.framework):
        sys.exit(f"framework.py not found at {args.framework} — pass --framework /path/to/framework.py")

    source_paths = {
        'ftse': os.path.join(args.data_dir, 'Fprice.csv'),
        'dax': os.path.join(args.data_dir, 'Dprice.csv'),
        'cac': os.path.join(args.data_dir, 'Cprice.csv'),
        'nikkei': os.path.join(args.data_dir, 'Nprice.csv'),
        'hangseng': os.path.join(args.data_dir, 'HSprice.csv'),
    }
    for key, path in source_paths.items():
        if not os.path.exists(path):
            sys.exit(f"Missing price file for {key}: {path} — pass --data-dir /path/to/csvs")

    record_start = datetime.strptime(args.start, '%Y-%m-%d')
    end = datetime.strptime(args.end, '%Y-%m-%d')
    warmup_start = record_start
    for _ in range(args.warmup_days):
        warmup_start = find_prev_weekday(warmup_start)

    print(f"Warm-up:  {warmup_start.date()} -> {record_start.date()} (not recorded)")
    print(f"Recorded: {record_start.date()} -> {end.date()}")
    print(f"Seeding {args.lookback_days} days of history per market...")

    t0 = time.time()
    fwt, controller, epic_to_key = tb.build_and_seed(warmup_start, source_paths, lookback_days=args.lookback_days)
    print(f"Seeded in {time.time()-t0:.1f}s. Running warm-up...")

    ig = tb.FakeIG()
    trades, n_ticks, elapsed, errors = tb.step_range(fwt, controller, ig, warmup_start, record_start, source_paths, record_start)
    print(f"Warm-up done: {n_ticks} ticks, {elapsed:.1f}s, {len(errors)} errors (expected on a cold start — see README)")

    print(f"Running recorded window {record_start.date()} -> {end.date()}...")
    t0 = time.time()
    trades, n_ticks, elapsed, errors = tb.step_range(fwt, controller, ig, record_start, end, source_paths, record_start)
    print(f"Done: {n_ticks} ticks, {elapsed:.1f}s, {len(errors)} errors")
    if errors:
        print(f"  (first few) {errors[:5]}")

    trades.sort(key=lambda t: t['close_time'])
    with open(args.out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'time', 'market', 'direction', 'entry', 'exit', 'points', 'size', 'gbp'])
        for t in trades:
            key = epic_to_key.get(t['epic'], t['epic'])
            pts = (t['close_price'] - t['open_price']) if t['direction'] == 'BUY' else (t['open_price'] - t['close_price'])
            gbp = pts * t['size']
            ct = t['close_time']
            w.writerow([ct.strftime('%Y-%m-%d'), ct.strftime('%H:%M'), key, t['direction'],
                        t['open_price'], t['close_price'], round(pts, 1), t['size'], round(gbp, 2)])

    print(f"\nWrote {len(trades)} trades to {args.out}")
    total_gbp = sum((t['close_price']-t['open_price'] if t['direction']=='BUY' else t['open_price']-t['close_price']) * t['size'] for t in trades)
    wins = sum(1 for t in trades if ((t['close_price']-t['open_price'] if t['direction']=='BUY' else t['open_price']-t['close_price']) * t['size']) > 0)
    print(f"Net: £{total_gbp:+.2f}   Win rate: {100*wins/len(trades):.1f}%" if trades else "No trades in this window.")


if __name__ == '__main__':
    main()
