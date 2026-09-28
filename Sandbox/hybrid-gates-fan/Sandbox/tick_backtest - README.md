# Tick-by-tick framework backtest

Runs your ACTUAL framework.py, tick by tick, through real 5-minute price
bars — not a reimplementation. It imports framework.py directly and calls
its real run_sequential()/run_market() functions once per simulated tick,
with only genuinely external things faked out: IG's order API (paper
fills at the bar close, always confirmed — see "What's simulated" below),
Telegram, and the system clock.

This is deliberately NOT the fast approach. A separate, hand-written
reimplementation of the framework's logic can drift from what the real
code actually does in subtle ways — that happened repeatedly during
development (reference-timing edge cases, pre-open quadrant locking,
sequential-lock scoping). Driving the real code eliminates that class of
error entirely, at the cost of speed.

## Setup

1. Put these 7 files in one folder:
   - `run_backtest.py`, `tick_backtest.py` (this package)
   - `framework.py` (your actual framework)
   - `Fprice.csv`, `Dprice.csv`, `Cprice.csv`, `Nprice.csv`, `HSprice.csv`
     (your real price history — same files streamer.py writes to. Each
     row: `2026-08-13T09:05:00,10768.1,10772.0,10765.0,10771.5`, no header)

2. `pip install pandas`

## Running it

```
python3 run_backtest.py --start 2026-08-13 --end 2026-08-21 --out trades.csv
```

- `--start` is the first date you want recorded trades for.
- `--end` is exclusive (the day after the last day you want).
- `--out` is the CSV it writes.

Other options: `--framework`, `--data-dir` if your files aren't alongside
the script; `--lookback-days` (default 150) for how much real history to
seed for MA20/RSI/D1-D3; `--warmup-days` (default 1) — see below.

## What "warm-up" is, and why you'll see ~36 errors at the start

In real production, day_info files (each market's persisted daily state)
always already exist — they were written yesterday, and the day before
that. This script starts from nothing, so on a genuinely cold start, a
market that's in blackout before its own pre-open window has no day_info
file yet, and run_sequential errors trying to read one. That's expected,
not a bug — it resolves itself once each market's first pre-open tick
runs and creates the file, exactly like the very first day this framework
ever ran live would have. The warm-up period exists purely to get past
this before your recorded window starts, so those errors don't land in
your actual results. If you see errors persisting past the warm-up day,
that's worth investigating — that would be a real problem.

## Output CSV columns

`date, time, market, direction, entry, exit, points, size, gbp`

- `time` is the close time (when the trade finished), in whatever local
  clock convention your price CSVs use — matches `_london_now()`'s
  convention, i.e. the same as your session_open_hour etc. settings.
- `size` comes from the real `_calc_size()`. Since there's no live IG
  account to fetch a running balance from, compound-mode markets fall
  back to `cfg.account_balance` (the static config value) for every
  trade — this matches exactly what the real framework does when no
  live account_value is available, it's not a simplification unique to
  this script.

## What's simulated vs. what's real

**Real:** every price bar, the reference/quadrant/RSI/divergence logic,
entry ceilings, stops, session timing, the sequential lock, position
sizing math — all your actual framework.py code, unmodified.

**Simulated:** IG order fills always succeed immediately at the current
bar's close price. There's no slippage, no rejected orders, no partial
fills. This means the backtest tests your SIGNAL LOGIC faithfully, but
won't reproduce execution-layer issues (like the rejected-order bug we
found in your live logs) unless you're specifically comparing its output
against your real IG transaction history to spot where they diverge —
that comparison is exactly what surfaces those issues, as it did before.

## Runtime

Expect roughly 1.5-4 minutes of real computation per trading day,
depending on your machine — this is the real _load_prices() re-parsing
the growing price file every tick, which is expensive by design (it's
built for production's small daily-growing file, not for being called
thousands of times against months of backtest data in one sitting). A
month of trading days will take a while; there's no fast mode, by design
— see the intro above. A performance patch is already applied to the
date-parsing step (framework's `_parse_date` bypassed via a faster,
verified bit-identical replacement) — that's a pure speed fix with zero
behavior change, already built in, not something you need to do.

If a run is interrupted partway, there's no resume — just re-run it
for the window you still need; the scratch directory
(`--scratch`, default `./tickbt_scratch`) gets wiped and rebuilt fresh
each time you call `run_backtest.py`.
