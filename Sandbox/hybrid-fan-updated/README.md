# RSI-gradient entry: framework + tick-by-tick backtest harness

## What this is

`framework.py` is your existing RSI-divergence spread-betting framework,
with a new, additional entry condition wired in: an RSI "gradient
channel" break. The rejection-wick filter and EU alignment gate are both
**disabled** in this file (not deleted — see below if you want to layer
them back in) so this is a clean test of the gradient idea on its own,
combined only with the existing RSI-divergence entry.

## The rule, as implemented

1. Identify the RSI's local peaks and troughs (turning points in the RSI
   series itself).
2. Fit **two parallel straight lines** (`y = mx + c`, same slope, two
   intercepts) to a pool of nearby peaks and troughs — which points end
   up on which line is decided by the fit itself (largest gap in sorted
   residuals), not by a fixed rule about peaks-vs-troughs. No minimum or
   maximum point count per line; one line can have many points and the
   other just one.
3. Search across a slope range centred on -0.45 (descending, bullish
   case) or +0.45 (ascending, bearish case) and keep whichever slope
   gives the best combined least-squares fit across both lines together.
4. Entry trigger: RSI breaks the channel's far line (above the upper
   line while RSI<50 for LONG, below the lower line while RSI>50 for
   SHORT) at any point within a trailing tolerance window — matching the
   existing `bull_lb`/`bear_lb` divergence lookback for consistency, not
   just the exact current bar.
5. This is combined with — not a replacement for — the existing RSI
   divergence check (`_bull_div`/`_bear_div`). Both must fire for an
   entry. The break itself is deliberately loose (any crossing counts,
   no minimum margin), since divergence is already providing the real
   selectivity.

Enabled for FTSE, DAX, CAC only (`use_rsi_gradient=True` in their
`MarketConfig` blocks). Nikkei, HangSeng, and ES are untouched.

## Two real bugs found and fixed during development — worth knowing about

1. **Slope drift.** The slope search only controls which points get
   *grouped* together into the two lines — the actual least-squares fit
   for that grouping can come out with a slope well outside the intended
   range (one real example drifted to -1.37 against a -0.65..-0.25
   target). Fixed by explicitly rejecting any fit whose recomputed slope
   falls outside the search band, in `_best_two_line_split`.
2. **Same-bar timing.** The first version required the divergence check
   and the gradient-break to fire on the *exact* same bar. Checked
   against real backtest data (not assumption): for LONG entries, the
   two conditions rarely land on the same bar even when clearly related
   to the same move — gaps of 10-28 bars were typical. Fixed with the
   trailing tolerance window described above (point 4). The SHORT side
   turned out to already coincide almost exactly on the same bar in the
   cases checked, so this fix mattered more for LONG.

Both were caught by testing against real tick-by-tick data before
trusting the mechanism, not by inspection — worth keeping that habit if
you extend this further.

## Validated results so far

Six independent weeks (March-June 2026), each with a matched control run
using the same divergence-only baseline (gate and wick both off in both
variants, isolating just the gradient's effect):

| Week | Baseline | RSI-gradient |
|---|---|---|
| 13-17 Apr | +98.2pts (12 trades) | +273.3pts (6, 83% wins) |
| 20-22 May | -242.1pts (13) | -41.0pts (8, 25% wins) |
| 16-19 Mar | -10.0pts (9) | +153.0pts (4, 75% wins) |
| 26-29 May | -157.6pts (18) | +50.3pts (9, 67% wins) |
| 30 Mar-2 Apr | -24.0pts (7) | +63.0pts (5, 40% wins) |
| 9-15 Jun | **+63.2pts (21)** | **-138.3pts (7, 29% wins)** |
| **Total** | **-272.3pts (66)** | **+360.3pts (39)** |

5 of 6 weeks favoured the gradient variant, one favoured the baseline
outright (9-15 June — worth not glossing over: this is real evidence
against "gradient always helps", not just a smaller win). Every backtest
chunk across both variants ran with zero errors. Six weeks is a real
sample but well short of the multi-year depth used to validate the
original alignment gate earlier — treat this as promising, not settled.

## Setup

1. Put these three files in one folder:
   - `framework.py` (this package — RSI-gradient entry, gate/wick off)
   - `run_backtest.py`, `tick_backtest.py` (the harness)
2. Add your own price files: `Fprice.csv`, `Dprice.csv`, `Cprice.csv`,
   `Nprice.csv`, `HSprice.csv` (same format streamer.py already writes —
   `2026-08-13T09:05:00,10768.1,10772.0,10765.0,10771.5`, no header).
   `ESprice.csv` is optional — if you don't have it, ES is automatically
   skipped (you'll see a "(skipping 'es'...)" note printed) rather than
   causing a `KeyError`. This was broken in an earlier version of
   `tick_backtest.py` — fixed and verified by reproducing the exact
   no-ES scenario before redelivering.
3. `pip install pandas`

## Running it

```
python3 run_backtest.py --start 2026-06-16 --end 2026-06-19 --out trades.csv
```

Runs your actual `framework.py` tick by tick through real 5-minute bars
— not a reimplementation — so results match exactly what the live
framework would have done. Expect roughly 60-110 seconds of real compute
per trading day; that's the genuine cost of driving the real code, not a
bug. A cold start (fresh scratch directory) will show errors for the
first ~30-40 ticks before day_info exists — expected, not a problem.
Errors continuing past that, or errors on every tick of a later day,
mean something is genuinely wrong and worth investigating before
trusting the run.

## Re-enabling the gate or wick filter, if you want to combine them

Both are disabled via commented-out code, not deleted. Search
`framework.py` for `"EU alignment gate DISABLED"` and
`require_rejection_wick=False` respectively — uncomment the relevant
blocks / flip the flags back to `True` to layer either on top of the
gradient check. Nothing about the gradient wiring depends on either
being off; they were only disabled to isolate the gradient's own effect
for this test.
