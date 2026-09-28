#!/usr/bin/env python3
"""
close_position.py — you closed a position by hand at IG; tell the framework
it's flat so the next cron tick goes back to looking for new entries instead
of trying to manage a trade that no longer exists.

This does NOT talk to IG. It only edits the local <MARKET>_day_info.txt
record. Use this after you've confirmed on the IG platform that the
position is genuinely closed — this script trusts you on that, it doesn't
check.

(Contrast with set_position.py, which does the opposite: register a
position that's open at IG but the framework doesn't know about yet.)

USAGE
-----
    python3 close_position.py <market> [--exit-price N] [--reason TEXT]
    python3 close_position.py <market> --show      # print current state, change nothing

    market: ftse | dax | cac | nikkei | hangseng

EXAMPLES
--------
    # You closed the DAX short by hand at 25012.0:
    python3 close_position.py dax --exit-price 25012.0

    # Same, with a note for the record:
    python3 close_position.py dax --exit-price 25012.0 --reason "manual close, news event"

    # Just clearing a stuck flag, no price to record:
    python3 close_position.py nikkei

What it does
------------
1. Reads the current day_info for <market>.
2. If a position is on record, prints it (direction, entry price, current
   best_profit) so you can see what you're clearing before it happens.
3. If --exit-price was given and there was a real position on record,
   computes and prints the P&L for your own reference. This number is NOT
   written anywhere — it's console output only, so it can't silently
   become a wrong "official" P&L if you mistype the exit price.
4. Writes a timestamped backup of the day_info file (.bak) before touching it.
5. Clears position to flat: position=none, entry_px=0.00, best_profit=0.00.
   trades_today is left untouched — the trade still counts toward today's
   limit, same as if the framework had closed it itself.
6. Prints a confirmation and the post-clear state.

If there was nothing on record (already flat), it says so and exits
without writing anything — running this twice by mistake is harmless.
"""

import argparse
import os
import shutil
import sys
from datetime import datetime

# Import the framework so this reuses its exact read/write logic and
# configs rather than re-implementing the day_info format here.
import types
if "trading_ig" not in sys.modules:
    _stub = types.ModuleType("trading_ig")
    class _IGService:  # placeholder, never used — this script never touches IG
        def __init__(self, *a, **k): ...
    _stub.IGService = _IGService
    sys.modules["trading_ig"] = _stub

FRAMEWORK_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, FRAMEWORK_DIR)

import framework as fw  # noqa: E402


def _cfg(market: str):
    key = market.lower()
    if key not in fw.MARKET_CONFIGS:
        valid = ", ".join(fw.MARKET_CONFIGS.keys())
        sys.exit(f"Unknown market '{market}'. Valid: {valid}")
    return fw.MARKET_CONFIGS[key]


def show(market: str):
    cfg = _cfg(market)
    if not os.path.exists(cfg.day_info_path):
        sys.exit(f"No day_info file yet at {cfg.day_info_path}")
    info = fw._read_day_info(cfg.day_info_path)
    print(f"{cfg.name} — {cfg.day_info_path}")
    print(f"  trade_date  : {info.get('trade_date')}")
    print(f"  position    : {info.get('position')}")
    print(f"  entry_px    : {info.get('entry_px')}")
    print(f"  best_profit : {info.get('best_profit')}")
    print(f"  trades_today: {info.get('trades_today')}")


def close(market: str, exit_price, reason):
    cfg = _cfg(market)
    path = cfg.day_info_path
    if not os.path.exists(path):
        sys.exit(f"No day_info file at {path}. The framework must have run at "
                 f"least once today for {cfg.name} before there's anything to clear.")

    info = fw._read_day_info(path)
    position = info.get("position")
    entry_px = float(info.get("entry_px") or 0.0)
    best_profit = info.get("best_profit")

    if not position or str(position).lower() == "none":
        print(f"{cfg.name}: already flat — nothing to close. No changes made.")
        return

    print(f"{cfg.name}: clearing recorded position")
    print(f"  was: {position} @ {entry_px:.2f}  (best_profit so far: {best_profit})")

    if exit_price is not None:
        pnl = (exit_price - entry_px) if position == "LONG" else (entry_px - exit_price)
        sign = "+" if pnl >= 0 else ""
        print(f"  exit @ {exit_price:.2f}  →  P&L = {sign}{pnl:.1f}pts  (for your reference only — not saved)")

    if reason:
        print(f"  reason: {reason}")

    # Preserve RSI history and today's trade count — only the position
    # fields change.
    rsi_c = info.get("rsi_closes", [])
    rsi_h = info.get("rsi_highs", [])
    rsi_l = info.get("rsi_lows", [])

    backup = f"{path}.{datetime.now():%Y%m%d_%H%M%S}.bak"
    shutil.copy2(path, backup)

    fw._save_state(path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)

    print(f"\n✓ {cfg.name} cleared to flat.")
    print(f"  backup written: {backup}")
    print(f"  next cron tick will look for new entries.")
    print()
    show(market)


def main():
    p = argparse.ArgumentParser(
        description="Tell the framework a position you closed manually at IG is now flat.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("market", help="ftse | dax | cac | nikkei | hangseng")
    p.add_argument("--exit-price", type=float, default=None,
                   help="price you closed at, for a P&L printout only (not saved)")
    p.add_argument("--reason", type=str, default=None,
                   help="optional note, printed to console for your own record")
    p.add_argument("--show", action="store_true", help="print current state and exit, change nothing")
    args = p.parse_args()

    if args.show:
        show(args.market)
        return

    close(args.market, args.exit_price, args.reason)


if __name__ == "__main__":
    main()
