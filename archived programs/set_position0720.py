#!/usr/bin/env python3
"""
set_position.py — tell the framework a trade is already open at IG so it will
manage and eventually close it.

The framework tracks each market's position in its <MARKET>_day_info.txt via
three fields:
    position   = LONG | SHORT | none
    entry_px   = price the position was opened at
    best_profit= best favourable excursion so far (points)

Once these are set, the next cron tick sees the position and runs the
management path (hard-stop check + RSI-divergence exit) instead of looking
for a new entry.

USAGE
-----
    python3 set_position.py <market> <LONG|SHORT> <entry_px> [--best-profit N] [--trades-today N]
    python3 set_position.py <market> flat            # clear a position
    python3 set_position.py <market> --show          # print current state

    market: ftse | dax | cac | nikkei | hangseng

EXAMPLES
--------
    # DAX long opened at 25729.1 this afternoon:
    python3 set_position.py dax LONG 25729.1

    # CAC long opened at 8489.6, already counts as today's 1st trade:
    python3 set_position.py cac LONG 8489.6 --trades-today 1

    # Clear a stuck flag:
    python3 set_position.py nikkei flat

Notes
-----
* Run this on the trading server (paths point at /home/john/projects/firstproject).
* A backup of the day_info file is written alongside it (.bak) before any change.
* best_profit defaults to 0.0. If the trade is already in profit you may set it
  so the RSI-divergence exit (which only arms once best_profit >= min_profit)
  behaves as if the move already happened; leaving it 0.0 is the safe default.
* This does NOT talk to IG. It only aligns the framework's record with reality.
  Make sure the position really is open at IG first.
"""

import argparse
import os
import shutil
import sys

# Import the framework so we reuse its exact read/write logic and configs.
# Stub trading_ig so the module imports without the broker library present.
import types
if "trading_ig" not in sys.modules:
    _stub = types.ModuleType("trading_ig")
    class _IGService:  # placeholder, never used here
        def __init__(self, *a, **k): ...
    _stub.IGService = _IGService
    sys.modules["trading_ig"] = _stub

# Adjust this path if the script isn't next to framework.py
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


def set_state(market, position, entry_px, best_profit, trades_today):
    cfg = _cfg(market)
    path = cfg.day_info_path
    if not os.path.exists(path):
        sys.exit(f"No day_info file at {path}. The framework must have run at "
                 f"least once today for {cfg.name} before you can set a position.")

    info = fw._read_day_info(path)

    # Preserve the existing RSI history and today's trade count.
    rsi_c = info.get("rsi_closes", [])
    rsi_h = info.get("rsi_highs", [])
    rsi_l = info.get("rsi_lows", [])
    if trades_today is not None:
        info["trades_today"] = int(trades_today)

    # Backup before writing.
    shutil.copy2(path, path + ".bak")

    fw._save_state(path, info, position, float(entry_px or 0.0),
                   float(best_profit or 0.0), rsi_c, rsi_h, rsi_l)

    action = "cleared (flat)" if position is None else f"{position} @ {entry_px}"
    print(f"✓ {cfg.name}: position {action}")
    print(f"  backup written: {path}.bak")
    print(f"  next cron tick will {'look for entries' if position is None else 'manage/close this position'}.")
    print()
    show(market)


def main():
    p = argparse.ArgumentParser(description="Register/clear an open position for the framework.")
    p.add_argument("market", help="ftse | dax | cac | nikkei | hangseng")
    p.add_argument("action", nargs="?", help="LONG | SHORT | flat")
    p.add_argument("entry_px", nargs="?", type=float, help="entry price (for LONG/SHORT)")
    p.add_argument("--best-profit", type=float, default=0.0,
                   help="best favourable excursion in points (default 0.0)")
    p.add_argument("--trades-today", type=int, default=None,
                   help="override today's trade count (e.g. 1)")
    p.add_argument("--show", action="store_true", help="print current state and exit")
    args = p.parse_args()

    if args.show or args.action is None:
        show(args.market)
        return

    act = args.action.upper()
    if act == "FLAT":
        set_state(args.market, None, 0.0, 0.0, args.trades_today)
    elif act in ("LONG", "SHORT"):
        if args.entry_px is None:
            sys.exit(f"{act} requires an entry price, e.g. "
                     f"`python3 set_position.py {args.market} {act} 25729.1`")
        set_state(args.market, act, args.entry_px, args.best_profit, args.trades_today)
    else:
        sys.exit("action must be LONG, SHORT, or flat")


if __name__ == "__main__":
    main()
