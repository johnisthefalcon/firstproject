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

A fourth field, position_size, is written by this script as a record of the
size (£/pt) the trade was actually opened at. See the SIZE CAVEAT below
before relying on it for anything beyond record-keeping.

USAGE
-----
    python3 set_position.py <market> <LONG|SHORT> <entry_px> <size> [--best-profit N] [--trades-today N]
    python3 set_position.py <market> flat            # clear a position
    python3 set_position.py <market> --show          # print current state

    market: ftse | dax | cac | nikkei | hangseng
    size  : trade size in £/pt (required for LONG/SHORT)

EXAMPLES
--------
    # DAX long opened at 25729.1 this afternoon, £0.44/pt:
    python3 set_position.py dax LONG 25729.1 0.44

    # CAC long opened at 8489.6, £1.00/pt, already counts as today's 1st trade:
    python3 set_position.py cac LONG 8489.6 1.00 --trades-today 1

    # Clear a stuck flag:
    python3 set_position.py nikkei flat

SIZE CAVEAT
-----------
framework.py does not currently read position_size back when closing a
position. Every exit path (hard stop, RSI-divergence exit, session close,
eve force-close) sizes the close order by calling _calc_size(cfg, info)
FRESH at the moment it fires — it does not look up what size the trade was
actually opened at. So:

  * flat sizing mode: this makes no difference. _calc_size() always returns
    cfg.flat_size regardless, so open and close sizes always match.

  * compound sizing mode: open and close sizes only match if
    info["account_value"] (the balance captured at the day's session open)
    hasn't changed between when you register this position and when it
    closes. If the account_value used for THIS trade differs from
    whatever's live in day_info when the exit fires — e.g. you're
    registering a position that was actually opened yesterday, or the
    balance has moved and the framework fetched a new one at today's
    session open — the close order will be sized off the current
    account_value, not the size you pass here.

The size argument is therefore recorded for your own reference/audit trail
and printed by --show, but is NOT currently authoritative for what the
framework sends to IG at close time. If you want the close to be guaranteed
to match, that needs a change to framework.py's exit paths to prefer a
stored position_size over a freshly-computed one — ask if you want that
done.

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


def _write_position_size(path: str, size: float) -> None:
    """
    Write/replace the position_size line directly. Not handled by
    fw._save_state() (it has no size field), so this is a small
    read-modify-write on top of whatever _save_state just wrote.

    position_size is not in _save_state's `mutable` set, so once written it
    is preserved verbatim by every later _save_state call until this script
    overwrites it again — the same "sticky unless explicitly touched"
    behaviour as every other non-mutable field in the file.
    """
    with open(path) as f:
        lines = f.readlines()
    lines = [ln for ln in lines if ln.split("=")[0].strip() != "position_size"]
    lines.append(f"position_size={size:.2f}\n")
    with open(path, "w") as f:
        f.writelines(lines)


def show(market: str):
    cfg = _cfg(market)
    if not os.path.exists(cfg.day_info_path):
        sys.exit(f"No day_info file yet at {cfg.day_info_path}")
    info = fw._read_day_info(cfg.day_info_path)
    print(f"{cfg.name} — {cfg.day_info_path}")
    print(f"  trade_date   : {info.get('trade_date')}")
    print(f"  position     : {info.get('position')}")
    print(f"  entry_px     : {info.get('entry_px')}")
    print(f"  position_size: {info.get('position_size', '(not set)')}  "
          f"£/pt  [record only — see SIZE CAVEAT in --help]")
    print(f"  best_profit  : {info.get('best_profit')}")
    print(f"  trades_today : {info.get('trades_today')}")


def set_state(market, position, entry_px, best_profit, trades_today, size):
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

    # position_size isn't part of _save_state's schema — set/clear it after.
    if position is None:
        _write_position_size(path, 0.0)
    else:
        _write_position_size(path, float(size))

    action = "cleared (flat)" if position is None else f"{position} @ {entry_px}"
    print(f"✓ {cfg.name}: position {action}")
    print(f"  backup written: {path}.bak")
    print(f"  next cron tick will {'look for entries' if position is None else 'manage/close this position'}.")
    print()
    show(market)


def main():
    p = argparse.ArgumentParser(
        description="Register/clear an open position for the framework.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("market", help="ftse | dax | cac | nikkei | hangseng")
    p.add_argument("action", nargs="?", help="LONG | SHORT | flat")
    p.add_argument("entry_px", nargs="?", type=float, help="entry price (for LONG/SHORT)")
    p.add_argument("size", nargs="?", type=float,
                   help="trade size in £/pt (required for LONG/SHORT; see "
                        "SIZE CAVEAT below — record only, not read back by "
                        "the framework at close time)")
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
        set_state(args.market, None, 0.0, 0.0, args.trades_today, None)
    elif act in ("LONG", "SHORT"):
        if args.entry_px is None or args.size is None:
            sys.exit(f"{act} requires an entry price and a size, e.g. "
                     f"`python3 set_position.py {args.market} {act} 25729.1 0.44`")
        set_state(args.market, act, args.entry_px, args.best_profit,
                  args.trades_today, args.size)
    else:
        sys.exit("action must be LONG, SHORT, or flat")


if __name__ == "__main__":
    main()
