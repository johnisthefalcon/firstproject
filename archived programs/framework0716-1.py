"""
RSI Divergence Framework — Multi-Market Cron Script
====================================================
Supports FTSE UK100, DAX Germany 40, CAC 40, Nikkei 225, Hang Seng.
One file, one cron job, any combination of markets.

CRON SETUP
──────────
Run all five markets in session-aware sequential mode (recommended):
    */5 * * * * . /home/john/projects/firstproject/.env && \
      /home/john/projects/firstproject/venv/bin/python \
      /home/john/projects/firstproject/framework.py \
      --sequential ftse dax cac nikkei hangseng \
      >> /home/john/projects/firstproject/trading.log 2>&1

Run European markets only:
    */5 * * * * ... framework.py --sequential ftse dax cac >> trading.log 2>&1

Run a single market:
    */5 * * * * ... framework.py ftse >> ftse.log 2>&1

HOW IT WORKS
────────────
Each cron tick the script:
  1. Reads London time (European) or UTC time (Asian) from the system clock.
  2. BLACKOUT: per-market blackout windows exit silently.
     European: 21:00-02:59 UTC  |  Nikkei: 07:00-18:59 UTC  |  HS: 08:30-19:59 UTC
  3. For each requested market:
     a. If the day-info file is missing or stale, runs daily setup.
     b. Reads the latest bar from that market's prices CSV.
     c. PRE-OPEN: RSI updated, quadrant recalculated from current close.
     d. SESSION OPEN: quadrant locked from the bar open.
     e. SESSION: full entry, exit and stop logic.
     f. Calls BUY_<MARKET> or SELL_<MARKET> when a signal fires.

SESSION-AWARE SEQUENTIAL MODE
──────────────────────────────
Nikkei (00:00-06:30 UTC) and Hang Seng (01:30-08:00 UTC) are in the
"asian" session group. FTSE/DAX/CAC are in the "european" group.
Both groups can take one trade on the same calendar day without conflict
— their sessions don't overlap. Within each group, only one market
trades at a time (strongest divergence signal wins).

CSV FORMAT:
    date,open,high,low,close,eve_close
    2026-05-06,10295,10340,10290,10336,10358

DAY INFO FILES  (one per market, auto-created):
    FTSE_day_info.txt  NIKKEI_day_info.txt
    DAX_day_info.txt   HANGSENG_day_info.txt
    CAC_day_info.txt
"""

import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from trading_ig import IGService



# ══════════════════════════════════════════════════════════════════
# MARKET CONFIGURATION
# ══════════════════════════════════════════════════════════════════

@dataclass
class MarketConfig:
    """All market-specific settings in one place."""
    name          : str
    data_path     : str    # path to this market's prices CSV
    day_info_path : str    # path to this market's _day_info.txt
    # Stops and zones — scale with the market's price level
    sl_long       : float = 30.0
    sl_short      : float = 35.0
    golden_rule   : float = 10.0   # max entry above D-1 eve close
    div_zone      : float = 5.0    # ±pts around reference level (short)
    min_profit    : float = 5.0    # min profit before exit div fires
    min_price_gap : float = 3.0    # min price gap for divergence
    # Session close — position is force-closed at this time each day.
    # Set to 5 minutes BEFORE the actual cash close so the order
    # executes within the open session (e.g. 16:25 for a 16:30 close).
   
    session_close_hour   : int = 16
    session_close_minute : int = 25
    # ── POSITION SIZING ─────────────────────────────────────────
    # Two modes — set sizing_mode to choose:
    #
    #   "flat"     : fixed £/pt on every trade  (use flat_size)
    #   "compound" : fraction of account equity (use compound_fraction)
    #                size = account_balance / compound_fraction
    #                e.g. compound_fraction=500 → 1/500th of account
    #
    # For compound mode, set account_balance to your current account value.
    # Update it periodically (or wire it to your broker's balance API).
    sizing_mode        : str   = "flat"   # "flat" or "compound"
    flat_size          : float = 0.1  # £ per point (flat mode)
    compound_fraction  : float = 500.0    # account divisor (compound mode)
    account_balance    : float = 10000.0  # current account value (compound mode)
    # RSI constants
    rsi_period    : int   = 14
    ma_period     : int   = 20     #standard is 20 day moving average
    bull_lb       : int   = 12     # 12 bullish divergence lookback (bars)
    bear_lb       : int   = 10     # 10 bearish divergence lookback (bars)
    min_rsi_gap   : float = 3.0


    # ── Per-market session timing (UTC hours) ────────────────────
    # Defaults match existing EU behaviour. Override for Asian markets.
    session_open_hour   : int = 8      # hour session opens (UTC)
    session_open_minute : int = 0    # minute session opens (default 0 = top of hour)
    pre_open_start_hour : int = 6      # hour pre-open begins (UTC)
    blackout_start_hour : int = 21     # hour blackout begins (UTC)
    blackout_end_hour   : int = 6      # hour blackout ends (exclusive)
    # ── Per-market CSV session filter times ──────────────────────
    csv_session_start : str = "08:00"  # first bar included in session
    csv_session_end   : str = "16:30"  # last bar included in session
    csv_eve_close_t   : str = "20:55"  # time of overnight/eve close bar
    # ── Session group for session-aware sequential ───────────────
    # "european" = 08:00-17:00 UTC   "asian" = 00:00-08:00 UTC
    # Markets in different groups can trade on the same calendar day
    # without conflict — their sessions don't overlap.
    session_group : str = "european"
    epic          : str = ""           # IG epic for position verification and trade execution



# ── Market definitions — edit paths and parameters to match your setup ──
MARKET_CONFIGS: dict[str, MarketConfig] = {
    "ftse": MarketConfig(
        name          = "FTSE",
        data_path     = "/home/john/projects/firstproject/Fprice.csv",
        day_info_path = "/home/john/projects/firstproject/FTSE_day_info.txt",
        sl_long=30.0, 
        sl_short=35.0, 
        golden_rule=10.0, 
        div_zone=3.0,
        min_profit=5.0, 
        min_price_gap=3.0,
        session_close_hour=16, 
        session_close_minute=25,
        sizing_mode="flat", 
        flat_size=0.7,
        session_open_hour=8, 
        epic="IX.D.FTSE.DAILY.IP",
        pre_open_start_hour=6,
        blackout_start_hour=21, 
        blackout_end_hour=6,
        csv_session_start="08:00", 
        csv_session_end="16:30",
        csv_eve_close_t="20:55", session_group="european",
   
    ),
    "dax": MarketConfig(
        name          = "DAX",
        data_path     = "/home/john/projects/firstproject/Dprice.csv",
        day_info_path = "/home/john/projects/firstproject/DAX_day_info.txt",
        # DAX trades at ~2x FTSE price — stops and zones scaled accordingly
        sl_long       = 60.0,
        sl_short      = 70.0,
        golden_rule   = 20.0,
        div_zone      = 25.0,
        min_profit    = 10.0,
        min_price_gap = 5.0,
        session_close_hour   = 16,   # force-close at 16:25 (DAX cash close 17:00 Frankfurt / 16:00 UTC)
        session_close_minute = 25,
        # Sizing — choose one mode:
        # Note: DAX equivalent of £10/pt FTSE = £22.56/pt DAX (ratio 8200/18500)
        # Set flat_size to your desired £/pt on the DAX instrument directly.
        sizing_mode       = "flat",  # "flat" or "compound"
        flat_size         = 0.3,    # £ per point on DAX (adjust to match FTSE exposure)
        compound_fraction = 500.0,   # 1/500th of account
        account_balance   = 10000.0, # update to your actual balance
        session_open_hour=8, 
        epic="IX.D.DAX.DAILY.IP",
        pre_open_start_hour=6,
        blackout_start_hour=21, 
        blackout_end_hour=6,
        csv_session_start="08:00", 
        csv_eve_close_t="20:55", 
        session_group="european",

    ),
    "cac": MarketConfig(
        name          = "CAC",
        data_path     = "/home/john/projects/firstproject/Cprice.csv",
        day_info_path = "/home/john/projects/firstproject/CAC_day_info.txt",
        # CAC similar price level to FTSE — same stops and zones
        sl_long       = 30.0,
        sl_short      = 35.0,
        golden_rule   = 10.0,
        div_zone      = 5.0,
        min_profit    = 5.0,
        min_price_gap = 3.0,
        session_close_hour   = 16,   # force-close at 16:25 (CAC cash close 17:30 Paris)
        session_close_minute = 25,
        sizing_mode       = "flat",
        flat_size         = 1,
        compound_fraction = 500.0,
        account_balance   = 10000.0,
        session_open_hour=8,
        epic="IX.D.CAC.DAILY.IP",
        pre_open_start_hour=7,
        blackout_start_hour=21, 
        blackout_end_hour=7,
        csv_session_start="08:00", 
        csv_session_end="16:30",
        csv_eve_close_t="20:55", 
        session_group="european",

    ),
  
    "nikkei": MarketConfig(
        name          = "NIKKEI",
        data_path     = "/home/john/projects/firstproject/Nprice.csv",
        day_info_path = "/home/john/projects/firstproject/NIKKEI_day_info.txt",
        # Nikkei ~40,000 pts — all stops/zones scaled ~5x FTSE
        sl_long=150.0, 
        sl_short=175.0, 
        golden_rule=50.0, 
        div_zone=25.0,
        min_profit=25.0, 
        min_price_gap=15.0,
        # Force-close at 06:25 UTC = 15:25 JST (cash closes 15:30 JST)
        session_close_hour=6, 
        session_close_minute=25,  # £2/pt ≈ £10/pt FTSE-equiv (40000/8200 ratio)
        sizing_mode= "flat", 
        flat_size = 0.10,
        epic="IX.D.NIKKEI.DAILY.IP",
        compound_fraction = 500.0,
        account_balance   = 10000.0,
        # Nikkei session (all UTC)
        session_open_hour=0,
             # Tokyo open 00:00 UTC (09:00 JST)
        pre_open_start_hour=19,  # Pre-open 19:00 UTC (04:00 JST next day)
        blackout_start_hour=7,   # Blackout 07:00–18:59 UTC
        blackout_end_hour=19,
        csv_session_start="00:00",
        csv_session_end="06:30",
        csv_eve_close_t="14:00",  # US close = 23:00 JST = 14:00 UTC
        session_group="asian",
    ),
    "hangseng": MarketConfig(
        name          = "HANGSENG",
        data_path     = "/home/john/projects/firstproject/HSprice.csv",
        day_info_path = "/home/john/projects/firstproject/HANGSENG_day_info.txt",
        # Hang Seng ~20,000 pts — stops/zones ~2.5x FTSE
        sl_long=100.0, 
        sl_short=120.0, 
        golden_rule=40.0, 
        div_zone=20.0,
        min_profit=20.0, 
        min_price_gap=10.0,
        # Force-close at 07:55 UTC = 15:55 HKT (cash closes 16:00 HKT)
        session_close_hour=6, 
        session_close_minute=55,
        # £4/pt ≈ £10/pt FTSE-equiv (20000/8200 ratio)
        sizing_mode="flat", 
        flat_size=0.15,
        epic="IX.D.HANGSENG.DAILY.IP",
        compound_fraction = 500.0,
        account_balance   = 10000.0,
        # HK session (all UTC)
        session_open_hour=1, 
        session_open_minute = 30,   # ← 01:30 UTC    # HK cash open 01:30 UTC (09:30 HKT)
        pre_open_start_hour=20,  # Pre-open 20:00 UTC (04:00 HKT next day)
        blackout_start_hour=8,   # Blackout 08:30–19:59 UTC
        blackout_end_hour=20,
        csv_session_start="01:30",
        csv_session_end="08:00",
        csv_eve_close_t="14:00",  # US close = 22:00 HKT = 14:00 UTC
        session_group="asian",
    ),

}

# ── Global settings ────────────────────────────────────────────────
LOG_ENABLED    = True

# ── Sequential mode: shared lock file ───────────────────────────────
# Stores which market currently holds the position in sequential mode.
SEQUENTIAL_LOCK = "/home/john/projects/firstproject/market_lock.txt"
MAX_TRADES_PER_DAY = 3
# Entries are refused when the newest CSV bar is older than this (minutes).
# Managing/closing an existing position is always allowed regardless.
STALE_FEED_MAX_MIN = 15
# When IG reports the historical-data allowance is exhausted, back off:
# marker file + duration. While the marker is fresh, fetches drop to
# num_points=1 and failure telegrams are suppressed (one alert per episode).
ALLOWANCE_BACKOFF_FILE = "/home/john/projects/firstproject/.ig_data_backoff"
ALLOWANCE_BACKOFF_MIN  = 60
# ── Time windows (London time, same for all markets) ──────────────
BLACKOUT_START = 21
BLACKOUT_END   =  3
PRE_OPEN_START =  3
SESSION_OPEN   =  8


# ══════════════════════════════════════════════════════════════════
# BROKER INTEGRATION  — log, telegram, and place trade with IG
# ══════════════════════════════════════════════════════════════════

def _place_and_notify(name: str, side: str, price: float, stop: float,
                      notes: str, ig, cfg, price_fmt: str = ".1f") -> bool:
    """
    Place an order at IG and notify Telegram based on the ACTUAL result.

    Critical: the confirmation Telegram and the caller's "success" both depend
    on the broker's return value. Previously the wrappers returned None and
    telegrammed a BUY/SELL confirmation *before* the order was attempted, so:
      * `success` was always None → the entry handler always logged
        "order failed — state not saved" even on a real fill, leaving the
        framework flat while IG held the position, and
      * a confirmation Telegram was sent even when the order failed.
    Both are fixed here: place first, then notify/return on the real outcome.

    Returns True only when IG returns a deal reference.
    """
    px = format(price, price_fmt)
    st = format(stop,  price_fmt)
    _log(name, f"[{side}]  @ {px}  stop={st}  | {notes}")

    # No live session (backtest / replay / monitoring): treat as a paper fill
    # so state tracking still works, but don't hit the network.
    if not (ig and cfg):
        telegram_bitbot(f"{name} | {side} | {px} | stop={st} | {notes}")
        return True

    open_fn = open_buy_trade if side == "BUY" else open_sell_trade
    stop_dist = cfg.sl_long if side == "BUY" else cfg.sl_short
    deal_ref = open_fn(ig, epic=cfg.epic, size=cfg.flat_size, stop_distance=stop_dist)

    if deal_ref:
        telegram_bitbot(f"{name} | {side} | {px} | stop={st} | {notes}")
        return True
    # Failure path: the entry handler will log the error + alert and stay flat.
    return False


def BUY_FTSE(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("FTSE", "BUY", price, stop, notes, ig, cfg, ".1f")

def SELL_FTSE(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("FTSE", "SELL", price, stop, notes, ig, cfg, ".1f")

def BUY_DAX(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("DAX", "BUY", price, stop, notes, ig, cfg, ".1f")

def SELL_DAX(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("DAX", "SELL", price, stop, notes, ig, cfg, ".1f")

def BUY_CAC(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("CAC", "BUY", price, stop, notes, ig, cfg, ".1f")

def SELL_CAC(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("CAC", "SELL", price, stop, notes, ig, cfg, ".1f")

def BUY_NIKKEI(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("NIKKEI", "BUY", price, stop, notes, ig, cfg, ".0f")

def SELL_NIKKEI(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("NIKKEI", "SELL", price, stop, notes, ig, cfg, ".0f")

def BUY_HANGSENG(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("HANGSENG", "BUY", price, stop, notes, ig, cfg, ".0f")

def SELL_HANGSENG(price, stop, notes="", ig=None, cfg=None) -> bool:
    return _place_and_notify("HANGSENG", "SELL", price, stop, notes, ig, cfg, ".0f")

# Maps market name → (buy_fn, sell_fn)
_BROKER: dict[str, tuple] = {
    "FTSE": (BUY_FTSE,  SELL_FTSE),
    "DAX":  (BUY_DAX,   SELL_DAX),
    "CAC":  (BUY_CAC,   SELL_CAC),
    "NIKKEI":   (BUY_NIKKEI,   SELL_NIKKEI),
    "HANGSENG": (BUY_HANGSENG, SELL_HANGSENG),
}

def _close_position(ig_service, cfg: MarketConfig, reason: str,
                    price: float, entry_px: float,
                    position: str, info: dict,
                    rsi_c: list, rsi_h: list, rsi_l: list) -> bool:
    """
    Verify a position is actually open at IG before closing it.
    Returns True if close was executed (or skipped safely).
    Logs and telegrams either way.
    """
    pnl = (price - entry_px) if position == "LONG" else (entry_px - price)

    # Check IG before closing
    open_count = get_open_positions(ig_service, cfg.epic)

    if open_count == 0:
        _log("IG", "open count =0")  
        # Position already closed at IG (manual close, stop hit at broker, etc.)
        msg = (f"[{reason}] {cfg.name} {position} @ {entry_px:.1f} "
               f"already closed at IG — skipping close | "
               f"last_price={price:.1f} | P&L≈{pnl:+.1f}pts")
        _log(cfg.name, msg)
        telegram_bitbot(msg)
        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
        return True   # treated as closed — state cleared
   
        
    # Position confirmed open (or API error returned -1) — proceed with close
    _log("IG", "open count = -1")    
    return False      # caller should execute the close normally

def _calc_size(cfg: MarketConfig, info: dict = None) -> float:
    """
    Calculate the position size (£ per point) for this market.

    flat mode:     returns cfg.flat_size directly.
    compound mode: derives size from the live account value fetched at the
                   session open (08:00) and stored in the day_info as
                   account_value. Falls back to cfg.account_balance if no
                   live value has been stored yet.
                   size = account_value / compound_fraction

    The returned value is passed to BUY_*/SELL_* so you can use it
    inside those functions to size the order with your broker API.
    """
    if cfg.sizing_mode == "compound":
        balance = cfg.account_balance   # fallback
        if info is not None:
            live = float(info.get("account_value", 0) or 0)
            if live > 0:
                balance = live
        return round(balance / cfg.compound_fraction, 4)
    return cfg.flat_size  # flat mode


# ══════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════

def _log(market: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts}  [{market}]  {msg}")

def _debug(market: str, msg: str) -> None:
    if LOG_ENABLED: _log(market, msg)


# ══════════════════════════════════════════════════════════════════
# TIME
# ══════════════════════════════════════════════════════════════════

def _london_now() -> datetime:
    try:
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo("Europe/London")).replace(tzinfo=None)
    except ImportError:
        utc = datetime.now(timezone.utc)
        return utc.replace(tzinfo=None) + timedelta(hours=1 if 3 < utc.month < 11 else 0)

def _in_blackout(hour: int, cfg: MarketConfig) -> bool:
    """Per-market blackout. Handles both midnight-wrapping and same-day ranges."""
    s = cfg.blackout_start_hour
    e = cfg.blackout_end_hour
    if s > e:       # wraps midnight e.g. 21:00–03:00
        return hour >= s or hour < e
    else:           # same-day range e.g. 07:00–19:00 (Nikkei/HS)
        return s <= hour < e


# ══════════════════════════════════════════════════════════════════
# MATHS
# ══════════════════════════════════════════════════════════════════

def _calc_rsi(closes: list, period: int = 14) -> list:
    if len(closes) < period + 1:
        return [None] * len(closes)
    result = [None] * period
    g = [max(closes[i]-closes[i-1], 0) for i in range(1, period+1)]
    ls = [max(closes[i-1]-closes[i], 0) for i in range(1, period+1)]
    ag, al = sum(g)/period, sum(ls)/period
    for i in range(period, len(closes)):
        if i > period:
            d = closes[i] - closes[i-1]
            ag = (ag*(period-1) + max(d,  0)) / period
            al = (al*(period-1) + max(-d, 0)) / period
        result.append(100 if al == 0 else 100 - (100/(1 + ag/al)))
    return result


def _bull_div(lows: list, rsis: list, cfg: MarketConfig,
              lb: Optional[int] = None) -> tuple[bool, float]:
    lb = lb or cfg.bull_lb
    i  = len(lows) - 1
    if i < 4 or rsis[i] is None or rsis[i] > 60: return False, 0.0
    best = 0.0; found = False
    for j in range(max(0, i-lb), i-2):
        if rsis[j] is None: continue
        if lows[i] < lows[j] - cfg.min_price_gap:
            gap = rsis[i] - rsis[j]
            if gap >= cfg.min_rsi_gap and rsis[j] <= 55:
                found = True; best = max(best, gap)
    return found, best


def _bear_div(highs: list, rsis: list, cfg: MarketConfig,
              lb: Optional[int] = None) -> tuple[bool, float]:
    lb = lb or cfg.bear_lb
    i  = len(highs) - 1
    if i < 4 or rsis[i] is None or rsis[i] < 45: return False, 0.0
    best = 0.0; found = False
    for j in range(max(0, i-lb), i-2):
        if rsis[j] is None: continue
        if highs[i] >= highs[j] - cfg.min_price_gap:
            gap = rsis[j] - rsis[i]
            if gap >= cfg.min_rsi_gap and rsis[j] >= 50:
                found = True; best = max(best, gap)
    return found, best


def _bull_div_exit(lows: list, rsis: list, cfg: MarketConfig) -> bool:
    i = len(lows) - 1
    if i < 4 or rsis[i] is None or rsis[i] > 60: return False
    for j in range(max(0, i-8), i-2):
        if rsis[j] is None: continue
        if lows[i] < lows[j] - cfg.min_price_gap:
            if rsis[i]-rsis[j] >= cfg.min_rsi_gap and rsis[j] <= 55: return True
    return False


def _bear_div_exit(highs: list, rsis: list, cfg: MarketConfig) -> bool:
    i = len(highs) - 1
    if i < 4 or rsis[i] is None or rsis[i] < 45: return False
    for j in range(max(0, i-8), i-2):
        if rsis[j] is None: continue
        if highs[i] >= highs[j] - cfg.min_price_gap:
            if rsis[j]-rsis[i] >= cfg.min_rsi_gap and rsis[j] >= 50: return True
    return False


def _quadrant(price: float, d1_uk: float, d1_eve: float) -> str:
    ep  = (d1_eve - d1_uk) > 0
    evp = (price  - d1_eve) > 0
    up  = (price  - d1_uk)  > 0
    if   ep  and  evp and  up:          return "E+Drift+UK+"
    elif ep  and not evp and  up:       return "E+Drift-UK+"
    elif ep  and not evp and not up:    return "E+Drift-UK-"
    elif ep  and  evp and not up:       return "E+Drift+UK-"
    elif not ep and  evp and  up:       return "E-Drift+UK+"
    elif not ep and  evp and not up:    return "E-Drift+UK-"
    elif not ep and not evp and not up: return "E-Drift-UK-"
    else:                               return "E-Drift-UK+"


# ══════════════════════════════════════════════════════════════════
# CSV LOADING
# ══════════════════════════════════════════════════════════════════

def _parse_date(s: str) -> datetime:
    s = s.strip().split("T")[0]   # strip time component if present
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(s, fmt)
        except ValueError: pass
    raise ValueError(f"Cannot parse date: {s!r}")

def _prev_day(d: datetime, n: int = 1) -> datetime:
    for _ in range(n):
        d -= timedelta(days=1)
        while d.weekday() >= 5: d -= timedelta(days=1)
    return d

def _eve(bar: dict) -> float:
    v = bar.get("eve_close")
    if v is not None and float(v) > 0: return float(v)
    return float(bar["close"])


def _load_prices(path: str, cfg: MarketConfig) -> dict:
    # Aggregate 5-min bars into daily OHLC
    # open  = first bar at or after 08:00
    # high  = highest high of the day
    # low   = lowest low of the day
    # close = last close at or before 16:30 (UK session close)
    # eve_close = last close at or before 20:55 (US session close)
    from collections import defaultdict
    bars = defaultdict(list)   # date → list of (time_str, o, h, l, c)

    with open(path, newline="") as f:
        sample = f.read(512); f.seek(0)
        try:
            has_hdr = not sample.split("\n")[0].split(",")[1].replace(".", "").replace("-","").isdigit()
        except IndexError:
            has_hdr = True
        reader = csv.reader(f)
        if has_hdr: next(reader)
        # Skip any further non-data rows (e.g. second header row like 'DateTime,,,,')
        for row in reader:
            if len(row) >= 5 and 'T' in row[0] and any(c.isdigit() for c in row[0]):
                # First real data row — put it back by processing it now
                try:
                    raw = row[0].strip(); parts = raw.split("T")
                    time_str = parts[1].strip()[:5].zfill(5) if len(parts) > 1 else "00:00"
                    dt = _parse_date(parts[0])
                    o,h,l,c = float(row[1]),float(row[2]),float(row[3]),float(row[4])
                    bars[dt].append((time_str, o, h, l, c))
                except (ValueError, IndexError): pass
                break  # stop header-skipping, fall through to main loop
        for row in reader:  # continue from here
            if len(row) < 5: continue
            try:
                raw   = row[0].strip()
                parts = raw.split("T")
                date_str = parts[0]
                time_str = parts[1].strip()[:5].zfill(5) if len(parts) > 1 else "00:00"
                dt   = _parse_date(date_str)
                o,h,l,c = float(row[1]),float(row[2]),float(row[3]),float(row[4])
                bars[dt].append((time_str, o, h, l, c))
            except (ValueError, IndexError):
                continue

    prices = {}
    for dt, day_bars in bars.items():
        if dt.weekday() >= 5: continue          # skip weekends
        day_bars.sort(key=lambda x: x[0])       # sort by time
        sess  = [b for b in day_bars if cfg.csv_session_start <= b[0] <= cfg.csv_session_end]
        if not sess: continue
        open_p  = sess[0][1]
  
        high_p  = max(b[2] for b in sess)
        low_p   = min(b[3] for b in sess)
        close_p  = sess[-1][4]                  # last bar at/before session end
        last_bar = day_bars[-1]                 # the most recent 5-min bar overall
                                                  # (may be after session close — used
                                                  # for live P&L tracking on positions
                                                  # held past cash close)
        # Eve close: last bar at or before eve_close time
        eve_bars = [b for b in day_bars if b[0] <= cfg.csv_eve_close_t]
        eve_p    = eve_bars[-1][4] if eve_bars else close_p
        prices[dt] = {"open": open_p, "high": high_p, "low": low_p,
                      "close": close_p, "eve_close": eve_p,
                      # Last individual 5-min bar — used for intraday stop/exit
                      # checks so we don't trigger stops on earlier session lows
                      "bar_high":  last_bar[2],
                      "bar_low":   last_bar[3],
                      "bar_open":  last_bar[1],
                      "bar_close": last_bar[4],
                      # Time ("HH:MM") of that last bar, and the full sorted
                      # intraday bar list [(time,o,h,l,c),...] for this date.
                      # Used by run_market's feed-gap handling: bar_time lets
                      # it detect a stale feed (same bar seen twice) or a gap
                      # (missing slots after an outage), and intraday lets it
                      # rebuild the day's RSI arrays from the backfilled CSV
                      # instead of trusting arrays poisoned during the outage.
                      "bar_time":  last_bar[0],
                      "intraday":  day_bars}
    return prices

def _build_ma20(prices: dict, period: int) -> dict:
    days = sorted(prices); closes = [prices[d]["close"] for d in days]; ma = {}
    for i, d in enumerate(days):
        if i >= period: ma[d] = sum(closes[i-period:i]) / period
    return ma


def _ref_levels(prices: dict, trade_date: datetime) -> Optional[dict]:
    sd1=_prev_day(trade_date,1); sd2=_prev_day(trade_date,2); sd3=_prev_day(trade_date,3)
    if sd1 not in prices or sd2 not in prices or sd3 not in prices: return None
    return {"d1_uk":prices[sd1]["close"],"d1_eve":_eve(prices[sd1]),
            "d2_uk":prices[sd2]["close"],"d2_eve":_eve(prices[sd2]),
            "d3_uk":prices[sd3]["close"]}


# ══════════════════════════════════════════════════════════════════
# DAY INFO FILE
# ══════════════════════════════════════════════════════════════════

def _write_day_info(path: str, info: dict, cfg: MarketConfig) -> None:
    d  = info["trade_date"]
    
    lines = [
        f"# {cfg.name} Framework — Daily Setup",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"# Trade date: {info['trade_date'].strftime('%d/%m/%Y')}\n",
        "",
        f"trade_date={d.strftime('%d/%m/%Y')}",
        f"d1_uk={info['d1_uk']:.2f}",
        f"d1_eve={info['d1_eve']:.2f}",
        f"d2_uk={info['d2_uk']:.2f}",
        f"d2_eve={info['d2_eve']:.2f}",
        f"d3_uk={info['d3_uk']:.2f}",
        f"ma20={info['ma20']:.2f}",
        f"above_ma={int(info['above_ma'])}",
        f"open_price={info['open_price']:.2f}",
        f"quadrant={info['quadrant']}",
        f"s1={info['s1']:.2f}",
        f"s2b={info['s2b']:.2f}",
        f"s2a={info['s2a']:.2f}",
        f"entry_ceiling={info['entry_ceiling']:.2f}",
      
       
        f"long_valid={int(info['long_valid'])}",
        f"e_drift_uk_minus_exception={int(info['e_drift_uk_minus_exception'])}",
        f"e_drift_uk_minus_trigger={info['e_drift_uk_minus_trigger']:.2f}",
        f"short_valid={int(info['short_valid'])}",
        f"short_levels={','.join(f'{v:.2f}' for v in info['short_levels'])}",
        f"t1={info['t1']:.2f}",
        f"t2={info['t2']:.2f}",
        f"t3={info['t3']:.2f}",
        f"position={info.get('position') or 'none'}",
        f"entry_px={info.get('entry_px', 0.0):.2f}",
        f"best_profit={info.get('best_profit', 0.0):.2f}",
        f"trades_today={int(info.get('trades_today', 0))}",
        f"account_value={float(info.get('account_value', 0.0)):.2f}",
        f"last_rsi_bar={info.get('last_rsi_bar', '') or ''}",
        f"rsi_closes={','.join(f'{v:.2f}' for v in info.get('rsi_closes', []))}",
        f"rsi_highs={','.join(f'{v:.2f}' for v in info.get('rsi_highs', []))}",
        f"rsi_lows={','.join(f'{v:.2f}' for v in info.get('rsi_lows', []))}",
    ]
    with open(path, "w") as f: f.write("\n".join(lines) + "\n")


def _read_day_info(path: str) -> dict:
    info = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            key, _, val = line.partition("=")
            key = key.strip(); val = val.strip()
            if key == "trade_date": info[key] = _parse_date(val)
            elif key in ("above_ma","long_valid","short_valid",
                         "e_drift_uk_minus_exception"): info[key] = bool(int(val))
            elif key == "trades_today": info[key] = int(val)
            elif key in ("short_levels","rsi_closes","rsi_highs","rsi_lows"):
                info[key] = [float(v) for v in val.split(",") if v.strip()]
            elif key == "position": info[key] = None if val == "none" else val
            elif key == "quadrant": info[key] = val
            else:
                try: info[key] = float(val)
                except: info[key] = val
    return info


def _is_today(path: str, cfg: Optional[MarketConfig] = None) -> bool:
    """
    Pure date check: is the day_info file dated today?

    The old time-based stale heuristic ("generated before session open →
    force one full re-setup at the open") has been removed. Reference
    staleness is now detected data-wise in run_market by re-deriving the
    reference levels and comparing them to the stored ones
    (_reference_is_stale), and corrected by _refresh_reference without
    touching live position/RSI state. That check needs no timezone
    arithmetic and no Asian-group special case, both of which this
    function previously carried (and which caused the mixed-clock
    double-setup RSI corruption on Asian markets).
    """
    try:
        with open(path) as f:
            content = f.read()
        trade_date_str = None
        for line in content.splitlines():
            if line.startswith("trade_date="):
                trade_date_str = line.strip().split("=",1)[1]
                break
        if not trade_date_str:
            return False
        today      = _london_now().date()
        trade_date = datetime.strptime(trade_date_str, "%d/%m/%Y").date()
        return trade_date == today
    except Exception:
        pass
    return False


def _save_state(path: str, info: dict, position,
                entry_px: float, best_profit: float,
                rsi_c: list, rsi_h: list, rsi_l: list) -> None:
    with open(path) as f: lines = f.readlines()
    # NOTE: any field updated in-memory during a tick MUST be in this set to
    # survive the save — everything else is kept verbatim from the old file.
    # above_ma / long_valid / short_valid / e_drift_uk_minus_exception /
    # open_price were previously missing, so the session-open ([OPEN]) and
    # pre-open handlers' direction updates were silently discarded at save
    # time: the 08:00 chart Telegram could say "Long ✅" (in-memory) while
    # the file — and therefore every later tick and log line — stayed short.
    mutable = {"position","entry_px","best_profit","trades_today",
               "rsi_closes","rsi_highs","rsi_lows","quadrant","last_rsi_bar",
               "above_ma","long_valid","short_valid",
               "e_drift_uk_minus_exception","open_price","account_value"}
    kept = [ln for ln in lines if ln.split("=")[0].strip() not in mutable]
    kept.append(f"quadrant={info['quadrant']}\n")
    kept.append(f"last_rsi_bar={info.get('last_rsi_bar', '') or ''}\n")
    kept.append(f"account_value={float(info.get('account_value', 0.0)):.2f}\n")
    kept.append(f"above_ma={int(info.get('above_ma', 0))}\n")
    kept.append(f"long_valid={int(info.get('long_valid', 0))}\n")
    kept.append(f"short_valid={int(info.get('short_valid', 0))}\n")
    kept.append(f"e_drift_uk_minus_exception={int(info.get('e_drift_uk_minus_exception', 0))}\n")
    kept.append(f"open_price={float(info.get('open_price', 0.0)):.2f}\n")
    kept.append(f"position={'none' if position is None else position}\n")
    kept.append(f"entry_px={entry_px:.2f}\n")
    kept.append(f"best_profit={best_profit:.2f}\n")
    kept.append(f"trades_today={int(info.get('trades_today', 0))}\n")
    win = 100
    kept.append(f"rsi_closes={','.join(f'{v:.2f}' for v in rsi_c[-win:])}\n")
    kept.append(f"rsi_highs={','.join(f'{v:.2f}' for v in rsi_h[-win:])}\n")
    kept.append(f"rsi_lows={','.join(f'{v:.2f}' for v in rsi_l[-win:])}\n")
    with open(path, "w") as f: f.writelines(kept)


# ══════════════════════════════════════════════════════════════════
# DAILY SETUP
# ══════════════════════════════════════════════════════════════════


def _derive_reference(prices: dict, cfg: MarketConfig) -> dict:
    """
    Pure reference derivation. Computes everything that depends only on
    PRIOR-DAY data: D1/D2/D3 levels, MA20, entry ceiling, stop base, target
    ladder, short levels, and the daily RSI seed. No file I/O, no knowledge
    of positions or intraday state. Both _daily_setup (new day) and
    _refresh_reference (same-day levels correction) build on this.

    Returns a dict of reference fields plus the daily RSI seed arrays.
    Raises ValueError when history is insufficient (same contract as before).
    """
    days       = sorted(prices)
    trade_date = datetime.combine(_london_now().date(), datetime.min.time())
    ma20       = _build_ma20(prices, cfg.ma_period)

    # Pre-open: today's session hasn't produced a bar yet, so neither
    # `prices` nor `ma20` will have an entry for trade_date. Fall back
    # to the most recently completed day's MA rather than erroring —
    # this is expected and normal before the session opens.
    if trade_date not in ma20:
        if days and trade_date not in prices:
            ma = ma20.get(days[-1])
            if ma is None:
                _log(cfg.name, f"[ERROR] Insufficient history for MA20")
                _log(cfg.name, f"        Need {cfg.ma_period + 1} daily rows, have {len(days)}")
                raise ValueError(f"{cfg.name}: insufficient history for MA20")
            ref_date = days[-1]
        else:
            _log(cfg.name, f"[ERROR] Insufficient history for MA20")
            _log(cfg.name, f"        Need {cfg.ma_period + 1} daily rows, have {len(days)}")
            if days:
                _log(cfg.name, f"        Date range in prices dict: {days[0].strftime('%d/%m/%Y')} → {days[-1].strftime('%d/%m/%Y')}")
                _log(cfg.name, f"        Daily rows found ({len(days)} total):")
                for d in days:
                    bar = prices[d]
                    _log(cfg.name, f"          {d.strftime('%d/%m/%Y')}  open={bar['open']}  close={bar['close']}  eve={bar.get('eve_close','n/a')}")
            else:
                _log(cfg.name, f"        No daily rows at all — _load_prices returned empty dict")
                _log(cfg.name, f"        Check session filter: bars need at least one candle between {cfg.csv_session_start}-{cfg.csv_session_end}")
            if ma20:
                ma_days = sorted(ma20.keys())
                _log(cfg.name, f"        MA20 computed for {len(ma20)} days: {ma_days[0].strftime('%d/%m/%Y')} → {ma_days[-1].strftime('%d/%m/%Y')}")
                _log(cfg.name, f"        Last 5 MA20 dates: {[d.strftime('%d/%m/%Y') for d in ma_days[-5:]]}")
            else:
                _log(cfg.name, f"        MA20 dict is empty — not enough rows to compute any MA value")
            try:
                with open(cfg.data_path) as _f:
                    raw = _f.readlines()
                _log(cfg.name, f"        Raw file: {len(raw)} lines")
                _log(cfg.name, f"        First line: {repr(raw[0][:80])}")
                _log(cfg.name, f"        Second line: {repr(raw[1][:80])}")
                _log(cfg.name, f"        Third line:  {repr(raw[2][:80])}")
            except Exception as _e:
                _log(cfg.name, f"        Could not read raw file: {_e}")
            raise ValueError(f"{cfg.name}: insufficient history for MA20")
    else:
        ma = ma20[trade_date]
        ref_date = trade_date

    # Anchor the LEVEL derivation to the calendar trade_date, not ref_date.
    # ref_date falls back to days[-1] (yesterday) before the session opens,
    # and _ref_levels computes d1 = _prev_day(<anchor>) — so anchoring to
    # ref_date pre-open silently shifted every level back one day (d1 became
    # the day BEFORE yesterday). That one-day shift was the entire "stale
    # reference" problem: the 06:00 setup was wrong by construction and had
    # to be corrected once today's first bar appeared (the old forced 09:00
    # re-setup; later the data-based refresh). Yesterday's close and eve
    # close are fully settled long before 06:00, so deriving from trade_date
    # makes the very first pre-open setup FINAL — the refresh becomes a
    # no-op safety net and no intraday re-evaluation occurs at all.
    # ref_date is still used for the MA fallback above (today's MA20 cannot
    # exist before today's first bar; yesterday's MA is the correct stand-in)
    # and for the pre-open open-price estimate below.
    ref = _ref_levels(prices, trade_date)
    if ref is None:
        raise ValueError(f"{cfg.name}: insufficient history for reference levels")

    d1_uk=ref["d1_uk"]; d1_eve=ref["d1_eve"]
    d2_uk=ref["d2_uk"]; d2_eve=ref["d2_eve"]; d3_uk=ref["d3_uk"]
    open_px  = prices.get(trade_date, prices[ref_date])["open"]
    above_ma = open_px > ma
    ceil     = d1_eve + cfg.golden_rule
    quad     = _quadrant(open_px, d1_uk, d1_eve)

    long_valid   = above_ma
    e_exc = (quad == "E-Drift-UK-") and (not above_ma) and (d2_eve <= ceil) and (open_px <= d2_eve + cfg.div_zone)
    short_valid  = not above_ma
    short_levels = sorted(set(filter(None, [d1_eve, d1_uk, d2_eve, d2_uk, d3_uk])))
    # RSI seed: all completed days before the trade date — anchored to
    # trade_date for the same reason, so the pre-open seed already includes
    # yesterday and is identical to the post-open seed (no baseline shift
    # at the session open).
    hist  = [d for d in days if d < trade_date]
    rsi_c = [prices[d]["close"] for d in hist]
    rsi_h = [prices[d]["high"]  for d in hist]
    rsi_l = [prices[d]["low"]   for d in hist]

    return {
        "trade_date": trade_date,
        "d1_uk":d1_uk,"d1_eve":d1_eve,"d2_uk":d2_uk,"d2_eve":d2_eve,"d3_uk":d3_uk,
        "ma20":ma,"above_ma":above_ma,"open_price":open_px,"quadrant":quad,
        "s1":d1_eve-d1_uk,"s2b":open_px-d1_eve,"s2a":open_px-d1_uk,
        "entry_ceiling":ceil,
        "long_stop": ceil - cfg.sl_long,
        "long_valid":long_valid,
        "e_drift_uk_minus_exception":e_exc,"e_drift_uk_minus_trigger":d2_eve,
        "short_valid":short_valid,"short_levels":short_levels,
        "t1":d1_uk,"t2":d2_eve,"t3":d3_uk,
        "rsi_seed_c":rsi_c,"rsi_seed_h":rsi_h,"rsi_seed_l":rsi_l,
    }


# Reference fields owned by _derive_reference. _refresh_reference updates
# exactly these (minus quadrant/above_ma/valids — see note there) and nothing
# else; _reference_is_stale compares exactly the level fields.
# NOTE: only fields that _write_day_info actually persists can be compared —
# long_stop is derived at runtime (entry_ceiling - sl_long) and never written
# to the file, so it is refreshed but NOT compared (entry_ceiling covers it).
_REFERENCE_LEVEL_FIELDS = (
    "d1_uk","d1_eve","d2_uk","d2_eve","d3_uk","ma20",
    "entry_ceiling","long_stop","e_drift_uk_minus_trigger",
    "t1","t2","t3",
)
_REFERENCE_COMPARE_FIELDS = tuple(
    # long_stop: derived at runtime, never persisted (entry_ceiling covers it).
    # ma20: drifts intraday once today's row exists (ma20[trade_date] includes
    # today's FORMING close), so comparing it would fire a spurious refresh on
    # every tick after the open. The initial pre-open setup writes the MA of
    # completed days — the stable frame the day trades against; genuine
    # d-level corrections still trigger refresh and carry the MA with them.
    f for f in _REFERENCE_LEVEL_FIELDS if f not in ("long_stop", "ma20")
)


def _reference_is_stale(info: dict, fresh: dict) -> bool:
    """
    Data-based staleness check: the stored day_info is stale iff re-deriving
    the reference from the current price data yields materially different
    level values. This replaces the old time-based ('generated before session
    open') heuristic, which mixed clock domains and misfired for Asian
    markets. Comparing the derived result against the stored one keys the
    decision on the thing we actually care about — did the reference data
    settle to different values — and is naturally one-shot: after a refresh
    the stored values match and the check goes quiet.
    """
    for k in _REFERENCE_COMPARE_FIELDS:
        try:
            if abs(float(info.get(k, 0.0)) - float(fresh[k])) > 0.005:
                return True
        except (TypeError, ValueError):
            return True
    # short_levels is a list — compare as rounded tuples
    try:
        a = tuple(round(float(x), 2) for x in info.get("short_levels", []))
        b = tuple(round(float(x), 2) for x in fresh["short_levels"])
        if a != b:
            return True
    except (TypeError, ValueError):
        return True
    return False


def _refresh_reference(prices: dict, cfg: MarketConfig, fresh: dict) -> dict:
    """
    Same-day reference correction. Updates ONLY the reference level fields in
    the existing day_info; never touches position, entry_px, best_profit,
    trades_today, or the intraday RSI arrays. This replaces the old forced
    full re-setup, which re-ran _daily_setup mid-session and (before the
    preservation patches) wiped live state.

    Deliberately NOT updated here:
      * quadrant / above_ma / long_valid / short_valid /
        e_drift_uk_minus_exception — these are owned by the pre-open and
        session-open ([OPEN]) handlers, which recompute them from the live
        price each tick against whatever levels are current. Overwriting the
        quadrant here with one derived from a historical open price would
        fight the 08:00 lock. On the tick after a refresh, those handlers
        naturally re-evaluate against the corrected levels.
      * open_price / s1 / s2a / s2b — informational fields tied to the setup
        snapshot; s1 derives from d1 levels so it IS refreshed.
    """
    info = _read_day_info(cfg.day_info_path)
    old_ceil = info.get("entry_ceiling")
    for k in _REFERENCE_LEVEL_FIELDS:
        info[k] = fresh[k]
    info["s1"] = fresh["s1"]
    info["short_levels"] = fresh["short_levels"]
    _write_day_info(cfg.day_info_path, info, cfg)
    _log(cfg.name, f"[SETUP] reference refreshed (levels only): "
                   f"ceiling {old_ceil} → {fresh['entry_ceiling']:.2f}; "
                   f"position/trades/RSI untouched")
    return info


def _daily_setup(prices: dict, cfg: MarketConfig) -> dict:
    """
    NEW-DAY initialiser. Derives the reference and writes a fresh day_info
    with flat position state and the daily RSI seed. This is the ONLY place
    live state is reset to flat, and it runs once per genuine new trading
    day (the same-day correction path is _refresh_reference).
    """
    fresh = _derive_reference(prices, cfg)
    trade_date = fresh["trade_date"]
    rsi_c = fresh.pop("rsi_seed_c")
    rsi_h = fresh.pop("rsi_seed_h")
    rsi_l = fresh.pop("rsi_seed_l")

    # Defensive belt-and-braces: if a same-day day_info already exists (e.g.
    # _daily_setup is invoked directly by an external script mid-session),
    # preserve accumulated intraday RSI and any open position rather than
    # wiping them — same rationale as the old preservation patches. In the
    # normal run_market flow the STALE path routes to _refresh_reference and
    # this block is not exercised.
    keep_position, keep_entry, keep_best, keep_trades = None, 0.0, 0.0, 0
    try:
        if os.path.exists(cfg.day_info_path):
            prev = _read_day_info(cfg.day_info_path)
            if prev.get("trade_date") == trade_date:
                keep_position = prev.get("position")
                keep_entry    = float(prev.get("entry_px", 0.0) or 0.0)
                keep_best     = float(prev.get("best_profit", 0.0) or 0.0)
                keep_trades   = int(prev.get("trades_today", 0) or 0)
                prev_c = prev.get("rsi_closes", [])
                prev_h = prev.get("rsi_highs",  [])
                prev_l = prev.get("rsi_lows",   [])
                if (len(prev_c) >= len(rsi_c)
                        and len(prev_h) >= len(rsi_h)
                        and len(prev_l) >= len(rsi_l)):
                    _log(cfg.name, "[SETUP] preserving intraday RSI history "
                                   f"({len(prev_c)} bars) across re-setup")
                    rsi_c, rsi_h, rsi_l = list(prev_c), list(prev_h), list(prev_l)
                if keep_position in ("LONG", "SHORT"):
                    _log(cfg.name, f"[SETUP] preserving open {keep_position} "
                                   f"@ {keep_entry:.1f} across re-setup")
    except Exception as _e:
        _log(cfg.name, f"[SETUP] state-preserve skipped: {_e}")

    info = dict(fresh)
    info.update({
        "position":keep_position,"entry_px":keep_entry,
        "best_profit":keep_best,"trades_today":keep_trades,
        "rsi_closes":rsi_c,"rsi_highs":rsi_h,"rsi_lows":rsi_l,
    })
    _write_day_info(cfg.day_info_path, info, cfg)
    regime = "ABOVE MA" if info["above_ma"] else f"BELOW MA ({info['open_price']-info['ma20']:+.0f}pts)"
    _log(cfg.name, f"[SETUP] {trade_date.strftime('%d/%m/%Y')}  {info['quadrant']}  {regime}  "
                   f"long={'✅' if info['long_valid'] else '❌'}  short={'✅' if info['short_valid'] else '❌'}")
    return info


# ══════════════════════════════════════════════════════════════════
# ENTRY SIGNALS
# ══════════════════════════════════════════════════════════════════

def _check_entries(o: float, h: float, l: float, c: float,
                   rsi_c: list, rsi_h: list, rsi_l: list,
                   info: dict, cfg: MarketConfig,
                   ig=None) -> tuple:
    """Returns (action, entry_price) or (None, None) if no signal."""
    rsis = _calc_rsi(rsi_c, cfg.rsi_period)
    buy_fn, sell_fn = _BROKER[cfg.name]
    d1_uk=info["d1_uk"]; d1_eve=info["d1_eve"]; d2_uk=info["d2_uk"]
    ceil=info["entry_ceiling"]; quad=info["quadrant"]
    if len(rsi_c) < cfg.rsi_period + 5: return None, None, False
    if info.get("trades_today", 0) >= MAX_TRADES_PER_DAY:
        _debug(cfg.name, f"  [LIMIT] {MAX_TRADES_PER_DAY} trades already taken today — no further entries")
        return None, None, False
    if info["long_valid"]:
        if l <= ceil:
            entry = min(o, ceil); entry = max(entry, l)
            if entry <= ceil:
                found, strength = _bull_div(rsi_l, rsis, cfg)
                if found:
               
                    stop = entry - cfg.sl_long
                    size = _calc_size(cfg, info)
                    success = buy_fn(c, stop,
                           f"Long|{quad}|price={c:.1f}|div={strength:.1f}|stop={stop:.1f}|"
                           f"T1={info['t1']:.1f} T2={info['t2']:.1f} T3={info['t3']:.1f}|"
                           f"size={size:.2f}£/pt",
                           ig=ig, cfg=cfg)
                    return "BUY", entry, success

    elif info["e_drift_uk_minus_exception"]:
        trigger = info["e_drift_uk_minus_trigger"]
        if h >= trigger and trigger <= ceil:
            found, strength = _bull_div(rsi_l, rsis, cfg)
            if found:
                stop = info["long_stop"]
                size = _calc_size(cfg, info)
                success = buy_fn(trigger, stop,
                       f"LongBelowMA|{quad}|price={c:.1f}|div={strength:.1f}|stop={stop:.1f}|"
                       f"T1={d1_uk:.1f} T2={d2_uk:.1f}|size={size:.2f}£/pt",
                       ig=ig, cfg=cfg)
                return "BUY_EXCEPTION", trigger, success

    elif info["short_valid"]:
        # E+Drift-UK- veto: below MA but the US session closed above the UK close
        # (d1_eve > d1_uk = evening premium in a downtrend = overnight bounce).
        # Backtest shows this quadrant has WR=20%, avg=-10.5pts for shorts below MA.
        # The US bounce flags a dead-cat recovery that runs through short levels.
        # Skip all short entries when this pattern is present.
        if quad == "E+Drift-UK-":
            _debug(cfg.name, "  [VETO] E+Drift-UK- below MA — short skipped")
            return None, None, False
        for level in info["short_levels"]:
            if level - cfg.div_zone <= h <= level + cfg.div_zone:
                found, strength = _bear_div(rsi_h, rsis, cfg)
                if found:
                    stop = c + cfg.sl_short
                    size = _calc_size(cfg, info)
                    success = sell_fn(c, stop,
                            f"Short|{quad}|price={c:.1f}|entry target={level:.1f}|div={strength:.1f}|"
                            f"stop={stop:.1f}|T1={d1_uk:.1f} T2={d1_eve:.1f}|"
                            f"size={size:.2f}£/pt",
                            ig=ig, cfg=cfg)
                    return "SELL", c, success
                break
    return None, None, False

def _in_pre_open(hour: int, cfg: MarketConfig) -> bool:
    s = cfg.pre_open_start_hour
    e = cfg.session_open_hour
    if s > e:   # wraps midnight e.g. Nikkei 19→0, HS 20→1
        return hour >= s or hour < e
    else:       # same-day e.g. EU 03→08
        return s <= hour < e

def get_open_positions(ig_service, epic: str) -> int:
    """
    Query IG Markets for open positions on a specific epic.
    Returns the number of open positions (0 = none open).
    Returns -1 on API error or no session (treated as 'position exists' to be safe).

    Note: trading_ig's fetch_open_positions() returns a pandas DataFrame, not a
    dict. Truthiness tests like `not positions` raise "truth value of a DataFrame
    is ambiguous", which previously sent every call into the except branch and
    made it always return -1. We handle the DataFrame explicitly here.
    """
    if ig_service is None:
        return -1   # no session available — assume open, proceed with close
    try:
        positions = ig_service.fetch_open_positions()

        # Normalise the possible return shapes from trading_ig:
        #   * pandas DataFrame (most common)
        #   * dict with a "positions" list (raw REST shape)
        #   * None / empty
        import pandas as pd

        if positions is None:
            return 0

        # DataFrame path
        if isinstance(positions, pd.DataFrame):
            if positions.empty:
                return 0
            # Column name for the epic varies by trading_ig version:
            # flattened "epic" or nested "market.epic".
            if "epic" in positions.columns:
                return int((positions["epic"] == epic).sum())
            if "market.epic" in positions.columns:
                return int((positions["market.epic"] == epic).sum())
            # Fallback: scan rows for a nested market dict.
            count = 0
            for _, row in positions.iterrows():
                mkt = row.get("market", {})
                row_epic = mkt.get("epic", "") if isinstance(mkt, dict) else ""
                if row_epic == epic:
                    count += 1
            return count

        # dict path (raw REST)
        if isinstance(positions, dict):
            rows = positions.get("positions", [])
            return sum(
                1 for p in rows
                if p.get("market", {}).get("epic", "") == epic
            )

        # Unknown shape — treat as no reliable answer.
        _log("IG", f"[WARN] get_open_positions: unexpected return type "
                   f"{type(positions).__name__} for {epic} — assuming open")
        return -1

    except Exception as e:
        _log("IG", f"[WARN] get_open_positions error for {epic}: {e} — assuming open")
        return -1   # safe default: treat as open, don't skip the close

# ══════════════════════════════════════════════════════════════════
# SINGLE-MARKET RUN
# ══════════════════════════════════════════════════════════════════
def run_market(cfg: MarketConfig, now: datetime, ig_service=None,
               allow_entry: bool = True) -> None:
    # allow_entry=False → manage/close an existing position only, never open a new
    # one. The sequential dispatcher passes False for every market except the one
    # it has selected to hold the lock this tick, so "other" markets can no longer
    # slip a fresh entry through run_market and defeat the sequential lock.
    if cfg.session_group == "asian":
        hour = datetime.now(timezone.utc).hour
    else:hour = now.hour

    # Honour blackout even when called directly
    if _in_blackout(hour, cfg):
        return

    if not os.path.exists(cfg.data_path):
        _log(cfg.name, f"[ERROR] prices not found: {cfg.data_path}"); return
    prices = _load_prices(cfg.data_path, cfg)
    if not prices:
        try:
            with open(cfg.data_path) as _f:
                raw_lines = _f.readlines()
            _log(cfg.name, f"[ERROR] no rows parsed from {cfg.data_path}")
            _log(cfg.name, f"        file has {len(raw_lines)} raw lines")
            if raw_lines:
                _log(cfg.name, f"        first line: {repr(raw_lines[0][:80])}")
                _log(cfg.name, f"        last  line: {repr(raw_lines[-1][:80])}")
            _log(cfg.name,     "        expected:   date,open,high,low,close[,eve_close]")
            _log(cfg.name,     "        date fmts:  YYYY-MM-DD  DD/MM/YYYY  DD-MM-YYYY")
        except Exception as _e:
            _log(cfg.name, f"[ERROR] could not read {cfg.data_path}: {_e}")
        return


    # ── Setup decision: NEW_DAY | STALE_REFERENCE | CURRENT ───────
    # NEW_DAY: day_info absent or dated a prior day → full init (flat state).
    # STALE_REFERENCE: day_info is today's but re-deriving the reference from
    #   the current price data yields different levels (prior-day bars settled
    #   after the original setup ran) → refresh levels only, live state kept.
    # CURRENT: levels match → nothing to do.
    # The data-based staleness check replaces the old time-based heuristic in
    # _is_today (generated-before-open), which mixed clock domains and needed
    # an Asian-market special case; comparing derived vs stored levels is
    # self-correcting and one-shot for every market group.
    if not _is_today(cfg.day_info_path, cfg):
        _daily_setup(prices, cfg)
    else:
        try:
            _fresh_ref = _derive_reference(prices, cfg)
            _stored    = _read_day_info(cfg.day_info_path)
            if _reference_is_stale(_stored, _fresh_ref):
                _refresh_reference(prices, cfg, _fresh_ref)
        except ValueError:
            # Insufficient history to re-derive (e.g. thin CSV) — keep the
            # existing day_info rather than failing the tick.
            pass

    info        = _read_day_info(cfg.day_info_path)
    position    = info["position"]
    entry_px    = float(info["entry_px"])
    best_profit = float(info["best_profit"])
    rsi_c       = list(info.get("rsi_closes", []))
    rsi_h       = list(info.get("rsi_highs",  []))
    rsi_l       = list(info.get("rsi_lows",   []))
  # Current 5-min bar values — used for stop/exit checks only.
    # The session-aggregate l/h above accumulate the session extremes which
    # would trigger stops based on earlier bars, not the current price.


    o, h, l, c, cb_o, cb_h, cb_l, cb_c = _latest_bar_values(prices)

    # ── Feed-gap handling for the intraday RSI series ─────────────
    # The RSI arrays are corrupted in two ways when the price feed breaks
    # (e.g. the 07-Jul 04:10–05:25 IG outage):
    #   * STALE FEED: no new bar arrives, so each tick re-appends the same
    #     cached bar → a flat plateau that skews Wilder's running average
    #     for the rest of the session (24 frozen bars that day).
    #   * GAP: when the feed recovers, bars for the outage window are
    #     missing, so the series silently skips time.
    # Strategy: track the timestamp of the last bar appended (last_rsi_bar).
    #   * same bar again        → skip the append (no plateau).
    #   * gap of >2 slots       → rebuild today's arrays from the CSV
    #     (which get_price_history has backfilled), i.e. daily seed +
    #     every actual intraday bar — the series the framework would have
    #     had if the outage never happened.
    #   * otherwise             → normal append.
    _today_key   = sorted(prices)[-1]
    _bar_time    = str(prices[_today_key].get("bar_time") or "")
    _last_bar    = str(info.get("last_rsi_bar") or "")

    def _gap_minutes(t_prev: str, t_cur: str) -> int:
        try:
            ph, pm = int(t_prev[:2]), int(t_prev[3:5])
            ch, cm = int(t_cur[:2]),  int(t_cur[3:5])
        except (ValueError, IndexError):
            return 0
        diff = (ch * 60 + cm) - (ph * 60 + pm)
        if diff < 0:
            diff += 24 * 60   # crossed midnight (Asian sessions)
        return diff

    if _bar_time and _bar_time == _last_bar:
        # Stale feed: nothing new — leave the arrays untouched this tick.
        _debug(cfg.name, f"  [FEED] stale bar {_bar_time} — RSI append skipped")
    elif (_last_bar and _bar_time
            and _gap_minutes(_last_bar, _bar_time) > 10):
        # Gap: rebuild the whole series from the (backfilled) CSV so the
        # outage window's bars are included and any plateau is purged.
        gap_min  = _gap_minutes(_last_bar, _bar_time)
        hist     = [d for d in sorted(prices) if d < _today_key]
        intraday = prices[_today_key].get("intraday", [])
        rsi_c = [prices[d]["close"] for d in hist] + [b[4] for b in intraday]
        rsi_h = [prices[d]["high"]  for d in hist] + [b[2] for b in intraday]
        rsi_l = [prices[d]["low"]   for d in hist] + [b[3] for b in intraday]
        _log(cfg.name, f"[FEED] {gap_min}min gap ({_last_bar} → {_bar_time}) — "
                       f"RSI series rebuilt from CSV "
                       f"({len(hist)} daily + {len(intraday)} intraday bars)")
    else:
        rsi_c.append(cb_c); rsi_h.append(cb_h); rsi_l.append(cb_l)
    info["last_rsi_bar"] = _bar_time
    
 

    # ── Log nearest entry and nearest target ──────────────────────
    if not position:
        ceil   = info["entry_ceiling"]
        curr_p = c   # current bar close — more accurate than day open by mid-session
        
        all_levels = sorted(set([
            info["d1_uk"], info["d1_eve"],
            info["d2_uk"], info["d2_eve"],
            info["d3_uk"]
        ]))

        if info["long_valid"] or info["e_drift_uk_minus_exception"]:
            entries  = sorted([lv for lv in all_levels if lv <= ceil and lv >= curr_p])
            targets  = sorted([lv for lv in all_levels if lv > curr_p])
            near_e   = min(entries, key=lambda lv: abs(lv - curr_p)) if entries else ceil
            near_t   = min(targets, key=lambda lv: abs(lv - curr_p)) if targets else None
            nearest_entry  = f"LONG ≤ {near_e:.1f} ({near_e - curr_p:+.1f} from current)"
            other_entries  = [lv for lv in entries if lv != near_e]
            nearest_target = f"{near_t:.1f} ({near_t - curr_p:+.1f}pts)" if near_t else "none"
            all_t_str      = " → ".join(f"{lv:.1f}" for lv in sorted(targets))

        elif info["short_valid"]:
            entries  = sorted([lv for lv in all_levels if lv > curr_p], reverse=True)
            targets  = sorted([lv for lv in all_levels if lv < curr_p], reverse=True)
            near_e   = min(entries, key=lambda lv: abs(lv - curr_p)) if entries else None
            near_t   = max(targets) if targets else None
            nearest_entry  = f"SHORT @ {near_e:.1f} ({near_e - curr_p:+.1f} from current)" if near_e else "SHORT — no level above current price"
            other_entries  = [lv for lv in entries if lv != near_e]
            nearest_target = f"{near_t:.1f} ({near_t - curr_p:+.1f}pts)" if near_t else "none"
            all_t_str      = " → ".join(f"{lv:.1f}" for lv in sorted(targets, reverse=True))

        else:
            nearest_entry  = "No entry — both vetoed"
            other_entries  = []
            nearest_target = "n/a"
            all_t_str      = "n/a"

        _log(cfg.name, f"  Entry:  {nearest_entry}")
        if other_entries:
            other_str = "  |  ".join(f"{lv:.1f} ({lv - curr_p:+.1f})" for lv in other_entries)
            _log(cfg.name, f"  Other entries: {other_str}")
        _log(cfg.name, f"  Target: {nearest_target}  All: {all_t_str} pos={position or 'flat'}")
    else:
        
        in_session = (
        cfg.session_open_hour < hour < cfg.session_close_hour or
        (hour == cfg.session_open_hour and now.minute >= cfg.session_open_minute) or
        (hour == cfg.session_close_hour and now.minute < cfg.session_close_minute)
        )

        if not in_session:
            pnl_now = (cb_c - entry_px) if position == "LONG" else (entry_px - cb_c)
            _log(cfg.name, f"  Position: {position} @ {entry_px:.1f}  current={cb_c:.1f}  P&L={pnl_now:+.1f}pts")
            pnl_str = f"+{pnl_now:.1f}" if pnl_now >= 0 else f"{pnl_now:.1f}"
            telegram_bitbot(f"Position: {position} @ {entry_px:.1f} | current={cb_c:.1f} | PnL={pnl_str}pts")
            pass
        elif position is not None:
            pnl_now = (cb_c - entry_px) if position == "LONG" else (entry_px - cb_c)
            _log(cfg.name, f"  Position: {position} @ {entry_px:.1f}  current={cb_c:.1f}  P&L={pnl_now:+.1f}pts")
            pnl_str = f"+{pnl_now:.1f}" if pnl_now >= 0 else f"{pnl_now:.1f}"
            telegram_bitbot(f"Position: {position} @ {entry_px:.1f} | current={cb_c:.1f} | PnL={pnl_str}pts")

        
    

    in_pre_open = _in_pre_open(hour, cfg)

    # ── PRE-OPEN: recalculate quadrant from current close ─────────
    if in_pre_open:
        new_quad = _quadrant(c, info["d1_uk"], info["d1_eve"])
        above_ma = c > info["ma20"]
        ceil     = info["entry_ceiling"]
        
        info["quadrant"]    = new_quad
        info["above_ma"]    = above_ma
        info["long_valid"]  = above_ma
        info["short_valid"] = not above_ma
        
        info["e_drift_uk_minus_exception"] = (
            new_quad == "E-Drift-UK-" and not above_ma and
            info["e_drift_uk_minus_trigger"] <= ceil
        )
        _debug(cfg.name, f"  [pre-open] quad={new_quad}  "
                         f"{'above' if above_ma else 'below'} MA  ")
        # Do NOT return here. Fall through to the management and entry logic
        # below, so the framework can evaluate and take positions during the
        # pre-open window (06:00–08:00 UTC for EU markets) just as it would
        # during the cash session. The quadrant, long_valid, and short_valid
        # fields have already been updated above to reflect the current price,
        # so _check_entries will use the correct direction and ceiling.
        # The _save_state call is intentionally removed here: state will be
        # persisted by _check_entries on a successful entry, or by _save_state
        # at the end of the management block if we are managing an open position.
        # This avoids a redundant write on every pre-open tick when flat.
                        

    # ── SESSION OPEN: lock quadrant from open price ───────────────
    elif (hour == cfg.session_open_hour and
        cfg.session_open_minute <= now.minute < cfg.session_open_minute + 4):
        new_quad = _quadrant(o, info["d1_uk"], info["d1_eve"])
        above_ma = o > info["ma20"]
        ceil     = info["entry_ceiling"]
        
        if new_quad != info["quadrant"]:
            _log(cfg.name, f"[OPEN] Quadrant {info['quadrant']} → {new_quad}  (open={o:.1f})")
        else:
            _debug(cfg.name, f"[OPEN] Quadrant confirmed: {new_quad}  (open={o:.1f})")
            
        info["quadrant"]    = new_quad
        info["above_ma"]    = above_ma
        # Direction flip at session open is a significant event on marginal
        # MA days (price straddling the MA20) — log it explicitly so the log
        # and the chart Telegram can never silently disagree again.
        if bool(info.get("long_valid")) != above_ma:
            _log(cfg.name, f"[OPEN] Direction flip: "
                           f"{'SHORT→LONG' if above_ma else 'LONG→SHORT'} "
                           f"(open {o:.1f} vs MA20 {info['ma20']:.1f})")
        info["long_valid"]  = above_ma
        info["short_valid"] = not above_ma
        # Refresh open_price to the real session open so the ascii chart's
        # OPEN row matches the regime line (it previously kept the pre-open
        # setup's price, making the chart internally contradictory).
        info["open_price"]  = o

        # ── Fetch live account value for position sizing ────────────
        # Done once per day at the session open — a single REST call that
        # is cheap, accurate (reflects overnight P&L and withdrawals) and
        # timed so compound sizing uses today's real equity, not a stale
        # config value. Stored in day_info as account_value; _calc_size
        # reads it from there. No failure here blocks the rest of the tick.
        if ig_service is not None and cfg.sizing_mode == "compound":
            _acct = get_account_info(ig_service)
            if _acct.get("balance", 0) > 0:
                info["account_value"] = _acct["balance"]
                _log(cfg.name, f"[OPEN] account balance: £{_acct['balance']:,.2f} "
                               f"→ size = £{_calc_size(cfg, info):.4f}/pt")
        
        info["e_drift_uk_minus_exception"] = (
            new_quad == "E-Drift-UK-" and not above_ma and
            info["e_drift_uk_minus_trigger"] <= ceil
        )
        #telegram_bitbot(f"[OPEN] Quadrant confirmed: {new_quad}  (open={o:.1f})  [S/L]{info['long_valid']} {info['short_valid']} [ENTRY]{info['entry_px']} [TARGET]{info['best_profit']}")
        if not position:
            chart = _ascii_chart(info, cfg)
            telegram_bitbot(chart)
        
    # ── SESSION (08:05+): quadrant frozen ─────────────────────────

    # ── MANAGE POSITION ───────────────────────────────────────────

    # Force-close: if we are at or past the session close time, exit
    # the position immediately at the current bar's close price.
    # This ensures the closing order is placed while the cash market
    # is still open (e.g. 16:25 bar, cash closes at 16:30).
    # Trigger force-close only at the exact configured minute (and the
    # next 4 minutes as safety — the cron fires every 5 min so we may
    # hit :26, :27 etc. if the :25 bar was missed). Use a 5-min window.
    at_session_close = (
        hour == cfg.session_close_hour and
        cfg.session_close_minute <= now.minute < cfg.session_close_minute + 5
    )
    if position and at_session_close:
        message = []
        pnl = (cb_c - entry_px) if position == "LONG" else (entry_px - cb_c)
        size = _calc_size(cfg, info)
        _log(cfg.name, f"[CLOSE] Session close @ {c:.1f}  "
                       f"({'LONG' if position == 'LONG' else 'SHORT'} "
                       f"{pnl:>+.1f}pts | size={size:.2f}£/pt)")
        message.append(f"[CLOSE] Session close @ {c:.1f}  ")

        # Actually close the position at IG before clearing our own state.
        # Previously this branch logged + telegrammed the close and wiped state
        # to flat WITHOUT ever calling close_buy/sell_trade, so the session-close
        # only happened in the framework's bookkeeping — the position stayed open
        # at IG with no management. _close_position returns True when IG confirms
        # the position is already gone; otherwise we place the closing order here.
        already_closed = _close_position(
            ig_service, cfg, "SESSION_CLOSE", cb_c, entry_px,
            position, info, rsi_c, rsi_h, rsi_l
        )
        if not already_closed:
            telegram_bitbot(message)
            if position == "LONG":
                close_buy_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)
            else:
                close_sell_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)

        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
        return

    if position == "LONG":
        message = []
        current_pnl = cb_c - entry_px
        best_profit = max(best_profit, current_pnl)
        if cb_l < entry_px - cfg.sl_long:
            exit_px   = entry_px - cfg.sl_long
            _log(cfg.name, f"[STOP] Long hard stop @ {exit_px:.1f}")
            message.append(f"[STOP] Long hard stop @ {exit_px:.1f}")
            already_closed = _close_position(
                ig_service, cfg, "STOP", exit_px, entry_px,
                position, info, rsi_c, rsi_h, rsi_l
            )
            # _close_position returns True only when IG confirms the position is
            # already gone (it clears state itself). When it returns False the
            # position is still open and we must actually place the closing order
            # here. The previous `return` above this block short-circuited that,
            # so hard stops never closed at the broker or alerted on Telegram.
            if not already_closed:
                telegram_bitbot(message)
                close_buy_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return

        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_h) >= 8:
            if _bear_div_exit(rsi_h, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bearish div @ {cb_c:.1f}  ({cb_c-entry_px:+.1f}pts)")
                message.append(f"[EXIT] RSI bearish div @ {cb_c:.1f}  ({cb_c-entry_px:+.1f}pts)")
                already_closed = _close_position(
                    ig_service, cfg, "RSI_DIV", cb_c, entry_px,
                    position, info, rsi_c, rsi_h, rsi_l
                )
                if not already_closed:
                    telegram_bitbot(message)
                    close_buy_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)
                    _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
                return
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit, rsi_c, rsi_h, rsi_l)
        return

    elif position == "SHORT":
        message = []
        current_pnl = entry_px - cb_c   # use current bar close, not session-aggregate low
        best_profit = max(best_profit, current_pnl)
        if cb_h > entry_px + cfg.sl_short:
            exit_px = entry_px + cfg.sl_short
            _log(cfg.name, f"[STOP] Short hard stop @ {exit_px:.1f}")
            message.append(f"[STOP] Short hard stop @ {exit_px:.1f}")
            already_closed = _close_position(
                ig_service, cfg, "STOP", exit_px, entry_px,
                position, info, rsi_c, rsi_h, rsi_l
            )
            # See long-stop note above: only skip the close when IG has already
            # confirmed the position is gone. Otherwise place the closing order.
            if not already_closed:
                telegram_bitbot(message)
                close_sell_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return

        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_l) >= 8:
            if _bull_div_exit(rsi_l, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bullish div @ {cb_c:.1f}  ({entry_px-cb_c:+.1f}pts)")
                message.append(f"[EXIT] RSI bullish div @ {cb_c:.1f}  ({entry_px-cb_c:+.1f}pts)")
                already_closed = _close_position(
                    ig_service, cfg, "RSI_DIV", cb_c, entry_px,
                    position, info, rsi_c, rsi_h, rsi_l
                )
                if not already_closed:
                    telegram_bitbot(message)
                    close_sell_trade(ig_service, epic=cfg.epic, size=cfg.flat_size)
                    _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
                return
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit, rsi_c, rsi_h, rsi_l)
        return

    # ── LOOK FOR ENTRY ────────────────────────────────────────────
    # Race-condition guard: re-read the group lock immediately before
    # attempting entry. Another EU market may have fired on this same
    # cron tick and written the lock after our strength probe but before
    # this run_market call.
    if cfg.session_group == "european":
        current_lock = _read_lock()
        if current_lock and current_lock != cfg.name.lower():
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return
    elif cfg.session_group == "asian":
        ASIAN_LOCK = SEQUENTIAL_LOCK.replace("market_lock", "asian_lock")
        if os.path.exists(ASIAN_LOCK):
            try:
                asian_held = open(ASIAN_LOCK).read().strip().lower()
                if asian_held and asian_held != cfg.name.lower():
                    _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
                    return
            except Exception:
                pass


    # Hard gate: if this run is manage-only (not the lock-holder / chosen market),
    # never open a new position. Managing an existing position already happened
    # above; reaching here with allow_entry=False means we must not enter.
    if not allow_entry:
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit,
                    rsi_c, rsi_h, rsi_l)
        return

    # ── Stale-feed entry interlock ─────────────────────────────────
    # If the newest bar in the CSV is older than STALE_FEED_MAX_MIN, the
    # framework is looking at frozen data (fetch failures, IG allowance
    # exhausted, feed outage). Opening a new position on stale data would
    # evaluate a frozen bar and then submit a live market order at whatever
    # the real price now is (13-Jul incident). Entries are refused until
    # fresh data flows again.
    #
    # SESSION-AWARE: skip the staleness check when the market is currently
    # outside its trading session. After close the streamer and IG REST
    # both stop writing bars — a 20:35 last bar is correct at 21:15 for
    # HangSeng (closes 08:00 UTC), not a feed failure. Applying the interlock
    # outside session hours would permanently block the first tick of the
    # next session when the market reopens and the first bar hasn't arrived yet.
    # The blackout check (_in_blackout) already handles the outer boundary;
    # here we only need to know whether we are currently inside the session.
    _now_min = now.hour * 60 + now.minute
    _sess_close_min = cfg.session_close_hour * 60 + cfg.session_close_minute
    _pre_open_min   = cfg.pre_open_start_hour * 60
    _in_pre_open_or_session = _pre_open_min <= _now_min <= _sess_close_min

    if _in_pre_open_or_session:
        feed_ts = _last_csv_timestamp(cfg.data_path)
        if feed_ts is not None:
            feed_age_min = (datetime.now() - feed_ts).total_seconds() / 60.0
            if feed_age_min > STALE_FEED_MAX_MIN:
                _log(cfg.name, f"[FEED] data {feed_age_min:.0f}min stale "
                               f"(last bar {feed_ts:%Y-%m-%d %H:%M}) — "
                               f"entries locked, managing only")
                _save_state(cfg.day_info_path, info, position, entry_px,
                            best_profit, rsi_c, rsi_h, rsi_l)
                return

    action, signal_entry, success = _check_entries(cb_o, cb_h, cb_l, cb_c, rsi_c, rsi_h, rsi_l, info, cfg, ig=ig_service)



    if action in ("BUY", "BUY_EXCEPTION"):
        if success:
            info["trades_today"] = info.get("trades_today", 0) + 1
            _log(cfg.name, f"[ENTRY] Trade {info['trades_today']}/{MAX_TRADES_PER_DAY} today")
            _save_state(cfg.day_info_path, info, "LONG", signal_entry, 0.0, rsi_c, rsi_h, rsi_l)
        else:   
            _log(cfg.name, "[ERROR] BUY order failed at IG — state not saved, remaining flat")
            telegram_bitbot(f"[ERROR] {cfg.name} BUY failed at IG — check account")
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
    elif action == "SELL":
        if success:
            info["trades_today"] = info.get("trades_today", 0) + 1
            _log(cfg.name, f"[ENTRY] Trade {info['trades_today']}/{MAX_TRADES_PER_DAY} today")
            _save_state(cfg.day_info_path, info, "SHORT", signal_entry, 0.0, rsi_c, rsi_h, rsi_l)
        else:   
            _log(cfg.name, "[ERROR] SELL order failed at IG — state not saved, remaining flat")
            telegram_bitbot(f"[ERROR] {cfg.name} SELL failed at IG — check account")
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
    else:
        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)

# ══════════════════════════════════════════════════════════════════
# Chart representation
# ══════════════════════════════════════════════════════════════════
def _ascii_chart(info: dict, cfg: MarketConfig) -> str:
    """
    Produces a vertical ASCII price chart showing key levels.
    Suitable for sending as a text message via Telegram.
    """
    d1_uk  = info["d1_uk"]
    d1_eve = info["d1_eve"]
    d2_uk  = info["d2_uk"]
    d2_eve = info["d2_eve"]
    open_p = info["open_price"]
    ceil   = info["entry_ceiling"]
    ma20   = info["ma20"]
    quad   = info["quadrant"]
    above  = info["above_ma"]

    # All levels we want to show
    levels = {
        "MA20":    ma20,
        "D2-UK":   d2_uk,
        "D2-EVE":  d2_eve,
        "D1-UK":   d1_uk,
        "D1-EVE":  d1_eve,
        "CEIL":    ceil,
        "OPEN":    open_p,
    }

    # Sort high to low
    sorted_levels = sorted(levels.items(), key=lambda x: x[1], reverse=True)

    # Build chart
    lines = []
    lines.append(f"{'─'*28}")
    lines.append(f"{cfg.name} | {info['trade_date'].strftime('%d/%m/%Y')}")
    lines.append(f"Quad: {quad}")
    lines.append(f"{'─'*28}")
    lines.append(f"{'LEVEL':<8} {'PRICE':>8}  {'':>6}")

    prev_price = None
    for label, price in sorted_levels:
        # Gap indicator between levels
        if prev_price is not None:
            gap = prev_price - price
            if gap > 20:
                lines.append(f"{'':8} {'  ...':>8}  │")
        
        # Choose marker for each level
        if label == "OPEN":
            marker = "◀ OPEN"
            bar    = "┤"
        elif label == "CEIL":
            marker = "▲ CEIL"
            bar    = "┤"
        elif label == "D1-EVE":
            marker = "◆ D1EV"
            bar    = "┤"
        elif label == "D1-UK":
            marker = "● D1UK"
            bar    = "┤"
        elif label == "D2-UK":
            marker = "● D2UK"
            bar    = "┤"
        elif label == "D2-EVE":
            marker = "◆ D2EV"
            bar    = "┤"
        elif label == "MA20":
            marker = "━ MA20"
            bar    = "┤"
        else:
            marker = f"  {label}"
            bar    = "┤"

        lines.append(f"{marker:<8} {price:>8.1f}  {bar}")
        prev_price = price

    lines.append(f"{'─'*28}")

    # Summary below chart
    regime = "ABOVE MA ↑" if above else "BELOW MA ↓"
    lines.append(f"Regime:  {regime}")
    lines.append(f"Long:    {'✅' if info['long_valid'] else '❌'}")
    lines.append(f"Short:   {'✅' if info['short_valid'] else '❌'}")
    
    if info["e_drift_uk_minus_exception"]:
        lines.append("⚡ E-Drift-UK- exception active")
    lines.append(f"{'─'*28}")
    lines.append(f"T1: {info['t1']:.1f}  T2: {info['t2']:.1f}  T3: {info['t3']:.1f}")

    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════
# SEQUENTIAL LOCK FILE
# ══════════════════════════════════════════════════════════════════

def _read_lock() -> Optional[str]:
    """Return the market name holding the lock, or None if free."""
    if not os.path.exists(SEQUENTIAL_LOCK):
        return None
    try:
        val = open(SEQUENTIAL_LOCK).read().strip().lower()
        return val or None
    except Exception:
        return None

def _write_lock(market: str) -> None:
    with open(SEQUENTIAL_LOCK, "w") as f:
        f.write(market.lower())   # store lowercase to match market_keys

def _clear_lock() -> None:
    try:
        os.remove(SEQUENTIAL_LOCK)
    except FileNotFoundError:
        pass

def _latest_bar_values(prices: dict) -> tuple:
    """
    Returns (o, h, l, c, cb_o, cb_h, cb_l, cb_c) from the most recent
    daily entry. Session-aggregate OHLC plus the last individual 5-min
    bar values for intraday stop/exit checks.
    """
    days = sorted(prices)
    bar  = prices[days[-1]]
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    cb_o = bar.get("bar_open",  o)
    cb_h = bar.get("bar_high",  h)
    cb_l = bar.get("bar_low",   l)
    cb_c = bar.get("bar_close", c)
    return o, h, l, c, cb_o, cb_h, cb_l, cb_c

def _ensure_setup(cfg: MarketConfig, now: datetime) -> bool:
    """
    Guarantee cfg's day_info holds today's setup with current reference
    levels. Returns True if day_info is usable this tick.

    Extracted from run_market's setup block so run_sequential can prepare
    every market BEFORE probing divergence strengths. _get_div_strength
    bails out via _is_today(), so on the first tick of a session — before
    _daily_setup had run — every market reported "no signal", the group
    selection was skipped, and the no-signal fallback ran every market
    with entries enabled and no lock (15-Jul: DAX entered at 06:02 four
    seconds after its own probe said no signal, and took no lock).

    Idempotent: run_market's own setup block becomes a no-op once this has
    run, because _is_today() is then True and the reference already matches.
    """
    if cfg.session_group == "asian":
        hour = datetime.now(timezone.utc).hour
    else:
        hour = now.hour
    if _in_blackout(hour, cfg):
        return False
    if not os.path.exists(cfg.data_path):
        return False
    prices = _load_prices(cfg.data_path, cfg)
    if not prices:
        return False
    if not _is_today(cfg.day_info_path, cfg):
        try:
            _daily_setup(prices, cfg)
        except ValueError as e:
            _log(cfg.name, f"[SETUP] deferred: {e}")
            return False
    else:
        try:
            _fresh_ref = _derive_reference(prices, cfg)
            _stored    = _read_day_info(cfg.day_info_path)
            if _reference_is_stale(_stored, _fresh_ref):
                _refresh_reference(prices, cfg, _fresh_ref)
        except ValueError:
            pass
    return True


def _get_div_strength(cfg: MarketConfig) -> Optional[float]:
    """
    Read-only probe: check current bar for a valid entry divergence signal.
    Returns the RSI divergence strength (pts) if a signal exists, else None.
    Does NOT enter any trade or modify state.
    Used by run_sequential() to compare signals before choosing a market.
    """
    if not os.path.exists(cfg.data_path) or not _is_today(cfg.day_info_path, cfg):
        return None
    prices = _load_prices(cfg.data_path, cfg)
    if not prices:
        return None
    info  = _read_day_info(cfg.day_info_path)
    rsi_c = list(info.get("rsi_closes", []))
    rsi_h = list(info.get("rsi_highs",  []))
    rsi_l = list(info.get("rsi_lows",   []))
    
    
    o, h, l, c, cb_o, cb_h, cb_l, cb_c = _latest_bar_values(prices)
    # Only append the current bar if the stored arrays don't already end with
    # it (run_market records the timestamp of the last bar it appended in
    # last_rsi_bar). On a stale-feed tick the bar is already in the arrays,
    # and appending it again here would duplicate it and skew the probe's RSI.
    _today_key = sorted(prices)[-1]
    _bar_time  = str(prices[_today_key].get("bar_time") or "")
    if not _bar_time or _bar_time != str(info.get("last_rsi_bar") or ""):
        rsi_c.append(cb_c); rsi_h.append(cb_h); rsi_l.append(cb_l)
    rsis  = _calc_rsi(rsi_c, cfg.rsi_period)
    if len(rsi_c) < cfg.rsi_period + 5:
        return None
    ceil = info.get("entry_ceiling", 0)
    if info.get("long_valid"):
        if l <= ceil:
            entry = max(min(o, ceil), l)
            if entry <= ceil:
                found, strength = _bull_div(rsi_l, rsis, cfg)
                if found:
                    return strength
    elif info.get("e_drift_uk_minus_exception"):
        trigger = info["e_drift_uk_minus_trigger"]
        if h >= trigger and trigger <= ceil:
            found, strength = _bull_div(rsi_l, rsis, cfg)
            if found:
                return strength
    elif info.get("short_valid"):
        # Mirror the E+Drift-UK- veto from _check_entries
        if info.get("quadrant") == "E+Drift-UK-":
            return None
        for level in info.get("short_levels", []):
            if level - cfg.div_zone <= h <= level + cfg.div_zone:
                found, strength = _bear_div(rsi_h, rsis, cfg)
                if found:
                    return strength
                break
    return None


# ══════════════════════════════════════════════════════════════════
# SEQUENTIAL RUN  — one trade at a time across all markets
# ══════════════════════════════════════════════════════════════════

def run_sequential(market_keys: list[str], now: datetime, ig=None) -> None:
    """
    Session-aware sequential mode.

    Asian markets (Nikkei 00:00-06:30 UTC, Hang Seng 01:30-08:00 UTC)
    and European markets (08:00-17:00 UTC) trade in separate sessions.
    Both groups can fire on the same calendar day without conflict.
    Within each group, only one market trades at a time.

    Asian priority:    strongest divergence signal wins.
    European priority: div gap ≥5pts → strongest wins;
                       DAX E-Drift-UK- → prefer FTSE/CAC;
                       default: DAX → CAC → FTSE.
    """
    cfgs = {k: MARKET_CONFIGS[k] for k in market_keys}

    # Prepare every market's day_info before any probe runs. Without this the
    # first tick of each session probes stale/absent day_info, _is_today() is
    # False, and _get_div_strength returns None for every market.
    for cfg in cfgs.values():
        _ensure_setup(cfg, now)

    asian_keys    = [k for k in market_keys if cfgs[k].session_group == "asian"]
    european_keys = [k for k in market_keys if cfgs[k].session_group == "european"]

    # Second lock file for the Asian group
    ASIAN_LOCK = SEQUENTIAL_LOCK.replace("market_lock", "asian_lock")

    def read_lock(path):
        if not os.path.exists(path): return None
        try: return open(path).read().strip().lower() or None
        except Exception: return None

    def write_lock(path, market):
        with open(path, "w") as f: f.write(market.lower())

    def clear_lock(path):
        try: os.remove(path)
        except FileNotFoundError: pass

    # ── EU-priority probe ─────────────────────────────────────────
    # European markets outrank Asian ones: when both groups signal on the
    # same tick, EU takes the trade and Asian stands down.
    #
    # Probed here, once, because the Asian group is dispatched first — by the
    # time the EU block is reached the Asian entry has already gone to IG
    # (15-Jul: NIKKEI entered 06:02:08, DAX 06:02:14). The result is reused
    # by the EU block below rather than probed a second time.
    #
    # The groups only contend between 06:00 (EU pre-open) and 06:55 (Hang
    # Seng close). Outside that window the EU markets are in blackout, their
    # day_info is not today's, _get_div_strength returns None, and the Asian
    # group is completely unaffected.
    #
    # Skipped when EU already holds the lock: a market EU is *holding* is not
    # a market EU wants to *enter*, so it must not suppress an Asian entry.
    eu_strengths: dict[str, float] = {}
    eu_probed = False
    if european_keys:
        _eu_lock_now = read_lock(SEQUENTIAL_LOCK)
        if not (_eu_lock_now and _eu_lock_now in european_keys):
            eu_probed = True
            for k in european_keys:
                s = _get_div_strength(cfgs[k])
                if s is not None:
                    eu_strengths[k] = s
                    _debug(cfgs[k].name, f"  [SEQ-EU] signal div={s:.1f}pts")
                else:
                    _debug(cfgs[k].name, "  [SEQ-EU] no signal")
    eu_has_signal = bool(eu_strengths)

    # ── Asian session group ───────────────────────────────────────
    if asian_keys:
        asian_cfgs = {k: cfgs[k] for k in asian_keys}
        asian_lock = read_lock(ASIAN_LOCK)

        if asian_lock and asian_lock in asian_cfgs:
            cfg = asian_cfgs[asian_lock]
            _debug(cfg.name, "  [SEQ-ASIAN] managing open position")
            pos_before = _read_day_info(cfg.day_info_path).get("position")
            run_market(cfg, now, ig_service=ig)
            pos_after  = _read_day_info(cfg.day_info_path).get("position")
            if pos_before and not pos_after:
                clear_lock(ASIAN_LOCK)
                _log(cfg.name, "[SEQ-ASIAN] Position closed — lock released")
            for k, c in asian_cfgs.items():
                if k != asian_lock: run_market(c, now, ig_service=ig, allow_entry=False)
        elif eu_has_signal:
            # EU outranks Asian. Defer entries for this tick only — any open
            # Asian position is still managed normally (the lock branch above
            # handles the lock-holder; this is the flat / unlocked case).
            for k, cfg in asian_cfgs.items():
                _debug(cfg.name, "  [SEQ-ASIAN] EU signalling → entries deferred")
                run_market(cfg, now, ig_service=ig, allow_entry=False)
        else:
            asian_strengths: dict[str, float] = {}
            for k, cfg in asian_cfgs.items():
                s = _get_div_strength(cfg)
                if s is not None:
                    asian_strengths[k] = s
                    _debug(cfg.name, f"  [SEQ-ASIAN] signal div={s:.1f}pts")
                else:
                    _debug(cfg.name, "  [SEQ-ASIAN] no signal")

            if not asian_strengths:
                # No market signalled via the probe — still run each market so
                # management (stops, exits) executes and a signal that only
                # appears once run_market rebuilds the RSI arrays can fire.
                # Enforce the lock invariant here: the first market to open a
                # position takes the lock and every market after it is
                # manage-only. Previously this path ran every market with
                # entries enabled and never wrote a lock, so two markets in the
                # same group could open on the same tick.
                taken = read_lock(ASIAN_LOCK)
                for k, cfg in asian_cfgs.items():
                    if taken:
                        run_market(cfg, now, ig_service=ig, allow_entry=False)
                        continue
                    pos_before = _read_day_info(cfg.day_info_path).get("position")
                    run_market(cfg, now, ig_service=ig)
                    pos_after  = _read_day_info(cfg.day_info_path).get("position")
                    if not pos_before and pos_after:
                        write_lock(ASIAN_LOCK, cfg.name)
                        taken = cfg.name.lower()
                        _log(cfg.name, "[SEQ-ASIAN] Lock acquired (no-probe path)")
            else:
                chosen = max(asian_strengths, key=lambda k: asian_strengths[k])
                chosen_cfg = asian_cfgs[chosen]
                pos_before = _read_day_info(chosen_cfg.day_info_path).get("position")
                run_market(chosen_cfg, now, ig_service=ig)
                pos_after  = _read_day_info(chosen_cfg.day_info_path).get("position")
                if not pos_before and pos_after:
                    write_lock(ASIAN_LOCK, chosen_cfg.name)
                    _log(chosen_cfg.name, "[SEQ-ASIAN] Lock acquired")
                for k, cfg in asian_cfgs.items():
                    if k != chosen: run_market(cfg, now, ig_service=ig, allow_entry=False)

    # ── EU session-boundary handling ──────────────────────────────
    # (a) 16:30 reset: the cash-close force-out happens at 16:25. By 16:30 any
    #     16:25 position has been closed, so clear the EU lock and any preferred-
    #     market state, leaving the framework free to trade the post-close session.
    # (b) 20:55 hard stop: force-close any EU position still open at the eve close
    #     and clear the lock, so nothing is carried overnight.
    if european_keys:
        # 16:30 lock reset (fires in the 16:30–16:34 slot, once).
        # IMPORTANT: only clear the lock if NO EU market is currently holding a
        # position. A blind clear would unlock an open position and let a second
        # EU market open simultaneously, re-introducing the multi-position bug the
        # sequential lock exists to prevent. If a position IS open, keep (or
        # re-point) the lock at that market so sequential mode still holds; the
        # lock will release naturally when that position closes.
        if now.hour == 16 and 30 <= now.minute < 35:
            holder = None
            for k in european_keys:
                try:
                    if _read_day_info(cfgs[k].day_info_path).get("position") in ("LONG", "SHORT"):
                        holder = cfgs[k].name.lower()
                        break
                except Exception:
                    continue
            if holder is None:
                if read_lock(SEQUENTIAL_LOCK) is not None:
                    clear_lock(SEQUENTIAL_LOCK)
                    _log("SEQ-EU", "[RESET] 16:30 — no EU position open, lock cleared; "
                                   "free to trade post-close session")
            else:
                # Ensure the lock points at the market that actually holds the
                # position (self-heal if it drifted), and leave it in place.
                if read_lock(SEQUENTIAL_LOCK) != holder:
                    write_lock(SEQUENTIAL_LOCK, holder)
                _log("SEQ-EU", f"[RESET] 16:30 — {holder.upper()} still holding a "
                               f"position, lock retained")
        # 20:55 hard stop for any EU market still holding a position
        if now.hour == 20 and 55 <= now.minute < 60:
            for k in european_keys:
                cfg = cfgs[k]
                try:
                    info = _read_day_info(cfg.day_info_path)
                except Exception:
                    continue
                pos = info.get("position")
                if pos in ("LONG", "SHORT"):
                    entry_px = float(info.get("entry_px", 0.0) or 0.0)
                    _log(cfg.name, f"[HARD-STOP 20:55] Force-closing {pos} @ eve close "
                                   f"(entry {entry_px:.1f})")
                    telegram_bitbot(f"{cfg.name} | HARD-STOP 20:55 | closing {pos}")
                    already = _close_position(ig, cfg, "EVE_HARD_STOP", 0.0, entry_px,
                                              pos, info,
                                              info.get("rsi_closes", []),
                                              info.get("rsi_highs", []),
                                              info.get("rsi_lows", []))
                    if not already:
                        if pos == "LONG":
                            close_buy_trade(ig, epic=cfg.epic, size=cfg.flat_size)
                        else:
                            close_sell_trade(ig, epic=cfg.epic, size=cfg.flat_size)
                    _save_state(cfg.day_info_path, info, None, 0.0, 0.0,
                                info.get("rsi_closes", []),
                                info.get("rsi_highs", []),
                                info.get("rsi_lows", []))
            if read_lock(SEQUENTIAL_LOCK) is not None:
                clear_lock(SEQUENTIAL_LOCK)
                _log("SEQ-EU", "[HARD-STOP 20:55] EU lock cleared")

    # ── European session group ────────────────────────────────────
    if european_keys:
        eu_cfgs = {k: cfgs[k] for k in european_keys}
        eu_lock = read_lock(SEQUENTIAL_LOCK)

        if eu_lock and eu_lock in eu_cfgs:
            cfg = eu_cfgs[eu_lock]
            _debug(cfg.name, "  [SEQ-EU] managing open position")
            pos_before = _read_day_info(cfg.day_info_path).get("position")
            run_market(cfg, now, ig_service=ig)
            pos_after  = _read_day_info(cfg.day_info_path).get("position")
            if pos_before and not pos_after:
                clear_lock(SEQUENTIAL_LOCK)
                _log(cfg.name, "[SEQ-EU] Position closed — lock released")
            for k, c in eu_cfgs.items():
                if k != eu_lock: run_market(c, now, ig_service=ig, allow_entry=False)
        else:
            # eu_strengths was probed at the top of the tick by the EU-priority
            # gate — reuse it rather than probing every market a second time.
            if not eu_probed:
                # The lock was held when the priority gate ran but has since
                # been released (16:30 reset / 20:55 hard stop). Probe now so
                # strongest-signal selection still applies on that tick.
                for k, cfg in eu_cfgs.items():
                    s = _get_div_strength(cfg)
                    if s is not None:
                        eu_strengths[k] = s
                        _debug(cfg.name, f"  [SEQ-EU] signal div={s:.1f}pts")
                    else:
                        _debug(cfg.name, "  [SEQ-EU] no signal")

            if not eu_strengths:
                # Same lock invariant as the Asian no-probe path above.
                taken = read_lock(SEQUENTIAL_LOCK)
                for k, cfg in eu_cfgs.items():
                    if taken:
                        run_market(cfg, now, ig_service=ig, allow_entry=False)
                        continue
                    pos_before = _read_day_info(cfg.day_info_path).get("position")
                    run_market(cfg, now, ig_service=ig)
                    pos_after  = _read_day_info(cfg.day_info_path).get("position")
                    if not pos_before and pos_after:
                        write_lock(SEQUENTIAL_LOCK, cfg.name)
                        taken = cfg.name.lower()
                        _log(cfg.name, "[SEQ-EU] Lock acquired (no-probe path)")
            else:
                if len(eu_strengths) == 1:
                    chosen_key = next(iter(eu_strengths))
                    _debug(eu_cfgs[chosen_key].name,
                           "  [SEQ-EU] only market signalling → selected")
                else:
                    sorted_keys = sorted(eu_strengths, key=lambda k: -eu_strengths[k])
                    strongest, second = sorted_keys[0], sorted_keys[1]
                    gap = eu_strengths[strongest] - eu_strengths[second]
                    if gap >= 5.0:
                        chosen_key = strongest
                        _debug(eu_cfgs[chosen_key].name,
                               f"  [SEQ-EU] div gap={gap:.1f}pts ≥5 → stronger wins")
                    else:
                        dax_quad = ""
                        if "dax" in eu_cfgs and _is_today(
                                MARKET_CONFIGS["dax"].day_info_path,
                                MARKET_CONFIGS["dax"]):
                            dax_quad = _read_day_info(
                                MARKET_CONFIGS["dax"].day_info_path
                            ).get("quadrant", "")
                        if "dax" in eu_cfgs and dax_quad == "E-Drift-UK-":
                            if   "ftse" in eu_cfgs: chosen_key = "ftse"
                            elif "cac"  in eu_cfgs: chosen_key = "cac"
                            else:                   chosen_key = strongest
                            _debug(eu_cfgs[chosen_key].name,
                                   "  [SEQ-EU] DAX E-Drift-UK- → prefer FTSE/CAC")
                        else:
                            for preferred in ["dax", "cac", "ftse"]:
                                if preferred in eu_cfgs:
                                    chosen_key = preferred; break
                            else:
                                chosen_key = strongest
                            _debug(eu_cfgs[chosen_key].name,
                                   "  [SEQ-EU] default priority")

                chosen_cfg = eu_cfgs[chosen_key]
                pos_before = _read_day_info(chosen_cfg.day_info_path).get("position")
                run_market(eu_cfgs[chosen_key], now, ig_service=ig)
                pos_after  = _read_day_info(chosen_cfg.day_info_path).get("position")
                if not pos_before and pos_after:
                    write_lock(SEQUENTIAL_LOCK, chosen_cfg.name)
                    _log(chosen_cfg.name, "[SEQ-EU] Lock acquired")
                for k, cfg in eu_cfgs.items():
                    if k != chosen_key: run_market(cfg, now, ig_service=ig, allow_entry=False)


# ══════════════════════════════════════════════════════════════════
#Connection
# ══════════════════════════════════════════════════════════════════


# --- CONFIGURATION ---
# Best practice: use environment variables rather than hardcoding credentials
# Set these in your shell or a .env file:
#   export IG_USERNAME="your_username"
#   export IG_PASSWORD="your_password"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="LIVE"   # or "DEMO"
#   export IG_ACC_NUMBER="your_account_number"

IG_USERNAME   = os.environ.get("IG_USERNAME", "")
IG_PASSWORD   = os.environ.get("IG_PASSWORD", "")
IG_API_KEY    = os.environ.get("IG_API_KEY",  "")
IG_ACC_TYPE   = os.environ.get("IG_ACC_TYPE", "LIVE")
IG_ACC_NUMBER = os.environ.get("IG_ACC_NUMBER", "")

def connect():
    """Create and return an authenticated IGService session."""
    print(f"Connecting to IG ({IG_ACC_TYPE}) ...")
    ig = IGService(
        username=IG_USERNAME,
        password=IG_PASSWORD,
        api_key=IG_API_KEY,
        acc_type=IG_ACC_TYPE,  # "DEMO" or "LIVE"
    )
    ig.create_session()
    print("✓ Session created successfully.")
 
    #telegram_bitbot("✓ Session created successfully.")
    return ig

# ══════════════════════════════════════════════════════════════════
#Account info
# ══════════════════════════════════════════════════════════════════

def get_account_info(ig: IGService) -> dict:
    """
    Fetch the current account balance and net asset value from IG.
    Returns {"balance": float, "nnv": float} or {} on failure.
    balance = cash deposited + realised P&L
    nnv     = balance + open position unrealised P&L (net asset value)
    """
    try:
        accounts = ig.fetch_accounts()
        # fetch_accounts returns a DataFrame; find the LIVE account row
        if hasattr(accounts, "iterrows"):
            for _, row in accounts.iterrows():
                if str(row.get("preferred", "")).upper() == "TRUE" \
                        or str(row.get("status", "")).upper() == "ENABLED":
                    return {
                        "balance": float(row.get("balance", 0) or 0),
                        "nnv":     float(row.get("available", 0) or 0),
                    }
            # fallback: first row
            row = accounts.iloc[0]
            return {
                "balance": float(row.get("balance", 0) or 0),
                "nnv":     float(row.get("available", 0) or 0),
            }
    except Exception as ex:
        _log("ACCOUNT", f"fetch_accounts failed: {ex}")
    return {}



def profit_loss(ig: IGService, epic: str = "IX.D.FTSE.DAILY.IP"):
    positions = ig.fetch_open_positions()
    
    message = []
    for _, row in positions.iterrows():
        level      = row['level']
        size       = row['size']
        direction  = row['direction']

        bid = get_live_price(ig, epic)
        
        if direction == 'BUY':
            profit = (bid -level) * size
        else:  # SELL
            profit = -(bid-level) * size


        print(f"{row['instrumentName']} | {direction} | Profit: {profit:.2f}")
        message.append(f"{row['instrumentName']} | {direction} | Profit: {profit:.2f} {row['currency']}")

        telegram_bitbot(message)


# ══════════════════════════════════════════════════════════════════
#Price info
# ══════════════════════════════════════════════════════════════════

def get_live_price(ig: IGService, epic: str = "IX.D.FTSE.DAILY.IP"):
    price = ig.fetch_market_by_epic(epic)
    bid = price["snapshot"]["bid"]
    
    return (bid)

def fetch_deal_id(ig: IGService, instrument):
    """Return the dealId of the first open position.
    Previously returned str({dealId}) — a stringified set; now returns the plain string."""
    positions = ig.fetch_open_positions()
    for _, row in positions.iterrows():
        return str(row['dealId'])
    return None

# ── Free-source gap fill (Yahoo Finance) ───────────────────────────
# Cash-index symbols corresponding to each price CSV. Yahoo's intraday
# 5-min history reaches back ~60 days and costs nothing, making it the
# fallback source for feed holes so IG's 10,000-point weekly allowance is
# reserved for live bars.
YF_SYMBOLS = {
    "Fprice.csv":  "^FTSE",
    "Dprice.csv":  "^GDAXI",
    "Cprice.csv":  "^FCHI",
    "Nprice.csv":  "^N225",
    "HSprice.csv": "^HSI",
}
# Holes of more than this many 5-min slots (3 slots = 15 minutes) are
# patched from Yahoo before anything is requested from IG.
YF_FILL_MIN_SLOTS = 3
# After an unproductive Yahoo attempt (<=1 bar written), leave Yahoo alone
# for this many minutes — hammering every 5-min tick invites throttling.
YF_RETRY_MIN = 15
# The streamer writes every completed 5-min bar. Any gap in the CSV means
# the streamer missed bars (brief disconnect, server restart). IG REST fills
# immediately on any gap, but capped to this many bars — the streamer is
# the primary source so gaps should always be small. Anything larger than
# this cap means a real outage; Yahoo handles the bulk, IG cleans the tail.
STREAMER_MAX_IG_BARS = 12


def _yahoo_backfill(save_path: str, storage_file: str,
                    gap_start, gap_end) -> int:
    """
    Patch a feed hole (gap_start, gap_end] in `save_path` using Yahoo's
    cash-index 5-min bars. Returns the number of bars written (0 on any
    failure — the caller then falls back to IG sizing as before).

    FETCH STYLE: uses `period=` rather than start/end. Naive start/end
    datetimes are interpreted inconsistently across yfinance versions
    (13-Jul incident: every attempt returned ~1 usable bar, so a 2-hour
    hole advanced one slot per tick and never closed). A period request
    has no timezone ambiguity; we filter to the gap window ourselves in
    UTC and log exactly what Yahoo served so the next incident is
    diagnosable from trading.log alone.

    THROTTLE: an attempt that writes <=1 bar touches a marker file and
    Yahoo is left alone for YF_RETRY_MIN minutes — repeated identical
    requests every 5 minutes invite Yahoo-side throttling and cannot help
    if the data simply isn't there (delayed feed, closed market).

    WHY OFFSET-ANCHORING IS SAFE FOR THE RSI: Yahoo serves the cash index,
    which trades at a slightly different absolute level from IG's CFD
    quote (fair-value/dividend basis, typically a handful of points). A
    constant offset applied to the whole patched segment leaves every
    close-to-close difference inside the segment unchanged — and Wilder's
    RSI is computed purely from those differences — so anchoring the
    segment to the last real IG close makes the boundary delta exact and
    the RSI undistorted. The residual error is only the basis DRIFT over
    the gap window (usually a point or two per hour), absorbed at the
    right-hand boundary when real IG bars resume.

    LIMITS, stated plainly: the cash index only trades exchange hours, so
    holes outside them (e.g. EU evening bars incl. the 20:55 eve close)
    cannot be patched from Yahoo and are left unfilled; Yahoo intraday
    history is ~60 days; delayed quotes mean the most recent ~15 minutes
    may be unavailable; and yfinance is an unauthenticated public API —
    treated as best-effort, never load-bearing.
    """
    symbol = YF_SYMBOLS.get(storage_file)
    if symbol is None:
        return 0

    # Unproductive-attempt throttle.
    mark = save_path + ".yfmark"
    try:
        mark_age_min = (datetime.now().timestamp()
                        - os.path.getmtime(mark)) / 60.0
        if mark_age_min < YF_RETRY_MIN:
            return 0
    except OSError:
        pass

    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        _log(storage_file, "[FEED] yfinance not installed — "
                           "pip install yfinance to enable free gap fill")
        return 0

    # Anchor value: close of the last real IG bar (at gap_start).
    ig_anchor_close = None
    try:
        with open(save_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 8192))
            tail = f.read().decode("utf-8", errors="ignore")
        want = gap_start.strftime("%Y-%m-%dT%H:%M")
        for line in tail.splitlines():
            p = line.split(",")
            if len(p) >= 5 and p[0].strip().startswith(want):
                ig_anchor_close = float(p[4])
                break
    except (OSError, ValueError):
        pass

    gap_hours = (gap_end - gap_start).total_seconds() / 3600.0
    period = "5d" if gap_hours > 40 else "2d"
    try:
        df = yf.download(symbol, period=period, interval="5m",
                         progress=False, auto_adjust=False)
    except Exception as e:
        _log(storage_file, f"[FEED] Yahoo fetch failed: {type(e).__name__}: {e}")
        try:
            open(mark, "w").write("fail")
        except OSError:
            pass
        return 0
    if df is None or len(df) == 0:
        _log(storage_file, f"[FEED] Yahoo returned no bars for {symbol} "
                           f"(period={period})")
        try:
            open(mark, "w").write("empty")
        except OSError:
            pass
        return 0

    # Normalise: single-level columns, naive-UTC index.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    idx = df.index
    if getattr(idx, "tz", None) is not None:
        df.index = idx.tz_convert("UTC").tz_localize(None)

    # Diagnostics: what did Yahoo actually serve?
    _log(storage_file, f"[FEED] Yahoo served {len(df)} rows for {symbol} "
                       f"({df.index.min():%m-%d %H:%M} → "
                       f"{df.index.max():%m-%d %H:%M} UTC), "
                       f"filling ({gap_start:%H:%M} → {gap_end:%H:%M}]")

    # Offset from the overlapping (or nearest preceding) Yahoo bar.
    # None sentinel — a genuine 0.00 offset is possible and is NOT a
    # missing-anchor condition.
    offset = None
    if ig_anchor_close is not None:
        pre = df[df.index <= gap_start]
        if len(pre) and (gap_start - pre.index[-1].to_pydatetime()
                         ).total_seconds() <= 3600:
            try:
                offset = ig_anchor_close - float(pre["Close"].iloc[-1])
            except (KeyError, TypeError, ValueError):
                offset = None
    if offset is None:
        _log(storage_file, "[FEED] Yahoo fill: no overlapping anchor bar — "
                           "writing unadjusted cash-index levels")
        offset = 0.0

    written = 0
    nan_dropped = 0
    try:
        with open(save_path, "a") as f:
            for ts, row in df.iterrows():
                ts_py = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                if not (gap_start < ts_py <= gap_end):
                    continue
                try:
                    o = float(row["Open"]) + offset
                    h = float(row["High"]) + offset
                    l = float(row["Low"]) + offset
                    c = float(row["Close"]) + offset
                except (KeyError, TypeError, ValueError):
                    nan_dropped += 1
                    continue
                if any(v != v for v in (o, h, l, c)):   # NaN guard
                    nan_dropped += 1
                    continue
                f.write(f"{ts_py.strftime('%Y-%m-%dT%H:%M:%S')},"
                        f"{o:.2f},{h:.2f},{l:.2f},{c:.2f}\n")
                written += 1
    except OSError as e:
        _log(storage_file, f"[FEED] Yahoo fill write failed: {e}")
    if written:
        _log(storage_file, f"[FEED] Yahoo fill: {written} bars "
                           f"({symbol}, offset {offset:+.2f}, "
                           f"{nan_dropped} NaN-dropped) into "
                           f"({gap_start:%H:%M} → {gap_end:%H:%M}]")
    if written <= 1:
        # Unproductive — back off so we don't hammer Yahoo every tick for
        # data that isn't there (delayed quote, closed session, throttling).
        try:
            open(mark, "w").write(f"unproductive:{written}")
        except OSError:
            pass
        if written == 0:
            _log(storage_file, f"[FEED] Yahoo fill unproductive "
                               f"({nan_dropped} NaN rows in window) — "
                               f"retry in {YF_RETRY_MIN}min")
    else:
        # Productive fill: clear any stale marker so recovery is immediate.
        try:
            os.remove(mark)
        except OSError:
            pass
    return written


def _allowance_backoff_active() -> bool:
    """True while the IG data-allowance backoff marker is fresh."""
    try:
        age_min = (datetime.now().timestamp()
                   - os.path.getmtime(ALLOWANCE_BACKOFF_FILE)) / 60.0
        return age_min < ALLOWANCE_BACKOFF_MIN
    except OSError:
        return False


def _enter_allowance_backoff() -> bool:
    """Record an allowance-exhausted episode. Returns True if this call
    STARTED the episode (caller should alert), False if already active."""
    already = _allowance_backoff_active()
    try:
        with open(ALLOWANCE_BACKOFF_FILE, "w") as f:
            f.write(datetime.now().isoformat())
    except OSError:
        pass
    return not already


def get_price_history(ig: IGService, storage_file, epic: str = "IX.D.FTSE.DAILY.IP",
                      resolution: str = "D", num_points: int = 10):
    """
    Fetch recent historical prices for a given EPIC.
    Common EPICs:
      CS.D.GBPUSD.TODAY.IP  -> GBP/USD
      IX.D.FTSE.DAILY.IP    -> FTSE 100
      IX.D.NASDAQ.IFA.IP    -> NASDAQ
   
    result = ig.fetch_historical_prices_by_epic(epic='IX.D.NASDAQ.IFA.IP')
    return result
    """
    # epic = 'CS.D.EURUSD.MINI.IP'
    epic = epic 
    # epic = "CS.D.GBPUSD.CFD.IP"  # sample CFD epic

    resolution = "5Min"
    save_path = "/home/john/projects/firstproject/" + storage_file

    # ── Gap-aware fetch ────────────────────────────────────────────
    # Previously hardcoded num_points=1: after any outage (IG 500s, network
    # drop, crash) the bars for the outage window were simply never written —
    # the CSV kept a permanent hole (e.g. Dprice.csv 04:00→05:25 on 07-Jul)
    # and the RSI series was blind to that window for the rest of the day.
    # Now: look at the last timestamp already in the CSV and request enough
    # recent bars to cover the hole (IG returns the most recent N bars;
    # Append_prices_to_file dedupes by timestamp, so overlap is harmless).
    num_points = 1
    last_ts = _last_csv_timestamp(save_path)
    if last_ts is not None:
        utc_now = datetime.now()
        behind = (utc_now - last_ts).total_seconds() / 60.0

        # ── Weekend guard ───────────────────────────────────────────
        wd = utc_now.weekday()
        if wd == 5 or (wd == 6 and utc_now.hour < 23):
            fillable = 0.0
        else:
            if wd == 6:
                reopen = utc_now.replace(hour=23, minute=0, second=0, microsecond=0)
            else:
                reopen = (utc_now - timedelta(days=wd + 1)).replace(
                    hour=23, minute=0, second=0, microsecond=0)
            since_reopen = (utc_now - reopen).total_seconds() / 60.0
            fillable = min(behind, max(since_reopen, 0.0))
        missing_slots = int(fillable // 5)

        # ── Priority 1: Streamer ────────────────────────────────────
        # The streamer writes every completed 5-min bar directly to the CSV.
        # Any gap means it missed bars (brief disconnect, restart). Backfill
        # immediately from IG, capped at STREAMER_MAX_IG_BARS — gaps should
        # always be small while the streamer is healthy. No tolerance window:
        # react to any gap straight away rather than waiting to see if the
        # streamer catches up by itself.
        if missing_slots == 0:
            return   # streamer is fully current — nothing to do

        # ── Priority 2: IG REST historical data ─────────────────────
        # Fill the gap authoritatively (correct basis, includes eve bars).
        # Capped at STREAMER_MAX_IG_BARS: if the gap is larger the streamer
        # has been down for a meaningful window and Yahoo handles the bulk
        # (via the failure path below), with IG catching the recent tail
        # once it recovers.
        if not _allowance_backoff_active():
            num_points = min(missing_slots + 1, STREAMER_MAX_IG_BARS)
            if missing_slots + 1 > STREAMER_MAX_IG_BARS:
                _log(storage_file, f"[FEED] streamer gap {behind:.0f}min — "
                                   f"IG filling last {num_points} bars "
                                   f"(larger hole deferred to Yahoo fallback)")
            else:
                _log(storage_file, f"[FEED] streamer missed {missing_slots} bar(s) "
                                   f"(last bar {last_ts:%H:%M}) — "
                                   f"backfilling {num_points} from IG")
    try:
        data = ig.fetch_historical_prices_by_epic_and_num_points(
            epic, resolution, num_points)
    except Exception as e:
        _log(storage_file, f"Error retrieving historical prices: {type(e).__name__}: {e}")
        if "historical-data-allowance" in str(e):
            # IG allowance exhausted — the only case where Yahoo is used.
            # For any other failure (500, network drop, timeout) we log and
            # return without touching Yahoo: those are transient and the next
            # cron tick will retry. Yahoo is delayed (~15min), cash-index
            # basis, and unauthenticated — it should never be used unless IG
            # has explicitly refused us on allowance grounds.
            if _enter_allowance_backoff():
                telegram_bitbot(f"IG historical-data allowance EXHAUSTED — "
                                f"falling back to Yahoo Finance for gap fill. "
                                f"Entries interlocked until IG data resumes.")
            if last_ts is not None:
                filled = _yahoo_backfill(save_path, storage_file,
                                         last_ts, datetime.now())
                if filled:
                    _log(storage_file, f"[FEED] Yahoo fallback: {filled} bars "
                                       f"written after IG allowance exhausted")
        else:
            # Transient IG failure (500, network drop, etc.) — log and wait.
            # The next 5-min tick will retry automatically. Do not fall back
            # to Yahoo: a single missed tick is harmless and Yahoo's delayed
            # cash-index prices are not worth the basis difference.
            telegram_bitbot(f"{storage_file}: IG fetch failed "
                            f"({type(e).__name__}) — will retry next tick")
        return

    # Success path only — `data` is guaranteed to exist here.
    if not data or "prices" not in data or data["prices"] is None or len(data["prices"]) == 0:
        _log(storage_file, "Price fetch returned no rows — skipping append this tick")
        return

    save_path = "/home/john/projects/firstproject/" + storage_file
    Append_prices_to_file(save_path, data)
    print(data["prices"])


def _last_csv_timestamp(path: str):
    """
    Return the newest parseable timestamp in a price CSV, or None.
    Reads only the file tail and tolerates every datetime variant seen in
    these files ('T' or space separator, with/without seconds, YYYY-MM-DD or
    DD-MM-YYYY) plus corrupt rows (#NUM!, #VALUE!, blanks) — the newest
    parseable row wins, so one bad tail row doesn't disable backfill.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 8192))
            tail = f.read().decode("utf-8", errors="ignore")
    except OSError:
        return None
    fmts = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d-%m-%YT%H:%M:%S", "%d-%m-%YT%H:%M",
            "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M")
    newest = None
    for line in tail.splitlines():
        ts = line.split(",")[0].strip()
        if not ts:
            continue
        for fmt in fmts:
            try:
                dt = datetime.strptime(ts, fmt)
                if newest is None or dt > newest:
                    newest = dt
                break
            except ValueError:
                continue
    return newest

def Append_prices_to_file(path_to_file, data):
 
    save_path = path_to_file
    print(f"saving historic prices to {save_path}")
    
    '''
    this has been removed as I have hard coded the csv file locations
    # Check if file already exists to decide whether to write header
    file_exists = save_path.exists()
    '''
    data["prices"].to_csv(
        save_path,
        mode="a",                 # append instead of overwrite
        #header=not file_exists,   # only write header if file is new
        date_format="%Y-%m-%dT%H:%M:%S%z"
    )
    
    # Read back, dedupe by timestamp, drop corrupt rows, and overwrite.
    # Rows like '#NUM!', '#VALUE!' or blanks (seen in Nprice/HSprice) would
    # otherwise persist forever and break downstream parsers; a row only
    # survives the rewrite if its timestamp looks real and all four price
    # fields parse as floats.
    def _row_ok(key, cols):
        if not key or not any(ch.isdigit() for ch in key):
            return False
        if len(cols) < 4:
            return False
        try:
            for v in cols[:4]:
                float(v)
        except (TypeError, ValueError):
            return False
        return True

    with open(save_path, "r") as f:
        reader = csv.reader(f)
        rows = {row[0]: row[1:5] for row in reader if row}  # dict keyed by timestamp, dedupes
    rows = {k: v for k, v in rows.items() if _row_ok(k, v)}

    # Sort by timestamp before writing back so the file is always
    # chronologically ordered regardless of the order bars arrived
    # (streamer, REST backfill, and Yahoo can all write out of sequence).
    def _ts_sort_key(ts_str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
                    "%d-%m-%YT%H:%M:%S", "%d-%m-%YT%H:%M"):
            try:
                return datetime.strptime(ts_str.strip(), fmt)
            except ValueError:
                continue
        return datetime.min

    with open(save_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([[index] + cols
                          for index, cols in sorted(rows.items(),
                                                    key=lambda x: _ts_sort_key(x[0]))])
        

def search_markets(ig: IGService, search_term: str = "FTSE"):
    """Search for markets by name."""
    results = ig.search_markets(search_term)
    print(f"\nMarket search results for '{search_term}':\n{results}")
    
    
    return results

def telegram_bitbot(message):
    
    TOKEN = "8042570662:AAFtTOk-H4eg7ssErDBWFSbXZMjCvrk-RfM"
    chat_id = "8690491455"
    message = message
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json()) 

def open_buy_trade(ig: IGService,
                   epic: str = 'IX.D.FTSE.DAILY.IP',
                   size: float = 0.1,
                   stop_distance: float = 35) -> Optional[str]:
    """
    Open a long (BUY) spread-bet position at market price.

    Args:
        ig:            Active IGService session.
        epic:          IG market epic, e.g. 'IX.D.DAX.IMF.IP'.
        size:          Stake in £/pt.
        stop_distance: Points distance for the initial hard stop.

    Returns:
        deal_reference string on success, None on failure.
    """
    _log("IG", f"trying open buy epic={epic}  size={size}")  
    try:
        result = ig.create_open_position(
            currency_code          = 'GBP',
            direction              = 'BUY',
            epic                   = epic,
            expiry                 = 'DFB',
            force_open             = 'true',
            guaranteed_stop        = 'false',
            level                  = None,
            limit_distance         = None,
            limit_level            = None,
            order_type             = 'MARKET',
            quote_id               = None,
            size                   = size,
            stop_distance          = stop_distance,
            stop_level             = None,
            trailing_stop          = 'false',
            trailing_stop_increment= None,
        )
        deal_ref = result.get('dealReference') if isinstance(result, dict) else None
        _log("IG", f"[OPEN_BUY] epic={epic}  size={size}  stop_dist={stop_distance}  ref={deal_ref}")
        return deal_ref
    except Exception as e:
        _log("IG", f"[ERROR] open_buy_trade failed for {epic}: {e}")
        return None


def open_sell_trade(ig: IGService,
                    epic: str = 'IX.D.FTSE.DAILY.IP',
                    size: float = 0.1,
                    stop_distance: float = 35) -> Optional[str]:
    """
    Open a short (SELL) spread-bet position at market price.

    Args:
        ig:            Active IGService session.
        epic:          IG market epic.
        size:          Stake in £/pt.
        stop_distance: Points distance for the initial hard stop.

    Returns:
        deal_reference string on success, None on failure.

    Note: This OPENS a new short position — it is NOT the same as
    closing an existing long. Use close_buy_trade() to close a long.
    """
    _log("IG", f"trying open sell epic={epic}  size={size} stop distance={stop_distance}")    
    try:
        result = ig.create_open_position(
            currency_code          = 'GBP',
            direction              = 'SELL',
            epic                   = epic,
            expiry                 = 'DFB',
            force_open             = 'true',
            guaranteed_stop        = 'false',
            level                  = None,
            limit_distance         = None,
            limit_level            = None,
            order_type             = 'MARKET',
            quote_id               = None,
            size                   = size,
            stop_distance          = stop_distance,
            stop_level             = None,
            trailing_stop          = 'false',
            trailing_stop_increment= None,
        )
        deal_ref = result.get('dealReference') if isinstance(result, dict) else None
        _log("IG", f"[OPEN_SELL] epic={epic}  size={size}  stop_dist={stop_distance}  ref={deal_ref}")
        return deal_ref
    except Exception as e:
        _log("IG", f"[ERROR] open_sell_trade failed for {epic}: {e}")
        return None


def _fetch_deal_id_for_epic(ig: IGService, epic: str) -> Optional[str]:
    """
    Return the dealId of the first open position matching epic, or None.
    Handles both DataFrame and dict responses from fetch_open_positions().
    """
    if ig is None:
        return None
    try:
        positions = ig.fetch_open_positions()
        if hasattr(positions, 'iterrows'):
            # DataFrame path (older trading-ig versions)
            _log("IG", "fetching open positions")  
            for _, row in positions.iterrows():
                if row.get('epic', '') == epic:
                    return str(row['dealId'])
        elif isinstance(positions, dict) and 'positions' in positions:
            # Dict path (newer trading-ig versions)
            _log("IG", "fetching open positions 2")  
            for p in positions['positions']:
                if p.get('market', {}).get('epic', '') == epic:
                    return p['position']['dealId']
    except Exception as e:
        _log("IG", f"[ERROR] _fetch_deal_id_for_epic {epic}: {e}")
    return None


def close_buy_trade(ig: IGService,
                    epic: str = 'IX.D.FTSE.DAILY.IP',
                    size: float = 0.1) -> bool:
    """
    Close an existing long (BUY) position via close_open_position().

    Uses the deal_id of the open position — NOT create_open_position()
    which would open a new opposing trade and double exposure.
    Closing a BUY requires direction='SELL' in the close call.

    Args:
        ig:   Active IGService session.
        epic: Epic of the position to close.
        size: Must match the original open position size exactly.

    Returns:
        True on success, False on failure.
    """
    deal_id = _fetch_deal_id_for_epic(ig, epic)
    _log("IG", f"trying close buy epic={epic}  size={size}")  

    try:
        _log("IG", f"[CLOSE_BUY] epic={epic}  deal_id={deal_id}  size={size}")
        ig.close_open_position(
            deal_id    = deal_id,
            direction  = 'SELL',   # closing a BUY requires direction=SELL
            epic       = None,      # must be None when deal_id is supplied
            expiry     = 'DFB',
            level      = None,
            order_type = 'MARKET',
            quote_id   = None,
            size       = size,
        )
        return True
    except Exception as e:
        _log("IG", f"[ERROR] close_buy_trade failed for {epic} deal {deal_id}: {e}")
        return False


def close_sell_trade(ig: IGService,
                     epic: str = 'IX.D.FTSE.DAILY.IP',
                     size: float = 0.1) -> bool:
    """
    Close an existing short (SELL) position via close_open_position().

    Closing a SELL requires direction='BUY' in the close call.

    Args:
        ig:   Active IGService session.
        epic: Epic of the position to close.
        size: Must match the original open position size exactly.

    Returns:
        True on success, False on failure.
    """
    deal_id = _fetch_deal_id_for_epic(ig, epic)
    _log("IG", f"trying close sell epic={epic}  size={size}")  
  
    try:
        _log("IG", f"[CLOSE_SELL] epic={epic}  deal_id={deal_id}  size={size}")
        ig.close_open_position(
            deal_id    = deal_id,
            direction  = 'BUY',    # closing a SELL requires direction=BUY
            epic       = None,      # must be None when deal_id is supplied
            expiry     = 'DFB',
            level      = None,
            order_type = 'MARKET',
            quote_id   = None,
            size       = size,
        )
        return True
    except Exception as e:
        _log("IG", f"[ERROR] close_sell_trade failed for {epic} deal {deal_id}: {e}")
        return False
     

def activity(ig: IGService, delta):
    from datetime import datetime, timedelta

    to_date = datetime.now()
    from_date = to_date - timedelta(hours=delta)
    act = ig.fetch_transaction_history(from_date=from_date, to_date=to_date)

    # Keep only columns 2, 3, 5, 7, 9 (0-indexed: 1, 2, 4, 6, 8)
    filtered = act.iloc[:, [0,3, 5, 8,9, 10]]

    telegram_bitbot(filtered)








# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main() -> None:
    import argparse

    p = argparse.ArgumentParser(
        description="RSI Divergence Framework — multi-market cron script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python3 framework.py ftse dax               # independent (each manages own position)
  python3 framework.py --sequential ftse dax   # sequential (one trade at a time)
  python3 framework.py ftse                    # FTSE only
  python3 framework.py dax                     # DAX only
  python3 framework.py --sequential all        # all markets sequential
  python3 framework.py --list                  # show configured paths
        """,
    )
    p.add_argument("markets", nargs="*", metavar="MARKET",
                   choices=list(MARKET_CONFIGS) + ["all"],
                   help="Market(s) to process: ftse  dax  all")
    p.add_argument("--sequential", action="store_true",
                   help="One trade at a time across all markets. Priority rules pick which market fires.")
    p.add_argument("--list", action="store_true",
                   help="Print configured markets and paths, then exit")
    args = p.parse_args()

    if args.list:
        print("\nConfigured markets:")
        for key, cfg in MARKET_CONFIGS.items():
            print(f"  {key.upper()}")
            print(f"    prices   → {cfg.data_path}")
            print(f"    day_info → {cfg.day_info_path}")
            print(f"    stops    → long={cfg.sl_long}pts  short={cfg.sl_short}pts")
            print(f"    zones    → golden={cfg.golden_rule}pts  div={cfg.div_zone}pts")
        sys.exit(0)

    if not args.markets:
        p.print_help(); sys.exit(1)

    # ── Weekend gate: exit BEFORE connecting or fetching ───────────
    # IG's index feeds are closed from Friday close until ~23:00 UTC Sunday,
    # so there is nothing to fetch and no gap that can be filled. This gate
    # previously sat AFTER the five get_price_history calls, which meant the
    # cron logged in to IG and requested historical bars every 5 minutes all
    # weekend. With gap-aware backfill sizing that becomes pathological: the
    # Friday->now gap maxes the request at 288 bars x 5 markets = 1,440
    # points per tick, exhausting IG's 10,000-point weekly historical-data
    # allowance ~35 minutes into Saturday and erroring every fetch until the
    # allowance resets — i.e. no price data on Monday. Exiting here costs
    # nothing: the closed-period bars don't exist, and Monday's first tick
    # backfills anything from the Sunday-evening reopen in one request.
    now = _london_now()
    if now.weekday() >= 5:   # 5=Saturday, 6=Sunday
        sys.exit(0)

    ig = connect()

    
    get_price_history(ig, "Dprice.csv",  "IX.D.DAX.DAILY.IP")
    get_price_history(ig, "Fprice.csv",  "IX.D.FTSE.DAILY.IP")
    get_price_history(ig, "Cprice.csv",  "IX.D.CAC.DAILY.IP")
    get_price_history(ig, "Nprice.csv",  "IX.D.NIKKEI.DAILY.IP")
    get_price_history(ig, "HSprice.csv", "IX.D.HANGSENG.DAILY.IP")
    
    markets = list(MARKET_CONFIGS) if "all" in args.markets else args.markets


    if getattr(args, 'sequential', False):
        if len(markets) < 2:
            print("NOTE: --sequential with one market is the same as independent mode.")
        try:
            run_sequential(markets, now, ig=ig)
        except Exception as e:
            _log("SEQ", f"[ERROR] {e}")
    else:
        for key in markets:
            cfg = MARKET_CONFIGS[key]
            try:
                run_market(cfg, now, ig_service=ig)
            except Exception as e:
                _log(cfg.name, f"[ERROR] {e}")


if __name__ == "__main__":
    main()
