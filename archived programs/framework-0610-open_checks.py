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


# ══════════════════════════════════════════════════════════════════
# MARKET CONFIGURATION
# ══════════════════════════════════════════════════════════════════

@dataclass
class MarketConfig:
    """All market-specific settings in one place."""
    name          : str
    epic          : str = ""    
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
    flat_size          : float = 0.1   # £ per point (flat mode)
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
    pre_open_start_hour : int = 3      # hour pre-open begins (UTC)
    blackout_start_hour : int = 21     # hour blackout begins (UTC)
    blackout_end_hour   : int = 3      # hour blackout ends (exclusive)
    # ── Per-market CSV session filter times ──────────────────────
    csv_session_start : str = "08:00"  # first bar included in session
    csv_session_end   : str = "16:30"  # last bar included in session
    csv_eve_close_t   : str = "20:55"  # time of overnight/eve close bar
    # ── Session group for session-aware sequential ───────────────
    # "european" = 08:00-17:00 UTC   "asian" = 00:00-08:00 UTC
    # Markets in different groups can trade on the same calendar day
    # without conflict — their sessions don't overlap.
    session_group : str = "european"



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
        flat_size=10.0,
        session_open_hour=8, 
        epic="IX.D.FTSE.DAILY.IP",
        pre_open_start_hour=3,
        blackout_start_hour=21, 
        blackout_end_hour=3,
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
        session_close_hour   = 16,   # force-close at 16:55 (DAX cash close 17:00 Frankfurt / 16:00 UTC)
        session_close_minute = 55,
        # Sizing — choose one mode:
        # Note: DAX equivalent of £10/pt FTSE = £22.56/pt DAX (ratio 8200/18500)
        # Set flat_size to your desired £/pt on the DAX instrument directly.
        sizing_mode       = "flat",  # "flat" or "compound"
        flat_size         = 10.0,    # £ per point on DAX (adjust to match FTSE exposure)
        compound_fraction = 500.0,   # 1/500th of account
        account_balance   = 10000.0, # update to your actual balance
        session_open_hour=8, 
        epic="IX.D.DAX.IMF.IP",
        pre_open_start_hour=3,
        blackout_start_hour=21, 
        blackout_end_hour=3,
        csv_session_start="08:00", 
        csv_session_end="16:30",
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
        flat_size         = 10.0,
        compound_fraction = 500.0,
        account_balance   = 10000.0,
        session_open_hour=8,
        epic="IX.D.CAC.IMF.IP",
        pre_open_start_hour=3,
        blackout_start_hour=21, 
        blackout_end_hour=3,
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
        flat_size = 2.0,
        epic="IX.D.NIKKEI.IFD.IP",
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
        session_close_hour=7, 
        session_close_minute=55,
        # £4/pt ≈ £10/pt FTSE-equiv (20000/8200 ratio)
        sizing_mode="flat", 
        flat_size=4.0,
        epic="IX.D.HANGSENG.IFD.IP",
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
# ── Time windows (London time, same for all markets) ──────────────
BLACKOUT_START = 21
BLACKOUT_END   =  3
PRE_OPEN_START =  3
SESSION_OPEN   =  8


# ══════════════════════════════════════════════════════════════════
# BROKER INTEGRATION  — replace with your broker API calls
# ══════════════════════════════════════════════════════════════════

def BUY_FTSE(price: float, stop: float, notes: str = "") -> None:
    _log("FTSE", f"[BUY]  @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"FTSE | BUY | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message)

    

def SELL_FTSE(price: float, stop: float, notes: str = "") -> None:
    _log("FTSE", f"[SELL] @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"FTSE | SELL | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message)
    

def BUY_DAX(price: float, stop: float, notes: str = "") -> None:
    _log("DAX",  f"[BUY]  @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"DAX | BUY | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message)   
    

def SELL_DAX(price: float, stop: float, notes: str = "") -> None:
    _log("DAX",  f"[SELL] @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"DAX | SELL | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message) 

def BUY_CAC(price: float, stop: float, notes: str = "") -> None:
    _log("CAC",  f"[BUY]  @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"CAC | BUY | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message)
    #open_buy_trade(ig_session, epic="IX.D.CAC.IMF.IP")

def SELL_CAC(price: float, stop: float, notes: str = "") -> None:
    _log("CAC",  f"[SELL] @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"CAC | SELL | {price:.1f} | stop={stop:.1f} | {notes}"
    telegram_bitbot(message)
    #open_sell_trade(ig_session, epic="IX.D.CAC.IMF.IP")

def BUY_NIKKEI(price: float, stop: float, notes: str = "") -> None:
    _log("NIKKEI", f"[BUY]  @ {price:.0f}  stop={stop:.0f}  | {notes}")
    message = f"NIKKEI | BUY | {price:.0f} | stop={stop:.0f} | {notes}"
    telegram_bitbot(message)
    #open_buy_trade(ig_session, epic="IX.D.NIKKEI.IFD.IP")

def SELL_NIKKEI(price: float, stop: float, notes: str = "") -> None:
    _log("NIKKEI", f"[SELL] @ {price:.0f}  stop={stop:.0f}  | {notes}")
    message = f"NIKKEI | SELL | {price:.0f} | stop={stop:.0f} | {notes}"
    telegram_bitbot(message)
    #open_sell_trade(ig_session, epic="IX.D.NIKKEI.IFD.IP")

def BUY_HANGSENG(price: float, stop: float, notes: str = "") -> None:
    _log("HANGSENG", f"[BUY]  @ {price:.0f}  stop={stop:.0f}  | {notes}")
    message = f"HANGSENG | BUY | {price:.0f} | stop={stop:.0f} | {notes}"
    telegram_bitbot(message)
    #open_buy_trade(ig_session, epic="IX.D.HANGSENG.IFD.IP")

def SELL_HANGSENG(price: float, stop: float, notes: str = "") -> None:
    _log("HANGSENG", f"[SELL] @ {price:.0f}  stop={stop:.0f}  | {notes}")
    message = f"HANGSENG | SELL | {price:.0f} | stop={stop:.0f} | {notes}"
    telegram_bitbot(message)
    #open_sell_trade(ig_session, epic="IX.D.HANGSENG.IFD.IP")
    
    

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
    if ig_service is None:
        return -1   # no session — assume open, proceed with close
    try:
        if open_count == 0:
            # Position already closed at IG (manual close, stop hit at broker, etc.)
            msg = (f"[{reason}] {cfg.name} {position} @ {entry_px:.1f} "
                   f"already closed at IG — skipping close | "
                   f"last_price={price:.1f} | P&L≈{pnl:+.1f}pts")
            _log(cfg.name, msg)
            telegram_bitbot(msg)
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return True   # treated as closed — state cleared

        # Position confirmed open (or API error returned -1) — proceed with close
        return False      # caller should execute the close normally

def _calc_size(cfg: MarketConfig) -> float:
    """
    Calculate the position size (£ per point) for this market.

    flat mode:     returns cfg.flat_size directly.
    compound mode: returns cfg.account_balance / cfg.compound_fraction.

    The returned value is passed to BUY_*/SELL_* so you can use it
    inside those functions to size the order with your broker API.
    """
    if cfg.sizing_mode == "compound":
        return round(cfg.account_balance / cfg.compound_fraction, 4)
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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d-%m-%Y"):
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
        last_bar = sess[-1]                     # the most recent 5-min bar
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
                      "bar_close": last_bar[4]}
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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# {cfg.name} Framework — Daily Setup",
        f"# Generated: {ts}",
        f"# Trade date: {d.strftime('%d/%m/%Y')}",
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
    if not os.path.exists(path): return False
    try:
        info = _read_day_info(path)
        # Asian markets: compare against UTC date, not London time.
        # During BST (UTC+1) London midnight is still "yesterday" UTC,
        # so a Nikkei session starting at 00:00 UTC would wrongly trigger
        # a fresh daily setup if we used London time.
        if cfg and getattr(cfg, "session_group", "european") == "asian":
            today = datetime.now(timezone.utc).date()
        else:
            today = _london_now().date()
        return info.get("trade_date") == datetime(today.year, today.month, today.day)
    except Exception: return False


def _save_state(path: str, info: dict, position,
                entry_px: float, best_profit: float,
                rsi_c: list, rsi_h: list, rsi_l: list) -> None:
    with open(path) as f: lines = f.readlines()
    mutable = {"position","entry_px","best_profit","trades_today",
               "rsi_closes","rsi_highs","rsi_lows","quadrant"}
    kept = [ln for ln in lines if ln.split("=")[0].strip() not in mutable]
    kept.append(f"quadrant={info['quadrant']}\n")
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

def _daily_setup(prices: dict, cfg: MarketConfig) -> dict:
    days       = sorted(prices)
    trade_date = days[-1]
    ma20       = _build_ma20(prices, cfg.ma_period)

    if trade_date not in ma20:
        days = sorted(prices.keys())
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
            _log(cfg.name, f"        trade_date={trade_date.strftime('%d/%m/%Y')} — is it in ma20? {trade_date in ma20}")
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

    ref = _ref_levels(prices, trade_date)
    if ref is None:
        raise ValueError(f"{cfg.name}: insufficient history for reference levels")

    d1_uk=ref["d1_uk"]; d1_eve=ref["d1_eve"]
    d2_uk=ref["d2_uk"]; d2_eve=ref["d2_eve"]; d3_uk=ref["d3_uk"]
    ma      = ma20[trade_date]
    open_px = prices[trade_date]["open"]
    above_ma = open_px > ma
    ceil     = d1_eve + cfg.golden_rule
    quad     = _quadrant(open_px, d1_uk, d1_eve)
    
    long_valid   = above_ma
    e_exc = (quad == "E-Drift-UK-") and (not above_ma) and (d2_eve <= ceil) and (open_px <= d2_eve + cfg.div_zone)
    short_valid  = not above_ma
    short_levels = sorted(set(filter(None, [d1_eve, d1_uk, d2_eve, d2_uk, d3_uk])))
    hist  = days[:-1]
    rsi_c = [prices[d]["close"] for d in hist]
    rsi_h = [prices[d]["high"]  for d in hist]
    rsi_l = [prices[d]["low"]   for d in hist]

    info = {
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
        "position":None,"entry_px":0.0,"best_profit":0.0,
        "trades_today":0,
        "rsi_closes":rsi_c,"rsi_highs":rsi_h,"rsi_lows":rsi_l,
    }
    _write_day_info(cfg.day_info_path, info, cfg)
    regime = "ABOVE MA" if above_ma else f"BELOW MA ({open_px-ma:+.0f}pts)"
    _log(cfg.name, f"[SETUP] {trade_date.strftime('%d/%m/%Y')}  {quad}  {regime}  "
                   f"long={'✅' if long_valid else '❌'}  short={'✅' if short_valid else '❌'}")
    return info


# ══════════════════════════════════════════════════════════════════
# ENTRY SIGNALS
# ══════════════════════════════════════════════════════════════════

def _check_entries(o: float, h: float, l: float, c: float,
                   rsi_c: list, rsi_h: list, rsi_l: list,
                   info: dict, cfg: MarketConfig) -> tuple:
    """Returns (action, entry_price) or (None, None) if no signal."""
    rsis = _calc_rsi(rsi_c, cfg.rsi_period)
    buy_fn, sell_fn = _BROKER[cfg.name]
    d1_uk=info["d1_uk"]; d1_eve=info["d1_eve"]
    d2_uk=info["d2_uk"]; d2_eve=info["d2_eve"]; d3_uk=info["d3_uk"]
    ceil=info["entry_ceiling"]; quad=info["quadrant"]
    if len(rsi_c) < cfg.rsi_period + 5: return None, None
    if info.get("trades_today", 0) >= MAX_TRADES_PER_DAY:
        _debug(cfg.name, f"  [LIMIT] {MAX_TRADES_PER_DAY} trades already taken today — no further entries")
        return None, None
    if info["long_valid"]:
        if l <= ceil:
            entry = min(o, ceil); entry = max(entry, l)
            if entry <= ceil:
                found, strength = _bull_div(rsi_l, rsis, cfg)
                if found:
               
                    stop = signal_entry - cfg.sl_long
                    size = _calc_size(cfg)
                    buy_fn(entry, stop,
                           f"Long|{quad}|price={c:.1f}|div={strength:.1f}|stop={stop:.1f}|"
                           f"T1={info['t1']:.1f} T2={info['t2']:.1f} T3={info['t3']:.1f}|"
                           f"size={size:.2f}£/pt")
                    return "BUY", entry

    elif info["e_drift_uk_minus_exception"]:
        trigger = info["e_drift_uk_minus_trigger"]
        if h >= trigger and trigger <= ceil:
            found, strength = _bull_div(rsi_l, rsis, cfg)
            if found:
                stop = info["long_stop"]
                size = _calc_size(cfg)
                buy_fn(trigger, stop,
                       f"LongBelowMA|{quad}|price={c:.1f}|div={strength:.1f}|stop={stop:.1f}|"
                       f"T1={d1_uk:.1f} T2={d2_uk:.1f}|size={size:.2f}£/pt")
                return "BUY_EXCEPTION", trigger

    elif info["short_valid"]:
        # E+Drift-UK- veto: below MA but the US session closed above the UK close
        # (d1_eve > d1_uk = evening premium in a downtrend = overnight bounce).
        # Backtest shows this quadrant has WR=20%, avg=-10.5pts for shorts below MA.
        # The US bounce flags a dead-cat recovery that runs through short levels.
        # Skip all short entries when this pattern is present.
        if quad == "E+Drift-UK-":
            _debug(cfg.name, f"  [VETO] E+Drift-UK- below MA — short skipped")
            return None, None
        for level in info["short_levels"]:
            if level - cfg.div_zone <= h <= level + cfg.div_zone:
                found, strength = _bear_div(rsi_h, rsis, cfg)
                if found:
                    stop = level + cfg.sl_short
                    size = _calc_size(cfg)
                    sell_fn(level, stop,
                            f"Short|{quad}|price={c:.1f}|entry target={level:.1f}|div={strength:.1f}|"
                            f"stop={stop:.1f}|T1={d1_uk:.1f} T2={d1_eve:.1f}|"
                            f"size={size:.2f}£/pt")
                    return "SELL", level
                break
    return None, None

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
    Returns -1 on API error (treated as 'position exists' to be safe).
    """
    try:
        positions = ig_service.fetch_open_positions()
        if not positions or "positions" not in positions:
            return 0
        count = sum(
            1 for p in positions["positions"]
            if p.get("market", {}).get("epic", "") == epic
        )
        return count
    except Exception as e:
        _log("IG", f"[WARN] get_open_positions error for {epic}: {e} — assuming open")
        return -1   # safe default: treat as open, don't skip the close

# ══════════════════════════════════════════════════════════════════
# SINGLE-MARKET RUN
# ══════════════════════════════════════════════════════════════════
def run_market(cfg: MarketConfig, now: datetime, ig_service=None) -> None:
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
            _log(cfg.name,     f"        expected:   date,open,high,low,close[,eve_close]")
            _log(cfg.name,     f"        date fmts:  YYYY-MM-DD  DD/MM/YYYY  DD-MM-YYYY")
        except Exception as _e:
            _log(cfg.name, f"[ERROR] could not read {cfg.data_path}: {_e}")
        return


    if not _is_today(cfg.day_info_path, cfg):
        _daily_setup(prices, cfg)

    info        = _read_day_info(cfg.day_info_path)
    position    = info["position"]
    entry_px    = float(info["entry_px"])
    best_profit = float(info["best_profit"])
    rsi_c       = list(info.get("rsi_closes", []))
    rsi_h       = list(info.get("rsi_highs",  []))
    rsi_l       = list(info.get("rsi_lows",   []))

    days = sorted(prices); bar = prices[days[-1]]
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    rsi_c.append(c); rsi_h.append(h); rsi_l.append(l)

    # Current 5-min bar values — used for stop/exit checks only.
    # The session-aggregate l/h above accumulate the session extremes which
    # would trigger stops based on earlier bars, not the current price.
    cb_h = bar.get("bar_high",  h)
    cb_l = bar.get("bar_low",   l)
    cb_c = bar.get("bar_close", c)
    cb_o = bar.get("bar_open", o)

    # ── Log nearest entry and nearest target ──────────────────────
    if not position:
        open_p = info["open_price"]
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
                         f"{'above' if above_ma else 'below'} MA  " )
                        

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
        info["long_valid"]  = above_ma
        info["short_valid"] = not above_ma
        
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
        pnl = (c - entry_px) if position == "LONG" else (entry_px - c)
        size = _calc_size(cfg)
        _log(cfg.name, f"[CLOSE] Session close @ {c:.1f}  "
                       f"({'LONG' if position == 'LONG' else 'SHORT'} "
                       f"{pnl:>+.1f}pts | size={size:.2f}£/pt)")
        message.append(f"[CLOSE] Session close @ {c:.1f}  ")
        telegram_bitbot(message)
        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
        return

    if position == "LONG":
        message = []
        best_profit = max(best_profit, cb_h - entry_px)
        if cb_l < entry_px - cfg.sl_long:
            reason    = "STOP"
            exit_px   = entry_px - cfg.sl_long
            _log(cfg.name, f"[STOP] Long hard stop @ {exit_px:.1f}")
            message.append(f"[STOP] Long hard stop @ {exit_px:.1f}")
            already_closed = _close_position(
                ig_service, cfg, "STOP", exit_px, entry_px,
                position, info, rsi_c, rsi_h, rsi_l
            )
            if not already_closed:
                telegram_bitbot(message)
                # ← your actual broker close call goes here e.g. close_trade(ig_service, cfg)
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return

        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_h) >= 8:
            if _bear_div_exit(rsi_h, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bearish div @ {cb_c:.1f}  (+{cb_c-entry_px:.1f}pts)")
                message.append(f"[EXIT] RSI bearish div @ {cb_c:.1f}  (+{cb_c-entry_px:.1f}pts)")
                already_closed = _close_position(
                    ig_service, cfg, "RSI_DIV", cb_c, entry_px,
                    position, info, rsi_c, rsi_h, rsi_l
                )
                if not already_closed:
                    telegram_bitbot(message)
                    # ← your actual broker close call goes here
                    _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
                return
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit, rsi_c, rsi_h, rsi_l)
        return

    elif position == "SHORT":
        message = []
        best_profit = max(best_profit, entry_px - cb_l)
        if cb_h > entry_px + cfg.sl_short:
            exit_px = entry_px + cfg.sl_short
            _log(cfg.name, f"[STOP] Short hard stop @ {exit_px:.1f}")
            message.append(f"[STOP] Short hard stop @ {exit_px:.1f}")
            already_closed = _close_position(
                ig_service, cfg, "STOP", exit_px, entry_px,
                position, info, rsi_c, rsi_h, rsi_l
            )
            if not already_closed:
                telegram_bitbot(message)
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
            return

        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_l) >= 8:
            if _bull_div_exit(rsi_l, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bullish div @ {cb_c:.1f}  (+{entry_px-cb_c:.1f}pts)")
                message.append(f"[EXIT] RSI bullish div @ {cb_c:.1f}  (+{entry_px-cb_c:.1f}pts)")
                already_closed = _close_position(
                    ig_service, cfg, "RSI_DIV", cb_c, entry_px,
                    position, info, rsi_c, rsi_h, rsi_l
                )
                if not already_closed:
                    telegram_bitbot(message)
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


    action, signal_entry = _check_entries(cb_o, cb_h, cb_l, cb_c, rsi_c, rsi_h, rsi_l, info, cfg)



    if action in ("BUY", "BUY_EXCEPTION"):
        info["trades_today"] = info.get("trades_today", 0) + 1
        _log(cfg.name, f"[ENTRY] Trade {info['trades_today']}/{MAX_TRADES_PER_DAY} today")
        _save_state(cfg.day_info_path, info, "LONG", signal_entry, 0.0, rsi_c, rsi_h, rsi_l)
    elif action == "SELL":
        info["trades_today"] = info.get("trades_today", 0) + 1
        _log(cfg.name, f"[ENTRY] Trade {info['trades_today']}/{MAX_TRADES_PER_DAY} today")
        _save_state(cfg.day_info_path, info, "SHORT", signal_entry, 0.0, rsi_c, rsi_h, rsi_l)
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
    d3_uk  = info["d3_uk"]
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
        lines.append(f"⚡ E-Drift-UK- exception active")
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
    days  = sorted(prices); bar = prices[days[-1]]
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    rsi_c.append(c); rsi_h.append(h); rsi_l.append(l)
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

def run_sequential(market_keys: list[str], now: datetime) -> None:
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
                if k != asian_lock: run_market(c, now)
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
                for cfg in asian_cfgs.values(): run_market(cfg, now)
            else:
                chosen = max(asian_strengths, key=lambda k: asian_strengths[k])
                chosen_cfg = asian_cfgs[chosen]
                pos_before = _read_day_info(chosen_cfg.day_info_path).get("position")
                run_market(cfg, now, ig_service=ig)
                pos_after  = _read_day_info(chosen_cfg.day_info_path).get("position")
                if not pos_before and pos_after:
                    write_lock(ASIAN_LOCK, chosen_cfg.name)
                    _log(chosen_cfg.name, "[SEQ-ASIAN] Lock acquired")
                for k, cfg in asian_cfgs.items():
                    if k != chosen: run_market(cfg, now)

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
                if k != eu_lock: run_market(c, now)
        else:
            eu_strengths: dict[str, float] = {}
            for k, cfg in eu_cfgs.items():
                s = _get_div_strength(cfg)
                if s is not None:
                    eu_strengths[k] = s
                    _debug(cfg.name, f"  [SEQ-EU] signal div={s:.1f}pts")
                else:
                    _debug(cfg.name, "  [SEQ-EU] no signal")

            if not eu_strengths:
                for cfg in eu_cfgs.values(): run_market(cfg, now)
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
                run_market(cfg, now, ig_service=ig)
                pos_after  = _read_day_info(chosen_cfg.day_info_path).get("position")
                if not pos_before and pos_after:
                    write_lock(SEQUENTIAL_LOCK, chosen_cfg.name)
                    _log(chosen_cfg.name, "[SEQ-EU] Lock acquired")
                for k, cfg in eu_cfgs.items():
                    if k != chosen_key: run_market(cfg, now)


# ══════════════════════════════════════════════════════════════════
#Connection
# ══════════════════════════════════════════════════════════════════


import os
from trading_ig import IGService
import requests
from pathlib import Path
import csv

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

def get_account_info(ig: IGService):
    try:     
        """Switch to your account and print basic details."""
        account_info = ig.fetch_accounts()
        print(f"\nAccount info:\n{account_info}")
        data = ig.read_session()
        print("read_session: %s" % data)
        return account_info
        
    except Exception as ex:
        print("Problem: " + repr(ex))



def profit_loss(ig: IGService, epic: str = "IX.D.FTSE.DAILY.IP"):
    positions = ig.fetch_open_positions()
    
    message = []
    for _, row in positions.iterrows():
        level      = row['level']        
        net_change = row['netChange']
        size       = row['size']
        direction  = row['direction']
        instrumentName = row['instrumentName']

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
    positions = ig.fetch_open_positions()
    for _, row in positions.iterrows():
        net_change = row['netChange']
        size       = row['size']
        direction  = row['direction']
        instrumentName = row['instrumentName']
        deal_id = row['dealId']

   

        print(f"{row['dealId']}")
        return(str({row['dealId']}))
        

      

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
    # see from pandas.tseries.frequencies import to_offset
    # resolution = 'H'
    # resolution = '1Min'

    num_points = 1
    data = ig.fetch_historical_prices_by_epic_and_num_points(
        epic, resolution, num_points
    )
    
    #save_path = Path("~").expanduser() / storage_file - original statement but removed to hardcode location see section removed in function: Append_prices_to_file()
    save_path = "/home/john/projects/firstproject/" + storage_file
    Append_prices_to_file(save_path, data)
    print(data["prices"])

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
    
    # Read back, dlogeduplicate, and overwrite
    with open(save_path, "r") as f:
        reader = csv.reader(f)
        #header = next(reader)[:5]          # index col + first 5 columns
        rows = {row[0]: row[1:5] for row in reader}  # dict keyed by index, dedupes automatically
    
    sorted_rows = sorted(rows.items(), key=lambda x: x[1][0])  # sort by column 1 A-Z

    with open(save_path, "w", newline="") as f:
        writer = csv.writer(f)
        #writer.writerow(header)
        writer.writerows([[index] + cols for index, cols in rows.items()])
        

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

def open_buy_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'):
    ig.create_open_position(currency_code='GBP', 
                            direction='BUY', 
                            epic=epic, 
                            expiry='DFB', 
                            force_open='true', 
                            guaranteed_stop='false',
                            level='', 
                            limit_distance=None, 
                            limit_level=None,
                            order_type='MARKET', 
                            quote_id=None, 
                            size=0.1, 
                            stop_distance=35, 
                            stop_level=None, 
                            trailing_stop='false', 
                            trailing_stop_increment=None)
    get_open_positions(ig, 'IX.D.FTSE.DAILY.IP')
    #telegram_bitbot(get_open_positions(ig))



def open_sell_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'):
    ig.create_open_position(currency_code='GBP', direction='SELL', epic=epic, expiry='DFB', force_open='true', guaranteed_stop='false',level='', limit_distance=None, limit_level=None,
order_type='MARKET', quote_id=None, size=0.1, stop_distance=35, stop_level=None, trailing_stop='false', trailing_stop_increment=None)
    
def close_buy_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'): 
    dealid = str(fetch_deal_id(ig, 'FTSE100')).strip("{}'")
    telegram_bitbot("Closing positions"+" " + dealid)  
    telegram_bitbot(str(get_open_positions(ig, 'IX.D.FTSE.DAILY.IP')))
    profit_loss(ig)
    
    
    ig.close_open_position(
                            direction='SELL', 
                            epic=None, 
                            expiry='DFB', 
                            level=None,
                            deal_id=dealid,
                            #force_open=True,
                            order_type='MARKET', 
                            quote_id=None, 
                            size=0.1, 
                            )
     

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
    ig = connect()
    get_price_history(ig, "Dprice.csv",  "IX.D.DAX.IMF.IP")
    get_price_history(ig, "Fprice.csv",  "IX.D.FTSE.DAILY.IP")
    get_price_history(ig, "Cprice.csv",  "IX.D.CAC.IMF.IP")
    get_price_history(ig, "Nprice.csv",  "IX.D.NIKKEI.IFD.IP")
    get_price_history(ig, "HSprice.csv", "IX.D.HANGSENG.IFD.IP")
    
    markets = list(MARKET_CONFIGS) if "all" in args.markets else args.markets

    # Weekend and blackout checks apply to all markets equally
    now = _london_now()
    if now.weekday() >= 5:   # 5=Saturday, 6=Sunday
        sys.exit(0)
    

    if getattr(args, 'sequential', False):
        if len(markets) < 2:
            print("NOTE: --sequential with one market is the same as independent mode.")
        try:
            run_sequential(markets, now)
        except Exception as e:
            _log("SEQ", f"[ERROR] {e}")
    else:
        for key in markets:
            cfg = MARKET_CONFIGS[key]
            try:
                run_market(cfg, now)
            except Exception as e:
                _log(cfg.name, f"[ERROR] {e}")


if __name__ == "__main__":
    main()
