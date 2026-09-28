"""
RSI Divergence Framework — Multi-Market Cron Script
====================================================
Supports FTSE UK100 and DAX Germany 40.
One file, one cron job, any combination of markets.

CRON SETUP
──────────
Independent (each market manages its own position):
    */5 * * * * /home/john/projects/firstproject/venv/bin/python /home/john/projects/firstproject/framework.py ftse dax >> /home/john/projects/firstproject/trading.log 2>&1

Sequential (one trade at a time, priority rules):
    */5 * * * * /home/john/projects/firstproject/venv/bin/python /home/john/projects/firstproject/framework.py --sequential ftse dax >> /home/john/projects/firstproject/trading.log 2>&1

HOW IT WORKS
────────────
Each cron tick the script:
  1. Reads London time from the system clock.
  2. BLACKOUT (21:00-02:59): exits silently.
  3. For each requested market:
     a. If the day-info file is missing or stale, runs daily setup.
     b. Reads the latest bar from that market's prices CSV.
     c. PRE-OPEN (03:00-07:59): RSI updated, quadrant recalculated
        from current close every bar.
     d. SESSION OPEN (08:00): quadrant locked from the bar open.
     e. SESSION (08:00+): full entry, exit and stop logic.
     f. Calls BUY_<MARKET> or SELL_<MARKET> when a signal fires.

CSV FORMAT (same for both markets):
    date,open,high,low,close,eve_close
    2026-05-06,10295,10340,10290,10336,10358

    close     = session close (16:30 UK / 17:00 Frankfurt)
    eve_close = US/overnight close at 20:55  ← primary reference
                Optional: falls back to close if absent.

DAY INFO FILES:
    FTSE_day_info.txt  — written/read for FTSE
    DAX_day_info.txt   — written/read for DAX
    Both are plain key=value, human-readable, editable manually.
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
    ma_period     : int   = 20
    bull_lb       : int   = 12     # bullish divergence lookback (bars)
    bear_lb       : int   = 10     # bearish divergence lookback (bars)
    min_rsi_gap   : float = 3.0


# ── Market definitions — edit paths and parameters to match your setup ──
MARKET_CONFIGS: dict[str, MarketConfig] = {
    "ftse": MarketConfig(
        name          = "FTSE",
        data_path     = "/home/john/projects/firstproject/Fprice.csv",
        day_info_path = "/home/john/projects/firstproject/FTSE_day_info.txt",
        sl_long       = 30.0,
        sl_short      = 35.0,
        golden_rule   = 10.0,
        div_zone      = 5.0,
        min_profit    = 5.0,
        min_price_gap = 3.0,
        session_close_hour   = 16,   # force-close at 16:25 (cash close 16:30)
        session_close_minute = 25,
        # Sizing — choose one mode:
        sizing_mode       = "flat",  # "flat" or "compound"
        flat_size         = 10.0,    # £10 per point
        compound_fraction = 500.0,   # 1/500th of account
        account_balance   = 10000.0, # update to your actual balance
    ),
    "dax": MarketConfig(
        name          = "DAX",
        data_path     = "/home/john/projects/firstproject/Dprice.csv",
        day_info_path = "/home/john/projects/firstproject/DAX_day_info.txt",
        # DAX trades at ~2x FTSE price — stops and zones scaled accordingly
        sl_long       = 60.0,
        sl_short      = 70.0,
        golden_rule   = 20.0,
        div_zone      = 20.0,
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
    ),
}

# ── Global settings ────────────────────────────────────────────────
LOG_ENABLED    = True

# ── Sequential mode: shared lock file ───────────────────────────────
# Stores which market currently holds the position in sequential mode.
SEQUENTIAL_LOCK = "/home/john/projects/firstproject/market_lock.txt"

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
    message = f"FTSE | BUY | {price:.1f} | stop={stop:.1f}"
    telegram_bitbot(message)

    

def SELL_FTSE(price: float, stop: float, notes: str = "") -> None:
    _log("FTSE", f"[SELL] @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"FTSE | SELL | {price:.1f} | stop={stop:.1f}"
    telegram_bitbot(message)
    

def BUY_DAX(price: float, stop: float, notes: str = "") -> None:
    _log("DAX",  f"[BUY]  @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"DAX | BUY | {price:.1f} | stop={stop:.1f}"
    telegram_bitbot(message)   
    

def SELL_DAX(price: float, stop: float, notes: str = "") -> None:
    _log("DAX",  f"[SELL] @ {price:.1f}  stop={stop:.1f}  | {notes}")
    message = f"DAX | SELL | {price:.1f} | stop={stop:.1f}"
    telegram_bitbot(message) 
    
    

# Maps market name → (buy_fn, sell_fn)
_BROKER: dict[str, tuple] = {
    "FTSE": (BUY_FTSE,  SELL_FTSE),
    "DAX":  (BUY_DAX,   SELL_DAX),
}

# Global IG session — set once in main() after connect(), used by broker functions
ig_session = None


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

def _in_blackout(hour: int) -> bool:
    return hour >= BLACKOUT_START or hour < BLACKOUT_END


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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(s.strip(), fmt)
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


def _load_prices(path: str) -> dict:
    prices = {}
    with open(path, newline="") as f:
        sample  = f.read(512); f.seek(0)
        has_hdr = not sample.split("\n")[0].split(",")[1].replace(".", "").isdigit()
        reader  = csv.reader(f)
        header  = [h.strip().lower() for h in next(reader)] if has_hdr else None
        for row in reader:
            if len(row) < 5: continue
            try:
                dt      = _parse_date(row[0])
                o,h,l,c = float(row[1]),float(row[2]),float(row[3]),float(row[4])
                eve_col = None
                if header:
                    for name in ("eve_close","eve close","us_close","us close","overnight"):
                        if name in header: eve_col = header.index(name); break
                    if eve_col is None and len(row) > 5: eve_col = 5
                elif len(row) > 5: eve_col = 5
                eve = float(row[eve_col]) if eve_col is not None and row[eve_col].strip() else None
                prices[dt] = {"open":o,"high":h,"low":l,"close":c,"eve_close":eve}
            except (ValueError, IndexError): continue
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
        f"long_stop={info['long_stop']:.2f}",
        f"dead_cat_veto={int(info['dead_cat_veto'])}",
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
            elif key in ("above_ma","dead_cat_veto","long_valid","short_valid",
                         "e_drift_uk_minus_exception"): info[key] = bool(int(val))
            elif key in ("short_levels","rsi_closes","rsi_highs","rsi_lows"):
                info[key] = [float(v) for v in val.split(",") if v.strip()]
            elif key == "position": info[key] = None if val == "none" else val
            elif key == "quadrant": info[key] = val
            else:
                try: info[key] = float(val)
                except: info[key] = val
    return info


def _is_today(path: str) -> bool:
    if not os.path.exists(path): return False
    try:
        info  = _read_day_info(path)
        today = _london_now().date()
        return info.get("trade_date") == datetime(today.year, today.month, today.day)
    except Exception: return False


def _save_state(path: str, info: dict, position,
                entry_px: float, best_profit: float,
                rsi_c: list, rsi_h: list, rsi_l: list) -> None:
    with open(path) as f: lines = f.readlines()
    mutable = {"position","entry_px","best_profit",
               "rsi_closes","rsi_highs","rsi_lows","quadrant"}
    kept = [ln for ln in lines if ln.split("=")[0].strip() not in mutable]
    kept.append(f"quadrant={info['quadrant']}\n")
    kept.append(f"position={'none' if position is None else position}\n")
    kept.append(f"entry_px={entry_px:.2f}\n")
    kept.append(f"best_profit={best_profit:.2f}\n")
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
    dead_cat = d2_uk < open_px < d1_uk
    long_valid   = above_ma and not dead_cat
    e_exc        = (quad == "E-Drift-UK-") and (not above_ma) and (d2_eve <= ceil)
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
        "entry_ceiling":ceil,"long_stop":d1_eve-2,
        "dead_cat_veto":dead_cat,"long_valid":long_valid,
        "e_drift_uk_minus_exception":e_exc,"e_drift_uk_minus_trigger":d2_eve,
        "short_valid":short_valid,"short_levels":short_levels,
        "t1":d1_uk,"t2":d2_eve,"t3":d3_uk,
        "position":None,"entry_px":0.0,"best_profit":0.0,
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
                   info: dict, cfg: MarketConfig) -> Optional[str]:
    rsis = _calc_rsi(rsi_c, cfg.rsi_period)
    buy_fn, sell_fn = _BROKER[cfg.name]
    d1_uk=info["d1_uk"]; d1_eve=info["d1_eve"]
    d2_uk=info["d2_uk"]; d2_eve=info["d2_eve"]; d3_uk=info["d3_uk"]
    ceil=info["entry_ceiling"]; quad=info["quadrant"]
    if len(rsi_c) < cfg.rsi_period + 5: return None

    if info["long_valid"]:
        if l <= ceil:
            entry = min(o, ceil); entry = max(entry, l)
            if entry <= ceil:
                found, strength = _bull_div(rsi_l, rsis, cfg)
                if found:
                    if d2_uk < entry < d1_uk:
                        _debug(cfg.name, f"  [VETO] Dead-cat @ {entry:.1f}")
                    else:
                        stop = info["long_stop"]
                        size = _calc_size(cfg)
                        buy_fn(entry, stop,
                               f"Long|{quad}|div={strength:.1f}|stop={stop:.1f}|"
                               f"T1={info['t1']:.1f} T2={info['t2']:.1f} T3={info['t3']:.1f}|"
                               f"size={size:.2f}£/pt")
                        return "BUY"

    elif info["e_drift_uk_minus_exception"]:
        trigger = info["e_drift_uk_minus_trigger"]
        if h >= trigger and trigger <= ceil:
            found, strength = _bull_div(rsi_l, rsis, cfg)
            if found and not (d2_uk < trigger < d1_uk):
                stop = info["long_stop"]
                size = _calc_size(cfg)
                buy_fn(trigger, stop,
                       f"LongBelowMA|{quad}|div={strength:.1f}|stop={stop:.1f}|"
                       f"T1={d1_uk:.1f} T2={d2_uk:.1f}|size={size:.2f}£/pt")
                return "BUY_EXCEPTION"

    elif info["short_valid"]:
        # E+Drift-UK- veto: below MA but the US session closed above the UK close
        # (d1_eve > d1_uk = evening premium in a downtrend = overnight bounce).
        # Backtest shows this quadrant has WR=20%, avg=-10.5pts for shorts below MA.
        # The US bounce flags a dead-cat recovery that runs through short levels.
        # Skip all short entries when this pattern is present.
        if quad == "E+Drift-UK-":
            _debug(cfg.name, f"  [VETO] E+Drift-UK- below MA — short skipped")
            return None
        for level in info["short_levels"]:
            if level - cfg.div_zone <= h <= level + cfg.div_zone:
                found, strength = _bear_div(rsi_h, rsis, cfg)
                if found:
                    stop = level + cfg.sl_short
                    size = _calc_size(cfg)
                    sell_fn(level, stop,
                            f"Short|{quad}|level={level:.1f}|div={strength:.1f}|"
                            f"stop={stop:.1f}|T1={d1_uk:.1f} T2={d1_eve:.1f}|"
                            f"size={size:.2f}£/pt")
                    return "SELL"
                break
    return None


# ══════════════════════════════════════════════════════════════════
# SINGLE-MARKET RUN
# ══════════════════════════════════════════════════════════════════

def run_market(cfg: MarketConfig, now: datetime) -> None:
    hour = now.hour

    # Honour blackout even when called directly
    if _in_blackout(hour):
        return

    if not os.path.exists(cfg.data_path):
        _log(cfg.name, f"[ERROR] prices not found: {cfg.data_path}"); return
    prices = _load_prices(cfg.data_path)
    if not prices:
        _log(cfg.name, f"[ERROR] no rows in {cfg.data_path}"); return

    if not _is_today(cfg.day_info_path):
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

    _debug(cfg.name, f"  O={o:.1f} H={h:.1f} L={l:.1f} C={c:.1f}  "
                     f"{now.strftime('%H:%M')}  pos={position or 'flat'}")

    in_pre_open = PRE_OPEN_START <= hour < SESSION_OPEN

    # ── PRE-OPEN: recalculate quadrant from current close ─────────
    if in_pre_open:
        new_quad = _quadrant(c, info["d1_uk"], info["d1_eve"])
        above_ma = c > info["ma20"]
        ceil     = info["entry_ceiling"]
        dead_cat = info["d2_uk"] < c < info["d1_uk"]
        info["quadrant"]    = new_quad
        info["above_ma"]    = above_ma
        info["long_valid"]  = above_ma and not dead_cat
        info["short_valid"] = not above_ma
        info["dead_cat_veto"] = dead_cat
        info["e_drift_uk_minus_exception"] = (
            new_quad == "E-Drift-UK-" and not above_ma and
            info["e_drift_uk_minus_trigger"] <= ceil
        )
        _debug(cfg.name, f"  [pre-open] quad={new_quad}  "
                         f"{'above' if above_ma else 'below'} MA  "
                         f"{'dc-veto' if dead_cat else 'ok'}")

    # ── SESSION OPEN: lock quadrant from open price ───────────────
    elif hour == SESSION_OPEN and now.minute < 6:
        new_quad = _quadrant(o, info["d1_uk"], info["d1_eve"])
        above_ma = o > info["ma20"]
        ceil     = info["entry_ceiling"]
        dead_cat = info["d2_uk"] < o < info["d1_uk"]
        if new_quad != info["quadrant"]:
            _log(cfg.name, f"[OPEN] Quadrant {info['quadrant']} → {new_quad}  (open={o:.1f})")
        else:
            _debug(cfg.name, f"[OPEN] Quadrant confirmed: {new_quad}  (open={o:.1f})")
        info["quadrant"]    = new_quad
        info["above_ma"]    = above_ma
        info["long_valid"]  = above_ma and not dead_cat
        info["short_valid"] = not above_ma
        info["dead_cat_veto"] = dead_cat
        info["e_drift_uk_minus_exception"] = (
            new_quad == "E-Drift-UK-" and not above_ma and
            info["e_drift_uk_minus_trigger"] <= ceil
        )
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
        pnl = (c - entry_px) if position == "LONG" else (entry_px - c)
        size = _calc_size(cfg)
        _log(cfg.name, f"[CLOSE] Session close @ {c:.1f}  "
                       f"({'LONG' if position == 'LONG' else 'SHORT'} "
                       f"{pnl:>+.1f}pts | size={size:.2f}£/pt)")
        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)
        return

    if position == "LONG":
        best_profit = max(best_profit, h - entry_px)
        if l < entry_px - cfg.sl_long:
            _log(cfg.name, f"[STOP] Long hard stop @ {entry_px - cfg.sl_long:.1f}")
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l); return
        if c < info["d1_eve"] - 2:
            _log(cfg.name, f"[EXIT] Stop-eve @ {c:.1f}")
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l); return
        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_h) >= 8:
            if _bear_div_exit(rsi_h, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bearish div @ {c:.1f}  (+{c-entry_px:.1f}pts)")
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l); return
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit, rsi_c, rsi_h, rsi_l)
        return

    elif position == "SHORT":
        best_profit = max(best_profit, entry_px - l)
        if h > entry_px + cfg.sl_short:
            _log(cfg.name, f"[STOP] Short hard stop @ {entry_px + cfg.sl_short:.1f}")
            _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l); return
        rsis = _calc_rsi(rsi_c, cfg.rsi_period)
        if best_profit >= cfg.min_profit and len(rsi_l) >= 8:
            if _bull_div_exit(rsi_l, rsis, cfg):
                _log(cfg.name, f"[EXIT] RSI bullish div @ {c:.1f}  (+{entry_px-c:.1f}pts)")
                _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l); return
        _save_state(cfg.day_info_path, info, position, entry_px, best_profit, rsi_c, rsi_h, rsi_l)
        return

    # ── LOOK FOR ENTRY ────────────────────────────────────────────
    action = _check_entries(o, h, l, c, rsi_c, rsi_h, rsi_l, info, cfg)

    if action in ("BUY", "BUY_EXCEPTION"):
        _save_state(cfg.day_info_path, info, "LONG", o, 0.0, rsi_c, rsi_h, rsi_l)
    elif action == "SELL":
        sell_entry = next(
            (lv for lv in info["short_levels"]
             if lv - cfg.div_zone <= h <= lv + cfg.div_zone), h
        )
        _save_state(cfg.day_info_path, info, "SHORT", sell_entry, 0.0, rsi_c, rsi_h, rsi_l)
    else:
        _save_state(cfg.day_info_path, info, None, 0.0, 0.0, rsi_c, rsi_h, rsi_l)


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
    if not os.path.exists(cfg.data_path) or not _is_today(cfg.day_info_path):
        return None
    prices = _load_prices(cfg.data_path)
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
                if found and not (info["d2_uk"] < entry < info["d1_uk"]):
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
    Sequential priority mode: at most one trade open across all markets.

    Priority rules (matched to backtest sequential strategy):
      1. Only one market signals → always take it.
      2. Both signal, div gap ≥5pts → take the stronger divergence.
      3. Both signal, DAX in E-Drift-UK- quad → take FTSE instead.
      4. Both signal, similar div → default to DAX
         (DAX outperforms FTSE on shared days post-spread).

    A shared lock file (SEQUENTIAL_LOCK) records which market holds the
    position. Non-chosen markets still get their RSI updated each bar
    so they are ready to signal when the lock is released.
    """
    cfgs = {k: MARKET_CONFIGS[k] for k in market_keys}
    locked_market = _read_lock()

    # ── Position already open: manage that market, tick others ────
    if locked_market and locked_market in cfgs:
        cfg = cfgs[locked_market]
        _debug(cfg.name, f"  [SEQ] managing open position")
        pos_before = _read_day_info(cfg.day_info_path).get("position")
        run_market(cfg, now)
        pos_after  = _read_day_info(cfg.day_info_path).get("position")
        if pos_before and not pos_after:
            _clear_lock()
            _log(cfg.name, "[SEQ] Position closed — lock released")
        for key, idle_cfg in cfgs.items():
            if key != locked_market:
                run_market(idle_cfg, now)
        return

    # ── No position: check signals and pick the best market ───────
    strengths: dict[str, float] = {}
    for key, cfg in cfgs.items():
        s = _get_div_strength(cfg)
        if s is not None:
            strengths[key] = s
            _debug(cfg.name, f"  [SEQ] signal  div={s:.1f}pts")
        else:
            _debug(cfg.name, "  [SEQ] no signal")

    if not strengths:
        for cfg in cfgs.values():
            run_market(cfg, now)
        return

    if len(strengths) == 1:
        chosen_key = next(iter(strengths))
        _debug(MARKET_CONFIGS[chosen_key].name, "  [SEQ] only market signalling → selected")
    else:
        sorted_keys = sorted(strengths, key=lambda k: -strengths[k])
        strongest, second = sorted_keys[0], sorted_keys[1]
        gap = strengths[strongest] - strengths[second]
        if gap >= 5.0:
            chosen_key = strongest
            _debug(MARKET_CONFIGS[chosen_key].name,
                   f"  [SEQ] div gap={gap:.1f}pts ≥5 → stronger signal wins")
        else:
            dax_quad = ""
            if "dax" in cfgs and _is_today(MARKET_CONFIGS["dax"].day_info_path):
                dax_quad = _read_day_info(
                    MARKET_CONFIGS["dax"].day_info_path).get("quadrant", "")
            if "dax" in cfgs and dax_quad == "E-Drift-UK-" and "ftse" in cfgs:
                chosen_key = "ftse"
                _debug(MARKET_CONFIGS["ftse"].name,
                       "  [SEQ] DAX in E-Drift-UK- → FTSE selected")
            else:
                chosen_key = "dax" if "dax" in cfgs else strongest
                _debug(MARKET_CONFIGS[chosen_key].name,
                       "  [SEQ] similar div → DAX default")

    # Run chosen market — it will call BUY/SELL if signal confirmed
    chosen_cfg = cfgs[chosen_key]
    pos_before  = _read_day_info(chosen_cfg.day_info_path).get("position")
    run_market(chosen_cfg, now)
    pos_after   = _read_day_info(chosen_cfg.day_info_path).get("position")
    if not pos_before and pos_after:
        _write_lock(chosen_cfg.name)
        _log(chosen_cfg.name, "[SEQ] Lock acquired")

    for key, cfg in cfgs.items():
        if key != chosen_key:
            run_market(cfg, now)


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
#   export IG_PASSWORD="your_passfetch_historical_prices_by_epic_and_num_pointsword"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="DEMO"   # or "LIVE"
#   export IG_ACC_NUMBER="your_account_number"

# ⚠️  SECURITY: credentials read from environment variables.
# Set them before running:
#   export IG_USERNAME="your_username"
#   export IG_PASSWORD="your_password"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="LIVE"
#   export IG_ACC_NUMBER="your_account_number"
IG_USERNAME   = os.environ.get("IG_USERNAME", "KEMEJO08709786")
IG_PASSWORD   = os.environ.get("IG_PASSWORD", "Ex204lfbn27gl!")
IG_API_KEY    = os.environ.get("IG_API_KEY",  "8b830abfbd08fc4eee85781f3fbb2e70d1e8436a")
IG_ACC_TYPE   = os.environ.get("IG_ACC_TYPE", "LIVE")   # "DEMO" or "LIVE"
IG_ACC_NUMBER = os.environ.get("IG_ACC_NUMBER", "KTWBE")

import os
from trading_ig import IGService
import requests
from pathlib import Path
import csv

# --- CONFIGURATION ---
# Best practice: use environment variables rather than hardcoding credentials
# Set these in your shell or a .env file:
#   export IG_USERNAME="your_username"
#   export IG_PASSWORD="your_passfetch_historical_prices_by_epic_and_num_pointsword"
#   export IG_API_KEY="your_api_key"
#   export IG_ACC_TYPE="DEMO"   # or "LIVE"
#   export IG_ACC_NUMBER="your_account_number"

#IG_USERNAME   = os.environ.get("IG_USERNAME", "")
#IG_PASSWORD   = os.environ.get("IG_PASSWORD", "")
#IG_API_KEY    = os.environ.get("IG_API_KEY",  "")
#IG_ACC_TYPE   = os.environ.get("IG_ACC_TYPE", "LIVE")
#IG_ACC_NUMBER = os.environ.get("IG_ACC_NUMBER", "")


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
 
    telegram_bitbot("✓ Session created successfully.")
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

def get_open_positions(ig: IGService):
    """Fetch and display all currently open positions."""
    positions = ig.fetch_open_positions()
    if positions.empty:
        print("\nNo open positions.")
    else:
        print(f"\nOpen positions:\n{positions}")
    
    
    filtered = positions.iloc[:, [3,5,6, 8,15]]
    
    return filtered

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
    get_open_positions(ig)
    telegram_bitbot(get_open_positions(ig))



def open_sell_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'):
    ig.create_open_position(currency_code='GBP', direction='SELL', epic=epic, expiry='DFB', force_open='true', guaranteed_stop='false',level='', limit_distance=None, limit_level=None,
order_type='MARKET', quote_id=None, size=0.1, stop_distance=35, stop_level=None, trailing_stop='false', trailing_stop_increment=None)
    
def close_buy_trade(ig: IGService, epic:str = 'IX.D.FTSE.DAILY.IP'): 
    dealid = str(fetch_deal_id(ig, 'FTSE100')).strip("{}'")
    telegram_bitbot("Closing positions"+" " + dealid)  
    telegram_bitbot(get_open_positions(ig))  
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
    global ig_session
    ig_session = connect()
    get_price_history(ig_session,"Dprice.csv", "IX.D.DAX.IMF.IP") #Fprice / Dprice.csv, "IX.D.FTSE.DAILY.IP"/ "IX.D.DAX.IMF.IP" 
    get_price_history(ig_session,"Fprice.csv", "IX.D.FTSE.DAILY.IP") #Fprice / Dprice.csv, "IX.D.FTSE.DAILY.IP"/ "IX.D.DAX.IMF.IP" 
    markets = list(MARKET_CONFIGS) if "all" in args.markets else args.markets

    # Weekend and blackout checks apply to all markets equally
    now = _london_now()
    if now.weekday() >= 5:   # 5=Saturday, 6=Sunday
        sys.exit(0)
    if _in_blackout(now.hour):
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
