import sys, os, csv, shutil, time
from datetime import datetime, timedelta
import pandas as pd

import datetime as _dt_module
try:
    import zoneinfo
    _LONDON_TZ = zoneinfo.ZoneInfo("Europe/London")
except ImportError:
    _LONDON_TZ = None

# Set these before calling build_and_seed()/resume_harness() — see run_backtest.py
SCRATCH = './tickbt_scratch'
FRAMEWORK_SRC = './framework.py'


def _install_ig_stub():
    """Injects a minimal fake `trading_ig` module into sys.modules so
    framework.py's `from trading_ig import IGService` import succeeds
    without needing the real (paid, IG-provided) package installed. Only
    the class name/shape matters — the object is never actually used;
    FakeIG below stands in for it entirely."""
    import types
    if 'trading_ig' in sys.modules:
        return
    mod = types.ModuleType('trading_ig')
    class IGService:
        def __init__(self, *a, **kw):
            pass
    mod.IGService = IGService
    sys.modules['trading_ig'] = mod


class TimeController:
    """Drives every time-source the framework reads: _london_now() (used
    for session-hour comparisons) AND the direct datetime.now(timezone.utc)
    call inside _ensure_setup's Asian-blackout check, which bypasses
    _london_now() entirely in the real code. Both must reflect the same
    simulated instant, correctly BST-adjusted, or Asian markets would be
    blackout-gated against the real wall-clock date this backtest happens
    to run on instead of the simulated one."""
    def __init__(self):
        self.london_naive = None   # naive datetime, "London local" — matches how price bar timestamps and every cfg.*_hour setting are interpreted

    def set(self, naive_london_dt):
        self.london_naive = naive_london_dt

    def london_now(self):
        return self.london_naive

    def utc_now(self):
        if _LONDON_TZ is not None:
            aware_london = self.london_naive.replace(tzinfo=_LONDON_TZ)
            return aware_london.astimezone(_dt_module.timezone.utc)
        # Fallback matching _london_now()'s own no-zoneinfo approximation
        offset = 1 if 3 < self.london_naive.month < 11 else 0
        naive_utc = self.london_naive - _dt_module.timedelta(hours=offset)
        return naive_utc.replace(tzinfo=_dt_module.timezone.utc)


class _DateTimeProxy:
    """Stands in for the `datetime` name inside the patched module. Not a
    subclass (avoids isinstance/construction edge cases) — delegates every
    attribute except now() to the real datetime class, and __call__ so
    datetime(...) construction still works unchanged."""
    def __init__(self, real_cls, controller):
        self._real = real_cls
        self._controller = controller

    def __call__(self, *a, **kw):
        return self._real(*a, **kw)

    def now(self, tz=None):
        if tz is not None:
            return self._controller.utc_now()
        return self._controller.london_now()

    def __getattr__(self, name):
        return getattr(self._real, name)

def fresh_scratch():
    if os.path.exists(SCRATCH):
        shutil.rmtree(SCRATCH)
    os.makedirs(f'{SCRATCH}/prices')
    os.makedirs(f'{SCRATCH}/day_info')

# ---------------------------------------------------------------------
# Fake IG — simulates instant, always-confirmed fills at the current bar
# close. This is a clean-execution assumption (no rejections, no
# slippage) — the point of this harness is testing the framework's own
# LOGIC against real price data, not re-litigating IG's own reliability.
# ---------------------------------------------------------------------
class FakeIG:
    def __init__(self):
        self.open_epics = {}   # epic -> dict(dealId, size, direction, open_price)
        self.deals = {}        # deal_ref -> dict(status, level, epic)
        self._counter = 0
        self.current_price = {}   # epic -> current bar close
        self.current_time = None  # current simulated tick, set by driver each tick
        self.closed_trades = []

    def create_open_position(self, **kwargs):
        epic = kwargs['epic']
        direction = kwargs['direction']
        size = kwargs['size']
        self._counter += 1
        ref = f"BT{self._counter:08d}"
        deal_id = f"DID{self._counter:08d}"
        price = self.current_price.get(epic, 0.0)
        self.deals[ref] = dict(status='CONFIRMED', level=price, epic=epic)
        self.open_epics[epic] = dict(dealId=deal_id, size=size, direction=direction, open_price=price)
        return {'dealReference': ref}

    def fetch_deal_by_deal_reference(self, ref):
        d = self.deals.get(ref)
        if d is None:
            return {'dealStatus': 'REJECTED', 'reason': 'UNKNOWN_REF'}
        return {'dealStatus': d['status'], 'level': d['level']}

    def fetch_open_positions(self):
        if not self.open_epics:
            return pd.DataFrame()
        rows = [{'epic': e, 'dealId': v['dealId']} for e, v in self.open_epics.items()]
        return pd.DataFrame(rows)

    def close_open_position(self, **kwargs):
        deal_id = kwargs['deal_id']
        epic = None
        for e, v in self.open_epics.items():
            if v['dealId'] == deal_id:
                epic = e
                break
        if epic:
            close_price = self.current_price.get(epic, 0.0)
            pos = self.open_epics.pop(epic)
            self.closed_trades.append(dict(epic=epic, direction=pos['direction'], size=pos['size'],
                                            open_price=pos['open_price'], close_price=close_price,
                                            close_time=self.current_time))
        return {'status': 'CLOSED'}


def _fast_parse_date(s):
    """Drop-in replacement for framework_check._parse_date — identical
    output for all three formats it supports, via direct integer slicing
    instead of datetime.strptime (which re-does locale lookups on every
    single call: profiling showed this as ~85% of total per-tick cost,
    ~3.3s of ~4.1s, called once per raw bar row during _load_prices'
    aggregation). This changes zero trading logic — same inputs produce
    bit-identical datetime objects — it only removes stdlib parsing
    overhead that has nothing to do with the framework's decisions."""
    s = s.strip().split("T")[0]
    if len(s) == 10:
        if s[4] == '-' and s[7] == '-':          # YYYY-MM-DD
            return _dt_module.datetime(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        if s[2] == '/' and s[5] == '/':           # DD/MM/YYYY
            return _dt_module.datetime(int(s[6:10]), int(s[3:5]), int(s[0:2]))
        if s[2] == '-' and s[5] == '-':           # DD-MM-YYYY
            return _dt_module.datetime(int(s[6:10]), int(s[3:5]), int(s[0:2]))
    raise ValueError(f"Cannot parse date: {s!r}")


def build_harness():
    fresh_scratch()

    _install_ig_stub()

    import importlib.util
    spec = importlib.util.spec_from_file_location('fw_tick', FRAMEWORK_SRC)
    fwt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fwt)

    # Neutralise network/telegram entirely.
    fwt.telegram_bitbot = lambda *a, **k: None

    # Controllable time: drives BOTH _london_now() and the direct
    # datetime.now(timezone.utc) call inside _ensure_setup's Asian
    # blackout check.
    controller = TimeController()
    fwt._london_now = controller.london_now
    fwt.datetime = _DateTimeProxy(_dt_module.datetime, controller)

    # Scratch lock files (module-level constant)
    fwt.SEQUENTIAL_LOCK = f'{SCRATCH}/market_lock.txt'

    # Pure performance fix: see _fast_parse_date docstring. Verified
    # identical output to the original for the three supported formats.
    fwt._parse_date = _fast_parse_date

    EPIC_TO_KEY = {}
    for key, cfg in fwt.MARKET_CONFIGS.items():
        cfg.data_path = f'{SCRATCH}/prices/{key}.csv'
        cfg.day_info_path = f'{SCRATCH}/day_info/{key}_day_info.txt'
        EPIC_TO_KEY[cfg.epic] = key
        open(cfg.data_path, 'w').close()

    return fwt, controller, EPIC_TO_KEY


def seed_history(fwt, key, cfg, source_csv, seed_end_exclusive, lookback_start):
    """Copy real bars in [lookback_start, seed_end_exclusive) into the
    market's scratch price file — enough real history for MA20/RSI-seed/
    D1-D3 to be genuine, without carrying the entire multi-decade file
    into every _load_prices() call from tick one."""
    with open(source_csv) as src, open(cfg.data_path, 'a', newline='') as dst:
        w = csv.writer(dst)
        for row in csv.reader(src):
            if lookback_start <= row[0] < seed_end_exclusive:
                w.writerow(row)


def load_bars_in_range(source_csv, start, end_exclusive):
    out = []
    with open(source_csv) as f:
        for row in csv.reader(f):
            if start <= row[0] < end_exclusive:
                out.append(row)
    return out


def build_and_seed(warmup_start, source_paths, lookback_days=150):
    """Fresh start: wipes scratch, builds harness, seeds history. Call once."""
    fwt, controller, EPIC_TO_KEY = build_harness()
    lookback_start = (warmup_start - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    for key, cfg in fwt.MARKET_CONFIGS.items():
        seed_history(fwt, key, cfg, source_paths[key], warmup_start.strftime('%Y-%m-%d'), lookback_start)
    return fwt, controller, EPIC_TO_KEY


def resume_harness():
    """Rebuild the harness pointed at the EXISTING scratch dir (day_info +
    price files already on disk from a prior chunk) without wiping or
    reseeding anything."""
    _install_ig_stub()
    import importlib.util
    spec = importlib.util.spec_from_file_location('fw_tick', FRAMEWORK_SRC)
    fwt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fwt)
    fwt.telegram_bitbot = lambda *a, **k: None
    controller = TimeController()
    fwt._london_now = controller.london_now
    fwt.datetime = _DateTimeProxy(_dt_module.datetime, controller)
    fwt.SEQUENTIAL_LOCK = f'{SCRATCH}/market_lock.txt'
    fwt._parse_date = _fast_parse_date
    EPIC_TO_KEY = {}
    for key, cfg in fwt.MARKET_CONFIGS.items():
        cfg.data_path = f'{SCRATCH}/prices/{key}.csv'
        cfg.day_info_path = f'{SCRATCH}/day_info/{key}_day_info.txt'
        EPIC_TO_KEY[cfg.epic] = key
    return fwt, controller, EPIC_TO_KEY


def step_range(fwt, controller, ig, start, end, source_paths, record_start):
    """Process every 5-min tick in [start, end). Appends new bars to the
    scratch price files and calls the real run_sequential each tick.
    Returns (new_trades_recorded, n_ticks, elapsed, errors)."""
    import contextlib, io
    bars_by_market = {}
    for key, cfg in fwt.MARKET_CONFIGS.items():
        rows = []
        with open(source_paths[key]) as f:
            for row in csv.reader(f):
                if start.strftime('%Y-%m-%d') <= row[0] < (end + timedelta(days=1)).strftime('%Y-%m-%d'):
                    rows.append(row)
        bars_by_market[key] = {r[0]: r for r in rows}

    market_keys = list(fwt.MARKET_CONFIGS.keys())
    tick = start
    n_ticks = 0
    errors = []
    t0 = time.time()
    while tick < end:
        if tick.weekday() < 5:
            ts_str = tick.strftime('%Y-%m-%dT%H:%M:%S')
            appended_any = False
            for key, cfg in fwt.MARKET_CONFIGS.items():
                row = bars_by_market[key].get(ts_str)
                if row is not None:
                    with open(cfg.data_path, 'a', newline='') as f:
                        csv.writer(f).writerow(row)
                    ig.current_price[cfg.epic] = float(row[4])
                    appended_any = True
            if appended_any:
                controller.set(tick)
                ig.current_time = tick
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    try:
                        fwt.run_sequential(market_keys, controller.london_now(), ig=ig)
                    except Exception as e:
                        errors.append((ts_str, str(e)))
                n_ticks += 1
        tick += timedelta(minutes=5)
    elapsed = time.time() - t0
    new_trades = [t for t in ig.closed_trades if t['close_time'] is not None and t['close_time'] >= record_start]
    return new_trades, n_ticks, elapsed, errors
