#!/usr/bin/env python3
"""
IG Lightstreamer 5-minute candle daemon
=======================================
Subscribes to IG's streaming CHART candles for the framework's five markets
and appends each COMPLETED 5-minute bar to the same price CSVs the trading
framework reads (Fprice.csv, Dprice.csv, ...).

WHY: streamed prices do NOT count against IG's 10,000-point weekly
historical-data allowance (that quota applies to the REST /prices
endpoints). With this daemon running, the framework's CSVs stay current in
real time, the REST fetch in the cron becomes a redundancy/backfill layer
only, and the class of failure from 13-Jul (allowance exhausted -> frozen
feed -> entries interlocked) cannot recur from normal operation.

PRICE BASIS: the CSVs store BID OHLC (Append_prices_to_file keeps the bid
columns of the REST response), so this daemon writes the BID_* candle
fields — same basis, no offset between streamed and RESTed rows.

RUNNING (same .env as the framework):
    cd /home/john/projects/firstproject
    . ./.env && nohup ./venv/bin/python streamer.py >> streamer.log 2>&1 &

or as a systemd service (recommended, restarts on failure/boot):
    [Unit]
    Description=IG candle streamer
    After=network-online.target
    [Service]
    EnvironmentFile=/home/john/projects/firstproject/.env
    ExecStart=/home/john/projects/firstproject/venv/bin/python \
              /home/john/projects/firstproject/streamer.py
    Restart=always
    RestartSec=15
    [Install]
    WantedBy=multi-user.target

NOTES / LIMITS (stated plainly):
  * The stream has NO history: bars missed while this daemon is down are
    not recovered here — the framework's REST backfill / Yahoo fill layers
    handle those, as now.
  * Only COMPLETED candles (CONS_END=1) are written, so the newest CSV row
    is up to 5 minutes old — identical to the cron cadence, and inside the
    framework's 15-minute stale-feed interlock threshold.
  * IG may drop a Lightstreamer connection when a new REST login occurs
    (the cron logs in every 5 minutes). Behaviour varies; this daemon
    assumes the worst and reconnects with backoff whenever the session
    drops. If your streamer.log shows a disconnect/reconnect every 5
    minutes it is coping, but tell me and we can switch the framework to a
    token-reuse login instead.
  * Weekend: IG closes the feed Fri night -> Sun ~23:00 UTC. The daemon
    stays connected but simply receives nothing; that is normal.
"""

import os
import sys
import time
import threading
from datetime import datetime, timezone

from trading_ig import IGService
from trading_ig.stream import IGStreamService
from lightstreamer.client import Subscription

# ── Configuration ──────────────────────────────────────────────────
BASE_DIR = "/home/john/projects/firstproject"
EPICS = {
    "IX.D.FTSE.DAILY.IP":     "Fprice.csv",
    "IX.D.DAX.DAILY.IP":      "Dprice.csv",
    "IX.D.CAC.DAILY.IP":      "Cprice.csv",
    "IX.D.NIKKEI.DAILY.IP":   "Nprice.csv",
    "IX.D.HANGSENG.DAILY.IP": "HSprice.csv",
}
FIELDS = ["UTM", "BID_OPEN", "BID_HIGH", "BID_LOW", "BID_CLOSE", "CONS_END"]

# ── Load .env automatically ────────────────────────────────────────
# Reads /home/john/projects/firstproject/.env so the streamer can be
# started without manually sourcing the file first (nohup, systemd, etc.)
# os.environ.setdefault means existing env vars always win, so the cron's
# explicit values still take precedence if set.
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH) as _ef:
        for _line in _ef:
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if _line.startswith("export "):
                _line = _line[7:]
            if "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(),
                                      _v.strip().strip('"').strip("'"))

IG_USERNAME = os.environ.get("IG_USERNAME", "")
IG_PASSWORD = os.environ.get("IG_PASSWORD", "")
IG_API_KEY  = os.environ.get("IG_API_KEY", "")
IG_ACC_TYPE = os.environ.get("IG_ACC_TYPE", "LIVE")

RECONNECT_MIN_S = 10          # first retry delay
RECONNECT_MAX_S = 300         # backoff cap


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  [STREAM] {msg}",
          flush=True)


def _tail_last_timestamp(path: str):
    """Newest parseable timestamp already in a CSV (mirrors the framework's
    tolerant tail parser) so we never write a bar at or before it."""
    fmts = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d-%m-%YT%H:%M:%S", "%d-%m-%YT%H:%M")
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 8192))
            tail = f.read().decode("utf-8", errors="ignore")
    except OSError:
        return None
    newest = None
    for line in tail.splitlines():
        ts = line.split(",")[0].strip()
        for fmt in fmts:
            try:
                dt = datetime.strptime(ts, fmt)
                if newest is None or dt > newest:
                    newest = dt
                break
            except ValueError:
                continue
    return newest


class CandleWriter:
    """Accumulates streamed candle updates; appends completed bars."""

    def __init__(self):
        self._lock = threading.Lock()
        # last timestamp written per epic — seeded from the CSV tails so a
        # daemon restart cannot double-write bars the file already has.
        self._last = {}
        for epic, fname in EPICS.items():
            self._last[epic] = _tail_last_timestamp(os.path.join(BASE_DIR, fname))
            log(f"{fname}: resuming after "
                f"{self._last[epic] or 'empty file'}")

    def on_update(self, update) -> None:
        try:
            item = update.getItemName()          # CHART:{epic}:5MINUTE
            epic = item.split(":")[1]
            fname = EPICS.get(epic)
            if fname is None:
                return
            if update.getValue("CONS_END") != "1":
                return                            # bar still forming
            utm = update.getValue("UTM")
            o = update.getValue("BID_OPEN")
            h = update.getValue("BID_HIGH")
            l = update.getValue("BID_LOW")
            c = update.getValue("BID_CLOSE")
            if not all((utm, o, h, l, c)):
                return                            # partial update
            ts = datetime.fromtimestamp(int(utm) / 1000.0, tz=timezone.utc
                                        ).replace(tzinfo=None)
            o, h, l, c = float(o), float(h), float(l), float(c)
        except (ValueError, TypeError, IndexError, AttributeError) as e:
            log(f"malformed update skipped: {e}")
            return

        with self._lock:
            last = self._last.get(epic)
            if last is not None and ts <= last:
                return                            # duplicate / out of order
            path = os.path.join(BASE_DIR, fname)
            try:
                with open(path, "a") as f:
                    f.write(f"{ts.strftime('%Y-%m-%dT%H:%M:%S')},"
                            f"{o:.2f},{h:.2f},{l:.2f},{c:.2f}\n")
            except OSError as e:
                log(f"{fname}: write failed: {e}")
                return
            self._last[epic] = ts
            log(f"{fname}: candle {ts:%H:%M} o={o:.1f} h={h:.1f} "
                f"l={l:.1f} c={c:.1f}")


class _SubListener:
    """Minimal lightstreamer SubscriptionListener duck-type."""
    def __init__(self, writer, drop_event):
        self._writer = writer
        self._drop = drop_event

    def onItemUpdate(self, update):
        self._writer.on_update(update)

    def onSubscriptionError(self, code, message):
        log(f"subscription error {code}: {message}")
        self._drop.set()

    # remaining listener callbacks are optional no-ops
    def __getattr__(self, name):
        return lambda *a, **k: None


class _ClientListener:
    """Connection-level listener: flag any terminal status for reconnect."""
    def __init__(self, drop_event):
        self._drop = drop_event

    def onStatusChange(self, status):
        log(f"connection status: {status}")
        if "DISCONNECTED" in status:
            self._drop.set()

    def onServerError(self, code, message):
        log(f"server error {code}: {message}")
        self._drop.set()

    def __getattr__(self, name):
        return lambda *a, **k: None


def run_once(writer: CandleWriter) -> None:
    """One connect->stream->drop cycle. Returns when the connection dies."""
    drop = threading.Event()

    ig = IGService(username=IG_USERNAME, password=IG_PASSWORD,
                   api_key=IG_API_KEY, acc_type=IG_ACC_TYPE)
    stream = IGStreamService(ig)
    stream.create_session()
    log("Lightstreamer session created")

    stream.ls_client.addListener(_ClientListener(drop))

    sub = Subscription(
        mode="MERGE",
        items=[f"CHART:{epic}:5MINUTE" for epic in EPICS],
        fields=FIELDS,
    )
    sub.addListener(_SubListener(writer, drop))
    stream.subscribe(sub)
    log(f"subscribed to {len(EPICS)} candle feeds — streaming")

    # Block until the connection drops (heartbeat log hourly).
    while not drop.wait(timeout=3600):
        log("heartbeat: stream alive")
    try:
        stream.disconnect()
    except Exception:
        pass


def main() -> None:
    if not (IG_USERNAME and IG_PASSWORD and IG_API_KEY):
        log("missing IG_USERNAME/IG_PASSWORD/IG_API_KEY in environment — "
            "source the framework's .env first")
        sys.exit(1)
    writer = CandleWriter()
    delay = RECONNECT_MIN_S
    while True:
        started = time.time()
        try:
            run_once(writer)
        except Exception as e:
            log(f"stream cycle failed: {type(e).__name__}: {e}")
        # backoff: reset if the last cycle survived a while
        if time.time() - started > 600:
            delay = RECONNECT_MIN_S
        log(f"reconnecting in {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, RECONNECT_MAX_S)


if __name__ == "__main__":
    main()
