"""
FTSE 15-Minute Data Fetcher
============================
Fetches 15-minute interval FTSE 100 data (spot + futures) and stores locally as CSV.

Requirements:
    pip install yfinance pandas

Tickers used:
    ^FTSE        - FTSE 100 Index (spot)
    Z.F          - FTSE 100 Index Futures (ICE, front month) via yfinance
                   Note: yfinance futures coverage can be limited; see notes below.

Notes on futures:
    - yfinance uses the ticker "Z.F" for FTSE 100 futures (ICE).
    - If "Z.F" is unavailable or stale, the script falls back to "ESM25.L" style
      contract tickers. You can override FUTURES_TICKERS below with specific contracts.
    - For professional-grade futures data consider: Bloomberg, Refinitiv, or
      Interactive Brokers API (ib_insync).

15-minute data availability:
    - yfinance supports 15m intervals for the last 60 days only.
    - Adjust PERIOD or use START/END dates within that window.
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

# ── Configuration ─────────────────────────────────────────────────────────────

OUTPUT_DIR = "gapdata"          # Local folder to save CSVs
INTERVAL   = "5m"                # Candle interval
PERIOD     = "30d"                # Lookback period (max 60d for 15m data)

# Override with specific date range if preferred (set both, or leave as None)
START_DATE = 2026-7-12   # e.g. "2026-03-01"
END_DATE   = None   # e.g. "2026-04-23"

# Tickers: spot index + futures
SPOT_TICKER    = "^FTSE"          # FTSE 100 cash index
FUTURES_TICKERS = [
    "ICE",                        # Generic front-month FTSE futures (ICE) — try first
    # Add explicit contract codes below if needed, e.g.:
    # "FTMIB.F",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(ticker: str, interval: str, period: str, start=None, end=None) -> pd.DataFrame | None:
    """Download OHLCV data for a single ticker. Returns None on failure."""
    print(f"  Fetching {ticker} ...")
    try:
        if start and end:
            df = yf.download(ticker, start=start, end=end, interval=interval,
                             auto_adjust=True, progress=False)
        else:
            df = yf.download(ticker, period=period, interval=interval,
                             auto_adjust=True, progress=False)

        if df is None or df.empty:
            print(f"  ⚠  No data returned for {ticker}.")
            return None

        # Flatten MultiIndex columns (yfinance ≥ 0.2 may produce them)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(c).strip("_") for c in df.columns]

        df.index.name = "Datetime"
        print(f"  ✓  {len(df)} rows fetched for {ticker}.")
        return df

    except Exception as exc:
        print(f"  ✗  Error fetching {ticker}: {exc}")
        return None


def save(df: pd.DataFrame, ticker: str, output_dir: str) -> str:
    """Save DataFrame to a timestamped CSV. Returns the file path."""
    os.makedirs(output_dir, exist_ok=True)
    safe_name  = ticker.replace("^", "").replace(".", "_")
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename   = f"{safe_name}_15m_{timestamp}.csv"
    filepath   = os.path.join(output_dir, filename)
    df.to_csv(filepath)
    print(f"  💾  Saved → {filepath}")
    return filepath


def load_latest(ticker: str, output_dir: str) -> pd.DataFrame | None:
    """Load the most recently saved CSV for a given ticker (utility function)."""
    safe_name = ticker.replace("^", "").replace(".", "_")
    pattern   = f"{safe_name}_15m_"
    files     = sorted(
        [f for f in os.listdir(output_dir) if f.startswith(pattern) and f.endswith(".csv")],
        reverse=True,
    )
    if not files:
        return None
    path = os.path.join(output_dir, files[0])
    return pd.read_csv(path, index_col="Datetime", parse_dates=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  FTSE 15-Minute Data Fetcher")
    print("=" * 55)

    results = {}

    # 1. Spot index
    print("\n[1/2] FTSE 100 Spot Index")
    df_spot = fetch(SPOT_TICKER, INTERVAL, PERIOD, START_DATE, END_DATE)
    if df_spot is not None:
        path = save(df_spot, SPOT_TICKER, OUTPUT_DIR)
        results["spot"] = {"ticker": SPOT_TICKER, "rows": len(df_spot), "file": path}

    # 2. Futures
    print("\n[2/2] FTSE 100 Futures")
    futures_saved = False
    for fticker in FUTURES_TICKERS:
        df_fut = fetch(fticker, INTERVAL, PERIOD, START_DATE, END_DATE)
        if df_fut is not None:
            path = save(df_fut, fticker, OUTPUT_DIR)
            results["futures"] = {"ticker": fticker, "rows": len(df_fut), "file": path}
            futures_saved = True
            break   # Stop after first successful futures ticker

    if not futures_saved:
        print(
            "\n  ⚠  Could not retrieve futures data via yfinance.\n"
            "     FTSE 100 futures (ICE 'Z' contract) have limited coverage.\n"
            "     Consider using the Interactive Brokers API (ib_insync) or\n"
            "     a premium data provider for reliable futures data."
        )

    # Summary
    print("\n" + "=" * 55)
    print("  Summary")
    print("=" * 55)
    if results:
        for kind, info in results.items():
            print(f"  {kind.upper():8s} | {info['ticker']:12s} | {info['rows']:>5} rows | {info['file']}")
    else:
        print("  No data was saved.")
    print()


if __name__ == "__main__":
    main()
