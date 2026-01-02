# ui/marketpredictor_tab.py
# -*- coding: utf-8 -*-
"""
Market Predictor Tab
- Scanner: fetches 15m & 30m klines, futures stats, computes indicators, stores to SQLite.
- Dashboard: Top lists + Recent Grouped Statistics (aligned, monospaced).
- Trend: plot time-series for AVG and CR buckets (DI+, DI-) and export CSV.

CR rules:
 4CR: CNDRating ≥ 7 and RSI < 30
 3CR: CNDRating ≥ 5 and RSI < 50 and MACDLine > MACDSignal
 2CR: CNDRating ≥ 3 and RSI < 70
 1CR: everything else

All displayed times are localized to Australia/Adelaide.

DB columns of interest for ADX:
- ADX       : legacy (15m) for backward compatibility (now ADX(30) on 15m)
- ADX_15m   : ADX(30) computed from 15m klines
- ADX_30m   : ADX(30) computed from 30m klines

NEW (OHLCV snapshot columns):
- OpenPrice, HighPrice, LowPrice, ClosePrice, Volume
(CurrentPrice remains as legacy "close" for compatibility.)

NEW (Liquidation stream ingestion):
- Table: liquidations
  Symbol TEXT
  price_level REAL
  liq_volume REAL
  side TEXT          -- "LONG" or "SHORT" (mapped from order side)
  timestamp INTEGER  -- milliseconds (Binance event time)
"""
from __future__ import annotations

import os
import gzip
import time
import queue
import threading
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import requests
from requests.adapters import HTTPAdapter, Retry

import numpy as np
import pandas as pd

from utils.db import connect
from utils.perf import timed

# Matplotlib (for Trend tab)
_MPL_OK = True
try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib.pyplot as plt
except Exception:
    _MPL_OK = False

# WebSocket (for liquidation stream)
_WS_OK = True
try:
    import websocket  # websocket-client
except Exception:
    _WS_OK = False

# -------------------------------
# Configuration
# -------------------------------
BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_FUTURES_DATA = "https://fapi.binance.com/futures/data"
DB_NAME = "marketpredictor.db"
ARCHIVE_DIR = "archive"
USER_AGENT = "AutoBot-MarketPredictor/1.0"
MAX_WORKERS = max(2, os.cpu_count() // 2)

# Monthly archive retention (rolling)
ARCHIVE_RETENTION_MONTHS = 12
ARCHIVE_PREFIX = "marketpredictor_"
ARCHIVE_SUFFIX = ".sqlite.gz"

OI_PERIOD = "5m"
LS_PERIOD = "5m"
TT_PERIOD = "5m"

KLINE_INTERVAL = "15m"
KLINE_LIMIT = 300

RSI_LEN = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
ATR_LEN = 14

# IMPORTANT: ADX_LEN now 30 to match Trending/Futures ADX(30)
ADX_LEN = 30

DASH_REFRESH_MS = 2000  # 2s

# Local timezone for all display
LOCAL_TZ = "Australia/Adelaide"

# Liquidation stream (All-market snapshot stream)
# Docs: !forceOrder@arr provides the latest liquidation order snapshot per symbol up to 1/s.
LIQ_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

# -------------------------------
# Utilities
# -------------------------------
def month_stamp(ts: Optional[dt.datetime] = None) -> str:
    if ts is None:
        ts = dt.datetime.utcnow()
    return ts.strftime("%Y_%m")


def ensure_dir(p: str | os.PathLike) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def cleanup_old_archives(retain: int = ARCHIVE_RETENTION_MONTHS) -> None:
    """
    Keep only the most recent `retain` archives in ARCHIVE_DIR.
    Archives are named: marketpredictor_YYYY_MM.sqlite.gz
    Sorting is by YYYY_MM in filename (lexicographically safe).
    """
    try:
        ensure_dir(ARCHIVE_DIR)
        files: List[str] = []
        for fn in os.listdir(ARCHIVE_DIR):
            if fn.startswith(ARCHIVE_PREFIX) and fn.endswith(ARCHIVE_SUFFIX):
                files.append(fn)

        files.sort()  # YYYY_MM sorts correctly
        if len(files) <= retain:
            return

        to_delete = files[: max(0, len(files) - retain)]
        for fn in to_delete:
            fpath = os.path.join(ARCHIVE_DIR, fn)
            try:
                os.remove(fpath)
            except Exception:
                pass
    except Exception:
        # Never allow retention errors to break scanning
        pass


# -------------------------------
# HTTP client
# -------------------------------
def make_http_session(timeout=10, total_retries=5, backoff=0.3) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
    )
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    sess.request_timeout = timeout
    return sess


SESSION = make_http_session()


def http_get(url: str, params: dict | None = None) -> dict | list:
    r = SESSION.get(url, params=params, timeout=getattr(SESSION, "request_timeout", 10))
    r.raise_for_status()
    ctype = r.headers.get("Content-Type", "")
    if "application/json" in ctype or r.text.strip().startswith(("{", "[")):
        return r.json()
    return r.text


# -------------------------------
# SQLite helpers (via utils.db.connect)
# -------------------------------
def get_db_path() -> str:
    return os.path.abspath(DB_NAME)


def with_conn():
    """
    Wrapper around utils.db.connect so all heavy DB work goes through the same
    connection factory (WAL, busy_timeout, etc. are centralized there).
    """
    conn = connect(get_db_path())
    try:
        conn.execute("PRAGMA busy_timeout=15000;")
    except Exception:
        pass
    return conn


def _table_columns(table: str) -> List[str]:
    with with_conn() as con:
        cur = con.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in cur.fetchall()]


def ensure_schema_upgrade():
    """Create tables if missing and add columns if absent."""
    with with_conn() as con:
        cur = con.cursor()
        # meta table
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )"""
        )

        # marketpredictor table
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS marketpredictor (
            Timestamp TEXT NOT NULL,
            Symbol TEXT NOT NULL,
            RSI REAL,
            LongShortRatio REAL,
            TopTraderRatio REAL,
            CNDRating REAL,
            SignalQuality REAL,
            CurrentPrice REAL,

            -- OHLCV snapshot (15m candle close-time)
            OpenPrice REAL,
            HighPrice REAL,
            LowPrice REAL,
            ClosePrice REAL,
            Volume REAL,

            MACDLine REAL,
            MACDSignal REAL,
            ATR REAL,
            DIp REAL,
            DIm REAL,
            ADX REAL,
            ADX_15m REAL,
            ADX_30m REAL,
            VWAP REAL,
            OpenInterest REAL,
            FundingRate REAL,
            FundingMarkPrice REAL,
            PRIMARY KEY (Timestamp, Symbol)
        )"""
        )

        # liquidation table
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS liquidations (
            Symbol TEXT NOT NULL,
            price_level REAL,
            liq_volume REAL,
            side TEXT,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (Symbol, timestamp, side, price_level)
        )"""
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_liq_time ON liquidations(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_liq_symbol_time ON liquidations(Symbol, timestamp)")

        # schema upgrade for existing DBs
        cols = _table_columns("marketpredictor")

        # OHLCV columns
        if "OpenPrice" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN OpenPrice REAL")
        if "HighPrice" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN HighPrice REAL")
        if "LowPrice" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN LowPrice REAL")
        if "ClosePrice" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN ClosePrice REAL")
        if "Volume" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN Volume REAL")

        if "MACDSignal" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN MACDSignal REAL")
        if "ADX_15m" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN ADX_15m REAL")
        if "ADX_30m" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN ADX_30m REAL")
        if "VWAP" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN VWAP REAL")
        if "OpenInterest" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN OpenInterest REAL")
        if "FundingRate" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN FundingRate REAL")
        if "FundingMarkPrice" not in cols:
            cur.execute("ALTER TABLE marketpredictor ADD COLUMN FundingMarkPrice REAL")

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mp_symbol_time ON marketpredictor(Symbol, Timestamp)"
        )
        cur.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('month', ?)",
            (month_stamp(),),
        )


def archive_if_month_changed():
    dbp = get_db_path()
    ensure_dir(ARCHIVE_DIR)
    with with_conn() as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT value FROM meta WHERE key='month'")
        row = cur.fetchone()
        now_m = month_stamp()
        if row is None:
            cur.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('month', ?)", (now_m,)
            )
            return
        prev_m = row[0]
        if prev_m == now_m:
            return

    # Month changed: archive existing DB contents
    arch_path = os.path.join(ARCHIVE_DIR, f"{ARCHIVE_PREFIX}{prev_m}{ARCHIVE_SUFFIX}")
    with open(dbp, "rb") as f_in, gzip.open(arch_path, "wb") as f_out:
        f_out.writelines(f_in)

    # Enforce rolling retention
    cleanup_old_archives(ARCHIVE_RETENTION_MONTHS)

    # Recreate tables with current schema (fresh month DB)
    with with_conn() as con:
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS marketpredictor")
        cur.execute(
            """
        CREATE TABLE marketpredictor (
            Timestamp TEXT NOT NULL,
            Symbol TEXT NOT NULL,
            RSI REAL,
            LongShortRatio REAL,
            TopTraderRatio REAL,
            CNDRating REAL,
            SignalQuality REAL,
            CurrentPrice REAL,

            -- OHLCV snapshot (15m candle close-time)
            OpenPrice REAL,
            HighPrice REAL,
            LowPrice REAL,
            ClosePrice REAL,
            Volume REAL,

            MACDLine REAL,
            MACDSignal REAL,
            ATR REAL,
            DIp REAL,
            DIm REAL,
            ADX REAL,
            ADX_15m REAL,
            ADX_30m REAL,
            VWAP REAL,
            OpenInterest REAL,
            FundingRate REAL,
            FundingMarkPrice REAL,
            PRIMARY KEY (Timestamp, Symbol)
        )
        """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mp_symbol_time ON marketpredictor(Symbol, Timestamp)"
        )

        cur.execute("DROP TABLE IF EXISTS liquidations")
        cur.execute(
            """
        CREATE TABLE liquidations (
            Symbol TEXT NOT NULL,
            price_level REAL,
            liq_volume REAL,
            side TEXT,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (Symbol, timestamp, side, price_level)
        )"""
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_liq_time ON liquidations(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_liq_symbol_time ON liquidations(Symbol, timestamp)")

        cur.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('month', ?)",
            (month_stamp(),),
        )


def upsert_row(row: Dict):
    """Insert or replace a single symbol snapshot row into marketpredictor."""
    with with_conn() as con:
        cur = con.cursor()
        cur.execute(
            """
        INSERT OR REPLACE INTO marketpredictor
        (Timestamp, Symbol, RSI, LongShortRatio, TopTraderRatio, CNDRating, SignalQuality,
         CurrentPrice, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume,
         MACDLine, MACDSignal, ATR, DIp, DIm, ADX, ADX_15m, ADX_30m,
         VWAP, OpenInterest, FundingRate, FundingMarkPrice)
        VALUES
        (:Timestamp, :Symbol, :RSI, :LongShortRatio, :TopTraderRatio, :CNDRating, :SignalQuality,
         :CurrentPrice, :OpenPrice, :HighPrice, :LowPrice, :ClosePrice, :Volume,
         :MACDLine, :MACDSignal, :ATR, :DIp, :DIm, :ADX, :ADX_15m, :ADX_30m,
         :VWAP, :OpenInterest, :FundingRate, :FundingMarkPrice)
        """,
            row,
        )


def insert_liquidation(symbol: str, price_level: Optional[float], liq_volume: Optional[float], side: str, ts_ms: int):
    """
    Upsert-like insert for liquidation events. Uses PRIMARY KEY to dedupe.
    side is stored as "LONG" or "SHORT".
    """
    if not symbol or not ts_ms:
        return
    side = (side or "").upper()
    if side not in ("LONG", "SHORT"):
        return
    with with_conn() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO liquidations(Symbol, price_level, liq_volume, side, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (symbol, price_level, liq_volume, side, int(ts_ms)),
        )


# -------------------------------
# Liquidation stream ingestion
# -------------------------------
class LiquidationStream(threading.Thread):
    """
    Ingests Binance USD-M Futures all-market liquidation snapshots via WebSocket stream:
      wss://fstream.binance.com/ws/!forceOrder@arr

    Payload pattern includes an "o" object containing fields such as:
      s (symbol), S (side BUY/SELL), ap (avg price), p (price), q (orig qty), l (last filled qty), T (time)
    We map:
      price_level = ap if available else p
      liq_volume  = l if available else q
      side        = "LONG" if S == "SELL" else "SHORT" if S == "BUY"
      timestamp   = T (ms)
    """
    def __init__(self, notify_queue: "queue.Queue[Tuple[str, str]]"):
        super().__init__(daemon=True)
        self.notify_queue = notify_queue
        self._stopping = threading.Event()
        self._ws = None

    def stop(self):
        self._stopping.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    @staticmethod
    def _map_side(order_side: str) -> Optional[str]:
        # If liquidation order is SELL => long positions liquidated (LONG).
        # If liquidation order is BUY  => short positions liquidated (SHORT).
        s = (order_side or "").upper()
        if s == "SELL":
            return "LONG"
        if s == "BUY":
            return "SHORT"
        return None

    def run(self):
        if not _WS_OK:
            self.notify_queue.put(("liq_status", "Liquidation stream unavailable (websocket-client not installed)."))
            return

        backoff = 1.0
        while not self._stopping.is_set():
            try:
                self.notify_queue.put(("liq_status", "Liquidation stream connecting..."))
                self._ws = websocket.create_connection(LIQ_WS_URL, timeout=10)
                self.notify_queue.put(("liq_status", "Liquidation stream connected."))

                backoff = 1.0
                while not self._stopping.is_set():
                    try:
                        raw = self._ws.recv()
                        if not raw:
                            continue
                        msg = None
                        try:
                            msg = pd.io.json.loads(raw)
                        except Exception:
                            # fallback for rare payload parsing issues
                            import json
                            msg = json.loads(raw)

                        # all-market stream returns array of events
                        if isinstance(msg, list):
                            events = msg
                        else:
                            events = [msg]

                        for ev in events:
                            o = ev.get("o") if isinstance(ev, dict) else None
                            if not isinstance(o, dict):
                                continue

                            symbol = o.get("s")
                            order_side = o.get("S")
                            mapped = self._map_side(order_side)
                            if not mapped:
                                continue

                            # Price: prefer avgPrice ("ap") else price ("p")
                            price_level = None
                            ap = o.get("ap")
                            p = o.get("p")
                            try:
                                if ap is not None:
                                    price_level = float(ap)
                                elif p is not None:
                                    price_level = float(p)
                            except Exception:
                                price_level = None

                            # Volume: prefer last filled qty ("l") else orig qty ("q")
                            liq_volume = None
                            lq = o.get("l")
                            q = o.get("q")
                            try:
                                if lq is not None:
                                    liq_volume = float(lq)
                                elif q is not None:
                                    liq_volume = float(q)
                            except Exception:
                                liq_volume = None

                            ts_ms = o.get("T") or ev.get("E") or ev.get("T")
                            try:
                                ts_ms = int(ts_ms)
                            except Exception:
                                continue

                            try:
                                insert_liquidation(symbol, price_level, liq_volume, mapped, ts_ms)
                            except Exception:
                                pass

                    except Exception:
                        # read loop broken: reconnect
                        break

                try:
                    self._ws.close()
                except Exception:
                    pass

            except Exception:
                pass

            if self._stopping.is_set():
                break

            self.notify_queue.put(("liq_status", f"Liquidation stream reconnecting in {backoff:.1f}s..."))
            time.sleep(backoff)
            backoff = min(30.0, backoff * 1.8)


# -------------------------------
# Indicator calculations
# -------------------------------
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(close: pd.Series, length: int = RSI_LEN) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1 / length, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    return (100 - (100 / (1 + rs))).fillna(0.0)


def macd_line(
    close: pd.Series, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd = ema_fast - ema_slow
    macd_signal = ema(macd, signal)
    hist = macd - macd_signal
    return macd, macd_signal, hist


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr


def atr(
    high: pd.Series, low: pd.Series, close: pd.Series, length: int = ATR_LEN
) -> pd.Series:
    return true_range(high, low, close).ewm(alpha=1 / length, adjust=False).mean()


def di_adx(
    high: pd.Series, low: pd.Series, close: pd.Series, length: int = ADX_LEN
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Wilder-style DI+/DI-/ADX consistent with your Trending tab implementation,
    returning DI+ / DI- / ADX as Series.
    """
    n = len(close)
    idx = high.index

    if n < (2 * length + 1):
        zero = pd.Series([0.0] * n, index=idx)
        return zero.copy(), zero.copy(), zero.copy()

    h = high.to_numpy(dtype=float)
    l = low.to_numpy(dtype=float)
    c = close.to_numpy(dtype=float)

    dm_plus = np.zeros(n, dtype=float)
    dm_minus = np.zeros(n, dtype=float)
    tr = np.zeros(n, dtype=float)

    for i in range(1, n):
        up = h[i] - h[i - 1]
        dn = l[i - 1] - l[i]
        dm_plus[i] = up if (up > dn and up > 0) else 0.0
        dm_minus[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(
            h[i] - l[i],
            abs(h[i] - c[i - 1]),
            abs(l[i] - c[i - 1]),
        )

    def _rma(vals: np.ndarray) -> np.ndarray:
        out = np.full(n, np.nan, dtype=float)
        acc = float(np.sum(vals[1 : length + 1]))
        out[length] = acc / length
        for i in range(length + 1, n):
            acc = acc - (acc / length) + vals[i]
            out[i] = acc / length
        return out

    atr_arr = _rma(tr)
    rma_p = _rma(dm_plus)
    rma_m = _rma(dm_minus)

    with np.errstate(divide="ignore", invalid="ignore"):
        di_p_raw = 100.0 * (rma_p / atr_arr)
        di_m_raw = 100.0 * (rma_m / atr_arr)

    di_p = np.where(np.isfinite(di_p_raw), di_p_raw, np.nan)
    di_m = np.where(np.isfinite(di_m_raw), di_m_raw, np.nan)

    dx = np.full(n, np.nan, dtype=float)
    for i in range(n):
        p = di_p[i]
        m = di_m[i]
        if np.isfinite(p) and np.isfinite(m) and (p + m) != 0.0:
            dx[i] = 100.0 * abs(p - m) / (p + m)

    adx_vals = np.full(n, np.nan, dtype=float)
    start = length + 1
    end = 2 * length + 1
    if end <= n and np.all(~np.isnan(dx[start:end])):
        seed = float(np.nanmean(dx[start:end]))
        adx_vals[end - 1] = seed
        for i in range(end, n):
            if np.isnan(dx[i]):
                adx_vals[i] = adx_vals[i - 1]
            else:
                adx_vals[i] = ((adx_vals[i - 1] * (length - 1)) + dx[i]) / length

    plus_di = pd.Series(di_p, index=idx).fillna(0.0)
    minus_di = pd.Series(di_m, index=idx).fillna(0.0)
    adx_series = pd.Series(adx_vals, index=idx).fillna(0.0)
    return plus_di, minus_di, adx_series


def vwap_from_klines(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = typical * df["volume"]
    cum_pv = pv.cumsum()
    cum_vol = df["volume"].cumsum().replace(0, np.nan)
    return (cum_pv / cum_vol).ffill().fillna(0.0)


def compute_cnd_and_signal(df: pd.DataFrame) -> Tuple[float, float]:
    try:
        rsi_latest = df["RSI"].iloc[-1]
        hist_slope = (
            df["MACDHist"].iloc[-1] - df["MACDHist"].iloc[-3] if len(df) >= 3 else 0.0
        )
        adx_latest = df["ADX"].iloc[-1]
        cnd = (
            (100 - abs(50 - rsi_latest) * 2) * 0.4
            + max(0.0, min(100.0, 50 + hist_slope * 500)) * 0.3
            + min(100.0, adx_latest) * 0.3
        )
        di_agree = (
            1.0
            if (df["DIp"].iloc[-1] >= df["DIm"].iloc[-1])
            == (df["MACD"].iloc[-1] >= 0)
            else 0.0
        )
        sq = 50 + 50 * di_agree
        return float(round(cnd, 2)), float(round(sq, 2))
    except Exception:
        return 0.0, 0.0


# -------------------------------
# Binance helpers
# -------------------------------
def fetch_usdt_symbols() -> List[str]:
    ex = http_get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    symbols = []
    for s in ex.get("symbols", []):
        if s.get("contractType") in ("PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"):
            sym = s.get("symbol", "")
            if sym.endswith("USDT"):
                symbols.append(sym)
    return sorted(set(symbols))


def fetch_klines(
    symbol: str, interval: str = KLINE_INTERVAL, limit: int = KLINE_LIMIT
) -> pd.DataFrame:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = http_get(f"{BINANCE_FAPI}/fapi/v1/klines", params=params)
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "qav",
        "num_trades",
        "taker_base",
        "taker_quote",
        "ignore",
    ]
    df = pd.DataFrame(data, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df[["ts", "open", "high", "low", "close", "volume"]]


def fetch_long_short_ratio(symbol: str, period: str = LS_PERIOD) -> Optional[float]:
    params = {"symbol": symbol, "period": period, "limit": 1}
    try:
        arr = http_get(
            f"{BINANCE_FUTURES_DATA}/globalLongShortAccountRatio", params=params
        )
        if arr:
            return float(arr[-1]["longShortRatio"])
    except Exception:
        pass
    return None


def fetch_top_trader_ratio(symbol: str, period: str = TT_PERIOD) -> Optional[float]:
    params = {"symbol": symbol, "period": period, "limit": 1}
    try:
        arr = http_get(
            f"{BINANCE_FUTURES_DATA}/topLongShortPositionRatio", params=params
        )
        if arr:
            return float(arr[-1]["longShortRatio"])
    except Exception:
        pass
    return None


def fetch_open_interest(symbol: str, period: str = OI_PERIOD) -> Optional[float]:
    params = {"symbol": symbol, "period": period, "limit": 1}
    try:
        arr = http_get(f"{BINANCE_FUTURES_DATA}/openInterestHist", params=params)
        if arr:
            return float(arr[-1]["sumOpenInterestValue"])
    except Exception:
        pass
    return None


def fetch_funding_and_mark(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    params = {"symbol": symbol}
    try:
        data = http_get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex", params=params)
        fr = (
            float(data.get("lastFundingRate", 0.0))
            if "lastFundingRate" in data
            else None
        )
        mark = float(data.get("markPrice", 0.0)) if "markPrice" in data else None
        return fr, mark
    except Exception:
        return None, None


# -------------------------------
# Scanner
# -------------------------------
@dataclass
class ScanResult:
    symbol: str
    row: Dict


class MarketScanner(threading.Thread):
    def __init__(
        self,
        symbols: List[str],
        interval_s: int,
        notify_queue: "queue.Queue[Tuple[str, str]]",
    ):
        super().__init__(daemon=True)
        self.symbols = symbols
        self.interval_s = max(2, int(interval_s))
        self.notify_queue = notify_queue
        self._stopping = threading.Event()

    def stop(self):
        self._stopping.set()

    @timed("mp_scan_symbol", threshold_ms=500)
    def _scan_symbol(self, symbol: str) -> Optional[ScanResult]:
        try:
            # --- 15m data (main, ADX(30) on 15m) ---
            df15 = fetch_klines(symbol, interval=KLINE_INTERVAL, limit=KLINE_LIMIT)
            if df15.empty:
                return None

            rsi_s = rsi(df15["close"], RSI_LEN)
            macd_s, macd_sig, macd_hist = macd_line(
                df15["close"], MACD_FAST, MACD_SLOW, MACD_SIGNAL
            )
            atr_s = atr(df15["high"], df15["low"], df15["close"], ATR_LEN)
            di_p_15, di_m_15, adx_15 = di_adx(
                df15["high"], df15["low"], df15["close"], ADX_LEN
            )
            vwap_s = vwap_from_klines(df15)

            df_ind = df15.copy()
            df_ind["RSI"] = rsi_s
            df_ind["MACD"] = macd_s
            df_ind["MACDSignal"] = macd_sig
            df_ind["MACDHist"] = macd_hist
            df_ind["ATR"] = atr_s
            df_ind["DIp"] = di_p_15
            df_ind["DIm"] = di_m_15
            df_ind["ADX"] = adx_15
            df_ind["VWAP"] = vwap_s

            # --- 30m ADX(30) (separate timeframe) ---
            adx_30_latest: Optional[float] = None
            try:
                df30 = fetch_klines(symbol, interval="30m", limit=KLINE_LIMIT)
                if not df30.empty:
                    _di_p_30, _di_m_30, adx_30 = di_adx(
                        df30["high"], df30["low"], df30["close"], ADX_LEN
                    )
                    adx_30_latest = float(round(adx_30.iloc[-1], 4))
            except Exception:
                adx_30_latest = None

            latest = df_ind.iloc[-1]
            lsr = fetch_long_short_ratio(symbol)
            ttr = fetch_top_trader_ratio(symbol)
            oi = fetch_open_interest(symbol)
            fr, mark = fetch_funding_and_mark(symbol)
            cnd, sigq = compute_cnd_and_signal(df_ind)

            ts = latest["ts"].strftime("%Y-%m-%d %H:%M:%S%z")
            adx_15_latest = float(round(adx_15.iloc[-1], 4))

            row = {
                "Timestamp": ts,
                "Symbol": symbol,

                # OHLCV snapshot (latest 15m candle close-time)
                "OpenPrice": float(round(latest["open"], 8)),
                "HighPrice": float(round(latest["high"], 8)),
                "LowPrice": float(round(latest["low"], 8)),
                "ClosePrice": float(round(latest["close"], 8)),
                "Volume": float(round(latest["volume"], 8)),

                "RSI": float(round(latest["RSI"], 4)),
                "LongShortRatio": float(lsr) if lsr is not None else None,
                "TopTraderRatio": float(ttr) if ttr is not None else None,
                "CNDRating": cnd,
                "SignalQuality": sigq,

                # Legacy "CurrentPrice" (kept for backward compatibility) == close
                "CurrentPrice": float(round(latest["close"], 8)),

                "MACDLine": float(round(latest["MACD"], 8)),
                "MACDSignal": float(round(latest["MACDSignal"], 8)),
                "ATR": float(round(latest["ATR"], 8)),
                "DIp": float(round(latest["DIp"], 4)),
                "DIm": float(round(latest["DIm"], 4)),

                # Legacy ADX column (15m) kept for backward compatibility
                "ADX": adx_15_latest,
                "ADX_15m": adx_15_latest,
                "ADX_30m": adx_30_latest,

                "VWAP": float(round(latest["VWAP"], 8)),
                "OpenInterest": float(oi) if oi is not None else None,
                "FundingRate": float(fr) if fr is not None else None,
                "FundingMarkPrice": float(mark) if mark is not None else None,
            }
            return ScanResult(symbol=symbol, row=row)
        except Exception:
            return None

    def run(self):
        ensure_schema_upgrade()
        while not self._stopping.is_set():
            try:
                archive_if_month_changed()
            except Exception:
                pass

            started = time.time()
            try:
                # Periodic refresh of symbol list (1x per hour)
                if int(started) % 3600 < self.interval_s:
                    try:
                        self.symbols = fetch_usdt_symbols()
                    except Exception:
                        pass

                for symbol in list(self.symbols):
                    if self._stopping.is_set():
                        break
                    res = self._scan_symbol(symbol)
                    if res:
                        try:
                            upsert_row(res.row)
                            self.notify_queue.put(("ok", res.symbol))
                        except Exception as dbe:
                            self.notify_queue.put(("db_error", f"{res.symbol}: {dbe}"))
                    time.sleep(0.12)
            except Exception as e:
                self.notify_queue.put(("error", str(e)))

            elapsed = time.time() - started
            sleep_for = max(0, self.interval_s - elapsed)
            self.notify_queue.put(
                ("status", f"Last scan took {elapsed:.1f}s. Sleeping {sleep_for:.1f}s.")
            )
            end_ts = time.time() + sleep_for
            while time.time() < end_ts and not self._stopping.is_set():
                time.sleep(0.25)


# -------------------------------
# Dashboard helpers
# -------------------------------
def _latest_per_symbol_df() -> pd.DataFrame:
    with with_conn() as con:
        df = pd.read_sql_query(
            """
        SELECT mp.*
        FROM marketpredictor mp
        JOIN (
            SELECT Symbol, MAX(Timestamp) AS max_ts
            FROM marketpredictor
            GROUP BY Symbol
        ) t
          ON t.Symbol = mp.Symbol AND t.max_ts = mp.Timestamp
        """,
            con,
        )
    return df


def _last_n_timestamps(n: int = 30) -> List[str]:
    with with_conn() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT Timestamp FROM marketpredictor ORDER BY Timestamp DESC LIMIT ?",
            (n,),
        )
        return [r[0] for r in cur.fetchall()]


def _rows_for_timestamp(ts: str) -> pd.DataFrame:
    cols = _table_columns("marketpredictor")
    want = ["Symbol", "DIp", "DIm", "RSI", "FundingRate", "CNDRating", "MACDLine"]
    select_cols = want.copy()
    has_macd_signal = "MACDSignal" in cols
    if has_macd_signal:
        select_cols.append("MACDSignal")
    with with_conn() as con:
        df = pd.read_sql_query(
            f"SELECT {', '.join(select_cols)} FROM marketpredictor WHERE Timestamp = ?",
            con,
            params=(ts,),
        )
    if not has_macd_signal:
        df["MACDSignal"] = np.nan
    return df


def _pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{x*100:.5f}%"


def _num(x: Optional[float], digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{float(x):.{digits}f}"


def _to_local_hms(ts: str) -> str:
    """Render DB UTC timestamp as HH:MM:SS in Australia/Adelaide."""
    try:
        dtv = pd.to_datetime(ts, errors="coerce", utc=True)
        if pd.isna(dtv):
            return ts
        return dtv.tz_convert(LOCAL_TZ).strftime("%H:%M:%S")
    except Exception:
        return ts


def _bucketize(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"1CR": df, "2CR": df, "3CR": df, "4CR": df}
    for c in [
        "CNDRating",
        "RSI",
        "MACDLine",
        "MACDSignal",
        "DIp",
        "DIm",
        "FundingRate",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    mask4 = (df["CNDRating"] >= 7.0) & (df["RSI"] < 30.0)
    df4 = df.loc[mask4]
    rem = df.loc[~mask4]
    mask3 = (rem["CNDRating"] >= 5.0) & (rem["RSI"] < 50.0) & (
        rem["MACDLine"] > rem["MACDSignal"]
    )
    df3 = rem.loc[mask3]
    rem = rem.loc[~mask3]
    mask2 = (rem["CNDRating"] >= 3.0) & (rem["RSI"] < 70.0)
    df2 = rem.loc[mask2]
    df1 = rem.loc[~mask2]
    return {"1CR": df1, "2CR": df2, "3CR": df3, "4CR": df4}


def _avg_line(
    df: pd.DataFrame,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if df.empty:
        return (None, None, None, None)
    return (
        df["DIp"].mean(),
        df["DIm"].mean(),
        df["RSI"].mean(),
        df["FundingRate"].mean(),
    )


def _avg3(
    df: pd.DataFrame,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if df.empty:
        return (None, None, None)
    return (df["DIp"].mean(), df["DIm"].mean(), df["RSI"].mean())


def _avg2(df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    if df.empty:
        return (None, None)
    return (df["DIp"].mean(), df["DIm"].mean())


# ---- Fixed-width formatting helpers (monospace) ----
def _is_num(x) -> bool:
    return x is not None and not (isinstance(x, float) and pd.isna(x))


def _fw_num(x, width=6, digits=2) -> str:
    return "—".rjust(width) if not _is_num(x) else f"{x:.{digits}f}".rjust(width)


def _fw_pct(x, width=10) -> str:
    return "—".rjust(width) if not _is_num(x) else f"{x*100:.5f}%".rjust(width)


def _seg(name: str, dip, dim, rsi, fr) -> str:
    return (
        f"{name:<3}: "
        f"DI+ {_fw_num(dip)}  "
        f"DI- {_fw_num(dim)}  "
        f"RSI {_fw_num(rsi)}  "
        f"Funding {_fw_pct(fr)}"
    )


# -------------------------------
# UI (Scanner + Dashboard + Trend)
# -------------------------------
class MarketPredictorTab(ttk.Frame):
    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        ensure_schema_upgrade()

        self._scanner: Optional[MarketScanner] = None
        self._liq_stream: Optional[LiquidationStream] = None
        self._queue: "queue.Queue[Tuple[str, str]]" = queue.Queue()

        self._root_nb = ttk.Notebook(self)
        self._root_nb.pack(fill="both", expand=True)

        self._scanner_tab = ttk.Frame(self._root_nb)
        self._dashboard_tab = ttk.Frame(self._root_nb)
        self._trend_tab = ttk.Frame(self._root_nb)

        self._root_nb.add(self._scanner_tab, text="Scanner")
        self._root_nb.add(self._dashboard_tab, text="Dashboard")
        self._root_nb.add(self._trend_tab, text="Trend")

        self._scanner_build(self._scanner_tab)
        self._dashboard_build(self._dashboard_tab)
        self._trend_build(self._trend_tab)

        self._poll_queue()
        self.after(DASH_REFRESH_MS, self._dashboard_refresh)

        self._dash_lock = threading.Lock()
        self._trend_lock = threading.Lock()

    # -------- Scanner --------
    def _scanner_build(self, container: ttk.Frame):
        top = ttk.Frame(container)
        top.pack(fill="x", padx=8, pady=6)

        ttk.Label(top, text="Scan interval (sec):").pack(side="left")
        self.interval_var = tk.IntVar(value=1800)  # default 30m
        ttk.Spinbox(
            top,
            from_=2,
            to=3600 * 6,
            textvariable=self.interval_var,
            width=7,
        ).pack(side="left", padx=(6, 12))

        self.btn_start = ttk.Button(top, text="Start Scanner", command=self._on_start)
        self.btn_start.pack(side="left", padx=6)
        self.btn_stop = ttk.Button(
            top, text="Stop", command=self._on_stop, state="disabled"
        )
        self.btn_stop.pack(side="left", padx=6)

        self.status_var = tk.StringVar(value="Idle.")
        ttk.Label(container, textvariable=self.status_var).pack(
            anchor="w", padx=8, pady=(4, 2)
        )

        # liquidation stream status
        self.liq_status_var = tk.StringVar(
            value=("Liquidation stream enabled." if _WS_OK else "Liquidation stream disabled (missing websocket-client).")
        )
        ttk.Label(container, textvariable=self.liq_status_var, foreground="#555").pack(
            anchor="w", padx=8, pady=(0, 6)
        )

        cols = ("Symbol", "Last Status")
        self.tree = ttk.Treeview(container, columns=cols, show="headings", height=14)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=160 if c == "Symbol" else 420, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=6)

        ttk.Label(container, text=f"DB: {get_db_path()}").pack(anchor="w", padx=8)

    def _on_start(self):
        if self._scanner and self._scanner.is_alive():
            messagebox.showinfo("Market Predictor", "Scanner already running.")
            return
        try:
            ensure_schema_upgrade()
            syms = fetch_usdt_symbols()
        except Exception as e:
            messagebox.showerror("Startup", f"Failed to start: {e}")
            return

        # Start liquidation stream (best-effort, independent of scanner)
        if _WS_OK and (self._liq_stream is None or not self._liq_stream.is_alive()):
            self._liq_stream = LiquidationStream(notify_queue=self._queue)
            self._liq_stream.start()

        self._scanner = MarketScanner(
            symbols=syms, interval_s=self.interval_var.get(), notify_queue=self._queue
        )
        self._scanner.start()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.status_var.set(
            f"Scanner started. Symbols: {len(syms)}. Interval: {self.interval_var.get()}s"
        )

    def _on_stop(self):
        if self._scanner and self._scanner.is_alive():
            self._scanner.stop()
            self._scanner.join(timeout=2.0)
        self._scanner = None

        if self._liq_stream and self._liq_stream.is_alive():
            self._liq_stream.stop()
            self._liq_stream.join(timeout=2.0)
        self._liq_stream = None

        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.status_var.set("Stopped.")

    def _poll_queue(self):
        try:
            while True:
                kind, msg = self._queue.get_nowait()
                if kind == "ok":
                    self._update_tree(msg, "OK")
                elif kind == "db_error":
                    self.status_var.set(f"DB Error: {msg}")
                elif kind == "error":
                    self.status_var.set(f"Error: {msg}")
                elif kind == "status":
                    self.status_var.set(msg)
                elif kind == "liq_status":
                    self.liq_status_var.set(str(msg))
        except queue.Empty:
            pass
        self.after(300, self._poll_queue)

    def _update_tree(self, symbol: str, status: str):
        for iid in self.tree.get_children():
            vals = self.tree.item(iid, "values")
            if vals and vals[0] == symbol:
                self.tree.item(iid, values=(symbol, status))
                return
        self.tree.insert("", "end", values=(symbol, status))

    # -------- Dashboard --------
    def _dashboard_build(self, container: ttk.Frame):
        top = ttk.Frame(container)
        top.pack(fill="both", expand=False, padx=10, pady=(10, 4))

        left = ttk.Frame(top)
        right = ttk.Frame(top)
        left.pack(side="left", fill="both", expand=True)
        right.pack(side="left", fill="both", expand=True, padx=(12, 0))

        ttk.Label(left, text="Top 10 Coins by DI+:", font=("", 12, "bold")).pack(
            anchor="w", pady=(0, 6)
        )
        ttk.Label(
            right,
            text="Top 10 Coins with Funding Rate < -0.2500%:",
            font=("", 12, "bold"),
        ).pack(anchor="w", pady=(0, 6))

        self.txt_top_dip = tk.Text(left, height=10, wrap="none", state="disabled")
        self.txt_top_negfund = tk.Text(right, height=10, wrap="none", state="disabled")
        self.txt_top_dip.pack(fill="both", expand=True)
        self.txt_top_negfund.pack(fill="both", expand=True)

        mid = ttk.Frame(container)
        mid.pack(fill="both", expand=True, padx=10, pady=(10, 4))
        ttk.Label(
            mid,
            text="Recent Grouped Statistics (Last 30):",
            font=("", 12, "bold"),
        ).pack(anchor="w", pady=(0, 6))

        list_wrap = ttk.Frame(mid)
        list_wrap.pack(fill="both", expand=True)

        self._mono = ("Courier New", 10)

        vsb = ttk.Scrollbar(list_wrap, orient="vertical")
        vsb.pack(side="right", fill="y")

        hsb = ttk.Scrollbar(list_wrap, orient="horizontal")
        hsb.pack(side="bottom", fill="x")

        self.lst_recent = tk.Listbox(
            list_wrap,
            height=12,
            font=self._mono,
            xscrollcommand=hsb.set,
            yscrollcommand=vsb.set,
            exportselection=False,
        )
        self.lst_recent.pack(side="left", fill="both", expand=True)

        vsb.config(command=self.lst_recent.yview)
        hsb.config(command=self.lst_recent.xview)

        legend = (
            "4CR (Highest): CND≥7 & RSI<30 — strong bullish (oversold)\n"
            "3CR (High): CND≥5 & RSI<50 & MACD>Signal — strong bullish momentum\n"
            "2CR (Medium): CND≥3 & RSI<70 — moderate bullish\n"
            "1CR (Basic): others — weak/neutral"
        )
        ttk.Label(container, text=legend, foreground="#555").pack(
            anchor="w", padx=10, pady=(6, 8)
        )

        bottom = ttk.Frame(container)
        bottom.pack(fill="x", expand=False, padx=10, pady=(4, 10))
        self.next_update_var = tk.StringVar(value="Next update in: —")
        ttk.Label(
            bottom,
            textvariable=self.next_update_var,
            font=("", 12, "bold"),
            foreground="#4a63ff",
        ).pack(anchor="s")

    def _dashboard_refresh(self):
        threading.Thread(target=self._dashboard_refresh_worker, daemon=True).start()
        self.after(DASH_REFRESH_MS, self._dashboard_refresh)

    @timed("mp_dashboard", threshold_ms=100)
    def _dashboard_refresh_worker(self):
        if not self._dash_lock.acquire(blocking=False):
            return
        try:
            latest_df = _latest_per_symbol_df()
            if latest_df.empty:
                left_text = "No data yet."
                right_text = "No data yet."
                recent_lines = ["No grouped data yet."]
                next_update = "Next update in: —"
            else:
                df_by_dip = latest_df.sort_values("DIp", ascending=False).head(10)
                left_lines = [
                    f"{r['Symbol']}:  DI+ {_num(r.get('DIp'),2)} | RSI {_num(r.get('RSI'),1)} | Funding {_pct(r.get('FundingRate'))}"
                    for _, r in df_by_dip.iterrows()
                ]
                left_text = "\n".join(left_lines) if left_lines else "—"

                df_neg = latest_df[latest_df["FundingRate"] < -0.0025].sort_values(
                    "FundingRate", ascending=True
                ).head(10)
                right_lines = [
                    f"{r['Symbol']}:  DI+ {_num(r.get('DIp'),2)} | RSI {_num(r.get('RSI'),1)} | Funding {_pct(r.get('FundingRate'))}"
                    for _, r in df_neg.iterrows()
                ]
                right_text = (
                    "\n".join(right_lines)
                    if len(df_neg)
                    else "No coins below -0.2500%."
                )

                ts_list = _last_n_timestamps(30)
                if not ts_list:
                    recent_lines = ["No grouped data yet."]
                    next_update = "Next update in: —"
                else:
                    recent_lines = []
                    for ts in ts_list:
                        df_ts = _rows_for_timestamp(ts)
                        buckets = _bucketize(df_ts)

                        avg_dip, avg_dim, avg_rsi = _avg3(df_ts)
                        avg_seg = (
                            f"AVG: "
                            f"DI+ {_fw_num(avg_dip)}  "
                            f"DI- {_fw_num(avg_dim)}  "
                            f"RSI {_fw_num(avg_rsi)}"
                        )

                        s1 = _seg("1CR", *_avg_line(buckets["1CR"]))
                        s2 = _seg("2CR", *_avg_line(buckets["2CR"]))
                        s3 = _seg("3CR", *_avg_line(buckets["3CR"]))
                        s4 = _seg("4CR", *_avg_line(buckets["4CR"]))

                        ts_str = _to_local_hms(ts).ljust(8)
                        line = f"{ts_str} | {avg_seg} | {s1} | {s2} | {s3} | {s4}"
                        recent_lines.append(line)

                    last_ts = max(ts_list) if ts_list else None
                    if last_ts:
                        dt_last = pd.to_datetime(last_ts, errors="coerce", utc=True)
                        if pd.isna(dt_last):
                            next_update = "Next update in: —"
                        else:
                            scan_period = max(2, int(self.interval_var.get() or 1800))
                            next_run = dt_last.to_pydatetime().timestamp() + scan_period
                            remaining = max(0.0, next_run - time.time())
                            next_update = (
                                f"Next update in: {int(remaining//60)}m "
                                f"{int(remaining%60)}s"
                            )
                    else:
                        next_update = "Next update in: —"

            self.after(
                0,
                lambda: self._apply_dashboard_update(
                    left_text, right_text, recent_lines, next_update
                ),
            )
        finally:
            self._dash_lock.release()

    def _apply_dashboard_update(
        self,
        left_text: str,
        right_text: str,
        recent_lines: List[str],
        next_update: str,
    ):
        self._set_text(self.txt_top_dip, left_text)
        self._set_text(self.txt_top_negfund, right_text)
        self._set_recent(recent_lines)
        self.next_update_var.set(next_update)

    # -------- Trend --------
    def _trend_build(self, container: ttk.Frame):
        if not _MPL_OK:
            ttk.Label(
                container,
                text="Matplotlib not available. Install with: pip install matplotlib",
                foreground="red",
            ).pack(padx=12, pady=12, anchor="w")
            return

        controls = ttk.Frame(container)
        controls.pack(fill="x", padx=10, pady=(10, 6))

        ttk.Label(controls, text="Points (most recent):").pack(side="left")
        self.trend_points_var = tk.IntVar(value=120)
        ttk.Spinbox(
            controls,
            from_=10,
            to=2000,
            increment=10,
            textvariable=self.trend_points_var,
            width=6,
        ).pack(side="left", padx=(6, 16))

        self.series_vars: Dict[str, tk.BooleanVar] = {}
        series_names = [
            "AVG DI+",
            "AVG DI-",
            "1CR DI+",
            "1CR DI-",
            "2CR DI+",
            "2CR DI-",
            "3CR DI+",
            "3CR DI-",
            "4CR DI+",
            "4CR DI-",
        ]
        for name in series_names:
            var = tk.BooleanVar(value=name in ("AVG DI+", "AVG DI-"))
            self.series_vars[name] = var
            ttk.Checkbutton(controls, text=name, variable=var).pack(side="left", padx=4)

        ttk.Button(controls, text="Refresh Plot", command=self._trend_refresh).pack(
            side="left", padx=(10, 4)
        )
        ttk.Button(controls, text="Export CSV", command=self._trend_export_csv).pack(
            side="left", padx=(4, 10)
        )

        fig_frame = ttk.Frame(container)
        fig_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.fig, self.ax = plt.subplots(figsize=(9, 4), dpi=100)
        self.ax.set_title("Trend — DI+ / DI- (Averages)")
        self.ax.set_xlabel(f"Time ({LOCAL_TZ})")
        self.ax.set_ylabel("Value")
        self.ax.grid(True, alpha=0.3)

        self.canvas = FigureCanvasTkAgg(self.fig, master=fig_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        self._trend_last_df: Optional[pd.DataFrame] = None

    @timed("mp_trend_df", threshold_ms=80)
    def _trend_build_dataframe(self, n_points: int) -> pd.DataFrame:
        try:
            n_points = int(n_points)
        except Exception:
            n_points = 120
        n_points = min(max(10, n_points), 500)

        ts_list = _last_n_timestamps(n_points)
        if not ts_list:
            return pd.DataFrame()

        rows: List[Dict[str, float]] = []
        for ts in reversed(ts_list):
            df_ts = _rows_for_timestamp(ts)
            buckets = _bucketize(df_ts)

            avg_dip, avg_dim = _avg2(df_ts)
            d1p, d1m = _avg2(buckets["1CR"])
            d2p, d2m = _avg2(buckets["2CR"])
            d3p, d3m = _avg2(buckets["3CR"])
            d4p, d4m = _avg2(buckets["4CR"])

            rows.append(
                {
                    "Timestamp": ts,
                    "AVG_DIp": avg_dip,
                    "AVG_DIm": avg_dim,
                    "CR1_DIp": d1p,
                    "CR1_DIm": d1m,
                    "CR2_DIp": d2p,
                    "CR2_DIm": d2m,
                    "CR3_DIp": d3p,
                    "CR3_DIm": d3m,
                    "CR4_DIp": d4p,
                    "CR4_DIm": d4m,
                }
            )
        df = pd.DataFrame(rows)
        dtv = pd.to_datetime(df["Timestamp"], utc=True, errors="coerce")
        df["LocalTime"] = dtv.dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
        df["ts_display"] = df["LocalTime"]
        return df

    def _trend_refresh(self):
        if not _MPL_OK:
            return
        threading.Thread(target=self._trend_refresh_worker, daemon=True).start()

    def _trend_refresh_worker(self):
        if not self._trend_lock.acquire(blocking=False):
            return
        try:
            try:
                n = max(10, int(self.trend_points_var.get() or 120))
            except Exception:
                n = 120
            n = min(n, 500)
            df = self._trend_build_dataframe(n)
            self.after(0, lambda: self._update_trend_figure(df))
        finally:
            self._trend_lock.release()

    def _update_trend_figure(self, df: pd.DataFrame):
        if not _MPL_OK:
            return
        self._trend_last_df = df.copy()

        self.ax.clear()
        self.ax.set_title("Trend — DI+ / DI- (Averages)")
        self.ax.set_xlabel(f"Time ({LOCAL_TZ})")
        self.ax.set_ylabel("Value")
        self.ax.grid(True, alpha=0.3)

        if df.empty:
            self.ax.text(
                0.5,
                0.5,
                "No data available yet",
                ha="center",
                va="center",
                transform=self.ax.transAxes,
            )
            self.canvas.draw_idle()
            return

        x = df["ts_display"]

        mapping = {
            "AVG DI+": "AVG_DIp",
            "AVG DI-": "AVG_DIm",
            "1CR DI+": "CR1_DIp",
            "1CR DI-": "CR1_DIm",
            "2CR DI+": "CR2_DIp",
            "2CR DI-": "CR2_DIm",
            "3CR DI+": "CR3_DIp",
            "3CR DI-": "CR3_DIm",
            "4CR DI+": "CR4_DIp",
            "4CR DI-": "CR4_DIm",
        }

        any_series = False
        for label, col in mapping.items():
            if self.series_vars.get(label, tk.BooleanVar(value=False)).get():
                self.ax.plot(x, df[col], label=label)
                any_series = True

        if any_series:
            self.ax.legend(loc="upper left", ncol=2)
        else:
            self.ax.text(
                0.5,
                0.5,
                "No series selected",
                ha="center",
                va="center",
                transform=self.ax.transAxes,
            )

        self.fig.autofmt_xdate()
        self.canvas.draw_idle()

    def _trend_export_csv(self):
        if self._trend_last_df is None or self._trend_last_df.empty:
            messagebox.showinfo("Export CSV", "Nothing to export yet.")
            return
        fpath = filedialog.asksaveasfilename(
            title="Export Trend Data",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")],
        )
        if not fpath:
            return
        cols = [c for c in self._trend_last_df.columns if c != "ts_display"]
        try:
            self._trend_last_df[cols].to_csv(fpath, index=False)
            messagebox.showinfo("Export CSV", f"Saved: {fpath}")
        except Exception as e:
            messagebox.showerror("Export CSV", f"Failed to save:\n{e}")

    # -------- small UI helpers --------
    def _set_text(self, widget: tk.Text, content: str):
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", content)
        widget.configure(state="disabled")

    def _set_recent(self, lines: List[str]):
        self.lst_recent.delete("0", "end")
        for ln in lines:
            self.lst_recent.insert("end", ln)
        try:
            self.lst_recent.xview_moveto(0.0)
        except Exception:
            pass


# Manual run
if __name__ == "__main__":
    root = tk.Tk()
    root.title("Market Predictor")
    try:
        ttk.Style().theme_use("clam")
    except tk.TclError:
        pass
    tab = MarketPredictorTab(root)
    tab.pack(fill="both", expand=True)
    root.geometry("1220x780")
    root.mainloop()
