# ui/trending_tab.py
# DCA Trending + Common Trending + All Trending (rectangular version of Common Trending)
#
# All Trending:
#   - Search, Load Symbols, Symbol, Interval, Limit, Fetch & Plot (same as Common)
#   - shows all 7 metrics in one rectangular frame
#   - extra Funding Rate checkbox
#   - auto refresh (sec)
#   - export last fetched data to Excel
#
# Update: Funding rate in All Trending is now plotted & displayed in PERCENT (%),
#         with both decimal and percent saved to export cache.
#
# 2025-11-12 Update:
#   - DCA Trending redesigned to rectangular multi-panel layout (like All Trending).
#   - Each technical has its own panel: ADX, RSI, Coin L/S, Trader L/S, CND, Funding (%),
#     PnL, CumPnL. "Price & Levels" panel combines Entry, Current, TP, SL together.
#   - Checkboxes re-used; visibility toggles per panel, with Entry/TP/SL toggles controlling
#     which lines appear within the single "Price & Levels" panel.

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import sqlite3
from pathlib import Path
import threading

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np

import pandas as pd

from services.rest_client import rest_client
from utils.db import connect
from utils.perf import timed

# =============================================================================
# CONFIG
# =============================================================================
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "dca_positions.db"
MARKET_DB_PATH = DATA_DIR / "marketpredictor.db"  # just for symbol guessing
ADEL = ZoneInfo("Australia/Adelaide")

# Limit number of points drawn for speed
MAX_POINTS = 600

# =============================================================================
# TK HELPERS
# =============================================================================
def on_tk(widget: tk.Misc, fn, *args, **kwargs):
    """Run fn on Tk main thread."""
    if not widget:
        return
    try:
        if not widget.winfo_exists():
            return
    except Exception:
        return
    widget.after(0, lambda: fn(*args, **kwargs))


def guard_destroyed(widget: tk.Misc) -> bool:
    try:
        return (widget is None) or (not widget.winfo_exists())
    except Exception:
        return True

# =============================================================================
# DB HELPERS (for DCA tab)
# =============================================================================
def _db_exists() -> bool:
    return DB_PATH.exists()


def _ensure_dca_positions_schema():
    conn = connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dca_positions (
            log_time TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT,
            size REAL,
            entry REAL,
            current REAL,
            pnl REAL,
            leverage INTEGER,
            margin REAL,
            tp REAL,
            sl REAL,
            adx REAL,
            rsi REAL,
            coin_ls_ratio REAL,
            trader_ls_ratio REAL,
            cnd REAL,
            funding REAL
        )
        """
    )
    cur.execute("PRAGMA table_info(dca_positions)")
    have = {r[1] for r in cur.fetchall()}
    need = [
        "tp","sl","adx","rsi","coin_ls_ratio","trader_ls_ratio","cnd","funding",
        "margin","leverage","pnl","current","entry","size","side"
    ]
    for col in need:
        if col not in have:
            cur.execute(f"ALTER TABLE dca_positions ADD COLUMN {col} REAL")
    conn.commit()
    conn.close()


def _market_db_exists() -> bool:
    return MARKET_DB_PATH.exists()


def _detect_marketpredictor_table_and_columns() -> Optional[tuple[str, dict]]:
    """Just to harvest symbols / latest sentiment if present."""
    if not _market_db_exists():
        return None
    try:
        con = connect(MARKET_DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for tname in tables:
            cur.execute(f"PRAGMA table_info({tname})")
            cols = cur.fetchall()
            names = [c[1] for c in cols]
            norm = {"".join(ch for ch in n.lower() if ch.isalnum()): n for n in names}
            ts = next((norm[k] for k in ("timestamp","time","ts","logtime") if k in norm), None)
            sym = next((norm[k] for k in ("symbol","pair","ticker") if k in norm), None)
            if ts and sym:
                con.close()
                return tname, {"ts": ts, "symbol": sym}
        con.close()
    except Exception:
        return None
    return None


def _symbols_from_marketpredictor_recent(hours: int = 24) -> list[str]:
    det = _detect_marketpredictor_table_and_columns()
    if not det:
        return []
    tbl, cmap = det
    sym_col = cmap["symbol"]
    conn = connect(MARKET_DB_PATH)
    cur = conn.cursor()
    syms = set()
    try:
        cur.execute(f"SELECT DISTINCT {sym_col} FROM {tbl}")
        for (s,) in cur.fetchall():
            syms.add(s)
    except Exception:
        pass
    conn.close()
    return sorted(syms)


def _symbols_from_dca() -> list[str]:
    if not _db_exists():
        return []
    _ensure_dca_positions_schema()
    conn = connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM dca_positions ORDER BY symbol")
    out = [r[0] for r in cur.fetchall()]
    conn.close()
    return out


def _load_symbols() -> list[str]:
    syms = set(_symbols_from_dca())
    syms.update(_symbols_from_marketpredictor_recent(72))
    return sorted(syms)

# =============================================================================
# Binance helpers (via rest_client)
# =============================================================================
def _get_symbols_live() -> list[str]:
    try:
        data = rest_client.get_exchange_info()
        return sorted([
            s["symbol"]
            for s in data.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
        ])
    except Exception:
        return []


def _get_mark_price(symbol: str) -> Optional[float]:
    try:
        data = rest_client.get_json(
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        mp = data.get("markPrice")
        return float(mp) if mp is not None else None
    except Exception:
        return None


def _get_funding_rate(symbol: str) -> Optional[float]:
    try:
        data = rest_client.get_json(
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        fr = data.get("lastFundingRate")
        return float(fr) if fr is not None else None
    except Exception:
        return None


def _get_klines(symbol: str, interval: str, limit: int = 500):
    return rest_client.get_json(
        "/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
    )


def _to_series_from_klines(kl):
    t = [datetime.fromtimestamp(k[0]/1000, tz=ADEL) for k in kl]
    o = [float(k[1]) for k in kl]
    h = [float(k[2]) for k in kl]
    l = [float(k[3]) for k in kl]
    c = [float(k[4]) for k in kl]
    v = [float(k[5]) for k in kl]
    return t, o, h, l, c, v


def _get_global_ls_series(symbol: str, period: str = "5m", limit: int = 500):
    try:
        data = rest_client.get_json(
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": min(limit, 1000)},
        )
        t = [datetime.fromtimestamp(int(d["timestamp"]) / 1000, tz=ADEL) for d in data]
        v = [float(d["longShortRatio"]) for d in data]
        return t, v
    except Exception:
        return [], []


def _get_toptrader_ls_series(symbol: str, period: str = "5m", limit: int = 500):
    try:
        data = rest_client.get_json(
            "/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": period, "limit": min(limit, 1000)},
        )
        t = [datetime.fromtimestamp(int(d["timestamp"]) / 1000, tz=ADEL) for d in data]
        v = [float(d["longShortRatio"]) for d in data]
        return t, v
    except Exception:
        return [], []

# =============================================================================
# TA
# =============================================================================
def _ema(series, period):
    k = 2/(period+1)
    out = []
    prev = None
    for x in series:
        prev = x if prev is None else x*k + prev*(1-k)
        out.append(prev)
    return out


def _rsi(closes, period=14):
    gains, losses = [], []
    rsis = [None]*len(closes)
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0))
        # FIX: use -ch here (was -chg, which caused NameError)
        losses.append(max(-ch, 0.0))
    if len(gains) < period:
        return rsis
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rs = (avg_gain / avg_loss) if avg_loss != 0 else float("inf")
    rsis[period] = 100 - (100/(1+rs))
    for i in range(period+1, len(closes)):
        gain = gains[i-1]
        loss = losses[i-1]
        avg_gain = (avg_gain*(period-1) + gain) / period
        avg_loss = (avg_loss*(period-1) + loss) / period
        rs = (avg_gain / avg_loss) if avg_loss != 0 else float("inf")
        rsis[i] = 100 - (100/(1+rs))
    return rsis


def _macd(closes, fast=12, slow=26, signal=9):
    ef = _ema(closes, fast)
    es = _ema(closes, slow)
    line = [f-s for f, s in zip(ef, es)]
    sig = _ema(line, signal)
    hist = [m-s for m, s in zip(line, sig)]
    return line, sig, hist


def _adx(high, low, close, period=14):
    n = len(close)
    if n < (2*period + 1):
        return [None]*n
    dm_plus = [0.0]*n
    dm_minus = [0.0]*n
    tr = [0.0]*n
    for i in range(1, n):
        up = high[i] - high[i-1]
        dn = low[i-1] - low[i]
        dm_plus[i] = up if (up > dn and up > 0) else 0.0
        dm_minus[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i-1]),
            abs(low[i] - close[i-1]),
        )
    def _rma(vals):
        out = [None]*n
        acc = sum(vals[1:period+1])
        out[period] = acc/period
        for i in range(period+1, n):
            acc = acc - (acc/period) + vals[i]
            out[i] = acc/period
        return out
    atr = _rma(tr)
    rma_p = _rma(dm_plus)
    rma_m = _rma(dm_minus)
    di_p = [(100*(p/a)) if a else None for p, a in zip(rma_p, atr)]
    di_m = [(100*(m/a)) if a else None for m, a in zip(rma_m, atr)]
    dx = [
        (100*abs(p-m)/(p+m)) if (p is not None and m is not None and (p+m) != 0) else None
        for p, m in zip(di_p, di_m)
    ]
    adx_vals = [None]*n
    start = period+1
    end = 2*period+1
    if end <= n and all(d is not None for d in dx[start:end]):
        seed = sum(dx[start:end]) / period
        adx_vals[end-1] = seed
        for i in range(end, n):
            adx_vals[i] = adx_vals[i-1] if dx[i] is None else ((adx_vals[i-1]*(period-1))+dx[i]) / period
    return adx_vals


# --- NEW: Parabolic RSI helper (used ONLY in AllTrendingFrame) ----------------
def _parabolic_trend_from_array(arr: np.ndarray, min_points: int = 15) -> Optional[np.ndarray]:
    """
    Fit a quadratic (parabolic) regression to the finite part of `arr`
    and return a same-length array with NaNs where RSI is NaN.

    Used to draw the dashed red curve over RSI in All Trending 1–5.
    """
    arr = np.asarray(arr, dtype=float)
    mask = np.isfinite(arr)
    if mask.sum() < min_points:
        return None

    y = arr[mask]
    n = len(y)
    x = np.arange(n, dtype=float)
    x = x - x.mean()  # centre x for numerical stability

    a, b, c = np.polyfit(x, y, 2)
    trend_y = a * x**2 + b * x + c

    trend_full = np.full_like(arr, np.nan)
    trend_full[mask] = trend_y
    return trend_full

# =============================================================================
# DCA TRENDING FRAME (rectangular multi-panel)
# =============================================================================
def _parse_time_adelaide(ts: str) -> Optional[datetime]:
    try:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S%z").astimezone(ADEL)
        except Exception:
            return datetime.fromisoformat(ts).astimezone(ADEL)
    except Exception:
        return None


def _append_dca_snapshot(symbol: str):
    if not _db_exists():
        _ensure_dca_positions_schema()
    now = datetime.now(ADEL)
    price = _get_mark_price(symbol)
    funding = _get_funding_rate(symbol)
    conn = connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dca_positions (
            log_time, symbol, current, funding
        ) VALUES (?, ?, ?, ?)
        """,
        (now.isoformat(), symbol, price, funding),
    )
    conn.commit()
    conn.close()


def _fetch_all_columns_no_date_filter(symbol: str) -> list[tuple]:
    if not _db_exists():
        return []
    _ensure_dca_positions_schema()
    conn = connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT log_time, symbol, side, size, entry, current, pnl, leverage, margin,
               tp, sl, adx, rsi, coin_ls_ratio, trader_ls_ratio, cnd, funding
        FROM dca_positions
        WHERE symbol=?
        ORDER BY log_time ASC
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _series_from_rows(rows: list[tuple], days: int) -> tuple[dict[str, list], int]:
    total_before = len(rows)
    cutoff = datetime.now(ADEL) - timedelta(days=int(days))

    t, side, size, entry, current, pnl, lev, margin = [], [], [], [], [], [], [], []
    tp, sl, adx, rsi, coin_lr, trader_lr, cnd, funding = [], [], [], [], [], [], [], []

    for r in rows:
        ts = _parse_time_adelaide(str(r[0]))
        if not ts or ts < cutoff:
            continue
        t.append(ts)
        side.append(r[2])
        size.append(r[3])
        entry.append(r[4])
        current.append(r[5])
        pnl.append(r[6])
        lev.append(r[7])
        margin.append(r[8])
        tp.append(r[9])
        sl.append(r[10])
        adx.append(r[11])
        rsi.append(r[12])
        coin_lr.append(r[13])
        trader_lr.append(r[14])
        cnd.append(r[15])
        funding.append(r[16])

    roe = []
    cum = []
    run = 0.0
    for i in range(len(t)):
        p = float(pnl[i]) if pnl[i] is not None else 0.0
        m = float(margin[i]) if margin[i] is not None else 0.0
        roe.append((p/m*100.0) if m else 0.0)
        run += p
        cum.append(run)

    return {
        "time": t,
        "side": side,
        "size": size,
        "entry": entry,
        "current": current,
        "pnl": pnl,
        "leverage": lev,
        "margin": margin,
        "tp": tp,
        "sl": sl,
        "adx": adx,
        "rsi": rsi,
        "coin_ls_ratio": coin_lr,
        "trader_ls_ratio": trader_lr,
        "cnd": cnd,
        "funding": funding,
        "roe": roe,
        "cum_pnl": cum,
    }, total_before


def _pad_to_len(lst, target_len):
    lst = list(lst or [])
    if len(lst) < target_len:
        lst += [None] * (target_len - len(lst))
    return lst[:target_len]


class DcaTrendingFrame(ttk.Frame):
    """Now uses rectangular, multi-panel layout like AllTrendingFrame."""
    def __init__(self, parent, dca_tab: Optional[tk.Widget] = None, trading_tab: Optional[tk.Widget] = None):
        super().__init__(parent)
        self.dca_tab = dca_tab
        self.trading_tab = trading_tab
        self._destroyed = False
        self._auto_job = None
        self._all_symbols: list[str] = []
        self.bind("<Destroy>", lambda e: setattr(self, "_destroyed", True))

        # cache for quick replotting without refetching DB
        self._cache = None  # dict of latest series
        self._cache_time_for_price = None  # price x-axis with live tail

        self._build_ui()
        self._init_plots()  # set up rectangular grid panels

    def _build_ui(self):
        top = ttk.Frame(self); top.pack(fill="x", padx=10, pady=8)

        ttk.Label(top, text="Symbol:").pack(side="left")
        self.symbol_var = tk.StringVar()
        self.symbol_cb = ttk.Combobox(top, textvariable=self.symbol_var, width=16,
                                      values=_load_symbols(), state="readonly")
        self.symbol_cb.pack(side="left", padx=6)
        self.symbol_cb.bind("<<ComboboxSelected>>", lambda e: self.refresh())

        ttk.Label(top, text="Range (days):").pack(side="left", padx=(12, 0))
        self.days_var = tk.IntVar(value=7)
        ttk.Spinbox(top, from_=1, to=365, textvariable=self.days_var, width=6).pack(side="left", padx=6)

        ttk.Button(top, text="Refresh", command=self.refresh).pack(side="left", padx=6)
        ttk.Button(top, text="Reload Symbols", command=self._reload_symbols).pack(side="left", padx=6)
        ttk.Button(top, text="Snap Now", command=self._snap).pack(side="left", padx=6)

        # toggles — panel visibility and line choices for Price & Levels
        togg = ttk.Frame(top); togg.pack(side="left", padx=14)
        self.show_price_panel = tk.BooleanVar(value=True)  # controls the whole Price & Levels panel visibility
        self.show_entry = tk.BooleanVar(value=True)
        self.show_current = tk.BooleanVar(value=True)
        self.show_tp = tk.BooleanVar(value=True)
        self.show_sl = tk.BooleanVar(value=True)
        self.show_adx = tk.BooleanVar(value=True)
        self.show_rsi = tk.BooleanVar(value=True)
        self.show_funding = tk.BooleanVar(value=True)
        self.show_coin = tk.BooleanVar(value=True)
        self.show_trader = tk.BooleanVar(value=True)
        self.show_cnd = tk.BooleanVar(value=True)
        self.show_pnl = tk.BooleanVar(value=True)
        self.show_cum = tk.BooleanVar(value=True)

        # Price panel visibility first, then internal line toggles
        ttk.Checkbutton(togg, text="Price & Levels", variable=self.show_price_panel,
                        command=self._replot_from_cache).pack(side="left", padx=(0,10))
        for text, var in [
            ("Entry", self.show_entry),
            ("Current", self.show_current),
            ("TP", self.show_tp),
            ("SL", self.show_sl),
            ("ADX", self.show_adx),
            ("RSI", self.show_rsi),
            ("Funding", self.show_funding),
            ("Coin L/S", self.show_coin),
            ("Trader L/S", self.show_trader),
            ("CND", self.show_cnd),
            ("PnL", self.show_pnl),
            ("CumPnL", self.show_cum),
        ]:
            ttk.Checkbutton(togg, text=text, variable=var, command=self._replot_from_cache).pack(side="left")

        auto = ttk.Frame(self); auto.pack(fill="x", padx=10)
        self.auto_log = tk.BooleanVar(value=False)
        ttk.Checkbutton(auto, text="Auto-log selected symbol", variable=self.auto_log,
                        command=self._toggle_auto).pack(side="left")
        ttk.Label(auto, text="Interval (s):").pack(side="left", padx=(10, 2))
        self.auto_sec = tk.IntVar(value=30)
        ttk.Spinbox(auto, from_=5, to=600, textvariable=self.auto_sec, width=6).pack(side="left")

        self.status = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.status).pack(anchor="w", padx=10)

        self.summary = tk.StringVar(value="—")
        ttk.Label(self, textvariable=self.summary).pack(anchor="w", padx=10, pady=(0,4))

        # rectangular plot area (like AllTrendingFrame)
        self.plot_container = ttk.Frame(self, relief="groove", borderwidth=2)
        self.plot_container.pack(fill="both", expand=True, padx=10, pady=(6, 10))

    def _init_plots(self):
        """Create a rectangular grid of persistent canvases (no recreate on each refresh)."""
        # key -> (fig, canvas, ax)
        self.plots = {}

        # Order/layout of panels
        metrics = [
            ("price_levels", "Price & Levels"),  # Entry, Current, TP, SL (single panel)
            ("adx", "ADX"),
            ("rsi", "RSI"),
            ("coin", "Coin L/S"),
            ("trader", "Trader L/S"),
            ("cnd", "CND"),
            ("funding", "Funding (%)"),
            ("pnl", "PnL"),
            ("cum", "CumPnL"),
        ]

        for c in self.plot_container.winfo_children():
            c.destroy()

        cols = 3
        row = 0
        col = 0
        for key, title in metrics:
            frame = ttk.Frame(self.plot_container)
            frame.grid(row=row, column=col, sticky="nsew", padx=4, pady=4)
            self.plot_container.grid_rowconfigure(row, weight=1)
            self.plot_container.grid_columnconfigure(col, weight=1)

            fig = plt.Figure(figsize=(3.5, 2.2), dpi=100)
            ax = fig.add_subplot(111)
            ax.set_title(title)
            ax.grid(True, alpha=0.3)

            canvas = FigureCanvasTkAgg(fig, master=frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)

            self.plots[key] = (fig, canvas, ax)

            col += 1
            if col >= cols:
                col = 0
                row += 1

    # --- symbol mgmt / actions
    def _reload_symbols(self):
        self.symbol_cb.configure(values=_load_symbols())

    def _snap(self):
        sym = (self.symbol_var.get() or "").strip().upper()
        if not sym:
            messagebox.showinfo("DCA", "Pick a symbol first")
            return
        _append_dca_snapshot(sym)
        self._reload_symbols()
        self.refresh()

    def _toggle_auto(self):
        if self.auto_log.get():
            self._schedule_auto()
        else:
            if self._auto_job:
                try:
                    self.after_cancel(self._auto_job)
                except Exception:
                    pass
                self._auto_job = None

    def _schedule_auto(self):
        if self._destroyed or not self.auto_log.get():
            return
        try:
            sec = max(5, int(self.auto_sec.get() or 30))
        except Exception:
            sec = 30
            self.auto_sec.set(30)

        def tick():
            if self._destroyed or not self.auto_log.get():
                return
            sym = (self.symbol_var.get() or "").strip().upper()
            if sym:
                _append_dca_snapshot(sym)
                self._reload_symbols()
                self.refresh()
            self._schedule_auto()
        self._auto_job = self.after(sec*1000, tick)

    # --- plotting helpers
    def _plot_metric_single(self, key: str, x_vals, y_vals, show: bool, ylabel: str = ""):
        """Plot a single series in a persistent ax."""
        if key not in self.plots:
            return
        fig, canvas, ax = self.plots[key]
        ax.clear()
        ax.grid(True, alpha=0.3)
        base_title = ax.get_title().split(" (hidden)")[0]
        if not show:
            ax.set_title(base_title + " (hidden)")
            canvas.draw_idle()
            return
        ax.set_title(base_title)
        if ylabel:
            ax.set_ylabel(ylabel)

        if x_vals and y_vals:
            x_arr = np.array(x_vals)
            y_arr = np.array([float(v) if v is not None else np.nan for v in y_vals], dtype=float)
            n = min(len(x_arr), len(y_arr))
            if n > 0:
                if n > MAX_POINTS:
                    x_arr = x_arr[-MAX_POINTS:]
                    y_arr = y_arr[-MAX_POINTS:]
                ax.plot(x_arr, y_arr, linewidth=1.1)
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center")
        canvas.draw_idle()

    def _plot_metric_multi(self, key: str, x_vals, series_list: list[tuple[list, str]], show: bool, ylabel: str = ""):
        """Plot multiple labeled series in a single panel (used for Price & Levels)."""
        if key not in self.plots:
            return
        fig, canvas, ax = self.plots[key]
        ax.clear()
        ax.grid(True, alpha=0.3)
        base_title = ax.get_title().split(" (hidden)")[0]
        if not show:
            ax.set_title(base_title + " (hidden)")
            canvas.draw_idle()
            return
        ax.set_title(base_title)
        if ylabel:
            ax.set_ylabel(ylabel)

        plotted = False
        x_arr = np.array(x_vals or [])
        if len(x_arr) > MAX_POINTS:
            x_arr = x_arr[-MAX_POINTS:]

        for y, label in series_list:
            if not y:
                continue
            y_arr = np.array([float(v) if v is not None else np.nan for v in y], dtype=float)
            n = min(len(x_arr), len(y_arr))
            if n == 0:
                continue
            xx = x_arr[:n]
            yy = y_arr[-n:]
            if np.isfinite(yy).any():
                ax.plot(xx, yy, label=label, linewidth=1.15)
                plotted = True
        if plotted:
            ax.legend(loc="best")
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center")
        canvas.draw_idle()

    def _replot_from_cache(self):
        """Replot panels from cached series (no DB access)."""
        if not self._cache:
            return
        S = self._cache

        # Price & Levels panel
        price_lines = []
        if self.show_entry.get():  price_lines.append((S["entry_price_series"], "Entry"))
        if self.show_current.get(): price_lines.append((S["current_price_series"], "Current"))
        if self.show_tp.get():     price_lines.append((S["tp_series"], "TP"))
        if self.show_sl.get():     price_lines.append((S["sl_series"], "SL"))
        self._plot_metric_multi("price_levels", S["t_price"], price_lines, self.show_price_panel.get(), "Price")

        # Individual technicals
        self._plot_metric_single("adx", S["time"], S["adx"], self.show_adx.get())
        self._plot_metric_single("rsi", S["time"], S["rsi"], self.show_rsi.get())
        self._plot_metric_single("coin", S["time"], S["coin_ls_ratio"], self.show_coin.get(), "Ratio")
        self._plot_metric_single("trader", S["time"], S["trader_ls_ratio"], self.show_trader.get(), "Ratio")
        self._plot_metric_single("cnd", S["time"], S["cnd"], self.show_cnd.get())
        self._plot_metric_single("funding", S["time"], S["funding_pct"], self.show_funding.get(), "%")
        self._plot_metric_single("pnl", S["time"], S["pnl"], self.show_pnl.get(), "USDT")
        self._plot_metric_single("cum", S["time"], S["cum_pnl"], self.show_cum.get(), "USDT")

    # --- refresh logic
    def refresh(self):
        if self._destroyed:
            return
        if not _db_exists():
            self.status.set(f"DB not found at {DB_PATH}")
            return
        sym = (self.symbol_var.get() or "").strip().upper()
        if not sym:
            syms = _load_symbols()
            if syms:
                sym = syms[0]
                self.symbol_var.set(sym)
            else:
                self.status.set("No symbols.")
                return
        try:
            days = int(self.days_var.get() or 7)
        except Exception:
            days = 7
            self.days_var.set(7)

        @timed("dca_trending_refresh_work", threshold_ms=100)
        def work():
            rows = _fetch_all_columns_no_date_filter(sym)
            if not rows:
                on_tk(self, self.status.set, "No rows, snapping now...")
                _append_dca_snapshot(sym)
                rows2 = _fetch_all_columns_no_date_filter(sym)
                rows = rows2

            series, total = _series_from_rows(rows, days)
            on_tk(self, self.status.set, f"{sym}: {len(series['time'])}/{total} rows in last {days} day(s)")

            if not series["time"]:
                on_tk(self, self.summary.set, "No data in range.")
                return

            # summary
            points = len(series["time"])
            wins = sum(1 for p in series["pnl"] if (p or 0) > 0)
            win_rate = (wins / points * 100.0) if points else 0.0
            cum = series["cum_pnl"][-1] if series["cum_pnl"] else 0.0
            on_tk(self, self.summary.set, f"{sym} | points={points} | win={win_rate:.1f}% | cumPnL={cum:.4f}")

            # Build cached data for replot
            # Price & Levels: append live price to Current and x-axis
            t_price = list(series["time"])
            current_series = list(series["current"])
            live_price = _get_mark_price(sym)
            if live_price is not None:
                t_price.append(datetime.now(ADEL))
                current_series.append(live_price)
            target_len = len(t_price)
            entry_series = _pad_to_len(series["entry"], target_len)
            tp_series = _pad_to_len(series["tp"], target_len)
            sl_series = _pad_to_len(series["sl"], target_len)
            current_series = _pad_to_len(current_series, target_len)

            # Funding percent conversion
            funding_pct = [(v*100 if v is not None else None) for v in series["funding"]]

            cache = {
                "time": series["time"],
                "t_price": t_price,
                "entry_price_series": entry_series,
                "current_price_series": current_series,
                "tp_series": tp_series,
                "sl_series": sl_series,
                "adx": series["adx"],
                "rsi": series["rsi"],
                "coin_ls_ratio": series["coin_ls_ratio"],
                "trader_ls_ratio": series["trader_ls_ratio"],
                "cnd": series["cnd"],
                "funding_pct": funding_pct,
                "pnl": series["pnl"],
                "cum_pnl": series["cum_pnl"],
            }
            self._cache = cache

            on_tk(self, self._replot_from_cache)

        threading.Thread(target=work, daemon=True).start()

# =============================================================================
# COMMON TRENDING FRAME – now rectangular & persistent plots
# =============================================================================
class CommonTrendingFrame(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self._destroyed = False
        self._all_symbols: list[str] = []
        self.bind("<Destroy>", lambda e: setattr(self, "_destroyed", True))

        # cache of last fetched series
        self._last = None  # dict of numpy / list series

        self._build_ui()
        self._init_plots()

    def _build_ui(self):
        top = ttk.Frame(self); top.pack(fill="x", pady=(6,2), padx=8)

        ttk.Label(top, text="Search:").pack(side="left")
        self.search_var = tk.StringVar()
        ent = ttk.Entry(top, textvariable=self.search_var, width=16)
        ent.pack(side="left", padx=(4,8))
        self.search_var.trace_add("write", lambda *_: self._apply_filter())

        ttk.Label(top, text="Symbol:").pack(side="left")
        self.symbol = tk.StringVar()
        self.symbol_cb = ttk.Combobox(top, textvariable=self.symbol, width=15)
        self.symbol_cb.pack(side="left", padx=(4,8))
        self.symbol_cb.bind("<<ComboboxSelected>>", lambda e: self.fetch_and_plot())

        ttk.Label(top, text="Interval:").pack(side="left")
        self.interval = tk.StringVar(value="15m")
        self.interval_cb = ttk.Combobox(top, textvariable=self.interval, width=7,
                                        values=["1m","3m","5m","15m","30m","1h","4h","1d"])
        self.interval_cb.pack(side="left", padx=(4,8))

        ttk.Label(top, text="Limit:").pack(side="left")
        self.limit = tk.IntVar(value=500)
        ttk.Spinbox(top, from_=100, to=1500, increment=50, textvariable=self.limit, width=6)\
            .pack(side="left", padx=(4,8))

        ttk.Button(top, text="Load Symbols", command=self._load_symbols).pack(side="left")
        ttk.Button(top, text="Fetch & Plot", command=self.fetch_and_plot).pack(side="left", padx=(6,0))

        togg = ttk.Frame(self); togg.pack(fill="x", padx=8, pady=(2,4))
        self.show_price = tk.BooleanVar(value=True)
        self.show_rsi = tk.BooleanVar(value=True)
        self.show_macd = tk.BooleanVar(value=True)
        self.show_adx = tk.BooleanVar(value=True)
        self.show_coin = tk.BooleanVar(value=True)
        self.show_trader = tk.BooleanVar(value=True)
        for text, var in [
            ("Price", self.show_price),
            ("RSI", self.show_rsi),
            ("MACD", self.show_macd),
            ("ADX", self.show_adx),
            ("Coin L/S", self.show_coin),
            ("Trader L/S", self.show_trader),
        ]:
            ttk.Checkbutton(togg, text=text, variable=var,
                            command=self._replot_from_cache).pack(side="left")

        self.status = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.status).pack(anchor="w", padx=10)

        self.plot_container = ttk.Frame(self, relief="groove", borderwidth=2)
        self.plot_container.pack(fill="both", expand=True, padx=8, pady=4)

    def _init_plots(self):
        """Create a fixed grid of subplots (one-time)."""
        self.plots = {}
        metrics = [
            ("price", "Price"),
            ("rsi", "RSI"),
            ("macd", "MACD"),
            ("adx", "ADX"),
            ("coin", "Coin L/S"),
            ("trader", "Trader L/S"),
        ]

        for c in self.plot_container.winfo_children():
            c.destroy()

        cols = 2
        row = 0
        col = 0
        for key, title in metrics:
            frame = ttk.Frame(self.plot_container)
            frame.grid(row=row, column=col, sticky="nsew", padx=4, pady=4)
            self.plot_container.grid_rowconfigure(row, weight=1)
            self.plot_container.grid_columnconfigure(col, weight=1)

            fig = plt.Figure(figsize=(4.5, 2.2), dpi=100)
            ax = fig.add_subplot(111)
            ax.set_title(title)
            ax.grid(True, alpha=0.3)

            canvas = FigureCanvasTkAgg(fig, master=frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)

            self.plots[key] = (fig, canvas, ax)

            col += 1
            if col >= cols:
                col = 0
                row += 1

    def _apply_filter(self):
        q = (self.search_var.get() or "").strip().upper()
        if not self._all_symbols:
            return
        self.symbol_cb.configure(
            values=[s for s in self._all_symbols if q in s] if q else self._all_symbols
        )

    def _load_symbols(self):
        self.status.set("Loading symbols from Binance...")
        @timed("common_trending_load_symbols", threshold_ms=100)
        def work():
            syms = _get_symbols_live()
            if not syms:
                syms = _load_symbols()
            self._all_symbols = syms
            on_tk(self, lambda: self.symbol_cb.configure(values=syms))
            if syms and not self.symbol.get():
                on_tk(self, self.symbol.set, "BTCUSDT")
            on_tk(self, self.status.set, f"Loaded {len(syms)} symbols.")
        threading.Thread(target=work, daemon=True).start()

    # plotting helpers -----------------------------------------------------
    def _plot_metric(self, key, x_vals, y_vals, show, label=None, ylabel=""):
        if key not in self.plots:
            return
        fig, canvas, ax = self.plots[key]
        ax.clear()
        ax.grid(True, alpha=0.3)

        base_title = ax.get_title().split(" (hidden)")[0] or key
        if not show:
            ax.set_title(base_title + " (hidden)")
            canvas.draw_idle()
            return

        ax.set_title(base_title)
        if ylabel:
            ax.set_ylabel(ylabel)

        if x_vals and y_vals:
            x_arr = np.array(x_vals)
            y_arr = np.array([float(v) if v is not None else np.nan for v in y_vals])
            n = min(len(x_arr), len(y_arr))
            if n > 0:
                if n > MAX_POINTS:
                    x_arr = x_arr[-MAX_POINTS:]
                    y_arr = y_arr[-MAX_POINTS:]
                    n = len(x_arr)
                ax.plot(x_arr[:n], y_arr[:n], linewidth=1.15,
                        label=label or base_title)
                if label:
                    ax.legend(loc="best")
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                    ha="center", va="center")

        fig.autofmt_xdate()
        canvas.draw_idle()

    def _plot_macd(self, x_vals, macd, sig, hist, show):
        if "macd" not in self.plots:
            return
        fig, canvas, ax = self.plots["macd"]
        ax.clear()
        ax.grid(True, alpha=0.3)

        title = "MACD"
        if not show:
            ax.set_title(title + " (hidden)")
            canvas.draw_idle()
            return

        ax.set_title(title)
        ax.set_ylabel("Value")

        if x_vals and macd:
            x_arr = np.array(x_vals)
            macd_arr = np.array([float(v) if v is not None else np.nan for v in macd])
            sig_arr = np.array([float(v) if v is not None else np.nan for v in sig])
            hist_arr = np.array([float(v) if v is not None else np.nan for v in hist])

            n = min(len(x_arr), len(macd_arr), len(sig_arr), len(hist_arr))
            if n > 0:
                if n > MAX_POINTS:
                    x_arr = x_arr[-MAX_POINTS:]
                    macd_arr = macd_arr[-MAX_POINTS:]
                    sig_arr = sig_arr[-MAX_POINTS:]
                    hist_arr = hist_arr[-MAX_POINTS:]
                    n = len(x_arr)
                xx = x_arr[:n]
                ax.plot(xx, macd_arr[:n], label="MACD")
                ax.plot(xx, sig_arr[:n], label="Signal")
                ax.plot(xx, hist_arr[:n], label="Hist")
                ax.legend(loc="best")
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                    ha="center", va="center")

        fig.autofmt_xdate()
        canvas.draw_idle()

    def _replot_from_cache(self):
        if not self._last:
            return
        L = self._last
        t = L["time"]
        self._plot_metric("price", t, L["close"], self.show_price.get(), ylabel="Price")
        self._plot_metric("rsi", t, L["rsi"], self.show_rsi.get(), label="RSI(14)", ylabel="RSI")
        self._plot_macd(t, L["macd"], L["macd_sig"], L["macd_hist"], self.show_macd.get())
        self._plot_metric("adx", t, L["adx"], self.show_adx.get(), label="ADX(30)", ylabel="ADX")

        # For L/S series, use their own time axis if present, otherwise price time
        self._plot_metric(
            "coin",
            L["t_coin"] if L["t_coin"] else t,
            L["coin_ls"] if L["coin_ls"] else [],
            self.show_coin.get(),
            label="Coin L/S", ylabel="Ratio"
        )
        self._plot_metric(
            "trader",
            L["t_trader"] if L["t_trader"] else t,
            L["trader_ls"] if L["trader_ls"] else [],
            self.show_trader.get(),
            label="Trader L/S", ylabel="Ratio"
        )

    # fetching --------------------------------------------------------------
    def fetch_and_plot(self):
        if self._destroyed:
            return
        sym = (self.symbol.get() or "").strip().upper()
        if not sym:
            self.status.set("Pick a symbol.")
            return
        interval = self.interval.get()
        limit = int(self.limit.get() or 500)
        if limit < 120:
            limit = 120
        self.status.set(f"Fetching {sym} {interval} x {limit}...")

        @timed("common_trending_fetch_plot", threshold_ms=200)
        def work():
            try:
                kl = _get_klines(sym, interval, limit)
                t, o, h, l, c, v = _to_series_from_klines(kl)
                rsi = _rsi(c, 14)
                macd, macd_sig, macd_hist = _macd(c)
                adx = _adx(h, l, c, 30)

                period_map = {
                    "1m": "5m", "3m": "5m", "5m": "5m",
                    "15m": "15m", "30m": "30m",
                    "1h": "1h", "4h": "4h", "1d": "1d"
                }
                p = period_map.get(interval, "15m")
                t_coin, v_coin = _get_global_ls_series(sym, period=p, limit=limit)
                t_trader, v_trader = _get_toptrader_ls_series(sym, period=p, limit=limit)

                self._last = {
                    "time": t,
                    "close": c,
                    "rsi": rsi,
                    "macd": macd,
                    "macd_sig": macd_sig,
                    "macd_hist": macd_hist,
                    "adx": adx,
                    "t_coin": t_coin,
                    "coin_ls": v_coin,
                    "t_trader": t_trader,
                    "trader_ls": v_trader,
                }
                on_tk(self, self._replot_from_cache)
                on_tk(self, self.status.set, f"Fetched {len(c)} bars.")
            except Exception as e:
                on_tk(self, self.status.set, f"Fetch failed: {e}")

        threading.Thread(target=work, daemon=True).start()

# =============================================================================
# ALL TRENDING FRAME – rectangular version of Common (UPDATED)
# =============================================================================
class AllTrendingFrame(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self._destroyed = False
        self._all_symbols: list[str] = []
        self.auto_refresh = tk.BooleanVar(value=False)
        self.auto_sec = tk.StringVar(value="5")

        # metric toggles
        self.show_price = tk.BooleanVar(value=True)
        self.show_rsi = tk.BooleanVar(value=True)
        self.show_macd = tk.BooleanVar(value=True)
        self.show_adx = tk.BooleanVar(value=True)
        self.show_coin = tk.BooleanVar(value=True)
        self.show_trader = tk.BooleanVar(value=True)
        self.show_funding = tk.BooleanVar(value=True)

        self.last_df: Optional[pd.DataFrame] = None

        self._build_ui()
        self._init_plots()
        self.after(1000, self._auto_tick)

    def _build_ui(self):
        top = ttk.Frame(self); top.pack(fill="x", padx=8, pady=5)

        ttk.Label(top, text="Search:").pack(side="left")
        self.search_var = tk.StringVar()
        ent = ttk.Entry(top, textvariable=self.search_var, width=16)
        ent.pack(side="left", padx=(4,8))
        self.search_var.trace_add("write", lambda *_: self._apply_filter())

        ttk.Label(top, text="Symbol:").pack(side="left")
        self.symbol = tk.StringVar()
        self.symbol_cb = ttk.Combobox(top, textvariable=self.symbol, width=14, values=_load_symbols())
        self.symbol_cb.pack(side="left", padx=(4,8))
        self.symbol_cb.bind("<<ComboboxSelected>>", lambda e: self.fetch_and_plot())

        ttk.Label(top, text="Interval:").pack(side="left")
        self.interval = tk.StringVar(value="15m")
        self.interval_cb = ttk.Combobox(top, textvariable=self.interval, width=7,
                                        values=["1m","3m","5m","15m","30m","1h","4h","1d"])
        self.interval_cb.pack(side="left", padx=(4,8))

        ttk.Label(top, text="Limit:").pack(side="left")
        self.limit = tk.IntVar(value=500)
        ttk.Spinbox(top, from_=100, to=1500, increment=50, textvariable=self.limit, width=6)\
            .pack(side="left", padx=(4,8))

        ttk.Button(top, text="Load Symbols", command=self._load_symbols).pack(side="left")
        ttk.Button(top, text="Fetch & Plot", command=self.fetch_and_plot).pack(side="left", padx=(6,0))

        # right side
        ttk.Button(top, text="Export Excel", command=self.export_excel).pack(side="right", padx=(4,0))
        ttk.Checkbutton(top, text="Auto refresh", variable=self.auto_refresh).pack(side="right", padx=(4,0))
        ttk.Label(top, text="sec").pack(side="right")
        ttk.Entry(top, width=4, textvariable=self.auto_sec).pack(side="right", padx=(0,4))

        # metric row
        tog = ttk.Frame(self); tog.pack(fill="x", padx=8, pady=(0,5))
        for text, var in [
            ("Price", self.show_price),
            ("RSI", self.show_rsi),
            ("MACD", self.show_macd),
            ("ADX", self.show_adx),
            ("Coin L/S", self.show_coin),
            ("Trader L/S", self.show_trader),
            ("Funding Rate", self.show_funding),
        ]:
            ttk.Checkbutton(tog, text=text, variable=var, command=self._replot_from_cache).pack(side="left")

        self.plot_container = ttk.Frame(self, relief="groove", borderwidth=2)
        self.plot_container.pack(fill="both", expand=True, padx=8, pady=5)

        self.status = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.status).pack(anchor="w", padx=8)

    def _init_plots(self):
        self.plots = {}
        metrics = [
            ("price", "Price"),
            ("rsi", "RSI"),
            ("macd", "MACD"),
            ("adx", "ADX"),
            ("coin", "Coin L/S"),
            ("trader", "Trader L/S"),
            ("funding", "Funding Rate"),
        ]
        for c in self.plot_container.winfo_children():
            c.destroy()

        cols = 3
        row = 0
        col = 0
        for key, title in metrics:
            frame = ttk.Frame(self.plot_container)
            frame.grid(row=row, column=col, sticky="nsew", padx=3, pady=3)
            self.plot_container.grid_rowconfigure(row, weight=1)
            self.plot_container.grid_columnconfigure(col, weight=1)

            fig = plt.Figure(figsize=(3, 2), dpi=100)
            ax = fig.add_subplot(111)
            ax.set_title(title)
            ax.grid(True, alpha=0.3)

            canvas = FigureCanvasTkAgg(fig, master=frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True)

            self.plots[key] = (fig, canvas, ax)

            col += 1
            if col >= cols:
                col = 0
                row += 1

    # --- symbol mgmt
    def _load_symbols(self):
        self.status.set("Loading symbols...")
        @timed("all_trending_load_symbols", threshold_ms=100)
        def work():
            syms = _get_symbols_live()
            if not syms:
                syms = _load_symbols()
            self._all_symbols = syms
            on_tk(self, lambda: self.symbol_cb.configure(values=syms))
            if syms and not self.symbol.get():
                on_tk(self, self.symbol.set, "BTCUSDT")
            on_tk(self, self.status.set, f"Loaded {len(syms)} symbols.")
        threading.Thread(target=work, daemon=True).start()

    def _apply_filter(self):
        q = (self.search_var.get() or "").strip().upper()
        if not self._all_symbols:
            return
        self.symbol_cb.configure(values=[s for s in self._all_symbols if q in s] if q else self._all_symbols)

    # --- fetch & plot
    def fetch_and_plot(self):
        sym = (self.symbol.get() or "").strip().upper()
        if not sym:
            self.status.set("Pick a symbol.")
            return
        interval = self.interval.get()
        limit = int(self.limit.get() or 500)
        if limit < 120: limit = 120
        self.status.set(f"Fetching {sym} {interval} x {limit}...")
        @timed("all_trending_fetch_plot", threshold_ms=200)
        def work():
            try:
                kl = _get_klines(sym, interval, limit)
                t,o,h,l,c,v = _to_series_from_klines(kl)
                rsi = _rsi(c, 14)
                macd, macd_sig, macd_hist = _macd(c)
                adx = _adx(h, l, c, 30)

                period_map = {"1m":"5m","3m":"5m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
                p = period_map.get(interval, "15m")
                t_coin, v_coin = _get_global_ls_series(sym, period=p, limit=limit)
                t_trader, v_trader = _get_toptrader_ls_series(sym, period=p, limit=limit)

                fr = _get_funding_rate(sym)  # decimal, e.g., -0.003481 == -0.3481%
                funding_series_pct = [(fr*100.0)] * len(t) if fr is not None else []

                # cache for export (keep both decimal & percent)
                df = pd.DataFrame({
                    "time": t,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "rsi": rsi,
                    "macd": macd,
                    "macd_signal": macd_sig,
                    "macd_hist": macd_hist,
                    "adx": adx,
                    "funding_rate_decimal": ([fr]*len(t)) if fr is not None else None,
                    "funding_rate_pct": funding_series_pct if funding_series_pct else None,
                })
                df["coin_ls"] = None
                if t_coin:
                    m = {tt.replace(second=0, microsecond=0): val for tt, val in zip(t_coin, v_coin)}
                    df["coin_ls"] = [m.get(tt.replace(second=0, microsecond=0)) for tt in t]
                df["trader_ls"] = None
                if t_trader:
                    m2 = {tt.replace(second=0, microsecond=0): val for tt, val in zip(t_trader, v_trader)}
                    df["trader_ls"] = [m2.get(tt.replace(second=0, microsecond=0)) for tt in t]
                self.last_df = df

                on_tk(self, lambda: self._plot_all(
                    t, c, rsi, macd, macd_sig, macd_hist, adx,
                    t_coin, v_coin, t_trader, v_trader,
                    funding_series_pct
                ))
                on_tk(self, self.status.set,
                      f"Fetched {len(c)} bars. Funding: {fr*100:.4f}%"
                      if fr is not None else f"Fetched {len(c)} bars. Funding: —")
            except Exception as e:
                on_tk(self, self.status.set, f"Fetch failed: {e}")
        threading.Thread(target=work, daemon=True).start()

    def _plot_all(self, t, close, rsi, macd, macd_sig, macd_hist, adx,
                  t_coin, v_coin, t_trader, v_trader, funding_series_pct):
        self._plot_metric("price", t, close, self.show_price.get())
        self._plot_metric("rsi", t, rsi, self.show_rsi.get())
        self._plot_macd(t, macd, macd_sig, macd_hist, self.show_macd.get())
        self._plot_metric("adx", t, adx, self.show_adx.get())
        self._plot_metric("coin", t_coin if t_coin else t, v_coin if v_coin else [], self.show_coin.get())
        self._plot_metric("trader", t_trader if t_trader else t, v_trader if v_trader else [], self.show_trader.get())
        self._plot_metric("funding", t, funding_series_pct, self.show_funding.get())

    def _plot_macd(self, x_vals, macd, sig, hist, show: bool):
        """Specialised MACD panel: MACD / Signal / Hist with legend."""
        if "macd" not in self.plots:
            return
        fig, canvas, ax = self.plots["macd"]
        ax.clear()
        ax.grid(True, alpha=0.3)
        title = "MACD"
        if not show:
            ax.set_title(title + " (hidden)")
            canvas.draw_idle()
            return

        ax.set_title(title)
        ax.set_ylabel("Value")

        if x_vals and macd:
            x_arr = np.array(x_vals)
            macd_arr = np.array([float(v) if v is not None else np.nan for v in macd])
            sig_arr = np.array([float(v) if v is not None else np.nan for v in sig])
            hist_arr = np.array([float(v) if v is not None else np.nan for v in hist])
            n = min(len(x_arr), len(macd_arr), len(sig_arr), len(hist_arr))
            if n > 0:
                if n > MAX_POINTS:
                    x_arr = x_arr[-MAX_POINTS:]
                    macd_arr = macd_arr[-MAX_POINTS:]
                    sig_arr = sig_arr[-MAX_POINTS:]
                    hist_arr = hist_arr[-MAX_POINTS:]
                    n = len(x_arr)
                xx = x_arr[:n]
                ax.plot(xx, macd_arr[:n], label="MACD")
                ax.plot(xx, sig_arr[:n], label="Signal")
                ax.plot(xx, hist_arr[:n], label="Hist")
                ax.legend(loc="best")
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                    ha="center", va="center")

        fig.autofmt_xdate()
        canvas.draw_idle()

    def _plot_metric(self, key, x_vals, y_vals, show):
        """
        Generic plotter for All Trending panels.

        SPECIAL CASE: for key == "rsi" we also draw a parabolic regression
        curve as a dashed red line over the RSI values.
        """
        if key not in self.plots:
            return
        fig, canvas, ax = self.plots[key]
        ax.clear()
        ax.grid(True, alpha=0.3)

        base_title = ax.get_title().split(" (hidden)")[0] or key

        if not show:
            ax.set_title(base_title + " (hidden)")
            canvas.draw_idle()
            return

        ylabel_map = {
            "price": "Price",
            "rsi": "RSI",
            "macd": "Value",
            "adx": "ADX",
            "coin": "Ratio",
            "trader": "Ratio",
            "funding": "%",
        }
        ax.set_ylabel(ylabel_map.get(key, ""))
        ax.set_title(base_title)

        if x_vals and y_vals:
            x_arr = np.array(x_vals)
            y_clean = [float(v) if v is not None else np.nan for v in y_vals]
            y_arr = np.array(y_clean, dtype=float)
            n = min(len(x_arr), len(y_arr))
            if n > 0:
                if n > MAX_POINTS:
                    x_arr = x_arr[-MAX_POINTS:]
                    y_arr = y_arr[-MAX_POINTS:]
                    n = len(x_arr)
                ax.plot(x_arr[:n], y_arr[:n], linewidth=1.1,
                        label="RSI" if key == "rsi" else base_title)

                # --- RSI parabolic curve (only in All Trending tabs) ----
                if key == "rsi":
                    trend = _parabolic_trend_from_array(y_arr[:n])
                    if trend is not None and np.isfinite(trend).any():
                        ax.plot(
                            x_arr[:n],
                            trend,
                            linestyle="--",
                            color="red",
                            linewidth=1.0,
                            label="RSI Parabolic"
                        )
                ax.legend(loc="best")
        else:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center")

        fig.autofmt_xdate()
        canvas.draw_idle()

    def _replot_from_cache(self):
        if self.last_df is None:
            return
        df = self.last_df
        t = df["time"].tolist()
        self._plot_metric("price", t, df["close"].tolist(), self.show_price.get())
        self._plot_metric("rsi", t, df["rsi"].tolist(), self.show_rsi.get())
        self._plot_macd(
            t,
            df["macd"].tolist(),
            df["macd_signal"].tolist(),
            df["macd_hist"].tolist(),
            self.show_macd.get()
        )
        self._plot_metric("adx", t, df["adx"].tolist(), self.show_adx.get())
        self._plot_metric("coin", t, df["coin_ls"].tolist(), self.show_coin.get())
        self._plot_metric("trader", t, df["trader_ls"].tolist(), self.show_trader.get())
        # replot using PERCENT series
        self._plot_metric(
            "funding", t,
            df["funding_rate_pct"].tolist() if "funding_rate_pct" in df else [],
            self.show_funding.get()
        )

    def _auto_tick(self):
        if self._destroyed:
            return
        if self.auto_refresh.get():
            try:
                sec = int(self.auto_sec.get() or "5")
            except Exception:
                sec = 5
                self.auto_sec.set("5")
            self.fetch_and_plot()
            self.after(max(1, sec)*1000, self._auto_tick)
        else:
            self.after(1000, self._auto_tick)

    def export_excel(self):
        if self.last_df is None or self.last_df.empty:
            messagebox.showwarning("Export", "No data to export. Fetch first.")
            return
        fpath = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            title="Save All Trending data as Excel",
        )
        if not fpath:
            return
        try:
            self.last_df.to_excel(fpath, index=False)
            messagebox.showinfo("Export", f"Exported to:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Export error", str(e))

# =============================================================================
# MAIN TAB
# =============================================================================
class TrendingTab(ttk.Frame):
    def __init__(self, parent, dca_tab: Optional[tk.Widget] = None, trading_tab: Optional[tk.Widget] = None):
        super().__init__(parent)
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)

        # DCA Trending
        self.dca_frame = DcaTrendingFrame(nb, dca_tab=dca_tab, trading_tab=trading_tab)
        nb.add(self.dca_frame, text="DCA Trending")

        # Common Trending
        self.common_frame = CommonTrendingFrame(nb)
        nb.add(self.common_frame, text="Common Trending")

        # --- FIVE independent All Trending tabs (so you can trend 5 different coins) ---
        self.all_frames = []
        for i in range(1, 6):
            frame = AllTrendingFrame(nb)
            nb.add(frame, text=f"All Trending {i}")
            self.all_frames.append(frame)


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Trending Tab Test")
    root.geometry("1400x900")
    tab = TrendingTab(root)
    tab.pack(fill="both", expand=True)
    root.mainloop()
