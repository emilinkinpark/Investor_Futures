# dca_tab.py
# DCA page with:
# - Left:  Scanner Controls + DCA Settings + TP/SL Settings
# - Right: Active Positions Treeview + actions
# - Bottom: Scanner Logs
#
# NEW (2025-10-11):
#   • First-DCA amount (USDT) in UI; first fill uses this exact notional.
#   • DCA flow strictly follows your 4 steps:
#       Step 1: Place DCA using UI First-DCA (then geometric growth by multiplier).
#       Step 2: Cancel existing TP/SL.
#       Step 3: Re-fetch recent entry (retry loop so avgEntry updates after fill).
#       Step 4: Recalc & place new TP/SL from UI.
#   • _apply_tpsl_for_position now cancels first, then fetches new entry, then places.
#   • Clear "why no DCA" logs: no SL found, max attempts reached, debounce, min qty/notional.
#   • Keeps auto-missing-brackets feature & indicators/funding caches.
#
# Behavior:
#   • After each DCA fill we ALWAYS run Steps 2–4 (regardless of the Auto TP/SL toggle).
#     (That toggle still affects the "fill missing only" behavior during scans.)
#
# Settings persist to ui_dca_settings.json (same folder).

import os
import json
import math
import time
import hmac
import hashlib
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import tkinter as tk
from zoneinfo import ZoneInfo
from tkinter import ttk, messagebox

from config import API_KEY, API_SECRET, BASE_URL
from utils.threading import run_io_bound  # shared IO-bound worker pool


# --- Thread-safe Tk dispatcher ---
def on_tk(widget, fn, *a, **k):
    try:
        if widget and widget.winfo_exists():
            widget.after(0, lambda: fn(*a, **k))
    except Exception:
        pass


SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "ui_dca_settings.json")
STOP_TYPES = {"STOP", "STOP_MARKET", "TRAILING_STOP_MARKET"}
TP_TYPES = {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}

# Indicators config
ADX_LEN = 30
RSI_LEN = 30
INDICATOR_INTERVAL = "30m"   # change to "1m", "15m", etc. if you prefer


# --- DCA scanner file logging + per-symbol CSV ---
from pathlib import Path as _Path
import csv


def _dca_log_write(line: str, symbol: str | None = None):
    try:
        base_dir = _Path(__file__).resolve().parents[1]  # .../binance_futures_trader
        log_root = base_dir / "logs" / "dca_scanner"
        log_root.mkdir(parents=True, exist_ok=True)
        daily = log_root / (datetime.now(ZoneInfo("Australia/Adelaide")).strftime("%Y-%m-%d") + ".log")
        with daily.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        if symbol is None:
            import re as _re
            m = _re.search(r"\b([A-Z]{3,15}(?:USDT|BUSD|USDC|FDUSD|TUSD|BTC|BNB))\b", line)
            if m:
                symbol = m.group(1)
        if symbol:
            bys = log_root / "by_symbol" / symbol
            bys.mkdir(parents=True, exist_ok=True)
            per = bys / (datetime.now(ZoneInfo("Australia/Adelaide")).strftime("%Y-%m-%d") + ".log")
            with per.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass


def _symbol_csv_log(symbol: str, row: dict):
    try:
        base_dir = _Path(__file__).resolve().parents[1]
        bys = base_dir / "logs" / "dca_scanner" / "by_symbol" / symbol
        bys.mkdir(parents=True, exist_ok=True)
        fpath = bys / (datetime.now(ZoneInfo("Australia/Adelaide")).strftime("%Y-%m-%d") + ".csv")
        header = [
            "log_time",
            "symbol",
            "side",
            "size",
            "entry",
            "current",
            "pnl",
            "leverage",
            "margin",
            "tp",
            "sl",
            "adx",
            "rsi",
            "coin_ls_ratio",
            "trader_ls_ratio",
            "cnd",
            "funding",
        ]
        new_file = not fpath.exists()
        with fpath.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            if new_file:
                w.writeheader()
            w.writerow({k: row.get(k, "") for k in header})
    except Exception:
        pass


# --- SQLite storage for trending ---
import sqlite3 as _sqlite


def _sqlite_db_path():
    base_dir = _Path(__file__).resolve().parents[1]  # .../binance_futures_trader
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "dca_positions.db")


def _sqlite_init_table():
    try:
        conn = _sqlite.connect(_sqlite_db_path())
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS dca_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        );
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_time ON dca_positions(symbol, log_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time ON dca_positions(log_time);")
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _sqlite_insert_row(row: dict):
    try:
        conn = _sqlite.connect(_sqlite_db_path())
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dca_positions
            (log_time,symbol,side,size,entry,current,pnl,leverage,margin,tp,sl,adx,rsi,coin_ls_ratio,trader_ls_ratio,cnd,funding)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
            (
                row.get("log_time"),
                row.get("symbol"),
                row.get("side"),
                row.get("size"),
                row.get("entry"),
                row.get("current"),
                row.get("pnl"),
                row.get("leverage"),
                row.get("margin"),
                row.get("tp"),
                row.get("sl"),
                row.get("adx"),
                row.get("rsi"),
                row.get("coin_ls_ratio"),
                row.get("trader_ls_ratio"),
                row.get("cnd"),
                row.get("funding"),
            ),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


class DCATab(ttk.Frame):
    def __init__(self, parent, trading_tab=None, trending_tab=None):
        super().__init__(parent)
        try:
            _sqlite_init_table()
        except Exception:
            pass
        self.trading_tab = trading_tab
        self.trending_tab = trending_tab  # handle to TrendingTab

        # ---------- runtime state ----------
        self._scanner_active = False
        self._scanner_thread: Optional[threading.Thread] = None
        self._exchange_cache: Dict[str, dict] = {}
        self._state: Dict[str, Dict[str, float]] = {}
        self._ui_refresh_job = None

        # cached snapshot of last known positions (shared between scanner and UI)
        self._last_positions: List[dict] = []
        self._positions_lock = threading.Lock()

        # caches
        self._orders_cache: Dict[str, Dict[str, object]] = {}    # {symbol: {"orders":[...], "ts":epoch}}
        self._funding_cache: Dict[str, Dict[str, object]] = {}   # {symbol: {"rate":float, "ts":epoch}}
        self._ind_cache: Dict[str, Dict[str, object]] = {}       # {(symbol,interval): {"adx":float,"rsi":float,"ts":epoch}}

        # ---------- UI variables ----------
        # DCA
        self.enable_dca = tk.BooleanVar(value=True)
        self.max_attempts = tk.IntVar(value=2)
        self.trigger_pct = tk.DoubleVar(value=2.0)
        self.multiplier = tk.DoubleVar(value=1.5)
        self.scan_interval = tk.IntVar(value=60)
        # NEW: first DCA notional in USDT
        self.first_dca_usdt = tk.DoubleVar(value=5.0)

        # Debounce/cooldown seconds to prevent double-fires within same second
        self.debounce_sec = tk.IntVar(value=6)

        # In-memory guards (not persisted): per-symbol lock and cooldown
        self._dca_locks = {}
        self._dca_cooldown = {}
        # TP/SL
        self.auto_tpsl = tk.BooleanVar(value=True)
        self.long_tp = tk.DoubleVar(value=20.0)
        self.long_sl = tk.DoubleVar(value=12.0)
        self.short_tp = tk.DoubleVar(value=20.0)
        self.short_sl = tk.DoubleVar(value=12.0)

        self.status_var = tk.StringVar(value="🟠 DCA: Idle")
        self.last_scan_var = tk.StringVar(value="Last scan: —")
        self.pnl7_var = tk.StringVar(value="7-Day PNL: Loading…")
        self.pnltoday_var = tk.StringVar(value="Today's PNL: Loading…")

        # PERF: TreeView index (key -> item id) to avoid full rebuild each refresh
        # key format: f"{symbol}:{side}" so LONG/SHORT rows are independent
        self._tree_index: Dict[str, str] = {}

        # NEW: track previous position qty to detect open/close transitions for timer
        self._prev_pos_qty: Dict[str, float] = {}

        self._build_ui()
        self._load_settings()
        self._ensure_entry_time_table()
        self._start_ui_refresher()
        self._start_timer_updater()

    # ---------- allow late wiring of TrendingTab ----------
    def set_trending_tab(self, trending_tab):
        self.trending_tab = trending_tab

    # ====================== UI LAYOUT ======================
    def _build_ui(self):
        root = ttk.Frame(self)
        root.pack(fill="both", expand=True)

        left = ttk.Frame(root)
        right = ttk.Frame(root)
        left.grid(row=0, column=0, sticky="nsw", padx=8, pady=8)
        right.grid(row=0, column=1, sticky="nsew", padx=(0, 8), pady=8)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(0, weight=1)

        # ---- Scanner Controls ----
        lf_scanner = ttk.Labelframe(left, text="Scanner Controls")
        lf_scanner.pack(fill="x")

        btns = ttk.Frame(lf_scanner)
        btns.pack(fill="x", padx=8, pady=8)
        ttk.Button(btns, text="Start Scanner", command=self._start_scanner).pack(side="left")
        ttk.Button(btns, text="Stop Scanner", command=self._stop_scanner).pack(side="left", padx=6)
        ttk.Button(btns, text="Save Settings", command=self._save_settings).pack(side="left")

        row2 = ttk.Frame(lf_scanner)
        row2.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Label(row2, text="Scan Interval (sec)").pack(side="left")
        ttk.Spinbox(row2, from_=1, to=600, increment=1, width=6, textvariable=self.scan_interval)\
           .pack(side="left", padx=6)
        ttk.Label(row2, textvariable=self.last_scan_var).pack(side="right")

        # ---- DCA Settings ----
        lf_dca = ttk.Labelframe(left, text="DCA Settings")
        lf_dca.pack(fill="x", pady=10)

        row = ttk.Frame(lf_dca); row.pack(fill="x", padx=8, pady=(8, 4))
        ttk.Checkbutton(row, text="Enable DCA", variable=self.enable_dca).pack(side="left")

        row = ttk.Frame(lf_dca); row.pack(fill="x", padx=8, pady=4)
        ttk.Label(row, text="Max DCA Attempts:").pack(side="left")
        ttk.Spinbox(row, from_=1, to=10, width=6, textvariable=self.max_attempts).pack(side="left", padx=6)

        row = ttk.Frame(lf_dca); row.pack(fill="x", padx=8, pady=4)
        ttk.Label(row, text="DCA Trigger (% around SL):").pack(side="left")
        ttk.Spinbox(row, from_=0.1, to=50.0, increment=0.1, width=6, textvariable=self.trigger_pct)\
           .pack(side="left", padx=6)
        ttk.Label(row, text="(LONG triggers <= SL×(1+%), SHORT >= SL×(1-%))").pack(side="left")

        row = ttk.Frame(lf_dca); row.pack(fill="x", padx=8, pady=4)
        ttk.Label(row, text="First DCA Margin (USDT):").pack(side="left")
        ttk.Spinbox(row, from_=1.0, to=10000.0, increment=1.0, width=8, textvariable=self.first_dca_usdt)\
           .pack(side="left", padx=6)

        row = ttk.Frame(lf_dca); row.pack(fill="x", padx=8, pady=(4, 8))
        ttk.Label(row, text="DCA Investment Multiplier:").pack(side="left")
        ttk.Spinbox(row, from_=1.0, to=5.0, increment=0.1, width=6, textvariable=self.multiplier)\
           .pack(side="left", padx=6)
        ttk.Label(row, text="(geometric growth per DCA)").pack(side="left")

        # ---- TP/SL Settings ----
        lf_tpsl = ttk.Labelframe(left, text="TP/SL Settings")
        lf_tpsl.pack(fill="x", pady=(0, 10))

        row = ttk.Frame(lf_tpsl); row.pack(fill="x", padx=8, pady=(8, 4))
        ttk.Checkbutton(row, text="Auto apply TP/SL when missing", variable=self.auto_tpsl).pack(side="left")

        row = ttk.Frame(lf_tpsl); row.pack(fill="x", padx=8, pady=4)
        ttk.Label(row, text="LONG  Take Profit (%):").pack(side="left")
        ttk.Spinbox(row, from_=0.1, to=300.0, increment=0.1, width=7, textvariable=self.long_tp).pack(side="left", padx=6)
        ttk.Label(row, text="Stop Loss (%):").pack(side="left", padx=(10, 0))
        ttk.Spinbox(row, from_=0.1, to=100.0, increment=0.1, width=7, textvariable=self.long_sl).pack(side="left", padx=6)

        row = ttk.Frame(lf_tpsl); row.pack(fill="x", padx=8, pady=(4, 8))
        ttk.Label(row, text="SHORT Take Profit (%):").pack(side="left")
        ttk.Spinbox(row, from_=0.1, to=300.0, increment=0.1, width=7, textvariable=self.short_tp).pack(side="left", padx=6)
        ttk.Label(row, text="Stop Loss (%):").pack(side="left", padx=(10, 0))
        ttk.Spinbox(row, from_=0.1, to=100.0, increment=0.1, width=7, textvariable=self.short_sl).pack(side="left", padx=6)

        # ---- Right: Active Positions ----
        rf_top = ttk.Labelframe(right, text="Active Positions")
        rf_top.pack(fill="both", expand=True)

        cols = ("symbol","side","size","entry","current","pnl","leverage","margin","tp","sl",
                "adx","rsi","funding","status","timer")
        self.tree = ttk.Treeview(rf_top, columns=cols, show="headings", height=12)
        self._tree_cols = cols
        self._col_index = {name: i for i, name in enumerate(cols)}
        for c, w in [
            ("symbol", 90), ("side", 60), ("size", 80), ("entry", 90), ("current", 90), ("pnl", 110),
            ("leverage", 70), ("margin", 100), ("tp", 90), ("sl", 90),
            ("adx", 60), ("rsi", 60), ("funding", 70), ("status", 90), ("timer", 90)
        ]:
            self.tree.heading(c, text=c.upper() if c in ("tp", "sl") else c.capitalize())
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)

        br = ttk.Frame(rf_top); br.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Button(br, text="Close Position", command=self._action_close_position).pack(side="left")
        ttk.Button(br, text="View Trend", command=self._action_view_trend).pack(side="left", padx=6)
        ttk.Button(br, text="Force TP/SL", command=self._action_force_tpsl).pack(side="left", padx=(0, 6))
        ttk.Button(br, text="Refresh Positions", command=self._refresh_positions).pack(side="right")

        status_line = ttk.Frame(rf_top); status_line.pack(fill="x", padx=6, pady=(0, 2))
        ttk.Label(status_line, textvariable=self.pnl7_var).pack(side="left")
        ttk.Label(status_line, textvariable=self.pnltoday_var).pack(side="right")

        # ---- Logs ----
        rf_bottom = ttk.Labelframe(right, text="Scanner Logs")
        rf_bottom.pack(fill="both", expand=True, pady=(8, 0))

        self.log = tk.Text(rf_bottom, height=12, state="disabled")
        yscroll = ttk.Scrollbar(rf_bottom, orient="vertical", command=self.log.yview)
        self.log.configure(yscrollcommand=yscroll.set)
        self.log.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        yscroll.pack(side="right", fill="y", padx=(0, 6), pady=6)

        sr = ttk.Frame(self); sr.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Label(sr, textvariable=self.status_var).pack(side="left")


    # ====================== Position entry timer (local tracking) ======================
    def _ensure_entry_time_table(self):
        try:
            conn = _sqlite.connect(_sqlite_db_path())
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS position_entry_time (
                    symbol TEXT PRIMARY KEY,
                    entry_ts INTEGER NOT NULL
                )
                """
            )
            conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _get_entry_ts(self, symbol: str):
        try:
            conn = _sqlite.connect(_sqlite_db_path())
            cur = conn.cursor()
            cur.execute("SELECT entry_ts FROM position_entry_time WHERE symbol=?", (symbol,))
            row = cur.fetchone()
            return int(row[0]) if row else None
        except Exception:
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _set_entry_ts(self, symbol: str, entry_ts: int):
        try:
            conn = _sqlite.connect(_sqlite_db_path())
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO position_entry_time(symbol, entry_ts)
                VALUES(?, ?)
                ON CONFLICT(symbol) DO UPDATE SET entry_ts=excluded.entry_ts
                """,
                (symbol, int(entry_ts)),
            )
            conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _clear_entry_ts(self, symbol: str):
        try:
            conn = _sqlite.connect(_sqlite_db_path())
            cur = conn.cursor()
            cur.execute("DELETE FROM position_entry_time WHERE symbol=?", (symbol,))
            conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fmt_age(self, entry_ts: int) -> str:
        if not entry_ts:
            return "-"
        age = max(0, int(time.time()) - int(entry_ts))
        hh = age // 3600
        mm = (age % 3600) // 60
        ss = age % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    def _start_timer_updater(self):
        if getattr(self, "_timer_job", None):
            return

        def _tick():
            try:
                self._update_timer_cells()
            finally:
                self._timer_job = self.after(1000, _tick)

        _tick()

    def _update_timer_cells(self):
        # Update only the TIMER column to avoid heavy redraws.
        if not hasattr(self, "tree"):
            return
        idx_timer = (getattr(self, "_col_index", {}) or {}).get("timer", None)
        idx_symbol = (getattr(self, "_col_index", {}) or {}).get("symbol", None)
        if idx_timer is None or idx_symbol is None:
            return

        for iid in self.tree.get_children():
            vals = list(self.tree.item(iid, "values") or [])
            if len(vals) <= max(idx_timer, idx_symbol):
                continue
            symbol = vals[idx_symbol]
            ts = self._get_entry_ts(symbol)
            vals[idx_timer] = self._fmt_age(ts)
            try:
                self.tree.item(iid, values=vals)
            except Exception:
                pass

    # ====================== UI helpers ======================
    def _log(self, msg: str):
        def _append():
            ts = datetime.now(ZoneInfo("Australia/Adelaide")).strftime("%Y-%m-%d %H:%M:%S%z")
            try:
                self.log.configure(state="normal")
                self.log.insert("end", f"[{ts}] {msg}\n")
                self.log.see("end")
                self.log.configure(state="disabled")
            except Exception:
                pass
            if self.trading_tab and hasattr(self.trading_tab, "log"):
                try:
                    self.trading_tab.log(msg)
                except Exception:
                    pass

        self.after(0, _append)

    def _save_settings(self):
        data = {
            "enable_dca": self.enable_dca.get(),
            "max_attempts": int(self.max_attempts.get()),
            "trigger_pct": float(self.trigger_pct.get()),
            "multiplier": float(self.multiplier.get()),
            "scan_interval": int(self.scan_interval.get()),
            "first_dca_usdt": float(self.first_dca_usdt.get()),
            "auto_tpsl": self.auto_tpsl.get(),
            "long_tp": float(self.long_tp.get()),
            "long_sl": float(self.long_sl.get()),
            "short_tp": float(self.short_tp.get()),
            "short_sl": float(self.short_sl.get()),
        }
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            self._log("Saved DCA & TP/SL settings.")
        except Exception as e:
            messagebox.showerror("Save Settings", f"Failed to save: {e}")

    def _load_settings(self):
        if not os.path.exists(SETTINGS_FILE):
            return
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.enable_dca.set(bool(data.get("enable_dca", True)))
            self.max_attempts.set(int(data.get("max_attempts", 2)))
            self.trigger_pct.set(float(data.get("trigger_pct", 2.0)))
            self.multiplier.set(float(data.get("multiplier", 1.5)))
            self.scan_interval.set(int(data.get("scan_interval", 60)))
            self.first_dca_usdt.set(float(data.get("first_dca_usdt", 5.0)))

            self.auto_tpsl.set(bool(data.get("auto_tpsl", True)))
            self.long_tp.set(float(data.get("long_tp", 20.0)))
            self.long_sl.set(float(data.get("long_sl", 12.0)))
            self.short_tp.set(float(data.get("short_tp", 20.0)))
            self.short_sl.set(float(data.get("short_sl", 12.0)))
        except Exception as e:
            self._log(f"Load settings failed: {e}")

    # ====================== Positions table ======================
    def _refresh_positions(self):
        """
        Refresh the Active Positions grid synchronously on the Tk thread.

        • Prefer positions cached by the scanner (self._last_positions).
        • If scanner isn't running yet, fall back to a fresh _safe_get_positions()
          so the DCA tab still shows live positions.
        """
        # Choose source
        if self._scanner_active and self._last_positions:
            with self._positions_lock:
                rows = list(self._last_positions)
        else:
            rows = self._safe_get_positions()
            if rows:
                with self._positions_lock:
                    self._last_positions = list(rows)

        display_rows = []

        # Track open/close transitions to set/clear entry timestamp for the Timer column
        now_ts = int(time.time())
        current_syms = set()
        for p in rows:
            try:
                sym0 = p.get("symbol", "")
                if sym0:
                    current_syms.add(sym0)
                    qty0 = float(p.get("size") or 0.0)
                    prev0 = float(self._prev_pos_qty.get(sym0, 0.0))
                    if prev0 == 0.0 and qty0 != 0.0:
                        self._set_entry_ts(sym0, now_ts)
                    if prev0 != 0.0 and qty0 == 0.0:
                        self._clear_entry_ts(sym0)
                    self._prev_pos_qty[sym0] = qty0
            except Exception:
                pass

        # Symbols that were previously open but no longer in the list -> treat as closed
        for sym0, prev0 in list(self._prev_pos_qty.items()):
            if sym0 not in current_syms and float(prev0 or 0.0) != 0.0:
                self._clear_entry_ts(sym0)
                self._prev_pos_qty[sym0] = 0.0

        for p in rows:
            sym = p.get("symbol", "")
            side = p.get("side", "")
            if not sym or not side:
                continue

            entry = float(p.get("entry", 0.0) or 0.0)
            mark = float(p.get("mark", 0.0) or 0.0)

            try:
                lev_val = float(p.get("leverage") or 0.0)
            except Exception:
                lev_val = p.get("leverage", 0.0)

            try:
                size_val = abs(float(p.get("size") or 0.0))
            except Exception:
                size_val = 0.0

            margin = (size_val * entry / lev_val) if lev_val and entry else 0.0

            # Use same caches as scanner
            orders = self._get_all_tpsl_orders(sym)
            tp_price, sl_price = self._get_tpsl_prices(sym, side, orders)
            adx_val, rsi_val = self._get_indicators(sym, INDICATOR_INTERVAL)
            fund_rate = self._get_funding_rate(sym)

            status = (
                "Bracket"
                if tp_price and sl_price
                else ("No SL" if tp_price else ("No TP" if sl_price else "No TP/SL"))
            )

            vals = (
                sym,
                side,
                f"{size_val:g}",
                f"{entry:.6f}",
                f"{mark:.6f}",
                f"{float(p.get('pnl', 0.0) or 0.0): .4f}".strip(),
                lev_val,
                f"{margin:.2f}",
                (f"{tp_price:.6f}" if tp_price else "-"),
                (f"{sl_price:.6f}" if sl_price else "-"),
                (f"{adx_val:.1f}" if adx_val is not None else "-"),
                (f"{rsi_val:.1f}" if rsi_val is not None else "-"),
                (f"{fund_rate * 100:.4f}%" if fund_rate is not None else "-"),
                status,
                self._fmt_age(self._get_entry_ts(sym)),
            )
            display_rows.append(vals)

        # Simple redraw
        self.tree.delete(*self.tree.get_children())
        self._tree_index.clear()
        for vals in display_rows:
            key = f"{vals[0]}:{vals[1]}"
            iid = self.tree.insert("", "end", values=vals)
            self._tree_index[key] = iid

    def _start_ui_refresher(self):
        """Periodically refresh positions and (less often) PnL labels."""
        # last time we refreshed PnL labels (epoch seconds)
        self._pnl_last_update_ts = getattr(self, "_pnl_last_update_ts", 0)

        def _tick():
            try:
                # Refresh open positions more frequently
                self._refresh_positions()

                # Refresh 7-day / today's PnL at most once per minute
                now = time.time()
                if now - getattr(self, "_pnl_last_update_ts", 0) > 60:
                    self._pnl_last_update_ts = now
                    self._update_pnl_labels()
            finally:
                # UI refresh every 5 seconds (independent of scanner loop)
                self._ui_refresh_job = self.after(5000, _tick)

        _tick()

    def _update_pnl_labels(self):
        """Fetch realised PnL from Binance and update the PnL labels."""

        def worker():
            try:
                summary = self._fetch_pnl_summary()
            except Exception:
                summary = None

            if not summary:
                return

            pnl_7d, pnl_today = summary

            def apply():
                try:
                    self.pnl7_var.set(f"7-Day PNL: {pnl_7d:.2f} USDT")
                    self.pnltoday_var.set(f"Today's PNL: {pnl_today:.2f} USDT")
                except Exception:
                    pass

            on_tk(self, apply)

        run_io_bound(worker)

    def _fetch_pnl_summary(self):
        """Return (pnl_last_7d, pnl_today) using /fapi/v1/income."""
        try:
            tz = ZoneInfo("Australia/Adelaide")
        except Exception:
            tz = None

        now = datetime.now(tz) if tz else datetime.utcnow()
        start_7d = int((now - timedelta(days=7)).timestamp() * 1000)

        try:
            rows = self._signed("GET", "/fapi/v1/income", {
                "incomeType": "REALIZED_PNL",
                "startTime": start_7d,
                "limit": 1000,
            })
        except Exception:
            return None

        pnl_7d = 0.0
        pnl_today = 0.0

        for row in rows or []:
            try:
                inc = float(row.get("income") or 0.0)
                ts_ms = int(row.get("time") or 0)
                if ts_ms <= 0:
                    continue
                if tz:
                    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=tz)
                else:
                    dt = datetime.utcfromtimestamp(ts_ms / 1000.0)

                # inside last 7 days?
                if (now - dt).days < 7:
                    pnl_7d += inc
                # today's PnL
                if dt.date() == now.date():
                    pnl_today += inc
            except Exception:
                continue

        return pnl_7d, pnl_today

    def destroy(self):
        try:
            if self._ui_refresh_job:
                self.after_cancel(self._ui_refresh_job)
        except Exception:
            pass
        self._stop_scanner()
        super().destroy()

    # ====================== Buttons under positions ======================
    def _selected_symbol(self) -> Optional[str]:
        sel = self.tree.selection()
        if not sel:
            return None
        vals = self.tree.item(sel[0]).get("values", [])
        return vals[0] if vals else None

    def _find_position(self, symbol: str) -> Optional[dict]:
        for p in self._safe_get_positions():
            if p.get("symbol") == symbol:
                return p
        return None

    def _action_close_position(self):
        sym = self._selected_symbol()
        if not sym:
            messagebox.showinfo("Close Position", "Select a position first.")
            return
        pos = self._find_position(sym)
        if not pos:
            self._log(f"[Close] No live position for {sym}.")
            return
        side = pos.get("side")
        qty = abs(float(pos.get("size") or 0))
        if side not in ("LONG", "SHORT") or qty <= 0:
            self._log(f"[Close] Invalid selection for {sym}.")
            return
        ok = self._close_position_market(sym, side, qty)
        if ok:
            self._log(f"[Close] Submitted MARKET reduceOnly for {sym}.")
        else:
            self._log(f"[Close] Failed for {sym}.")

    def _action_view_trend(self):
        sym = self._selected_symbol()
        if not sym:
            messagebox.showinfo("View Trend", "Select a position first.")
            return
        pos = self._find_position(sym) or {}
        if self.trending_tab:
            try:
                self.trending_tab.open_for_position(sym, pos)
                nb = self.nametowidget(self.winfo_parent())
                try:
                    idx = list(nb.tabs()).index(str(self.trending_tab))
                    nb.select(idx)
                except Exception:
                    nb.select(self.trending_tab)
                self._log(f"[Trend] Opened {sym} in Trending tab.")
            except Exception as e:
                self._log(f"[Trend] Failed: {e}")
        else:
            self._log("[Trend] TrendingTab not available (wire it via set_trending_tab).")

    def _action_force_tpsl(self):
        sym = self._selected_symbol()
        if not sym:
            messagebox.showinfo("Force TP/SL", "Select a position first.")
            return
        pos = self._find_position(sym)
        if not pos:
            self._log("[TP/SL] Could not load position info.")
            return
        self._apply_tpsl_for_position(pos)

    # ====================== Scanner lifecycle ======================
    def _start_scanner(self):
        if self._scanner_active:
            return
        self._scanner_active = True
        self.status_var.set("🟢 DCA: Active")
        self._scanner_thread = threading.Thread(target=self._loop, daemon=True)
        self._scanner_thread.start()
        self._log("DCA scanner started.")

    def _stop_scanner(self):
        if not self._scanner_active:
            return
        self._scanner_active = False
        self.status_var.set("🔴 DCA: Stopped")
        self._log("DCA scanner stopped.")

    def _loop(self):
        while self._scanner_active:
            try:
                self._scan_cycle()
                # keep UI positions strongly in sync with scanner
                on_tk(self, self._refresh_positions)
                on_tk(self, lambda: self.last_scan_var.set(f"Last scan: {datetime.now().strftime('%H:%M:%S')}"))
                # respect user Scan Interval (sec) exactly, min 1s
                interval = max(1, int(self.scan_interval.get()))
                time.sleep(interval)
            except Exception as e:
                self._log(f"[DCA] scanner error: {e}")
                time.sleep(5)

    def _scan_cycle(self):
        if not self.enable_dca.get():
            self._log("[DCA] disabled; skipping scan.")
            return

        # reset orders cache once per cycle
        self._orders_cache = {}

        positions = self._safe_get_positions()
        if not positions:
            self._log("[DCA] No active positions found.")
            # clear cached snapshot so UI reflects there are no positions
            with self._positions_lock:
                self._last_positions = []
            return

        # update cached snapshot so UI uses the same positions as scanner
        with self._positions_lock:
            self._last_positions = list(positions)

        # log + DB snapshot
        try:
            tz = ZoneInfo("Australia/Adelaide")
        except Exception:
            tz = None
        now = datetime.now(tz) if tz else datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S%z")

        if not hasattr(self, "_last_symbol_log_ts"):
            self._last_symbol_log_ts = {}

        for p in positions:
            try:
                symbol = p.get("symbol")
                side = p.get("side")
                if not symbol or side not in ("LONG", "SHORT"):
                    continue

                orders = self._get_all_tpsl_orders(symbol)
                tp_price, sl_price = self._get_tpsl_prices(symbol, side, orders)
                adx_val, rsi_val = self._get_indicators(symbol, INDICATOR_INTERVAL)
                fund_rate = self._get_funding_rate(symbol)

                try:
                    entry = float(p.get("entry") or 0.0)
                except Exception:
                    entry = 0.0
                try:
                    mark = float(p.get("mark") or 0.0)
                except Exception:
                    mark = 0.0
                try:
                    lev_val = float(p.get("leverage") or 0.0)
                except Exception:
                    lev_val = 0.0
                try:
                    size_val = abs(float(p.get("size") or 0.0))
                except Exception:
                    size_val = 0.0
                margin = (size_val * entry / lev_val) if lev_val and entry else 0.0

                row_struct = {
                    "log_time": now_str,
                    "symbol": symbol,
                    "side": side,
                    "size": size_val,
                    "entry": entry,
                    "current": mark,
                    "pnl": float(p.get("pnl", 0) or 0),
                    "leverage": int(lev_val) if isinstance(lev_val, (int, float)) else None,
                    "margin": margin,
                    "tp": float(tp_price) if tp_price else None,
                    "sl": float(sl_price) if sl_price else None,
                    "adx": float(adx_val) if adx_val is not None else None,
                    "rsi": float(rsi_val) if rsi_val is not None else None,
                    "coin_ls_ratio": None,
                    "trader_ls_ratio": None,
                    "cnd": None,
                    "funding": float(fund_rate) if fund_rate is not None else None,
                }

                import time as _t

                last_ts = self._last_symbol_log_ts.get(symbol, 0)
                now_ts = _t.time()
                if now_ts - last_ts >= 60:
                    try:
                        _symbol_csv_log(symbol, row_struct)
                    except Exception:
                        pass
                    try:
                        _sqlite_insert_row(row_struct)
                    except Exception:
                        pass
                    self._last_symbol_log_ts[symbol] = now_ts
            except Exception:
                continue

        for p in positions:
            try:
                symbol = p.get("symbol")
                side = p.get("side")
                if not symbol or side not in ("LONG", "SHORT"):
                    continue

                # 1) Ensure TP/SL exist; if missing, apply the missing ones based on UI
                orders = self._get_all_tpsl_orders(symbol)
                has_tp, has_sl = self._detect_tpsl_presence(orders, side)
                if self.auto_tpsl.get() and not (has_tp and has_sl):
                    self._apply_missing_tpsl_for_position(p, has_tp, has_sl)
                    orders = self._get_all_tpsl_orders(symbol, force_refresh=True)

                # 2) Run standard DCA evaluation (reuse orders)
                self._evaluate_position(p, orders=orders)

            except Exception as e:
                self._log(f"[DCA] eval {p.get('symbol')}: {e}")

    # ====================== Position evaluation & orders ======================
    def _safe_get_positions(self) -> List[dict]:
        """
        Return current futures positions.

        IMPORTANT: To keep DCA and the Trading tab perfectly in sync,
        we *first* try to read from TradingTab's cached positions, and
        only fall back to direct Binance /positionRisk if TradingTab
        is not wired or raises an error.
        """
        # ---- 1) Preferred source: TradingTab cache ----
        try:
            if self.trading_tab:
                # Most builds expose get_positions_cached(); if not, fall back to get_positions()
                getter = None
                if hasattr(self.trading_tab, "get_positions_cached"):
                    getter = self.trading_tab.get_positions_cached
                elif hasattr(self.trading_tab, "get_positions"):
                    getter = self.trading_tab.get_positions
                if getter is not None:
                    res = getter() or []
                    norm: List[dict] = []
                    for r in res:
                        try:
                            symbol = r.get("symbol") or r.get("Symbol")
                            if not symbol:
                                continue

                            # size / qty
                            raw_size = (
                                r.get("size")
                                or r.get("qty")
                                or r.get("positionAmt")
                                or r.get("position_amt")
                            )
                            # some versions wrap the size in a dict, handle that
                            if isinstance(raw_size, dict):
                                raw_size = (
                                    raw_size.get("value")
                                    or raw_size.get("positionAmt")
                                    or raw_size.get("qty")
                                    or raw_size.get("size")
                                )
                            size = float(raw_size or 0.0)
                            if abs(size) <= 0.0:
                                continue

                            # prices
                            entry = float(
                                r.get("entry")
                                or r.get("entryPrice")
                                or r.get("avgEntryPrice")
                                or 0.0
                            )
                            mark = float(
                                r.get("mark")
                                or r.get("markPrice")
                                or r.get("lastPrice")
                                or 0.0
                            )
                            pnl = float(
                                r.get("pnl")
                                or r.get("unRealizedProfit")
                                or 0.0
                            )

                            lev_raw = (
                                r.get("leverage")
                                or r.get("lev")
                                or r.get("isolatedLeverage")
                            )
                            try:
                                leverage = float(lev_raw) if lev_raw is not None else 0.0
                            except Exception:
                                leverage = 0.0

                            # side / positionSide
                            side = (r.get("side") or r.get("positionSide") or "").upper()
                            if not side:
                                side = "LONG" if size > 0 else "SHORT"

                            norm.append(
                                {
                                    "symbol": symbol,
                                    "side": side,
                                    "size": size,
                                    "entry": entry,
                                    "mark": mark,
                                    "pnl": pnl,
                                    "leverage": leverage,
                                }
                            )
                        except Exception:
                            # If one row is bad, skip it but keep others
                            continue

                    if norm:
                        return norm
        except Exception as e:
            self._log(f"[DCA] TradingTab positions error, falling back to Binance: {e}")

        # ---- 2) Fallback: Binance /fapi/v2/positionRisk (ground truth) ----
        try:
            api_positions = self._fetch_positions_via_api()
            if api_positions:
                return api_positions
        except Exception as e:
            self._log(f"[DCA] positionRisk error: {e}")

        return []

    def _fetch_positions_via_api(self) -> List[dict]:
        """
        Direct Binance fallback for live positions using /fapi/v2/positionRisk.
        """
        try:
            data = self._signed("GET", "/fapi/v2/positionRisk", {})
        except Exception as e:
            self._log(f"[DCA] direct positionRisk failed: {e}")
            return []

        results: List[dict] = []
        dual = self._is_dual_side()

        for pos in data or []:
            try:
                amt = float(pos.get("positionAmt") or 0.0)
            except Exception:
                continue
            if abs(amt) <= 0.0:
                continue

            symbol = pos.get("symbol")
            if not symbol:
                continue

            try:
                entry = float(pos.get("entryPrice") or 0.0)
                mark = float(pos.get("markPrice") or 0.0)
                pnl = float(pos.get("unRealizedProfit") or 0.0)
                lev = float(pos.get("leverage") or 0.0)
            except Exception:
                continue

            if dual:
                side_raw = (pos.get("positionSide") or "BOTH").upper()
                if side_raw == "BOTH":
                    side = "LONG" if amt > 0 else "SHORT"
                else:
                    side = side_raw
            else:
                side = "LONG" if amt > 0 else "SHORT"

            results.append({
                "symbol": symbol,
                "side": side,
                "size": abs(amt),
                "entry": entry,
                "mark": mark,
                "pnl": pnl,
                "leverage": lev,
            })

        return results

    def _evaluate_position(self, p: dict, orders: Optional[List[dict]] = None):
        symbol = p["symbol"]
        side = p["side"]              # "LONG" / "SHORT"
        entry = float(p["entry"])
        mark = float(p["mark"]) or self._fetch_mark_price(symbol)
        size = abs(float(p["size"]))

        if not symbol or size <= 0 or entry <= 0 or mark <= 0 or side not in ("LONG","SHORT"):
            return

        st = self._state.setdefault(symbol, {
            "dca_count": 0,
            # base_margin = First DCA (USDT) from UI
            "base_margin": max(0.0, float(self.first_dca_usdt.get())),  # margin (USDT) from UI; notional = margin * leverage
            "last_dca_price": None,
        })

        # Max attempts guard
        if st["dca_count"] >= max(1, int(self.max_attempts.get())):
            self._log(f"[DCA] {symbol}: max attempts reached ({st['dca_count']}/{self.max_attempts.get()}).")
            return

        # Find SL (reuse orders if provided)
        sl_price = self._find_stop_loss(symbol, side, orders=orders)
        if not sl_price or sl_price <= 0:
            self._log(f"[DCA] {symbol}: No SL found, skipping DCA trigger.")
            return

        pct = max(0.0001, float(self.trigger_pct.get())) / 100.0
        if side == "LONG":
            trigger = sl_price * (1.0 + pct)
            should = mark <= trigger
        else:  # SHORT
            trigger = sl_price * (1.0 - pct)
            should = mark >= trigger

        self._log(
            f"[DCA] {symbol} {side}: mark={mark:.6f}, SL={sl_price:.6f}, "
            f"trigger={trigger:.6f} → should_dca={should}"
        )
        if not should:
            return

        # Debounce (avoid rapid-fire if price hasn't cleared last DCA zone)
        last = st["last_dca_price"]
        if last is not None:
            if side == "LONG" and mark > last * 0.995:
                self._log(f"[DCA] {symbol}: debounce — LONG mark {mark:.6f} > {last*0.995:.6f}.")
                return
            if side == "SHORT" and mark < last * 1.005:
                self._log(f"[DCA] {symbol}: debounce — SHORT mark {mark:.6f} < {last*1.005:.6f}.")
                return

        # Margin → Notional using current leverage, then grow geometrically
        mult = max(1.0, float(self.multiplier.get()))
        try:
            cur_lev = float(p.get("leverage") or 0)  # live leverage from position
        except Exception:
            cur_lev = 0.0
        if cur_lev <= 0:
            # safety fallback if leverage not reported; treat as 1x
            cur_lev = 1.0
        base_margin = st.get("base_margin", max(0.0, float(self.first_dca_usdt.get())))
        target_notional = base_margin * cur_lev * (mult ** st["dca_count"])  # <-- use leverage
        qty = target_notional / max(mark, 1e-12)
        self._log(
            f"[DCA] {symbol}: using margin={base_margin:.4f} × lev={cur_lev:.2f} → "
            f"target_notional={target_notional:.4f} → qty≈{qty:.6f}"
        )

        # Filters
        qty = self._format_quantity(symbol, qty)
        min_notional = self._get_min_notional(symbol)
        min_qty = self._get_min_qty(symbol)
        step = self._get_step(symbol)
        if qty < min_qty:
            qty = self._ceil_to_step(min_qty, step)
        if mark * qty < min_notional:
            need = max(min_notional / mark, min_qty)
            qty = self._ceil_to_step(need, step)
        if qty <= 0:
            self._log(f"[DCA] {symbol}: qty too small after filters (min_notional={min_notional}, min_qty={min_qty}).")
            return

        side_http = "BUY" if side == "LONG" else "SELL"
        # --- Per-symbol lock & cooldown to avoid double DCA within the same tick ---
        now = time.time()
        # Cooldown window
        cd_until = self._dca_cooldown.get(symbol, 0)
        if now < cd_until:
            self._log(f"[DCA] {symbol}: cooldown active ({cd_until - now:.1f}s remaining), skipping.")
            return
        # Acquire or create lock
        lock = self._dca_locks.setdefault(symbol, threading.Lock())
        if not lock.acquire(blocking=False):
            self._log(f"[DCA] {symbol}: another DCA in progress, skipping.")
            return
        try:
            # set cooldown immediately
            self._dca_cooldown[symbol] = now + max(1, int(self.debounce_sec.get()))
            client_id = f"DCA-{symbol}-{int(now)}-{st['dca_count'] + 1}"
            if self._place_market_order(symbol, side_http, qty, client_id):
                st["dca_count"] += 1
                st["last_dca_price"] = mark
                self._log(f"[DCA] {symbol}: DCA #{st['dca_count']} placed, qty={qty}")
                # === Enforce your Steps 2–4 unconditionally after a DCA fill ===
                try:
                    self._post_dca_reset_brackets(symbol, side)
                except Exception as e:
                    self._log(f"[DCA] post-DCA TP/SL failed: {e}")
        finally:
            try:
                lock.release()
            except Exception:
                pass

    # --------- Post-DCA bracket reset (Steps 2–4) ----------
    def _post_dca_reset_brackets(self, symbol: str, side: str):
        # Step 2: Cancel existing TP/SL
        self._cancel_existing_brackets(symbol, [*STOP_TYPES, *TP_TYPES])

        # Step 3: Re-fetch recent entry with short retry to ensure avg entry updated
        entry = self._fetch_recent_entry_with_retry(symbol)
        if entry is None or entry <= 0:
            # Fallback to TradingTab position entry
            pos = self._find_position(symbol) or {}
            entry = float(pos.get("entry") or 0.0)
        if entry is None or entry <= 0:
            self._log(f"[TP/SL] {symbol}: invalid entry after DCA; skipping TP/SL.")
            return

        # Step 4: Compute new TP/SL and place them
        tp_price, sl_price, tp_side, sl_side = self._compute_brackets(symbol, side, entry)
        ok_tp = self._place_tpsl_order(symbol, tp_side, "TAKE_PROFIT_MARKET", tp_price, position_side=side)
        ok_sl = self._place_tpsl_order(symbol, sl_side, "STOP_MARKET", sl_price, position_side=side)
        if ok_tp and ok_sl:
            self._log(f"[TP/SL] {symbol}: TP={tp_price} SL={sl_price} set (entry={entry}).")
        else:
            self._log(f"[TP/SL] {symbol}: one or more orders failed (TP ok={ok_tp}, SL ok={ok_sl}).")

    def _compute_brackets(self, symbol: str, side: str, entry: float) -> Tuple[float, float, str, str]:
        if side == "LONG":
            tp_pct = max(0.0001, float(self.long_tp.get())) / 100.0
            sl_pct = max(0.0001, float(self.long_sl.get())) / 100.0
            tp_price = self._format_price(symbol, entry * (1.0 + tp_pct))
            sl_price = self._format_price(symbol, entry * (1.0 - sl_pct))
            return tp_price, sl_price, "SELL", "SELL"
        else:  # SHORT
            tp_pct = max(0.0001, float(self.short_tp.get())) / 100.0
            sl_pct = max(0.0001, float(self.short_sl.get())) / 100.0
            tp_price = self._format_price(symbol, entry * (1.0 - tp_pct))
            sl_price = self._format_price(symbol, entry * (1.0 + sl_pct))
            return tp_price, sl_price, "BUY", "BUY"

    # --------- TP/SL presence detection, TP/SL values, indicators, funding ----------
    def _detect_tpsl_presence(self, orders: List[dict], side: str) -> Tuple[bool, bool]:
        want_side = "SELL" if side == "LONG" else "BUY"
        has_tp, has_sl = False, False
        for o in orders or []:
            if o.get("side") != want_side:
                continue
            t = o.get("type")
            if t in TP_TYPES:
                has_tp = True
            elif t in STOP_TYPES:
                has_sl = True
            if has_tp and has_sl:
                break
        return has_tp, has_sl

    def _get_tpsl_prices(self, symbol: str, side: str, orders: Optional[List[dict]]) -> Tuple[Optional[float], Optional[float]]:
        want_side = "SELL" if side == "LONG" else "BUY"
        tp_price = None
        sl_price = None
        for o in (orders or []):
            if o.get("side") != want_side:
                continue
            t = o.get("type")
            px = float(o.get("stopPrice") or o.get("price") or 0.0)
            if px <= 0:
                continue
            if t in TP_TYPES:
                tp_price = px
            elif t in STOP_TYPES:
                sl_price = px
        return tp_price, sl_price

    def _get_recent_entry_price(self, symbol: str) -> Optional[float]:
        """Fetch the latest entryPrice for the active position from /fapi/v2/positionRisk."""
        try:
            data = self._signed("GET", "/fapi/v2/positionRisk", {"timestamp": 0})
            for pos in data or []:
                if pos.get("symbol") == symbol and float(pos.get("positionAmt") or 0) != 0.0:
                    ep = float(pos.get("entryPrice") or 0.0)
                    return ep if ep > 0 else None
        except Exception:
            pass
        return None

    def _fetch_recent_entry_with_retry(self, symbol: str, attempts: int = 6, delay: float = 0.25) -> Optional[float]:
        """Retry helper: after a fill, Binance may take a beat to refresh avg entry."""
        ep = None
        for _ in range(max(1, attempts)):
            ep = self._get_recent_entry_price(symbol)
            if ep and ep > 0:
                break
            time.sleep(max(0.05, delay))
        return ep

    def _apply_missing_tpsl_for_position(self, pos: dict, has_tp: bool, has_sl: bool):
        """
        Apply only the missing bracket(s) using current UI settings and the most recent entry price.
        Does NOT cancel existing TP/SL.
        """
        symbol = pos["symbol"]
        side = pos["side"]

        recent_entry = self._get_recent_entry_price(symbol)
        entry = float(recent_entry if (recent_entry and recent_entry > 0) else pos.get("entry", 0.0))
        if entry <= 0:
            self._log(f"[TP/SL] {symbol}: invalid entry; cannot apply missing TP/SL.")
            return

        tp_price, sl_price, tp_side, sl_side = self._compute_brackets(symbol, side, entry)
        placed_any = False
        if not has_tp:
            ok_tp = self._place_tpsl_order(symbol, tp_side, "TAKE_PROFIT_MARKET", tp_price, position_side=side)
            placed_any = placed_any or ok_tp
            self._log(f"[TP/SL] {symbol}: TP {'set' if ok_tp else 'failed'} at {tp_price} (entry={entry})")
        if not has_sl:
            ok_sl = self._place_tpsl_order(symbol, sl_side, "STOP_MARKET", sl_price, position_side=side)
            placed_any = placed_any or ok_sl
            self._log(f"[TP/SL] {symbol}: SL {'set' if ok_sl else 'failed'} at {sl_price} (entry={entry})")

        if not placed_any:
            self._log(f"[TP/SL] {symbol}: nothing applied (both present or placement failed).")

    def _apply_tpsl_for_position(self, pos: dict):
        """Full re-apply: cancel existing TP/SL, then set new ones from UI using the most recent entry price."""
        symbol = pos["symbol"]
        side = pos["side"]

        # Cancel first (so we don't duplicate or misread stale orders)
        self._cancel_existing_brackets(symbol, [*STOP_TYPES, *TP_TYPES])

        # Fetch updated entry (with retry) AFTER cancel to reflect fresh state
        entry = self._fetch_recent_entry_with_retry(symbol)
        if entry is None or entry <= 0:
            entry = float(pos.get("entry", 0.0))
        if entry is None or entry <= 0:
            self._log(f"[TP/SL] {symbol}: invalid entry.")
            return

        tp_price, sl_price, tp_side, sl_side = self._compute_brackets(symbol, side, entry)
        ok_tp = self._place_tpsl_order(symbol, tp_side, "TAKE_PROFIT_MARKET", tp_price, position_side=side)
        ok_sl = self._place_tpsl_order(symbol, sl_side, "STOP_MARKET", sl_price, position_side=side)
        if ok_tp and ok_sl:
            self._log(f"[TP/SL] {symbol}: TP={tp_price} SL={sl_price} set (entry={entry}).")
        else:
            self._log(f"[TP/SL] {symbol}: one or more orders failed (TP ok={ok_tp}, SL ok={ok_sl}).")

    # ====================== Binance helpers & indicators ======================
    def _normalize_params(self, params: dict) -> dict:
        """Normalize params for Binance: avoid scientific notation and drop None."""
        out = {}
        for k, v in (params or {}).items():
            if v is None:
                continue
            if isinstance(v, bool):
                out[k] = "true" if v else "false"
            elif isinstance(v, float):
                s = f"{v:.16f}".rstrip("0").rstrip(".")
                out[k] = s if s else "0"
            elif isinstance(v, int):
                out[k] = v
            else:
                out[k] = str(v)
        return out

    def _signed(self, method: str, path: str, params: dict):
        ts = int(time.time() * 1000)

        params = self._normalize_params(dict(params or {}))
        params["timestamp"] = ts

        items = sorted(params.items(), key=lambda kv: kv[0])
        q = "&".join([f"{k}={v}" for k, v in items])
        sig = hmac.new(API_SECRET.encode("utf-8"), q.encode("utf-8"), hashlib.sha256).hexdigest()

        headers = {"X-MBX-APIKEY": API_KEY}
        url = f"{BASE_URL}{path}"

        send_params = dict(params)
        send_params["signature"] = sig

        if method == "GET":
            resp = requests.get(url, params=send_params, headers=headers, timeout=10)
        elif method == "DELETE":
            resp = requests.delete(url, params=send_params, headers=headers, timeout=10)
        else:
            resp = requests.post(url, params=send_params, headers=headers, timeout=10)

        resp.raise_for_status()
        return resp.json()


    def _public_get(self, path: str, params: dict = None):
        return requests.get(f"{BASE_URL}{path}", params=params or {}, timeout=10)

    def _is_dual_side(self) -> bool:
        try:
            data = self._signed("GET", "/fapi/v1/positionSide/dual", {})
            return bool(data.get("dualSidePosition"))
        except Exception:
            return False

    def _close_position_market(self, symbol: str, side: str, qty: float) -> bool:
        qty = self._format_quantity(symbol, float(qty))
        step = self._get_step(symbol)
        min_qty = self._get_min_qty(symbol)
        if qty < min_qty:
            qty = self._ceil_to_step(min_qty, step)
        if qty <= 0:
            self._log(f"[Close] {symbol}: qty too small after rounding.")
            return False
        http_side = "SELL" if side == "LONG" else "BUY"
        params = {
            "symbol": symbol,
            "side": http_side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": "true",
        }
        if self._is_dual_side():
            params["positionSide"] = side
        try:
            self._signed("POST", "/fapi/v1/order", params)
            return True
        except requests.exceptions.RequestException as e:
            try:
                j = e.response.json() if e.response is not None else {}
                self._log(f"[Close] {symbol} error {j.get('code')}: {j.get('msg')}")
            except Exception:
                self._log(f"[Close] {symbol} error: {e}")
            return False

    def _get_symbol_info(self, symbol: str) -> dict:
        if symbol in self._exchange_cache:
            return self._exchange_cache[symbol]
        data = self._public_get("/fapi/v1/exchangeInfo", {"symbol": symbol}).json()
        info = next(item for item in data.get("symbols", []) if item.get("symbol") == symbol)
        self._exchange_cache[symbol] = info
        return info

    def _filters(self, symbol: str) -> dict:
        info = self._get_symbol_info(symbol)
        return {fil.get("filterType"): fil for fil in info.get("filters", [])}

    def _get_step(self, symbol: str) -> float:
        return float(self._filters(symbol).get("LOT_SIZE", {}).get("stepSize", 0.001))

    def _get_min_qty(self, symbol: str) -> float:
        return float(self._filters(symbol).get("LOT_SIZE", {}).get("minQty", 0.0))

    def _get_tick(self, symbol: str) -> float:
        return float(self._filters(symbol).get("PRICE_FILTER", {}).get("tickSize", 0.0001))

    def _get_min_notional(self, symbol: str) -> float:
        f = self._filters(symbol)
        if "MIN_NOTIONAL" in f and "notional" in f["MIN_NOTIONAL"]:
            return float(f["MIN_NOTIONAL"]["notional"])
        return 5.0

    def _ceil_to_step(self, value: float, step: float) -> float:
        if step <= 0:
            return float(value)
        return math.ceil(value / step) * step

    def _format_quantity(self, symbol: str, qty: float) -> float:
        step = self._get_step(symbol)
        if step <= 0:
            return float(qty)
        steps = math.floor(float(qty) / step)
        return round(steps * step, 12)

    def _format_price(self, symbol: str, price: float) -> float:
        tick = self._get_tick(symbol)
        if tick <= 0:
            return float(price)
        ticks = math.floor(float(price) / tick)
        return round(ticks * tick, 12)

    def _fetch_mark_price(self, symbol: str) -> float:
        try:
            r = self._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})
            if r.ok:
                return float(r.json().get("markPrice", 0.0))
        except Exception:
            pass
        return 0.0

    # ---- Open orders cache helpers ----
    def _get_open_orders(self, symbol: str, force_refresh: bool = False) -> List[dict]:
        try:
            cached = self._orders_cache.get(symbol)
            if cached and not force_refresh and (time.time() - cached.get("ts", 0)) < 3:
                return cached.get("orders", [])
            orders = self._signed("GET", "/fapi/v1/openOrders", {"symbol": symbol})
            self._orders_cache[symbol] = {"orders": orders, "ts": time.time()}
            return orders
        except Exception:
            return []

    def _get_all_tpsl_orders(self, symbol: str, force_refresh: bool = False) -> List[dict]:
        """
        Return a unified list of TP/SL orders for the symbol, combining:
          • classic openOrders (type in STOP/TAKE_PROFIT*)
          • new Algo conditional orders from openAlgoOrders
        """
        results: List[dict] = []

        # 1) Classic openOrders
        base_orders = self._get_open_orders(symbol, force_refresh=force_refresh)
        for o in base_orders or []:
            t = o.get("type")
            if t in TP_TYPES or t in STOP_TYPES:
                results.append(o)

        # 2) Algo conditional orders
        try:
            algo_orders = self._signed("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
        except Exception:
            algo_orders = []

        for ao in algo_orders or []:
            ot = ao.get("orderType")
            if ot not in TP_TYPES and ot not in STOP_TYPES:
                continue
            try:
                trig = float(ao.get("triggerPrice") or 0.0)
            except Exception:
                trig = 0.0
            try:
                px = float(ao.get("price") or 0.0)
            except Exception:
                px = 0.0
            results.append(
                {
                    "type": ot,
                    "side": ao.get("side"),
                    "stopPrice": trig,
                    "price": px,
                    "algoId": ao.get("algoId"),
                    "clientAlgoId": ao.get("clientAlgoId"),
                    "algoType": ao.get("algoType"),
                }
            )

        return results

    def _find_stop_loss(self, symbol: str, side: str, orders: Optional[List[dict]] = None) -> Optional[float]:
        try:
            ords = orders if orders is not None else self._get_all_tpsl_orders(symbol)
            want_side = "SELL" if side == "LONG" else "BUY"
            cands = []
            for o in ords:
                if o.get("type") in STOP_TYPES and o.get("side") == want_side:
                    px = float(o.get("stopPrice") or o.get("price") or 0.0)
                    if px > 0:
                        cands.append(px)
            if not cands:
                return None
            mark = self._fetch_mark_price(symbol) or 0.0
            return min(cands, key=lambda x: abs(x - mark)) if mark > 0 else cands[1 if len(cands) > 1 else 0]
        except Exception:
            return None

    def _place_market_order(self, symbol: str, side_http: str, qty: float, client_id: str) -> bool:
        params = {
            "symbol": symbol,
            "side": side_http,
            "type": "MARKET",
            "quantity": qty,
            "newClientOrderId": client_id,
        }

        try:
            data = self._signed("POST", "/fapi/v1/order", params)
            oid = data.get("orderId")
            self._log(f"[DCA] order ack: {oid}")
            return True
        except requests.exceptions.RequestException as e:
            try:
                j = e.response.json() if e.response is not None else {}
                self._log(f"[DCA] order error {j.get('code')}: {j.get('msg')}")
            except Exception:
                self._log(f"[DCA] order error: {e}")
            return False

    def _cancel_existing_brackets(self, symbol: str, types: List[str]):
        try:
            # 1) Classic stop/take-profit orders on /fapi/v1/openOrders
            orders = self._get_open_orders(symbol, force_refresh=True)
            for o in orders or []:
                if o.get("type") in types:
                    oid = o.get("orderId")
                    try:
                        self._signed("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": oid})
                        self._log(f"[TP/SL] cancelled {o.get('type')} (id={oid})")
                    except Exception as e:
                        self._log(f"[TP/SL] cancel failed id={oid}: {e}")

            # 2) Conditional algo TP/SL orders on /fapi/v1/openAlgoOrders
            try:
                algo_orders = self._signed("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})
            except Exception:
                algo_orders = []

            for ao in algo_orders or []:
                ot = ao.get("orderType")
                if ot not in types:
                    continue
                if ao.get("algoType") != "CONDITIONAL":
                    continue
                algo_id = ao.get("algoId")
                if not algo_id:
                    continue
                try:
                    self._signed("DELETE", "/fapi/v1/algoOrder", {"algoId": algo_id})
                    self._log(f"[TP/SL] cancelled ALGO {ot} (algoId={algo_id})")
                except Exception as e:
                    self._log(f"[TP/SL] cancel algo failed algoId={algo_id}: {e}")
        except Exception as e:
            self._log(f"[TP/SL] cancel query failed: {e}")

    def _place_tpsl_order(
        self,
        symbol: str,
        side_http: str,
        ord_type: str,
        stop_price: float,
        position_side: Optional[str] = None,
    ) -> bool:
        """
        Place a close-position conditional TP/SL for USDT-M Futures using the
        new Algo Order API (POST /fapi/v1/algoOrder).

        - algoType="CONDITIONAL"
        - type in {STOP_MARKET, TAKE_PROFIT_MARKET, STOP, TAKE_PROFIT}
        - triggerPrice = stop_price
        - closePosition="true" to close all on that side
        """
        params = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side_http,
            "type": ord_type,
            "triggerPrice": stop_price,
            "workingType": "MARK_PRICE",
            "closePosition": "true",
        }

        # In Hedge Mode, positionSide must be sent
        try:
            if self._is_dual_side() and position_side:
                params["positionSide"] = position_side.upper()
        except Exception:
            pass

        try:
            self._signed("POST", "/fapi/v1/algoOrder", params)
            return True
        except requests.exceptions.RequestException as e:
            try:
                j = e.response.json() if e.response is not None else {}
                self._log(f"[TP/SL] {ord_type} error {j.get('code')}: {j.get('msg')}")
            except Exception:
                self._log(f"[TP/SL] {ord_type} error: {e}")
            return False

    # ---- Funding & indicators (with caches) ----
    def _get_funding_rate(self, symbol: str) -> Optional[float]:
        try:
            cached = self._funding_cache.get(symbol)
            if cached and (time.time() - cached.get("ts", 0)) < 30:
                return cached.get("rate")
            r = self._public_get("/fapi/v1/premiumIndex", {"symbol": symbol})
            if r.ok:
                rate = float(r.json().get("lastFundingRate") or 0.0)
            else:
                rate = None
            self._funding_cache[symbol] = {"rate": rate, "ts": time.time()}
            return rate
        except Exception:
            return None

    def _get_indicators(self, symbol: str, interval: str) -> Tuple[Optional[float], Optional[float]]:
        key = (symbol, interval)
        cached = self._ind_cache.get(key)
        if cached and (time.time() - cached.get("ts", 0)) < 60:
            return cached.get("adx"), cached.get("rsi")

        try:
            limit = max(200, ADX_LEN * 5 + 5)
            r = self._public_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
            if not r.ok:
                return None, None
            k = r.json()
            highs = [float(x[2]) for x in k]
            lows = [float(x[3]) for x in k]
            closes = [float(x[4]) for x in k]
            adx_val = self._calc_adx(highs, lows, closes, ADX_LEN)
            rsi_val = self._calc_rsi(closes, RSI_LEN)
        except Exception:
            adx_val = None
            rsi_val = None

        self._ind_cache[key] = {"adx": adx_val, "rsi": rsi_val, "ts": time.time()}
        return adx_val, rsi_val

    # ---- RSI( Wilder ) ----
    def _calc_rsi(self, closes: List[float], length: int) -> Optional[float]:
        if len(closes) < length + 1:
            return None
        gains, losses = [], []
        for i in range(1, length + 1):
            chg = closes[i] - closes[i - 1]
            gains.append(max(chg, 0.0))
            losses.append(max(-chg, 0.0))
        avg_gain = sum(gains) / length
        avg_loss = sum(losses) / length

        for i in range(length + 1, len(closes)):
            chg = closes[i] - closes[i - 1]
            gain = max(chg, 0.0)
            loss = max(-chg, 0.0)
            avg_gain = (avg_gain * (length - 1) + gain) / length
            avg_loss = (avg_loss * (length - 1) + loss) / length

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ---- ADX( Wilder ) ----
    def _calc_adx(self, highs: List[float], lows: List[float], closes: List[float], length: int) -> Optional[float]:
        n = len(closes)
        if n < length + 2:
            return None

        trs, plus_dm, minus_dm = [], [], []
        for i in range(1, n):
            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]
            plus_dm.append(up if (up > down and up > 0) else 0.0)
            minus_dm.append(down if (down > up and down > 0) else 0.0)
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)

        if len(trs) < length:
            return None
        tr14 = sum(trs[:length])
        pdi14 = sum(plus_dm[:length])
        mdi14 = sum(minus_dm[:length])

        adx_vals = []
        for i in range(length, len(trs)):
            if i > length:
                tr14 = tr14 - (tr14 / length) + trs[i]
                pdi14 = pdi14 - (pdi14 / length) + plus_dm[i]
                mdi14 = mdi14 - (mdi14 / length) + minus_dm[i]

            plus_di = 100.0 * (pdi14 / tr14) if tr14 > 0 else 0.0
            minus_di = 100.0 * (mdi14 / tr14) if tr14 > 0 else 0.0
            dx = 100.0 * abs(plus_di - minus_di) / max(plus_di + minus_di, 1e-12)
            adx_vals.append(dx)

        if len(adx_vals) < length:
            return sum(adx_vals) / max(len(adx_vals), 1)

        adx = sum(adx_vals[:length]) / length
        for i in range(length, len(adx_vals)):
            adx = (adx * (length - 1) + adx_vals[i]) / length
        return adx
