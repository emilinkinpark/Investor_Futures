import os
import csv
import json
import urllib.request
import sqlite3
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Sequence, Tuple

DEFAULT_DB_PATH = "C:/Users/Administrator/Documents/Binance Trade/AppDevelopment13.0/binance_futures_trader/marketpredictor.db"
ADELAIDE_TZ = ZoneInfo("Australia/Adelaide")


# --- BKL lifecycle (Excel-aligned) ---
# In your Excel, the column label is "BKL Time Zone" but the values are NOT a time.
# They represent a *lifecycle age counter* (minutes since the signal was first detected),
# persisted across refreshes. This implementation stores the first-seen timestamp per symbol
# in the same SQLite DB and computes:
#   BKL = floor((now - first_seen) / 60s), clamped to BKL_MAX_MINUTES.
# Symbols that drop out of the current top list are removed, so if they re-appear later
# they start at 0 again (matching how such lifecycle counters behave in practice).
BKL_MAX_MINUTES = 120

# --- Fear & Greed Index (Alternative.me) ---
_FNG_CACHE = {"value": "", "ts": 0.0}


def fetch_fear_greed_index(cache_ttl_sec: int = 300) -> str:
    """Fetch Crypto Fear & Greed Index.

    Returns a display string like: '72 (Greed)'.
    Cached in-memory to avoid repeated HTTP calls.

    If the API is unavailable, returns an empty string.
    """
    now = time.time()
    try:
        if _FNG_CACHE.get("value") and (now - float(_FNG_CACHE.get("ts") or 0.0)) < float(cache_ttl_sec):
            return str(_FNG_CACHE["value"])
    except Exception:
        pass

    try:
        with urllib.request.urlopen(
            "https://api.alternative.me/fng/?limit=1&format=json",
            timeout=5,
        ) as r:
            data = json.loads(r.read().decode("utf-8"))
            item = (data.get("data") or [{}])[0] or {}
            value = str(item.get("value") or "").strip()
            label = str(item.get("value_classification") or "").strip()
            txt = f"{value} ({label})" if (value and label) else value
    except Exception:
        txt = ""

    _FNG_CACHE["value"] = txt
    _FNG_CACHE["ts"] = now
    return txt


def _norm_symbol(sym: str) -> str:
    """Normalize symbol keys so lifecycle persistence is stable.

    The UI sometimes renders symbols as 'ABC/USDT' while the DB stores 'ABCUSDT'.
    If we don't normalize, lifecycle rows will be deleted/reinserted every refresh,
    and BKL will remain stuck at 0.
    """
    s = (sym or "").strip().upper()
    if not s:
        return ""
    # Common formatting differences
    s = s.replace("/", "")
    s = s.replace(" ", "")
    return s



class _HeaderTooltip:
    """Lightweight tooltip for ttk.Treeview column headers."""

    def __init__(self, tree: ttk.Treeview, columns: Sequence[str], text_by_col: Dict[str, str]):
        self.tree = tree
        self.columns = list(columns)
        self.text_by_col = text_by_col
        self.tip = None
        self._after_id = None
        self._last_col = None

        tree.bind("<Motion>", self._on_motion, add="+")
        tree.bind("<Leave>", self._on_leave, add="+")

    def _on_leave(self, _evt=None):
        self._cancel()
        self._hide()

    def _cancel(self):
        if self._after_id is not None:
            try:
                self.tree.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def _on_motion(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region != "heading":
            self._cancel()
            self._hide()
            self._last_col = None
            return

        col_id = self.tree.identify_column(event.x)  # '#1', '#2', ...
        try:
            idx = int(col_id.replace("#", "")) - 1
        except Exception:
            return
        if idx < 0 or idx >= len(self.columns):
            return

        col_name = self.columns[idx]
        text = self.text_by_col.get(col_name)
        if not text:
            self._cancel()
            self._hide()
            self._last_col = None
            return

        if self._last_col == col_name and self.tip is not None:
            return

        self._last_col = col_name
        self._cancel()
        self._after_id = self.tree.after(400, lambda: self._show(text, event))

    def _show(self, text: str, event):
        self._hide()
        try:
            x = self.tree.winfo_rootx() + event.x + 16
            y = self.tree.winfo_rooty() + event.y + 16
        except Exception:
            return

        self.tip = tk.Toplevel(self.tree)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")

        frm = ttk.Frame(self.tip, padding=(8, 6))
        frm.pack(fill="both", expand=True)

        lbl = ttk.Label(frm, text=text, justify="left", wraplength=520)
        lbl.pack(fill="both", expand=True)

    def _hide(self):
        if self.tip is not None:
            try:
                self.tip.destroy()
            except Exception:
                pass
            self.tip = None

class SignalTab(ttk.Frame):
    """SIGNAL tab.

    Provides two grids:
      1) Support/Resistance grid with ranks + RVOL/Efficiency/VWAP
      2) VIP grid with INV Index + CR-based Signal Quality + SCD + BKL (lifecycle minutes) + Max Dip proxy

    Notes:
      - SCD Rating Index is CNDRating (Schema Wizard: CND Rating)
      - BKL Time Zone is an Excel mislabel: it is a lifecycle age counter in minutes (0..BKL_MAX_MINUTES)
      - MAX Dip is a proxy based on lookback closes (until signal persistence is wired)
    """

    SR_COLS = [
        "COIN",
        "4H_N14_TOP_RANK",
        "Signal Quality",
        "8H_N5_TOP_RANK",
        "RVOL_8H_N5",
        "Effeciency_8H",
        "Efficiency_Signal",
        "VWAP_POS_%",
        "RVOL_4H_N14",
        "RVOL_4H_N11",
        "RVOL_4H_N6",
    ]

    VIP_COLS = [
        "CRYPTO NAME",
        "Signal Buy Status",
        "INV Index",
        "Signal Quality",
        "Quality Index",
        "RSI Value",
        "SCD Rating Index",
        "BKL Time Zone",
        "MAX Dip",
        "Total Time",
        "Greed & Fear",
    ]

    _SCHEMA_FIELDS: List[Tuple[str, str, bool]] = [
        ("Timestamp*", "timestamp", True),
        ("Symbol*", "symbol", True),
        ("Price*", "price", True),
        ("Volume", "volume", False),
        ("Adx", "adx", False),
        ("Di Plus", "di_plus", False),
        ("Di Minus", "di_minus", False),
        ("Macd", "macd", False),
        ("Macd Signal", "macd_signal", False),
        ("Vwap", "vwap", False),
        ("Oi", "oi", False),
        ("Liq", "liq", False),
        ("Rsi", "rsi", False),
        ("Funding", "funding", False),
        ("Coin Ls", "coin_ls", False),
        ("Trader Ls", "trader_ls", False),
        ("CND Rating", "cnd", False),
    ]

    def __init__(self, parent):
        super().__init__(parent)

        self.on_export_all_csv = None
        self._auto_job = None

        # Top controls are intentionally split across TWO rows.
        # Your window is often narrow and the DB path is long; keeping everything on one row
        # causes visual collisions/overlaps when the tab is refreshed.
        top = ttk.Frame(self)
        top.pack(fill="x", padx=6, pady=4)

        self.db_path_var = tk.StringVar(value=DEFAULT_DB_PATH)
        self.bars_per_symbol_var = tk.IntVar(value=500)
        self.lookback_var = tk.IntVar(value=50)
        self.auto_refresh_enabled = tk.BooleanVar(value=False)
        self.auto_refresh_seconds = tk.IntVar(value=10)

        self.last_run_var = tk.StringVar(value="—")
        self.ready_var = tk.StringVar(value="Ready")

        # Single-row toolbar (match Analysis tab layout)
        ttk.Label(top, text="DB:").grid(row=0, column=0, sticky="w", padx=(0, 4))
        ttk.Entry(top, textvariable=self.db_path_var, width=80).grid(row=0, column=1, sticky="we", padx=(0, 6))
        ttk.Button(top, text="Browse...", command=self._browse_db).grid(row=0, column=2, sticky="w", padx=(0, 8))
        ttk.Button(top, text="Schema Wizard", command=self._schema_wizard).grid(row=0, column=3, sticky="w", padx=(0, 14))

        ttk.Label(top, text="Bars/symbol:").grid(row=0, column=4, sticky="w", padx=(0, 4))
        ttk.Spinbox(top, from_=50, to=10000, increment=50, width=7, textvariable=self.bars_per_symbol_var).grid(
            row=0, column=5, sticky="w", padx=(0, 14)
        )

        ttk.Label(top, text="Lookback:").grid(row=0, column=6, sticky="w", padx=(0, 4))
        ttk.Spinbox(top, from_=5, to=500, increment=5, width=7, textvariable=self.lookback_var).grid(
            row=0, column=7, sticky="w", padx=(0, 14)
        )

        ttk.Checkbutton(top, variable=self.auto_refresh_enabled, command=self._toggle_auto_refresh).grid(
            row=0, column=8, sticky="w", padx=(0, 4)
        )
        ttk.Label(top, text="Auto-refresh (s):").grid(row=0, column=9, sticky="w", padx=(0, 4))
        ttk.Spinbox(top, from_=2, to=3600, increment=1, width=6, textvariable=self.auto_refresh_seconds).grid(
            row=0, column=10, sticky="w", padx=(0, 14)
        )

        ttk.Button(top, text="Run Now", command=self._run_now).grid(row=0, column=11, sticky="w", padx=(0, 8))
        ttk.Button(top, text="Export ALL CSV", command=self._export_all_csv).grid(row=0, column=12, sticky="w", padx=(0, 14))

        ttk.Label(top, text="Last run:").grid(row=0, column=13, sticky="e", padx=(10, 4))
        ttk.Label(top, textvariable=self.last_run_var).grid(row=0, column=14, sticky="w", padx=(0, 10))
        ttk.Label(top, textvariable=self.ready_var).grid(row=0, column=15, sticky="w")

        top.grid_columnconfigure(1, weight=1)


        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        self._panel_title(body, "SUPPORT & RESISTENCE : LEVEL    [  LONG   POSITION  ]").pack(fill="x", pady=(0, 4))
        self.sr_tree = self._make_tree(body, self.SR_COLS, height=12)

        # Excel-style highlight: Efficiency_8H in [0.8, 1.8] -> light green
        # Note: ttk.Treeview does not support per-cell background; tag applies to the row.
        try:
            self.sr_tree.tag_configure("eff_ok", background="#C6EFCE")
        except Exception:
            pass

        self._panel_title(body, "Total VIP Premium Signal  ( Buy Signal Status )").pack(fill="x", pady=(10, 4))
        self.vip_tree = self._make_tree(body, self.VIP_COLS, height=8)

        # ---------------- Tooltips (Excel-aligned guidance) ----------------
        try:
            sr_tips = {
                "Effeciency_8H": (
                    "Efficiency_8H = |Close(Open) move| / ATR(14) on 8H candles.\n"
                    "Interpretation: volatility-normalised impulse strength.\n\n"
                    "Long Setpoint (preferred): 0.8 to 1.8 (highlighted).\n"
                    "Below 0.5 = weak/noise; above ~1.8 = exhaustion/chase risk."
                ),
                "Efficiency_Signal": (
                    "Efficiency_Signal is the regime label derived from Efficiency_8H and RVOL_8H_N5.\n\n"
                    "Long Setpoint (preferred): Positive / TREND / TREND_VOL / TREND_STRONG.\n"
                    "Avoid: CHOP / CHOP_HIGHVOL unless other metrics are exceptional."
                ),
                "VWAP_POS_%": (
                    "VWAP_POS_% = (Price - VWAP) / VWAP × 100.\n"
                    "Positive = trading above VWAP; Negative = below VWAP.\n\n"
                    "Long Setpoint (preferred entries): -10% to +10% (near VWAP).\n"
                    "Breakout continuation: +10% to +100% only if RVOL & Efficiency are strong.\n"
                    "Very extended: > +100% increases pullback risk."
                ),
                "RVOL_4H_N14": (
                    "RVOL_4H_N14 = current 4H volume / average volume over last 14×4H bars (~56h).\n\n"
                    "Long Setpoint: ≥ 1.2 (above-average participation).\n"
                    "Strong: ≥ 1.5. Extreme: ≥ 3.0 (often news/spike)."
                ),
                "RVOL_4H_N11": (
                    "RVOL_4H_N11 = current 4H volume / average over last 11×4H bars (~44h).\n\n"
                    "Long Setpoint: ≥ 1.3. Strong: ≥ 1.6."
                ),
                "RVOL_4H_N6": (
                    "RVOL_4H_N6 = current 4H volume / average over last 6×4H bars (~24h).\n\n"
                    "Long Setpoint: ≥ 1.5 (fast acceleration).\n"
                    "If < 0.8, momentum is fading."
                ),
            }
            vip_tips = {
                "INV Index": (
                    "INV Index (Excel) estimates opportunity magnitude (capital efficiency / impact).\n"
                    "Formula: INV = (RVOL_8H_N5 × Efficiency_8H × |VWAP_POS_%|) / Price.\n\n"
                    "Higher = stronger, more 'responsive' move per unit capital (often low-priced coins).\n"
                    "INV does not mean 'safe'—use CR/Quality and BKL timing."
                ),
                "BKL Time Zone": (
                    "BKL Time Zone is an Excel mislabel.\n"
                    "It is Breakout Life (minutes since this symbol first appeared in the active signal list).\n\n"
                    "Long Setpoint: 0–30 = fresh; 30–90 = still valid; 90–120 = aging; >120 clamped."
                ),
                "MAX Dip": (
                    "MAX Dip = worst drawdown (%) observed after signal (negative numbers).\n\n"
                    "Long Setpoint guidance:\n"
                    "  0% to -5%  : very clean\n"
                    " -5% to -10% : acceptable\n"
                    "-10% to -15% : risk increasing\n"
                    "< -15%       : avoid / stop logic required"
                ),
            }
            _HeaderTooltip(self.sr_tree, self.SR_COLS, sr_tips)
            _HeaderTooltip(self.vip_tree, self.VIP_COLS, vip_tips)
        except Exception:
            pass


    # ---------------- UI ----------------
    def _panel_title(self, parent, text: str):
        bar = tk.Frame(parent, bg="#0b2a57", height=22)
        bar.pack_propagate(False)
        tk.Label(bar, text=text, fg="red", bg="#0b2a57", font=("TkDefaultFont", 9, "bold")).pack(side="top", pady=2)
        return bar

    def _make_tree(self, parent, columns: Sequence[str], height: int = 10) -> ttk.Treeview:
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True)

        tree = ttk.Treeview(container, columns=list(columns), show="headings", height=height)
        vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        for col in columns:
            tree.heading(col, text=col)
            w = 160 if col in ("COIN", "CRYPTO NAME", "Signal Quality") else 120
            if col in ("BKL Time Zone", "Greed & Fear"):
                w = 170
            tree.column(col, width=w, stretch=True, anchor="center")

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="we")
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        return tree

    # ---------------- Actions ----------------
    def _browse_db(self):
        path = filedialog.askopenfilename(
            title="Select database",
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("All Files", "*.*")],
        )
        if path:
            self.db_path_var.set(path)

    def _export_all_csv(self):
        if callable(self.on_export_all_csv):
            try:
                self.on_export_all_csv()
                return
            except Exception as e:
                messagebox.showerror("Export ALL CSV", f"Export failed:\n{e}")
                return

        out = filedialog.asksaveasfilename(
            title="Export ALL CSV (Signal tab)",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All Files", "*.*")],
        )
        if not out:
            return

        base, ext = os.path.splitext(out)
        sr_out = out
        vip_out = f"{base}_vip{ext or '.csv'}"

        try:
            self._export_tree(sr_out, self.sr_tree)
            self._export_tree(vip_out, self.vip_tree)
            messagebox.showinfo("Export ALL CSV", f"Exported:\n{sr_out}\n{vip_out}")
        except Exception as e:
            messagebox.showerror("Export ALL CSV", f"Export failed:\n{e}")

    @staticmethod
    def _export_tree(path: str, tree: ttk.Treeview):
        cols = list(tree["columns"])
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for iid in tree.get_children():
                w.writerow(tree.item(iid, "values"))

    def _toggle_auto_refresh(self):
        if self.auto_refresh_enabled.get():
            self._schedule_auto_refresh()
        else:
            if self._auto_job:
                try:
                    self.after_cancel(self._auto_job)
                except Exception:
                    pass
            self._auto_job = None

    def _schedule_auto_refresh(self):
        if self._auto_job:
            try:
                self.after_cancel(self._auto_job)
            except Exception:
                pass
        self._auto_job = None

        if not self.auto_refresh_enabled.get():
            return

        sec = int(self.auto_refresh_seconds.get() or 10)
        sec = max(2, min(sec, 3600))

        def _tick():
            if not self.auto_refresh_enabled.get():
                return
            self._run_now()
            self._auto_job = self.after(sec * 1000, _tick)

        self._auto_job = self.after(sec * 1000, _tick)

    # ---------------- Utilities ----------------
    @staticmethod
    def _safe_float(x) -> Optional[float]:
        try:
            if x is None:
                return None
            if isinstance(x, str):
                s = x.strip()
                if not s:
                    return None
                s = s.replace(",", "")
                return float(s)
            return float(x)
        except Exception:
            return None

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, v))

    @staticmethod
    def _fmt_inv(v: Optional[float]) -> str:
        return "" if v is None else f"{v:.1f}"

    @staticmethod
    def _parse_ts_any(ts_val) -> Optional[datetime]:
        """Parse TEXT timestamp with tz or epoch seconds/ms."""
        if ts_val is None:
            return None
        try:
            if isinstance(ts_val, (int, float)):
                n = float(ts_val)
                if n > 1e12:
                    n = n / 1000.0
                return datetime.fromtimestamp(n, tz=timezone.utc)
            if isinstance(ts_val, str):
                s = ts_val.strip()
                if not s:
                    return None
                # common: 2026-01-05 08:58:36+0000
                for fmt in ("%Y-%m-%d %H:%M:%S%z",):
                    try:
                        dtv = datetime.strptime(s, fmt)
                        if dtv.tzinfo is None:
                            dtv = dtv.replace(tzinfo=timezone.utc)
                        return dtv.astimezone(timezone.utc)
                    except Exception:
                        pass
                try:
                    dtv = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dtv.tzinfo is None:
                        dtv = dtv.replace(tzinfo=timezone.utc)
                    return dtv.astimezone(timezone.utc)
                except Exception:
                    return None
        except Exception:
            return None
        return None

    @staticmethod
    def _fmt_dt_adelaide(ts_utc: Optional[datetime]) -> str:
        if ts_utc is None:
            return ""
        try:
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            local = ts_utc.astimezone(ADELAIDE_TZ)
            return local.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    # ---------------- INV Index (Excel) ----------------
    def _inv_index_excel(
        self,
        rvol_8h_n5: Optional[float],
        efficiency_8h: Optional[float],
        vwap_pos_pct: Optional[float],
        price: Optional[float],
    ) -> float:
        """Excel-aligned INV Index.

        Reference sheet behaviour indicates INV is a continuous (unbounded) index that scales with:
          - RVOL_8H_N5 (volume expansion)
          - Efficiency_8H (impulse strength)
          - |VWAP_POS_%| (distance/extension from VWAP, in percent points)
          - and is normalised by Price (USDT) to amplify lower-priced assets.

        Formula:
            INV = (RVOL_8H_N5 * Efficiency_8H * abs(VWAP_POS_%)) / Price

        Notes:
          - VWAP_POS_% is used as shown in the UI (e.g. 98.48 means 98.48, not 0.9848).
          - Price is taken from the most recent available close/price field in the DB row.
          - Returns 0.0 if any required input is missing/invalid.
        """
        try:
            rvol = float(rvol_8h_n5) if rvol_8h_n5 is not None else None
            eff = float(efficiency_8h) if efficiency_8h is not None else None
            vwap = float(vwap_pos_pct) if vwap_pos_pct is not None else None
            px = float(price) if price is not None else None
        except Exception:
            return 0.0

        if rvol is None or eff is None or vwap is None or px is None:
            return 0.0
        if px <= 0:
            return 0.0

        inv = (rvol * eff * abs(vwap)) / px
        # keep Excel-like precision (do NOT clamp)
        return float(inv)


    # ---------------- CR + Signal Quality label ----------------
    def _validate_inv_against_excel(self, vip_rows: List[Dict[str, object]]) -> None:
        """Best-effort regression check against known Excel screenshots.

        This runs silently and only logs when the expected symbols are present
        in the VIP rows (i.e., you are viewing the same historical snapshot).
        """
        expected = {
            # Photo - 5_06012026.jpg (6 January 2026 example)
            "CLOUSDT": 0.4177,
            "RIVERUSDT": 16.57,
            "VIRTUALUSDT": 1.0966,
            "TAUSDT": 0.03093,
            "QUSDT": 0.020152,
            "MINAUSDT": 0.0922,
        }
        # Tolerance: % for large, absolute for small
        for r in vip_rows:
            sym_raw = str(r.get("symbol") or r.get("Crypto Name") or "")
            sym = _norm_symbol(sym_raw)
            if not sym or sym not in expected:
                continue
            exp = expected[sym]
            got = r.get("inv")
            try:
                got_f = float(got)
            except Exception:
                continue

            tol = max(0.02 * float(exp), 0.02)  # 2% or 0.02 absolute
            if abs(got_f - float(exp)) > tol:
                print(f"[INV VALIDATION] {sym}: expected≈{exp} got={got_f} (Δ={got_f-exp:.4f})")
            else:
                print(f"[INV VALIDATION] {sym}: OK (expected≈{exp}, got={got_f})")

    def _cr_bucket(self, cnd: Optional[float], rsi: Optional[float], macd_line: Optional[float], macd_signal: Optional[float]) -> int:
        if cnd is None or rsi is None:
            return 1
        if cnd >= 7.0 and rsi < 30.0:
            return 4
        if cnd >= 5.0 and rsi < 50.0 and macd_line is not None and macd_signal is not None and macd_line > macd_signal:
            return 3
        if cnd >= 3.0 and rsi < 70.0:
            return 2
        return 1

    def _is_good_signal(self, inv: float, rvol_primary: Optional[float], rvol_fallback: Optional[float], eff_sig: str) -> bool:
        conds = 0
        if inv >= 7.0:
            conds += 1
        rvol = rvol_primary if rvol_primary is not None else rvol_fallback
        if rvol is not None and rvol >= 1.2:
            conds += 1
        eff = (eff_sig or "").upper()
        if ("TREND_STRONG" in eff) or ("TREND" in eff):
            conds += 1
        return conds >= 2

    @staticmethod
    def _signal_quality_label(cr: int, good: bool) -> str:
        cr = int(cr) if cr in (1, 2, 3, 4) else 1
        return f"{cr}CR_" + ("GoodSignal" if good else "AverageSignal")

    @staticmethod
    def _quality_index(cr: int, good: bool) -> int:
        base_by_cr = {1: 40, 2: 55, 3: 70, 4: 85}
        base = base_by_cr.get(int(cr), 40)
        return int(max(0, min(100, base + (10 if good else 0))))

    # ---------------- Timeframe inference ----------------
    @staticmethod
    def _bars_per_8h_from_adx_col(adx_col: str) -> int:
        lc = (adx_col or "").lower()
        if "30m" in lc:
            return 16
        if "15m" in lc:
            return 32
        if "1h" in lc or "60m" in lc:
            return 8
        return 16

    @staticmethod
    def _bars_per_4h_from_adx_col(adx_col: str) -> int:
        lc = (adx_col or "").lower()
        if "30m" in lc:
            return 8
        if "15m" in lc:
            return 16
        if "1h" in lc or "60m" in lc:
            return 4
        return 8

    # ---------------- Metrics ----------------
    def _compute_efficiency(self, closes_newest_first: List[float]) -> Optional[float]:
        if len(closes_newest_first) < 2:
            return None
        net = abs(closes_newest_first[0] - closes_newest_first[-1])
        path = 0.0
        for i in range(len(closes_newest_first) - 1):
            path += abs(closes_newest_first[i] - closes_newest_first[i + 1])
        return round(net / path, 3) if path > 0 else 0.0

    def _compute_rvol(self, vols_newest_first: List[float], win_bars: int, n_back: int) -> Optional[float]:
        if win_bars <= 0 or len(vols_newest_first) < win_bars:
            return None
        cur = sum(vols_newest_first[:win_bars])
        past = []
        for i in range(1, n_back + 1):
            start = i * win_bars
            end = start + win_bars
            if end <= len(vols_newest_first):
                past.append(sum(vols_newest_first[start:end]))
        if not past:
            return None
        avg_past = sum(past) / len(past)
        if avg_past <= 0:
            return 0.0 if cur == 0 else None
        return round(cur / avg_past, 2)

    def _compute_efficiency_signal(self, eff_8h: Optional[float], rvol_8h: Optional[float]) -> str:
        if eff_8h is None:
            return ""
        if eff_8h >= 0.70:
            base = "TREND_STRONG"
        elif eff_8h >= 0.55:
            base = "TREND"
        elif eff_8h <= 0.40:
            base = "CHOP"
        else:
            base = "NEUTRAL"

        if rvol_8h is None:
            return base
        if base in ("TREND_STRONG", "TREND") and rvol_8h >= 1.20:
            return base + "_VOL"
        if base in ("TREND_STRONG", "TREND") and rvol_8h <= 0.85:
            return base + "_LOWVOL"
        if base == "CHOP" and rvol_8h >= 1.20:
            return "CHOP_HIGHVOL"
        return base

    # ---------------- Legacy SR quality (top grid label) ----------------
    def _legacy_sr_quality(
        self,
        adx: Optional[float],
        rsi: Optional[float] = None,
        macd_line: Optional[float] = None,
        macd_signal: Optional[float] = None,
        price: Optional[float] = None,
        vwap: Optional[float] = None,
    ) -> str:
        score = 0
        if adx is not None:
            if adx >= 35:
                score += 40
            elif adx >= 25:
                score += 30
            elif adx >= 20:
                score += 20

        if rsi is not None:
            if 55 <= rsi <= 70:
                score += 15
            elif rsi >= 50:
                score += 8

        if macd_line is not None and macd_signal is not None:
            if macd_line > macd_signal:
                score += 15
        elif macd_line is not None and macd_signal is None:
            if macd_line > 0:
                score += 10

        if price is not None and vwap is not None and vwap > 0:
            delta_pct = (price - vwap) / vwap * 100.0
            if 0 <= delta_pct <= 2:
                score += 20
            elif delta_pct > 2:
                score += 10

        if score >= 75:
            return "High_Quality"
        if score >= 60:
            return "Good"
        if score >= 45:
            return "Normal"
        return "Weak"

    # ---------------- Run ----------------
    def _run_now(self):
        self.ready_var.set("Running...")
        self.update_idletasks()

        db = self.db_path_var.get().strip()
        if not db:
            self.ready_var.set("Error")
            messagebox.showerror("Signal", "DB path is empty.")
            return
        if not os.path.exists(db):
            self.ready_var.set("Error")
            messagebox.showerror("Signal", f"DB not found:\n{db}")
            return

        try:
            rows = self._fetch_top10_distinct(db)

            # Excel-aligned BKL lifecycle: minutes since first seen in the *current* top list.
            # Persisted in DB so it survives refreshes/restarts.
            # Use normalized symbols for lifecycle persistence.
            # Without this, symbols like "ABC/USDT" vs "ABCUSDT" will reset BKL every refresh.
            symbols_now = [_norm_symbol(str(r.get("symbol") or "")) for r in rows]
            symbols_now = [s for s in symbols_now if s]
            bkl_map = self._bkl_update_and_get(db, symbols_now)

            self._render_sr(rows)
            self._render_vip(rows, bkl_map)
            self.last_run_var.set(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.ready_var.set("Ready")
        except Exception as e:
            self.ready_var.set("Error")
            messagebox.showerror("Signal", str(e))

    def _render_sr(self, rows: List[Dict[str, object]]):
        self._clear_tree(self.sr_tree)

        rows_for_8h_rank = [r for r in rows if r.get("adx8h_n5") is not None]
        rows_for_8h_rank.sort(key=lambda x: float(x.get("adx8h_n5") or 0.0), reverse=True)
        rank8h_map = {r["symbol"]: (i + 1) for i, r in enumerate(rows_for_8h_rank)}

        for rank4h, r in enumerate(rows, start=1):
            sym = str(r.get("symbol") or "")

            price = self._safe_float(r.get("price"))
            vwap = self._safe_float(r.get("vwap"))
            vwap_pos_str = ""
            if price is not None and vwap is not None and vwap > 0:
                vwap_pos_str = f"{((price - vwap) / vwap * 100.0):.2f}"

            # Excel-style rank labels: Top-1, Top-2, else Normal
            rank4h_label = "Top-1" if rank4h == 1 else ("Top-2" if rank4h == 2 else "Normal")
            rank8h = rank8h_map.get(sym, "")
            rank8h_label = ""
            if isinstance(rank8h, int):
                rank8h_label = "Top-1" if rank8h == 1 else ("Top-2" if rank8h == 2 else "Normal")

            values = (
                sym,
                rank4h_label,
                r.get("sr_signal_quality") or "",
                rank8h_label,
                "" if r.get("rvol_8h_n5") is None else r.get("rvol_8h_n5"),
                "" if r.get("eff_8h") is None else r.get("eff_8h"),
                r.get("eff_signal") or "",
                vwap_pos_str,
                "" if r.get("rvol_4h_n14") is None else r.get("rvol_4h_n14"),
                "" if r.get("rvol_4h_n11") is None else r.get("rvol_4h_n11"),
                "" if r.get("rvol_4h_n6") is None else r.get("rvol_4h_n6"),
            )

            # Highlight when 0.8 <= Efficiency_8H <= 1.8
            tags = []
            eff8 = self._safe_float(r.get("eff_8h"))
            if eff8 is not None and 0.8 <= eff8 <= 1.8:
                tags.append("eff_ok")

            self.sr_tree.insert("", "end", values=values, tags=tuple(tags))

    def _render_vip(self, rows: List[Dict[str, object]], bkl_map: Optional[Dict[str, int]] = None):
        self._clear_tree(self.vip_tree)

        now_utc = datetime.now(tz=timezone.utc)
        fear_greed = fetch_fear_greed_index()
        lookback_n = int(self.lookback_var.get() or 50)
        lookback_n = max(5, min(lookback_n, 500))

        vip_out: List[Dict[str, object]] = []

        for r in rows:
            sym = str(r.get("symbol") or "")

            adx = self._safe_float(r.get("adx"))
            rsi_v = self._safe_float(r.get("rsi"))
            macd_line = self._safe_float(r.get("macd_line"))
            macd_sig = self._safe_float(r.get("macd_signal"))
            cnd = self._safe_float(r.get("cnd"))

            price = self._safe_float(r.get("price"))
            vwap = self._safe_float(r.get("vwap"))
            vwap_pos = None
            if price is not None and vwap is not None and vwap > 0:
                vwap_pos = (price - vwap) / vwap * 100.0

            eff_sig = str(r.get("eff_signal") or "")
            rvol_4h_n14 = self._safe_float(r.get("rvol_4h_n14"))
            rvol_8h_n5 = self._safe_float(r.get("rvol_8h_n5"))

            inv = self._inv_index_excel(
                rvol_8h_n5=rvol_8h_n5,
                efficiency_8h=self._safe_float(r.get("eff_8h")),
                vwap_pos_pct=vwap_pos,
                price=self._safe_float(r.get("price")),
            )

            cr = self._cr_bucket(cnd, rsi_v, macd_line, macd_sig)
            good = self._is_good_signal(inv, rvol_4h_n14, rvol_8h_n5, eff_sig)
            sig_quality = self._signal_quality_label(cr, good)
            q_index = self._quality_index(cr, good)

            buy_status = "BUY" if (good and inv >= 7.0 and cr >= 3) else "WAIT"

            # SCD Rating Index = CNDRating
            scd_idx = "" if cnd is None else int(round(cnd))

            # BKL Time Zone (Excel): lifecycle minutes since first seen in the current top list.
            # If not available, fall back to blank.
            bkl_val = ""
            if bkl_map:
                bkl_val = bkl_map.get(_norm_symbol(sym), "")

            # Timestamp used for Total Time (kept)
            ts_utc = self._parse_ts_any(r.get("ts_raw"))

            # Total Time since timestamp
            total_time = ""
            if ts_utc is not None:
                delta = now_utc - ts_utc
                secs = int(max(0.0, delta.total_seconds()))
                hh = secs // 3600
                mm = (secs % 3600) // 60
                ss = secs % 60
                total_time = f"{hh:02d}:{mm:02d}:{ss:02d}"

            # MAX Dip proxy: min(close) over lookback vs current close
            max_dip = ""
            closes = r.get("closes_lb")
            if isinstance(closes, list) and closes and price is not None:
                try:
                    mn = min([x for x in closes if isinstance(x, (int, float))])
                    if price > 0:
                        dip_pct = (mn - price) / price * 100.0
                        # show negative if dipped below current
                        max_dip = f"{dip_pct:.2f}%"
                except Exception:
                    max_dip = ""
            # Greed & Fear (global index)
            greed_fear = fear_greed

            # IMPORTANT: values tuple order MUST match VIP_COLS order
            values = (
                sym,                       # CRYPTO NAME
                buy_status,                # Signal Buy Status
                self._fmt_inv(inv),        # INV Index
                sig_quality,               # Signal Quality
                q_index,                   # Quality Index
                "" if rsi_v is None else round(rsi_v, 2),  # RSI Value
                scd_idx,                   # SCD Rating Index
                bkl_val,                   # BKL Time Zone (minutes)
                max_dip,                   # MAX Dip
                total_time,                # Total Time
                greed_fear,                # Greed & Fear
            )
            vip_out.append({"symbol": sym, "inv": inv})
            self.vip_tree.insert("", "end", values=values)

    
        try:
            self._validate_inv_against_excel(vip_out)
        except Exception:
            pass

    @staticmethod
    def _clear_tree(tree: ttk.Treeview):
        for iid in tree.get_children():
            tree.delete(iid)

    # ---------------- DB ----------------
    def _fetch_top10_distinct(self, db_path: str) -> List[Dict[str, object]]:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            self._ensure_meta_table(conn)

            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            schema = self._load_signal_schema(conn)
            if not schema.get("table"):
                schema = self._auto_default_schema(conn, tables)
                self._save_signal_schema(conn, schema)

            table = (schema.get("table") or "").strip()
            sym_col = (schema.get("symbol") or "").strip()
            ts_col = (schema.get("timestamp") or "").strip()
            adx_col = (schema.get("adx") or "").strip()
            rsi_col = (schema.get("rsi") or "").strip()
            macd_col = (schema.get("macd") or "").strip()
            macd_sig_col = (schema.get("macd_signal") or "").strip()
            price_col = (schema.get("price") or "").strip()
            vwap_col = (schema.get("vwap") or "").strip()
            volume_col = (schema.get("volume") or "").strip()
            cnd_col = (schema.get("cnd") or "").strip()

            if not table or table not in tables:
                raise RuntimeError("Schema Wizard: selected table is missing. Please open Schema Wizard and save again.")

            pragma = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            cols = [c[1] for c in pragma]
            col_types = {c[1]: (c[2] or "").upper() for c in pragma}

            ts_is_numeric = False
            if ts_col and ts_col in col_types:
                t = col_types.get(ts_col, "")
                ts_is_numeric = ("INT" in t) or ("REAL" in t) or ("NUM" in t)

            for req_name, req_col in (("Symbol", sym_col), ("Adx", adx_col), ("Price", price_col)):
                if not req_col or req_col not in cols:
                    raise RuntimeError(
                        "Schema Wizard mapping is missing/invalid.\n"
                        f"Required field '{req_name}' is not mapped to an existing column.\n\n"
                        "Open 'Schema Wizard' on the SIGNAL tab and map required fields."
                    )

            def sel_or_null(colname: str, alias: str) -> str:
                if colname and colname in cols:
                    return f"CAST({colname} AS REAL) AS {alias}"
                return f"NULL AS {alias}"

            def sel_ts(colname: str) -> str:
                if not colname or colname not in cols:
                    return "NULL AS ts_raw"
                return f"{colname} AS ts_raw"

            if ts_col and ts_col in cols:
                ts_order = f"CAST({ts_col} AS INTEGER) DESC" if ts_is_numeric else f"{ts_col} DESC"
                q = f"""
                WITH ranked AS (
                  SELECT
                    {sym_col} AS symbol,
                    CAST({adx_col} AS REAL) AS adx,
                    {sel_or_null(rsi_col, "rsi")},
                    {sel_or_null(macd_col, "macd_line")},
                    {sel_or_null(macd_sig_col, "macd_signal")},
                    {sel_or_null(cnd_col, "cnd")},
                    CAST({price_col} AS REAL) AS price,
                    {sel_or_null(vwap_col, "vwap")},
                    {sel_or_null(volume_col, "volume")},
                    {sel_ts(ts_col)},
                    rowid AS rid,
                    ROW_NUMBER() OVER (
                      PARTITION BY {sym_col}
                      ORDER BY {ts_order}, rowid DESC
                    ) AS rn
                  FROM '{table}'
                  WHERE {sym_col} IS NOT NULL AND {sym_col} <> '' AND {adx_col} IS NOT NULL
                )
                SELECT symbol, adx, rsi, macd_line, macd_signal, cnd, price, vwap, volume, ts_raw
                FROM ranked
                WHERE rn = 1
                ORDER BY adx DESC
                LIMIT 10
                """
            else:
                q = f"""
                SELECT
                    {sym_col} AS symbol,
                    MAX(CAST({adx_col} AS REAL)) AS adx,
                    NULL AS rsi,
                    NULL AS macd_line,
                    NULL AS macd_signal,
                    NULL AS cnd,
                    NULL AS price,
                    NULL AS vwap,
                    NULL AS volume,
                    NULL AS ts_raw
                FROM '{table}'
                WHERE {sym_col} IS NOT NULL AND {sym_col} <> '' AND {adx_col} IS NOT NULL
                GROUP BY {sym_col}
                ORDER BY MAX(CAST({adx_col} AS REAL)) DESC
                LIMIT 10
                """

            top_rows = conn.execute(q).fetchall()

            bars_8h = self._bars_per_8h_from_adx_col(adx_col)
            bars_4h = self._bars_per_4h_from_adx_col(adx_col)

            need_8h = (1 + 5) * bars_8h
            need_4h = (1 + 14) * bars_4h
            hist_limit = max(need_8h, need_4h, int(self.bars_per_symbol_var.get() or 500), 100)

            out: List[Dict[str, object]] = []
            for rr in top_rows:
                r = dict(rr)  # critical: sqlite3.Row -> dict
                sym = r.get("symbol")
                if not sym:
                    continue

                # Fallback: some rows have CNDRating NULL on the latest bar.
                # If so, fetch the most recent non-null CNDRating for this symbol.
                try:
                    if (r.get("cnd") is None) and cnd_col and (cnd_col in cols):
                        if ts_col and (ts_col in cols):
                            ts_order2 = f"CAST({ts_col} AS INTEGER) DESC" if ts_is_numeric else f"{ts_col} DESC"
                            q_cnd = f"SELECT CAST({cnd_col} AS REAL) AS cnd FROM '{table}' WHERE {sym_col}=? AND {cnd_col} IS NOT NULL AND TRIM(CAST({cnd_col} AS TEXT))<>'' ORDER BY {ts_order2}, rowid DESC LIMIT 1"
                        else:
                            q_cnd = f"SELECT CAST({cnd_col} AS REAL) AS cnd FROM '{table}' WHERE {sym_col}=? AND {cnd_col} IS NOT NULL AND TRIM(CAST({cnd_col} AS TEXT))<>'' ORDER BY rowid DESC LIMIT 1"
                        rr_cnd = conn.execute(q_cnd, (sym,)).fetchone()
                        if rr_cnd is not None:
                            # rr_cnd can be Row/tuple
                            try:
                                r["cnd"] = rr_cnd[0]
                            except Exception:
                                r["cnd"] = None
                except Exception:
                    pass

                cols_needed = []
                if ts_col and ts_col in cols:
                    cols_needed.append(ts_col)
                if price_col and price_col in cols:
                    cols_needed.append(price_col)
                if volume_col and volume_col in cols:
                    cols_needed.append(volume_col)
                if adx_col and adx_col in cols:
                    cols_needed.append(adx_col)

                hist = self._fetch_symbol_history(
                    conn=conn,
                    table=table,
                    sym_col=sym_col,
                    ts_col=ts_col if (ts_col and ts_col in cols) else "",
                    symbol=sym,
                    limit=hist_limit,
                    cols_needed=cols_needed,
                    ts_is_numeric=ts_is_numeric,
                )

                closes: List[float] = []
                vols: List[float] = []
                adxs: List[float] = []

                for hr in hist:
                    pv = self._safe_float(hr[price_col]) if (price_col and price_col in hr.keys()) else None
                    if pv is not None:
                        closes.append(pv)
                    vv = self._safe_float(hr[volume_col]) if (volume_col and volume_col in hr.keys()) else None
                    vols.append(vv if vv is not None else 0.0)
                    av = self._safe_float(hr[adx_col]) if (adx_col and adx_col in hr.keys()) else None
                    if av is not None:
                        adxs.append(av)

                eff_8h = None
                if len(closes) >= bars_8h:
                    eff_8h = self._compute_efficiency(closes[:bars_8h])

                rvol_8h_n5 = self._compute_rvol(vols, bars_8h, 5) if (volume_col and len(vols) >= (1 + 5) * bars_8h) else None
                rvol_4h_n14 = self._compute_rvol(vols, bars_4h, 14) if (volume_col and len(vols) >= (1 + 14) * bars_4h) else None
                rvol_4h_n11 = self._compute_rvol(vols, bars_4h, 11) if (volume_col and len(vols) >= (1 + 11) * bars_4h) else None
                rvol_4h_n6 = self._compute_rvol(vols, bars_4h, 6) if (volume_col and len(vols) >= (1 + 6) * bars_4h) else None

                adx8h_n5 = None
                if len(adxs) >= 1:
                    n = min(5, len(adxs))
                    adx8h_n5 = round(sum(adxs[:n]) / n, 3)

                eff_signal = self._compute_efficiency_signal(eff_8h, rvol_8h_n5)

                sr_label = self._legacy_sr_quality(
                    adx=self._safe_float(r.get("adx")),
                    rsi=self._safe_float(r.get("rsi")),
                    macd_line=self._safe_float(r.get("macd_line")),
                    macd_signal=self._safe_float(r.get("macd_signal")),
                    price=self._safe_float(r.get("price")),
                    vwap=self._safe_float(r.get("vwap")),
                )

                # closes lookback for MAX Dip proxy
                lb = int(self.lookback_var.get() or 50)
                lb = max(5, min(lb, len(closes)))
                closes_lb = closes[:lb] if closes else []

                out.append(
                    {
                        "symbol": sym,
                        "adx": r.get("adx"),
                        "rsi": r.get("rsi"),
                        "macd_line": r.get("macd_line"),
                        "macd_signal": r.get("macd_signal"),
                        "cnd": r.get("cnd"),
                        "price": r.get("price"),
                        "vwap": r.get("vwap"),
                        "volume": r.get("volume"),
                        "ts_raw": r.get("ts_raw"),
                        "eff_8h": eff_8h,
                        "rvol_8h_n5": rvol_8h_n5,
                        "adx8h_n5": adx8h_n5,
                        "eff_signal": eff_signal,
                        "rvol_4h_n14": rvol_4h_n14,
                        "rvol_4h_n11": rvol_4h_n11,
                        "rvol_4h_n6": rvol_4h_n6,
                        "sr_signal_quality": sr_label,
                        "closes_lb": closes_lb,
                    }
                )

            return out

    def _fetch_symbol_history(
        self,
        conn: sqlite3.Connection,
        table: str,
        sym_col: str,
        ts_col: str,
        symbol: str,
        limit: int,
        cols_needed: Sequence[str],
        ts_is_numeric: bool,
    ) -> List[sqlite3.Row]:
        select_cols = [c for c in cols_needed if c]
        select_cols = list(dict.fromkeys(select_cols))
        if not select_cols:
            select_cols = ["rowid"]

        if ts_col:
            if ts_is_numeric:
                q = f"""
                SELECT {", ".join(select_cols)}
                FROM '{table}'
                WHERE {sym_col} = ?
                ORDER BY CAST({ts_col} AS INTEGER) DESC, rowid DESC
                LIMIT ?
                """
            else:
                q = f"""
                SELECT {", ".join(select_cols)}
                FROM '{table}'
                WHERE {sym_col} = ?
                ORDER BY {ts_col} DESC, rowid DESC
                LIMIT ?
                """
        else:
            q = f"""
            SELECT {", ".join(select_cols)}
            FROM '{table}'
            WHERE {sym_col} = ?
            ORDER BY rowid DESC
            LIMIT ?
            """
        return conn.execute(q, (symbol, limit)).fetchall()

    # ---------------- BKL lifecycle (Excel-aligned) ----------------
    def _bkl_update_and_get(self, db_path: str, active_symbols: List[str]) -> Dict[str, int]:
        """Update lifecycle table for the current run and return BKL minutes per active symbol.

        Behavior (matches the Excel intent):
          - When a symbol first appears in the current list, it starts at BKL=0.
          - As long as it stays in the list across refreshes/restarts, BKL increments (minutes).
          - If it drops out of the list, its lifecycle row is removed. If it reappears later, it resets to 0.
          - Values are clamped to BKL_MAX_MINUTES.
        """
        now_ms = int(time.time() * 1000)
        # Normalize keys so persistence is stable across UI/DB symbol formatting.
        active_symbols = [_norm_symbol(s) for s in active_symbols]
        active_symbols = [s for s in active_symbols if s]

        with sqlite3.connect(db_path) as conn:
            self._ensure_bkl_table(conn)

            # Remove symbols not active anymore
            if active_symbols:
                placeholders = ",".join(["?"] * len(active_symbols))
                conn.execute(f"DELETE FROM signal_lifecycle WHERE symbol NOT IN ({placeholders})", active_symbols)
            else:
                conn.execute("DELETE FROM signal_lifecycle")

            # Upsert active symbols
            for sym in active_symbols:
                conn.execute(
                    """
                    INSERT INTO signal_lifecycle(symbol, first_seen_ts, last_seen_ts)
                    VALUES (?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET last_seen_ts=excluded.last_seen_ts
                    """,
                    (sym, now_ms, now_ms),
                )

            conn.commit()

            # Read back first_seen_ts to compute BKL minutes
            bkl_map: Dict[str, int] = {}
            if active_symbols:
                placeholders = ",".join(["?"] * len(active_symbols))
                rows = conn.execute(
                    f"SELECT symbol, first_seen_ts FROM signal_lifecycle WHERE symbol IN ({placeholders})",
                    active_symbols,
                ).fetchall()
                for sym, first_seen_ts in rows:
                    try:
                        age_min = int(max(0, (now_ms - int(first_seen_ts)) // 60000))
                    except Exception:
                        age_min = 0
                    if age_min > BKL_MAX_MINUTES:
                        age_min = BKL_MAX_MINUTES
                    bkl_map[str(sym)] = age_min

            return bkl_map

    @staticmethod
    def _ensure_bkl_table(conn: sqlite3.Connection):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_lifecycle (
                symbol TEXT PRIMARY KEY,
                first_seen_ts INTEGER NOT NULL,
                last_seen_ts INTEGER NOT NULL
            )
            """
        )
        conn.commit()

    # ---------------- Schema Wizard ----------------
    def _schema_wizard(self):
        db = self.db_path_var.get().strip()
        if not db:
            messagebox.showerror("Schema Wizard", "DB path is empty.")
            return
        if not os.path.exists(db):
            messagebox.showerror("Schema Wizard", f"DB not found:\n{db}")
            return

        try:
            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                self._ensure_meta_table(conn)
                tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
                if not tables:
                    messagebox.showerror("Schema Wizard", "No tables found in DB.")
                    return

                table_cols: Dict[str, List[str]] = {}
                for t in tables:
                    cols = [c[1] for c in conn.execute(f"PRAGMA table_info('{t}')").fetchall()]
                    table_cols[t] = cols

                saved = self._load_signal_schema(conn)
                if not saved.get("table"):
                    saved = self._auto_default_schema(conn, set(tables))

        except Exception as e:
            messagebox.showerror("Schema Wizard", f"Failed to read schema:\n{e}")
            return

        win = tk.Toplevel(self)
        win.title("Analysis Schema Wizard")
        win.geometry("760x600")
        win.transient(self.winfo_toplevel())

        frm = ttk.Frame(win, padding=10)
        frm.grid(row=0, column=0, sticky="nsew")
        win.grid_rowconfigure(0, weight=1)
        win.grid_columnconfigure(0, weight=1)

        ttk.Label(frm, text="Table:").grid(row=0, column=0, sticky="w")
        table_var = tk.StringVar(value=saved.get("table") or ("marketpredictor" if "marketpredictor" in table_cols else tables[0]))
        ttk.Combobox(frm, textvariable=table_var, values=tables, state="readonly", width=45).grid(row=0, column=1, sticky="w", padx=(6, 0))

        vars_by_key: Dict[str, tk.StringVar] = {}
        combos_by_key: Dict[str, ttk.Combobox] = {}

        def _col_options_for(table_name: str) -> List[str]:
            cols = table_cols.get(table_name, [])
            return [""] + cols

        def _guess_default(key: str, cols: List[str]) -> str:
            lc = [c.lower() for c in cols]

            def pick(names: List[str]) -> str:
                for n in names:
                    if n.lower() in lc:
                        return cols[lc.index(n.lower())]
                return ""

            if key == "timestamp":
                return pick(["timestamp", "time", "ts", "open_time", "datetime"])
            if key == "symbol":
                return pick(["symbol", "coin", "pair", "ticker"])
            if key == "price":
                return pick(["closeprice", "close", "lastprice", "price", "markprice", "currentprice"])
            if key == "volume":
                return pick(["volume", "quotevolume", "basevolume"])
            if key == "adx":
                v = pick(["adx_30m", "adx_15m", "adx"])
                if v:
                    return v
                for c in cols:
                    if "adx" in c.lower():
                        return c
                return ""
            if key == "rsi":
                return pick(["rsi", "rsi_14"])
            if key == "macd":
                return pick(["macdline", "macd", "macd_hist"])
            if key == "macd_signal":
                return pick(["macdsignal", "macd_signal"])
            if key == "vwap":
                return pick(["vwap", "vwapprice"])
            if key == "funding":
                return pick(["fundingrate", "funding", "funding_rate"])
            if key == "coin_ls":
                return pick(["longshortratio", "coinls", "coin_ls"])
            if key == "trader_ls":
                return pick(["toptraderratio", "traderls", "trader_ls"])
            if key == "di_plus":
                return pick(["dip", "di_plus", "di+"])
            if key == "di_minus":
                return pick(["dim", "di_minus", "di-"])
            if key == "oi":
                return pick(["openinterest", "oi"])
            if key == "cnd":
                return pick(["cndrating", "cnrating", "cnd"])
            return ""

        start_row = 1
        for idx, (label, key, _required) in enumerate(self._SCHEMA_FIELDS, start=start_row):
            ttk.Label(frm, text=label).grid(row=idx, column=0, sticky="w", pady=2)
            v = tk.StringVar(value="")
            vars_by_key[key] = v
            cmb = ttk.Combobox(frm, textvariable=v, values=_col_options_for(table_var.get()), state="readonly", width=45)
            cmb.grid(row=idx, column=1, sticky="w", padx=(6, 0), pady=2)
            combos_by_key[key] = cmb

        def _apply_defaults():
            cols = table_cols.get(table_var.get(), [])
            for _, key, _req in self._SCHEMA_FIELDS:
                if saved.get(key):
                    vars_by_key[key].set(saved[key])
                else:
                    vars_by_key[key].set(_guess_default(key, cols))

        def _on_table_change(*_):
            cols = _col_options_for(table_var.get())
            for key, cmb in combos_by_key.items():
                cmb.configure(values=cols)
                cur = vars_by_key[key].get()
                if cur and cur not in cols:
                    vars_by_key[key].set("")
            _apply_defaults()

        table_var.trace_add("write", _on_table_change)
        _apply_defaults()

        btns = ttk.Frame(frm)
        btns.grid(row=start_row + len(self._SCHEMA_FIELDS), column=0, columnspan=2, sticky="e", pady=(12, 0))

        def _save():
            missing = []
            for label, key, required in self._SCHEMA_FIELDS:
                if required and not vars_by_key[key].get().strip():
                    missing.append(label)
            if missing:
                messagebox.showerror("Schema Wizard", "Please map required fields:\n- " + "\n- ".join(missing), parent=win)
                return

            data: Dict[str, str] = {"table": table_var.get().strip()}
            for _label, key, _required in self._SCHEMA_FIELDS:
                val = vars_by_key[key].get().strip()
                if val:
                    data[key] = val

            if not data.get("adx"):
                messagebox.showerror("Schema Wizard", "Please map 'Adx' (used for ranking).", parent=win)
                return

            try:
                with sqlite3.connect(db) as conn2:
                    self._ensure_meta_table(conn2)
                    self._save_signal_schema(conn2, data)
                messagebox.showinfo("Schema Wizard", "Saved.", parent=win)
                win.destroy()
            except Exception as e:
                messagebox.showerror("Schema Wizard", f"Save failed:\n{e}", parent=win)

        ttk.Button(btns, text="Cancel", command=win.destroy).pack(side="right", padx=(0, 8))
        ttk.Button(btns, text="Save", command=_save).pack(side="right")

    # ---------------- schema persistence ----------------
    @staticmethod
    def _ensure_meta_table(conn: sqlite3.Connection):
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

    @staticmethod
    def _load_signal_schema(conn: sqlite3.Connection) -> Dict[str, str]:
        try:
            rows = conn.execute("SELECT key, value FROM meta WHERE key LIKE 'signal_schema.%'").fetchall()
        except sqlite3.OperationalError:
            return {}
        out: Dict[str, str] = {}
        for k, v in rows:
            if not isinstance(k, str):
                continue
            kk = k.split("signal_schema.", 1)[-1]
            out[kk] = "" if v is None else str(v)
        return out

    @staticmethod
    def _save_signal_schema(conn: sqlite3.Connection, data: Dict[str, str]):
        conn.execute("DELETE FROM meta WHERE key LIKE 'signal_schema.%'")
        for k, v in data.items():
            if not k:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                (f"signal_schema.{k}", "" if v is None else str(v)),
            )
        conn.commit()

    def _auto_default_schema(self, conn: sqlite3.Connection, tables: set) -> Dict[str, str]:
        preferred_table = "marketpredictor" if "marketpredictor" in tables else (sorted(list(tables))[0] if tables else "")
        cols = []
        if preferred_table:
            cols = [c[1] for c in conn.execute(f"PRAGMA table_info('{preferred_table}')").fetchall()]

        def pick(*names: str) -> str:
            lc = {c.lower(): c for c in cols}
            for n in names:
                if n.lower() in lc:
                    return lc[n.lower()]
            return ""

        data = {
            "table": preferred_table,
            "timestamp": pick("Timestamp", "time", "ts"),
            "symbol": pick("Symbol", "symbol", "Coin"),
            "price": pick("ClosePrice", "Close", "LastPrice", "CurrentPrice", "Price"),
            "volume": pick("Volume"),
            "adx": pick("ADX_30m", "ADX_15m", "ADX"),
            "rsi": pick("RSI"),
            "macd": pick("MACDLine", "MACD"),
            "macd_signal": pick("MACDSignal", "MACD_Signal"),
            "vwap": pick("VWAP"),
            "di_plus": pick("DIp"),
            "di_minus": pick("DIm"),
            "funding": pick("FundingRate"),
            "coin_ls": pick("LongShortRatio"),
            "trader_ls": pick("TopTraderRatio"),
            "oi": pick("OpenInterest"),
            "cnd": pick("CNDRating"),
        }
        return {k: v for k, v in data.items() if v}
