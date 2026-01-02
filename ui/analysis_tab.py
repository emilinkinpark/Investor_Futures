# ui/analysis_tab.py
#
# Analysis tab for Binance Futures Trader
# - Large Transactions, Volume Analysis, Chart Patterns, Key S/R, Liquidation Heatmaps
# - Composite Alpha (bullish and bearish)
# - ADX Combo
# - Price Breakout Filters
# - Strong Long/Short (Balanced Technical Model)
#
# Tabs removed (per user request):
# - Bullish/Bearish Suggestions
# - MACD Pattern Finder
# - Future Bias Meter

import os
import json
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import csv
import math
from collections import defaultdict, deque

from utils.db import connect
from utils.perf import timed

ADELAIDE_TZ = ZoneInfo("Australia/Adelaide")
SCHEMA_JSON = "analysis_schema.json"


# ---------------------------------------------------------------------------
# Helpers: schema & DB
# ---------------------------------------------------------------------------
def _norm(s: str) -> str:
    return s.strip().lower().replace(" ", "").replace("-", "_")


FIELD_ALIASES = {
    "timestamp": {
        "timestamp",
        "time",
        "ts",
        "datetime",
        "opentime",
        "closetime",
        "kline_open_time",
        "kline_close_time",
        "t",
    },
    "symbol": {"symbol", "pair", "ticker", "sym"},
    "price": {
        "currentprice",
        "current_price",
        "close",
        "price",
        "last",
        "c",
        "markprice",
        "lastprice",
        "closeprice",
        "avgprice",
    },
    "volume": {"volume", "vol", "quote_volume", "qvol", "v", "basevolume", "quotevolume"},
    "adx": {"adx"},
    "di_plus": {"di+", "di_plus", "diplus", "di_p", "plusdi"},
    "di_minus": {"di-", "di_minus", "diminus", "di_m", "minusdi"},
    "macd": {"macd", "macdline", "macd_line"},
    "vwap": {"vwap"},
    "oi": {"openinterest", "open_interest", "oi"},
    "liq": {"liq_density", "liquidation_density", "liquidations", "liqdensity"},
    "rsi": {"rsi", "rsi14", "rsi_14"},
    "funding": {"funding", "funding_rate", "fundingrate"},
    "coin_ls": {"coin_ls", "coin_longshort", "coin_long_short", "coin_longshort_ratio"},
    "trader_ls": {
        "trader_ls",
        "trader_longshort",
        "trader_long_short",
        "toptrader_longshort_ratio",
    },
}

OPTIONAL_FIELDS = [
    "volume",
    "adx",
    "di_plus",
    "di_minus",
    "macd",
    "vwap",
    "oi",
    "liq",
    "rsi",
    "funding",
    "coin_ls",
    "trader_ls",
]


def _load_saved_schema(db_path: str):
    try:
        if os.path.exists(SCHEMA_JSON):
            with open(SCHEMA_JSON, "r", encoding="utf-8") as f:
                all_cfg = json.load(f)
            return all_cfg.get(os.path.abspath(db_path))
    except Exception:
        pass
    return None


def _save_schema(db_path: str, table: str, cols_map: dict):
    try:
        all_cfg = {}
        if os.path.exists(SCHEMA_JSON):
            with open(SCHEMA_JSON, "r", encoding="utf-8") as f:
                all_cfg = json.load(f)
        all_cfg[os.path.abspath(db_path)] = {"table": table, "columns": cols_map}
        with open(SCHEMA_JSON, "w", encoding="utf-8") as f:
            json.dump(all_cfg, f, indent=2)
    except Exception:
        pass


def _best_guess_columns(all_cols):
    by_norm = {_norm(c): c for c in all_cols}
    out = {}
    for key, aliases in FIELD_ALIASES.items():
        for a in aliases:
            if a in by_norm:
                out[key] = by_norm[a]
                break
    if all(k in out for k in ("timestamp", "symbol", "price")):
        return out
    return None


def _detect_table_and_cols(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f"PRAGMA table_info('{t}')")
        cols = [r[1] for r in cur.fetchall()]
        guess = _best_guess_columns(cols)
        if guess:
            return t, guess
    return None, None


def _to_epoch_seconds(x):
    if isinstance(x, (int, float)):
        return x / 1000.0 if x > 10_000_000_000 else float(x)
    if isinstance(x, str):
        try:
            dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return float("nan")
    return float("nan")


def _fetch_rows_windowed(conn, table, cols, limit_per_symbol=300):
    """
    Pulls latest N rows per symbol using a window function.
    Includes all optional fields if present.
    """
    c = conn.cursor()
    q = f"""
        WITH ranked AS (
            SELECT
                "{cols['symbol']}" AS sym,
                "{cols['timestamp']}" AS ts,
                "{cols['price']}" AS px,
                {f'"{cols["volume"]}"' if cols.get("volume") else 'NULL'} AS vol,
                {f'"{cols["adx"]}"' if cols.get("adx") else 'NULL'} AS adx,
                {f'"{cols["di_plus"]}"' if cols.get("di_plus") else 'NULL'} AS di_p,
                {f'"{cols["di_minus"]}"' if cols.get("di_minus") else 'NULL'} AS di_m,
                {f'"{cols["macd"]}"' if cols.get("macd") else 'NULL'} AS macd,
                {f'"{cols["vwap"]}"' if cols.get("vwap") else 'NULL'} AS vwap,
                {f'"{cols["oi"]}"' if cols.get("oi") else 'NULL'} AS oi,
                {f'"{cols["liq"]}"' if cols.get("liq") else 'NULL'} AS liq,
                {f'"{cols["rsi"]}"' if cols.get("rsi") else 'NULL'} AS rsi,
                {f'"{cols["funding"]}"' if cols.get("funding") else 'NULL'} AS funding,
                {f'"{cols["coin_ls"]}"' if cols.get("coin_ls") else 'NULL'} AS coin_ls,
                {f'"{cols["trader_ls"]}"' if cols.get("trader_ls") else 'NULL'} AS trader_ls,
                ROW_NUMBER() OVER (
                    PARTITION BY "{cols['symbol']}"
                    ORDER BY "{cols['timestamp']}" DESC
                ) AS rn
            FROM "{table}"
        )
        SELECT sym, ts, px, vol, adx, di_p, di_m, macd, vwap, oi, liq, rsi, funding, coin_ls, trader_ls
        FROM ranked
        WHERE rn <= ?
        ORDER BY sym, ts ASC;
    """
    c.execute(q, (limit_per_symbol,))
    rows = c.fetchall()
    data = defaultdict(list)
    for r in rows:
        data[r[0]].append(r[1:])
    return dict(data)


# ---------------------------------------------------------------------------
# Math / analytics helpers
# ---------------------------------------------------------------------------
def _zscore_window(values, window=30):
    zs = []
    q = deque(maxlen=window)
    for v in values:
        q.append(v)
        if len(q) < 5:
            zs.append(float("nan"))
            continue
        m = sum(q) / len(q)
        var = sum((x - m) ** 2 for x in q) / len(q)
        sd = math.sqrt(var) if var > 0 else 0.0
        zs.append(0.0 if sd == 0 else (v - m) / sd)
    return zs


def _ema(values, span=9):
    if not values:
        return []
    k = 2.0 / (span + 1.0)
    out = []
    ema = None
    for v in values:
        if math.isnan(v):
            out.append(float("nan"))
            continue
        if ema is None:
            ema = v
        else:
            ema = (v * k) + (ema * (1.0 - k))
        out.append(ema)
    return out


def _compute_rsi(prices, period=14):
    out = []
    gains = deque(maxlen=period)
    losses = deque(maxlen=period)
    prev = None
    for p in prices:
        if prev is None:
            out.append(float("nan"))
            prev = p
            continue
        ch = p - prev
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
        prev = p
        if len(gains) < period:
            out.append(float("nan"))
            continue
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            out.append(100.0)
        else:
            rs = avg_gain / avg_loss
            out.append(100.0 - (100.0 / (1.0 + rs)))
    return out


def _tight_range_score(prices, lookback=50):
    if len(prices) < lookback:
        return float("nan")
    window = prices[-lookback:]
    pmax = max(window)
    pmin = min(window)
    mid = sum(window) / len(window)
    if mid <= 0:
        return float("nan")
    rng = (pmax - pmin) / mid
    return -rng  # tighter → higher score


def _max_drawup_breakout(prices, lookback=50):
    if len(prices) < lookback + 1:
        return float("nan")
    recent_max = max(prices[-lookback - 1 : -1])
    if recent_max == 0:
        return float("nan")
    return (prices[-1] - recent_max) / recent_max


def _max_drawdown_breakout(prices, lookback=50):
    if len(prices) < lookback + 1:
        return float("nan")
    recent_min = min(prices[-lookback - 1 : -1])
    if recent_min == 0:
        return float("nan")
    return (recent_min - prices[-1]) / recent_min


def _support_resistance_break(prices, volumes, adx=None, lookback=50):
    brk = _max_drawup_breakout(prices, lookback=lookback)
    if math.isnan(brk):
        return float("nan")
    vol_z = _zscore_window([v or 0.0 for v in volumes], window=30)
    vol_boost = vol_z[-1] if vol_z and not math.isnan(vol_z[-1]) else 0.0
    adx_boost = 0.0
    if adx and len(adx) >= 5 and not math.isnan(adx[-1]):
        adx_boost = adx[-1] / 100.0
    return brk + 0.25 * vol_boost + 0.25 * adx_boost


def _support_breakdown(prices, volumes, adx=None, lookback=50):
    brk = _max_drawdown_breakout(prices, lookback=lookback)
    if math.isnan(brk):
        return float("nan")
    vol_z = _zscore_window([v or 0.0 for v in volumes], window=30)
    vol_boost = vol_z[-1] if vol_z and not math.isnan(vol_z[-1]) else 0.0
    adx_boost = 0.0
    if adx and len(adx) >= 5 and not math.isnan(adx[-1]):
        adx_boost = adx[-1] / 100.0
    return brk + 0.25 * vol_boost + 0.25 * adx_boost


def _volume_spike_score(prices, volumes):
    if len(prices) < 3 or len(volumes) < 3:
        return float("nan")
    prev = prices[-2] or 1.0
    move = abs(prices[-1] - prev) / prev
    vz = _zscore_window([v or 0.0 for v in volumes], window=30)
    z = vz[-1] if vz and not math.isnan(vz[-1]) else 0.0
    return move * (1.0 + max(0.0, z))


def _volume_spike_bearish(prices, volumes):
    if len(prices) < 3 or len(volumes) < 3:
        return float("nan")
    prev = prices[-2] or 1.0
    move_down = max(0.0, (prev - prices[-1]) / prev)
    vz = _zscore_window([v or 0.0 for v in volumes], window=30)
    z = vz[-1] if vz and not math.isnan(vz[-1]) else 0.0
    return move_down * (1.0 + max(0.0, z))


def _large_tx_proxy(volumes, oi=None):
    vz = _zscore_window([v or 0.0 for v in volumes], window=30)
    score = vz[-1] if vz and not math.isnan(vz[-1]) else float("nan")
    if oi and len(oi) >= 2 and not math.isnan(oi[-1]) and not math.isnan(oi[-2]) and abs(oi[-2]) > 0:
        d_oi = (oi[-1] - oi[-2]) / abs(oi[-2])
        score += 0.1 * d_oi
    return score


# ---------------------------------------------------------------------------
# UI class
# ---------------------------------------------------------------------------
class AnalysisTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)

        # ---------------- Top bar ----------------
        top = ttk.Frame(self)
        top.pack(fill="x", padx=8, pady=6)

        ttk.Label(top, text="DB:").pack(side="left")
        self.db_path_var = tk.StringVar(value=os.path.abspath("marketpredictor.db"))
        ttk.Entry(top, textvariable=self.db_path_var, width=50).pack(side="left", padx=4)
        ttk.Button(top, text="Browse…", command=self._pick_db).pack(side="left")

        ttk.Button(top, text="Schema Wizard", command=self._open_schema_wizard).pack(side="left", padx=(8, 0))

        ttk.Label(top, text="Bars/symbol:").pack(side="left", padx=(12, 2))
        self.bars_var = tk.IntVar(value=500)
        ttk.Spinbox(top, from_=100, to=5000, increment=100, textvariable=self.bars_var, width=6).pack(side="left")

        ttk.Label(top, text="Lookback:").pack(side="left", padx=(12, 2))
        self.lookback_var = tk.IntVar(value=50)
        ttk.Spinbox(top, from_=10, to=300, increment=5, textvariable=self.lookback_var, width=6).pack(side="left")

        self.auto_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            top, text="Auto-refresh (s):", variable=self.auto_var, command=self._on_auto_toggle
        ).pack(side="left", padx=(12, 2))
        self.auto_secs_var = tk.IntVar(value=10)
        self.auto_secs_spin = ttk.Spinbox(
            top, from_=2, to=3600, increment=1, textvariable=self.auto_secs_var, width=6
        )
        self.auto_secs_spin.pack(side="left")

        ttk.Button(top, text="Run Now", command=self.run_analysis).pack(side="left", padx=(12, 4))

        ttk.Button(top, text="Export ALL CSV", command=self._export_all_csv).pack(side="left")

        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(top, textvariable=self.status_var).pack(side="right", padx=(0, 4))

        self.last_run_var = tk.StringVar(value="Last run: —")
        ttk.Label(top, textvariable=self.last_run_var).pack(side="right", padx=(0, 8))

        # ---------------- Notebook ----------------
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=8, pady=6)

        self.views = {}
        self.legend_frames = {}
        self.results_cache = defaultdict(list)
        self._color_tags = set()
        self._schema = None
        self._last_raw = None
        self._auto_after_id = None
        # NEW: prevent multiple overlapping analysis runs
        self._analysis_running = False

        # Tabs
        tab_names = [
            "Large Transactions (Whale Watching)",
            "Volume Analysis",
            "Chart Patterns",
            "Key Support/Resistance",
            "Support/Resistance",
            "Liquidation Heatmaps",
            "Composite Alpha",
            "Composite Alpha (Bearish)",
            "ADX Combo",
            "Price Breakout Filters",
            "Strong Long/Short",
        ]

        for name in tab_names:
            frame = ttk.Frame(self.nb)
            self.nb.add(frame, text=name)

            # Special controls per tab
            if name == "Composite Alpha":
                self._build_composite_controls(frame, bearish=False)
            elif name == "Composite Alpha (Bearish)":
                self._build_composite_controls(frame, bearish=True)
            elif name == "ADX Combo":
                self._build_adx_controls(frame)
            elif name == "Price Breakout Filters":
                self._build_breakout_controls(frame)


            # Tree / Custom UI
            if name == "Support/Resistance":
                tree, vip_tree = self._build_support_resistance_ui(frame)
                self.views[name] = tree
                self.views["Support/Resistance (VIP)"] = vip_tree
            elif name == "Strong Long/Short":
                tree = self._make_tree(
                    frame,
                    cols=("rank", "symbol", "long_score", "short_score", "details"),
                    coldefs={
                        "rank": (40, "center"),
                        "symbol": (120, "w"),
                        "long_score": (90, "e"),
                        "short_score": (90, "e"),
                        "details": (800, "w"),
                    },
                )
                self.views[name] = tree
            else:
                tree = self._make_tree(frame)
                self.views[name] = tree

            # Legend
            leg = ttk.Frame(frame)
            leg.pack(fill="x", padx=6, pady=(0, 6))
            self.legend_frames[name] = leg
            self._populate_legend(name, leg)

            ttk.Button(frame, text="Export CSV", command=lambda c=name: self._export_csv(c)).pack(
                anchor="e", padx=6, pady=(0, 6)
            )

        self._set_status("Ready.")

    # -----------------------------------------------------------------------
    # Generic tree + legends
    # -----------------------------------------------------------------------
    def _make_tree(self, parent, cols=None, coldefs=None):
        if cols is None:
            cols = ("rank", "symbol", "score", "details")
            coldefs = {
                "rank": (40, "center"),
                "symbol": (120, "w"),
                "score": (110, "e"),
                "details": (800, "w"),
            }
        tree = ttk.Treeview(parent, columns=cols, show="headings", height=15)
        tree.pack(fill="both", expand=True, padx=6, pady=6)
        for c in cols:
            text = c.title().replace("_", " ")
            width, anchor = coldefs.get(c, (100, "w"))
            tree.heading(c, text=text)
            tree.column(c, width=width, anchor=anchor)
        return tree

    def _populate_legend(self, tab_name, leg_frame):
        for w in leg_frame.winfo_children():
            w.destroy()

        def add(line):
            ttk.Label(leg_frame, text=line, foreground="#555555").pack(anchor="w")

        if tab_name == "Large Transactions (Whale Watching)":
            add("Score: Volume z-score (30 bars) + 0.1 × ΔOI% (whale flow proxy).")
        elif tab_name == "Volume Analysis":
            add("Score: |ΔPrice| × Volume z-score (30 bars).")
        elif tab_name == "Chart Patterns":
            add("Score: 0.6×Tight range (tighter=better) + 0.4×Breakout vs prior max.")
        elif tab_name == "Key Support/Resistance":
            add("Score: Breakout strength + 0.25×Volume z + 0.25×(ADX/100).")
        elif tab_name == "Support/Resistance":
            add("Excel-style panel: RVOL(8H), Efficiency, VWAP position, 4H change, decay, composite rank.")
        elif tab_name == "Liquidation Heatmaps":
            add("Score: Latest liquidation density, if column available.")
        elif tab_name == "Composite Alpha":
            add("Score: Weighted rank blend of LargeTx, Volume, Patterns, and S/R (bullish).")
        elif tab_name == "Composite Alpha (Bearish)":
            add("Score: Weighted rank blend of LargeTx↓, Volume↓, Patterns↓, and Breakdown.")
        elif tab_name == "ADX Combo":
            add("Shows: Top 10 Max ADX, Top 10 Min ADX, and filtered ADX+RSI combos.")
        elif tab_name == "Price Breakout Filters":
            add("Filters: price breaking above/below recent swing with user % threshold.")
        elif tab_name == "Strong Long/Short":
            add("Balanced Technical Model (0–10):")
            add(" - ADX strength (0–2), DI+/DI− (0–2), RSI band (0–1),")
            add(" - MACD histogram wave (0–2), VWAP trend (0–1),")
            add(" - Breakout/Breakdown (0–1), Funding & L/S support (0–1).")
            add("Two sections: TOP STRONG LONG (score ≥5) and TOP STRONG SHORT (score ≥5).")

    # Color helpers for composite alpha heatmap
    def _score_to_color(self, score: float, bearish: bool = False) -> str:
        if not isinstance(score, (int, float)) or math.isnan(score):
            s = 0.0
        else:
            s = max(0.0, min(1.0, float(score)))
        if bearish:
            r = int(60 + 195 * s)
            g = int(255 * (1.0 - 0.7 * s))
            b = int(255 * (1.0 - 0.7 * s))
        else:
            r = int(255 * (1.0 - 0.7 * s))
            g = int(60 + 195 * s)
            b = int(255 * (1.0 - 0.7 * s))
        return f"#{r:02x}{g:02x}{b:02x}"

    def _tag_for_color(self, tree: ttk.Treeview, color_hex: str) -> str:
        tag = f"bg_{color_hex[1:]}"
        if tag not in self._color_tags:
            try:
                tree.tag_configure(tag, background=color_hex)
                self._color_tags.add(tag)
            except Exception:
                pass
        return tag

    # -----------------------------------------------------------------------
    # File / export / status
    # -----------------------------------------------------------------------
    def _pick_db(self):
        p = filedialog.askopenfilename(
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")]
        )
        if p:
            self.db_path_var.set(p)

    def _export_csv(self, category):
        rows = self.results_cache.get(category, [])
        if not rows:
            messagebox.showinfo("Export CSV", f"No rows to export for '{category}'.")
            return
        fn = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile=f"{category.replace(' ', '_').replace('/', '_').lower()}.csv",
        )
        if not fn:
            return
        with open(fn, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Rank", "Symbol", "Score/Long", "Short/Details"])
            for r in rows:
                w.writerow(r)
        messagebox.showinfo("Export CSV", f"Saved {fn}")

    def _export_all_csv(self):
        if not self.results_cache:
            messagebox.showinfo("Export CSV", "No analysis results to export yet.")
            return
        fn = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile="analysis_all_tabs.csv",
        )
        if not fn:
            return
        with open(fn, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Category", "Rank", "Symbol", "Score/Long", "Short/Details"])
            for cat, rows in self.results_cache.items():
                for r in rows:
                    w.writerow([cat, *r])
        messagebox.showinfo("Export CSV", f"Saved {fn}")

    def _set_last_run(self):
        now_local = datetime.now(ADELAIDE_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        self.last_run_var.set(f"Last run: {now_local}")

    def _set_status(self, txt):
        self.status_var.set(txt)

    # -----------------------------------------------------------------------
    # Auto-refresh
    # -----------------------------------------------------------------------
    def _on_auto_toggle(self):
        if self.auto_var.get():
            self._schedule_auto()
        else:
            self._cancel_auto()

    def _schedule_auto(self):
        self._cancel_auto()
        try:
            secs = int(self.auto_secs_var.get())
        except Exception:
            secs = 10
            self.auto_secs_var.set(10)
        secs = max(2, secs)
        self._auto_after_id = self.after(secs * 1000, self._auto_tick)

    def _cancel_auto(self):
        if self._auto_after_id is not None:
            try:
                self.after_cancel(self._auto_after_id)
            except Exception:
                pass
            self._auto_after_id = None

    def _auto_tick(self):
        self.run_analysis()
        if self.auto_var.get():
            self._schedule_auto()

    # -----------------------------------------------------------------------
    # Schema Wizard
    # -----------------------------------------------------------------------
    def _open_schema_wizard(self):
        path = self.db_path_var.get().strip()
        if not os.path.exists(path):
            messagebox.showerror("Schema Wizard", f"Database not found:\n{path}")
            return
        try:
            conn = connect(path)
        except Exception as e:
            messagebox.showerror("Schema Wizard", f"Failed to open DB:\n{e}")
            return

        win = tk.Toplevel(self)
        win.title("Analysis Schema Wizard")
        win.geometry("720x520")
        frm = ttk.Frame(win)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]

        ttk.Label(frm, text="Table:").grid(row=0, column=0, sticky="w")
        table_var = tk.StringVar(value=tables[0] if tables else "")
        table_cb = ttk.Combobox(frm, values=tables, textvariable=table_var, width=40, state="readonly")
        table_cb.grid(row=0, column=1, sticky="w", pady=4)

        field_keys = ["timestamp", "symbol", "price"] + OPTIONAL_FIELDS
        field_vars = {k: tk.StringVar() for k in field_keys}
        field_cbs = {}

        rowi = 1
        for label, key in [
            ("Timestamp*", "timestamp"),
            ("Symbol*", "symbol"),
            ("Price*", "price"),
        ]:
            ttk.Label(frm, text=label).grid(row=rowi, column=0, sticky="w", pady=2)
            cb = ttk.Combobox(frm, values=[], textvariable=field_vars[key], width=30, state="readonly")
            cb.grid(row=rowi, column=1, sticky="w", pady=2)
            field_cbs[key] = cb
            rowi += 1

        for key in OPTIONAL_FIELDS:
            ttk.Label(frm, text=key.replace("_", " ").title()).grid(row=rowi, column=0, sticky="w", pady=2)
            cb = ttk.Combobox(frm, values=[], textvariable=field_vars[key], width=30, state="readonly")
            cb.grid(row=rowi, column=1, sticky="w", pady=2)
            field_cbs[key] = cb
            rowi += 1

        def refresh_cols(_evt=None):
            tbl = table_var.get()
            if not tbl:
                return
            cur.execute(f"PRAGMA table_info('{tbl}')")
            cols = [r[1] for r in cur.fetchall()]
            for v in field_vars.values():
                v.set("")
            for cb in field_cbs.values():
                cb["values"] = cols
            guess = _best_guess_columns(cols)
            if guess:
                for k, v in guess.items():
                    if k in field_vars:
                        field_vars[k].set(v)

        table_cb.bind("<<ComboboxSelected>>", refresh_cols)
        refresh_cols()

        def save_and_close():
            tbl = table_var.get().strip()
            ts = field_vars["timestamp"].get().strip()
            sy = field_vars["symbol"].get().strip()
            pr = field_vars["price"].get().strip()
            if not (tbl and ts and sy and pr):
                messagebox.showerror(
                    "Schema Wizard",
                    "Please select Table, Timestamp, Symbol, and Price.",
                )
                return
            cols_map = {k: (field_vars[k].get().strip() or None) for k in field_vars}
            _save_schema(path, tbl, cols_map)
            self._schema = {"table": tbl, "columns": cols_map}
            self._set_status(f"Schema set: table={tbl}, ts={ts}, sym={sy}, price={pr}")
            win.destroy()

        btnrow = ttk.Frame(frm)
        btnrow.grid(row=rowi, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btnrow, text="Save", command=save_and_close).pack(side="right")
        ttk.Button(btnrow, text="Cancel", command=win.destroy).pack(side="right", padx=(0, 6))

        def on_close():
            try:
                conn.close()
            except Exception:
                pass
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", on_close)

    # -----------------------------------------------------------------------
    # Composite control frames
    # -----------------------------------------------------------------------
    def _build_composite_controls(self, frame, bearish=False):
        bar = ttk.Frame(frame)
        bar.pack(fill="x", padx=6, pady=(6, 0))
        if not bearish:
            self.w_large = tk.DoubleVar(value=1.0)
            self.w_volume = tk.DoubleVar(value=1.0)
            self.w_patterns = tk.DoubleVar(value=1.0)
            self.w_sr = tk.DoubleVar(value=1.0)
            for label, var in [
                ("LargeTx", self.w_large),
                ("Volume", self.w_volume),
                ("Patterns", self.w_patterns),
                ("S/R", self.w_sr),
            ]:
                col = ttk.Frame(bar)
                col.pack(side="left", padx=10)
                ttk.Label(col, text=label).pack(anchor="w")
                ttk.Scale(col, from_=0.0, to=2.0, orient="horizontal", variable=var, length=120).pack()
            ttk.Button(bar, text="Recompute", command=self._recompute_composite_only).pack(side="right", padx=6)
        else:
            self.w_large_b = tk.DoubleVar(value=1.0)
            self.w_volume_b = tk.DoubleVar(value=1.0)
            self.w_patterns_b = tk.DoubleVar(value=1.0)
            self.w_sr_b = tk.DoubleVar(value=1.0)
            for label, var in [
                ("LargeTx↓", self.w_large_b),
                ("Volume↓", self.w_volume_b),
                ("Patterns↓", self.w_patterns_b),
                ("Breakdown", self.w_sr_b),
            ]:
                col = ttk.Frame(bar)
                col.pack(side="left", padx=10)
                ttk.Label(col, text=label).pack(anchor="w")
                ttk.Scale(col, from_=0.0, to=2.0, orient="horizontal", variable=var, length=120).pack()
            ttk.Button(bar, text="Recompute", command=self._recompute_composite_bearish_only).pack(
                side="right", padx=6
            )

    def _build_adx_controls(self, frame):
        bar = ttk.Frame(frame)
        bar.pack(fill="x", padx=6, pady=(6, 0))
        ttk.Label(bar, text="RSI min").pack(side="left")
        self.adx_rsi_min = tk.DoubleVar(value=30.0)
        ttk.Spinbox(bar, from_=0, to=100, increment=1.0, width=6, textvariable=self.adx_rsi_min).pack(
            side="left", padx=4
        )
        ttk.Label(bar, text="RSI max").pack(side="left")
        self.adx_rsi_max = tk.DoubleVar(value=70.0)
        ttk.Spinbox(bar, from_=0, to=100, increment=1.0, width=6, textvariable=self.adx_rsi_max).pack(
            side="left", padx=4
        )
        ttk.Label(bar, text="ADX ≥").pack(side="left", padx=(12, 2))
        self.adx_min = tk.DoubleVar(value=20.0)
        ttk.Spinbox(bar, from_=0, to=100, increment=1.0, width=6, textvariable=self.adx_min).pack(
            side="left", padx=4
        )
        ttk.Button(bar, text="Run Filter", command=self._build_adx_combo_view).pack(side="left", padx=8)

    def _build_breakout_controls(self, frame):
        bar = ttk.Frame(frame)
        bar.pack(fill="x", padx=6, pady=(6, 0))
        ttk.Label(bar, text="Lookback bars:").pack(side="left")
        self.breakout_lookback = tk.IntVar(value=50)
        ttk.Spinbox(bar, from_=10, to=300, increment=5, width=6, textvariable=self.breakout_lookback).pack(
            side="left", padx=4
        )
        ttk.Label(bar, text="Breakout %:").pack(side="left", padx=(12, 2))
        self.breakout_pct = tk.DoubleVar(value=1.0)
        ttk.Spinbox(bar, from_=0.1, to=10.0, increment=0.1, width=6, textvariable=self.breakout_pct).pack(
            side="left", padx=4
        )
        ttk.Button(bar, text="Apply Filters", command=self._build_price_breakout_view).pack(
            side="left", padx=8
        )

    # -----------------------------------------------------------------------
    # Run analysis
    # -----------------------------------------------------------------------
    def run_analysis(self):
        # Prevent overlapping runs that can stall UI / spam DB
        if self._analysis_running:
            self._set_status("Analysis already running…")
            return
        self._analysis_running = True
        threading.Thread(target=self._run_analysis_worker, daemon=True).start()

    @timed("analysis_run", threshold_ms=200)
    def _run_analysis_worker(self):
        path = self.db_path_var.get().strip()
        bars = max(50, int(self.bars_var.get() or 500))
        lookback = max(10, int(self.lookback_var.get() or 50))
        self._set_status(f"Analyzing… bars={bars}, lookback={lookback}")

        if not os.path.exists(path):
            self._post_fail("Database not found.")
            self._analysis_running = False
            return

        try:
            conn = connect(path)
            try:
                # Extra performance PRAGMAs on top of connect() defaults
                conn.execute("PRAGMA synchronous=OFF;")
                conn.execute("PRAGMA temp_store=MEMORY;")
            except Exception:
                pass
        except Exception as e:
            self._post_fail(f"Failed to open DB: {e}")
            self._analysis_running = False
            return

        try:
            if not self._schema:
                saved = _load_saved_schema(path)
                if saved:
                    table, cols = saved["table"], saved["columns"]
                else:
                    table, cols = _detect_table_and_cols(conn)
            else:
                table, cols = self._schema["table"], self._schema["columns"]

            if not (table and cols and cols.get("timestamp") and cols.get("symbol") and cols.get("price")):
                self._post_fail("No suitable table with timestamp/symbol/price. Use Schema Wizard.")
                return

            raw = _fetch_rows_windowed(conn, table, cols, limit_per_symbol=bars)
            if not raw:
                self._post_fail("No rows read from DB.")
                return

            per_sym = {}
            for sym, rows in raw.items():
                ts = [_to_epoch_seconds(r[0]) for r in rows]
                px = [float(r[1]) if r[1] is not None else float("nan") for r in rows]
                vol = [float(r[2]) if r[2] is not None else 0.0 for r in rows]
                adx = [float(r[3]) if r[3] is not None else float("nan") for r in rows]
                dip = [float(r[4]) if r[4] is not None else float("nan") for r in rows]
                dim = [float(r[5]) if r[5] is not None else float("nan") for r in rows]
                macd = [float(r[6]) if r[6] is not None else float("nan") for r in rows]
                vwap = [float(r[7]) if r[7] is not None else float("nan") for r in rows]
                oi = [float(r[8]) if r[8] is not None else float("nan") for r in rows]
                liq = [float(r[9]) if r[9] is not None else float("nan") for r in rows]
                rsi = [float(r[10]) if r[10] is not None else float("nan") for r in rows]
                funding = [float(r[11]) if r[11] is not None else float("nan") for r in rows]
                coin_ls = [float(r[12]) if r[12] is not None else float("nan") for r in rows]
                trader_ls = [float(r[13]) if r[13] is not None else float("nan") for r in rows]

                if all(math.isnan(x) for x in rsi):
                    rsi = _compute_rsi(px, period=14)

                per_sym[sym] = dict(
                    ts=ts,
                    px=px,
                    vol=vol,
                    adx=adx,
                    dip=dip,
                    dim=dim,
                    macd=macd,
                    vwap=vwap,
                    oi=oi,
                    liq=liq,
                    rsi=rsi,
                    funding=funding,
                    coin_ls=coin_ls,
                    trader_ls=trader_ls,
                )

            results = {}

            # 1) Large Transactions
            large_tx = []
            for s, d in per_sym.items():
                sc = _large_tx_proxy(d["vol"], d["oi"])
                if sc is None or math.isnan(sc):
                    continue
                note = f"vol_z≈{sc:.2f}"
                if len(d["oi"]) >= 2 and not math.isnan(d["oi"][-1]) and not math.isnan(d["oi"][-2]):
                    d_oi = d["oi"][-1] - d["oi"][-2]
                    note += f"; ΔOI={d_oi:.2f}"
                large_tx.append((s, sc, note))
            large_tx.sort(key=lambda x: x[1], reverse=True)
            results["Large Transactions (Whale Watching)"] = large_tx[:10]

            # 2) Volume Analysis
            vol_res = []
            for s, d in per_sym.items():
                sc = _volume_spike_score(d["px"], d["vol"])
                if sc is None or math.isnan(sc):
                    continue
                move_pct = 0.0
                if len(d["px"]) >= 2 and d["px"][-2]:
                    move_pct = 100.0 * (d["px"][-1] - d["px"][-2]) / d["px"][-2]
                note = f"ΔP≈{move_pct:.2f}% with spike"
                vol_res.append((s, sc, note))
            vol_res.sort(key=lambda x: x[1], reverse=True)
            results["Volume Analysis"] = vol_res[:10]

            # 3) Chart Patterns
            lookback = max(10, int(self.lookback_var.get() or 50))
            patt = []
            for s, d in per_sym.items():
                tight = _tight_range_score(d["px"], lookback=lookback)
                brk = _max_drawup_breakout(d["px"], lookback=lookback)
                if math.isnan(tight) or math.isnan(brk):
                    continue
                sc = tight * 0.6 + brk * 0.4
                note = f"tight={tight:.4f}, breakout={brk:.4f}"
                patt.append((s, sc, note))
            patt.sort(key=lambda x: x[1], reverse=True)
            results["Chart Patterns"] = patt[:10]

            # 4) Key Support/Resistance
            sr = []
            for s, d in per_sym.items():
                sc = _support_resistance_break(d["px"], d["vol"], adx=d["adx"], lookback=lookback)
                if sc is None or math.isnan(sc):
                    continue
                note = "price > prior max with vol/ADX boost" if sc > 0 else "—"
                sr.append((s, sc, note))
            sr.sort(key=lambda x: x[1], reverse=True)
            results["Key Support/Resistance"] = sr[:10]

            # 5) Liquidation Heatmaps
            liq_rows = []
            have_liq = any(not math.isnan(x) for d in per_sym.values() for x in d["liq"])
            if have_liq:
                for s, d in per_sym.items():
                    val = d["liq"][-1] if d["liq"] else float("nan")
                    if math.isnan(val):
                        continue
                    liq_rows.append((s, val, "higher = more cluster density"))
                liq_rows.sort(key=lambda x: x[1], reverse=True)
                results["Liquidation Heatmaps"] = liq_rows[:10]
            else:
                results["Liquidation Heatmaps"] = [("—", float("nan"), "No liquidation density column in DB.")]

            # 6) Composite Alpha (bullish)
            comp_bull = self._compute_composite(results)
            results["Composite Alpha"] = comp_bull[:10] if comp_bull else [
                ("—", float("nan"), "Adjust weights or run analysis.")
            ]

            # 7) Composite Alpha (Bearish)
            bear_lists = self._build_bearish_lists(per_sym, lookback)
            comp_bear = self._compute_composite_bearish(bear_lists)
            results["Composite Alpha (Bearish)"] = comp_bear[:10] if comp_bear else [
                ("—", float("nan"), "Adjust weights or run analysis.")
            ]

            self._last_raw = (per_sym, results)
            self._post_results(results)
            # Dependent views that use per_sym directly:
            self._build_adx_combo_view()
            self._build_price_breakout_view()
            self._build_strong_ls_view()
            self._build_support_resistance_view()

            self._set_status(f"OK: table={table}")
        except Exception as e:
            self._post_fail(f"Analysis failed: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
            # mark run finished
            self._analysis_running = False

    def _post_results(self, results):
        def _apply():
            for tree in self.views.values():
                for i in tree.get_children():
                    tree.delete(i)
            self.results_cache.clear()

            for cat, rows in results.items():
                tree = self.views.get(cat)
                if not tree:
                    continue
                if not rows:
                    tree.insert("", "end", values=("-", "-", "-", "No results"))
                    continue
                for i, (sym, score, note) in enumerate(rows, start=1):
                    if not isinstance(score, (int, float)) or math.isnan(score):
                        sc_str = "—"
                    else:
                        sc_str = f"{score:.4f}" if abs(score) >= 1e-4 else f"{score:.6g}"
                    vals = (i, sym, sc_str, note)
                    item = tree.insert("", "end", values=vals)
                    if cat in ("Composite Alpha", "Composite Alpha (Bearish)") and isinstance(
                        score, (int, float)
                    ) and not math.isnan(score):
                        col = self._score_to_color(score, bearish=(cat == "Composite Alpha (Bearish)"))
                        tag = self._tag_for_color(tree, col)
                        tree.item(item, tags=(tag,))
                    self.results_cache[cat].append(vals)

            self._set_last_run()
        self.after(0, _apply)

    def _post_fail(self, msg):
        def _apply():
            for tree in self.views.values():
                for i in tree.get_children():
                    tree.delete(i)
                tree.insert("", "end", values=("-", "-", "-", msg))
            self._set_last_run()
            self._set_status("Error.")
        self.after(0, _apply)

    # -----------------------------------------------------------------------
    # Composite Alpha scoring
    # -----------------------------------------------------------------------
    def _compute_composite(self, results):
        picks = {
            "Large Transactions (Whale Watching)": float(getattr(self, "w_large", tk.DoubleVar(value=1.0)).get()),
            "Volume Analysis": float(getattr(self, "w_volume", tk.DoubleVar(value=1.0)).get()),
            "Chart Patterns": float(getattr(self, "w_patterns", tk.DoubleVar(value=1.0)).get()),
            "Key Support/Resistance": float(getattr(self, "w_sr", tk.DoubleVar(value=1.0)).get()),
        }

        rank_maps = {}
        for cat, w in picks.items():
            rows = results.get(cat, [])
            rows = [r for r in rows if isinstance(r[1], (int, float)) and not math.isnan(r[1])]
            if not rows or w <= 0:
                continue
            rows_sorted = sorted(rows, key=lambda x: x[1], reverse=True)
            n = len(rows_sorted)
            if n <= 1:
                rp = {rows_sorted[0][0]: 1.0}
            else:
                rp = {sym: 1.0 - (i / (n - 1)) for i, (sym, _sc, _note) in enumerate(rows_sorted)}
            rank_maps[cat] = (rp, w)

        combined = defaultdict(float)
        total_w = sum(w for (_rp, w) in rank_maps.values()) or 1.0
        for (_cat, (rp, w)) in rank_maps.items():
            for sym, val in rp.items():
                combined[sym] += w * val

        scored = [(sym, sc / total_w, "weighted rank blend") for sym, sc in combined.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _build_bearish_lists(self, per_sym, lookback):
        bear_large = []
        bear_vol = []
        bear_patterns = []
        bear_break = []

        for s, d in per_sym.items():
            sc_l = _large_tx_proxy(d["vol"], d["oi"])
            if sc_l is not None and not math.isnan(sc_l):
                bear_large.append((s, sc_l, "whale flow (direction neutral)"))

            sc_v = _volume_spike_bearish(d["px"], d["vol"])
            if sc_v is not None and not math.isnan(sc_v):
                bear_vol.append((s, sc_v, "down move with spike"))

            tight = _tight_range_score(d["px"], lookback=lookback)
            brkd = _max_drawdown_breakout(d["px"], lookback=lookback)
            if not (math.isnan(tight) or math.isnan(brkd)):
                bear_patterns.append(
                    (s, (tight * 0.6 + brkd * 0.4), f"tight={tight:.4f}, breakdown={brkd:.4f}")
                )

            sc_b = _support_breakdown(d["px"], d["vol"], adx=d["adx"], lookback=lookback)
            if sc_b is not None and not math.isnan(sc_b):
                bear_break.append((s, sc_b, "price < prior min with vol/ADX boost"))

        for lst_name, lst in [
            ("LargeTx↓", bear_large),
            ("Volume↓", bear_vol),
            ("Patterns↓", bear_patterns),
            ("Breakdown", bear_break),
        ]:
            lst.sort(key=lambda x: x[1], reverse=True)

        return {
            "LargeTx↓": bear_large,
            "Volume↓": bear_vol,
            "Patterns↓": bear_patterns,
            "Breakdown": bear_break,
        }

    def _compute_composite_bearish(self, bear_lists):
        picks = {
            "LargeTx↓": float(getattr(self, "w_large_b", tk.DoubleVar(value=1.0)).get()),
            "Volume↓": float(getattr(self, "w_volume_b", tk.DoubleVar(value=1.0)).get()),
            "Patterns↓": float(getattr(self, "w_patterns_b", tk.DoubleVar(value=1.0)).get()),
            "Breakdown": float(getattr(self, "w_sr_b", tk.DoubleVar(value=1.0)).get()),
        }
        rank_maps = {}
        for cat, w in picks.items():
            rows = bear_lists.get(cat, [])
            rows = [r for r in rows if isinstance(r[1], (int, float)) and not math.isnan(r[1])]
            if not rows or w <= 0:
                continue
            rows_sorted = sorted(rows, key=lambda x: x[1], reverse=True)
            n = len(rows_sorted)
            if n <= 1:
                rp = {rows_sorted[0][0]: 1.0}
            else:
                rp = {sym: 1.0 - (i / (n - 1)) for i, (sym, _sc, _note) in enumerate(rows_sorted)}
            rank_maps[cat] = (rp, w)

        combined = defaultdict(float)
        total_w = sum(w for (_rp, w) in rank_maps.values()) or 1.0
        for (_cat, (rp, w)) in rank_maps.items():
            for sym, val in rp.items():
                combined[sym] += w * val

        scored = [(sym, sc / total_w, "weighted rank blend (bearish)") for sym, sc in combined.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _recompute_composite_only(self):
        if not self._last_raw:
            return
        _per_sym, results = self._last_raw
        comp_bull = self._compute_composite(results)
        self._refresh_single_composite("Composite Alpha", comp_bull, bearish=False)

    def _recompute_composite_bearish_only(self):
        if not self._last_raw:
            return
        per_sym, _results = self._last_raw
        lookback = max(10, int(self.lookback_var.get() or 50))
        bear_lists = self._build_bearish_lists(per_sym, lookback)
        comp_bear = self._compute_composite_bearish(bear_lists)
        self._refresh_single_composite("Composite Alpha (Bearish)", comp_bear, bearish=True)

    def _refresh_single_composite(self, tab_name, comp_rows, bearish=False):
        def _apply():
            tree = self.views.get(tab_name)
            if not tree:
                return
            for i in tree.get_children():
                tree.delete(i)
            self.results_cache[tab_name].clear()
            rows = comp_rows[:10] if comp_rows else []
            if not rows:
                tree.insert("", "end", values=("-", "-", "-", "No results"))
                return
            for i, (sym, score, note) in enumerate(rows, start=1):
                if not isinstance(score, (int, float)) or math.isnan(score):
                    sc_str = "—"
                else:
                    sc_str = f"{score:.4f}" if abs(score) >= 1e-4 else f"{score:.6g}"
                vals = (i, sym, sc_str, note)
                item = tree.insert("", "end", values=vals)
                if isinstance(score, (int, float)) and not math.isnan(score):
                    col = self._score_to_color(score, bearish=bearish)
                    tag = self._tag_for_color(tree, col)
                    tree.item(item, tags=(tag,))
                self.results_cache[tab_name].append(vals)

        self.after(0, _apply)

    # -----------------------------------------------------------------------
    # ADX Combo tab builder
    # -----------------------------------------------------------------------
    @timed("analysis_adx_combo", threshold_ms=50)
    def _build_adx_combo_view(self):
        if not self._last_raw:
            return
        per_sym, _ = self._last_raw
        tree = self.views.get("ADX Combo")
        if not tree:
            return

        for i in tree.get_children():
            tree.delete(i)
        self.results_cache["ADX Combo"].clear()

        latest = []
        for s, d in per_sym.items():
            a = d["adx"][-1] if d["adx"] else float("nan")
            r = d["rsi"][-1] if d["rsi"] else float("nan")
            if math.isnan(a):
                continue
            latest.append((s, a, r))

        top_max = sorted(latest, key=lambda x: x[1], reverse=True)[:10]
        top_min = sorted(latest, key=lambda x: x[1])[:10]

        tree.insert("", "end", values=("-", "-", "-", "[Top 10 Max ADX]"))
        for i, (s, a, r) in enumerate(top_max, start=1):
            note = f"ADX={a:.2f}; RSI={r:.2f}" if not math.isnan(r) else f"ADX={a:.2f}; RSI=—"
            tree.insert("", "end", values=(i, s, f"{a:.2f}", note))

        tree.insert("", "end", values=("-", "-", "-", ""))

        tree.insert("", "end", values=("-", "-", "-", "[Top 10 Min ADX]"))
        for i, (s, a, r) in enumerate(top_min, start=1):
            note = f"ADX={a:.2f}; RSI={r:.2f}" if not math.isnan(r) else f"ADX={a:.2f}; RSI=—"
            tree.insert("", "end", values=(i, s, f"{a:.2f}", note))

        tree.insert("", "end", values=("-", "-", "-", ""))

        rsi_min = float(self.adx_rsi_min.get() if hasattr(self, "adx_rsi_min") else 30.0)
        rsi_max = float(self.adx_rsi_max.get() if hasattr(self, "adx_rsi_max") else 70.0)
        adx_min = float(self.adx_min.get() if hasattr(self, "adx_min") else 20.0)

        tree.insert(
            "", "end", values=("-", "-", "-", f"[ADX & RSI Combo] ADX≥{adx_min:.0f}, RSI∈[{rsi_min:.0f},{rsi_max:.0f}]")
        )
        combo = []
        for s, a, r in latest:
            if math.isnan(a) or math.isnan(r):
                continue
            if a >= adx_min and rsi_min <= r <= rsi_max:
                combo.append((s, a, r))
        combo.sort(key=lambda x: x[1], reverse=True)
        for i, (s, a, r) in enumerate(combo[:20], start=1):
            tree.insert("", "end", values=(i, s, f"{a:.2f}", f"ADX={a:.2f}; RSI={r:.2f}"))

    # -----------------------------------------------------------------------
    # Price Breakout Filters tab builder
    # -----------------------------------------------------------------------
    @timed("analysis_breakout_filters", threshold_ms=50)
    def _build_price_breakout_view(self):
        if not self._last_raw:
            return
        per_sym, _ = self._last_raw
        tree = self.views.get("Price Breakout Filters")
        if not tree:
            return

        for i in tree.get_children():
            tree.delete(i)
        self.results_cache["Price Breakout Filters"].clear()

        lookback = max(10, int(self.breakout_lookback.get() if hasattr(self, "breakout_lookback") else 50))
        pct = float(self.breakout_pct.get() if hasattr(self, "breakout_pct") else 1.0)
        thr = pct / 100.0

        rows = []
        for s, d in per_sym.items():
            px = d["px"]
            if len(px) < lookback + 1:
                continue
            window = px[-lookback - 1 : -1]
            p_last = px[-1]
            hi = max(window)
            lo = min(window)
            if hi <= 0 or lo <= 0:
                continue

            bullish = False
            bearish = False
            bull_amt = 0.0
            bear_amt = 0.0
            if p_last >= hi * (1.0 + thr):
                bullish = True
                bull_amt = (p_last - hi) / hi
            if p_last <= lo * (1.0 - thr):
                bearish = True
                bear_amt = (lo - p_last) / lo

            if bullish:
                rows.append((s, "Bull Breakout", bull_amt, f"Last={p_last:.4g}, Hi={hi:.4g}, +{bull_amt*100:.2f}%"))
            if bearish:
                rows.append((s, "Bear Breakdown", bear_amt, f"Last={p_last:.4g}, Lo={lo:.4g}, -{bear_amt*100:.2f}%"))

        rows.sort(key=lambda x: x[2], reverse=True)
        if not rows:
            tree.insert("", "end", values=("-", "-", "-", f"No breakouts at {pct:.1f}% over {lookback} bars."))
            return

        for i, (sym, direction, amt, note) in enumerate(rows, start=1):
            sc_str = f"{amt*100:.2f}%"
            vals = (i, sym, sc_str, f"{direction}: {note}")
            tree.insert("", "end", values=vals)
            self.results_cache["Price Breakout Filters"].append(vals)

    # -----------------------------------------------------------------------
    # Strong Long/Short tab builder (Balanced Technical Model)
    # -----------------------------------------------------------------------
    def _score_long_short(self, d):
        """
        Balanced Technical Model:
        ADX strength (0–2)
        DI+/DI− (0–2)
        RSI band (0–1)
        MACD histogram wave (0–2)
        VWAP trend (0–1)
        Breakout/Breakdown (0–1)
        Funding & L/S support (0–1)
        """
        px = d["px"]
        adx = d["adx"]
        dip = d["dip"]
        dim = d["dim"]
        macd = d["macd"]
        vwap = d["vwap"]
        rsi = d["rsi"]
        funding = d["funding"]
        coin_ls = d["coin_ls"]
        trader_ls = d["trader_ls"]

        if not px:
            return 0.0, 0.0, "no data"

        p = px[-1]
        a = adx[-1] if adx else float("nan")
        di_p = dip[-1] if dip else float("nan")
        di_m = dim[-1] if dim else float("nan")
        r = rsi[-1] if rsi else float("nan")
        vwp = vwap[-1] if vwap else float("nan")
        fnd = funding[-1] if funding else float("nan")
        cls = coin_ls[-1] if coin_ls else float("nan")
        tls = trader_ls[-1] if trader_ls else float("nan")

        # MACD histogram & slope
        hist = []
        if macd and len(macd) >= 3:
            sig = _ema(macd, span=9)
            for m, s in zip(macd, sig):
                if math.isnan(m) or math.isnan(s):
                    hist.append(float("nan"))
                else:
                    hist.append(m - s)
        else:
            hist = [float("nan")] * len(macd)

        h = hist[-1] if hist else float("nan")
        h_prev = hist[-2] if len(hist) >= 2 else float("nan")
        h_prev2 = hist[-3] if len(hist) >= 3 else float("nan")

        # Breakout / breakdown
        lookback = max(10, int(self.lookback_var.get() or 50))
        brk_up = _max_drawup_breakout(px, lookback=lookback)
        brk_dn = _max_drawdown_breakout(px, lookback=lookback)

        # ADX strength (0–2)
        long_adx = 0.0
        short_adx = 0.0
        if not math.isnan(a):
            if a >= 30:
                long_adx = short_adx = 2.0
            elif a >= 20:
                long_adx = short_adx = 1.0

        # DI+ / DI− (0–2)
        long_di = 0.0
        short_di = 0.0
        if not (math.isnan(di_p) or math.isnan(di_m)):
            if di_p > di_m:
                long_di = 2.0
            elif di_m > di_p:
                short_di = 2.0

        # RSI band (0–1)
        long_rsi = 0.0
        short_rsi = 0.0
        if not math.isnan(r):
            if 40 <= r <= 65:
                long_rsi = 1.0
            if 35 <= r <= 60:
                # mild support to SHORT when RSI rolling over in mid band
                short_rsi = 0.5

        # MACD histogram wave (0–2)
        long_macd = 0.0
        short_macd = 0.0
        if not math.isnan(h):
            if h > 0:
                long_macd = 1.0
                if not (math.isnan(h_prev) or math.isnan(h_prev2)) and h > h_prev > h_prev2:
                    long_macd = 2.0
            elif h < 0:
                short_macd = 1.0
                if not (math.isnan(h_prev) or math.isnan(h_prev2)) and h < h_prev < h_prev2:
                    short_macd = 2.0

        # VWAP trend (0–1)
        long_vwap = 0.0
        short_vwap = 0.0
        if not (math.isnan(p) or math.isnan(vwp)):
            if p > vwp:
                long_vwap = 1.0
            elif p < vwp:
                short_vwap = 1.0

        # Breakout/Breakdown (0–1)
        long_brk = 0.0
        short_brk = 0.0
        if not math.isnan(brk_up) and brk_up > 0:
            long_brk = 1.0
        if not math.isnan(brk_dn) and brk_dn < 0:
            short_brk = 1.0

        # Funding & L/S support (0–1)
        long_fl = 0.0
        short_fl = 0.0
        if not math.isnan(fnd) or not math.isnan(cls) or not math.isnan(tls):
            if fnd >= 0 and ((not math.isnan(cls) and cls > 1.0) or (not math.isnan(tls) and tls > 1.0)):
                long_fl = 1.0
            if fnd <= 0 and ((not math.isnan(cls) and cls < 1.0) or (not math.isnan(tls) and tls < 1.0)):
                short_fl = 1.0

        long_score = long_adx + long_di + long_rsi + long_macd + long_vwap + long_brk + long_fl
        short_score = short_adx + short_di + short_rsi + short_macd + short_vwap + short_brk + short_fl

        # Details text
        parts = []
        parts.append(f"ADX={a:.2f}" if not math.isnan(a) else "ADX=—")
        parts.append(f"DI+={di_p:.2f}, DI-={di_m:.2f}" if not (math.isnan(di_p) or math.isnan(di_m)) else "DI=—")
        parts.append(f"RSI={r:.2f}" if not math.isnan(r) else "RSI=—")
        parts.append(f"VWAP={vwp:.4g}" if not math.isnan(vwp) else "VWAP=—")
        parts.append(f"Hist={h:.4g}" if not math.isnan(h) else "Hist=—")
        if not math.isnan(brk_up):
            parts.append(f"BrkUp={brk_up*100:.2f}%")
        if not math.isnan(brk_dn):
            parts.append(f"BrkDn={brk_dn*100:.2f}%")
        if not math.isnan(fnd):
            parts.append(f"Funding={fnd:.5f}")
        if not math.isnan(cls):
            parts.append(f"Coin L/S={cls:.2f}")
        if not math.isnan(tls):
            parts.append(f"Trader L/S={tls:.2f}")
        detail = "; ".join(parts)

        return long_score, short_score, detail

    @timed("analysis_strong_ls", threshold_ms=100)
    def _build_strong_ls_view(self):
        if not self._last_raw:
            return
        per_sym, _ = self._last_raw
        tree = self.views.get("Strong Long/Short")
        if not tree:
            return

        for i in tree.get_children():
            tree.delete(i)
        self.results_cache["Strong Long/Short"].clear()

        longs = []
        shorts = []

        for s, d in per_sym.items():
            long_sc, short_sc, detail = self._score_long_short(d)
            if long_sc >= 5.0:
                longs.append((s, long_sc, short_sc, detail))
            if short_sc >= 5.0:
                shorts.append((s, long_sc, short_sc, detail))

        longs.sort(key=lambda x: x[1], reverse=True)
        shorts.sort(key=lambda x: x[2], reverse=True)

        # TOP STRONG LONG
        tree.insert("", "end", values=("-", "=== STRONG LONG (score ≥5) ===", "-", "-", ""))
        if not longs:
            tree.insert("", "end", values=("-", "-", "-", "-", "No strong LONG setups."))
        else:
            for i, (sym, long_sc, short_sc, detail) in enumerate(longs, start=1):
                vals = (i, sym, f"{long_sc:.2f}", f"{short_sc:.2f}", detail)
                tree.insert("", "end", values=vals)
                self.results_cache["Strong Long/Short"].append(vals)

        tree.insert("", "end", values=("-", "", "", "", ""))

        # TOP STRONG SHORT
        tree.insert("", "end", values=("-", "=== STRONG SHORT (score ≥5) ===", "-", "-", ""))
        if not shorts:
            tree.insert("", "end", values=("-", "-", "-", "-", "No strong SHORT setups."))
        else:
            for i, (sym, long_sc, short_sc, detail) in enumerate(shorts, start=1):
                vals = (i, sym, f"{long_sc:.2f}", f"{short_sc:.2f}", detail)
                tree.insert("", "end", values=vals)
                self.results_cache["Strong Long/Short"].append(vals)


    # -----------------------------------------------------------------------
    # Support/Resistance (mimic Excel-style panel shown in screenshot)
    # -----------------------------------------------------------------------
    def _build_support_resistance_ui(self, frame: ttk.Frame):
        """
        Creates two stacked tables:
        1) SUPPORT & RESISTENCE : LEVEL [ LONG POSITION ]
        2) Total VIP Premium Signal (Buy Signal Status)
        Returns: (sr_tree, vip_tree)
        """
        container = ttk.Frame(frame)
        container.pack(fill="both", expand=True, padx=6, pady=6)

        # ---- Header 1 ----
        hdr1 = tk.Label(
            container,
            text="SUPPORT & RESISTENCE : LEVEL   [  LONG  POSITION  ]",
            bg="#0b2a5b",
            fg="#ff4d4d",
            font=("Segoe UI", 10, "bold"),
            anchor="center",
            pady=4,
        )
        hdr1.pack(fill="x")

        # Timestamp line (right-aligned like the sheet)
        self._sr_time_var = tk.StringVar(value="")
        ts_row = ttk.Frame(container)
        ts_row.pack(fill="x")
        ttk.Label(ts_row, textvariable=self._sr_time_var, foreground="#444").pack(side="right", padx=6, pady=2)

        cols = (
            "coin",
            "top_ranking",
            "signal_quality",
            "rvol_8h_n5",
            "efficiency_8h",
            "efficiency_signal",
            "vwap_pos",
            "rvol_4h_n14",
            "rvol_4h_n6",
            "volume_decay",
            "rank",
        )
        coldefs = {
            "coin": (120, "w"),
            "top_ranking": (90, "center"),
            "signal_quality": (110, "center"),
            "rvol_8h_n5": (95, "center"),
            "efficiency_8h": (95, "center"),
            "efficiency_signal": (120, "center"),
            "vwap_pos": (110, "center"),
            "rvol_4h_n14": (110, "center"),
            "rvol_4h_n6": (110, "center"),
            "volume_decay": (110, "center"),
            "rank": (70, "center"),
        }

        sr_tree = ttk.Treeview(container, columns=cols, show="headings", height=7)
        sr_tree.pack(fill="x", padx=0, pady=(4, 10))
        for c in cols:
            text = c.replace("_", " ").title()
            w, a = coldefs.get(c, (100, "w"))
            sr_tree.heading(c, text=text)
            sr_tree.column(c, width=w, anchor=a)

        # Tag styles (mimic key highlights)
        sr_tree.tag_configure("coin_cell", background="#d9f2d9")              # light green
        sr_tree.tag_configure("top1", background="#fff4a3")                  # light yellow
        sr_tree.tag_configure("top2", background="#fff4a3")
        sr_tree.tag_configure("hq", background="#ff4fd8")                    # magenta
        sr_tree.tag_configure("eff_hi", background="#fff4a3")                # yellow for Efficiency_Signal standout
        sr_tree.tag_configure("vwap_pos", background="#d9f2d9")              # green
        sr_tree.tag_configure("vwap_neg", background="#f6d7d7")              # light red

        # ---- Header 2 ----
        hdr2 = tk.Label(
            container,
            text="Total VIP Premium Signal  ( Buy Signal Status )",
            bg="#0b2a5b",
            fg="#ff4d4d",
            font=("Segoe UI", 10, "bold"),
            anchor="center",
            pady=4,
        )
        hdr2.pack(fill="x", pady=(6, 0))

        vip_cols = (
            "sno",
            "crypto_name",
            "signal_buy_status",
            "inv_index",
            "signal_quality",
            "quality_index",
            "rsi_value",
            "scd_rating_index",
            "bkl_time_zone",
            "max_dip",
            "total_profit",
            "result_time",
        )
        vip_defs = {
            "sno": (45, "center"),
            "crypto_name": (120, "w"),
            "signal_buy_status": (190, "w"),
            "inv_index": (90, "center"),
            "signal_quality": (120, "center"),
            "quality_index": (110, "center"),
            "rsi_value": (90, "center"),
            "scd_rating_index": (120, "center"),
            "bkl_time_zone": (110, "center"),
            "max_dip": (90, "center"),
            "total_profit": (95, "center"),
            "result_time": (95, "center"),
        }

        vip_tree = ttk.Treeview(container, columns=vip_cols, show="headings", height=7)
        vip_tree.pack(fill="both", expand=True, pady=(4, 0))
        for c in vip_cols:
            text = c.replace("_", " ").title()
            w, a = vip_defs.get(c, (100, "w"))
            vip_tree.heading(c, text=text)
            vip_tree.column(c, width=w, anchor=a)

        vip_tree.tag_configure("goodq", background="#d9f2d9")
        vip_tree.tag_configure("buy", background="#d9f2d9")
        vip_tree.tag_configure("warn", background="#fff4a3")

        # Keep references for population
        self._sr_tree = sr_tree
        self._vip_tree = vip_tree

        return sr_tree, vip_tree

    def _fmt_x(self, x: float) -> str:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "—"
        return f"{x:.1f}x"

    def _fmt_pct(self, x: float) -> str:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "—"
        return f"{x:+.2f}%"

    def _compute_sr_snapshot(self, per_sym: dict):
        """
        Produces rows for the Support/Resistance snapshot from per_sym series.
        This is a best-effort implementation to match the screenshot layout.
        Assumes per_sym contains: ts (epoch seconds), px, vol, vwap, rsi.
        """
        # NOTE: Windows strftime does not support "%-I" (no-leading-zero hour).
        # Build a portable 12-hour time string.
        _dt = datetime.now(ADELAIDE_TZ)
        _hh = _dt.strftime("%I").lstrip("0") or "0"
        now_local = f"{_hh}:{_dt.strftime('%M')} {_dt.strftime('%p')}"
        self._sr_time_var.set(f"=   {now_local}")

        # helper: window sum by seconds
        def sum_in_window(ts, vals, start_t):
            s = 0.0
            for t, v in zip(ts, vals):
                if t >= start_t:
                    s += (v or 0.0)
            return s

        def price_at_or_before(ts, px, target):
            # last price at/before target
            p = px[0] if px else float("nan")
            for t, v in zip(ts, px):
                if t <= target:
                    p = v
                else:
                    break
            return p

        rows = []
        for sym, d in per_sym.items():
            ts = d.get("ts") or []
            px = d.get("px") or []
            vol = d.get("vol") or []
            vwap = d.get("vwap") or []
            rsi = d.get("rsi") or []

            if not ts or not px:
                continue
            t_now = ts[-1]
            p_now = px[-1]
            vwap_now = vwap[-1] if vwap else float("nan")

            # 8H RVOL vs previous 5 x 8H windows
            w8 = 8 * 3600
            vol_8h = sum_in_window(ts, vol, t_now - w8)
            prev = []
            # previous 5 windows: [8h..16h], [16h..24h], ...
            for i in range(1, 6):
                end = t_now - (i * w8)
                start = end - w8
                prev.append(sum_in_window(ts, vol, start) - sum_in_window(ts, vol, end))
            base_8h = (sum(prev) / len(prev)) if prev and sum(prev) > 0 else float("nan")
            rvol_8h = (vol_8h / base_8h) if base_8h and not math.isnan(base_8h) and base_8h > 0 else float("nan")

            # bucket labels
            rvol_label = "Normal"
            if not math.isnan(rvol_8h):
                if rvol_8h >= 10:
                    rvol_label = "Top-1"
                elif rvol_8h >= 3:
                    rvol_label = "Top-2"

            # Efficiency_8H: |return| per normalized volume
            p_8h_ago = price_at_or_before(ts, px, t_now - w8)
            ret_8h = abs((p_now - p_8h_ago) / p_8h_ago) if p_8h_ago and not math.isnan(p_8h_ago) else float("nan")
            # normalize volume to avoid massive skew (log)
            eff_raw = (ret_8h / max(1e-9, math.log1p(vol_8h))) if not math.isnan(ret_8h) and vol_8h > 0 else float("nan")
            # scale to "x" feel
            eff_x = eff_raw * 50.0 if not math.isnan(eff_raw) else float("nan")

            # Efficiency_Signal: combine eff + rvol + VWAP alignment
            vwap_pos = "Positive" if (not math.isnan(vwap_now) and p_now >= vwap_now) else "Negative"
            vwap_gate = 1.0 if vwap_pos == "Positive" else 0.6
            eff_sig = (eff_x * (1.0 + (math.log1p(rvol_8h) if not math.isnan(rvol_8h) else 0.0)) * vwap_gate) if not math.isnan(eff_x) else float("nan")
            # compress to screenshot-like range
            if not math.isnan(eff_sig):
                eff_sig = max(0.0, min(2.5, eff_sig / 10.0))

            # 4H “N14” appears like % change in your screenshot; implement as 4H price change %
            w4 = 4 * 3600
            p_4h_ago = price_at_or_before(ts, px, t_now - w4)
            chg_4h = ((p_now - p_4h_ago) / p_4h_ago * 100.0) if p_4h_ago and not math.isnan(p_4h_ago) else float("nan")

            # 4H RVOL vs previous 6 windows
            vol_4h = sum_in_window(ts, vol, t_now - w4)
            prev4 = []
            for i in range(1, 7):
                end = t_now - (i * w4)
                start = end - w4
                prev4.append(sum_in_window(ts, vol, start) - sum_in_window(ts, vol, end))
            base_4h = (sum(prev4) / len(prev4)) if prev4 and sum(prev4) > 0 else float("nan")
            rvol_4h = (vol_4h / base_4h) if base_4h and not math.isnan(base_4h) and base_4h > 0 else float("nan")

            # Volume Decay: last 1H vs previous 1H
            w1 = 3600
            vol_1h = sum_in_window(ts, vol, t_now - w1)
            prev_1h = sum_in_window(ts, vol, t_now - 2*w1) - sum_in_window(ts, vol, t_now - w1)
            vdec = (vol_1h / prev_1h) if prev_1h and prev_1h > 0 else float("nan")

            # Rank: weighted composite (compressed to ~0.3x–1.5x)
            score = 0.0
            if not math.isnan(eff_sig):
                score += 0.55 * eff_sig
            if not math.isnan(rvol_8h):
                score += 0.25 * min(3.0, math.log1p(rvol_8h))
            if not math.isnan(rvol_4h):
                score += 0.15 * min(2.0, math.log1p(rvol_4h))
            if not math.isnan(vdec):
                score += 0.05 * min(2.0, vdec)
            rank_x = max(0.1, min(2.0, score))

            # Signal quality (sheet-like)
            sig_quality = "Normal"
            if vwap_pos == "Positive" and not math.isnan(eff_sig) and eff_sig >= 1.20:
                sig_quality = "High_Quality"

            rows.append(
                dict(
                    coin=sym,
                    rvol_label=rvol_label,
                    rvol_8h=rvol_8h,
                    eff_x=eff_x,
                    eff_sig=eff_sig,
                    vwap_pos=vwap_pos,
                    chg_4h=chg_4h,
                    rvol_4h=rvol_4h,
                    vdec=vdec,
                    rank_x=rank_x,
                    signal_quality=sig_quality,
                    rsi_last=(rsi[-1] if rsi else float("nan")),
                )
            )

        # Determine TOP_Ranking (Top-1/Top-2 labels) based on LONG eligibility
        elig = [r for r in rows if r["vwap_pos"] == "Positive"]
        # prioritize those with rvol_label Top buckets, then eff_sig
        elig.sort(key=lambda r: ((r["rvol_label"] != "Top-1"), (r["rvol_label"] != "Top-2"), -(r["eff_sig"] if not math.isnan(r["eff_sig"]) else -1e9)))
        top_syms = [r["coin"] for r in elig[:2]]

        # Assign TOP_Ranking output column to match screenshot semantics
        for r in rows:
            top_rank = "Normal"
            if r["coin"] in top_syms:
                top_rank = "Top-1" if top_syms and r["coin"] == top_syms[0] else "Top-2"
            r["top_ranking"] = top_rank

        # sort display: keep top at top, then by rank
        rows.sort(key=lambda r: (0 if r["top_ranking"] in ("Top-1", "Top-2") else 1, -(r["rank_x"] if not math.isnan(r["rank_x"]) else 0.0)))
        return rows

    @timed("analysis_support_resistance", threshold_ms=80)
    def _build_support_resistance_view(self):
        if not self._last_raw:
            return
        per_sym, _ = self._last_raw
        if not hasattr(self, "_sr_tree") or not hasattr(self, "_vip_tree"):
            return

        sr_tree = self._sr_tree
        vip_tree = self._vip_tree

        # Clear
        for i in sr_tree.get_children():
            sr_tree.delete(i)
        for i in vip_tree.get_children():
            vip_tree.delete(i)

        snap = self._compute_sr_snapshot(per_sym)

        # Populate SR table (top 6 to mimic screenshot)
        for i, r in enumerate(snap[:6], start=1):
            vals = (
                r["coin"],
                r["top_ranking"],
                r["signal_quality"],
                r["rvol_label"] if r["rvol_label"] in ("Top-1", "Top-2") else "Normal",
                self._fmt_x(r["eff_x"]),
                f"{r['eff_sig']:.2f}" if not math.isnan(r["eff_sig"]) else "—",
                r["vwap_pos"],
                self._fmt_pct(r["chg_4h"]),
                self._fmt_x(r["rvol_4h"]),
                self._fmt_x(r["vdec"]),
                self._fmt_x(r["rank_x"]),
            )

            tags = []
            tags.append("coin_cell")
            if r["top_ranking"] == "Top-1":
                tags.append("top1")
            elif r["top_ranking"] == "Top-2":
                tags.append("top2")
            if r["signal_quality"] == "High_Quality":
                tags.append("hq")
            if not math.isnan(r["eff_sig"]) and r["eff_sig"] >= 1.30:
                tags.append("eff_hi")
            tags.append("vwap_pos" if r["vwap_pos"] == "Positive" else "vwap_neg")

            sr_tree.insert("", "end", values=vals, tags=tuple(tags))

        # Populate VIP table (best-effort from available fields)
        now_local = datetime.now(ADELAIDE_TZ).strftime("%d %B %Y").upper()
        for idx, r in enumerate(snap[:6], start=1):
            sym = r["coin"]
            inv_index = 0.0
            if not math.isnan(r["rank_x"]):
                inv_index = max(0.0, min(5.0, r["rank_x"])) / 10.0

            rsi_val = r.get("rsi_last", float("nan"))
            sigq = "1CR_GoodSignal"
            if r["signal_quality"] == "High_Quality":
                sigq = "4CR_GoodSignal"

            qindex = "GoodQuality" if r["vwap_pos"] == "Positive" else "—"
            bkl = int(60 + 30 * (r["rank_x"] if not math.isnan(r["rank_x"]) else 0.0))

            vals = (
                idx,
                sym,
                f"Buy Signal ({sym})",
                f"{inv_index:.6g}",
                sigq,
                qindex,
                f"{rsi_val:.2f}" if not math.isnan(rsi_val) else "—",
                "",  # SCD Rating Index (not enough info in DB)
                f"{bkl}",
                "",  # MAX Dip
                "",  # Total Profit
                "",  # Result Time
            )
            tags = []
            if qindex == "GoodQuality":
                tags.append("goodq")
            tags.append("buy")
            vip_tree.insert("", "end", values=vals, tags=tuple(tags))

    # Hook S/R view build into analysis pipeline
