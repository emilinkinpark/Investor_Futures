# ui/auto_order_frame.py
"""
AutoOrder 2.7.1 — Multi-Rule + Re-entry Block + Rules Panel Toggle + Per-Rule SIDE
+ Advanced Condition Types (RSI/ADX slopes, MACD hist, VWAP, Swing High, ADX(1m/15m/30m)
  with DB support for 15m & 30m from marketpredictor.db, plus RSI / Coin L/S / Trader L/S
  from DB when Source=DB)
+ Status sub-tab: shows symbols matching 60%, 80%, 100% Running rules
+ Auto Mode – Matching (Selected Rules) panel
+ LIVE Binance Futures MARKET order placement using .env keys
+ EXCHANGE FILTERS (stepSize/tickSize) — precision-safe qty rounding per symbol

Fix:
- deque mutated during iteration -> we now take a safe snapshot of k1m bars
  before computing indicators (RSI, ADX, MACD, SPIKE, etc.).
- ADX(15m) & ADX(30m) now read only from marketpredictor.db when Source=DB.
- SQLite thread issue: marketpredictor.db is now opened per call with
  check_same_thread=False to avoid cross-thread usage of a single connection.
- Evaluation engine runs entirely off the Tk main thread, and TreeView updates
  are marshalled via `after(...)`.
- TOP 20 / TOP 20 (15m) / TOP 20 (1m) conditions:
    TOP 20          -> symbol must be in Top 20 list injected from Futures tab
    TOP 20 (15m)    -> symbol in Top 20 AND ADX(15m) >= threshold
    TOP 20 (1m)     -> symbol in Top 20 AND ADX(1m) >= threshold
- NEW: Scanner (exchangeInfo + WS streams) is now lazily started on first
  Auto/Manual run in a background thread, instead of blocking __init__.

Conditions supported (type column):
- PRICE ranges: V1 = min%, V2 = max% vs last close (from scanner)
- RSI, RSI(14), RSI Slope, MACD, MACD HIST, ADX(1m/15m/30m)
- FUNDING, COIN L/S, TRADER L/S
- PRICE SPIKE: compare recent close vs close N minutes ago
- CLOSE>VWAP: checks if price > VWAP (from scanner state)
- CLOSE>SWINGHIGH(10): checks if last close > max close of last 10 bars
- TOP 20: symbol must be in Futures Tab Top 20 list (via set_top20_symbols)
- TOP 20 (15m): symbol in Top 20 + ADX(15m) meets threshold
- TOP 20 (1m): symbol in Top 20 + ADX(1m) meets threshold

Notes:
- Uses AutoOrderScanner (kline stream + derived indicators) via queue events
- Auto engine scans symbols and evaluates rules every X seconds (interval)
- Can operate in MANUAL “Scan Now” or AUTO mode with autoscheduling

Requires:
- .env with:
    BINANCE_API_KEY=...
    BINANCE_API_SECRET=...
    BINANCE_BASE_URL=https://fapi.binance.com  (futures)
- Python deps:
    pip install websocket-client requests python-dotenv
"""

import os
import json
import time
import queue
import sqlite3
from utils.db import connect
import threading
from collections import deque, defaultdict
from datetime import datetime
import hmac
import hashlib
import math
from urllib.parse import urlencode

import tkinter as tk
from tkinter import ttk, messagebox

import requests
import websocket  # pip install websocket-client
from dotenv import load_dotenv  # pip install python-dotenv


# ----------------------------- Env / API Keys -----------------------------


load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")


# ----------------------------- Paths & DB -----------------------------


def _project_root():
    # project root one level up from ui/
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


AUTO_DB_PATH = os.path.join(_project_root(), "auto_order.db")
MARKET_DB_PATH = os.path.join(_project_root(), "marketpredictor.db")


# ----------------------------- Scanner State -----------------------------


class ScannerState:
    """
    In-memory state shared between AutoOrderScanner (producer) and
    AutoOrderFrame (consumer).

    k1m: symbol -> deque of last N bars (dict with open/high/low/close/ts)
    metrics: symbol -> dict of indicators:
        {
            "RSI": float,
            "RSI14": float,
            "ADX_1m": float,
            "MACD": float,
            "MACD_HIST": float,
            "PRICE_SPIKE": float,
            "LAST_CLOSE": float,
            "VWAP": float (optional),
            "SWINGHIGH10": float (optional),
            "FUNDING": float (optional),
            "COIN_LS": float (optional),
            "TRADER_LS": float (optional),
            "ADX_15m": float (optional),
            "ADX_30m": float (optional),
        }
    """

    def __init__(self, maxlen=300):
        self.k1m = {}
        self.metrics = {}
        self.maxlen = maxlen
        self._lock = threading.Lock()

    def update_kline(self, symbol: str, bar: dict):
        with self._lock:
            dq = self.k1m.get(symbol)
            if dq is None:
                dq = deque(maxlen=self.maxlen)
                self.k1m[symbol] = dq
            dq.append(bar)

    def update_metrics(self, symbol: str, metrics: dict):
        with self._lock:
            self.metrics[symbol] = metrics

    def get_snapshot(self):
        """
        Return a safe snapshot of metrics for evaluation:
        symbol -> metrics-dict
        """
        with self._lock:
            # no deep copy; just clone top-level dict & refs (we don't mutate)
            return dict(self.metrics)


# ----------------------------- AutoOrderScanner -----------------------------


class AutoOrderScanner(threading.Thread):
    """
    Background scanner that:
      - streams kline data via websocket
      - periodically updates ADX/RSI/MACD/PriceSpike metrics in ScannerState
      - sends status updates into evt_q for UI logging
    """

    def __init__(self, state: ScannerState, evt_q: queue.Queue):
        super().__init__(daemon=True)
        self.state = state
        self.evt_q = evt_q
        self._stop = threading.Event()
        self.ws = None
        self._deliver_q = queue.Queue()
        self.stream_symbols = []

    # ----------------- WebSocket callbacks -----------------

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            s = data.get("stream", "")
            payload = data.get("data", {})
            if "@" in s:
                symbol = s.split("@", 1)[0].upper()
            else:
                symbol = payload.get("s", "").upper()
            if not symbol:
                return
            k = payload.get("k", {})
            if not k:
                return
            if not k.get("x"):  # need CLOSED candle
                return

            bar = {
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "ts": int(k["T"]),
            }
            self._deliver_q.put((symbol, bar))
        except Exception as e:
            self.evt_q.put({"type": "status", "msg": f"WS parse error: {e}"})

    def _on_error(self, ws, error):
        self.evt_q.put({"type": "status", "msg": f"WS error: {error}"})

    def _on_close(self, ws, *_args):
        self.evt_q.put({"type": "status", "msg": "WS closed"})

    def _start_ws(self):
        if not self.stream_symbols:
            return
        stream = "/".join([f"{s.lower()}@kline_1m" for s in self.stream_symbols])
        url = f"wss://fstream.binance.com/stream?streams={stream}"
        self.ws = websocket.WebSocketApp(
            url, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close
        )

    # ----------------- Deliver loop / compute metrics -----------------

    def _deliver_loop(self):
        while not self._stop.is_set():
            try:
                symbol, bar = self._deliver_q.get(timeout=1)
            except queue.Empty:
                continue

            # Update bar deque
            self.state.update_kline(symbol, bar)

            # Compute metrics from a snapshot of deque (avoid concurrent mutation)
            dq = None
            with self.state._lock:
                dq = self.state.k1m.get(symbol)
                if dq is not None:
                    dq = deque(dq, maxlen=dq.maxlen)
            if dq is None:
                continue

            metrics = self._compute_metrics_for_symbol(symbol, dq)
            self.state.update_metrics(symbol, metrics)

    def _compute_metrics_for_symbol(self, symbol, dq: deque):
        """Compute RSI, ADX(1m), MACD, PriceSpike from the local 1m deque."""
        closes = [b["close"] for b in dq]
        if len(closes) < 30:
            return {}

        # Simple RSI(14)
        rsi = self._rsi(closes, 14)
        # Simple ADX-like from 1m
        adx_1m = self._adx_like_from_bars(dq, period=14)
        # MACD(12,26,9)
        macd_line, macd_signal, macd_hist = self._macd(closes, 12, 26, 9)
        # Price spike: last close vs close 10min ago
        spike = self._price_spike(closes, 10)

        return {
            "RSI": rsi,
            "RSI14": rsi,
            "ADX_1m": adx_1m,
            "MACD": macd_line,
            "MACD_HIST": macd_hist,
            "PRICE_SPIKE": spike,
            "LAST_CLOSE": closes[-1],
        }

    # ---- Indicators ----

    def _rsi(self, closes, length=14):
        gains = []
        losses = []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i - 1]
            if diff > 0:
                gains.append(diff)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-diff)
        if len(gains) < length:
            return float("nan")
        avg_gain = sum(gains[-length:]) / length
        avg_loss = sum(losses[-length:]) / length
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1 + rs))

    def _adx_like_from_bars(self, dq: deque, period=14):
        if len(dq) < period + 1:
            return float("nan")
        dx = []
        for i in range(1, len(dq)):
            high = dq[i]["high"]
            low = dq[i]["low"]
            prev_high = dq[i - 1]["high"]
            prev_low = dq[i - 1]["low"]
            up_move = high - prev_high
            down_move = prev_low - low
            plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
            minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0
            tr = max(
                high - low, abs(high - dq[i - 1]["close"]), abs(low - dq[i - 1]["close"])
            )
            if tr == 0:
                continue
            plus_di = 100.0 * (plus_dm / tr)
            minus_di = 100.0 * (minus_dm / tr)
            denom = plus_di + minus_di
            if denom == 0:
                dx.append(0.0)
            else:
                dx.append(100.0 * abs(plus_di - minus_di) / denom)
        if len(dx) < period:
            return float("nan")
        return sum(dx[-period:]) / period

    def _macd(self, closes, fast, slow, signal):
        def ema(series, length):
            if not series or len(series) < length:
                return float("nan")
            k = 2 / (length + 1)
            ema_val = series[0]
            for v in series[1:]:
                ema_val = v * k + ema_val * (1 - k)
            return ema_val

        fast_ema = ema(closes, fast)
        slow_ema = ema(closes, slow)
        if math.isnan(fast_ema) or math.isnan(slow_ema):
            return float("nan"), float("nan"), float("nan")
        macd_line = fast_ema - slow_ema
        # crude: treat last N as macd history for signal
        hist = [closes[i] - closes[i - slow] for i in range(slow, len(closes))]
        sig = ema(hist, signal)
        if math.isnan(sig):
            sig = 0.0
        macd_hist = macd_line - sig
        return macd_line, sig, macd_hist

    def _price_spike(self, closes, minutes_back=10):
        if len(closes) <= minutes_back:
            return 0.0
        recent = closes[-1]
        old = closes[-minutes_back - 1]
        if old == 0:
            return 0.0
        return (recent - old) / old * 100.0

    # ----------------- Thread run -----------------

    def start(self, symbols):
        self.stream_symbols = [s.upper() for s in symbols]
        if not self.stream_symbols:
            return
        super().start()

    def stop(self):
        self._stop.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def run(self):
        # Start WS on a dedicated thread
        self._start_ws()
        if self.ws is None:
            return

        ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(ping_interval=20, ping_timeout=10),
            daemon=True,
        )
        ws_thread.start()
        self.evt_q.put({"type": "status", "msg": "WS opened"})

        # Deliver loop
        self._deliver_loop()


# ----------------------------- AutoOrderFrame UI -----------------------------


class AutoOrderFrame(ttk.Frame):
    """
    Auto-Order Tab:
      - Left: Rule config, conditions, order settings
      - Right: Matching status, logs, and running info
    """

    def __init__(self, master, controller=None, **kwargs):
        super().__init__(master, **kwargs)
        self.controller = controller

        # runtime (single & multi rule)
        self._eval_after_id = None
        self._manual_thread = None
        self._manual_stop = threading.Event()

        # local cache of rules
        self.rules = []
        self.auto_orders_done_per_rule = defaultdict(int)
        self._first_true_ts = {}  # (rule_name, symbol) -> first_true_epoch_s
        self._last_trigger_ts = {}  # (rule_name, symbol) -> last_trigger_epoch_s

        # re-entry block (local flags; replace with real positions check if desired)
        self._block_if_open = True
        self._open_positions = {}  # symbol -> bool (local view only)
        # in-memory cache for DB rules: name -> (rule, conditions)
        self._rules_cache = {}

        # live
        self.scanner = None
        self.scanner_symbols = []
        # symbol -> {"stepSize": float, "tickSize": float, "minQty": float}
        self.symbol_filters = {}

        # statusbar var
        self.status_var = tk.StringVar(value="Ready")

        # HTTP session for Binance
        self._http = requests.Session()
        self.api_key = BINANCE_API_KEY.encode("utf-8")
        self.api_secret = BINANCE_API_SECRET.encode("utf-8")
        self.base_url = BINANCE_BASE_URL

        # scanner state & events
        self.scanner_state = ScannerState()
        self.scanner_evt_q = queue.Queue()

        # Top 20 list injected by Futures tab
        self._top20_symbols = set()

        # Stream universe control (to reduce WS load)
        # Default: stream Top 20 (when provided by Futures tab) + optional watchlist.
        # If Top 20 not available yet, we fall back to streaming 50 symbols.
        self._watchlist_symbols = set()
        self._stream_topn = 20
        self._stream_fallback_n = 50


        # Condition status cache: (rule_name, symbol, signature) -> "SCANNING"/"TRUE"/"FALSE"
        self._cond_status = {}
        # Which symbol to show condition status for per rule
        self._focus_symbol_by_rule = {}

        # evaluation lock
        self._eval_lock = threading.Lock()

        # placement locks + circuit breaker (Tier 1 safety)
        self._place_locks = {}  # (rule_name, symbol) -> threading.Lock
        self._order_ts_global = deque(maxlen=500)
        self._order_ts_by_symbol = defaultdict(lambda: deque(maxlen=200))
        self._cb_window_s = 60          # rolling window seconds
        self._cb_symbol_max = 2         # max orders per symbol in window
        self._cb_global_max = 8         # max orders total in window

        # build UI
        self._setup_styles()
        self._build_layout()

        # init DB
        self._init_db()
        self._refresh_rule_list()

        # NOTE: scanner is now started lazily in _on_start_auto_clicked /
        # _on_manual_scan_clicked, so we do NOT start it in __init__.

    # -------------------------- Styles / Theme ------------------------

    def _setup_styles(self):
        self.style = ttk.Style()
        base = "clam" if "clam" in self.style.theme_names() else self.style.theme_use()
        self.style.theme_use(base)
        self.style.configure("Title.TLabel", font=("Segoe UI", 14, "bold"))
        self.style.configure("Section.TLabelframe", padding=8)
        self.style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        self.style.configure("SubSection.TLabelframe", padding=6)
        self.style.configure("SubSection.TLabelframe.Label", font=("Segoe UI", 9, "bold"))
        self.style.configure("TButton", padding=(10, 4))
        self.style.configure("Toolbar.TButton", padding=(12, 6))
        self.style.configure("Small.TButton", padding=(8, 2))
        self.style.configure("Status.Treeview", rowheight=20)

    # -------------------------- DB Init / Helpers ------------------------

    def _init_db(self):
        os.makedirs(os.path.dirname(AUTO_DB_PATH), exist_ok=True)
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                """
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                logic_mode TEXT,
                hold_seconds INTEGER,
                interval_seconds INTEGER,
                one_shot INTEGER,
                cooldown_seconds INTEGER,
                max_auto_orders INTEGER,
                order_type TEXT,
                qty_mode TEXT,
                qty_value REAL,
                tp_enabled INTEGER,
                tp_value REAL,
                sl_enabled INTEGER,
                sl_value REAL,
                margin_mode TEXT,
                leverage INTEGER,
                mode TEXT,
                side TEXT,
                created_ts TEXT,
                updated_ts TEXT
            )
            """
            )
            c.execute(
                """
            CREATE TABLE IF NOT EXISTS conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT,
                cond_type TEXT,
                operator TEXT,
                value1 REAL,
                value2 REAL,
                source TEXT
            )
            """
            )
            c.execute(
                """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                rule_name TEXT,
                symbol TEXT,
                action TEXT,
                result TEXT,
                order_id TEXT,
                error TEXT
            )
            """
            )
            conn.commit()

    def _save_log(self, rule_name, symbol, action, result, order_id="", error=""):
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO logs (ts, rule_name, symbol, action, result, order_id, error) VALUES (?,?,?,?,?,?,?)",
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    rule_name,
                    symbol,
                    action,
                    result,
                    order_id,
                    error,
                ),
            )
            conn.commit()

    # ---- marketpredictor.db helpers (thread-safe connections) ----

    def _get_market_db_conn(self):
        """
        Open a NEW SQLite connection for marketpredictor.db.

        We do NOT reuse the same connection across threads because SQLite
        connection objects are not thread-safe by default.
        """
        path = MARKET_DB_PATH
        if not os.path.exists(path):
            return None
        try:
            return sqlite3.connect(path, check_same_thread=False)
        except Exception:
            return None

    def _get_adx_from_db(self, symbol: str, timeframe: str):
        """
        Return latest ADX value for symbol from marketpredictor.db.

        timeframe: '15m' -> ADX_15m
                   '30m' -> ADX_30m
                   others -> ADX (legacy 15m)
        """
        conn = self._get_market_db_conn()
        if conn is None:
            return None

        if timeframe == "15m":
            col = "ADX_15m"
        elif timeframe == "30m":
            col = "ADX_30m"
        else:
            col = "ADX"

        try:
            c = conn.cursor()
            # marketpredictor schema uses Symbol / Timestamp columns
            c.execute(
                f"SELECT {col} FROM marketpredictor "
                "WHERE Symbol=? ORDER BY Timestamp DESC LIMIT 1",
                (symbol.upper(),),
            )
            row = c.fetchone()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not row:
            return None
        try:
            return float(row[0])
        except (TypeError, ValueError):
            return None

    def _get_metric_from_db(self, symbol: str, kind: str):
        """
        kind: 'RSI', 'COIN_LS', 'TRADER_LS'
        """
        conn = self._get_market_db_conn()
        if conn is None:
            return None

        if kind == "RSI":
            col = "RSI"
        elif kind == "COIN_LS":
            col = "Coin_LS"
        elif kind == "TRADER_LS":
            col = "Trader_LS"
        else:
            return None

        try:
            c = conn.cursor()
            c.execute(
                f"SELECT {col} FROM marketpredictor "
                "WHERE Symbol=? ORDER BY Timestamp DESC LIMIT 1",
                (symbol.upper(),),
            )
            row = c.fetchone()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if not row:
            return None
        try:
            return float(row[0])
        except (TypeError, ValueError):
            return None

    # -------------------------- Layout / Widgets ------------------------

    def _build_layout(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        # Header
        header = ttk.Frame(self, padding=(6, 4))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        header.columnconfigure(1, weight=0)

        ttk.Label(
            header,
            text="Auto-Order Engine",
            style="Title.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            textvariable=self.status_var,
        ).grid(row=0, column=1, sticky="e", padx=(10, 0))

        # Toolbar
        toolbar = ttk.Frame(self, padding=(6, 4))
        toolbar.grid(row=1, column=0, sticky="ew")
        for i in range(0, 12):
            toolbar.columnconfigure(i, weight=0)
        toolbar.columnconfigure(12, weight=1)

        self.auto_run_var = tk.BooleanVar(value=False)

        self.start_button = ttk.Button(
            toolbar,
            text="Start Auto",
            style="Primary.TButton",
            command=self._on_start_auto_clicked,
        )
        self.start_button.grid(row=0, column=0, padx=(0, 6))

        ttk.Button(
            toolbar,
            text="Stop Auto",
            style="Toolbar.TButton",
            command=self._on_stop_auto_clicked,
        ).grid(row=0, column=1, padx=(0, 6))

        ttk.Button(
            toolbar,
            text="Scan Now (Manual)",
            style="Toolbar.TButton",
            command=self._on_manual_scan_clicked,
        ).grid(row=0, column=2, padx=(0, 6))

        ttk.Button(
            toolbar,
            text="View Logs",
            style="Toolbar.TButton",
            command=self._on_view_logs,
        ).grid(row=0, column=4, padx=(0, 6))

        self.reentry_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            toolbar,
            text="Block if already open (local)",
            variable=self.reentry_var,
            command=self._on_reentry_toggle,
        ).grid(row=0, column=5, padx=(20, 6))

        self.show_rules_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            toolbar,
            text="Show Rules Panel (toggle only logs)",
            variable=self.show_rules_var,
            command=self._toggle_rules_panel,
        ).grid(row=0, column=6, padx=(20, 6))

        # Main body: left (rules/config) + right (matches/logs)
        body = ttk.Frame(self, padding=(6, 4))
        body.grid(row=2, column=0, sticky="nsew")
        body.columnconfigure(0, weight=2)
        body.columnconfigure(1, weight=3)
        body.rowconfigure(0, weight=1)

        # Left: Rule config
        left = ttk.Frame(body)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        left.rowconfigure(1, weight=1)
        left.columnconfigure(0, weight=1)

        # Right: Matches + logs
        right = ttk.Frame(body)
        right.grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        right.rowconfigure(0, weight=2)
        right.rowconfigure(1, weight=1)
        right.columnconfigure(0, weight=1)

        self._build_rule_section(left)
        self._build_matches_section(right)
        self._build_logs_section(right)

        # Status bar
        status_bar = ttk.Frame(self, padding=(6, 4))
        status_bar.grid(row=3, column=0, sticky="ew")
        status_bar.columnconfigure(0, weight=1)

        ttk.Label(status_bar, textvariable=self.status_var).grid(
            row=0, column=0, sticky="w"
        )

    # ---- Rule section ----

    def _build_rule_section(self, parent):
        # RULE LIST
        rule_frame = ttk.Labelframe(
            parent,
            text="Rules",
            style="Section.TLabelframe",
            padding=8,
        )
        rule_frame.grid(row=0, column=0, sticky="nsew")
        rule_frame.columnconfigure(0, weight=1)
        rule_frame.rowconfigure(1, weight=1)

        top = ttk.Frame(rule_frame)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Rule:").grid(row=0, column=0, sticky="w")
        self.rule_name_var = tk.StringVar()
        self.rule_combo = ttk.Combobox(
            top,
            textvariable=self.rule_name_var,
            state="readonly",
            width=40,
        )
        self.rule_combo.grid(row=0, column=1, sticky="ew", padx=(4, 4))
        self.rule_combo.bind("<<ComboboxSelected>>", self._on_rule_selected)

        ttk.Button(
            top,
            text="New",
            style="Small.TButton",
            command=self._on_new_rule,
        ).grid(row=0, column=2, padx=(2, 2))

        ttk.Button(
            top,
            text="Delete",
            style="Small.TButton",
            command=self._on_delete_rule,
        ).grid(row=0, column=3, padx=(2, 2))

        # RULE SETUP
        setup = ttk.Labelframe(
            rule_frame,
            text="Rule Setup",
            style="SubSection.TLabelframe",
            padding=6,
        )
        setup.grid(row=1, column=0, sticky="nsew")
        setup.columnconfigure(1, weight=1)
        setup.columnconfigure(3, weight=1)

        row = 0

        ttk.Label(setup, text="Logic:").grid(row=row, column=0, sticky="w")
        self.logic_mode_var = tk.StringVar(value="ALL")
        ttk.Combobox(
            setup,
            textvariable=self.logic_mode_var,
            values=["ALL", "ANY"],
            width=8,
        ).grid(row=row, column=1, sticky="w", padx=(4, 4))

        ttk.Label(setup, text="Mode:").grid(row=row, column=2, sticky="w")
        self.mode_var = tk.StringVar(value="AUTO")
        ttk.Combobox(
            setup,
            textvariable=self.mode_var,
            values=["AUTO", "ALERT-ONLY"],
            width=10,
        ).grid(row=row, column=3, sticky="w", padx=(4, 4))
        row += 1

        ttk.Label(setup, text="Hold True (s):").grid(row=row, column=0, sticky="w")
        self.hold_secs_var = tk.IntVar(value=0)
        ttk.Spinbox(
            setup, from_=0, to=3600, textvariable=self.hold_secs_var, width=8
        ).grid(row=row, column=1, sticky="w", pady=(6, 0))

        ttk.Label(setup, text="Cooldown (s):").grid(row=row, column=2, sticky="w")
        self.cooldown_secs_var = tk.IntVar(value=0)
        ttk.Spinbox(
            setup, from_=0, to=3600, textvariable=self.cooldown_secs_var, width=8
        ).grid(row=row, column=3, sticky="w", pady=(6, 0))
        row += 1

        ttk.Label(setup, text="Interval (s):").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        self.interval_secs_var = tk.IntVar(value=5)
        ttk.Spinbox(
            setup, from_=1, to=3600, textvariable=self.interval_secs_var, width=8
        ).grid(row=row, column=1, sticky="w", pady=(6, 0))
        self.one_shot_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(setup, text="One-shot", variable=self.one_shot_var).grid(
            row=row, column=2, sticky="e", pady=(6, 0)
        )
        row += 1

        ttk.Label(setup, text="Max Auto Orders:").grid(
            row=row, column=0, sticky="w", pady=(6, 0)
        )
        self.max_auto_orders_var = tk.IntVar(value=5)
        ttk.Spinbox(
            setup, from_=0, to=999, textvariable=self.max_auto_orders_var, width=8
        ).grid(row=row, column=1, sticky="w", pady=(6, 0))
        row += 1

        # Account/Order
        account = ttk.Labelframe(
            parent, text="Order Settings", style="Section.TLabelframe", padding=8
        )
        account.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        account.columnconfigure(1, weight=1)
        account.columnconfigure(3, weight=1)

        row = 0

        ttk.Label(account, text="Order Type:").grid(row=row, column=0, sticky="w")
        self.order_type_var = tk.StringVar(value="MARKET")
        ttk.Combobox(
            account,
            textvariable=self.order_type_var,
            values=["MARKET"],
            width=10,
        ).grid(row=row, column=1, sticky="w", padx=(4, 4))

        ttk.Label(account, text="Side:").grid(row=row, column=2, sticky="w")
        self.side_var = tk.StringVar(value="LONG")
        ttk.Combobox(
            account,
            textvariable=self.side_var,
            values=["LONG", "SHORT"],
            width=10,
        ).grid(row=row, column=3, sticky="w", padx=(4, 4))
        row += 1

        ttk.Label(account, text="Qty Mode:").grid(row=row, column=0, sticky="w")
        self.qty_mode_var = tk.StringVar(value="USDT")
        ttk.Combobox(
            account,
            textvariable=self.qty_mode_var,
            values=["USDT", "COIN"],
            width=10,
        ).grid(row=row, column=1, sticky="w", padx=(4, 4))

        ttk.Label(account, text="Qty Value:").grid(row=row, column=2, sticky="w")
        self.qty_value_var = tk.DoubleVar(value=5.0)
        ttk.Entry(account, textvariable=self.qty_value_var, width=12).grid(
            row=row, column=3, sticky="w", padx=(4, 4)
        )
        row += 1

        ttk.Label(account, text="Margin Mode:").grid(row=row, column=0, sticky="w")
        self.margin_mode_var = tk.StringVar(value="ISOLATED")
        ttk.Combobox(
            account,
            textvariable=self.margin_mode_var,
            values=["ISOLATED", "CROSS"],
            width=10,
        ).grid(row=row, column=1, sticky="w", padx=(4, 4))

        ttk.Label(account, text="Leverage:").grid(row=row, column=2, sticky="w")
        self.leverage_var = tk.IntVar(value=10)
        ttk.Spinbox(
            account, from_=1, to=125, textvariable=self.leverage_var, width=6
        ).grid(row=row, column=3, sticky="w", padx=(4, 4))
        row += 1

        self.tp_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            account, text="TP %", variable=self.tp_enabled_var
        ).grid(row=row, column=0, sticky="w")
        self.tp_value_var = tk.DoubleVar(value=2.0)
        ttk.Entry(account, textvariable=self.tp_value_var, width=8).grid(
            row=row, column=1, sticky="w", padx=(4, 4)
        )

        self.sl_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            account, text="SL %", variable=self.sl_enabled_var
        ).grid(row=row, column=2, sticky="w")
        self.sl_value_var = tk.DoubleVar(value=2.0)
        ttk.Entry(account, textvariable=self.sl_value_var, width=8).grid(
            row=row, column=3, sticky="w", padx=(4, 4)
        )

        # CONDITIONS
        cond_frame = ttk.Labelframe(
            parent,
            text="Conditions",
            style="Section.TLabelframe",
            padding=8,
        )
        cond_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        cond_frame.columnconfigure(0, weight=1)
        cond_frame.rowconfigure(1, weight=1)

        # add bar
        add = ttk.Frame(cond_frame)
        add.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        for i in range(0, 12):
            add.columnconfigure(i, weight=0)

        ttk.Label(add, text="Type:").grid(row=0, column=0, sticky="w")
        self.cond_type_var = tk.StringVar(value="PRICE")
        ttk.Combobox(
            add,
            textvariable=self.cond_type_var,
            width=18,
            values=[
                "PRICE",
                "RSI",
                "RSI (14)",
                "RSI SLOPE",
                "MACD",
                "MACD HIST",
                "ADX",
                "ADX (1m)",
                "ADX (15m)",
                "ADX (30m)",
                "FUNDING",
                "COIN L/S",
                "TRADER L/S",
                "PRICE SPIKE",
                "CLOSE>VWAP",
                "CLOSE>SWINGHIGH(10)",
                "TOP 20",
                "TOP 20 (15m)",
                "TOP 20 (1m)",
            ],
        ).grid(row=0, column=1, sticky="w", padx=4)

        ttk.Label(add, text="Op:").grid(row=0, column=2, sticky="w")
        self.cond_op_var = tk.StringVar(value=">")
        ttk.Combobox(
            add,
            textvariable=self.cond_op_var,
            width=4,
            values=[">", "<", ">=", "<=", "==", "!="],
        ).grid(row=0, column=3, sticky="w", padx=4)

        ttk.Label(add, text="V1:").grid(row=0, column=4, sticky="w")
        self.cond_v1_var = tk.DoubleVar(value=0.0)
        ttk.Entry(add, textvariable=self.cond_v1_var, width=7).grid(
            row=0, column=5, sticky="w", padx=4
        )

        ttk.Label(add, text="V2:").grid(row=0, column=6, sticky="w")
        self.cond_v2_var = tk.DoubleVar(value=0.0)
        ttk.Entry(add, textvariable=self.cond_v2_var, width=7).grid(
            row=0, column=7, sticky="w", padx=4
        )

        ttk.Label(add, text="Source:").grid(row=0, column=8, sticky="w")
        self.cond_source_var = tk.StringVar(value="SCANNER")
        ttk.Combobox(
            add,
            textvariable=self.cond_source_var,
            width=10,
            values=["SCANNER", "DB"],
        ).grid(row=0, column=9, sticky="w", padx=4)

        ttk.Button(
            add, text="Add / Update", style="Small.TButton", command=self._on_add_condition
        ).grid(row=0, column=10, padx=(10, 2))

        ttk.Button(
            add, text="Delete", style="Small.TButton", command=self._on_delete_condition
        ).grid(row=0, column=11, padx=(2, 2))

        # tree
        self.conditions_tree = ttk.Treeview(
            cond_frame,
            columns=("Type", "Op", "V1", "V2", "Source", "Status"),
            show="headings",
            height=6,
        )
        self.conditions_tree.grid(row=1, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(
            cond_frame, orient="vertical", command=self.conditions_tree.yview
        )
        vsb.grid(row=1, column=1, sticky="ns")
        self.conditions_tree.configure(yscrollcommand=vsb.set)
        for col in ("Type", "Op", "V1", "V2", "Source", "Status"):
            self.conditions_tree.heading(col, text=col)
        self.conditions_tree.column("Type", width=140, anchor="w")
        self.conditions_tree.column("Op", width=40, anchor="center")
        self.conditions_tree.column("V1", width=80, anchor="e")
        self.conditions_tree.column("V2", width=80, anchor="e")
        self.conditions_tree.column("Source", width=80, anchor="center")
        self.conditions_tree.column("Status", width=90, anchor="center")

    def _build_matches_section(self, parent):
        box = ttk.Labelframe(
            parent,
            text="Matches (>=60% rules true)",
            style="Section.TLabelframe",
            padding=8,
        )
        box.grid(row=0, column=0, sticky="nsew")
        box.rowconfigure(0, weight=1)
        box.columnconfigure(0, weight=1)

        self.matches_tree = ttk.Treeview(
            box,
            columns=("Symbol", "Rule", "Match%", "Side"),
            show="headings",
            style="Status.Treeview",
        )
        self.matches_tree.grid(row=0, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(box, orient="vertical", command=self.matches_tree.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.matches_tree.configure(yscrollcommand=vsb.set)
        self.matches_tree.bind("<<TreeviewSelect>>", self._on_match_selected)

        for col, txt, width, anchor in [
            ("Symbol", "Symbol", 80, "w"),
            ("Rule", "Rule", 180, "w"),
            ("Match%", "Match %", 70, "e"),
            ("Side", "Side", 60, "center"),
        ]:
            self.matches_tree.heading(col, text=txt)
            self.matches_tree.column(col, width=width, anchor=anchor)

    def _build_logs_section(self, parent):
        box = ttk.Labelframe(
            parent,
            text="Status / Logs",
            style="Section.TLabelframe",
            padding=8,
        )
        box.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        box.rowconfigure(0, weight=1)
        box.columnconfigure(0, weight=1)

        self.log_text = tk.Text(
            box,
            height=6,
            wrap="word",
            font=("Consolas", 9),
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(box, orient="vertical", command=self.log_text.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=vsb.set)

    # -------------------------- UI Handlers ----------------------------

    def _on_view_logs(self):
        messagebox.showinfo(
            "Logs",
            "Full DB logs are stored in auto_order.db / logs table.\n"
            "Use an external viewer (e.g. DB Browser for SQLite) to inspect.",
        )

    def _append_status(self, msg: str):
        if not msg:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.log_text.insert("end", line)
        self.log_text.see("end")
        self.status_var.set(msg)

    def _on_start_auto_clicked(self):
        # lazily start scanner on first Auto-run
        if self.scanner is None:
            self._append_status("Starting scanner...")
            self._load_symbols_and_start_scanner()
        self.auto_run_var.set(True)
        self._append_status("Auto-run started.")
        self._schedule_next_eval()

    def _on_stop_auto_clicked(self):
        self.auto_run_var.set(False)
        self._append_status("Auto-run stopped.")
        if self._eval_after_id:
            try:
                self.after_cancel(self._eval_after_id)
            except Exception:
                pass
            self._eval_after_id = None

    def _on_manual_scan_clicked(self):
        # lazily start scanner if needed
        if self.scanner is None:
            self._append_status("Starting scanner for manual scan...")
            self._load_symbols_and_start_scanner()
        if self._manual_thread and self._manual_thread.is_alive():
            messagebox.showinfo("Manual Scan", "Manual scan already running.")
            return
        self._manual_stop.clear()
        self._manual_thread = threading.Thread(
            target=self._manual_scan_loop, daemon=True
        )
        self._manual_thread.start()
        self._append_status("Manual scan started.")

    def _manual_scan_loop(self):
        # Single pass evaluation in background
        if self._manual_stop.is_set():
            return
        self._eval_with_lock("MANUAL")

    def _on_reentry_toggle(self):
        self._block_if_open = bool(self.reentry_var.get())
        self._append_status(
            f"Block-if-open (local flags) set to {self._block_if_open!r}."
        )

    def _toggle_rules_panel(self):
        # Here you could hide/show the rules_tab if you convert to paned/frames
        # For now, we just log the state.
        self._append_status(
            f"Rules Panel visibility toggled -> {self.show_rules_var.get()!r}"
        )

    def _on_add_condition(self):
        cond_type = self.cond_type_var.get()
        op = self.cond_op_var.get()
        v1 = self.cond_v1_var.get()
        v2 = self.cond_v2_var.get()
        source = self.cond_source_var.get()

        rule_name = self.rule_name_var.get().strip()
        if not rule_name:
            messagebox.showerror("Add Condition", "Please select or enter a rule name.")
            return

        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO conditions (rule_name, cond_type, operator, value1, value2, source)
                VALUES (?,?,?,?,?,?)
            """,
                (rule_name, cond_type, op, v1, v2, source),
            )
            conn.commit()

        self._append_status(f"Condition added for rule '{rule_name}'.")
        self._load_rule_to_ui(rule_name)

    def _on_delete_condition(self):
        sel = self.conditions_tree.selection()
        if not sel:
            return
        rule_name = self.rule_name_var.get().strip()
        if not rule_name:
            return
        # simplest: delete all and reload
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM conditions WHERE rule_name=?", (rule_name,))
            conn.commit()
        self._append_status(f"All conditions deleted for rule '{rule_name}'.")
        self._load_rule_to_ui(rule_name)

    def _on_new_rule(self):
        name = tk.simpledialog.askstring("New Rule", "Enter name:")
        if not name:
            return
        name = name.strip()
        if not name:
            return
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute(
                "INSERT OR IGNORE INTO rules (name, logic_mode, hold_seconds, interval_seconds,"
                " one_shot, cooldown_seconds, max_auto_orders, order_type, qty_mode, qty_value,"
                " tp_enabled, tp_value, sl_enabled, sl_value, margin_mode, leverage, mode, side,"
                " created_ts, updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    name,
                    "ALL",
                    0,
                    5,
                    0,
                    0,
                    5,
                    "MARKET",
                    "USDT",
                    5.0,
                    0,
                    2.0,
                    0,
                    2.0,
                    "ISOLATED",
                    10,
                    "AUTO",
                    "LONG",
                    ts,
                    ts,
                ),
            )
            conn.commit()
        self._append_status(f"Rule '{name}' created.")
        self._refresh_rule_list()
        self.rule_name_var.set(name)
        self._load_rule_to_ui(name)

    def _on_delete_rule(self):
        name = self.rule_name_var.get().strip()
        if not name:
            return
        if not messagebox.askyesno(
            "Delete Rule", f"Delete rule '{name}' and all its conditions?"
        ):
            return
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM rules WHERE name=?", (name,))
            c.execute("DELETE FROM conditions WHERE rule_name=?", (name,))
            conn.commit()
        self._append_status(f"Rule '{name}' deleted.")
        self._refresh_rule_list()
        self.rule_name_var.set("")
        self._clear_rule_ui()

    def _on_rule_selected(self, _event=None):
        name = self.rule_name_var.get().strip()
        if not name:
            return
        self._load_rule_to_ui(name)

    # ---- rule DB <-> UI ----

    def _refresh_rule_list(self):
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM rules ORDER BY name")
            rows = [r[0] for r in c.fetchall()]
        self.rules = rows
        self.rule_combo["values"] = rows

    def _clear_rule_ui(self):
        self.logic_mode_var.set("ALL")
        self.hold_secs_var.set(0)
        self.interval_secs_var.set(5)
        self.one_shot_var.set(False)
        self.cooldown_secs_var.set(0)
        self.max_auto_orders_var.set(5)
        self.order_type_var.set("MARKET")
        self.qty_mode_var.set("USDT")
        self.qty_value_var.set(5.0)
        self.tp_enabled_var.set(False)
        self.tp_value_var.set(2.0)
        self.sl_enabled_var.set(False)
        self.sl_value_var.set(2.0)
        self.margin_mode_var.set("ISOLATED")
        self.leverage_var.set(10)
        self.mode_var.set("AUTO")
        self.side_var.set("LONG")
        self.conditions_tree.delete(*self.conditions_tree.get_children())

    def _load_rule_to_ui(self, name: str):
        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT logic_mode, hold_seconds, interval_seconds, one_shot, cooldown_seconds,"
                " max_auto_orders, order_type, qty_mode, qty_value, tp_enabled, tp_value, sl_enabled,"
                " sl_value, margin_mode, leverage, mode, side FROM rules WHERE name=?",
                (name,),
            )
            row = c.fetchone()
            if not row:
                return
            (
                logic_mode,
                hold_seconds,
                interval_seconds,
                one_shot,
                cooldown_seconds,
                max_auto_orders,
                order_type,
                qty_mode,
                qty_value,
                tp_enabled,
                tp_value,
                sl_enabled,
                sl_value,
                margin_mode,
                leverage,
                mode,
                side,
            ) = row

            self.logic_mode_var.set(logic_mode or "ALL")
            self.hold_secs_var.set(hold_seconds or 0)
            self.interval_secs_var.set(interval_seconds or 5)
            self.one_shot_var.set(bool(one_shot))
            self.cooldown_secs_var.set(cooldown_seconds or 0)
            self.max_auto_orders_var.set(max_auto_orders or 0)
            self.order_type_var.set(order_type or "MARKET")
            self.qty_mode_var.set(qty_mode or "USDT")
            self.qty_value_var.set(qty_value or 5.0)
            self.tp_enabled_var.set(bool(tp_enabled))
            self.tp_value_var.set(tp_value or 2.0)
            self.sl_enabled_var.set(bool(sl_enabled))
            self.sl_value_var.set(sl_value or 2.0)
            self.margin_mode_var.set(margin_mode or "ISOLATED")
            self.leverage_var.set(leverage or 10)
            self.mode_var.set(mode or "AUTO")
            self.side_var.set(side or "LONG")

            # load conditions
            c.execute(
                "SELECT cond_type, operator, value1, value2, source FROM conditions WHERE rule_name=?",
                (name,),
            )
            conds = [
                {
                    "type": r[0],
                    "op": r[1],
                    "v1": r[2],
                    "v2": r[3],
                    "source": r[4] or "SCANNER",
                }
                for r in c.fetchall()
            ]

        self.conditions_tree.delete(*self.conditions_tree.get_children())
        for c in conds:
            self.conditions_tree.insert(
                "",
                "end",
                values=(c["type"], c["op"], c["v1"], c["v2"], c["source"], "SCANNING"),
            )

        # Refresh condition statuses for the currently focused symbol (if any)
        sym_focus = self._focus_symbol_by_rule.get(name) or ""
        if sym_focus:
            self._refresh_conditions_status(name, sym_focus)


    # -------------------------- Condition Status (UI) --------------------

    def _on_match_selected(self, _event=None):
        """
        When user clicks a match row, show per-condition status for that symbol.
        """
        try:
            sel = self.matches_tree.selection()
            if not sel:
                return
            vals = self.matches_tree.item(sel[0], "values")
            if not vals:
                return
            symbol = str(vals[0]).upper()
            rule = self.rule_var.get().strip()
            if not rule:
                return
            self._focus_symbol_by_rule[rule] = symbol
            self._refresh_conditions_status(rule, symbol)
        except Exception:
            return

    def _refresh_conditions_status(self, rule_name: str, symbol: str):
        """
        Updates the Status column in the Conditions TreeView for the given rule + symbol.
        """
        try:
            symbol = (symbol or "").upper()
            # Update rows in-place
            for iid in self.conditions_tree.get_children():
                vals = list(self.conditions_tree.item(iid, "values"))
                if len(vals) < 6:
                    continue
                signature = (str(vals[0]), str(vals[1]), str(vals[2]), str(vals[3]), str(vals[4]))
                state = self._cond_status.get((rule_name, symbol, signature), "SCANNING")
                vals[5] = state
                self.conditions_tree.item(iid, values=tuple(vals))
        except Exception:
            pass

    def _set_default_focus_symbol(self, rule_name: str, matches_rows: list):
        """
        If no symbol is selected, default the status view to first match; else keep last focus.
        """
        if not rule_name:
            return
        if self._focus_symbol_by_rule.get(rule_name):
            return
        if matches_rows:
            self._focus_symbol_by_rule[rule_name] = str(matches_rows[0].get("symbol", "")).upper()


    # -------------------------- Scanner Wiring -------------------------

    def _load_symbols_and_start_scanner(self):
        """Start scanner in a background thread (non-blocking for Tk)."""

        # If scanner already running, skip
        if self.scanner is not None:
            return

        def worker():
            # For now, we derive symbol list from exchangeInfo and store filters
            try:
                syms, filters = self._fetch_symbols_and_filters()
            except Exception as e:
                # marshal status back onto Tk thread
                try:
                    self.after(0, lambda: self._append_status(f"Error fetching exchangeInfo: {e}"))
                except Exception:
                    pass
                return

            # Keep full filters for precision-safe orders
            self.symbol_filters = filters

            # Reduce WS load: stream only Top20 + watchlist (or small fallback)
            stream_syms = self._get_stream_universe(syms)
            self.scanner_symbols = stream_syms

            # Create and start the websocket scanner (already runs on its own thread)
            self.scanner = AutoOrderScanner(self.scanner_state, self.scanner_evt_q)
            self.scanner.start(stream_syms)

            # background thread for scanner events
            threading.Thread(target=self._scanner_evt_loop, daemon=True).start()

            try:
                self.after(
                    0,
                    lambda: self._append_status(
                        f"Scanner started (WS streams={len(stream_syms)}; filters={len(syms)})."
                    ),
                )
            except Exception:
                pass

        threading.Thread(target=worker, daemon=True).start()

    def set_top20_symbols(self, symbols_list):
        """Called by Futures Tab. Updates the Top 20 list.

        If the scanner is already running, it will be restarted using the new,
        reduced stream universe (Top 20 + watchlist).
        """
        try:
            self._top20_symbols = set([s.upper() for s in (symbols_list or [])])
            self._append_status(f"Top 20 list updated ({len(self._top20_symbols)} symbols).")
        except Exception:
            self._top20_symbols = set()

        # Restart scanner to reduce WS load to Top20 + watchlist (if running)
        try:
            if self.scanner is not None:
                self._restart_scanner_streams_async()
        except Exception:
            pass

    def set_watchlist_symbols(self, symbols_list):
        """Optional: set a user watchlist for streaming/evaluation (Top20 + watchlist)."""
        try:
            self._watchlist_symbols = set([str(s).upper() for s in (symbols_list or []) if str(s).strip()])
            self._append_status(f"Watchlist updated ({len(self._watchlist_symbols)} symbols).")
        except Exception:
            self._watchlist_symbols = set()

        # Restart scanner to apply new stream universe (if running)
        try:
            if self.scanner is not None:
                self._restart_scanner_streams_async()
        except Exception:
            pass

    def _get_stream_universe(self, all_symbols: list):
        """Return the reduced universe of symbols to stream over WS."""
        # Primary: Top20 + watchlist (when Top20 provided)
        desired = set()
        if getattr(self, "_top20_symbols", None):
            desired |= set(self._top20_symbols)
        if getattr(self, "_watchlist_symbols", None):
            desired |= set(self._watchlist_symbols)

        # If nothing provided yet, fall back to a small subset to keep WS stable
        if not desired:
            n = int(getattr(self, "_stream_fallback_n", 50) or 50)
            return [s.upper() for s in (all_symbols or [])[:n]]

        return sorted([s.upper() for s in desired])

    def _restart_scanner_streams_async(self):
        """Restart scanner in background to apply a new stream universe."""
        def worker():
            try:
                # Stop existing scanner (best effort)
                try:
                    if self.scanner is not None:
                        self.scanner.stop()
                except Exception:
                    pass

                # Re-create scanner with the reduced stream universe
                syms, filters = self._fetch_symbols_and_filters()
                self.symbol_filters = filters

                stream_syms = self._get_stream_universe(syms)
                self.scanner_symbols = stream_syms

                self.scanner = AutoOrderScanner(self.scanner_state, self.scanner_evt_q)
                self.scanner.start(stream_syms)

                threading.Thread(target=self._scanner_evt_loop, daemon=True).start()

                try:
                    self.after(0, lambda: self._append_status(
                        f"Scanner restarted (WS streams={len(stream_syms)})."
                    ))
                except Exception:
                    pass
            except Exception as e:
                try:
                    self.after(0, lambda: self._append_status(f"Scanner restart failed: {e}"))
                except Exception:
                    pass

        threading.Thread(target=worker, daemon=True).start()

    def _fetch_symbols_and_filters(self):
        """
        Fetch exchangeInfo once; store stepSize/tickSize/minQty per symbol.

        Returns:
            syms: list of USDT-M futures symbols
            filters: dict symbol -> {stepSize, tickSize, minQty}
        """
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        resp = self._http.get(url, timeout=10)
        resp.raise_for_status()
        j = resp.json()
        syms = []
        filters = {}
        for s in j.get("symbols", []):
            if s.get("contractType") != "PERPETUAL":
                continue
            sym = s.get("symbol")
            if not sym.endswith("USDT"):
                continue
            syms.append(sym)
            info = {"stepSize": 0.0, "tickSize": 0.0, "minQty": 0.0}
            for f in s.get("filters", []):
                ftype = f.get("filterType")
                if ftype == "LOT_SIZE":
                    info["stepSize"] = float(f.get("stepSize", "0"))
                    info["minQty"] = float(f.get("minQty", "0"))
                elif ftype == "PRICE_FILTER":
                    info["tickSize"] = float(f.get("tickSize", "0"))
            filters[sym] = info

        if not syms:
            raise RuntimeError("No USDT-M PERPETUAL symbols from exchangeInfo")

        self._append_status(
            f"Fetched {len(syms)} USDT-M symbols with precision filters."
        )
        return syms, filters

    def _scanner_evt_loop(self):
        while True:
            try:
                evt = self.scanner_evt_q.get(timeout=1)
            except queue.Empty:
                continue
            self.after(0, self._scanner_update_cb, evt, self.scanner_state)

    def _scanner_update_cb(self, evt, _state):
        if evt.get("type") == "status":
            self._append_status(evt.get("msg", ""))

    # -------------------------- Auto-Run Scheduler ---------------------

    def _schedule_next_eval(self):
        if not self.auto_run_var.get():
            return
        interval = max(1, int(self.interval_secs_var.get() or 5))
        if self._eval_after_id:
            try:
                self.after_cancel(self._eval_after_id)
            except Exception:
                pass
        self._eval_after_id = self.after(interval * 1000, self._auto_run_step)

    def _auto_run_step(self):
        self._eval_after_id = None
        if not self.auto_run_var.get():
            return
        # run heavy evaluation off the Tk main thread
        self._run_eval_once_background("AUTO")
        self._schedule_next_eval()

    # -------------------------- Evaluation Core ------------------------

    def _load_rule_params(self, name: str):
        """
        Return (rule_dict, conditions_list).
        Cached in self._rules_cache for faster repeated evaluation.
        """
        if name in self._rules_cache:
            return self._rules_cache[name]

        with connect(AUTO_DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT logic_mode, hold_seconds, interval_seconds, one_shot, cooldown_seconds,"
                " max_auto_orders, order_type, qty_mode, qty_value, tp_enabled, tp_value, sl_enabled,"
                " sl_value, margin_mode, leverage, mode, side FROM rules WHERE name=?",
                (name,),
            )
            row = c.fetchone()
            if not row:
                return None, None
            (
                logic_mode,
                hold_seconds,
                interval_seconds,
                one_shot,
                cooldown_seconds,
                max_auto_orders,
                order_type,
                qty_mode,
                qty_value,
                tp_enabled,
                tp_value,
                sl_enabled,
                sl_value,
                margin_mode,
                leverage,
                mode,
                side,
            ) = row

            c.execute(
                "SELECT cond_type, operator, value1, value2, source FROM conditions WHERE rule_name=?",
                (name,),
            )
            conds = [
                {
                    "type": r[0],
                    "op": r[1],
                    "v1": r[2],
                    "v2": r[3],
                    "source": r[4] or "SCANNER",
                }
                for r in c.fetchall()
            ]

        rule = {
            "name": name,
            "logic_mode": logic_mode or "ALL",
            "hold_seconds": hold_seconds or 0,
            "interval_seconds": interval_seconds or 5,
            "one_shot": bool(one_shot),
            "cooldown_seconds": cooldown_seconds or 0,
            "max_auto_orders": max_auto_orders or 0,
            "order_type": order_type or "MARKET",
            "qty_mode": qty_mode or "USDT",
            "qty_value": qty_value or 5.0,
            "tp_enabled": bool(tp_enabled),
            "tp_value": tp_value or 2.0,
            "sl_enabled": bool(sl_enabled),
            "sl_value": sl_value or 2.0,
            "margin_mode": margin_mode or "ISOLATED",
            "leverage": leverage or 10,
            "mode": mode or "AUTO",
            "side": side or "LONG",
        }
        self._rules_cache[name] = (rule, conds)
        return rule, conds

    def _run_eval_core(self, mode: str):
        """
        Evaluate all symbols against selected rules.

        Returns:
            matches: list of (symbol, rule_name, match_pct, side)
        """
        metrics_snapshot = self.scanner_state.get_snapshot()
        if not metrics_snapshot:
            return []

        # which rules? For now, evaluate all rules
        rules = self.rules

        matches = []

        for sym, m in metrics_snapshot.items():
            for rule_name in rules:
                rule, conds = self._load_rule_params(rule_name)
                if not rule or not conds:
                    continue
                match_pct, all_true = self._evaluate_conditions(rule.get("name",""), sym, m, conds)
                if match_pct >= 60:  # threshold to list
                    matches.append((sym, rule_name, match_pct, rule.get("side", "LONG")))
                # If mode AUTO and fully matched, maybe place order
                # Tier 1: NEVER place orders unless ALL conditions are TRUE (100% match)
                if mode == "AUTO" and all_true and match_pct == 100:
                    self._maybe_place_order_for_match(sym, rule, conds)

        # sort & limit matches to keep TreeView light
        matches.sort(key=lambda x: (-x[2], x[0], x[1]))
        return matches[:200]

    def _eval_with_lock(self, mode: str):
        """
        Run evaluation under a lock, then apply TreeView update on the Tk thread.
        """
        if not self._eval_lock.acquire(blocking=False):
            # Previous evaluation still running; skip this cycle
            return
        try:
            matches = self._run_eval_core(mode)
        finally:
            try:
                self.after(0, self._update_matches_tree, matches)
            except Exception:
                pass
            self._eval_lock.release()

    def _run_eval_once_background(self, mode: str):
        """
        Spawn a worker thread to run evaluation without blocking Tk.
        """
        threading.Thread(target=self._eval_with_lock, args=(mode,), daemon=True).start()

    def _update_matches_tree(self, matches):
        """
        Incremental TreeView update:
        - Reuse existing rows whenever possible (no full delete/rebuild).
        - Remove rows that are no longer in the new matches list.
        """
        # Build a set of keys present in the new matches
        new_keys = set()
        for sym, rule_name, match_pct, side in matches:
            new_keys.add((sym, rule_name))

        # Existing rows
        existing = {}
        for iid in self.matches_tree.get_children():
            vals = self.matches_tree.item(iid, "values")
            if len(vals) < 4:
                continue
            sym, rule_name, _pct, _side = vals
            existing[(sym, rule_name)] = iid

        # Remove rows that disappeared
        for key, iid in list(existing.items()):
            if key not in new_keys:
                self.matches_tree.delete(iid)

        # Upsert new rows
        for sym, rule_name, match_pct, side in matches:
            key = (sym, rule_name)
            pct_str = f"{match_pct:.0f}"
            if key in existing:
                iid = existing[key]
                self.matches_tree.item(
                    iid, values=(sym, rule_name, pct_str, side.upper())
                )
            else:
                self.matches_tree.insert(
                    "",
                    "end",
                    values=(sym, rule_name, pct_str, side.upper()),
                )
        # Default condition-status focus symbol (first match) if nothing selected
        try:
            current_rule = self.rule_var.get().strip()
            # matches is list of dicts with 'symbol' etc.
            self._set_default_focus_symbol(current_rule, matches)
            sym_focus = self._focus_symbol_by_rule.get(current_rule, "")
            if sym_focus:
                self._refresh_conditions_status(current_rule, sym_focus)
        except Exception:
            pass


    # ---- condition evaluation ----

    def _evaluate_conditions(self, rule_name: str, symbol: str, m: dict, conds: list):
        """
        Return (match_pct, all_true) for the given symbol/metrics and conditions.
        """
        if not conds:
            return 0, False

        results = []
        all_true = True

        for cond in conds:
            ctype = cond.get("type", "PRICE")
            op = cond.get("op") or cond.get("operator") or ">"
            try:
                v1 = float(cond.get("v1", 0.0))
            except (TypeError, ValueError):
                v1 = 0.0
            try:
                v2 = float(cond.get("v2", 0.0))
            except (TypeError, ValueError):
                v2 = 0.0

            source = cond.get("source", "SCANNER")

            val = None

            if ctype == "PRICE":
                val = m.get("LAST_CLOSE")
                if val is None:
                    # cannot evaluate, treat as false
                    res = False
                else:
                    # For simplicity: absolute price must be between v1 and v2.
                    try:
                        res = float(v1) <= float(val) <= float(v2) if v2 != 0 else self._compare(val, op, v1)
                    except (TypeError, ValueError):
                        res = False

            elif ctype in ("RSI", "RSI (14)"):
                if source == "DB":
                    val = self._get_metric_from_db(symbol, "RSI")
                else:
                    val = m.get("RSI")
                res = self._compare(val, op, v1)

            elif ctype == "RSI SLOPE":
                # Not implemented in scanner yet; treat as false for now
                res = False

            elif ctype == "MACD":
                val = m.get("MACD")
                res = self._compare(val, op, v1)

            elif ctype == "MACD HIST":
                val = m.get("MACD_HIST")
                res = self._compare(val, op, v1)

            elif ctype in ("ADX", "ADX (1m)"):
                val = m.get("ADX_1m")
                res = self._compare(val, op, v1)

            elif ctype == "ADX (15m)":
                if source == "DB":
                    val = self._get_adx_from_db(symbol, "15m")
                else:
                    val = m.get("ADX_15m")
                res = self._compare(val, op, v1)

            elif ctype == "ADX (30m)":
                if source == "DB":
                    val = self._get_adx_from_db(symbol, "30m")
                else:
                    val = m.get("ADX_30m")
                res = self._compare(val, op, v1)

            elif ctype == "FUNDING":
                val = m.get("FUNDING")
                res = self._compare(val, op, v1)

            elif ctype == "COIN L/S":
                if source == "DB":
                    val = self._get_metric_from_db(symbol, "COIN_LS")
                else:
                    val = m.get("COIN_LS")
                res = self._compare(val, op, v1)

            elif ctype == "TRADER L/S":
                if source == "DB":
                    val = self._get_metric_from_db(symbol, "TRADER_LS")
                else:
                    val = m.get("TRADER_LS")
                res = self._compare(val, op, v1)

            elif ctype == "PRICE SPIKE":
                # spike% in last N minutes
                val = m.get("PRICE_SPIKE")
                res = self._compare(val, op, v1)

            elif ctype == "CLOSE>VWAP":
                vwap = m.get("VWAP")
                last_close = m.get("LAST_CLOSE")
                if vwap is None or last_close is None:
                    res = False
                else:
                    # interpret as: last_close > VWAP + v1
                    try:
                        res = float(last_close) > float(vwap) + v1
                    except (TypeError, ValueError):
                        res = False

            elif ctype == "CLOSE>SWINGHIGH(10)":
                swing_high = m.get("SWINGHIGH10")
                last_close = m.get("LAST_CLOSE")
                if swing_high is None or last_close is None:
                    res = False
                else:
                    try:
                        res = float(last_close) > float(swing_high)
                    except (TypeError, ValueError):
                        res = False

            
            elif ctype == "TOP 20":
                # If Futures tab provided Top-20 list, enforce membership.
                # If list is empty (not wired yet), do not block matching.
                if self._top20_symbols:
                    res = symbol.upper() in self._top20_symbols
                else:
                    res = True

            elif ctype == "TOP 20 (15m)":
                # Enforce Top-20 membership only if list is available, otherwise just evaluate ADX(15m).
                if self._top20_symbols and symbol.upper() not in self._top20_symbols:
                    res = False
                else:
                    if source == "DB":
                        val = self._get_adx_from_db(symbol, "15m")
                    else:
                        val = m.get("ADX_15m")
                    res = val is not None and self._compare(val, op, v1)

            elif ctype == "TOP 20 (1m)":
                # Enforce Top-20 membership only if list is available, otherwise just evaluate ADX(1m).
                if self._top20_symbols and symbol.upper() not in self._top20_symbols:
                    res = False
                else:
                    val = m.get("ADX_1m")
                    res = val is not None and self._compare(val, op, v1)

            else:
                # unknown condition type
                res = False

                        # store per-condition status (SCANNING / TRUE / FALSE) for UI
            try:
                signature = (str(ctype), str(op), str(v1), str(v2), str(source))
                if ctype.startswith("TOP 20"):
                    state = "TRUE" if res else "FALSE"
                elif val is None or (isinstance(val, float) and math.isnan(val)):
                    state = "SCANNING"
                else:
                    state = "TRUE" if res else "FALSE"
                self._cond_status[(rule_name, symbol.upper(), signature)] = state
            except Exception:
                pass

            results.append(res)
            all_true = all_true and res

        true_count = sum(1 for r in results if r)
        match_pct = int(round(100.0 * true_count / len(results)))
        return match_pct, all_true

    def _compare(self, val, op, threshold):
        if val is None or isinstance(val, float) and math.isnan(val):
            return False
        try:
            v = float(val)
            thr = float(threshold)
        except (TypeError, ValueError):
            return False

        if op == ">":
            return v > thr
        if op == "<":
            return v < thr
        if op == ">=":
            return v >= thr
        if op == "<=":
            return v <= thr
        if op == "==":
            return v == thr
        if op == "!=":
            return v != thr
        return False

    # -------------------------- Order Placement ------------------------

    def _maybe_place_order_for_match(self, symbol, rule, conds):
        """Tier 1 safe placement:
        - Per (rule, symbol) placement lock
        - Circuit breaker (rate-limit) precheck
        - Exchange-verified re-entry block (positionRisk) when enabled
        - Fail-closed on safety check errors
        """
        side = (rule.get("side") or "LONG").upper()
        rule_name = rule.get("name", "Unnamed")
        symbol = str(symbol).upper()
        key = (rule_name, symbol)

        # One-shot limit (per rule)
        if rule.get("one_shot"):
            if self.auto_orders_done_per_rule[rule_name] >= int(rule.get("max_auto_orders", 0) or 0):
                self._append_status(f"[{symbol}] One-shot limit reached for rule '{rule_name}'.")
                return

        # Cooldown (per rule+symbol)
        cooldown = int(rule.get("cooldown_seconds") or 0)
        now_ts = time.time()
        last_ts = float(self._last_trigger_ts.get(key, 0) or 0)
        if cooldown > 0 and (now_ts - last_ts) < cooldown:
            return

        # Hold-True logic (per rule+symbol)
        hold_secs = int(rule.get("hold_seconds") or 0)
        if hold_secs > 0:
            first_ts = self._first_true_ts.get(key)
            if first_ts is None:
                self._first_true_ts[key] = now_ts
                return
            if (now_ts - float(first_ts)) < hold_secs:
                return
        else:
            self._first_true_ts.pop(key, None)

        # Tier 1: lock order placement per (rule, symbol) to prevent overlapping entries
        place_lock = self._get_place_lock(rule_name, symbol)
        if not place_lock.acquire(blocking=False):
            return

        try:
            # Tier 1: circuit breaker rate-limit
            if self._circuit_breaker_precheck(symbol):
                return

            # Re-entry block (exchange-verified when enabled)
            if self._block_if_open:
                try:
                    # Prefer exchange truth; if it errors, fail-closed (block)
                    if self._has_open_position_binance(symbol):
                        self._append_status(
                            f"[{symbol}] Skipping order for '{rule_name}' because an open position exists (Binance positionRisk)."
                        )
                        # also set local flag for UI consistency
                        try:
                            self._set_open_flag(symbol, True)
                        except Exception:
                            pass
                        return
                except Exception:
                    # fail-closed: if we cannot validate, do NOT place an order
                    self._append_status(
                        f"[{symbol}] Safety check failed (positionRisk). Blocking order for '{rule_name}'."
                    )
                    return

            # In ALERT-ONLY mode, don't place orders
            if (rule.get("mode") or "AUTO") != "AUTO":
                self._append_status(f"[{symbol}] ALERT-ONLY match for rule '{rule_name}' ({side}).")
                return

            # Place order
            try:
                self._place_order(symbol, rule, side)
            except Exception as e:
                self._append_status(f"[{symbol}] AUTO ERROR [{rule_name}] {side}: {e}")
                self._save_log(rule_name, symbol, "ERROR", "FAIL", error=str(e))
                return

            # Success bookkeeping
            self.auto_orders_done_per_rule[rule_name] += 1
            self._last_trigger_ts[key] = now_ts
            try:
                self._set_open_flag(symbol, True)   # immediate local block (UI)
            except Exception:
                pass
            try:
                self._record_order_ts(symbol)        # circuit breaker accounting
            except Exception:
                pass
            self._append_status(f"[{symbol}] AUTO ORDER [{rule_name}] {side} placed.")

        finally:
            try:
                place_lock.release()
            except Exception:
                pass

    # -------------------------- Tier 1 Safety Helpers ------------------

    def _get_place_lock(self, rule_name: str, symbol: str) -> threading.Lock:
        key = (str(rule_name), str(symbol).upper())
        lock = self._place_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            self._place_locks[key] = lock
        return lock

    def _prune_ts_deque(self, dq: deque, window_s: int):
        """Remove timestamps older than window_s from the left side."""
        now = time.time()
        while dq and (now - dq[0]) > window_s:
            dq.popleft()

    def _circuit_breaker_trip(self, symbol: str, reason: str):
        """Hard stop auto-run and log the reason."""
        try:
            self.auto_run_var.set(False)
            if self._eval_after_id:
                try:
                    self.after_cancel(self._eval_after_id)
                except Exception:
                    pass
                self._eval_after_id = None
        except Exception:
            pass
        self._append_status(f"CIRCUIT BREAKER: {reason} (symbol={symbol})")

    def _circuit_breaker_precheck(self, symbol: str) -> bool:
        """Return True if we should block placement due to excessive order rate."""
        try:
            sym = str(symbol).upper()

            # prune windows
            self._prune_ts_deque(self._order_ts_global, self._cb_window_s)
            dq = self._order_ts_by_symbol[sym]
            self._prune_ts_deque(dq, self._cb_window_s)

            # check limits
            if len(dq) >= int(self._cb_symbol_max):
                self._circuit_breaker_trip(sym, f"Too many orders for symbol in {self._cb_window_s}s (>= {self._cb_symbol_max})")
                return True
            if len(self._order_ts_global) >= int(self._cb_global_max):
                self._circuit_breaker_trip(sym, f"Too many orders globally in {self._cb_window_s}s (>= {self._cb_global_max})")
                return True
            return False
        except Exception:
            # fail closed: if we cannot validate, block the order
            self._circuit_breaker_trip(str(symbol).upper(), "Safety precheck error")
            return True

    def _record_order_ts(self, symbol: str):
        """Record a successful order timestamp for circuit breaker accounting."""
        sym = str(symbol).upper()
        ts = time.time()
        self._order_ts_global.append(ts)
        self._order_ts_by_symbol[sym].append(ts)

    def _has_open_position_binance(self, symbol: str) -> bool:
        """Exchange-verified open-position check using /fapi/v2/positionRisk.

        Fail-closed: if keys are missing or the request fails, returns True (block).
        """
        try:
            if not BINANCE_API_KEY or not BINANCE_API_SECRET:
                return True
            data = self._signed_request(
                "GET",
                "/fapi/v2/positionRisk",
                {"symbol": str(symbol).upper()},
            )
            if isinstance(data, dict):
                data = [data]
            for row in (data or []):
                if str(row.get("symbol", "")).upper() != str(symbol).upper():
                    continue
                amt = row.get("positionAmt", row.get("positionAmt", "0"))
                try:
                    amt_f = float(amt)
                except Exception:
                    amt_f = 0.0
                return abs(amt_f) > 0.0
            # if no row found, assume no position
            return False
        except Exception:
            return True
# ---- local open position flags ----

    def _set_open_flag(self, symbol, is_open: bool):
        self._open_positions[symbol.upper()] = bool(is_open)

    def _has_open_position(self, symbol) -> bool:
        # Tier 1: Exchange-verified re-entry protection (fail-closed).
        # If we cannot confirm position status, we block re-entry.
        try:
            if self._has_open_position_binance(symbol):
                return True
        except Exception:
            return True
        # Fallback to local flag (best-effort)
        return bool(self._open_positions.get(str(symbol).upper(), False))

    def _reset_open_flags(self):
        self._open_positions.clear()
        self._append_status("Open-position flags reset.")

    # -------------------------- Tier 1 Placement Lock Helpers ------------------

    def _get_place_lock(self, rule_name: str, symbol: str):
        """Return a per-(rule,symbol) lock to prevent concurrent duplicate placements."""
        key = (str(rule_name), str(symbol).upper())
        lock = self._place_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            self._place_locks[key] = lock
        return lock


    # -------------------------- Binance REST helpers ------------------

    def _signed_request(self, method: str, path: str, params: dict = None):
        if params is None:
            params = {}
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(
            self.api_secret,
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        query += f"&signature={sig}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        url = f"{self.base_url}{path}?{query}"
        if method == "GET":
            resp = self._http.get(url, headers=headers, timeout=10)
        elif method == "POST":
            resp = self._http.post(url, headers=headers, timeout=10)
        else:
            raise ValueError(f"Unsupported method {method}")
        if resp.status_code != 200:
            raise RuntimeError(
                f"{method} {path} failed: {resp.status_code} {resp.text}"
            )
        return resp.json()

    def _set_margin_type(self, symbol, mode: str):
        mode = mode.upper()
        if mode not in ("CROSS", "ISOLATED"):
            mode = "ISOLATED"
        try:
            self._signed_request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": symbol.upper(), "marginType": mode},
            )
        except Exception:
            # ignore 400 "No need to change margin type"
            pass

    def _set_leverage(self, symbol, leverage: int):
        lev = max(1, min(int(leverage), 125))
        self._signed_request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": symbol.upper(), "leverage": lev},
        )

    def _place_order(self, symbol, rule, side: str):
        qty_mode = rule.get("qty_mode", "USDT")
        qty_value = float(rule.get("qty_value", 5.0))
        leverage = int(rule.get("leverage", 10))
        margin_mode = rule.get("margin_mode", "ISOLATED")

        # set margin/leverage
        self._set_margin_type(symbol, margin_mode)
        self._set_leverage(symbol, leverage)

        # fetch last price from scanner metrics
        metrics = self.scanner_state.metrics.get(symbol.upper(), {})
        last_price = metrics.get("LAST_CLOSE")
        if not last_price:
            raise RuntimeError("No LAST_CLOSE from scanner metrics for qty calc")

        # qty
        if qty_mode == "USDT":
            notional = qty_value
            qty = notional * leverage / float(last_price)
        else:
            qty = qty_value

        filters = self.symbol_filters.get(symbol.upper(), {})
        step = filters.get("stepSize", 0.0) or 0.0
        min_qty = filters.get("minQty", 0.0) or 0.0

        def _round_step(q, step_size):
            if step_size <= 0:
                return float(q)
            return math.floor(q / step_size) * step_size

        qty = _round_step(qty, step)
        if min_qty > 0 and qty < min_qty:
            qty = min_qty

        if qty <= 0:
            raise RuntimeError("Qty after rounding <= 0")

        side_api = "BUY" if side.upper() == "LONG" else "SELL"

        params = {
            "symbol": symbol.upper(),
            "side": side_api,
            "type": "MARKET",
            "quantity": f"{qty:.8f}",
        }

        # Place order
        res = self._signed_request("POST", "/fapi/v1/order", params)
        order_id = res.get("orderId", "")
        self._save_log(
            rule.get("name", "Unnamed"),
            symbol.upper(),
            "AUTO_ORDER",
            "OK",
            order_id=str(order_id),
        )