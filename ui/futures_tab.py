# binance_futures_trader/ui/futures_tab.py — READY-TO-DROP (Patched)
#
#   ✔ Funding % column + funding-based color
#   ✔ Coin L/S + Trader L/S columns
#   ✔ Signal column showing "⚠ SQUEEZE"
#   ✔ Color logic priority:
#        - SQUEEZE (Trader L/S > 1.3 AND Coin L/S < 0.8) => amber (highest priority)
#        - Trader L/S > 1.3 => green
#        - Trader L/S < 0.8 => red
#        - Otherwise funding-based color
#   ✔ Uses Local PC time for "Last update"
#   ✔ FIX: Auto-refresh no longer stops when a fetch overlaps refresh interval
#   ✔ OPT: Use requests.Session() for connection pooling
#
#   ✅ NEW:
#        - Futures tab uses SAME Trader L/S endpoint as Trending tab:
#            /futures/data/topLongShortPositionRatio
#        - Futures tab uses SAME period_map logic as Trending tab
#          (e.g., 1m interval -> 5m L/S period)
#
#   ✅ PATCH (THIS FIXES YOUR ERROR):
#        - Initialize _top20_lock and _top20_cache in __init__
#        - Call _set_top20_cache(rows) on every successful refresh
#        - Ensures TelegramTab callback futures_tab.get_top20_rows() works reliably

import requests
import threading
import queue
import tkinter as tk
from tkinter import ttk
from datetime import datetime

try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib.pyplot as plt
    _MPL_OK = True
except Exception:
    _MPL_OK = False

BINANCE_FAPI = "https://fapi.binance.com"


# --- Wilder ADX helper (kept same) ---
def _adx(high, low, close, period=30):
    n = len(close)
    if n < (2 * period + 1):
        return [None] * n

    dm_plus = [0.0] * n
    dm_minus = [0.0] * n
    tr = [0.0] * n

    for i in range(1, n):
        up = high[i] - high[i - 1]
        dn = low[i - 1] - low[i]
        dm_plus[i] = up if (up > dn and up > 0) else 0.0
        dm_minus[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )

    def _rma(vals):
        out = [None] * n
        acc = sum(vals[1: period + 1])
        out[period] = acc / period
        for j in range(period + 1, n):
            acc = acc - (acc / period) + vals[j]
            out[j] = acc / period
        return out

    atr = _rma(tr)
    rma_p = _rma(dm_plus)
    rma_m = _rma(dm_minus)

    di_p = [(100 * (p / a)) if a else None for p, a in zip(rma_p, atr)]
    di_m = [(100 * (m / a)) if a else None for m, a in zip(rma_m, atr)]

    dx = [
        (100 * abs(p - m) / (p + m))
        if (p is not None and m is not None and (p + m) != 0)
        else None
        for p, m in zip(di_p, di_m)
    ]

    adx_vals = [None] * n
    start = period + 1
    end = 2 * period + 1
    if end <= n and all(d is not None for d in dx[start:end]):
        seed = sum(dx[start:end]) / period
        adx_vals[end - 1] = seed
        for i in range(end, n):
            adx_vals[i] = (
                adx_vals[i - 1]
                if dx[i] is None
                else ((adx_vals[i - 1] * (period - 1)) + dx[i]) / period
            )
    return adx_vals


class FuturesTab(ttk.Frame):
    """
    Futures tab: top USD-margined PERP futures by 24h %,
    with ADX(1m), ADX(15m), Funding %, Coin L/S, Trader L/S, and Signal.

    NOTE:
      - Coin L/S is globalLongShortAccountRatio (account-based global)
      - Trader L/S is topLongShortPositionRatio (POSITION-based, matches Trending tab)
      - L/S period is mapped from interval (matches Trending tab period_map)
    """

    # Match TrendingTab's mapping
    _LS_PERIOD_MAP = {
        "1m": "5m", "3m": "5m", "5m": "5m",
        "15m": "15m", "30m": "30m",
        "1h": "1h", "4h": "4h", "1d": "1d"
    }

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        self.refresh_interval_var = tk.IntVar(value=60)
        self.auto_refresh_var = tk.BooleanVar(value=True)

        self._refresh_job = None
        self._fetching = False
        self._result_queue = queue.Queue()

        self._session = requests.Session()

        # ---- PATCH: TelegramTab depends on these existing ----
        self._top20_lock = threading.Lock()
        self._top20_cache = []
        # -----------------------------------------------------

        self.figure = None
        self.ax = None
        self.canvas = None
        self.price_up_label = None
        self.price_down_label = None

        self._build_ui()
        self._start_result_poller()
        self._initial_load()

    # ──────────────────────────────────────────────────────────────
    # UI
    # ──────────────────────────────────────────────────────────────
    def _build_ui(self):
        top_frame = ttk.Frame(self)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        ttk.Label(top_frame, text="Auto-refresh (s):").pack(side=tk.LEFT, padx=(0, 5))

        self.refresh_spin = ttk.Spinbox(
            top_frame,
            from_=10,
            to=3600,
            width=6,
            textvariable=self.refresh_interval_var,
            increment=10,
        )
        self.refresh_spin.pack(side=tk.LEFT, padx=(0, 10))

        self.auto_refresh_check = ttk.Checkbutton(
            top_frame,
            text="Auto-refresh",
            variable=self.auto_refresh_var,
            command=self._toggle_auto_refresh,
        )
        self.auto_refresh_check.pack(side=tk.LEFT, padx=(0, 10))

        self.refresh_button = ttk.Button(
            top_frame, text="Refresh now", command=self.refresh_now
        )
        self.refresh_button.pack(side=tk.LEFT, padx=(0, 10))

        self.status_label = ttk.Label(top_frame, text="Ready.", foreground="#888888")
        self.status_label.pack(side=tk.LEFT, padx=(10, 0))

        table_frame = ttk.Frame(self)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        columns = (
            "rank",
            "symbol",
            "signal",
            "price",
            "funding",
            "coin_ls",
            "trader_ls",
            "adx1m",
            "adx15m",
            "change",
        )

        self.tree = ttk.Treeview(
            table_frame, columns=columns, show="headings", height=18
        )

        self.tree.heading("rank", text="#")
        self.tree.heading("symbol", text="Name")
        self.tree.heading("signal", text="Signal")
        self.tree.heading("price", text="Price")
        self.tree.heading("funding", text="Funding %")
        self.tree.heading("coin_ls", text="Coin L/S")
        self.tree.heading("trader_ls", text="Trader L/S")
        self.tree.heading("adx1m", text="ADX(1m)")
        self.tree.heading("adx15m", text="ADX(15m)")
        self.tree.heading("change", text="Change")

        self.tree.column("rank", width=40, anchor=tk.CENTER)
        self.tree.column("symbol", width=140, anchor=tk.W)
        self.tree.column("signal", width=90, anchor=tk.CENTER)
        self.tree.column("price", width=100, anchor=tk.E)
        self.tree.column("funding", width=80, anchor=tk.E)
        self.tree.column("coin_ls", width=80, anchor=tk.E)
        self.tree.column("trader_ls", width=90, anchor=tk.E)
        self.tree.column("adx1m", width=80, anchor=tk.E)
        self.tree.column("adx15m", width=80, anchor=tk.E)
        self.tree.column("change", width=100, anchor=tk.E)

        # Funding-based row colors (whole row)
        self.tree.tag_configure("funding_pos", foreground="#16c784")
        self.tree.tag_configure("funding_neg", foreground="#f6465d")
        self.tree.tag_configure("funding_neutral", foreground="#808080")

        # Trader L/S dominance colors (override funding when triggered)
        self.tree.tag_configure("trader_ls_long", foreground="#00c176")
        self.tree.tag_configure("trader_ls_short", foreground="#ff4d4d")

        # Divergence / squeeze warning (highest priority)
        self.tree.tag_configure("squeeze_warn", foreground="#ffb020")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # Distribution Chart
        dist_frame = ttk.LabelFrame(self, text="Price Change Distribution")
        dist_frame.pack(fill=tk.BOTH, expand=False, padx=5, pady=(0, 5))

        if _MPL_OK:
            self.figure, self.ax = plt.subplots(figsize=(7, 3), dpi=100)
            self.ax.set_title("Price Change Distribution")
            self.ax.set_ylabel("Number of symbols")

            self.canvas = FigureCanvasTkAgg(self.figure, master=dist_frame)
            self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

            bottom_labels = ttk.Frame(dist_frame)
            bottom_labels.pack(fill=tk.X, padx=4, pady=(0, 4))

            self.price_up_label = ttk.Label(bottom_labels, text="Price up: 0")
            self.price_up_label.pack(side=tk.LEFT)

            self.price_down_label = ttk.Label(bottom_labels, text="Price down: 0")
            self.price_down_label.pack(side=tk.RIGHT)
        else:
            ttk.Label(
                dist_frame,
                text="matplotlib not available — install to see distribution chart.",
                foreground="red",
            ).pack(padx=6, pady=6, anchor="w")

    # ──────────────────────────────────────────────────────────────
    # Queue Poller
    # ──────────────────────────────────────────────────────────────
    def _start_result_poller(self):
        self.after(200, self._poll_result_queue)

    def _poll_result_queue(self):
        try:
            while True:
                msg_type, payload = self._result_queue.get_nowait()
                if msg_type == "ok":
                    rows, dist, ts = payload

                    # ---- PATCH: keep Top20 cache updated for TelegramTab ----
                    self._set_top20_cache(rows)
                    # --------------------------------------------------------

                    self._update_table(rows)
                    self._update_distribution_chart(dist)
                    self._set_status(f"Last update: {ts}")
                elif msg_type == "error":
                    self._set_status(f"Error: {payload}")
        except queue.Empty:
            pass

        self.after(200, self._poll_result_queue)

    # ──────────────────────────────────────────────────────────────
    # Data Fetcher
    # ──────────────────────────────────────────────────────────────
    def _initial_load(self):
        self.refresh_now()
        if self.auto_refresh_var.get():
            self._schedule_next_refresh()

    def _toggle_auto_refresh(self):
        if self.auto_refresh_var.get():
            self._schedule_next_refresh()
        else:
            if self._refresh_job:
                try:
                    self.after_cancel(self._refresh_job)
                except Exception:
                    pass
                self._refresh_job = None
            self._set_status("Auto-refresh paused.")

    def _schedule_next_refresh(self):
        if self._refresh_job:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None

        interval = max(10, int(self.refresh_interval_var.get() or 60))
        self.refresh_interval_var.set(interval)
        self._refresh_job = self.after(interval * 1000, self.refresh_now)

    def refresh_now(self):
        # If fetch overlaps, do not stop auto-refresh: just retry later.
        if self._fetching:
            self._set_status("Still fetching... (auto-refresh will retry)")
            if self.auto_refresh_var.get():
                self._schedule_next_refresh()
            return

        self._fetching = True
        self._set_status("Fetching futures data...")

        t = threading.Thread(target=self._fetch_worker, daemon=True)
        t.start()

        if self.auto_refresh_var.get():
            self._schedule_next_refresh()

    def _fetch_worker(self):
        try:
            rows, dist = self._get_usd_futures_rankings(limit=20)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._result_queue.put(("ok", (rows, dist, ts)))
        except Exception as e:
            self._result_queue.put(("error", str(e)))
        finally:
            self._fetching = False

    # ──────────────────────────────────────────────────────────────
    # Binance API Work
    # ──────────────────────────────────────────────────────────────
    def _get_usd_futures_rankings(self, limit=20):
        s = self._session

        info_resp = s.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=10)
        info_resp.raise_for_status()
        info = info_resp.json()

        usd_perp_symbols = {
            sym["symbol"]
            for sym in info.get("symbols", [])
            if sym.get("contractType") == "PERPETUAL"
            and sym.get("quoteAsset") in ("USDT", "USD")
        }

        ticker_resp = s.get(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", timeout=10)
        ticker_resp.raise_for_status()
        tickers = ticker_resp.json()

        funding_resp = s.get(f"{BINANCE_FAPI}/fapi/v1/premiumIndex", timeout=10)
        funding_resp.raise_for_status()
        funding_raw = funding_resp.json()

        funding_map = {}
        for item in funding_raw:
            try:
                funding_map[item["symbol"]] = float(item.get("lastFundingRate", 0))
            except Exception:
                funding_map[item["symbol"]] = 0.0

        dist = {
            "up_0_3": 0, "up_3_5": 0, "up_5_7": 0, "up_7_10": 0, "up_10+": 0,
            "zero": 0,
            "down_0_3": 0, "down_3_5": 0, "down_5_7": 0, "down_7_10": 0, "down_10+": 0,
        }

        rows = []
        for t in tickers:
            symbol = t.get("symbol")
            if symbol not in usd_perp_symbols:
                continue

            try:
                price = float(t.get("lastPrice", "0"))
                change_pct = float(t.get("priceChangePercent", "0"))
            except Exception:
                continue

            funding_rate = funding_map.get(symbol, 0.0) * 100.0

            rows.append(
                {"symbol": symbol, "price": price, "funding": funding_rate, "change_pct": change_pct}
            )
            self._accumulate_distribution(dist, change_pct)

        dist["up_total"] = dist["up_0_3"] + dist["up_3_5"] + dist["up_5_7"] + dist["up_7_10"] + dist["up_10+"]
        dist["down_total"] = dist["down_0_3"] + dist["down_3_5"] + dist["down_5_7"] + dist["down_7_10"] + dist["down_10+"]

        rows.sort(key=lambda r: r["change_pct"], reverse=True)
        top_rows = rows[:limit]

        # L/S period aligned to "1m context" (same mapping rule as TrendingTab)
        # If later you add an interval selector to FuturesTab, pass it here.
        ls_period = self._LS_PERIOD_MAP.get("1m", "5m")

        for row in top_rows:
            sym = row["symbol"]

            try:
                row["adx_1m"] = self._get_adx_for_interval(sym, "1m")
            except Exception:
                row["adx_1m"] = None

            try:
                row["adx_15m"] = self._get_adx_for_interval(sym, "15m")
            except Exception:
                row["adx_15m"] = None

            try:
                coin_ls, trader_ls = self._get_ls_ratios(sym, period=ls_period)
            except Exception:
                coin_ls, trader_ls = None, None

            row["coin_ls"] = coin_ls
            row["trader_ls"] = trader_ls

        return top_rows, dist

    @staticmethod
    def _accumulate_distribution(dist, change_pct):
        if abs(change_pct) < 0.0001:
            dist["zero"] += 1
            return

        if change_pct > 0:
            c = change_pct
            if c <= 3:
                dist["up_0_3"] += 1
            elif c <= 5:
                dist["up_3_5"] += 1
            elif c <= 7:
                dist["up_5_7"] += 1
            elif c <= 10:
                dist["up_7_10"] += 1
            else:
                dist["up_10+"] += 1
        else:
            c = abs(change_pct)
            if c <= 3:
                dist["down_0_3"] += 1
            elif c <= 5:
                dist["down_3_5"] += 1
            elif c <= 7:
                dist["down_5_7"] += 1
            elif c <= 10:
                dist["down_7_10"] += 1
            else:
                dist["down_10+"] += 1

    def _get_adx_for_interval(self, symbol, interval, period=30, limit=120):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        resp = self._session.get(f"{BINANCE_FAPI}/fapi/v1/klines", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None

        high = [float(k[2]) for k in data]
        low = [float(k[3]) for k in data]
        close = [float(k[4]) for k in data]

        adx_vals = _adx(high, low, close, period=period)
        for val in reversed(adx_vals):
            if val is not None:
                return float(val)
        return None

    def _get_ls_ratios(self, symbol, period="5m"):
        """
        Coin L/S:   /futures/data/globalLongShortAccountRatio
        Trader L/S: /futures/data/topLongShortPositionRatio   (MATCHES TrendingTab)
        """
        params = {"symbol": symbol, "period": period, "limit": 1}

        coin_resp = self._session.get(
            f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
            params=params,
            timeout=8,
        )
        coin_resp.raise_for_status()
        coin_data = coin_resp.json()

        trader_resp = self._session.get(
            f"{BINANCE_FAPI}/futures/data/topLongShortPositionRatio",
            params=params,
            timeout=8,
        )
        trader_resp.raise_for_status()
        trader_data = trader_resp.json()

        coin_ls = float(coin_data[0]["longShortRatio"]) if coin_data else None
        trader_ls = float(trader_data[0]["longShortRatio"]) if trader_data else None
        return coin_ls, trader_ls

    # ──────────────────────────────────────────────────────────────
    # UI Helpers
    # ──────────────────────────────────────────────────────────────
    def _update_table(self, rows):
        for item in self.tree.get_children():
            self.tree.delete(item)

        for idx, row in enumerate(rows, start=1):
            symbol = row["symbol"]
            price = row["price"]
            change_pct = row["change_pct"]
            funding = row["funding"]

            adx_1m = row.get("adx_1m")
            adx_15m = row.get("adx_15m")
            coin_ls = row.get("coin_ls")
            trader_ls = row.get("trader_ls")

            is_squeeze = (
                trader_ls is not None and coin_ls is not None
                and trader_ls > 1.30 and coin_ls < 0.80
            )
            signal_text = "⚠ SQUEEZE" if is_squeeze else ""

            change_str = f"{change_pct:+.2f}%"
            price_str = f"{price:.8f}".rstrip("0").rstrip(".")
            funding_str = f"{funding:+.4f}%"

            adx1m_str = "" if adx_1m is None else f"{adx_1m:.2f}"
            adx15m_str = "" if adx_15m is None else f"{adx_15m:.2f}"

            coin_ls_str = "" if coin_ls is None else f"{coin_ls:.2f}"
            trader_ls_str = "" if trader_ls is None else f"{trader_ls:.2f}"

            if is_squeeze:
                tag = "squeeze_warn"
            elif trader_ls is not None and trader_ls > 1.30:
                tag = "trader_ls_long"
            elif trader_ls is not None and trader_ls < 0.80:
                tag = "trader_ls_short"
            elif funding > 0.0001:
                tag = "funding_pos"
            elif funding < -0.0001:
                tag = "funding_neg"
            else:
                tag = "funding_neutral"

            self.tree.insert(
                "",
                "end",
                values=(
                    idx,
                    symbol,
                    signal_text,
                    price_str,
                    funding_str,
                    coin_ls_str,
                    trader_ls_str,
                    adx1m_str,
                    adx15m_str,
                    change_str,
                ),
                tags=(tag,),
            )

    def _update_distribution_chart(self, dist):
        if not _MPL_OK or self.ax is None or self.canvas is None:
            return

        labels = [">10%", "7-10%", "5-7%", "3-5%", "0-3%", "0%", "0-3%", "3-5%", "5-7%", "7-10%", ">10%"]
        values = [
            dist.get("up_10+", 0),
            dist.get("up_7_10", 0),
            dist.get("up_5_7", 0),
            dist.get("up_3_5", 0),
            dist.get("up_0_3", 0),
            dist.get("zero", 0),
            dist.get("down_0_3", 0),
            dist.get("down_3_5", 0),
            dist.get("down_5_7", 0),
            dist.get("down_7_10", 0),
            dist.get("down_10+", 0),
        ]

        x = list(range(len(labels)))
        colors = (["#16c784"] * 5) + ["#808080"] + (["#f6465d"] * 5)

        self.ax.clear()
        self.ax.bar(x, values, color=colors)
        self.ax.set_xticks(x)
        self.ax.set_xticklabels(labels)
        self.ax.set_ylabel("Number of symbols")
        self.ax.set_title("Price Change Distribution")

        if values:
            max_val = max(values)
            self.ax.set_ylim(0, max_val * 1.15 if max_val > 0 else 1)

        for xi, val in zip(x, values):
            if val > 0:
                self.ax.text(xi, val, str(val), ha="center", va="bottom", fontsize=8)

        self.figure.tight_layout()
        self.canvas.draw_idle()

        if self.price_up_label is not None:
            self.price_up_label.config(text=f"Price up: {dist.get('up_total', 0)}")
        if self.price_down_label is not None:
            self.price_down_label.config(text=f"Price down: {dist.get('down_total', 0)}")

    def _set_status(self, text):
        self.status_label.config(text=text)

    # ──────────────────────────────────────────────────────────────
    # Top 20 Cache (for TelegramTab and other consumers)
    # ──────────────────────────────────────────────────────────────
    def _set_top20_cache(self, rows):
        """Store a normalized snapshot of the latest Top 20 rows.

        TelegramTab expects keys:
          - symbol (str)
          - adx_1m (float-like)
          - adx_15m (float-like)
          - rank (optional)
        """
        normalized = []
        try:
            for i, r in enumerate(rows or [], start=1):
                if not isinstance(r, dict):
                    continue
                symbol = (r.get("symbol") or r.get("Symbol") or "").upper().strip()
                if not symbol:
                    continue

                def _get(*keys):
                    for k in keys:
                        if k in r and r.get(k) is not None:
                            return r.get(k)
                    return None

                normalized.append({
                    "symbol": symbol,
                    "rank": _get("rank", "Rank") or i,
                    "adx_1m": _get("adx_1m", "ADX_1m", "ADX1m", "ADX(1m)", "adx1m"),
                    "adx_15m": _get("adx_15m", "ADX_15m", "ADX15m", "ADX(15m)", "adx15m"),
                    # extra fields (harmless for TelegramTab)
                    "price": _get("price", "Price"),
                    "change_pct": _get("change_pct", "Change%", "change", "chg"),
                    "funding": _get("funding", "Funding", "fundingRate"),
                    "coin_ls": _get("coin_ls", "Coin L/S", "coinLS", "coin_ls_ratio"),
                    "trader_ls": _get("trader_ls", "Trader L/S", "traderLS", "trader_ls_ratio"),
                })
        except Exception:
            normalized = []

        with self._top20_lock:
            self._top20_cache = normalized

    def get_top20_rows(self):
        """Public hook for TelegramTab.

        Returns a copy of the most recent Top 20 snapshot.
        """
        with self._top20_lock:
            return list(self._top20_cache)
