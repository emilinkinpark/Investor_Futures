import tkinter as tk
from tkinter import ttk

from ui.market_tab import MarketTab
from ui.order_tab import OrderTab

from api.websocket import BinanceWebSocketManager
from services.rest_client import rest_client

import requests
import hmac
import hashlib
import time
from threading import Thread

from config import BASE_URL, API_KEY, API_SECRET
from services.position_logger import PositionLogger


class TradingTab(tk.Frame):
    def __init__(self, master, ws_manager=None):
        super().__init__(master)

        self.current_symbol = None
        self.ws_manager = ws_manager
        self._owns_ws_manager = ws_manager is None

        self.price_data = {}
        self.ratio_data = {}
        self.cnd_rating = "N/A"
        self.positions = []

        self._mark_price_cache = {}
        self._mark_price_cache_last_ts = 0.0
        self._mark_price_updater_running = False
        self._positions_refresh_in_flight = False

        self.trending_tab = None

        self._prev_positions = {}

        self.margin_mode_var = tk.StringVar(value="Cross")
        self.isolated_margin_var = tk.StringVar(value="0")

        self.logger = PositionLogger()

        self.create_widgets()

        if self._owns_ws_manager:
            self.init_websocket()

        self._start_mark_price_cache_updater()

    # ------------------------------------------------------
    def set_trending_tab(self, trending_tab):
        self.trending_tab = trending_tab

    def log(self, msg: str, level: str = "INFO"):
        pass  # disabled

    # ------------------------------------------------------
    def create_widgets(self):
        left_frame = tk.Frame(self)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)

        right_frame = tk.Frame(self)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        self._create_trade_settings(left_frame)

        self.order_tab = OrderTab(left_frame, current_symbol=None, price_callback=self._price_for_symbol)
        self.order_tab.pack(fill=tk.BOTH, expand=True, pady=10)
        self._propagate_margin_mode()
        self._propagate_isolated_margin()

        self.market_tab = MarketTab(left_frame, on_symbol_select=self.on_symbol_select)
        self.market_tab.pack(fill=tk.BOTH, expand=True)

        self.symbol_info_frame = tk.LabelFrame(right_frame, text="Symbol Info")
        self.symbol_info_frame.pack(fill=tk.X, pady=5)

        self.positions_frame = tk.LabelFrame(right_frame, text="Current Positions")
        self.positions_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.create_positions_table()
        self.create_symbol_info_widgets()

    # ----- Trade settings -----
    def _create_trade_settings(self, parent):
        settings = tk.LabelFrame(parent, text="Trade Settings")
        settings.pack(fill=tk.X, pady=5)

        # Margin mode
        row1 = tk.Frame(settings)
        row1.pack(fill=tk.X, padx=6, pady=4)
        tk.Label(row1, text="Margin Mode:", width=14, anchor="w").pack(side=tk.LEFT)
        self.margin_mode_combo = ttk.Combobox(
            row1,
            textvariable=self.margin_mode_var,
            values=["Cross", "Isolated"],
            state="readonly",
            width=12,
        )
        self.margin_mode_combo.pack(side=tk.LEFT)
        self.margin_mode_combo.bind("<<ComboboxSelected>>", self._on_margin_mode_changed)

        # Isolated Amount
        row2 = tk.Frame(settings)
        row2.pack(fill=tk.X, padx=6, pady=4)
        self.iso_label = tk.Label(row2, text="Isolated Amount (USDT):", width=22, anchor="w")
        self.iso_label.pack(side=tk.LEFT)

        vcmd = (self.register(self._validate_usdt), "%P")
        self.iso_entry = tk.Entry(
            row2,
            textvariable=self.isolated_margin_var,
            validate="key",
            validatecommand=vcmd,
        )
        self.iso_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.iso_entry.bind("<FocusOut>", self._on_isolated_margin_changed)

        self._refresh_iso_enabled()

    def _validate_usdt(self, new):
        if new == "":
            return True
        try:
            return float(new) >= 0
        except:
            return False

    def _on_isolated_margin_changed(self, _e=None):
        try:
            val = float(self.isolated_margin_var.get() or "0")
        except ValueError:
            val = 0.0

        if hasattr(self.order_tab, "set_isolated_margin_usdt"):
            self.order_tab.set_isolated_margin_usdt(val)

    def _refresh_iso_enabled(self):
        is_iso = self.margin_mode_var.get() == "Isolated"
        self.iso_entry.configure(state="normal" if is_iso else "disabled")
        self.iso_label.configure(fg="white" if is_iso else "#888")

    def _on_margin_mode_changed(self, _e=None):
        self._refresh_iso_enabled()
        self._propagate_margin_mode()

    def _propagate_margin_mode(self):
        if hasattr(self.order_tab, "set_margin_mode"):
            self.order_tab.set_margin_mode(self.margin_mode_var.get())

    def _propagate_isolated_margin(self):
        self._on_isolated_margin_changed()

    # ------------------------------------------------------
    # Positions table
    # ------------------------------------------------------
    def create_positions_table(self):
        columns = ("symbol", "side", "size", "entry", "mark", "pnl", "roe", "leverage", "liquidation")
        self.positions_tree = ttk.Treeview(self.positions_frame, columns=columns, show="headings")

        for col, text in zip(
            columns,
            ["Symbol", "Side", "Size", "Entry Price", "Mark Price", "PnL", "ROE %", "Leverage", "Liq Price"],
        ):
            self.positions_tree.heading(col, text=text)

        for c, w in zip(columns, [80, 60, 100, 100, 100, 100, 80, 80, 100]):
            self.positions_tree.column(c, width=w)

        self.positions_tree.pack(fill=tk.BOTH, expand=True)

        btn_frame = tk.Frame(self.positions_frame)
        btn_frame.pack(fill=tk.X, pady=5)

        tk.Button(btn_frame, text="Refresh Positions", command=self.refresh_positions_async).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="View Trend", command=self._view_trend_from_trading).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Close Position", command=self.close_selected_position).pack(side=tk.LEFT, padx=5)

    # ------------------------------------------------------
    # Binance positions
    # ------------------------------------------------------
    def get_binance_signature(self, data: str) -> str:
        return hmac.new(API_SECRET.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()

    def _parse_position_list(self, data):
        """
        Parse Binance positionRisk list into our internal structure.

        Important: liquidationPrice can be "", None, "0", etc.
        We must NEVER let float("") crash and silently drop valid positions.
        """
        positions = []

        for pos in data:
            # ---- position amount ----
            try:
                amt = float(pos.get("positionAmt", 0))
            except Exception:
                amt = 0.0

            if amt == 0.0:
                continue  # skip flat positions

            # ---- entry / mark / leverage ----
            try:
                entry = float(pos.get("entryPrice") or 0.0)
            except Exception:
                entry = 0.0

            try:
                mark = float(pos.get("markPrice") or 0.0)
            except Exception:
                mark = 0.0

            try:
                lev = float(pos.get("leverage") or 0)
            except Exception:
                lev = 0.0

            # ---- liquidation price (safe) ----
            liq_raw = pos.get("liquidationPrice")
            try:
                liquidation = float(liq_raw) if liq_raw not in (None, "", "0", "0.0") else 0.0
            except Exception:
                liquidation = 0.0

            # ---- PnL / side / margin / ROE ----
            if amt > 0:
                side = "LONG"
                pnl = (mark - entry) * amt
            else:
                side = "SHORT"
                pnl = (entry - mark) * abs(amt)

            notional = abs(amt) * entry
            margin = notional / max(lev, 1.0)
            roe = (pnl / margin * 100.0) if margin > 0 else 0.0

            positions.append(
                {
                    "symbol": pos.get("symbol", ""),
                    "side": side,
                    "size": abs(amt),
                    "entry": entry,
                    "mark": mark,
                    "pnl": pnl,
                    "roe": roe,
                    "leverage": lev,
                    "liquidation": liquidation,
                }
            )

        return positions

    def get_positions(self):
        # Try rest_client
        try:
            if hasattr(rest_client, "get_position_risk"):
                data = rest_client.get_position_risk()
                return self._parse_position_list(data)
            elif hasattr(rest_client, "get_positions"):
                data = rest_client.get_positions()
                return self._parse_position_list(data)
        except Exception:
            pass  # silent

        # Raw fallback
        try:
            ts = int(time.time() * 1000)
            qs = f"timestamp={ts}"
            sig = self.get_binance_signature(qs)
            headers = {"X-MBX-APIKEY": API_KEY}

            resp = requests.get(
                f"{BASE_URL}/fapi/v2/positionRisk",
                params={"timestamp": ts, "signature": sig},
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                resp.raise_for_status()
            return self._parse_position_list(resp.json())
        except Exception:
            return []

    # ------------------------------------------------------
    def _log_position_deltas(self, new_positions):
        new_map = {p["symbol"]: p for p in new_positions}
        prev_map = self._prev_positions

        for sym, prev in prev_map.items():
            if sym not in new_map:
                self.logger.log_close(sym, prev)

        for sym, curr in new_map.items():
            if sym not in prev_map:
                self.logger.log_open(sym, curr)
            else:
                prev = prev_map[sym]
                if (
                    curr["side"] != prev["side"]
                    or abs(curr["size"] - prev["size"]) > 1e-10
                    or abs(curr["entry"] - prev["entry"]) > 1e-10
                ):
                    self.logger.log_change(sym, prev, curr)

        self._prev_positions = new_map

    def refresh_positions_async(self):
        if self._positions_refresh_in_flight:
            return
        self._positions_refresh_in_flight = True

        def worker():
            positions = self.get_positions()
            try:
                self.after(0, lambda: self._apply_positions_to_ui(positions))
            except Exception:
                pass

        Thread(target=worker, daemon=True).start()

    def _apply_positions_to_ui(self, positions):
        self._positions_refresh_in_flight = False
        self.positions = positions

        try:
            self._log_position_deltas(positions)
        except Exception:
            pass

        for item in self.positions_tree.get_children():
            self.positions_tree.delete(item)

        for pos in positions:
            try:
                values = (
                    str(pos.get("symbol", "")),
                    str(pos.get("side", "")),
                    str(pos.get("size", "")),
                    str(pos.get("entry", "")),
                    str(pos.get("mark", "")),
                    str(pos.get("pnl", "")),
                    str(pos.get("roe", "")),
                    str(pos.get("leverage", "")),
                    str(pos.get("liquidation", "")),
                )
                self.positions_tree.insert("", tk.END, values=values)
            except Exception:
                pass

    # ------------------------------------------------------
    def _price_for_symbol(self, symbol: str):
        if self.current_symbol == symbol and self.price_data:
            try:
                return float(self.price_data.get("current_price", 0.0))
            except Exception:
                return 0.0
        try:
            return float(self._mark_price_cache.get(symbol, 0.0))
        except Exception:
            return 0.0

    # ------------------------------------------------------
    def init_websocket(self):
        self.ws_manager = BinanceWebSocketManager(on_message_callback=self.handle_ws_message)
        self.ws_manager.start()

    def handle_ws_message(self, data):
        if data.get("e") == "24hrTicker":
            self.handle_ticker_update(data)
        elif data.get("e") == "aggTrade":
            self.handle_price_update(data)

    def handle_ticker_update(self, data):
        if data.get("s") == self.current_symbol:
            self.price_data["current_price"] = float(data.get("c", 0.0))
            self.price_data["price_change"] = float(data.get("P", 0.0))
            self.update_symbol_info()

    def handle_price_update(self, data):
        if data.get("s") == self.current_symbol:
            self.price_data["current_price"] = float(data.get("p", 0.0))
            self.update_symbol_info()

    # ------------------------------------------------------
    def _start_mark_price_cache_updater(self):
        if self._mark_price_updater_running:
            return
        self._mark_price_updater_running = True

        def worker():
            while self._mark_price_updater_running:
                try:
                    prices = rest_client.get_mark_price_all()
                    self._mark_price_cache = {p["symbol"]: float(p["markPrice"]) for p in prices}
                except Exception:
                    pass
                time.sleep(3)

        Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------
    def update_symbol_info(self):
        if hasattr(self, "price_label") and self.price_data:
            price = self.price_data.get("current_price", 0.0)
            change = self.price_data.get("price_change", 0.0)
            self.price_label.config(text=f"Price: {price:.4f} ({change:+.2f}%)")

        if hasattr(self, "ratio_label") and self.ratio_data:
            ratio = self.ratio_data.get("long_short_ratio", "N/A")
            self.ratio_label.config(text=f"L/S Ratio: {ratio}")

        if hasattr(self, "cnd_label"):
            self.cnd_label.config(text=f"Coin CnD Rating: {self.cnd_rating}")

    def create_symbol_info_widgets(self):
        for w in self.symbol_info_frame.winfo_children():
            w.destroy()

        curr = self.current_symbol or "N/A"

        tk.Label(self.symbol_info_frame, text=f"Symbol: {curr}").pack(anchor="w")

        self.price_label = tk.Label(self.symbol_info_frame, text="Price: N/A")
        self.price_label.pack(anchor="w")

        self.ratio_label = tk.Label(self.symbol_info_frame, text="L/S Ratio: N/A")
        self.ratio_label.pack(anchor="w")

        ratio_frame = tk.Frame(self.symbol_info_frame)
        ratio_frame.pack(anchor="w", pady=(5, 0))
        tk.Label(ratio_frame, text="Coin CnD Rating:").pack(side=tk.LEFT)

        self.cnd_label = tk.Label(ratio_frame, text="-")
        self.cnd_label.pack(side=tk.LEFT, padx=(5, 0))

        Thread(target=self.fetch_ratio_data, daemon=True).start()

    def fetch_ratio_data(self):
        self.ratio_data = {"long_short_ratio": "N/A"}
        self.cnd_rating = "N/A"

    # ------------------------------------------------------
    def _selected_position(self):
        sel = self.positions_tree.selection()
        if not sel:
            return None
        vals = self.positions_tree.item(sel[0], "values")
        if not vals:
            return None
        try:
            return {
                "symbol": vals[0],
                "side": vals[1],
                "size": float(vals[2]),
                "entry": float(vals[3]),
                "mark": float(vals[4]),
                "pnl": float(vals[5]),
                "leverage": float(vals[7]),
                "liquidation": float(vals[8]),
            }
        except Exception:
            return {"symbol": vals[0]}

    def _view_trend_from_trading(self):
        pos = self._selected_position()
        if not pos:
            return
        symbol = pos["symbol"]
        if self.trending_tab and hasattr(self.trending_tab, "focus_on_symbol_from_trading"):
            try:
                self.trending_tab.focus_on_symbol_from_trading(symbol)
            except Exception:
                pass

    def close_selected_position(self):
        pass  # intentionally disabled noisy behavior

    # ------------------------------------------------------
    def on_symbol_select(self, symbol):
        if self.current_symbol and self.ws_manager and self._owns_ws_manager:
            try:
                self.ws_manager.unsubscribe(self.current_symbol)
            except Exception:
                pass

        self.current_symbol = symbol
        self.price_data = {}
        self.ratio_data = {}
        self.cnd_rating = "N/A"
        self.create_symbol_info_widgets()
        self.order_tab.update_symbol(symbol)
        self._propagate_margin_mode()
        self._propagate_isolated_margin()

        if symbol and self.ws_manager:
            try:
                self.ws_manager.subscribe(symbol)
            except Exception:
                pass

    def destroy(self):
        if self.ws_manager and self._owns_ws_manager:
            try:
                self.ws_manager.stop()
            except Exception:
                pass
        self._mark_price_updater_running = False
        super().destroy()
