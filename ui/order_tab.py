import tkinter as tk
from tkinter import ttk, messagebox
from config import API_KEY, API_SECRET
import requests
import hmac
import hashlib
import time
import threading
import math

from utils.threading import run_io_bound  # use shared IO threadpool

BASE_URL = "https://fapi.binance.com"


class OrderTab(tk.Frame):
    def __init__(self, master, current_symbol=None, price_callback=None):
        super().__init__(master)
        self.current_symbol = current_symbol
        self.position_side = "LONG"
        self.order_type = "MARKET"

        # Margin integration
        self.margin_mode = "Cross"            # "Cross" | "Isolated"
        self.isolated_margin_usdt = 0.0       # user input from TradingTab
        self.isolated_input_mode = "Notional (USDT)"  # or "Margin (USDT)"
        self.price_callback = price_callback  # callback provided by TradingTab

        self.price_entry = None
        self.qty_entry = None
        self.usdt_entry = None
        self.tp_entry = None
        self.sl_entry = None

        self.tp_var = tk.BooleanVar(value=False)
        self.sl_var = tk.BooleanVar(value=False)

        self.price_display = None
        self.current_price = 0.0
        self.price_updater = None

        # Exchange info cache (LOT_SIZE, MIN_NOTIONAL, etc.), preloaded in background
        self._exchange_cache = {}

        self.create_widgets()
        self.start_price_updater()

    # ---------------- UI ----------------
    def create_widgets(self):
        main_frame = tk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.price_display = tk.Label(main_frame, text="Price: -", font=("Segoe UI", 12, "bold"))
        self.price_display.pack(anchor=tk.W, pady=(0, 8))

        control_frame = tk.LabelFrame(main_frame, text="Position Control", padx=5, pady=5)
        control_frame.pack(fill=tk.X, pady=5)

        # Side
        side_frame = tk.Frame(control_frame)
        side_frame.pack(fill=tk.X, pady=5)
        tk.Label(side_frame, text="Side:").pack(side=tk.LEFT)
        self.side_var = tk.StringVar(value=self.position_side)
        tk.Radiobutton(
            side_frame,
            text="Long",
            variable=self.side_var,
            value="LONG",
            command=self.update_position_side
        ).pack(side=tk.LEFT, padx=5)
        tk.Radiobutton(
            side_frame,
            text="Short",
            variable=self.side_var,
            value="SHORT",
            command=self.update_position_side
        ).pack(side=tk.LEFT, padx=5)

        # Order type
        type_frame = tk.Frame(control_frame)
        type_frame.pack(fill=tk.X, pady=5)
        tk.Label(type_frame, text="Order Type:").pack(side=tk.LEFT)
        self.type_var = tk.StringVar(value=self.order_type)
        tk.Radiobutton(
            type_frame,
            text="Market",
            variable=self.type_var,
            value="MARKET",
            command=self.update_order_type
        ).pack(side=tk.LEFT, padx=5)
        tk.Radiobutton(
            type_frame,
            text="Limit",
            variable=self.type_var,
            value="LIMIT",
            command=self.update_order_type
        ).pack(side=tk.LEFT, padx=5)

        # Price
        price_frame = tk.Frame(control_frame)
        price_frame.pack(fill=tk.X, pady=5)
        tk.Label(price_frame, text="Price:").pack(side=tk.LEFT)
        self.price_entry = tk.Entry(price_frame, state=tk.DISABLED)
        self.price_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        price_btn_frame = tk.Frame(control_frame)
        price_btn_frame.pack(fill=tk.X, pady=2)
        tk.Button(price_btn_frame, text="-20%", command=lambda: self.adjust_price(-20)).pack(side=tk.LEFT)
        tk.Button(price_btn_frame, text="-10%", command=lambda: self.adjust_price(-10)).pack(side=tk.LEFT)
        tk.Button(price_btn_frame, text="SET", command=self.set_current_price).pack(side=tk.LEFT, padx=10)
        tk.Button(price_btn_frame, text="+10%", command=lambda: self.adjust_price(10)).pack(side=tk.LEFT)
        tk.Button(price_btn_frame, text="+20%", command=lambda: self.adjust_price(20)).pack(side=tk.LEFT)

        # Quantity
        qty_frame = tk.Frame(control_frame)
        qty_frame.pack(fill=tk.X, pady=5)
        tk.Label(qty_frame, text="Quantity:").pack(side=tk.LEFT)
        self.qty_entry = tk.Entry(qty_frame)
        self.qty_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # USDT value helper (Cross converter)
        usdt_frame = tk.Frame(control_frame)
        usdt_frame.pack(fill=tk.X, pady=5)
        tk.Label(usdt_frame, text="USDT Value:").pack(side=tk.LEFT)
        self.usdt_entry = tk.Entry(usdt_frame)
        self.usdt_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.usdt_entry.bind("<KeyRelease>", self.calculate_quantity_from_usdt)
        self.qty_entry.bind("<KeyRelease>", self.calculate_usdt_from_quantity)

        # Leverage
        leverage_frame = tk.Frame(control_frame)
        leverage_frame.pack(fill=tk.X, pady=5)
        tk.Label(leverage_frame, text="Leverage:").pack(side=tk.LEFT)
        self.leverage_var = tk.StringVar(value="10")
        leverage_options = ["1", "2", "5", "10", "20", "50", "100"]
        self.leverage_menu = ttk.Combobox(
            leverage_frame,
            textvariable=self.leverage_var,
            values=leverage_options,
            state="readonly"
        )
        self.leverage_menu.pack(side=tk.LEFT, padx=5)
        tk.Button(leverage_frame, text="Set Leverage", command=self.set_leverage).pack(side=tk.LEFT, padx=10)

        # TP/SL
        tpsl_frame = tk.LabelFrame(main_frame, text="TP/SL", padx=5, pady=5)
        tpsl_frame.pack(fill=tk.X, pady=5)

        tp_frame = tk.Frame(tpsl_frame)
        tp_frame.pack(fill=tk.X, pady=5)
        tk.Checkbutton(
            tp_frame,
            text="Take Profit",
            variable=self.tp_var,
            command=self.toggle_take_profit
        ).pack(side=tk.LEFT)
        self.tp_entry = tk.Entry(tp_frame, state=tk.DISABLED)
        self.tp_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        sl_frame = tk.Frame(tpsl_frame)
        sl_frame.pack(fill=tk.X, pady=5)
        tk.Checkbutton(
            sl_frame,
            text="Stop Loss",
            variable=self.sl_var,
            command=self.toggle_stop_loss
        ).pack(side=tk.LEFT)
        self.sl_entry = tk.Entry(sl_frame, state=tk.DISABLED)
        self.sl_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # Actions
        action_frame = tk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=10)
        tk.Button(action_frame, text="Place Order", command=self.place_order).pack(side=tk.LEFT, padx=5)
        tk.Button(action_frame, text="Close Position", command=self.close_position).pack(side=tk.LEFT, padx=5)
        tk.Button(action_frame, text="Refresh", command=self.force_refresh).pack(side=tk.LEFT, padx=5)

    # ---------- Price updater ----------
    def start_price_updater(self):
        # still use Timer, but NO HTTP here – only the callback/current_price
        if self.price_updater:
            self.price_updater.cancel()
        self.price_updater = threading.Timer(1.0, self.update_price_display)
        self.price_updater.daemon = True
        self.price_updater.start()

    def update_price_display(self):
        if self.current_symbol:
            # Always prefer price_callback (TradingTab provides websocket/mark cache)
            if self.price_callback:
                try:
                    price = self.price_callback(self.current_symbol)
                except Exception:
                    price = None
            else:
                price = None  # no direct HTTP fallback here

            if price is not None and price > 0 and price != self.current_price:
                color = 'green' if price >= self.current_price else 'red'
                self.current_price = price
                price_text = f"Price: {price:.6f}"
                self.price_display.config(text=price_text, fg=color)

                if self.order_type == "LIMIT" and not self.price_entry.get():
                    self.price_entry.config(state=tk.NORMAL)
                    self.price_entry.delete(0, tk.END)
                    self.price_entry.insert(0, f"{price:.6f}")
                    self.price_entry.config(state=tk.NORMAL)

        self.start_price_updater()

    def update_position_side(self):
        self.position_side = self.side_var.get()

    def update_order_type(self):
        self.order_type = self.type_var.get()
        if self.order_type == "LIMIT":
            self.price_entry.config(state=tk.NORMAL)
        else:
            self.price_entry.delete(0, tk.END)
            self.price_entry.config(state=tk.DISABLED)

    def adjust_price(self, percent):
        if self.order_type != "LIMIT":
            messagebox.showinfo("Info", "Price can be set only for LIMIT orders")
            return
        try:
            base_price = float(self.price_entry.get() if self.price_entry.get() else self.current_price)
            if base_price <= 0:
                raise ValueError
            adj = base_price * (1 + percent / 100.0)
            adj = self.format_price(adj, self.current_symbol)
            self.price_entry.delete(0, tk.END)
            self.price_entry.insert(0, f"{adj}")
        except Exception:
            messagebox.showerror("Error", "Invalid price to adjust")

    def set_current_price(self):
        if self.order_type != "LIMIT":
            messagebox.showinfo("Info", "Price can be set only for LIMIT orders")
            return
        if self.current_price > 0:
            self.price_entry.config(state=tk.NORMAL)
            self.price_entry.delete(0, tk.END)
            self.price_entry.insert(0, f"{self.format_price(self.current_price, self.current_symbol)}")
            self.price_entry.config(state=tk.NORMAL)
        else:
            messagebox.showwarning("Warning", "No current price available")

    # ---------- Conversions ----------
    def calculate_quantity_from_usdt(self, _event=None):
        try:
            usdt = float(self.usdt_entry.get() or 0)
            price = self._active_price_for_calc()
            if price <= 0:
                return
            raw_qty = usdt / price
            qty = self.format_quantity(raw_qty, self.current_symbol)
            self.qty_entry.delete(0, tk.END)
            self.qty_entry.insert(0, f"{qty}")
            self._update_usdt_display(qty, price)
        except ValueError:
            pass

    def calculate_usdt_from_quantity(self, _event=None):
        try:
            qty = float(self.qty_entry.get() or 0)
            price = self._active_price_for_calc()
            if price <= 0:
                return
            qty = self.format_quantity(qty, self.current_symbol)
            self.qty_entry.delete(0, tk.END)
            self.qty_entry.insert(0, f"{qty}")
            self._update_usdt_display(qty, price)
        except ValueError:
            pass

    def _update_usdt_display(self, qty, price):
        usdt = qty * price
        self.usdt_entry.delete(0, tk.END)
        self.usdt_entry.insert(0, f"{usdt:.2f}")

    def _active_price_for_calc(self):
        if self.order_type == "LIMIT" and self.price_entry.get():
            return float(self.price_entry.get())
        return float(self.current_price or 0)

    # ---------- TP/SL toggles ----------
    def toggle_take_profit(self):
        self.tp_entry.config(state=tk.NORMAL if self.tp_var.get() else tk.DISABLED)

    def toggle_stop_loss(self):
        self.stop_loss_enabled = self.sl_var.get()
        self.sl_entry.config(state=tk.NORMAL if self.stop_loss_enabled else tk.DISABLED)

    # ---------- Setters from TradingTab ----------
    def set_margin_mode(self, mode: str):
        self.margin_mode = mode or "Cross"

    def set_isolated_margin_usdt(self, value: float):
        try:
            self.isolated_margin_usdt = float(value)
        except Exception:
            self.isolated_margin_usdt = 0.0

    def set_isolated_input_mode(self, mode: str):
        self.isolated_input_mode = mode or "Notional (USDT)"

    # ---------- Leverage ----------
    def set_leverage(self):
        leverage = self.leverage_var.get()
        if not self.current_symbol:
            messagebox.showerror("Error", "No symbol selected")
            return
        try:
            self._signed_post("/fapi/v1/leverage", {"symbol": self.current_symbol, "leverage": leverage})
            messagebox.showinfo("Success", f"Leverage set to {leverage}x for {self.current_symbol}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to set leverage: {str(e)}")

    # ---------- Signed helpers ----------
    def _signed_post(self, path: str, params: dict):
        """Low-level signed POST. Used for trading endpoints (orders, leverage, marginType)."""
        ts = int(time.time() * 1000)
        params = dict(params or {})
        params["timestamp"] = ts
        q = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        sig = hmac.new(API_SECRET.encode('utf-8'), q.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': API_KEY}
        resp = requests.post(f"{BASE_URL}{path}", params=f"{q}&signature={sig}", headers=headers, timeout=10)
        resp.raise_for_status()
        return resp

    # ---------- Order flow ----------
    def place_order(self):
        if not self.current_symbol:
            messagebox.showerror("Error", "No symbol selected")
            return

        try:
            # Ensure leverage & marginType are applied first
            lev = int(float(self.leverage_var.get()))
            self._signed_post("/fapi/v1/leverage", {"symbol": self.current_symbol, "leverage": lev})

            # margin type
            mt = "ISOLATED" if self.margin_mode == "Isolated" else "CROSSED"
            try:
                self._signed_post("/fapi/v1/marginType", {"symbol": self.current_symbol, "marginType": mt})
            except requests.exceptions.HTTPError as e:
                # -4046 already in this margin type; treat as OK
                try:
                    data = e.response.json()
                    if data.get("code") not in (-4046, -4048):
                        raise
                except Exception:
                    raise

            # Determine price for notional/limit validation
            if self.order_type == "LIMIT":
                price = self.validate_price()
                price = self.format_price(price, self.current_symbol)
            else:
                # For MARKET, rely on current websocket/mark price from TradingTab
                price = float(self.current_price or 0.0)
            if price <= 0:
                raise ValueError("No valid mark price to compute quantity")

            # ---- Quantity sizing ----
            if self.margin_mode == "Isolated":
                if self.isolated_margin_usdt <= 0:
                    raise ValueError("Enter a positive Isolated Amount (USDT)")

                if (self.isolated_input_mode or "").startswith("Margin"):
                    desired_notional = self.isolated_margin_usdt * max(lev, 1)
                    user_margin = self.isolated_margin_usdt
                else:
                    # Default: treat input as final notional, regardless of leverage
                    desired_notional = self.isolated_margin_usdt
                    user_margin = desired_notional / max(lev, 1)

                raw_qty = desired_notional / price
                quantity = self.format_quantity(raw_qty, self.current_symbol)
            else:
                # Cross: use the USDT box (if filled) or quantity box
                try:
                    usdt_box = float(self.usdt_entry.get() or 0.0)
                except ValueError:
                    usdt_box = 0.0
                if usdt_box > 0:
                    raw_qty = usdt_box / price
                    quantity = self.format_quantity(raw_qty, self.current_symbol)
                    desired_notional = usdt_box
                else:
                    quantity = self.validate_quantity()
                    quantity = self.format_quantity(quantity, self.current_symbol)
                    desired_notional = quantity * price
                user_margin = None  # N/A for Cross

            if quantity <= 0:
                raise ValueError("Computed quantity is too small for this symbol")

            # Enforce exchange minimums (symbol filters are preloaded in background)
            min_qty = self.get_min_qty(self.current_symbol)
            min_notional = self.get_min_notional(self.current_symbol)  # often ~5.0
            notional = price * quantity

            if quantity < min_qty:
                quantity = self._ceil_to_step(min_qty, self.get_step_size(self.current_symbol))
                notional = price * quantity

            if notional < min_notional:
                step = self.get_step_size(self.current_symbol)
                needed = self._ceil_to_step(max(min_notional / max(price, 1e-12), min_qty), step)
                self.qty_entry.delete(0, tk.END)
                self.qty_entry.insert(0, f"{needed}")
                self._update_usdt_display(needed, price)
                raise ValueError(
                    f"Order notional {notional:.4f} < minimum {min_notional:.2f}. "
                    f"Adjusted quantity to {needed}."
                )

            # TP/SL values (optional)
            tp_price = self.validate_tp_sl('take profit') if self.tp_var.get() else None
            sl_price = self.validate_tp_sl('stop loss') if self.sl_var.get() else None
            if tp_price:
                tp_price = self.format_price(tp_price, self.current_symbol)
            if sl_price:
                sl_price = self.format_price(sl_price, self.current_symbol)

            # Confirmation
            extra = []
            if self.margin_mode == "Isolated":
                extra.append(f"Leverage: {lev}x")
                extra.append(f"Initial Margin: {user_margin:.2f} USDT")
                extra.append(f"Input Mode: {self.isolated_input_mode}")
            if not self.show_confirmation(
                quantity,
                (price if self.order_type == "LIMIT" else None),
                tp_price,
                sl_price,
                notional,
                extra_lines=extra
            ):
                return

            # Prepare and send order
            params = {
                'symbol': self.current_symbol,
                'side': 'BUY' if self.position_side == 'LONG' else 'SELL',
                'type': self.order_type,
                'quantity': quantity,
            }
            if self.order_type == "LIMIT":
                params['price'] = price
                params['timeInForce'] = 'GTC'

            # NOTE: If you run hedge mode, add positionSide here

            resp = self._signed_post("/fapi/v1/order", params)
            order_data = resp.json()
            self.show_order_success(order_data)

        except ValueError as e:
            messagebox.showerror("Input Error", str(e))
        except requests.exceptions.RequestException as e:
            self.handle_api_error(e, {'symbol': self.current_symbol})
        except Exception as e:
            messagebox.showerror("System Error", f"Unexpected error: {str(e)}")

    def validate_quantity(self):
        try:
            quantity = float(self.qty_entry.get())
            if quantity <= 0:
                raise ValueError("Quantity must be positive")
            return quantity
        except ValueError:
            raise ValueError("Invalid quantity format")

    def validate_price(self):
        try:
            price = float(self.price_entry.get())
            if price <= 0:
                raise ValueError("Price must be positive")
            return price
        except ValueError:
            raise ValueError("Invalid price format")

    def validate_tp_sl(self, tp_sl_type):
        entry = self.tp_entry if tp_sl_type == 'take profit' else self.sl_entry
        try:
            value = float(entry.get())
            if value <= 0:
                raise ValueError(f"{tp_sl_type.title()} must be positive")
            return value
        except ValueError:
            raise ValueError(f"Invalid {tp_sl_type} format")

    def show_confirmation(self, quantity, price, tp_price, sl_price, notional=None, extra_lines=None):
        confirm_msg = f"Confirm {self.position_side} position for {self.current_symbol}\n\n"
        confirm_msg += f"Mode: {self.margin_mode}\n"
        confirm_msg += f"Type: {self.order_type}\n"
        confirm_msg += f"Quantity: {quantity}\n"
        if notional is not None:
            confirm_msg += f"Notional: {notional:.2f} USDT\n"
        if price is not None:
            confirm_msg += f"Price: {price}\n"
        if tp_price is not None:
            confirm_msg += f"Take Profit: {tp_price}\n"
        if sl_price is not None:
            confirm_msg += f"Stop Loss: {sl_price}\n"
        if extra_lines:
            for line in extra_lines:
                confirm_msg += f"{line}\n"
        return messagebox.askyesno("Confirm Order", confirm_msg)

    # ---------- Error handler ----------
    def show_order_success(self, order_data):
        msg = (
            f"Order placed successfully!\n\n"
            f"Symbol: {order_data.get('symbol')}\n"
            f"Side: {order_data.get('side')}\n"
            f"Type: {order_data.get('type')}\n"
            f"Status: {order_data.get('status')}\n"
        )
        messagebox.showinfo("Order Success", msg)

    def handle_api_error(self, error, params):
        error_msg = "Binance API Error\n\n"
        if hasattr(error, 'response') and error.response is not None:
            try:
                error_data = error.response.json()
                code = error_data.get('code', 'N/A')
                msg = error_data.get('msg', 'N/A')
                error_msg += f"Error Code: {code}\nMessage: {msg}\n"

                # Helpful details – use cached symbol info only (no network here)
                symbol_info = self.get_symbol_info(params.get('symbol'))
                if symbol_info:
                    step = self.get_step_size(params['symbol'])
                    tick = self.get_tick_size(params['symbol'])
                    min_qty = self.get_min_qty(params['symbol'])
                    min_notional = self.get_min_notional(params['symbol'])
                    error_msg += (
                        f"\nStep size: {step}\nTick size: {tick}\n"
                        f"Min qty: {min_qty}\nMin notional: {min_notional}\n"
                    )

                if code in (-1013, -4164):
                    error_msg += "\nHint: Increase quantity or use Close Position (reduce only) for sub-$5 exits."
            except ValueError:
                error_msg += f"Response: {error.response.text[:200]}\n"
        else:
            error_msg += str(error)
        messagebox.showerror("Order Error", error_msg)

    # ---------- Exchange info & formatting ----------
    def get_symbol_info(self, symbol):
        """Return cached exchangeInfo for symbol (no network here)."""
        if not symbol:
            return None
        return self._exchange_cache.get(symbol)

    def _preload_symbol_info(self, symbol):
        """Fetch exchangeInfo in a background thread and cache it.

        This avoids blocking the Tk main thread later when we need LOT_SIZE / MIN_NOTIONAL.
        """
        if not symbol or symbol in self._exchange_cache:
            return

        def _worker():
            self._fetch_symbol_info_sync(symbol)

        run_io_bound(_worker)

    def _fetch_symbol_info_sync(self, symbol):
        """Synchronous exchangeInfo fetch – only called from background worker."""
        try:
            resp = requests.get(
                f'{BASE_URL}/fapi/v1/exchangeInfo',
                params={'symbol': symbol},
                timeout=8
            )
            resp.raise_for_status()
            data = resp.json()
            info = next(item for item in data.get('symbols', []) if item.get('symbol') == symbol)
            self._exchange_cache[symbol] = info
        except Exception as e:
            print(f"[OrderTab] Failed to preload symbol info for {symbol}: {e}")

    def _filters(self, symbol):
        info = self.get_symbol_info(symbol)
        f = {}
        if not info:
            return f
        for fil in info.get('filters', []):
            f[fil.get('filterType')] = fil
        return f

    def get_step_size(self, symbol):
        f = self._filters(symbol).get('LOT_SIZE', {})
        return float(f.get('stepSize', 0.001))

    def get_min_qty(self, symbol):
        f = self._filters(symbol).get('LOT_SIZE', {})
        return float(f.get('minQty', 0.0))

    def get_tick_size(self, symbol):
        f = self._filters(symbol).get('PRICE_FILTER', {})
        return float(f.get('tickSize', 0.0001))

    def get_min_notional(self, symbol):
        f = self._filters(symbol).get('MIN_NOTIONAL', {})
        return float(f.get('notional', 5.0))

    def _ceil_to_step(self, value, step):
        if step <= 0:
            return value
        return math.ceil(value / step) * step

    def format_quantity(self, quantity, symbol):
        step = self.get_step_size(symbol)
        if step <= 0:
            return float(quantity)
        steps = math.floor(float(quantity) / step)
        return round(steps * step, 12)

    def format_price(self, price, symbol):
        tick = self.get_tick_size(symbol)
        if tick <= 0:
            return float(price)
        ticks = math.floor(float(price) / tick)
        return round(ticks * tick, 12)

    # ---------- Close position (reduce-only) ----------
    def close_position(self):
        if not self.current_symbol:
            messagebox.showerror("Error", "No symbol selected")
            return

        try:
            headers = {'X-MBX-APIKEY': API_KEY}
            ts = int(time.time() * 1000)
            params = {'timestamp': ts, 'recvWindow': 5000}
            q = '&'.join([f"{k}={v}" for k, v in params.items()])
            sig = hmac.new(API_SECRET.encode('utf-8'), q.encode('utf-8'), hashlib.sha256).hexdigest()
            params['signature'] = sig

            resp = requests.get(
                f'{BASE_URL}/fapi/v2/positionRisk',
                params=params,
                headers=headers,
                timeout=8
            )
            resp.raise_for_status()
            positions = resp.json()
            position = next(
                (p for p in positions
                 if p['symbol'] == self.current_symbol and float(p['positionAmt']) != 0.0),
                None
            )
            if not position:
                messagebox.showinfo("Info", f"No open position for {self.current_symbol}")
                return

            quantity = abs(float(position['positionAmt']))
            quantity = self.format_quantity(quantity, self.current_symbol)
            if quantity <= 0:
                messagebox.showinfo("Info", "Nothing to close.")
                return

            side = 'SELL' if float(position['positionAmt']) > 0 else 'BUY'
            close_params = {
                'symbol': self.current_symbol,
                'side': side,
                'type': 'MARKET',
                'reduceOnly': 'true',
                'quantity': quantity,
            }
            self._signed_post("/fapi/v1/order", close_params)
            messagebox.showinfo("Success", f"Position closed successfully for {self.current_symbol}")
        except requests.exceptions.RequestException as e:
            self.handle_api_error(e, {'symbol': self.current_symbol})
        except Exception as e:
            messagebox.showerror("Error", f"Failed to close position: {str(e)}")

    # ---------- Misc ----------
    def force_refresh(self):
        # Just ask the price updater to run once immediately
        self.update_price_display()

    def update_symbol(self, symbol):
        self.current_symbol = symbol
        if symbol:
            self.current_price = 0.0
            self.price_display.config(text="Price: -", fg='black')
            # Preload symbol filters in the background (no UI blocking)
            self._preload_symbol_info(symbol)
            self.force_refresh()

    def destroy(self):
        if self.price_updater:
            self.price_updater.cancel()
        super().destroy()
