# websocket.py (safe UI dispatch + resilient reconnect + resubscribe)
#
# Full replacement for:
#   binance_futures_trader/api/websocket.py
#
# Key behaviors:
# - Tracks intended subscriptions and replays them on reconnect (batched)
# - Exponential backoff reconnect with timer de-dup (prevents reconnect storms)
# - ping/pong keepalive + TCP keepalive sockopt (where supported)
# - Tkinter-safe dispatch via widget.after(0, ...)
#
# Notes:
# - This manager is designed for Binance USD-M Futures base endpoint:
#     wss://fstream.binance.com/ws
# - Current subscribe/unsubscribe API remains compatible with your existing code:
#     subscribe("BTCUSDT") subscribes to "btcusdt@ticker"
#     unsubscribe("BTCUSDT") unsubscribes from "btcusdt@ticker"

from __future__ import annotations

import json
import random
import socket
import threading
import time
import traceback
from typing import Callable, Optional, Set

import tkinter as tk
import websocket


def _on_tk(widget: tk.Misc | None, fn, *a, **k):
    """Dispatch fn on Tk main loop if widget exists; otherwise call directly."""
    if widget and widget.winfo_exists():
        widget.after(0, lambda: fn(*a, **k))
    else:
        fn(*a, **k)


class BinanceWebSocketManager:
    def __init__(
        self,
        on_message_callback: Optional[Callable[[dict], None]] = None,
        ui_widget: tk.Misc | None = None,
        url: str = "wss://fstream.binance.com/ws",
        ping_interval: int = 15,
        ping_timeout: int = 10,
    ):
        self.url = url
        self.ws: websocket.WebSocketApp | None = None

        self.on_message_callback = on_message_callback
        self.ui_widget = ui_widget

        self.thread: threading.Thread | None = None
        self.running = False

        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self._lock = threading.Lock()
        self._subs: Set[str] = set()  # stores stream strings, e.g. "btcusdt@ticker"

        self._reconnect_attempt = 0
        self._reconnect_timer: threading.Timer | None = None

    # ----------------------------
    # WebSocket callbacks
    # ----------------------------
    def on_open(self, ws):
        self._reconnect_attempt = 0
        print("WebSocket connection opened")

        # Replay subscriptions (batched) after reconnect/open
        self._resubscribe_all(ws)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket connection closed ({close_status_code}): {close_msg}")
        if self.running:
            self.reconnect()

    def on_error(self, ws, error):
        print(f"[WS] Error: {error}")
        traceback.print_exc()
        # Do not reconnect here unconditionally; on_close / run_forever return will handle it.

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if self.on_message_callback:
                _on_tk(self.ui_widget, self.on_message_callback, data)
        except json.JSONDecodeError as e:
            print(f"[WS] Error decoding message: {e}")
        except Exception as e:
            print(f"[WS] on_message exception: {e}")
            traceback.print_exc()

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _resubscribe_all(self, ws: websocket.WebSocketApp):
        with self._lock:
            streams = sorted(self._subs)

        if not streams:
            return

        # Batch subscribe to reduce payload size and improve reliability.
        # Binance accepts large SUBSCRIBE lists, but batching is safer.
        batch_size = 200
        for i in range(0, len(streams), batch_size):
            params = streams[i : i + batch_size]
            msg = {"method": "SUBSCRIBE", "params": params, "id": 1}
            try:
                ws.send(json.dumps(msg))
            except Exception as e:
                print(f"[WS] resubscribe error: {e}")
            time.sleep(0.15)

    def _run(self):
        """
        Run a single websocket-client loop.
        If it exits while still running, schedule a reconnect.
        """
        while self.running and self.ws is not None:
            try:
                # Enable TCP keepalive where possible
                sockopt = (
                    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                )

                self.ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    ping_payload="keepalive",
                    skip_utf8_validation=True,
                    reconnect=0,  # we control reconnects ourselves
                    sockopt=sockopt,
                )

            except Exception as e:
                print(f"[WS] run_forever exception: {e}")
                traceback.print_exc()

            # If run_forever returned and we still want to run, reconnect.
            if self.running:
                self.reconnect()
            return  # exit thread; reconnect() will start a new one

    # ----------------------------
    # Public API
    # ----------------------------
    def start(self):
        """
        Start the websocket manager if not already running.
        Safe to call multiple times.
        """
        with self._lock:
            if self.running and self.ws is not None:
                return
            self.running = True

        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message,
        )

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        """
        Stop the websocket manager and close the socket.
        """
        with self._lock:
            self.running = False
            # Cancel pending reconnect timer if any
            if self._reconnect_timer and self._reconnect_timer.is_alive():
                try:
                    self._reconnect_timer.cancel()
                except Exception:
                    pass
                self._reconnect_timer = None

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None

    def subscribe(self, symbol: str):
        """
        Subscribe to <symbol>@ticker and remember it for reconnect resubscribe.
        """
        stream = f"{symbol.lower()}@ticker"
        with self._lock:
            self._subs.add(stream)

        if self.ws:
            msg = {"method": "SUBSCRIBE", "params": [stream], "id": 1}
            try:
                self.ws.send(json.dumps(msg))
            except Exception as e:
                print(f"[WS] subscribe error: {e}")

    def unsubscribe(self, symbol: str):
        """
        Unsubscribe from <symbol>@ticker and remove it from reconnect set.
        """
        stream = f"{symbol.lower()}@ticker"
        with self._lock:
            self._subs.discard(stream)

        if self.ws:
            msg = {"method": "UNSUBSCRIBE", "params": [stream], "id": 1}
            try:
                self.ws.send(json.dumps(msg))
            except Exception as e:
                print(f"[WS] unsubscribe error: {e}")

    def reconnect(self):
        """
        Schedule a reconnect with exponential backoff.
        De-dupes reconnect timers to prevent stacking.
        """
        with self._lock:
            if not self.running:
                return

            # If a reconnect is already scheduled, do not schedule another.
            if self._reconnect_timer and self._reconnect_timer.is_alive():
                return

            self._reconnect_attempt += 1
            delay = min(60, 2 ** self._reconnect_attempt) + random.uniform(0, 1.0)
            print(f"[WS] Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempt})...")

            def _delayed_restart():
                if not self.running:
                    return
                try:
                    self.stop()
                except Exception:
                    pass
                self.start()

            t = threading.Timer(delay, _delayed_restart)
            t.daemon = True
            self._reconnect_timer = t
            t.start()
