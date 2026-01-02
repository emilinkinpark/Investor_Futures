# binance_futures_trader/ui/telegram_tab.py
#
# Telegram tab for Binance Futures Trader UI.
#
# Features:
# - Configure Telegram Bot Token + Chat ID
# - Enable/disable alerts
# - Thresholds for ADX(1m) and ADX(15m)
# - NEW: Require BOTH ADX timeframes checkbox
# - Monitor Top 20 list via a callback: get_top20_rows() -> list[dict]
# - Anti-spam: per-symbol cooldown
# - NEW: Combined hysteresis + re-arm only after BOTH ADX drop below (threshold - hysteresis)
# - NEW: Visual status updates: "Waiting for ADX(15m)…" / "Waiting for ADX(1m)…"
# - Test message button + live log window
# - Saves config to: binance_futures_trader/config/telegram_config.json
#
# Integration expectation:
# - You must pass a callable that returns current Top 20 rows:
#     [{"symbol":"BTCUSDT","adx_1m":12.3,"adx_15m":25.1,"rank":1}, ...]
#   Keys are flexible: "adx_1m"/"ADX_1m"/"ADX(1m)" accepted; same for 15m.
#
# Example from main_window.py:
#   from ui.telegram_tab import TelegramTab
#   telegram_tab = TelegramTab(notebook, get_top20_rows=futures_tab.get_top20_rows)
#   notebook.add(telegram_tab, text="Telegram")
#
# If you cannot pass futures_tab.get_top20_rows at creation time,
# you can later do:
#   telegram_tab.set_top20_source(futures_tab.get_top20_rows)

from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None


ADELAIDE_TZ = "Australia/Adelaide"


def _now_local_str() -> str:
    try:
        if ZoneInfo:
            return datetime.now(ZoneInfo(ADELAIDE_TZ)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _get_any(d: dict, keys: List[str]):
    for k in keys:
        if k in d:
            return d.get(k)
    return None


@dataclass
class TelegramConfig:
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""

    # thresholds
    enable_adx_1m: bool = True
    enable_adx_15m: bool = True
    adx_1m_threshold: float = 40.0
    adx_15m_threshold: float = 40.0

    # NEW: require both ADX timeframes to be true to alert
    require_both_adx: bool = True

    # monitor controls
    check_interval_sec: float = 15.0
    cooldown_min: float = 30.0

    # hysteresis for re-arm
    # In combined mode (require_both_adx=True):
    #   After alert, re-arm ONLY when BOTH adx1 and adx15 drop below (threshold - hysteresis).
    hysteresis: float = 0.0  # 0 disables


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, timeout_sec: float = 10.0):
        self.token = token.strip()
        self.chat_id = str(chat_id).strip()
        self.timeout_sec = timeout_sec

    def is_ready(self) -> bool:
        return bool(self.token) and bool(self.chat_id)

    def send_message(self, text: str) -> (bool, str):
        """
        Returns (ok, message). Uses Telegram sendMessage endpoint via urllib.
        """
        if not self.is_ready():
            return False, "Missing bot token or chat id"

        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
            data = urllib.parse.urlencode(payload).encode("utf-8")

            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")

            try:
                j = json.loads(body)
                if j.get("ok") is True:
                    return True, "Sent"
                return False, j.get("description", "Telegram returned ok=false")
            except Exception:
                return True, "Sent (unparsed response)"
        except Exception as e:
            return False, str(e)


class TelegramMonitor(threading.Thread):
    """
    Background monitor that:
    - polls get_top20_rows()
    - triggers alerts based on ADX thresholds
    - supports combined mode: require BOTH ADX timeframes to be true
    - cooldown + hysteresis re-arm logic
    - emits status events (waiting messages)
    """

    def __init__(
        self,
        cfg_getter: Callable[[], TelegramConfig],
        notifier_getter: Callable[[], TelegramNotifier],
        get_top20_rows: Callable[[], List[dict]],
        event_q: "queue.Queue[dict]",
    ):
        super().__init__(daemon=True)
        self._cfg_getter = cfg_getter
        self._notifier_getter = notifier_getter
        self._get_top20_rows = get_top20_rows
        self._event_q = event_q

        # IMPORTANT: do NOT name this attribute "_stop" (Thread uses internal _stop()).
        self._stop_event = threading.Event()

        # per-symbol tracking
        self._prev_adx_1m: Dict[str, float] = {}
        self._prev_adx_15m: Dict[str, float] = {}

        # last-sent timestamps (used for cooldown)
        self._last_sent: Dict[str, float] = {}

        # NEW: combined arming (prevents re-alert until both drop below thresholds - hyst)
        self._armed_combined: Dict[str, bool] = {}

        # Status throttling
        self._last_status_ts: float = 0.0
        self._last_status_msg: str = ""

    def stop(self):
        self._stop_event.set()

    def _log(self, msg: str):
        self._event_q.put({"type": "log", "msg": msg})

    def _status(self, msg: str):
        # throttle + avoid repeating identical messages
        now = time.time()
        if msg == self._last_status_msg and (now - self._last_status_ts) < 1.0:
            return
        if (now - self._last_status_ts) < 0.25:
            return
        self._last_status_ts = now
        self._last_status_msg = msg
        self._event_q.put({"type": "status", "msg": msg})

    def _extract_metrics(self, row: dict):
        symbol = (row.get("symbol") or row.get("Symbol") or "").upper().strip()
        rank = row.get("rank") or row.get("Rank")

        adx1 = _safe_float(
            _get_any(row, ["adx_1m", "ADX_1m", "ADX1m", "ADX(1m)", "ADX_1M", "adx1m"])
        )
        adx15 = _safe_float(
            _get_any(row, ["adx_15m", "ADX_15m", "ADX15m", "ADX(15m)", "ADX_15M", "adx15m"])
        )
        return symbol, rank, adx1, adx15

    def _cooldown_ok(self, symbol: str, cooldown_min: float) -> bool:
        last_sent_ts = self._last_sent.get(symbol)
        if not last_sent_ts:
            return True
        return (time.time() - last_sent_ts) >= (cooldown_min * 60.0)

    def _send_alert(
        self,
        symbol: str,
        rank,
        adx1: Optional[float],
        adx15: Optional[float],
        cfg: TelegramConfig,
        reason: str,
    ):
        notifier = self._notifier_getter()
        if not notifier.is_ready():
            self._log("Telegram not configured (token/chat id missing).")
            return

        lines = [
            "[Top20 ADX Alert]",
            f"Symbol: {symbol}",
        ]
        if rank is not None:
            lines.append(f"Rank: {rank}")
        if adx1 is not None:
            lines.append(f"ADX(1m): {adx1:.2f} (>= {cfg.adx_1m_threshold:g})")
        if adx15 is not None:
            lines.append(f"ADX(15m): {adx15:.2f} (>= {cfg.adx_15m_threshold:g})")
        lines.append(f"Reason: {reason}")
        lines.append(f"Time: {_now_local_str()} ({ADELAIDE_TZ})")

        ok, msg = notifier.send_message("\n".join(lines))
        if ok:
            self._log(f"Sent: {symbol} | {reason}")
        else:
            self._log(f"Send failed: {symbol} | {msg}")

    # ---------------- Combined logic ----------------

    def _combined_logic(self, symbol: str, rank, adx1: float, adx15: float, cfg: TelegramConfig):
        """
        Combined rule:
          - Alert only when BOTH ADX(1m) and ADX(15m) are >= thresholds.
          - Prevent re-alert until BOTH drop below (threshold - hysteresis).
          - Status indicator shows what we are waiting for.
        """
        if symbol not in self._armed_combined:
            self._armed_combined[symbol] = True

        hyst = max(0.0, float(cfg.hysteresis))
        thr1 = float(cfg.adx_1m_threshold)
        thr15 = float(cfg.adx_15m_threshold)

        prev1 = self._prev_adx_1m.get(symbol)
        prev15 = self._prev_adx_15m.get(symbol)

        # update prevs
        self._prev_adx_1m[symbol] = adx1
        self._prev_adx_15m[symbol] = adx15

        # Re-arm rule: BOTH must drop below (threshold - hyst)
        if not self._armed_combined[symbol]:
            if hyst <= 0.0:
                # strict: both must drop below raw thresholds
                if (adx1 < thr1) and (adx15 < thr15):
                    self._armed_combined[symbol] = True
            else:
                if (adx1 < (thr1 - hyst)) and (adx15 < (thr15 - hyst)):
                    self._armed_combined[symbol] = True

        # Visual waiting status
        if self._armed_combined[symbol]:
            if adx1 >= thr1 and adx15 < thr15:
                self._status(f"Waiting for ADX(15m)… {symbol} (1m={adx1:.1f}, 15m={adx15:.1f})")
            elif adx15 >= thr15 and adx1 < thr1:
                self._status(f"Waiting for ADX(1m)… {symbol} (1m={adx1:.1f}, 15m={adx15:.1f})")
            elif adx1 < thr1 and adx15 < thr15:
                # keep status stable but not noisy
                self._status("Monitor running")

        # Trigger: both are >= thresholds and we are armed and we "entered" the zone
        now_both = (adx1 >= thr1) and (adx15 >= thr15)
        prev_both = False
        if prev1 is not None and prev15 is not None:
            prev_both = (prev1 >= thr1) and (prev15 >= thr15)

        entered_both_zone = now_both and (not prev_both)

        if self._armed_combined[symbol] and entered_both_zone:
            cooldown = max(0.0, float(cfg.cooldown_min))
            if self._cooldown_ok(symbol, cooldown):
                self._send_alert(
                    symbol=symbol,
                    rank=rank,
                    adx1=adx1,
                    adx15=adx15,
                    cfg=cfg,
                    reason="ADX(1m) AND ADX(15m) are both above threshold",
                )
                self._last_sent[symbol] = time.time()
                # prevent re-alert until re-armed (both drop below thresholds - hyst)
                self._armed_combined[symbol] = False

    # ---------------- Independent logic (fallback if Require BOTH is OFF) ----------------

    def _independent_logic(self, symbol: str, rank, adx1: Optional[float], adx15: Optional[float], cfg: TelegramConfig):
        """
        Independent rule (legacy):
          - If ADX(1m) enabled -> alert when ADX(1m) enters >= threshold.
          - If ADX(15m) enabled -> alert when ADX(15m) enters >= threshold.
        Note: still prevents spamming via cooldown.
        """
        cooldown = max(0.0, float(cfg.cooldown_min))

        # ADX 1m
        if cfg.enable_adx_1m and adx1 is not None:
            thr1 = float(cfg.adx_1m_threshold)
            prev1 = self._prev_adx_1m.get(symbol)
            self._prev_adx_1m[symbol] = adx1

            entered = (adx1 >= thr1) and (prev1 is None or prev1 < thr1)
            if entered and self._cooldown_ok(symbol, cooldown):
                self._send_alert(
                    symbol=symbol,
                    rank=rank,
                    adx1=adx1,
                    adx15=adx15 if cfg.enable_adx_15m else None,
                    cfg=cfg,
                    reason="ADX(1m) crossed above threshold",
                )
                self._last_sent[symbol] = time.time()

        # ADX 15m
        if cfg.enable_adx_15m and adx15 is not None:
            thr15 = float(cfg.adx_15m_threshold)
            prev15 = self._prev_adx_15m.get(symbol)
            self._prev_adx_15m[symbol] = adx15

            entered = (adx15 >= thr15) and (prev15 is None or prev15 < thr15)
            if entered and self._cooldown_ok(symbol, cooldown):
                self._send_alert(
                    symbol=symbol,
                    rank=rank,
                    adx1=adx1 if cfg.enable_adx_1m else None,
                    adx15=adx15,
                    cfg=cfg,
                    reason="ADX(15m) crossed above threshold",
                )
                self._last_sent[symbol] = time.time()

    def run(self):
        self._status("Monitor running")
        while not self._stop_event.is_set():
            cfg = self._cfg_getter()

            if not cfg.enabled:
                self._status("Monitor idle (disabled)")
                for _ in range(10):
                    if self._stop_event.is_set():
                        break
                    time.sleep(0.2)
                continue

            # Validate callback exists
            try:
                rows = self._get_top20_rows() or []
            except Exception as e:
                self._log(f"Top20 source error: {e}")
                rows = []

            # Main scan
            for row in rows:
                symbol, rank, adx1, adx15 = self._extract_metrics(row)
                if not symbol:
                    continue

                if cfg.require_both_adx:
                    # If require both but one is missing, show status and skip
                    if adx1 is None and adx15 is None:
                        continue
                    if adx1 is None:
                        self._status(f"Waiting for ADX(1m)… {symbol} (15m={adx15:.1f})")
                        continue
                    if adx15 is None:
                        self._status(f"Waiting for ADX(15m)… {symbol} (1m={adx1:.1f})")
                        continue

                    # Combined decision
                    self._combined_logic(symbol, rank, float(adx1), float(adx15), cfg)
                else:
                    # Independent decision
                    self._independent_logic(symbol, rank, adx1, adx15, cfg)

            # Sleep (responsive)
            interval = max(2.0, float(cfg.check_interval_sec))
            end = time.time() + interval
            while not self._stop_event.is_set() and time.time() < end:
                time.sleep(0.2)

        self._status("Monitor stopped")


class TelegramTab(ttk.Frame):
    def __init__(
        self,
        parent,
        get_top20_rows: Optional[Callable[[], List[dict]]] = None,
    ):
        super().__init__(parent)

        self._event_q: "queue.Queue[dict]" = queue.Queue()
        self._monitor: Optional[TelegramMonitor] = None

        self._get_top20_rows = get_top20_rows  # may be set later via set_top20_source()

        self._config_path = self._default_config_path()
        self._cfg = TelegramConfig()
        self._load_config()

        # UI vars
        self.var_enabled = tk.BooleanVar(value=self._cfg.enabled)
        self.var_token = tk.StringVar(value=self._cfg.bot_token)
        self.var_chat_id = tk.StringVar(value=self._cfg.chat_id)

        self.var_enable_1m = tk.BooleanVar(value=self._cfg.enable_adx_1m)
        self.var_enable_15m = tk.BooleanVar(value=self._cfg.enable_adx_15m)
        self.var_thr_1m = tk.StringVar(value=str(self._cfg.adx_1m_threshold))
        self.var_thr_15m = tk.StringVar(value=str(self._cfg.adx_15m_threshold))

        # NEW
        self.var_require_both = tk.BooleanVar(value=self._cfg.require_both_adx)

        self.var_interval = tk.StringVar(value=str(self._cfg.check_interval_sec))
        self.var_cooldown = tk.StringVar(value=str(self._cfg.cooldown_min))
        self.var_hyst = tk.StringVar(value=str(self._cfg.hysteresis))

        self._build_ui()
        self.after(150, self._poll_events)

    # ---------------- Config ----------------

    def _default_config_path(self) -> str:
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config"))
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, "telegram_config.json")

    def _cfg_from_vars(self) -> TelegramConfig:
        cfg = TelegramConfig()
        cfg.enabled = bool(self.var_enabled.get())
        cfg.bot_token = self.var_token.get().strip()
        cfg.chat_id = self.var_chat_id.get().strip()

        cfg.enable_adx_1m = bool(self.var_enable_1m.get())
        cfg.enable_adx_15m = bool(self.var_enable_15m.get())

        cfg.adx_1m_threshold = float(_safe_float(self.var_thr_1m.get()) or 0.0)
        cfg.adx_15m_threshold = float(_safe_float(self.var_thr_15m.get()) or 0.0)

        cfg.require_both_adx = bool(self.var_require_both.get())

        cfg.check_interval_sec = float(_safe_float(self.var_interval.get()) or 15.0)
        cfg.cooldown_min = float(_safe_float(self.var_cooldown.get()) or 30.0)
        cfg.hysteresis = float(_safe_float(self.var_hyst.get()) or 0.0)
        return cfg

    def _save_config(self):
        self._cfg = self._cfg_from_vars()
        data = self._cfg.__dict__.copy()
        try:
            with open(self._config_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            self._log(f"Saved config: {self._config_path}")
        except Exception as e:
            self._log(f"Save failed: {e}")

    def _load_config(self):
        try:
            if os.path.exists(self._config_path):
                with open(self._config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # merge defaults + loaded to handle new keys safely
                merged = {**TelegramConfig().__dict__, **(data or {})}
                self._cfg = TelegramConfig(**merged)
        except Exception:
            self._cfg = TelegramConfig()

    # ---------------- Public integration hook ----------------

    def set_top20_source(self, get_top20_rows: Callable[[], List[dict]]):
        self._get_top20_rows = get_top20_rows
        self._log("Top20 source connected.")

    # ---------------- UI ----------------

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        outer = ttk.Frame(self)
        outer.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        outer.columnconfigure(0, weight=1)

        # Telegram settings
        cfg_frame = ttk.LabelFrame(outer, text="Telegram Settings")
        cfg_frame.grid(row=0, column=0, sticky="ew")
        cfg_frame.columnconfigure(1, weight=1)

        ttk.Checkbutton(cfg_frame, text="Enable Telegram Alerts", variable=self.var_enabled).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(6, 6)
        )

        ttk.Label(cfg_frame, text="Bot Token:").grid(row=1, column=0, sticky="w", padx=(6, 6), pady=3)
        ttk.Entry(cfg_frame, textvariable=self.var_token).grid(row=1, column=1, sticky="ew", padx=(0, 6), pady=3)

        ttk.Label(cfg_frame, text="Chat ID:").grid(row=2, column=0, sticky="w", padx=(6, 6), pady=3)
        ttk.Entry(cfg_frame, textvariable=self.var_chat_id).grid(row=2, column=1, sticky="ew", padx=(0, 6), pady=3)

        # Thresholds
        thr_frame = ttk.LabelFrame(outer, text="Top 20 ADX Thresholds")
        thr_frame.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        for i in range(4):
            thr_frame.columnconfigure(i, weight=1)

        ttk.Checkbutton(thr_frame, text="Alert on ADX(1m)", variable=self.var_enable_1m).grid(
            row=0, column=0, sticky="w", padx=6, pady=6
        )
        ttk.Label(thr_frame, text="ADX(1m) Threshold:").grid(row=0, column=1, sticky="e", padx=6, pady=6)
        ttk.Entry(thr_frame, textvariable=self.var_thr_1m, width=10).grid(
            row=0, column=2, sticky="w", padx=6, pady=6
        )

        ttk.Checkbutton(thr_frame, text="Alert on ADX(15m)", variable=self.var_enable_15m).grid(
            row=1, column=0, sticky="w", padx=6, pady=(0, 6)
        )
        ttk.Label(thr_frame, text="ADX(15m) Threshold:").grid(row=1, column=1, sticky="e", padx=6, pady=(0, 6))
        ttk.Entry(thr_frame, textvariable=self.var_thr_15m, width=10).grid(
            row=1, column=2, sticky="w", padx=6, pady=(0, 6)
        )

        # NEW: Require both checkbox
        ttk.Checkbutton(
            thr_frame,
            text="Require BOTH ADX timeframes",
            variable=self.var_require_both,
        ).grid(row=0, column=3, rowspan=2, sticky="w", padx=6, pady=6)

        # Monitor controls
        mon_frame = ttk.LabelFrame(outer, text="Monitor Controls")
        mon_frame.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        mon_frame.columnconfigure(5, weight=1)

        ttk.Label(mon_frame, text="Check interval (sec):").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(mon_frame, textvariable=self.var_interval, width=8).grid(row=0, column=1, sticky="w", padx=6, pady=6)

        ttk.Label(mon_frame, text="Cooldown (min):").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(mon_frame, textvariable=self.var_cooldown, width=8).grid(row=0, column=3, sticky="w", padx=6, pady=6)

        ttk.Label(mon_frame, text="Hysteresis:").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(mon_frame, textvariable=self.var_hyst, width=8).grid(row=0, column=5, sticky="w", padx=6, pady=6)

        btn_frame = ttk.Frame(mon_frame)
        btn_frame.grid(row=1, column=0, columnspan=6, sticky="ew", padx=6, pady=(0, 6))

        ttk.Button(btn_frame, text="Save", command=self._on_save).pack(side="left", padx=(0, 6))
        ttk.Button(btn_frame, text="Test Message", command=self._on_test).pack(side="left", padx=(0, 6))
        ttk.Button(btn_frame, text="Start Monitor", command=self._on_start).pack(side="left", padx=(0, 6))
        ttk.Button(btn_frame, text="Stop Monitor", command=self._on_stop).pack(side="left", padx=(0, 6))

        self.lbl_state = ttk.Label(btn_frame, text="Status: Idle")
        self.lbl_state.pack(side="right")

        # Log view
        log_frame = ttk.LabelFrame(outer, text="Telegram Log")
        log_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        outer.rowconfigure(3, weight=1)

        self.txt_log = tk.Text(log_frame, height=14, wrap="word")
        self.txt_log.pack(side="left", fill="both", expand=True)

        sb = ttk.Scrollbar(log_frame, orient="vertical", command=self.txt_log.yview)
        sb.pack(side="right", fill="y")
        self.txt_log.configure(yscrollcommand=sb.set)

        self._log("Telegram tab ready.")

    # ---------------- Actions ----------------

    def _on_save(self):
        self._save_config()

    def _make_notifier(self) -> TelegramNotifier:
        cfg = self._cfg_from_vars()
        return TelegramNotifier(cfg.bot_token, cfg.chat_id)

    def _on_test(self):
        self._save_config()
        notifier = self._make_notifier()
        if not notifier.is_ready():
            messagebox.showwarning("Telegram", "Please enter Bot Token and Chat ID before testing.")
            return
        ok, msg = notifier.send_message(f"Test message from Binance Futures Trader at {_now_local_str()} ({ADELAIDE_TZ})")
        if ok:
            self._log("Test message sent successfully.")
        else:
            self._log(f"Test message failed: {msg}")
            messagebox.showerror("Telegram", f"Test message failed:\n{msg}")

    def _on_start(self):
        self._save_config()

        if self._get_top20_rows is None:
            messagebox.showwarning(
                "Telegram",
                "Top20 source is not connected.\n\n"
                "You must connect FuturesTab.get_top20_rows() to this Telegram tab.",
            )
            return

        if self._monitor and self._monitor.is_alive():
            self._log("Monitor already running.")
            return

        self._monitor = TelegramMonitor(
            cfg_getter=lambda: self._cfg_from_vars(),
            notifier_getter=self._make_notifier,
            get_top20_rows=self._get_top20_rows,
            event_q=self._event_q,
        )
        self._monitor.start()
        self._log("Monitor started.")

    def _on_stop(self):
        if self._monitor and self._monitor.is_alive():
            self._monitor.stop()
            self._log("Stopping monitor...")
        else:
            self._log("Monitor is not running.")

    # ---------------- Event/log helpers ----------------

    def _log(self, msg: str):
        ts = _now_local_str()
        line = f"[{ts}] {msg}\n"
        try:
            self.txt_log.insert("end", line)
            self.txt_log.see("end")
        except Exception:
            pass

    def _set_status(self, msg: str):
        try:
            self.lbl_state.configure(text=f"Status: {msg}")
        except Exception:
            pass

    def _poll_events(self):
        try:
            while True:
                evt = self._event_q.get_nowait()
                et = evt.get("type")
                if et == "log":
                    self._log(evt.get("msg", ""))
                elif et == "status":
                    self._set_status(evt.get("msg", ""))
                else:
                    self._log(str(evt))
        except queue.Empty:
            pass
        except Exception as e:
            self._log(f"Event pump error: {e}")

        self.after(150, self._poll_events)
