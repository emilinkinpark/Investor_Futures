import tkinter as tk
from tkinter import ttk

import time
from pathlib import Path

from ui.trending_tab import TrendingTab
from ui.trading_tab import TradingTab
from ui.dca_tab import DCATab
from ui.marketpredictor_tab import MarketPredictorTab
from ui.settings_tab import SettingsTab
from ui.data_logging_tab import DataLoggingTab
from ui.analysis_tab import AnalysisTab
from ui.auto_order_frame import AutoOrderFrame   # <-- AutoOrder 3.0
from ui.futures_tab import FuturesTab            # <-- NEW Futures tab
from ui.telegram_tab import TelegramTab          # <-- NEW Telegram tab

from api.websocket import BinanceWebSocketManager  # Shared WebSocket manager
from services.rest_client import rest_client
from utils.threading import run_io_bound


# --- (optional) Thread-safe Tk dispatcher (not used here, but safe helper) ---
def on_tk(widget, fn, *a, **k):
    try:
        if widget and widget.winfo_exists():
            widget.after(0, lambda: fn(*a, **k))
    except Exception:
        # Best-effort UI dispatch; swallow errors to avoid killing background threads.
        pass


def run_app():
    root = tk.Tk()
    root.title("Binance Futures Trader")
    # Bigger default window helps with dense tabs (Trading, AutoOrder, DCA, etc.)
    root.geometry("1400x900")

    # --- Paths for DB health checks ---
    project_root = Path(__file__).resolve().parents[1]  # .../binance_futures_trader
    market_db_path = project_root / "marketpredictor.db"
    dca_db_path = project_root / "data" / "dca_positions.db"

    # --- Main notebook ---
    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)

    # --- Global status bar at the bottom ---
    status_frame = ttk.Frame(root)
    status_frame.pack(side="bottom", fill="x")

    rest_status_var = tk.StringVar(value="🟡 REST: checking…")
    # Start in "connecting" state; we start WS immediately below.
    ws_status_var = tk.StringVar(value="🟡 WS: connecting…")
    db_status_var = tk.StringVar(value="DB: checking…")

    rest_label = ttk.Label(status_frame, textvariable=rest_status_var)
    rest_label.pack(side="left", padx=6)

    ws_label = ttk.Label(status_frame, textvariable=ws_status_var)
    ws_label.pack(side="left", padx=6)

    db_label = ttk.Label(status_frame, textvariable=db_status_var)
    db_label.pack(side="left", padx=6)

    # --- Create shared WebSocket manager ---
    # We pass the Tk root so WebSocket callbacks can safely dispatch to the UI thread.
    ws_manager = BinanceWebSocketManager(ui_widget=root)
    # Start the shared WebSocket connection once, here.
    try:
        ws_manager.start()
    except Exception as e:  # noqa: BLE001
        print(f"[WS] Failed to start WebSocket: {e}")

    # --- Tabs ---

    # Trading tab (takes shared ws_manager so it does NOT try to start its own WS)
    trading_tab = TradingTab(notebook, ws_manager)
    notebook.add(trading_tab, text="Trading")

    # Auto Order 3.0 tab
    # (constructor in your project takes just parent; we wire references later)
    auto_order_tab = AutoOrderFrame(notebook)
    notebook.add(auto_order_tab, text="Auto Order")

    # DCA tab (needs trading_tab so it can read positions)
    dca_tab = DCATab(notebook, trading_tab=trading_tab)
    notebook.add(dca_tab, text="DCA Manager")

    # Trending tab (already expects dca_tab & trading_tab in its __init__)
    trending_tab = TrendingTab(notebook, dca_tab=dca_tab, trading_tab=trading_tab)
    notebook.add(trending_tab, text="Trending")

    # Futures tab (NEW)
    futures_tab = FuturesTab(notebook)
    notebook.add(futures_tab, text="Futures")

    # Telegram tab (NEW)
    # We try to bind to FuturesTab Top 20 source if FuturesTab exposes get_top20_rows().
    get_top20 = futures_tab.get_top20_rows if hasattr(futures_tab, "get_top20_rows") else None
    telegram_tab = TelegramTab(notebook, get_top20_rows=get_top20)
    notebook.add(telegram_tab, text="Telegram")

    # Market Predictor tab
    mp_tab = MarketPredictorTab(notebook)
    notebook.add(mp_tab, text="Market Predictor")

    # Data Logging tab
    data_tab = DataLoggingTab(notebook)
    notebook.add(data_tab, text="Data Logging")

    # Settings tab
    settings_tab = SettingsTab(notebook)
    notebook.add(settings_tab, text="Settings")

    # Analysis tab
    analysis_tab = AnalysisTab(notebook)
    notebook.add(analysis_tab, text="Analysis")

    # --- Wire cross-references ---

    # Trading / DCA <-> Trending
    if hasattr(trading_tab, "set_trending_tab"):
        trading_tab.set_trending_tab(trending_tab)
    if hasattr(dca_tab, "set_trending_tab"):
        dca_tab.set_trending_tab(trending_tab)

    # AutoOrder wiring (if methods exist in your version)
    if hasattr(auto_order_tab, "set_trading_tab"):
        auto_order_tab.set_trading_tab(trading_tab)
    if hasattr(auto_order_tab, "set_trending_tab"):
        auto_order_tab.set_trending_tab(trending_tab)
    if hasattr(auto_order_tab, "set_marketpredictor_tab"):
        auto_order_tab.set_marketpredictor_tab(mp_tab)

    # If FuturesTab gains get_top20_rows later, allow Telegram tab to bind dynamically.
    if hasattr(telegram_tab, "set_top20_source") and hasattr(futures_tab, "get_top20_rows"):
        try:
            telegram_tab.set_top20_source(futures_tab.get_top20_rows)
        except Exception:
            pass

    # --- Status bar update helpers ---

    rest_state = {
        "last_ok": 0.0,
        "last_error": None,
        "ping_in_flight": False,
    }

    def _ping_rest_worker():
        """Background worker to ping Binance REST without blocking Tk."""
        try:
            # Simple, lightweight endpoint
            rest_client.get_json("/fapi/v1/time")
            rest_state["last_ok"] = time.time()
            rest_state["last_error"] = None
        except Exception as e:  # noqa: BLE001
            rest_state["last_error"] = str(e)
        finally:
            rest_state["ping_in_flight"] = False

    def update_rest_health():
        now = time.time()

        # Schedule a ping every ~15s or if we've never succeeded before
        need_ping = (
            not rest_state["ping_in_flight"]
            and (
                rest_state["last_ok"] == 0.0
                or now - rest_state["last_ok"] > 15
            )
        )
        if need_ping:
            rest_state["ping_in_flight"] = True
            run_io_bound(_ping_rest_worker)

        age = None if rest_state["last_ok"] == 0.0 else now - rest_state["last_ok"]

        if rest_state["last_error"]:
            rest_status_var.set("🔴 REST: error")
        elif age is None:
            rest_status_var.set("🟡 REST: checking…")
        elif age < 10:
            rest_status_var.set(f"🟢 REST: OK ({int(age)}s)")
        elif age < 30:
            rest_status_var.set(f"🟡 REST: stale ({int(age)}s)")
        else:
            rest_status_var.set(f"🔴 REST: stale ({int(age)}s)")

        root.after(2000, update_rest_health)

    def update_ws_health():
        running = getattr(ws_manager, "running", False)
        ws_obj = getattr(ws_manager, "ws", None)

        if running and ws_obj is not None:
            ws_status_var.set("🟢 WS: open")
        elif running:
            ws_status_var.set("🟡 WS: connecting…")
        else:
            ws_status_var.set("🔴 WS: stopped")

        root.after(1000, update_ws_health)

    def update_db_health():
        market_ok = market_db_path.exists()
        dca_ok = dca_db_path.exists()

        market_icon = "🟢" if market_ok else "🔴"
        dca_icon = "🟢" if dca_ok else "🔴"

        db_status_var.set(
            f"DB: marketpredictor {market_icon}   |   dca_positions {dca_icon}"
        )

        root.after(5000, update_db_health)

    # Clean shutdown handler so WebSocket / background tasks stop cleanly
    def _on_close():
        try:
            if ws_manager:
                ws_manager.stop()
        except Exception:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)

    # Kick off periodic status updates
    update_rest_health()
    update_ws_health()
    update_db_health()

    root.mainloop()


if __name__ == "__main__":
    run_app()
