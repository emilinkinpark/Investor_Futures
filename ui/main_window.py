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
from ui.signal_tab import SignalTab
from ui.auto_order_frame import AutoOrderFrame
from ui.futures_tab import FuturesTab
from ui.telegram_tab import TelegramTab

from api.websocket import BinanceWebSocketManager
from services.rest_client import rest_client
from utils.threading import run_io_bound


def run_app():
    root = tk.Tk()
    root.title("Binance Futures Trader")
    root.geometry("1400x900")

    # -------------------------------------------------
    # Paths / DB health
    # -------------------------------------------------
    project_root = Path(__file__).resolve().parents[1]
    market_db_path = project_root / "marketpredictor.db"
    dca_db_path = project_root / "data" / "dca_positions.db"

    # -------------------------------------------------
    # Notebook
    # -------------------------------------------------
    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)

    # -------------------------------------------------
    # Status bar
    # -------------------------------------------------
    status_frame = ttk.Frame(root)
    status_frame.pack(side="bottom", fill="x")

    rest_status_var = tk.StringVar(value="🟡 REST: checking…")
    ws_status_var = tk.StringVar(value="🟡 WS: connecting…")
    db_status_var = tk.StringVar(value="DB: checking…")

    ttk.Label(status_frame, textvariable=rest_status_var).pack(side="left", padx=6)
    ttk.Label(status_frame, textvariable=ws_status_var).pack(side="left", padx=6)
    ttk.Label(status_frame, textvariable=db_status_var).pack(side="left", padx=6)

    # -------------------------------------------------
    # WebSocket manager
    # -------------------------------------------------
    ws_manager = BinanceWebSocketManager(ui_widget=root)
    try:
        ws_manager.start()
    except Exception as e:
        print(f"[WS] Failed to start WebSocket: {e}")

    # -------------------------------------------------
    # Tabs
    # -------------------------------------------------
    trading_tab = TradingTab(notebook, ws_manager)
    # IMPORTANT: route WS messages into TradingTab
    ws_manager.on_message_callback = trading_tab.handle_ws_message
    notebook.add(trading_tab, text="Trading")

    auto_order_tab = AutoOrderFrame(notebook)
    notebook.add(auto_order_tab, text="Auto Order")

    dca_tab = DCATab(notebook, trading_tab=trading_tab)
    notebook.add(dca_tab, text="DCA Manager")

    trending_tab = TrendingTab(notebook, dca_tab=dca_tab, trading_tab=trading_tab)
    notebook.add(trending_tab, text="Trending")

    futures_tab = FuturesTab(notebook)
    notebook.add(futures_tab, text="Futures")

    get_top20 = futures_tab.get_top20_rows if hasattr(futures_tab, "get_top20_rows") else None
    telegram_tab = TelegramTab(notebook, get_top20_rows=get_top20)
    notebook.add(telegram_tab, text="Telegram")

    mp_tab = MarketPredictorTab(notebook)
    notebook.add(mp_tab, text="Market Predictor")

    data_tab = DataLoggingTab(notebook)
    notebook.add(data_tab, text="Data Logging")

    # -------------------------------------------------
    # SIGNAL tab (Support / Resistance moved here)
    # -------------------------------------------------
    signal_tab = SignalTab(notebook)
    notebook.add(signal_tab, text="SIGNAL")

    # -------------------------------------------------
    # Analysis tab (no SR here)
    # -------------------------------------------------
    analysis_tab = AnalysisTab(notebook)
    notebook.add(analysis_tab, text="Analysis")

    settings_tab = SettingsTab(notebook)
    notebook.add(settings_tab, text="Settings")

    # -------------------------------------------------
    # Cross-wiring
    # -------------------------------------------------
    if hasattr(trading_tab, "set_trending_tab"):
        trading_tab.set_trending_tab(trending_tab)

    if hasattr(dca_tab, "set_trending_tab"):
        dca_tab.set_trending_tab(trending_tab)

    if hasattr(auto_order_tab, "set_trading_tab"):
        auto_order_tab.set_trading_tab(trading_tab)

    if hasattr(auto_order_tab, "set_trending_tab"):
        auto_order_tab.set_trending_tab(trending_tab)

    if hasattr(auto_order_tab, "set_marketpredictor_tab"):
        auto_order_tab.set_marketpredictor_tab(mp_tab)

    if hasattr(telegram_tab, "set_top20_source") and get_top20:
        telegram_tab.set_top20_source(get_top20)

    # -------------------------------------------------
    # Health checks
    # -------------------------------------------------
    rest_state = {"last_ok": 0.0, "last_error": None, "ping_in_flight": False}

    def _ping_rest_worker():
        try:
            rest_client.get_json("/fapi/v1/time")
            rest_state["last_ok"] = time.time()
            rest_state["last_error"] = None
        except Exception as e:
            rest_state["last_error"] = str(e)
        finally:
            rest_state["ping_in_flight"] = False

    def update_rest_health():
        now = time.time()
        if not rest_state["ping_in_flight"] and (
            rest_state["last_ok"] == 0.0 or now - rest_state["last_ok"] > 15
        ):
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
        ws_status_var.set("🟢 WS: open" if running else "🔴 WS: stopped")
        root.after(1000, update_ws_health)

    def update_db_health():
        market_icon = "🟢" if market_db_path.exists() else "🔴"
        dca_icon = "🟢" if dca_db_path.exists() else "🔴"
        db_status_var.set(f"DB: marketpredictor {market_icon} | dca_positions {dca_icon}")
        root.after(5000, update_db_health)

    def _on_close():
        try:
            ws_manager.stop()
        except Exception:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)

    update_rest_health()
    update_ws_health()
    update_db_health()

    root.mainloop()


if __name__ == "__main__":
    run_app()
