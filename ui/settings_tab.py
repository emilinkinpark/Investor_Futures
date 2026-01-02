import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import pytz


# -------------------------
# Visual clock "Card"
# -------------------------
class ClockCard(tk.Frame):
    def __init__(self, parent, label, tz, get_local_dt, use_24h_var, show_seconds_var, on_close=None):
        super().__init__(parent, bd=0, highlightthickness=0, padx=12, pady=10, bg="#1b2430")
        self.label = label          # e.g., "Sydney 🇦🇺"
        self.tz = tz                # pytz timezone object
        self.get_local_dt = get_local_dt
        self.use_24h_var = use_24h_var
        self.show_seconds_var = show_seconds_var
        self.on_close = on_close

        # Header: City + close
        top = tk.Frame(self, bg=self["bg"])
        top.pack(fill="x")
        self.city_lbl = tk.Label(top, text=self.label, font=("Segoe UI", 11, "bold"), fg="white", bg=self["bg"])
        self.city_lbl.pack(side="left")

        if callable(on_close):
            close = tk.Button(
                top, text="✕", font=("Segoe UI", 9), bd=0, bg=self["bg"], fg="#94a3b8",
                activebackground=self["bg"], activeforeground="white", cursor="hand2",
                command=self.on_close
            )
            close.pack(side="right")

        # Big time
        self.time_lbl = tk.Label(self, text="00:00", font=("Segoe UI", 22, "bold"), fg="white", bg=self["bg"])
        self.time_lbl.pack(anchor="w", pady=(4, 2))

        # Sub-row: date + day/night icon
        sub = tk.Frame(self, bg=self["bg"])
        sub.pack(fill="x")
        self.date_lbl = tk.Label(sub, text="", font=("Segoe UI", 10), fg="#cbd5e1", bg=self["bg"])
        self.date_lbl.pack(side="left")
        self.daynight_lbl = tk.Label(sub, text="", font=("Segoe UI Emoji", 12), fg="#e5e7eb", bg=self["bg"])
        self.daynight_lbl.pack(side="right")

        # Footer: UTC offset + Δ vs local + DST
        foot = tk.Frame(self, bg=self["bg"])
        foot.pack(fill="x", pady=(8, 0))
        self.offset_lbl = tk.Label(foot, text="", font=("Segoe UI", 9), fg="#93c5fd", bg=self["bg"])
        self.offset_lbl.pack(side="left")
        self.delta_lbl = tk.Label(foot, text="", font=("Segoe UI", 9), fg="#86efac", bg=self["bg"])
        self.delta_lbl.pack(side="left", padx=(8, 0))
        self.dst_lbl = tk.Label(foot, text="", font=("Segoe UI", 9), fg="#fcd34d", bg=self["bg"])
        self.dst_lbl.pack(side="right")

        self._last_is_day = None

    # --------- helpers ----------
    def _fmt_offset(self, dt: datetime) -> str:
        off = dt.utcoffset()
        if not off:
            return "UTC±00:00"
        total_minutes = int(off.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        total_minutes = abs(total_minutes)
        return f"UTC{sign}{total_minutes // 60:02d}:{total_minutes % 60:02d}"

    def _fmt_time(self, dt: datetime, use_24h: bool, seconds: bool) -> str:
        if use_24h:
            fmt = "%H:%M:%S" if seconds else "%H:%M"
        else:
            fmt = "%I:%M:%S %p" if seconds else "%I:%M %p"
        s = dt.strftime(fmt)
        return s.lstrip("0") if (not use_24h and s and s[0] == "0") else s

    def _fmt_date(self, dt: datetime) -> str:
        # Tue, 07 Oct 2025
        return dt.strftime("%a, %d %b %Y")

    def _delta_vs_local(self, zdt: datetime, local_dt: datetime) -> str:
        zoff = zdt.utcoffset() or timedelta(0)
        loff = local_dt.utcoffset() or timedelta(0)
        diff_min = int((zoff - loff).total_seconds() // 60)
        sign = "+" if diff_min >= 0 else "-"
        diff_min = abs(diff_min)
        return f"Δ {sign}{diff_min // 60:d}h{diff_min % 60:02d}m"

    def _is_day(self, dt: datetime) -> bool:
        # simple heuristic for visual cue
        return 7 <= dt.hour <= 20

    # --------- main update ----------
    def update_values(self):
        use_24h = bool(self.use_24h_var.get())
        seconds = bool(self.show_seconds_var.get())

        zdt = datetime.now(self.tz)
        local_dt = self.get_local_dt()

        # Day/night background shift
        is_day = self._is_day(zdt)
        if is_day != self._last_is_day:
            self._last_is_day = is_day
            bg = "#1b2430" if is_day else "#141a22"
            for w in (self, self.city_lbl, self.time_lbl, self.date_lbl,
                      self.daynight_lbl, self.offset_lbl, self.delta_lbl, self.dst_lbl):
                w.configure(bg=bg)
            self.configure(bg=bg)

        self.daynight_lbl.config(text="☀️" if is_day else "🌙")

        # Content
        self.time_lbl.config(text=self._fmt_time(zdt, use_24h, seconds))
        self.date_lbl.config(text=self._fmt_date(zdt))
        self.offset_lbl.config(text=self._fmt_offset(zdt))
        self.delta_lbl.config(text=self._delta_vs_local(zdt, local_dt))

        # DST flag
        try:
            dst_active = (zdt.dst() or timedelta(0)).total_seconds() > 0
        except Exception:
            dst_active = False
        self.dst_lbl.config(text="DST" if dst_active else "")


# -------------------------
# Settings Tab
# -------------------------
class SettingsTab(ttk.Frame):
    """
    Settings tab with API settings + visual world clock gallery.
    Keeps fixed clocks and lets users add/remove more zones.
    """
    def __init__(self, parent):
        super().__init__(parent)
        self._init_vars()
        self._build_ui()
        self._tick()

    # -------------------------
    # State / Variables
    # -------------------------
    def _init_vars(self):
        # Common settings (preserved names)
        self.api_key = tk.StringVar(value="")
        self.api_secret = tk.StringVar(value="")
        self.use_testnet = tk.BooleanVar(value=False)
        self.default_leverage = tk.IntVar(value=10)
        self.quote_asset = tk.StringVar(value="USDT")

        # Clock preferences
        self.use_24h = tk.BooleanVar(value=True)
        self.show_seconds = tk.BooleanVar(value=True)

        # Fixed clocks (always shown)
        self.fixed_defs = {
            "UTC ⏱": pytz.utc,
            "Sydney 🇦🇺": pytz.timezone("Australia/Sydney"),
            "Tokyo 🇯🇵": pytz.timezone("Asia/Tokyo"),
            "London 🇬🇧": pytz.timezone("Europe/London"),
            "New York 🇺🇸": pytz.timezone("America/New_York"),
        }

        # User-added zones (store pytz zone strings, keep order)
        self.user_tzs = []
        # key -> ClockCard (key is zone_name for user, "__fixed__:Label" for fixed)
        self.cards = {}

        self.add_zone_var = tk.StringVar(value="")
        # Local tzinfo for delta comparison (system local)
        self._local_tzinfo = datetime.now().astimezone().tzinfo

    # -------------------------
    # UI
    # -------------------------
    def _build_ui(self):
        header = ttk.Label(self, text="Settings", font=("Segoe UI", 14, "bold"))
        header.grid(row=0, column=0, columnspan=3, pady=(12, 6), padx=12, sticky="w")

        row = 1
        ttk.Label(self, text="API Key").grid(row=row, column=0, sticky="e", padx=12, pady=6)
        ttk.Entry(self, textvariable=self.api_key, width=50, show="•").grid(row=row, column=1, columnspan=2, sticky="we", padx=12, pady=6)

        row += 1
        ttk.Label(self, text="API Secret").grid(row=row, column=0, sticky="e", padx=12, pady=6)
        ttk.Entry(self, textvariable=self.api_secret, width=50, show="•").grid(row=row, column=1, columnspan=2, sticky="we", padx=12, pady=6)

        row += 1
        ttk.Checkbutton(self, text="Use Testnet", variable=self.use_testnet).grid(row=row, column=1, sticky="w", padx=12, pady=6)

        row += 1
        ttk.Label(self, text="Default Leverage").grid(row=row, column=0, sticky="e", padx=12, pady=6)
        ttk.Spinbox(self, from_=1, to=125, textvariable=self.default_leverage, width=8).grid(row=row, column=1, sticky="w", padx=12, pady=6)

        row += 1
        ttk.Label(self, text="Quote Asset").grid(row=row, column=0, sticky="e", padx=12, pady=6)
        ttk.Combobox(self, textvariable=self.quote_asset, values=["USDT", "BUSD", "USDC"], width=12, state="readonly").grid(row=row, column=1, sticky="w", padx=12, pady=6)

        # ---------------- World Clocks ----------------
        row += 1
        clock_header = ttk.Label(self, text="World Clocks (Visual)", font=("Segoe UI", 12, "bold"))
        clock_header.grid(row=row, column=0, columnspan=3, pady=(16, 6), padx=12, sticky="w")

        # Preferences row
        row += 1
        pref = ttk.Frame(self)
        pref.grid(row=row, column=0, columnspan=3, sticky="we", padx=12)
        ttk.Checkbutton(pref, text="24-hour", variable=self.use_24h).pack(side="left")
        ttk.Checkbutton(pref, text="Show seconds", variable=self.show_seconds).pack(side="left", padx=(12, 0))

        # Card gallery (scrollable)
        row += 1
        gallery = ttk.Frame(self)
        gallery.grid(row=row, column=0, columnspan=3, sticky="nsew", padx=12, pady=(8, 8))
        self.grid_rowconfigure(row, weight=1)
        self.grid_columnconfigure(1, weight=1)

        self.canvas = tk.Canvas(gallery, highlightthickness=0, bg="#0f1720", height=260)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        gallery.grid_rowconfigure(0, weight=1)
        gallery.grid_columnconfigure(0, weight=1)
        self.scroll = ttk.Scrollbar(gallery, orient="vertical", command=self.canvas.yview)
        self.scroll.grid(row=0, column=1, sticky="ns")
        self.canvas.configure(yscrollcommand=self.scroll.set)

        self.cards_frame = tk.Frame(self.canvas, bg="#0f1720")
        self.canvas_window = self.canvas.create_window((0, 0), window=self.cards_frame, anchor="nw")

        self.cards_frame.bind("<Configure>", lambda e: self._on_cards_configure())
        self.canvas.bind("<Configure>", lambda e: self._on_canvas_configure())

        # Fixed cards
        for label, tz in self.fixed_defs.items():
            self._create_card(label, tz, is_user=False)

        # Add Zone controls
        row += 1
        add_row = ttk.Frame(self)
        add_row.grid(row=row, column=0, columnspan=3, sticky="we", padx=12, pady=(4, 8))
        ttk.Label(add_row, text="Add time zone").pack(side="left")
        self.zone_combo = ttk.Combobox(add_row, textvariable=self.add_zone_var, values=pytz.common_timezones, width=42)
        self.zone_combo.pack(side="left", padx=(8, 8))
        ttk.Button(add_row, text="Add", command=self._add_zone).pack(side="left")

        # Save/Reset mock (keep hooks for later persistence)
        row += 1
        btns = ttk.Frame(self)
        btns.grid(row=row, column=0, columnspan=3, sticky="w", padx=12, pady=(6, 12))
        ttk.Button(btns, text="Save (Mock)", command=self._mock_save).pack(side="left")
        ttk.Button(btns, text="Reset (Mock)", command=self._mock_reset).pack(side="left", padx=8)

    # -------------------------
    # Layout helpers
    # -------------------------
    def _on_cards_configure(self):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self):
        # keep inner frame width synced to canvas width
        canvas_w = self.canvas.winfo_width()
        self.canvas.itemconfig(self.canvas_window, width=canvas_w)
        self._relayout_cards()

    def _relayout_cards(self):
        # Wrap cards by available width (1–4 cols)
        w = max(self.canvas.winfo_width(), 1)
        pad = 10
        card_w = 260
        cols = max(1, min(4, (w - 16) // (card_w + pad)))
        for i, (_, card) in enumerate(self.cards.items()):
            r = i // cols
            c = i % cols
            card.grid(row=r, column=c, padx=8, pady=8, sticky="n")
        # expand columns
        for i in range(6):
            self.cards_frame.grid_columnconfigure(i, weight=1 if i < cols else 0)

    # -------------------------
    # Cards
    # -------------------------
    def _get_local_dt(self) -> datetime:
        return datetime.now(self._local_tzinfo)

    def _create_card(self, label, tz, is_user: bool, zone_name: str | None = None):
        # Close handler for user-added cards
        def handle_close():
            if zone_name is None:
                return
            card = self.cards.pop(zone_name, None)
            if card:
                card.destroy()
            if zone_name in self.user_tzs:
                self.user_tzs.remove(zone_name)
            self._relayout_cards()

        close_cb = handle_close if is_user else None
        card = ClockCard(
            self.cards_frame, label=label, tz=tz,
            get_local_dt=self._get_local_dt,
            use_24h_var=self.use_24h,
            show_seconds_var=self.show_seconds,
            on_close=close_cb
        )
        key = zone_name if zone_name else f"__fixed__:{label}"
        self.cards[key] = card
        self._relayout_cards()

    # -------------------------
    # Actions
    # -------------------------
    def _add_zone(self):
        zone = (self.add_zone_var.get() or "").strip()
        if not zone:
            return
        if zone not in pytz.common_timezones:
            messagebox.showerror("Invalid time zone", f"'{zone}' is not a recognized time zone.")
            return

        # Prevent duplicates (including fixed)
        fixed_zones = {getattr(tz, "zone", None) for tz in self.fixed_defs.values()}
        if zone in self.user_tzs or zone in fixed_zones:
            messagebox.showinfo("Already added", f"'{zone}' is already displayed.")
            return

        label = zone
        tz = pytz.timezone(zone)
        self.user_tzs.append(zone)
        self._create_card(label, tz, is_user=True, zone_name=zone)
        self.add_zone_var.set("")

    def _mock_save(self):
        # Hook for persistence if you wire a settings store later
        messagebox.showinfo("Settings", "Settings saved (placeholder).")

    def _mock_reset(self):
        # Reset settings
        self.api_key.set("")
        self.api_secret.set("")
        self.use_testnet.set(False)
        self.default_leverage.set(10)
        self.quote_asset.set("USDT")
        self.use_24h.set(True)
        self.show_seconds.set(True)

        # Remove all user-added cards
        for z in list(self.user_tzs):
            card = self.cards.pop(z, None)
            if card:
                card.destroy()
        self.user_tzs.clear()
        self._relayout_cards()

    # -------------------------
    # Clock updater
    # -------------------------
    def _tick(self):
        for card in self.cards.values():
            card.update_values()
        # 1s refresh
        self.after(1000, self._tick)
