# ui/data_logging_tab.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
from services.position_logger import PositionLogger, DT_FMT


def _hms_to_seconds(hms: str) -> int:
    try:
        h, m, s = [int(x) for x in (hms or "").split(":", 2)]
        return h * 3600 + m * 60 + s
    except Exception:
        return 0


def _seconds_to_hms(total_seconds: int) -> str:
    if total_seconds <= 0:
        return "00:00:00"
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"


class DataLoggingTab(ttk.Frame):
    """
    Data Logging tab (updated):
      • Shows hold time as HH:MM:SS (column: hold_time_hms)
      • Removes order_id entirely
      • KPIs compute Avg Hold (HH:MM:SS)
    """

    def __init__(self, parent, controller=None):
        """
        `controller` is kept optional so this class works with both:
            DataLoggingTab(notebook)
        and:
            DataLoggingTab(notebook, controller)
        """
        super().__init__(parent)
        self.controller = controller
        self.logger = PositionLogger()
        self._build_ui()
        self._refresh_table()

    # ---------- UI ----------

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        # Toolbar
        bar = ttk.Frame(self)
        bar.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        bar.columnconfigure(8, weight=1)

        ttk.Label(bar, text="Status:").grid(row=0, column=0, padx=4)
        self.status_var = tk.StringVar(value="ALL")
        ttk.Combobox(
            bar,
            textvariable=self.status_var,
            values=["ALL", "OPEN", "CLOSED"],
            width=8,
            state="readonly",
        ).grid(row=0, column=1, padx=4)

        ttk.Label(bar, text="Symbol:").grid(row=0, column=2, padx=4)
        self.symbol_var = tk.StringVar()
        ttk.Entry(bar, textvariable=self.symbol_var, width=10).grid(
            row=0, column=3, padx=4
        )

        ttk.Label(bar, text="From (UTC):").grid(row=0, column=4, padx=4)
        self.from_var = tk.StringVar()
        ttk.Entry(bar, textvariable=self.from_var, width=16).grid(
            row=0, column=5, padx=4
        )
        ttk.Label(bar, text="To (UTC):").grid(row=0, column=6, padx=4)
        self.to_var = tk.StringVar()
        ttk.Entry(bar, textvariable=self.to_var, width=16).grid(
            row=0, column=7, padx=4
        )

        ttk.Button(bar, text="Apply Filters", command=self._refresh_table).grid(
            row=0, column=9, padx=4
        )
        ttk.Button(bar, text="Clear", command=self._clear_filters).grid(
            row=0, column=10, padx=4
        )
        ttk.Button(bar, text="Refresh", command=self._refresh_table).grid(
            row=0, column=11, padx=4
        )

        ttk.Button(bar, text="Export CSV", command=self._export_csv).grid(
            row=0, column=12, padx=4
        )
        ttk.Button(bar, text="Import CSV", command=self._import_csv).grid(
            row=0, column=13, padx=4
        )

        # KPIs row
        kpi = ttk.Frame(self)
        kpi.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        self.kpi_total = ttk.Label(kpi, text="Total Trades: 0")
        self.kpi_total.pack(side="left", padx=10)
        self.kpi_win = ttk.Label(kpi, text="Win Rate: 0.0%")
        self.kpi_win.pack(side="left", padx=10)
        self.kpi_pnl = ttk.Label(kpi, text="Net PnL: 0")
        self.kpi_pnl.pack(side="left", padx=10)
        self.kpi_hold = ttk.Label(kpi, text="Avg Hold: 00:00:00")
        self.kpi_hold.pack(side="left", padx=10)

        # Table
        table_frame = ttk.Frame(self)
        table_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=0)
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        cols = [
            "symbol",
            "side",
            "qty",
            "entry_price",
            "exit_price",
            "realized_pnl",
            "status",
            "time_open",
            "time_close",
            "hold_time_hms",
            "last_pnl",
            "notes",
        ]
        self.tree = ttk.Treeview(
            table_frame, columns=cols, show="headings", selectmode="browse"
        )
        for c in cols:
            header = "Hold (HH:MM:SS)" if c == "hold_time_hms" else c
            self.tree.heading(c, text=header)
            self.tree.column(
                c,
                width=110 if c == "hold_time_hms" else 100,
                anchor=("w" if c == "notes" else "center"),
            )
        self.tree.column("notes", width=240, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")

        yscroll = ttk.Scrollbar(
            table_frame, orient="vertical", command=self.tree.yview
        )
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=yscroll.set)

        # Editor / actions
        actions = ttk.Frame(self)
        actions.grid(row=3, column=0, sticky="ew", padx=8, pady=8)
        ttk.Label(actions, text="Note:").pack(side="left")
        self.note_var = tk.StringVar()
        ttk.Entry(actions, textvariable=self.note_var, width=80).pack(
            side="left", padx=6
        )
        ttk.Button(
            actions,
            text="Save Note to Selected",
            command=self._save_note,
        ).pack(side="left", padx=6)

        # Right-click menu
        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Copy row", command=self._copy_selected)
        self.menu.add_command(label="Copy cell", command=self._copy_cell)
        self.tree.bind("<Button-3>", self._popup_menu)

        # Auto refresh every 5s
        self.after(5000, self._auto_refresh)

    # ---------- Actions ----------

    def _auto_refresh(self):
        try:
            self._refresh_table(kpi_only=True)
        finally:
            self.after(5000, self._auto_refresh)

    def _popup_menu(self, event):
        try:
            self.tree.identify_row(event.y)
            self.menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.menu.grab_release()

    def _copy_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        self.clipboard_clear()
        self.clipboard_append("\t".join(vals))

    def _copy_cell(self):
        sel = self.tree.selection()
        if not sel:
            return
        # For simplicity, copy full row
        self._copy_selected()

    def _save_note(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("No selection", "Select a row to save note.")
            return
        symbol = self.tree.item(sel[0], "values")[0]
        note = self.note_var.get().strip()
        if not note:
            return
        self.logger.annotate(symbol, note)
        self._refresh_table()

    def _clear_filters(self):
        self.status_var.set("ALL")
        self.symbol_var.set("")
        self.from_var.set("")
        self.to_var.set("")
        self._refresh_table()

    def _export_csv(self):
        path = filedialog.asksaveasfilename(
            title="Export CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return
        try:
            self.logger.export_csv(path)
            messagebox.showinfo("Export", f"Exported to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def _import_csv(self):
        path = filedialog.askopenfilename(
            title="Import CSV",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return
        try:
            self.logger.import_csv(path)
            self._refresh_table()
            messagebox.showinfo("Import", f"Imported:\n{path}")
        except Exception as e:
            messagebox.showerror("Import failed", str(e))

    # ---------- Data / KPIs ----------

    def _apply_filters(self, rows):
        status = self.status_var.get()
        sym_q = self.symbol_var.get().strip().upper()
        f = self.from_var.get().strip()
        t = self.to_var.get().strip()
        f_dt = self._parse_dt(f)
        t_dt = self._parse_dt(t)

        out = []
        for r in rows:
            if status != "ALL" and r.get("status") != status:
                continue
            if sym_q and sym_q not in (r.get("symbol", "").upper()):
                continue
            # date filter on time_open
            ropen = r.get("time_open")
            if f_dt and ropen:
                try:
                    ropen_dt = datetime.strptime(ropen, DT_FMT)
                    if ropen_dt < f_dt:
                        continue
                except Exception:
                    pass
            if t_dt and ropen:
                try:
                    ropen_dt = datetime.strptime(ropen, DT_FMT)
                    if ropen_dt > t_dt:
                        continue
                except Exception:
                    pass
            out.append(r)
        return out

    def _parse_dt(self, s):
        if not s:
            return None
        try:
            return datetime.strptime(s, DT_FMT)
        except Exception:
            return None

    def _refresh_table(self, kpi_only=False):
        rows = self.logger.get_rows()
        rows = self._apply_filters(rows)

        if not kpi_only:
            for i in self.tree.get_children():
                self.tree.delete(i)
            for r in rows:
                vals = [
                    r.get(h, "")
                    for h in [
                        "symbol",
                        "side",
                        "qty",
                        "entry_price",
                        "exit_price",
                        "realized_pnl",
                        "status",
                        "time_open",
                        "time_close",
                        "hold_time_hms",
                        "last_pnl",
                        "notes",
                    ]
                ]
                self.tree.insert("", "end", values=vals)

        # KPIs
        total, wins, pnl_sum, hold_sum_s, hold_count = 0, 0, 0.0, 0, 0
        for r in rows:
            if r.get("status") == "CLOSED":
                total += 1
                try:
                    pnl = float(r.get("realized_pnl") or 0.0)
                    pnl_sum += pnl
                    if pnl > 0:
                        wins += 1
                except Exception:
                    pass
                try:
                    hold_sum_s += _hms_to_seconds(
                        r.get("hold_time_hms") or "00:00:00"
                    )
                    hold_count += 1
                except Exception:
                    pass
        win_rate = (wins / total * 100.0) if total else 0.0
        avg_hold_s = int(hold_sum_s / hold_count) if hold_count else 0
        avg_hold_hms = _seconds_to_hms(avg_hold_s)

        self.kpi_total.config(text=f"Total Trades: {total}")
        self.kpi_win.config(text=f"Win Rate: {win_rate:.1f}%")
        self.kpi_pnl.config(text=f"Net PnL: {pnl_sum:.4f}")
        self.kpi_hold.config(text=f"Avg Hold: {avg_hold_hms}")
