# services/position_logger.py
import csv
import os
import threading
from datetime import datetime, timezone, timedelta

DT_FMT = "%Y-%m-%d %H:%M:%S"

def utcnow():
    return datetime.now(timezone.utc)

def _format_hms(total_seconds: int) -> str:
    if total_seconds is None or total_seconds < 0:
        return ""
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"

def _parse_hms(hms: str) -> int:
    try:
        parts = [int(p) for p in (hms or "").split(":", 2)]
        if len(parts) != 3:
            return 0
        return parts[0]*3600 + parts[1]*60 + parts[2]
    except Exception:
        return 0

class PositionLogger:
    """
    Thread-safe singleton that records:
      - OPEN: symbol, side, qty, entry_price, time_open
      - UPDATE: running unrealized pnl snapshots (optional)
      - CLOSE: exit_price, realized_pnl, time_close, hold_time_hms
    Persists to CSV and keeps an in-memory list for the UI table.

    Public API:
      on_position_open(symbol, side, qty, entry_price, note="")
      on_position_update(symbol, pnl=None)   # optional periodic snapshots
      on_position_close(symbol, exit_price, realized_pnl, note="")
      annotate(symbol, note)
    """
    _instance = None
    _lock = threading.Lock()

    # NOTE: order_id removed; hold_time_s -> hold_time_hms
    HEADERS = [
        "symbol","side","qty","entry_price","exit_price","realized_pnl",
        "status","time_open","time_close","hold_time_hms","last_pnl","notes"
    ]

    def __new__(cls, csv_path="logs/position_log.csv"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init(csv_path)
        return cls._instance

    def _init(self, csv_path):
        self.csv_path = csv_path
        self.rows = []           # list of dicts with HEADERS
        self.index_open = {}     # symbol -> index of open row (latest)
        self.data_lock = threading.Lock()
        self._ensure_csv()
        self._load_csv()

    def _ensure_csv(self):
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writeheader()

    def _load_csv(self):
        """
        Best-effort backward compatibility:
        - If older file contains 'order_id', ignore it.
        - If it contains 'hold_time_s', convert to hold_time_hms.
        """
        try:
            with open(self.csv_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    # Normalize to new schema
                    item = {h: r.get(h, "") for h in self.HEADERS}
                    # Convert from legacy fields if present
                    if not item.get("hold_time_hms"):
                        legacy_secs = r.get("hold_time_s")
                        if legacy_secs:
                            try:
                                item["hold_time_hms"] = _format_hms(int(float(legacy_secs)))
                            except Exception:
                                item["hold_time_hms"] = ""
                    self.rows.append(item)
                    if item.get("status") == "OPEN":
                        self.index_open[item["symbol"]] = len(self.rows) - 1
        except Exception:
            # if malformed, ignore and start fresh schema on next write
            pass

    # ---------- Public API ----------

    def on_position_open(self, symbol, side, qty, entry_price, note=""):
        now = utcnow().strftime(DT_FMT)
        with self.data_lock:
            row = {
                "symbol": symbol,
                "side": (str(side) or "").upper(),
                "qty": str(qty),
                "entry_price": str(entry_price),
                "exit_price": "",
                "realized_pnl": "",
                "status": "OPEN",
                "time_open": now,
                "time_close": "",
                "hold_time_hms": "",
                "last_pnl": "",
                "notes": note or "",
            }
            self.rows.append(row)
            self.index_open[symbol] = len(self.rows) - 1
            self._append_csv(row)

    def on_position_update(self, symbol, pnl=None):
        if pnl is None:
            return
        with self.data_lock:
            idx = self.index_open.get(symbol)
            if idx is None:
                return
            self.rows[idx]["last_pnl"] = str(pnl)

    def on_position_close(self, symbol, exit_price, realized_pnl, note=""):
        now_dt = utcnow()
        now = now_dt.strftime(DT_FMT)
        with self.data_lock:
            idx = self.index_open.get(symbol)
            if idx is None:
                # No open record; create synthetic open to not lose closure.
                self.on_position_open(symbol, side="UNKNOWN", qty="", entry_price="", note="synthetic open (no prior row)")
                idx = self.index_open.get(symbol)

            row = self.rows[idx]
            row["exit_price"] = str(exit_price)
            row["realized_pnl"] = str(realized_pnl)
            row["status"] = "CLOSED"
            row["time_close"] = now
            # compute hold time
            try:
                t_open = datetime.strptime(row["time_open"], DT_FMT).replace(tzinfo=timezone.utc)
                hold_s = int((now_dt - t_open).total_seconds())
                row["hold_time_hms"] = _format_hms(hold_s)
            except Exception:
                row["hold_time_hms"] = ""
            if note:
                row["notes"] = (row.get("notes","") + (" | " if row.get("notes") else "") + note)

            # persist whole file
            self._rewrite_csv()
            # remove from open index
            self.index_open.pop(symbol, None)

    def annotate(self, symbol, note):
        with self.data_lock:
            idx = self.index_open.get(symbol)
            if idx is None:
                # append to last known row for that symbol
                for i in range(len(self.rows)-1, -1, -1):
                    if self.rows[i]["symbol"] == symbol:
                        idx = i
                        break
            if idx is None:
                return
            r = self.rows[idx]
            r["notes"] = (r.get("notes","") + (" | " if r.get("notes") else "") + note)
            self._rewrite_csv()

    # ---------- Helpers ----------

    def _append_csv(self, row):
        try:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writerow(row)
        except Exception:
            pass

    def _rewrite_csv(self):
        try:
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writeheader()
                writer.writerows(self.rows)
        except Exception:
            pass

    # Exposed for the UI
    def get_rows(self):
        with self.data_lock:
            return [dict(r) for r in self.rows]

    def import_csv(self, path):
        with self.data_lock:
            imported = []
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    item = {h: r.get(h, "") for h in self.HEADERS}
                    # legacy support on import
                    if not item.get("hold_time_hms"):
                        legacy_secs = r.get("hold_time_s")
                        if legacy_secs:
                            try:
                                item["hold_time_hms"] = _format_hms(int(float(legacy_secs)))
                            except Exception:
                                item["hold_time_hms"] = ""
                    imported.append(item)
            self.rows.extend(imported)
            # rebuild open index
            self.index_open = {}
            for i, r in enumerate(self.rows):
                if r.get("status") == "OPEN":
                    self.index_open[r["symbol"]] = i
            self._rewrite_csv()

    def export_csv(self, path):
        with self.data_lock:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writeheader()
                writer.writerows(self.rows)
