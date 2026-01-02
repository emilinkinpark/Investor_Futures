# api.py
import hmac
import hashlib
import time
import random
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import BASE_URL, API_KEY, API_SECRET


class BinanceAPI:
    """
    Robust wrapper around Binance Futures REST endpoints used by the app.
    Adds: persistent session, retries with backoff, bigger timeouts, edge rotation.
    """

    ALT_BASES = [
        "https://fapi.binance.com",
        "https://fapi1.binance.com",
        "https://fapi3.binance.com",
    ]

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        recv_window: int = 5000,
    ):
        self.base_url = base_url or BASE_URL
        self.api_key = api_key or API_KEY
        self.api_secret = (api_secret or API_SECRET).encode()
        self.recv_window = recv_window

        # Single shared session with retries + backoff
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})

        retry = Retry(
            total=6,                # total retry budget
            connect=6,              # retry connect errors
            read=4,                 # retry read timeouts
            status=6,               # retry 5xx/429
            status_forcelist=(429, 500, 502, 503, 504),
            backoff_factor=0.6,     # exponential backoff
            allowed_methods=frozenset(["GET", "POST", "DELETE"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Timeouts: (connect, read)
        self.timeout = (25, 20)

        # Cache exchange filters
        self._exchange_filters_cache: dict[str, Tuple[str, str]] = {}

        # Failure grouping for logs
        self._fail_count = 0
        self._last_fail_bucket = 0

    # ---------- low-level helpers ----------

    def _ts(self) -> int:
        return int(time.time() * 1000)

    def _sign_params(self, params: dict) -> dict:
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        signature = hmac.new(self.api_secret, query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = signature
        return params

    def _request(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> requests.Response:
        params = (params or {}).copy()
        if signed:
            params.update({"timestamp": self._ts(), "recvWindow": self.recv_window})
            params = self._sign_params(params)

        url = f"{self.base_url}{path}"
        if method == "GET":
            return self.session.get(url, params=params, timeout=self.timeout)
        elif method == "POST":
            return self.session.post(url, params=params, timeout=self.timeout)
        else:
            raise ValueError("Unsupported method")

    def _ping(self, base: Optional[str] = None) -> bool:
        base = base or self.base_url
        try:
            r = self.session.get(f"{base}/fapi/v1/ping", timeout=(10, 10))
            return r.status_code == 200
        except requests.RequestException:
            return False

    def _maybe_rotate_edge(self) -> None:
        """Try alternate REST edges if current base is not responding to ping."""
        if self._ping(self.base_url):
            return
        for alt in self.ALT_BASES:
            if alt == self.base_url:
                continue
            if self._ping(alt):
                print(f"[REST] Switching REST base to {alt}")
                self.base_url = alt
                return

    def _log_fail(self, msg: str):
        self._fail_count += 1
        bucket = min(self._fail_count // 3, 5)
        if bucket != self._last_fail_bucket or self._fail_count <= 2:
            print(f"[REST] {msg} (fail #{self._fail_count})")
            self._last_fail_bucket = bucket

    def _reset_fail(self):
        self._fail_count = 0
        self._last_fail_bucket = 0

    # ---------- symbol filters / qty rounding ----------

    def _get_symbol_filters(self, symbol: str) -> Tuple[str, str]:
        if symbol in self._exchange_filters_cache:
            return self._exchange_filters_cache[symbol]

        try:
            resp = self.session.get(
                f"{self.base_url}/fapi/v1/exchangeInfo",
                params={"symbol": symbol},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            info = resp.json()
            if "symbols" not in info or not info["symbols"]:
                raise RuntimeError(f"exchangeInfo missing for {symbol}")

            s = info["symbols"][0]
            step_size = "0.001"
            min_qty = "0.0"
            for f in s.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    step_size = f.get("stepSize", step_size)
                    min_qty = f.get("minQty", min_qty)
                    break

            self._exchange_filters_cache[symbol] = (step_size, min_qty)
            return step_size, min_qty
        except Exception as e:
            self._log_fail(f"exchangeInfo error for {symbol}: {e}")
            # Safe defaults in worst-case; you can handle this upstream if preferred
            return "0.001", "0.0"

    def round_qty(self, symbol: str, qty: float) -> str:
        step_size, _ = self._get_symbol_filters(symbol)
        step = Decimal(step_size)
        q = (Decimal(str(qty)) // step) * step  # floor to step
        return format(q.normalize(), "f")

    # ---------- account/position mode ----------

    def is_dual_side(self) -> bool:
        r = self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        r.raise_for_status()
        return bool(r.json().get("dualSidePosition", False))

    # ---------- high-level helpers ----------

    def fetch_ratio_data(self, symbol: str) -> Dict[str, str]:
        if not symbol:
            return {}
        try:
            coin = self.session.get(
                f"{self.base_url}/futures/data/topLongShortPositionRatio",
                params={"symbol": symbol, "period": "1h", "limit": 1},
                timeout=self.timeout,
            ).json()
            trader = self.session.get(
                f"{self.base_url}/futures/data/globalLongShortAccountRatio",
                params={"symbol": symbol, "period": "1h", "limit": 1},
                timeout=self.timeout,
            ).json()
            return {
                "coin_ratio": f"{float(coin[0]['longShortRatio']):.2f}" if coin else "N/A",
                "trader_ratio": f"{float(trader[0]['longShortRatio']):.2f}" if trader else "N/A",
            }
        except Exception:
            return {}

    def fetch_cnd_rating(self, symbol: str) -> str:
        cnd_ratings = {
            "BTCUSDT": "Strong Buy",
            "ETHUSDT": "Buy",
            "SOLUSDT": "Neutral",
            "XRPUSDT": "Sell",
            "ADAUSDT": "Strong Sell",
        }
        return cnd_ratings.get(symbol or "", "N/A")

    def get_positions(self) -> List[Dict]:
        """
        Returns processed open positions with PNL/ROE computed.
        On REST outage, returns [] and logs the root cause clearly.
        """
        # Quick health + edge rotation if needed
        if not self._ping(self.base_url):
            self._log_fail("Ping failed; REST edge unreachable.")
            self._maybe_rotate_edge()
            if not self._ping(self.base_url):
                # Still unreachable — bail out this cycle
                return []

        try:
            resp = self._request("GET", "/fapi/v2/positionRisk", signed=True)
            if not resp.ok:
                # Differentiate common cases
                if resp.status_code == 429:
                    self._log_fail(f"HTTP 429 rate limited; body={resp.text[:180]}")
                elif 500 <= resp.status_code <= 599:
                    self._log_fail(f"HTTP {resp.status_code}; body={resp.text[:180]}")
                else:
                    self._log_fail(f"HTTP {resp.status_code}; body={resp.text[:180]}")
                return []

            self._reset_fail()
            out: List[Dict] = []
            for pos in resp.json():
                amt = float(pos["positionAmt"])
                if amt == 0:
                    continue
                entry = float(pos["entryPrice"])
                mark = float(pos["markPrice"])
                lev = int(pos.get("leverage", 0) or 0)
                pnl = amt * (mark - entry) * (1 if amt > 0 else -1)
                denom = (abs(amt) * entry / lev) if (lev and entry and abs(amt)) else 0.0
                roe = (pnl / denom * 100.0) if denom else 0.0
                out.append(
                    {
                        "symbol": pos["symbol"],
                        "side": "LONG" if amt > 0 else "SHORT",
                        "size": abs(amt),
                        "entry": entry,
                        "mark": mark,
                        "pnl": pnl,
                        "roe": roe,
                        "leverage": lev,
                        "liquidation": float(pos.get("liquidationPrice", 0.0) or 0.0),
                    }
                )
            return out

        except requests.exceptions.ConnectTimeout:
            self._log_fail("ConnectTimeout to Binance (check network or firewall).")
            self._maybe_rotate_edge()
            return []
        except requests.exceptions.ReadTimeout:
            self._log_fail("ReadTimeout awaiting response from Binance.")
            return []
        except requests.RequestException as e:
            self._log_fail(f"RequestException: {e}")
            return []

    def close_position(self, symbol: str, side: str, position_size: float) -> bool:
        if position_size <= 0:
            print(f"[API] Nothing to close for {symbol}")
            return False

        try:
            dual = self.is_dual_side()
        except Exception as e:
            print(f"[API] Could not fetch position mode: {e}")
            dual = False

        opposite = "SELL" if side == "LONG" else "BUY"
        position_side = "LONG" if side == "LONG" else "SHORT"

        qty_str = self.round_qty(symbol, position_size)
        if Decimal(qty_str) <= 0:
            print(f"[API] Computed qty is 0 after rounding for {symbol}")
            return False

        params = {
            "symbol": symbol,
            "side": opposite,
            "type": "MARKET",
            "quantity": qty_str,
            "reduceOnly": "true",
        }
        if dual:
            params["positionSide"] = position_side

        r = self._request("POST", "/fapi/v1/order", params=params, signed=True)
        if not r.ok:
            print(f"[API] Close order failed for {symbol}: {r.text[:200]}")
            return False

        print(f"[API] Close order sent for {symbol}: {params}")
        return True
