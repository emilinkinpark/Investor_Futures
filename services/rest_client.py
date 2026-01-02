"""
Centralised REST client for your app.

- Reuses a single requests.Session with connection pooling
- Adds retry logic for transient network errors
- Provides thin helpers for Binance Futures endpoints
- Designed to be called from worker threads (via utils.threading)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RestClient:
    """
    Singleton-style REST client.

    Usage:
        from services.rest_client import rest_client

        data = rest_client.get_json("/fapi/v1/exchangeInfo")
        klines = rest_client.get_json(
            "/fapi/v1/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 500},
        )
    """

    _BASE_URL = "https://fapi.binance.com"

    def __init__(self, base_url: str | None = None, timeout: float = 10.0):
        self.base_url = (base_url or self._BASE_URL).rstrip("/")
        self.timeout = timeout
        self._session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=40)

        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    # --------------- Core helpers ---------------

    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> requests.Response:
        url = self._build_url(path)
        t = timeout or self.timeout
        logger.debug("GET %s params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=t)
        return resp

    def get_json(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        resp = self.get(path, params=params, timeout=timeout)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.warning("HTTP error for %s: %s (%s)", path, e, resp.text)
            raise
        return resp.json()

    # --------------- Binance convenience helpers ---------------

    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Fetch full /fapi/v1/exchangeInfo. Cached by caller if needed.
        """
        return self.get_json("/fapi/v1/exchangeInfo")

    def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        params: Dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        if extra_params:
            params.update(extra_params)
        return self.get_json("/fapi/v1/klines", params=params)

    def get_mark_price_all(self) -> Any:
        """
        !markPrice@arr equivalent via REST: /fapi/v1/premiumIndex
        """
        return self.get_json("/fapi/v1/premiumIndex")

    def get_funding_rate(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
    ) -> Any:
        params: Dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol.upper()
        return self.get_json("/fapi/v1/fundingRate", params=params)


# Global singleton instance
rest_client = RestClient()
