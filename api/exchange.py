# exchange.py (shorter timeouts to avoid UI stalls)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import BASE_URL

_session = requests.Session()
_retry = Retry(
    total=4,
    connect=4,
    read=2,
    status=4,
    status_forcelist=(429, 500, 502, 503, 504),
    backoff_factor=0.5,
    allowed_methods=frozenset(["GET"]),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=50, pool_maxsize=50)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

def get_symbols():
    """Fetch available USDT perpetual symbols."""
    try:
        response = _session.get(
            f"{BASE_URL}/fapi/v1/exchangeInfo",
            timeout=(3, 10)  # (connect, read)
        )
        response.raise_for_status()
        data = response.json()
        return sorted([
            s["symbol"] for s in data.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
        ])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching symbols: {e}")
        return []
