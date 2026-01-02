"""
Lightweight performance timing decorator.

Usage:

    from utils.perf import timed

    @timed("auto_scan", threshold_ms=100)
    def _scan_all_symbols(...):
        ...

If threshold_ms is given, it only logs when the function
takes longer than that many milliseconds.
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def timed(label: Optional[str] = None, threshold_ms: Optional[float] = None):
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        name = label or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                dt_ms = (time.perf_counter() - start) * 1000.0
                if threshold_ms is None or dt_ms >= threshold_ms:
                    logger.info("[PERF] %s took %.1f ms", name, dt_ms)

        return wrapper

    return decorator
