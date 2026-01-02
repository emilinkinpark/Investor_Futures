"""
Shared thread pool utilities for the whole app.

Use this instead of creating ad-hoc threads in every module.

Typical usage:

    from utils.threading import run_in_thread, run_io_bound

    def heavy_task(arg1, arg2):
        ...

    # Fire-and-forget:
    run_in_thread(heavy_task, "A", 123)

    # If you need the result:
    fut = run_io_bound(heavy_task, "A", 123)
    result = fut.result()
"""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


# A single process-wide pool is enough for this app.
# You can tune the worker count based on CPU/network load.
_MAX_WORKERS = 16

_executor: concurrent.futures.ThreadPoolExecutor | None = None


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _executor
    if _executor is None:
        logger.info("Creating global ThreadPoolExecutor with %d workers", _MAX_WORKERS)
        _executor = concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS)
        atexit.register(_shutdown)
    return _executor


def _shutdown() -> None:
    global _executor
    if _executor is not None:
        logger.info("Shutting down global ThreadPoolExecutor")
        _executor.shutdown(wait=False)
        _executor = None


def run_in_thread(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """
    Fire-and-forget execution of `fn(*args, **kwargs)` in the global pool.
    Exceptions are logged.
    """
    executor = _get_executor()

    def _wrapped() -> None:
        try:
            fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            logger.exception("Error in background task %r: %s", fn, e)

    executor.submit(_wrapped)


def run_io_bound(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> concurrent.futures.Future:
    """
    Submit an IO-bound or CPU-heavy function and return its Future.
    Caller is responsible for checking result/exception.
    """
    executor = _get_executor()
    return executor.submit(fn, *args, **kwargs)
