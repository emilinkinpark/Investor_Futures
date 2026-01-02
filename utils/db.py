"""
SQLite helper utilities.

Goals:
- Enable WAL mode for better concurrency
- Set a busy timeout to avoid "database is locked"
- Provide small helpers for common patterns
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

logger = logging.getLogger(__name__)


def connect(path: str | Path) -> sqlite3.Connection:
    """
    Open a SQLite connection with WAL + busy timeout.
    Caller is responsible for closing the connection.
    """
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=10.0, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")  # 5 seconds
    except sqlite3.Error as e:  # pragma: no cover - defensive
        logger.warning("Failed to set pragmas on %s: %s", db_path, e)
    return conn


def fetch_one(
    conn: sqlite3.Connection, query: str, params: Sequence[Any] | None = None
) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, params or [])
    return cur.fetchone()


def fetch_all(
    conn: sqlite3.Connection, query: str, params: Sequence[Any] | None = None
) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, params or [])
    return cur.fetchall()


def execute_many(
    conn: sqlite3.Connection,
    query: str,
    seq_of_params: Iterable[Sequence[Any]],
) -> None:
    cur = conn.cursor()
    cur.executemany(query, seq_of_params)
    conn.commit()
