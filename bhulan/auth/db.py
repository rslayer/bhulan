"""SQLite storage for users, magic-link tokens, sessions, and history.

Intentionally uses stdlib ``sqlite3`` — no ORM. The schema is four tables,
each row stays under 64 kB (history payloads are capped), and the read
pattern is always "WHERE user_id = ? ORDER BY id DESC LIMIT N", so an
ORM would be over-engineering.

Concurrency: one connection per request (cheap — SQLite opens are sub-ms),
``check_same_thread=False`` disabled because FastAPI dispatches each
request on the threadpool. We rely on SQLite's own locking (WAL mode)
for write concurrency.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    email      TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL  -- unix seconds
);

CREATE TABLE IF NOT EXISTS magic_links (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    email        TEXT NOT NULL,
    token_hash   TEXT NOT NULL UNIQUE,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    consumed_at  INTEGER          -- NULL until redeemed
);
CREATE INDEX IF NOT EXISTS idx_magic_links_expires
    ON magic_links(expires_at);

CREATE TABLE IF NOT EXISTS sessions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    revoked_at  INTEGER          -- NULL while active
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at    INTEGER NOT NULL,
    kind          TEXT NOT NULL,            -- 'insights' | 'compare' | ...
    label         TEXT,                     -- user-supplied or auto-generated
    request_json  TEXT NOT NULL,            -- the raw payload (capped upstream)
    summary_json  TEXT NOT NULL             -- a compact projection of the result
);
CREATE INDEX IF NOT EXISTS idx_history_user_created
    ON history(user_id, created_at DESC);
"""

# A module-level lock guards the one-time initialization. SQLite itself is
# safe for concurrent use, but we want to avoid two workers both trying to
# run CREATE TABLE at exactly the same instant on first boot.
_INIT_LOCK = threading.Lock()
_INITIALIZED_PATHS: set[str] = set()


def _ensure_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def init_db(path: str) -> None:
    """Create the DB file + schema if missing. Idempotent."""
    with _INIT_LOCK:
        if path in _INITIALIZED_PATHS:
            return
        _ensure_dir(path)
        conn = sqlite3.connect(path)
        try:
            # WAL gives us concurrent reads while a writer holds the DB,
            # which is what we want for FastAPI's threadpool.
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()
        _INITIALIZED_PATHS.add(path)


@contextmanager
def connect(path: str) -> Iterator[sqlite3.Connection]:
    """Open a short-lived connection with foreign keys + row factory set up."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reset_db_for_tests(path: Optional[str] = None) -> None:
    """Forget the cached init state. Only for tests that use a fresh tempfile."""
    with _INIT_LOCK:
        if path is None:
            _INITIALIZED_PATHS.clear()
        else:
            _INITIALIZED_PATHS.discard(path)
