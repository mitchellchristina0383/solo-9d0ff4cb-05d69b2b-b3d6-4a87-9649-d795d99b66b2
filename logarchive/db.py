"""SQLite metadata store for dictionaries and archived log segments."""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

# SQLite uses "database is locked" for lock contention and "database table is
# locked" when a writer hits a read-only connection holding a schema lock.
# Both are transient and clear as soon as the holder commits or rolls back.
_LOCKED_MARKERS = ("database is locked", "database table is locked")


def is_locked_error(exc: BaseException) -> bool:
    """Return True when *exc* is a transient SQLite lock-contention error."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return any(marker in message for marker in _LOCKED_MARKERS)

SCHEMA = """
CREATE TABLE IF NOT EXISTS dictionaries (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS dictionaries_one_active
    ON dictionaries(active) WHERE active = 1;

CREATE TABLE IF NOT EXISTS segments (
    number                INTEGER PRIMARY KEY AUTOINCREMENT,
    filename              TEXT NOT NULL,
    original_sha256       TEXT NOT NULL,
    original_size         INTEGER NOT NULL,
    blob_name             TEXT NOT NULL UNIQUE,
    compressed_sha256     TEXT NOT NULL,
    compressed_size       INTEGER NOT NULL,
    dictionary_id         TEXT REFERENCES dictionaries(id),
    created_at            TEXT NOT NULL,
    recompress_state      TEXT,
    recompress_token      TEXT,
    recompress_attempts   INTEGER NOT NULL DEFAULT 0,
    recompress_next_at    REAL NOT NULL DEFAULT 0,
    recompress_error      TEXT
);
CREATE INDEX IF NOT EXISTS segments_dictionary
    ON segments(dictionary_id);
CREATE INDEX IF NOT EXISTS segments_recompress
    ON segments(recompress_state, recompress_next_at);
"""


class Database:
    def __init__(self, path: Path, busy_timeout_seconds: float = 30.0):
        self.path = Path(path)
        self.busy_timeout_ms = max(1, int(busy_timeout_seconds * 1000))
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            # The busy timeout makes each individual statement wait for the
            # write lock instead of failing instantly; the worker adds its own
            # retry loop on top, so contention only ever delays work.
            timeout=self.busy_timeout_ms / 1000.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialise(self) -> None:
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        """A serialised read-modify-write transaction.

        BEGIN IMMEDIATE takes the write lock up front so a read followed by a
        conditional write cannot interleave with dictionary activation or a
        concurrent recompression.
        """
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.close()

    @contextmanager
    def write_with_retry(
        self,
        *,
        initial_delay: float = 0.1,
        max_delay: float = 5.0,
        wait=None,
    ) -> Iterator[sqlite3.Connection]:
        """Like :meth:`write`, but survive a long-running external lock.

        A maintenance connection can hold the SQLite write lock for minutes.
        Instead of letting ``OperationalError`` kill the worker, the whole
        transaction is retried with capped exponential backoff until it
        acquires the lock.  *wait* is an optional ``Event.wait``-style callable
        used for the backoff; when it returns true (e.g. shutdown was
        requested), the last lock error is re-raised so callers can unwind.
        """
        delay = initial_delay
        while True:
            try:
                with self.write() as conn:
                    yield conn
                return
            except sqlite3.OperationalError as exc:
                if not is_locked_error(exc):
                    raise
                if wait is not None:
                    interrupted = wait(delay)
                else:
                    time.sleep(delay)
                    interrupted = False
                if interrupted:
                    raise
                delay = min(max_delay, delay * 2)

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
        with self.read() as conn:
            return conn.execute(sql, params).fetchone()

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.read() as conn:
            return conn.execute(sql, params).fetchall()
