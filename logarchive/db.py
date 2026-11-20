"""SQLite metadata store for dictionaries and archived log segments."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

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
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
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

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
        with self.read() as conn:
            return conn.execute(sql, params).fetchone()

    def query_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.read() as conn:
            return conn.execute(sql, params).fetchall()
