"""Archive domain logic: dictionaries, segment ingestion and recompression."""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import storage
from .config import Config
from .db import Database, is_locked_error
from .storage import StorageError

log = logging.getLogger(__name__)

SEGMENT_LIMIT_DEFAULT = 100
SEGMENT_LIMIT_MAX = 200
MAX_FILENAME_LENGTH = 200
MAX_DICTIONARY_NAME = 80

# Backoff between attempts to (re)acquire the SQLite write lock while an
# external maintenance transaction holds it.  The wait itself is interruptible
# by shutdown, and the cap keeps recovery prompt once the lock clears.
LOCK_RETRY_INITIAL_DELAY = 0.1
LOCK_RETRY_MAX_DELAY = 5.0


class NotFoundError(Exception):
    pass


class ConflictError(Exception):
    def __init__(self, message: str, code: str = "conflict", details: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class ValidationError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sanitize_filename(raw: Optional[str], fallback: str = "segment.log") -> str:
    if not raw:
        return fallback
    raw = raw.replace("\\", "/").split("/")[-1]
    cleaned = "".join(ch for ch in raw if ch.isprintable() and ch not in "\r\n\t")
    cleaned = cleaned.strip().strip(".")
    if not cleaned:
        return fallback
    if len(cleaned) > MAX_FILENAME_LENGTH:
        stem, dot, suffix = cleaned.rpartition(".")
        if dot and len(suffix) <= 16:
            cleaned = stem[: MAX_FILENAME_LENGTH - len(suffix) - 1] + "." + suffix
        else:
            cleaned = cleaned[:MAX_FILENAME_LENGTH]
    return cleaned


class ArchiveService:
    def __init__(self, config: Config):
        self.config = config
        self.data_dir = Path(config.data_dir)
        self.blob_dir = self.data_dir / "blobs"
        self.dict_dir = self.data_dir / "dictionaries"
        self.tmp_dir = self.data_dir / "tmp"
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        self.dict_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        self.db = Database(
            self.data_dir / "archive.sqlite3",
            busy_timeout_seconds=config.db_busy_timeout_seconds,
        )
        self.db.initialise()

        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._worker_state = "stopped" if not config.worker_enabled else "starting"
        self._current_segment: Optional[int] = None

        self._recover_after_restart()
        if config.worker_enabled:
            # Orphan cleanup touches the shared blob directory, so it only runs
            # in the single process that also owns the conversion worker.  A
            # second, worker-less instance (e.g. a read-only sidecar) can then
            # share the same volume safely.
            self._cleanup_orphans()

    # ------------------------------------------------------------------ setup

    def _recover_after_restart(self) -> None:
        """Resume conversions after a restart.

        'running' rows mean the process died mid-conversion: the old blob was
        never replaced, so the job is simply queued again.  Failed jobs get a
        fresh attempt budget too, because a restart commonly clears transient
        causes such as a temporarily unreadable volume.
        """
        with self.db.write_with_retry() as conn:
            interrupted = conn.execute(
                """
                UPDATE segments
                   SET recompress_state = 'pending',
                       recompress_token = NULL,
                       recompress_next_at = 0
                 WHERE recompress_state = 'running'
                """
            ).rowcount
            retried = conn.execute(
                """
                UPDATE segments
                   SET recompress_state = 'pending',
                       recompress_token = NULL,
                       recompress_attempts = 0,
                       recompress_next_at = 0
                 WHERE recompress_state = 'failed'
                """
            ).rowcount
        if interrupted:
            log.info("reset %d interrupted recompression job(s)", interrupted)
        if retried:
            log.info("re-queued %d previously failed recompression job(s)", retried)

    def _cleanup_orphans(self) -> None:
        referenced_blobs = {
            row["blob_name"]
            for row in self.db.query_all("SELECT blob_name FROM segments")
        }
        for entry in self.blob_dir.iterdir():
            if entry.is_file() and entry.name not in referenced_blobs:
                log.warning("removing orphaned blob %s", entry.name)
                storage.remove_quietly(entry)

        known_dicts = {
            f"{row['id']}.dict"
            for row in self.db.query_all("SELECT id FROM dictionaries")
        }
        for entry in self.dict_dir.iterdir():
            if entry.is_file() and entry.name not in known_dicts:
                log.warning("removing orphaned dictionary file %s", entry.name)
                storage.remove_quietly(entry)

        for entry in self.tmp_dir.iterdir():
            if entry.is_file():
                storage.remove_quietly(entry)

    def _new_temp(self, prefix: str) -> Path:
        fd, path = tempfile.mkstemp(prefix=f"{prefix}-", dir=self.tmp_dir)
        os.close(fd)
        return Path(path)

    # --------------------------------------------------------------- lifecycle

    def start_worker(self) -> None:
        if not self.config.worker_enabled:
            self._worker_state = "disabled"
            return
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker_state = "idle"
        self._worker = threading.Thread(
            target=self._worker_loop, name="recompression-worker", daemon=True
        )
        self._worker.start()

    def stop_worker(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._worker:
            self._worker.join(timeout=60)
            if self._worker.is_alive():
                log.warning("recompression worker did not stop in time")
            self._worker = None
        self._worker_state = "stopped"

    def notify_worker(self) -> None:
        self._wake_event.set()

    def _worker_loop(self) -> None:
        log.info("recompression worker started")
        try:
            while not self._stop_event.is_set():
                self._wake_event.clear()
                # An earlier loop iteration (or a process that vanished without
                # a clean shutdown) can leave rows in 'running'.  In this
                # single-instance service the worker is the only owner of that
                # state, so anything left behind is orphaned work that must be
                # re-queued.  Retry counters are deliberately untouched.
                if not self._reap_stale_running():
                    continue
                processed = 0
                try:
                    batch = self._claim_batch()
                except Exception as exc:
                    # A locked database must never kill the worker: the next
                    # iteration retries once the maintenance transaction ends.
                    if is_locked_error(exc):
                        log.warning(
                            "database locked while claiming recompression work; "
                            "will retry after the lock clears"
                        )
                        self._wake_event.wait(LOCK_RETRY_MAX_DELAY)
                        continue
                    log.exception("could not claim recompression work")
                    batch = []
                lock_stalled = False
                for index, row in enumerate(batch):
                    if self._stop_event.is_set():
                        self._requeue_claimed(batch[index:])
                        break
                    try:
                        self._convert_claimed(row)
                    except sqlite3.OperationalError as exc:
                        if not is_locked_error(exc):
                            raise
                        # The conversion's bookkeeping transaction could not
                        # acquire the write lock.  Put this segment and every
                        # sibling not started yet back in the queue without
                        # spending the segment's retry budget, then wait for
                        # the lock.  The reaper at the top of the next loop
                        # also picks up anything this re-queue could not write.
                        log.warning(
                            "database locked during recompression of segment %d; "
                            "re-queuing the rest of the batch until the lock clears",
                            row["number"],
                        )
                        self._requeue_claimed([row, *batch[index + 1 :]])
                        lock_stalled = True
                        break
                    processed += 1
                    if self.config.recompress_delay_ms and not self._stop_event.is_set():
                        self._stop_event.wait(self.config.recompress_delay_ms / 1000.0)
                self._worker_state = "idle"
                if not processed:
                    self._wake_event.wait(
                        LOCK_RETRY_MAX_DELAY if lock_stalled else 0.25
                    )
        except Exception:
            # Absolute last line of defence: the worker must never die silently
            # and leave the web UI stuck on a never-finishing conversion.  The
            # stale-row reaper on the next pass recovers any half-written job.
            log.exception("unexpected error in the recompression worker loop")
            self._worker_state = "idle"
            if not self._stop_event.is_set():
                self._stop_event.wait(LOCK_RETRY_MAX_DELAY)
                if not self._stop_event.is_set():
                    self._worker_loop()
        finally:
            if self._stop_event.is_set():
                self._worker_state = "stopped"
            self._current_segment = None
            log.info("recompression worker stopped")

    def _reap_stale_running(self) -> bool:
        """Re-queue 'running' rows no live conversion can still own.

        Only this single-instance worker ever writes the 'running' state, and
        it converts one segment at a time inside the worker loop.  The reaper
        runs when no conversion is in flight (between batches), so every
        'running' row it sees is orphaned: a previous loop iteration was
        interrupted before its bookkeeping, typically by database lock
        contention.  The rows go back to 'pending' with
        ``recompress_attempts`` preserved, so a lock-induced interruption
        never spends the segment's retry budget.

        Returns ``False`` when shutdown interrupted the write and the loop
        should exit instead of claiming new work.
        """
        try:
            with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
                conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'pending',
                           recompress_token = NULL,
                           recompress_next_at = 0
                     WHERE recompress_state = 'running'
                    """
                )
        except sqlite3.OperationalError as exc:
            if is_locked_error(exc) and self._stop_event.is_set():
                return False
            raise
        return True

    def _claim_batch(self) -> list[sqlite3.Row]:
        with self.db.read() as conn:
            active = self._active_dictionary(conn)
            if not active:
                return []
            limit = self.config.recompress_batch_size
            rows = conn.execute(
                """
                SELECT * FROM segments
                 WHERE recompress_state = 'pending'
                   AND recompress_next_at <= ?
                   AND (dictionary_id IS NULL OR dictionary_id != ?)
                 ORDER BY number
                 LIMIT ?
                """,
                (time.time(), active["id"], limit),
            ).fetchall()
        if not rows:
            return []
        token = uuid.uuid4().hex
        claimed: list[dict[str, Any]] = []
        with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
            for row in rows:
                cur = conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'running',
                           recompress_token = ?,
                           recompress_error = NULL
                     WHERE number = ? AND recompress_state = 'pending'
                    """,
                    (token, row["number"]),
                )
                if cur.rowcount:
                    claimed.append({**dict(row), "recompress_token": token})
        return claimed

    def _requeue_claimed(self, rows: list[dict[str, Any]]) -> None:
        """Return claimed-but-unfinished rows to the queue.

        Retry counters and backoff are intentionally left untouched: this is
        used for shutdown, for dictionary switches and for lock-induced
        interruptions, none of which are conversion failures.
        """
        if not rows:
            return
        with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
            for row in rows:
                conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'pending',
                           recompress_token = NULL,
                           recompress_next_at = 0
                     WHERE number = ? AND recompress_state = 'running'
                       AND recompress_token = ?
                    """,
                    (row["number"], row.get("recompress_token")),
                )

    def _convert_claimed(self, row: dict[str, Any]) -> None:
        number = row["number"]
        self._worker_state = "converting"
        self._current_segment = number
        new_blob_path: Optional[Path] = None
        original_tmp: Optional[Path] = None
        try:
            with self.db.read() as conn:
                active = self._active_dictionary(conn)
            if not active:
                self._requeue_claimed([row])
                return
            target_id = active["id"]
            if row["dictionary_id"] == target_id:
                self._mark_done_without_work(row, target_id)
                return

            target_dict = self._load_dictionary_bytes(active["id"], active["sha256"])
            source_dict = None
            if row["dictionary_id"]:
                source_row = self.db.query_one(
                    "SELECT * FROM dictionaries WHERE id = ?", (row["dictionary_id"],)
                )
                if not source_row:
                    raise StorageError(
                        f"source dictionary {row['dictionary_id']} is missing"
                    )
                source_dict = self._load_dictionary_bytes(
                    source_row["id"], source_row["sha256"]
                )

            original_tmp = self._new_temp("recompress-source")
            source_blob = self.blob_dir / row["blob_name"]
            sha, size, _ = storage.decompress_file(
                source_blob, original_tmp, source_dict
            )
            if sha != row["original_sha256"] or size != row["original_size"]:
                raise StorageError(
                    "stored segment does not reproduce the recorded original bytes"
                )

            candidate = self._new_temp("recompress-target")
            new_blob_path = candidate
            comp_sha, comp_size = storage.compress_file(
                original_tmp, candidate, target_dict
            )
            if self.config.verify_writes:
                self._verify_compressed(
                    candidate, target_dict, row["original_sha256"], row["original_size"]
                )

            blob_name = f"{uuid.uuid4().hex}.deflate"
            final_blob = self.blob_dir / blob_name
            storage.atomic_install(candidate, final_blob)
            new_blob_path = final_blob

            committed = False
            abandoned = False
            # The commit is the only write lock this conversion needs.  If a
            # maintenance transaction holds that lock, the whole commit is
            # retried (the conditional UPDATE makes every retry idempotent)
            # instead of failing the segment or stalling the worker.
            with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
                current = self._active_dictionary(conn)
                if not current or current["id"] != target_id:
                    conn.execute(
                        """
                        UPDATE segments
                           SET recompress_state = 'pending',
                               recompress_token = NULL,
                               recompress_next_at = 0
                         WHERE number = ? AND recompress_state = 'running'
                           AND recompress_token = ?
                        """,
                        (number, row["recompress_token"]),
                    )
                    abandoned = True
                else:
                    cur = conn.execute(
                        """
                        UPDATE segments
                           SET blob_name = ?, compressed_sha256 = ?,
                               compressed_size = ?, dictionary_id = ?,
                               recompress_state = NULL, recompress_token = NULL,
                               recompress_error = NULL, recompress_attempts = 0,
                               recompress_next_at = 0
                         WHERE number = ? AND recompress_state = 'running'
                           AND recompress_token = ? AND blob_name = ?
                        """,
                        (
                            blob_name,
                            comp_sha,
                            comp_size,
                            target_id,
                            number,
                            row["recompress_token"],
                            row["blob_name"],
                        ),
                    )
                    committed = cur.rowcount == 1

            # The blob cleanup runs only after the transaction eventually
            # commits, so a lock-induced retry can never remove a file another
            # attempt still needs.
            if committed:
                old_blob = self.blob_dir / row["blob_name"]
                if old_blob != final_blob:
                    storage.remove_quietly(old_blob)
                log.info(
                    "segment %d recompressed with dictionary %s (%d -> %d bytes)",
                    number,
                    target_id,
                    row["original_size"],
                    comp_size,
                )
            elif abandoned:
                storage.remove_quietly(final_blob)
                new_blob_path = None
                log.info(
                    "segment %d recompression abandoned; dictionary changed mid-flight",
                    number,
                )
            else:
                # The conditional update matched nothing: another flow (e.g. a
                # dictionary activation) already took ownership of the row.
                storage.remove_quietly(final_blob)
                new_blob_path = None
        except sqlite3.OperationalError as exc:
            # Lock contention is an infrastructure delay, not a failed
            # conversion: do not spend the retry budget.  Drop any blob that
            # was installed, re-raise so the worker loop re-queues the batch,
            # and the next loop iteration resumes automatically once the
            # maintenance transaction releases the write lock.
            if not is_locked_error(exc):
                self._record_conversion_failure(row, exc, new_blob_path, original_tmp)
                return
            log.warning(
                "recompression of segment %d blocked by a database lock: %s",
                number,
                exc,
            )
            if new_blob_path:
                storage.remove_quietly(new_blob_path)
            raise
        except Exception as exc:
            self._record_conversion_failure(row, exc, new_blob_path, original_tmp)
        finally:
            if original_tmp:
                storage.remove_quietly(original_tmp)
            self._current_segment = None
            self._worker_state = "idle"

    def _record_conversion_failure(
        self,
        row: dict[str, Any],
        exc: Exception,
        new_blob_path: Optional[Path],
        original_tmp: Optional[Path],
    ) -> None:
        log.warning("recompression of segment %d failed: %s", row["number"], exc)
        if new_blob_path:
            storage.remove_quietly(new_blob_path)
        self._record_failure(row, exc)

    def _mark_done_without_work(self, row: dict[str, Any], target_id: str) -> None:
        with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
            conn.execute(
                """
                UPDATE segments
                   SET recompress_state = NULL, recompress_token = NULL,
                       recompress_error = NULL, recompress_attempts = 0
                 WHERE number = ? AND recompress_state = 'running'
                   AND recompress_token = ?
                """,
                (row["number"], row["recompress_token"]),
            )

    def _record_failure(self, row: dict[str, Any], exc: Exception) -> None:
        attempts = int(row["recompress_attempts"] or 0) + 1
        exhausted = attempts >= self.config.max_recompress_attempts
        backoff = 0.0 if exhausted else min(60.0, float(2 ** min(attempts, 6)))
        message = f"{type(exc).__name__}: {exc}"[:500]
        # The original blob is left untouched: only retry bookkeeping changes.
        # A locked database must not prevent the failure from being recorded,
        # otherwise the row would be stuck in 'running' forever; the write is
        # retried until the lock clears.  Lock contention itself is handled one
        # level up and never reaches this method, so each genuine failure
        # increments the attempt counter exactly once.
        with self.db.write_with_retry(wait=self._stop_event.wait) as conn:
            cur = conn.execute(
                """
                UPDATE segments
                   SET recompress_state = ?,
                       recompress_token = NULL,
                       recompress_attempts = ?,
                       recompress_next_at = ?,
                       recompress_error = ?
                 WHERE number = ? AND recompress_state = 'running'
                   AND recompress_token = ?
                """,
                (
                    "failed" if exhausted else "pending",
                    attempts,
                    time.time() + backoff,
                    message,
                    row["number"],
                    row["recompress_token"],
                ),
            )
            recorded = cur.rowcount == 1
        if not recorded:
            # The row was re-queued or reclaimed elsewhere (e.g. a dictionary
            # switch reset it); leave its retry budget alone.
            log.info(
                "segment %d no longer owns its conversion; failure not recorded",
                row["number"],
            )

    def _verify_compressed(
        self,
        blob: Path,
        dictionary: Optional[bytes],
        expected_sha: str,
        expected_size: int,
    ) -> None:
        verify_tmp = self._new_temp("verify")
        try:
            sha, size, _ = storage.decompress_file(blob, verify_tmp, dictionary)
            if sha != expected_sha or size != expected_size:
                raise StorageError("round-trip verification produced different bytes")
        finally:
            storage.remove_quietly(verify_tmp)

    # ------------------------------------------------------------- dictionaries

    @staticmethod
    def _active_dictionary(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM dictionaries WHERE active = 1"
        ).fetchone()

    def _load_dictionary_bytes(self, dictionary_id: str, expected_sha: str) -> bytes:
        path = self.dict_dir / f"{dictionary_id}.dict"
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise StorageError(f"dictionary file {dictionary_id} is missing") from exc
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_sha:
            raise StorageError(f"dictionary {dictionary_id} failed checksum validation")
        return data

    def create_dictionary(self, name: str, data: bytes) -> dict[str, Any]:
        if not data:
            raise ValidationError("dictionary must not be empty")
        if len(data) > storage.MAX_DICTIONARY_SIZE:
            raise ValidationError("dictionary must be at most 32768 bytes")
        name = (name or "").strip() or "dictionary"
        if len(name) > MAX_DICTIONARY_NAME:
            name = name[:MAX_DICTIONARY_NAME]

        dictionary_id = uuid.uuid4().hex
        sha = hashlib.sha256(data).hexdigest()
        temp = self._new_temp("dictionary")
        try:
            temp.write_bytes(data)
            with open(temp, "rb+") as fh:
                os.fsync(fh.fileno())
            final = self.dict_dir / f"{dictionary_id}.dict"
            storage.atomic_install(temp, final)
            with self.db.write_with_retry() as conn:
                conn.execute(
                    """
                    INSERT INTO dictionaries (id, name, size, sha256, active, created_at)
                    VALUES (?, ?, ?, ?, 0, ?)
                    """,
                    (dictionary_id, name, len(data), sha, utc_now()),
                )
        except Exception:
            storage.remove_quietly(temp)
            storage.remove_quietly(self.dict_dir / f"{dictionary_id}.dict")
            raise
        return self.get_dictionary(dictionary_id)

    def get_dictionary(self, dictionary_id: str) -> dict[str, Any]:
        row = self.db.query_one(
            """
            SELECT d.*,
                   (SELECT COUNT(*) FROM segments s WHERE s.dictionary_id = d.id)
                       AS segment_count
              FROM dictionaries d WHERE d.id = ?
            """,
            (dictionary_id,),
        )
        if not row:
            raise NotFoundError("dictionary not found")
        return self._dict_row(row)

    def list_dictionaries(self) -> list[dict[str, Any]]:
        rows = self.db.query_all(
            """
            SELECT d.*,
                   (SELECT COUNT(*) FROM segments s WHERE s.dictionary_id = d.id)
                       AS segment_count
              FROM dictionaries d
             ORDER BY d.active DESC, d.created_at DESC
            """
        )
        return [self._dict_row(row) for row in rows]

    @staticmethod
    def _dict_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "size": row["size"],
            "sha256": row["sha256"],
            "active": bool(row["active"]),
            "created_at": row["created_at"],
            "segment_count": row["segment_count"] if "segment_count" in row.keys() else None,
        }

    def activate_dictionary(self, dictionary_id: str) -> dict[str, Any]:
        with self.db.write_with_retry() as conn:
            row = conn.execute(
                "SELECT * FROM dictionaries WHERE id = ?", (dictionary_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("dictionary not found")
            if not row["active"]:
                conn.execute("UPDATE dictionaries SET active = 0 WHERE active = 1")
                conn.execute(
                    "UPDATE dictionaries SET active = 1 WHERE id = ?", (dictionary_id,)
                )
                # Every segment that is not using the new dictionary becomes a
                # recompression candidate.  Running jobs are re-queued, and the
                # worker notices that its in-flight target is no longer active.
                conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'pending',
                           recompress_token = NULL,
                           recompress_attempts = 0,
                           recompress_next_at = 0,
                           recompress_error = NULL
                     WHERE dictionary_id IS NULL OR dictionary_id != ?
                    """,
                    (dictionary_id,),
                )
                log.info("activated dictionary %s", dictionary_id)
        self.notify_worker()
        return self.get_dictionary(dictionary_id)

    def delete_dictionary(self, dictionary_id: str) -> None:
        with self.db.write_with_retry() as conn:
            row = conn.execute(
                "SELECT * FROM dictionaries WHERE id = ?", (dictionary_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("dictionary not found")
            if row["active"]:
                raise ConflictError(
                    "the active dictionary cannot be deleted; activate another one first",
                    code="dictionary_active",
                )
            dependent = conn.execute(
                "SELECT COUNT(*) AS n FROM segments WHERE dictionary_id = ?",
                (dictionary_id,),
            ).fetchone()["n"]
            if dependent:
                raise ConflictError(
                    "segments still depend on this dictionary; wait for recompression",
                    code="dictionary_in_use",
                    details={"segment_count": dependent},
                )
            conn.execute("DELETE FROM dictionaries WHERE id = ?", (dictionary_id,))
        storage.remove_quietly(self.dict_dir / f"{dictionary_id}.dict")
        log.info("deleted dictionary %s", dictionary_id)

    # ---------------------------------------------------------------- ingestion

    def ingest_file(
        self,
        source_path: os.PathLike[str] | str,
        filename: Optional[str],
        *,
        expected_sha256: Optional[str] = None,
    ) -> dict[str, Any]:
        source = Path(source_path)
        with open(source, "rb") as handle:
            sha, size = storage.hash_stream(handle)
        if expected_sha256 and sha.lower() != expected_sha256.lower():
            raise ValidationError("uploaded content does not match X-Content-SHA256")
        if size > self.config.max_segment_bytes:
            raise ValidationError("segment is larger than the configured limit")

        name = sanitize_filename(filename)
        last_reason = "active dictionary kept changing"
        for _ in range(8):
            with self.db.read() as conn:
                active = self._active_dictionary(conn)
            dictionary_id = active["id"] if active else None
            dictionary = (
                self._load_dictionary_bytes(active["id"], active["sha256"])
                if active
                else None
            )

            blob_tmp = self._new_temp("segment")
            installed: Optional[Path] = None
            try:
                comp_sha, comp_size = storage.compress_file(
                    source, blob_tmp, dictionary
                )
                if self.config.verify_writes:
                    self._verify_compressed(blob_tmp, dictionary, sha, size)

                blob_name = f"{uuid.uuid4().hex}.deflate"
                final_blob = self.blob_dir / blob_name
                storage.atomic_install(blob_tmp, final_blob)
                installed = final_blob

                with self.db.write_with_retry() as conn:
                    current = self._active_dictionary(conn)
                    current_id = current["id"] if current else None
                    if current_id != dictionary_id:
                        # The dictionary was activated while this upload was in
                        # flight.  The segment must use the new version, so the
                        # upload is compressed again before it is admitted.
                        last_reason = "dictionary changed during upload; retrying"
                        continue
                    cur = conn.execute(
                        """
                        INSERT INTO segments (
                            filename, original_sha256, original_size, blob_name,
                            compressed_sha256, compressed_size, dictionary_id,
                            created_at, recompress_state, recompress_attempts,
                            recompress_next_at, recompress_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, NULL)
                        """,
                        (
                            name,
                            sha,
                            size,
                            blob_name,
                            comp_sha,
                            comp_size,
                            dictionary_id,
                            utc_now(),
                        ),
                    )
                    number = cur.lastrowid
                installed = None
                result = self.get_segment(number)
                self.notify_worker()
                return result
            finally:
                storage.remove_quietly(blob_tmp)
                if installed:
                    storage.remove_quietly(installed)
        raise ConflictError(
            "could not commit the segment because the active dictionary kept changing",
            code="dictionary_churn",
            details={"reason": last_reason},
        )

    # ------------------------------------------------------------------ reading

    def _segment_query(self) -> str:
        return """
            SELECT s.*, d.name AS dictionary_name, d.sha256 AS dictionary_sha256,
                   d.active AS dictionary_active
              FROM segments s
              LEFT JOIN dictionaries d ON d.id = s.dictionary_id
        """

    def get_segment(self, number: int) -> dict[str, Any]:
        row = self.db.query_one(self._segment_query() + " WHERE s.number = ?", (number,))
        if not row:
            raise NotFoundError("segment not found")
        return self._segment_row(row)

    def list_segments(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        limit = max(1, min(int(limit), SEGMENT_LIMIT_MAX))
        offset = max(0, int(offset))
        total = self.db.query_one("SELECT COUNT(*) AS n FROM segments")["n"]
        rows = self.db.query_all(
            self._segment_query() + " ORDER BY s.number DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return {
            "items": [self._segment_row(row) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def _segment_row(row: sqlite3.Row) -> dict[str, Any]:
        # No recompression state means "nothing queued", i.e. the segment is
        # stored exactly as the archive wants it right now.
        state = row["recompress_state"] or "current"
        return {
            "number": row["number"],
            "filename": row["filename"],
            "original_sha256": row["original_sha256"],
            "original_size": row["original_size"],
            "compressed_size": row["compressed_size"],
            "dictionary_id": row["dictionary_id"],
            "dictionary_name": row["dictionary_name"],
            "dictionary_sha256": row["dictionary_sha256"],
            "created_at": row["created_at"],
            "recompress_state": state,
            "recompress_attempts": row["recompress_attempts"],
            "recompress_error": row["recompress_error"],
        }

    def spool_segment(self, number: int) -> tuple[dict[str, Any], Path]:
        """Materialise a verified copy of a segment's original bytes.

        The metadata is read, then the blob is opened.  A concurrent
        recompression may swap and remove the old blob between those two
        steps; in that case the lookup is retried against fresh metadata.  On
        POSIX an already-open descriptor remains readable even if the file is
        unlinked, so downloads are never blocked by recompression.
        """
        last_error: Optional[Exception] = None
        for attempt in range(40):
            row = self.db.query_one(self._segment_query() + " WHERE s.number = ?", (number,))
            if not row:
                raise NotFoundError("segment not found")
            dictionary: Optional[bytes] = None
            if row["dictionary_id"]:
                try:
                    dictionary = self._load_dictionary_bytes(
                        row["dictionary_id"], row["dictionary_sha256"]
                    )
                except StorageError as exc:
                    last_error = exc
                    time.sleep(0.01)
                    continue
            blob = self.blob_dir / row["blob_name"]
            try:
                handle = open(blob, "rb")
            except FileNotFoundError as exc:
                last_error = exc
                time.sleep(0.01)
                continue
            try:
                target = self._new_temp("download")
                try:
                    sha, size, _ = storage.decompress_stream(
                        handle, target, dictionary
                    )
                except Exception:
                    storage.remove_quietly(target)
                    raise
                if sha != row["original_sha256"] or size != row["original_size"]:
                    storage.remove_quietly(target)
                    raise StorageError(
                        f"segment {number} failed integrity verification"
                    )
                return self._segment_row(row), target
            finally:
                handle.close()
        raise StorageError(
            f"could not open segment {number} for download"
            + (f": {last_error}" if last_error else "")
        )

    # ------------------------------------------------------------------- status

    def status(self) -> dict[str, Any]:
        with self.db.read() as conn:
            active = self._active_dictionary(conn)
            counts = {
                row["state"]: row["n"]
                for row in conn.execute(
                    """
                    SELECT COALESCE(recompress_state, 'current') AS state,
                           COUNT(*) AS n
                      FROM segments GROUP BY COALESCE(recompress_state, 'current')
                    """
                )
            }
            totals = conn.execute(
                """
                SELECT COUNT(*) AS segments,
                       COALESCE(SUM(original_size), 0) AS original_bytes,
                       COALESCE(SUM(compressed_size), 0) AS stored_bytes
                  FROM segments
                """
            ).fetchone()
            if active:
                backlog = conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM segments
                     WHERE (dictionary_id IS NULL OR dictionary_id != ?)
                       AND (recompress_state IS NULL OR recompress_state != 'failed')
                    """,
                    (active["id"],),
                ).fetchone()["n"]
            else:
                backlog = totals["segments"]
            deprecation = conn.execute(
                """
                SELECT s.number, s.recompress_error, s.created_at
                  FROM segments s
                 WHERE s.recompress_error IS NOT NULL
                 ORDER BY s.number DESC LIMIT 1
                """
            ).fetchone()
            dict_count = conn.execute(
                "SELECT COUNT(*) AS n FROM dictionaries"
            ).fetchone()["n"]
        return {
            "active_dictionary": self._dict_row(active) if active else None,
            "dictionaries": dict_count,
            "segments": totals["segments"],
            "original_bytes": totals["original_bytes"],
            "stored_bytes": totals["stored_bytes"],
            "backlog": backlog,
            "states": {
                "pending": counts.get("pending", 0),
                "running": counts.get("running", 0),
                "failed": counts.get("failed", 0),
                "current": counts.get("current", 0),
            },
            "worker": {
                "enabled": self.config.worker_enabled,
                "state": self._worker_state,
                "current_segment": self._current_segment,
            },
            "last_recompression_error": (
                {
                    "segment": deprecation["number"],
                    "error": deprecation["recompress_error"],
                }
                if deprecation
                else None
            ),
        }

    # --------------------------------------------------------------- operations

    def retry_recompression(self, number: Optional[int] = None) -> dict[str, Any]:
        with self.db.write_with_retry() as conn:
            if number is not None:
                row = conn.execute(
                    "SELECT number FROM segments WHERE number = ?", (number,)
                ).fetchone()
                if not row:
                    raise NotFoundError("segment not found")
                cur = conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'pending', recompress_token = NULL,
                           recompress_attempts = 0, recompress_next_at = 0,
                           recompress_error = NULL
                     WHERE number = ? AND recompress_state != 'running'
                    """,
                    (number,),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE segments
                       SET recompress_state = 'pending', recompress_token = NULL,
                           recompress_attempts = 0, recompress_next_at = 0,
                           recompress_error = NULL
                     WHERE recompress_state IN ('pending', 'failed')
                    """
                )
        self.notify_worker()
        return {"requeued": cur.rowcount}

    def run_recompression_once(self, limit: int = 1) -> int:
        """Synchronous conversion helper used by tests and manual recovery."""
        processed = 0
        for _ in range(max(1, limit)):
            # Same stale-row recovery as the background loop, so a manual run
            # also resumes work that an interrupted run left in 'running'.
            if not self._reap_stale_running():
                break
            batch = self._claim_batch()
            if not batch:
                break
            try:
                self._convert_claimed(batch[0])
            except sqlite3.OperationalError as exc:
                if is_locked_error(exc):
                    # The background loop owns recovery when the database is
                    # locked; a synchronous caller just reports nothing done.
                    self._requeue_claimed(batch)
                    break
                raise
            if len(batch) > 1:
                self._requeue_claimed(batch[1:])
            processed += 1
        return processed

    def wait_for_recompression(self, timeout: float = 30.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.status()
            if status["backlog"] == 0 and status["states"]["running"] == 0:
                return True
            time.sleep(0.05)
        return False
