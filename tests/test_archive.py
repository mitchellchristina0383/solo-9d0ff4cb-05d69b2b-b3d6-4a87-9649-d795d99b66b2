"""Service-level tests for dictionary versioning and segment recompression."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from logarchive import storage
from logarchive.archive import ArchiveService, ConflictError, ValidationError
from logarchive.config import Config

DICT_V1 = (b"timestamp= severity=INFO service=api-gateway user_id= request_id= "
           b"endpoint= method=GET status= duration_ms= message=") * 2
DICT_V2 = (b"ts= level=INFO ERROR WARN svc=gateway uid= rid= path= verb= code= "
           b"latency= msg= traceparent=") * 2

SAMPLE = b"".join(
    f"timestamp=2026-11-20T00:{i % 60:02d}:00Z severity=INFO service=api-gateway "
    f"request_id=req-{i} endpoint=/v1/items/{i % 50} status=200 user_id=u{i % 97} "
    f"message=request completed for user u{i % 97}\n".encode()
    for i in range(3000)
)


class ArchiveTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="logarchive-test-"))
        self.service = ArchiveService(
            Config(
                data_dir=self.tmp,
                worker_enabled=False,
                verify_writes=True,
                recompress_delay_ms=0,
            )
        )

    def tearDown(self) -> None:
        self.service.stop_worker()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---------------------------------------------------------------- helpers

    def write_segment(self, name: str, payload: bytes = SAMPLE) -> Path:
        path = self.tmp / name
        path.write_bytes(payload)
        return path

    def upload(self, name: str, payload: bytes = SAMPLE) -> dict:
        return self.service.ingest_file(self.write_segment(name, payload), name)

    def download(self, number: int) -> bytes:
        row, path = self.service.spool_segment(number)
        try:
            return path.read_bytes()
        finally:
            os.unlink(path)

    def make_dictionaries(self) -> tuple[dict, dict]:
        v1 = self.service.create_dictionary("v1", DICT_V1)
        v2 = self.service.create_dictionary("v2", DICT_V2)
        return v1, v2


class DictionaryLifecycleTests(ArchiveTestCase):
    def test_dictionary_is_stored_immutably_and_validated(self) -> None:
        v1 = self.service.create_dictionary("v1", DICT_V1)
        self.assertFalse(v1["active"])
        self.assertEqual(v1["size"], len(DICT_V1))
        self.assertEqual(v1["sha256"], hashlib.sha256(DICT_V1).hexdigest())
        self.assertEqual(
            (self.service.dict_dir / f"{v1['id']}.dict").read_bytes(), DICT_V1
        )

        with self.assertRaises(ValidationError):
            self.service.create_dictionary("empty", b"")
        with self.assertRaises(ValidationError):
            self.service.create_dictionary("huge", b"x" * 32_769)

    def test_activation_switches_new_uploads_and_queues_old_segments(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        first = self.upload("first.log")
        second = self.upload("second.log")
        self.assertEqual(first["dictionary_id"], v1["id"])

        self.service.activate_dictionary(v2["id"])
        status = self.service.status()
        self.assertEqual(status["active_dictionary"]["id"], v2["id"])
        self.assertEqual(status["backlog"], 2)
        self.assertEqual(status["states"]["pending"], 2)

        later = self.upload("later.log")
        self.assertEqual(later["dictionary_id"], v2["id"])
        self.assertEqual(later["recompress_state"], "current")

        converted = self.service.run_recompression_once(limit=10)
        self.assertEqual(converted, 2)
        for number in (first["number"], second["number"]):
            row = self.service.get_segment(number)
            self.assertEqual(row["dictionary_id"], v2["id"])
            self.assertEqual(row["recompress_state"], "current")

    def test_only_one_active_dictionary(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        self.service.activate_dictionary(v2["id"])
        active = [d for d in self.service.list_dictionaries() if d["active"]]
        self.assertEqual([d["id"] for d in active], [v2["id"]])

    def test_dictionary_file_is_content_verified_before_use(self) -> None:
        v1, _ = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("first.log")
        (self.service.dict_dir / f"{v1['id']}.dict").write_bytes(b"tampered")
        with self.assertRaises(storage.StorageError):
            self.service.spool_segment(segment["number"])

    def test_delete_is_refused_while_segments_depend(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        self.upload("first.log")
        self.service.activate_dictionary(v2["id"])

        # v1 is no longer the active dictionary, but a segment still depends
        # on it, so deletion is refused with the dependency count.
        with self.assertRaises(ConflictError) as ctx:
            self.service.delete_dictionary(v1["id"])
        self.assertEqual(ctx.exception.code, "dictionary_in_use")
        self.assertEqual(ctx.exception.details["segment_count"], 1)
        self.assertTrue((self.service.dict_dir / f"{v1['id']}.dict").exists())

        self.service.run_recompression_once(limit=5)
        self.service.delete_dictionary(v1["id"])
        self.assertEqual(len(self.service.list_dictionaries()), 1)

    def test_active_dictionary_cannot_be_deleted(self) -> None:
        v1, _ = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        with self.assertRaises(ConflictError) as ctx:
            self.service.delete_dictionary(v1["id"])
        self.assertEqual(ctx.exception.code, "dictionary_active")

    def test_delete_after_recompression_keeps_bytes_downloadable(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("first.log", SAMPLE)
        self.service.activate_dictionary(v2["id"])
        self.service.run_recompression_once(limit=10)

        self.service.delete_dictionary(v1["id"])
        self.assertFalse((self.service.dict_dir / f"{v1['id']}.dict").exists())
        self.assertEqual(self.download(segment["number"]), SAMPLE)

    def test_missing_dictionary_file_is_reported_not_silently_ignored(self) -> None:
        v1, _ = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("first.log")
        (self.service.dict_dir / f"{v1['id']}.dict").unlink()
        with self.assertRaises(storage.StorageError):
            self.service.spool_segment(segment["number"])


class IngestionConsistencyTests(ArchiveTestCase):
    def test_upload_started_before_activation_is_stored_with_new_dictionary(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        original_compress = storage.compress_file
        fired = {"done": False}

        def compress_then_activate(*args, **kwargs):
            result = original_compress(*args, **kwargs)
            if not fired["done"]:
                fired["done"] = True
                self.service.activate_dictionary(v2["id"])
            return result

        with mock.patch("logarchive.archive.storage.compress_file", compress_then_activate):
            segment = self.upload("during-activation.log")

        self.assertTrue(fired["done"], "activation was never triggered")
        self.assertEqual(segment["dictionary_id"], v2["id"])
        self.assertEqual(segment["recompress_state"], "current")
        self.assertEqual(self.download(segment["number"]), SAMPLE)
        self.assertEqual(self.service.status()["backlog"], 0)

    def test_upload_produces_identical_bytes_across_dictionary_changes(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        first = self.upload("a.log", SAMPLE)
        self.service.activate_dictionary(v2["id"])
        second = self.upload("b.log", SAMPLE)
        self.service.run_recompression_once(limit=5)

        payloads = {
            self.download(first["number"]),
            self.download(second["number"]),
        }
        self.assertEqual(payloads, {SAMPLE})

    def test_content_sha_header_mismatch_is_rejected(self) -> None:
        segment_file = self.write_segment("bad.log")
        with self.assertRaises(ValidationError):
            self.service.ingest_file(
                segment_file, "bad.log", expected_sha256="0" * 64
            )
        self.assertEqual(self.service.list_segments()["total"], 0)

    def test_filename_is_sanitised(self) -> None:
        segment = self.service.ingest_file(
            self.write_segment("raw.log"), "../../etc/passwd"
        )
        self.assertEqual(segment["filename"], "passwd")


class RecompressionResilienceTests(ArchiveTestCase):
    def test_failed_conversion_keeps_original_segment_downloadable(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("first.log")
        self.service.activate_dictionary(v2["id"])

        with mock.patch(
            "logarchive.archive.storage.compress_file",
            side_effect=storage.StorageError("simulated disk error"),
        ):
            self.service.run_recompression_once(limit=1)

        row = self.service.get_segment(segment["number"])
        self.assertEqual(row["dictionary_id"], v1["id"])
        self.assertEqual(row["recompress_state"], "pending")
        self.assertIn("simulated disk error", row["recompress_error"])
        self.assertEqual(self.download(segment["number"]), SAMPLE)

        # Clear the retry backoff so the next attempt happens immediately.
        with self.service.db.write() as conn:
            conn.execute(
                "UPDATE segments SET recompress_next_at = 0 WHERE number = ?",
                (segment["number"],),
            )
        self.service.run_recompression_once(limit=1)
        row = self.service.get_segment(segment["number"])
        self.assertEqual(row["dictionary_id"], v2["id"])
        self.assertEqual(row["recompress_state"], "current")

    def test_retries_exhaust_into_failed_state_and_can_be_requeued(self) -> None:
        self.service.stop_worker()
        service = ArchiveService(
            Config(
                data_dir=self.tmp,
                worker_enabled=False,
                max_recompress_attempts=2,
                recompress_delay_ms=0,
            )
        )
        try:
            v1 = service.create_dictionary("v1", DICT_V1)
            v2 = service.create_dictionary("v2", DICT_V2)
            service.activate_dictionary(v1["id"])
            segment = service.ingest_file(self.write_segment("x.log"), "x.log")
            service.activate_dictionary(v2["id"])

            with mock.patch(
                "logarchive.archive.storage.compress_file",
                side_effect=storage.StorageError("persistent failure"),
            ):
                for _ in range(3):
                    with service.db.write() as conn:
                        conn.execute(
                            "UPDATE segments SET recompress_next_at = 0 WHERE number = ?",
                            (segment["number"],),
                        )
                    service.run_recompression_once(limit=1)

            row = service.get_segment(segment["number"])
            self.assertEqual(row["recompress_state"], "failed")
            row, path = service.spool_segment(segment["number"])
            try:
                self.assertEqual(path.read_bytes(), SAMPLE)
            finally:
                os.unlink(path)

            service.retry_recompression(segment["number"])
            service.run_recompression_once(limit=1)
            row = service.get_segment(segment["number"])
            self.assertEqual(row["recompress_state"], "current")
            self.assertEqual(row["dictionary_id"], v2["id"])
        finally:
            service.stop_worker()

    def test_restart_resumes_interrupted_conversion(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        first = self.upload("first.log")
        second = self.upload("second.log")
        self.service.activate_dictionary(v2["id"])

        # Simulate a crash: the worker claimed segment #1 and died mid-way.
        with self.service.db.write() as conn:
            conn.execute(
                """
                UPDATE segments SET recompress_state = 'running',
                                    recompress_token = 'dead-token'
                 WHERE number = ?
                """,
                (first["number"],),
            )
            conn.execute(
                """
                UPDATE segments SET recompress_state = 'failed',
                                    recompress_attempts = 9
                 WHERE number = ?
                """,
                (second["number"],),
            )

        restarted = ArchiveService(
            Config(data_dir=self.tmp, worker_enabled=False, recompress_delay_ms=0)
        )
        try:
            status = restarted.status()
            self.assertEqual(status["backlog"], 2)
            self.assertEqual(status["states"]["running"], 0)
            self.assertEqual(status["states"]["failed"], 0)
            restored = restarted.run_recompression_once(limit=10)
            self.assertEqual(restored, 2)
            for number in (first["number"], second["number"]):
                row = restarted.get_segment(number)
                self.assertEqual(row["dictionary_id"], v2["id"])
                self.assertEqual(row["recompress_state"], "current")
            row, path = restarted.spool_segment(first["number"])
            try:
                self.assertEqual(path.read_bytes(), SAMPLE)
            finally:
                os.unlink(path)
        finally:
            restarted.stop_worker()

    def test_dictionary_change_mid_conversion_discards_stale_result(self) -> None:
        v1, v2 = self.make_dictionaries()
        third = self.service.create_dictionary("v3", DICT_V2 + b"extra")
        self.service.activate_dictionary(v1["id"])
        segment = self.upload("first.log")
        self.service.activate_dictionary(v2["id"])

        original_decompress = storage.decompress_file
        switched = {"done": False}

        def decompress_then_switch(*args, **kwargs):
            result = original_decompress(*args, **kwargs)
            if not switched["done"]:
                switched["done"] = True
                self.service.activate_dictionary(third["id"])
            return result

        with mock.patch(
            "logarchive.archive.storage.decompress_file", decompress_then_switch
        ):
            self.service.run_recompression_once(limit=1)

        row = self.service.get_segment(segment["number"])
        self.assertEqual(row["dictionary_id"], v1["id"])
        self.assertEqual(row["recompress_state"], "pending")

        self.service.run_recompression_once(limit=5)
        row = self.service.get_segment(segment["number"])
        self.assertEqual(row["dictionary_id"], third["id"])
        self.assertEqual(row["recompress_state"], "current")
        self.assertEqual(self.download(segment["number"]), SAMPLE)
        # No stale blob was left behind by the abandoned conversion.
        self.assertEqual(
            len(list(self.service.blob_dir.iterdir())),
            self.service.list_segments()["total"],
        )


class ConcurrencyTests(ArchiveTestCase):
    def test_download_and_ingest_continue_while_conversion_is_running(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        existing = self.upload("existing.log")
        self.service.activate_dictionary(v2["id"])

        started = threading.Event()
        release = threading.Event()
        original_compress = storage.compress_file

        def slow_compress(*args, **kwargs):
            started.set()
            release.wait(10)
            return original_compress(*args, **kwargs)

        with mock.patch("logarchive.archive.storage.compress_file", slow_compress):
            worker = threading.Thread(
                target=lambda: self.service.run_recompression_once(limit=1)
            )
            worker.start()
            self.assertTrue(started.wait(5), "conversion never started")
            try:
                # A download of the segment being converted still works.
                self.assertEqual(self.download(existing["number"]), SAMPLE)
                # And a new upload is admitted with the new dictionary.
                fresh = self.upload("fresh.log", SAMPLE + b"fresh\n")
                self.assertEqual(fresh["dictionary_id"], v2["id"])
                self.assertEqual(fresh["recompress_state"], "current")
            finally:
                release.set()
                worker.join(15)
        self.assertFalse(worker.is_alive())

        row = self.service.get_segment(existing["number"])
        self.assertEqual(row["dictionary_id"], v2["id"])
        self.assertEqual(self.download(existing["number"]), SAMPLE)

    def test_concurrent_uploads_all_land_with_a_single_dictionary(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        payloads = [SAMPLE + f"variant-{i}\n".encode() for i in range(8)]
        results: list[dict] = []
        errors: list[Exception] = []

        def upload(index: int) -> None:
            try:
                results.append(
                    self.service.ingest_file(
                        self.write_segment(f"c{index}.log", payloads[index]),
                        f"c{index}.log",
                    )
                )
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=upload, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        time.sleep(0.01)
        self.service.activate_dictionary(v2["id"])
        for thread in threads:
            thread.join(20)

        self.assertFalse(errors)
        self.assertEqual(len(results), 8)
        expected = {f"c{i}.log": payloads[i] for i in range(8)}
        for row in results:
            # Each upload commits under whichever dictionary was active at the
            # moment it was admitted; anything older is queued for conversion.
            self.assertIn(row["dictionary_id"], {v1["id"], v2["id"]})
            self.assertEqual(self.download(row["number"]), expected[row["filename"]])

        for _ in range(5):
            self.service.run_recompression_once(limit=10)
        for row in results:
            final = self.service.get_segment(row["number"])
            self.assertEqual(final["dictionary_id"], v2["id"])
            self.assertEqual(final["recompress_state"], "current")
            self.assertEqual(self.download(row["number"]), expected[row["filename"]])

    def test_numbered_download_matches_upload_after_round_of_recompression(self) -> None:
        v1, v2 = self.make_dictionaries()
        self.service.activate_dictionary(v1["id"])
        payloads = {f"seg-{i}.log": SAMPLE + bytes([i]) for i in range(5)}
        numbers = {}
        for name, payload in payloads.items():
            numbers[name] = self.upload(name, payload)["number"]
        self.service.activate_dictionary(v2["id"])
        self.service.run_recompression_once(limit=10)
        for name, number in numbers.items():
            self.assertEqual(self.download(number), payloads[name])
            self.assertEqual(
                self.service.get_segment(number)["filename"], name
            )


if __name__ == "__main__":
    unittest.main()
