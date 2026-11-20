"""End-to-end HTTP tests against the real threaded server."""

from __future__ import annotations

import http.client
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from logarchive.app import ArchiveServer
from logarchive.archive import ArchiveService
from logarchive.config import Config

DICT_V1 = b"timestamp= severity=INFO service= user_id= request_id= endpoint=" * 4
DICT_V2 = b"ts= level=INFO svc= uid= rid= path= status= latency_ms= msg=" * 4
SAMPLE = b"timestamp=2026-11-20T00:00:00Z severity=INFO service=api user_id=7\n" * 500


def multipart_body(fields: dict[str, bytes], files: dict[str, tuple[str, bytes]]) -> tuple[bytes, str]:
    boundary = f"----test{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            + value
            + b"\r\n"
        )
    for name, (filename, data) in files.items():
        chunks.append(
            (
                f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; '
                f'filename="{filename}"\r\nContent-Type: application/octet-stream\r\n\r\n'
            ).encode()
            + data
            + b"\r\n"
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


class HttpTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="logarchive-http-"))
        self.service = ArchiveService(
            Config(data_dir=self.tmp, worker_enabled=False, recompress_delay_ms=0)
        )
        self.server = ArchiveServer(("127.0.0.1", 0), self.service)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        self.service.stop_worker()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            payload = response.read()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, payload
        finally:
            conn.close()

    def get_json(self, path: str) -> tuple[int, dict]:
        status, _, body = self.request("GET", path)
        return status, json.loads(body)

    def upload_dictionary(self, name: str, data: bytes, activate: bool = False):
        fields = {"name": name.encode()}
        if activate:
            fields["activate"] = b"1"
        body, content_type = multipart_body(fields, {"dictionary": ("dict.bin", data)})
        return self.request(
            "POST",
            "/api/dictionaries",
            body=body,
            headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        )

    def upload_segment(self, filename: str, data: bytes, sha: str | None = None):
        body, content_type = multipart_body({}, {"file": (filename, data)})
        headers = {"Content-Type": content_type, "Content-Length": str(len(body))}
        if sha:
            headers["X-Content-SHA256"] = sha
        return self.request("POST", "/api/segments", body=body, headers=headers)

    def activate(self, dictionary_id: str) -> None:
        status, _, _ = self.request("POST", f"/api/dictionaries/{dictionary_id}/activate")
        self.assertEqual(status, 200)


class HttpApiTests(HttpTestCase):
    def test_index_and_static_assets_are_served(self) -> None:
        status, headers, body = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"<!DOCTYPE html>", body)
        self.assertTrue(headers["content-type"].startswith("text/html"))
        status, _, js = self.request("GET", "/static/app.js")
        self.assertEqual(status, 200)
        self.assertIn(b"uploadSegments", js)

    def test_static_path_traversal_is_blocked(self) -> None:
        status, _, _ = self.request("GET", "/static/../app.py")
        self.assertEqual(status, 404)

    def test_full_dictionary_and_segment_flow_over_http(self) -> None:
        status, headers, body = self.upload_dictionary("v1", DICT_V1)
        self.assertEqual(status, 201, body)
        v1 = json.loads(body)
        self.assertFalse(v1["active"])
        self.activate(v1["id"])

        status, headers, body = self.upload_segment("2026-11-20.log", SAMPLE)
        self.assertEqual(status, 201, body)
        first = json.loads(body)
        self.assertEqual(first["dictionary_id"], v1["id"])
        self.assertEqual(headers["location"], f"/api/segments/{first['number']}")

        status, _, body = self.upload_dictionary("v2", DICT_V2, activate=True)
        self.assertEqual(status, 201)
        v2 = json.loads(body)
        self.assertTrue(v2["active"])

        # A new segment is admitted under v2 right away.
        status, _, body = self.upload_segment("after.log", SAMPLE + b"x\n")
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body)["dictionary_id"], v2["id"])

        # The old segment is still downloadable before conversion finishes.
        status, headers, body = self.request(
            "GET", f"/api/segments/{first['number']}/download"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, SAMPLE)
        self.assertIn("attachment", headers["content-disposition"])

        # Deletion is refused while the old dictionary is still referenced.
        status, _, body = self.request("DELETE", f"/api/dictionaries/{v1['id']}")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "dictionary_in_use")

        self.assertEqual(self.service.run_recompression_once(limit=10), 1)

        # After conversion the old dictionary can go, and bytes stay identical.
        status, _, _ = self.request("DELETE", f"/api/dictionaries/{v1['id']}")
        self.assertEqual(status, 204)
        status, _, body = self.request(
            "GET", f"/api/segments/{first['number']}/download"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, SAMPLE)

        status, payload = self.get_json("/api/status")
        self.assertEqual(status, 200)
        self.assertEqual(payload["active_dictionary"]["id"], v2["id"])
        self.assertEqual(payload["backlog"], 0)

    def test_upload_with_checksum_header(self) -> None:
        import hashlib

        digest = hashlib.sha256(SAMPLE).hexdigest()
        status, _, body = self.upload_segment("ok.log", SAMPLE, sha=digest)
        self.assertEqual(status, 201, body)
        status, _, body = self.upload_segment("bad.log", SAMPLE, sha="0" * 64)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "invalid_input")

    def test_invalid_dictionary_is_rejected(self) -> None:
        status, _, body = self.upload_dictionary("too-big", b"x" * 40_000)
        self.assertEqual(status, 400, body)
        status, _, body = self.upload_dictionary("empty", b"")
        self.assertEqual(status, 400)

    def test_missing_segment_returns_404(self) -> None:
        status, _, body = self.request("GET", "/api/segments/999/download")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["code"], "not_found")

    def test_active_dictionary_delete_conflict(self) -> None:
        status, _, body = self.upload_dictionary("v1", DICT_V1, activate=True)
        self.assertEqual(status, 201)
        dictionary = json.loads(body)
        status, _, body = self.request("DELETE", f"/api/dictionaries/{dictionary['id']}")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "dictionary_active")

    def test_unknown_route_and_method(self) -> None:
        status, _, _ = self.request("GET", "/api/nope")
        self.assertEqual(status, 404)
        status, _, body = self.request("PUT", "/api/segments")
        self.assertEqual(status, 405)
        self.assertEqual(json.loads(body)["error"]["code"], "method_not_allowed")

    def test_segment_listing_paginates(self) -> None:
        for i in range(5):
            status, _, body = self.upload_segment(f"s{i}.log", SAMPLE + bytes([i]))
            self.assertEqual(status, 201, body)
        status, payload = self.get_json("/api/segments?limit=2&offset=0")
        self.assertEqual(status, 200)
        self.assertEqual(payload["total"], 5)
        self.assertEqual([s["number"] for s in payload["items"]], [5, 4])
        status, payload = self.get_json("/api/segments?limit=2&offset=4")
        self.assertEqual([s["number"] for s in payload["items"]], [1])

    def test_body_larger_than_limit_is_rejected(self) -> None:
        self.service.config = Config(
            data_dir=self.tmp,
            worker_enabled=False,
            max_segment_bytes=1024,
        )
        body, content_type = multipart_body({}, {"file": ("big.log", b"y" * 5000)})
        status, _, payload = self.request(
            "POST",
            "/api/segments",
            body=body,
            headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        )
        # The part exceeds MAX_SEGMENT_BYTES even though the envelope is legal.
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(payload)["error"]["code"], "invalid_multipart")
        self.assertEqual(self.service.list_segments()["total"], 0)

    def test_envelope_larger_than_limit_is_rejected_before_reading(self) -> None:
        self.service.config = Config(
            data_dir=self.tmp, worker_enabled=False, max_segment_bytes=1024
        )
        # Announce a huge body but send only the headers: the server must
        # refuse from Content-Length alone instead of reading 2 MB.
        import socket

        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        try:
            sock.sendall(
                b"POST /api/segments HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Type: multipart/form-data; boundary=zzz\r\n"
                b"Content-Length: 2000000\r\n\r\n"
            )
            raw = b""
            sock.settimeout(5)
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                raw += chunk
        finally:
            sock.close()
        self.assertTrue(raw.startswith(b"HTTP/1.1 413"), raw[:80])
        self.assertIn(b"payload_too_large", raw)
        self.assertEqual(self.service.list_segments()["total"], 0)

    def test_head_download_has_headers_but_no_body(self) -> None:
        status, _, body = self.upload_segment("h.log", SAMPLE)
        number = json.loads(body)["number"]
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            conn.request("HEAD", f"/api/segments/{number}/download")
            response = conn.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(int(response.getheader("Content-Length")), len(SAMPLE))
            self.assertEqual(response.read(), b"")
        finally:
            conn.close()


class RangeDownloadTests(HttpTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.payload = bytes(range(256)) * 40
        status, _, body = self.upload_segment("range.log", self.payload)
        self.assertEqual(status, 201)
        self.number = json.loads(body)["number"]

    def range_request(self, header: str):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            conn.request("GET", f"/api/segments/{self.number}/download",
                         headers={"Range": header})
            response = conn.getresponse()
            headers = {k.lower(): v for k, v in response.getheaders()}
            return response.status, headers, response.read()
        finally:
            conn.close()

    def test_full_body_without_range(self) -> None:
        status, headers, body = self.request(
            "GET", f"/api/segments/{self.number}/download"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, self.payload)
        self.assertEqual(headers["accept-ranges"], "bytes")

    def test_partial_ranges(self) -> None:
        status, headers, body = self.range_request("bytes=10-19")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.payload[10:20])
        self.assertEqual(headers["content-range"], f"bytes 10-19/{len(self.payload)}")

        status, headers, body = self.range_request("bytes=-16")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.payload[-16:])

        status, headers, body = self.range_request("bytes=1000-")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.payload[1000:])

    def test_unsatisfiable_range(self) -> None:
        status, headers, body = self.range_request("bytes=99999-")
        self.assertEqual(status, 416)
        self.assertEqual(headers["content-range"], f"bytes */{len(self.payload)}")
        self.assertEqual(body, b"")

    def test_range_bytes_are_stable_after_recompression(self) -> None:
        _, _, body = self.upload_dictionary("v1", DICT_V1, activate=True)
        v1 = json.loads(body)
        _, _, body = self.upload_dictionary("v2", DICT_V2, activate=True)
        v2 = json.loads(body)
        self.assertEqual(self.service.run_recompression_once(limit=10), 1)
        self.assertEqual(
            self.service.get_segment(self.number)["dictionary_id"], v2["id"]
        )
        status, _, body = self.range_request("bytes=7-42")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.payload[7:43])


class WorkerIntegrationTests(HttpTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Enable the background worker on the already-running service: all
        # other settings stay as they were.
        self.service.config = Config(
            data_dir=self.tmp,
            worker_enabled=True,
            recompress_delay_ms=25,
            recompress_batch_size=2,
        )
        self.service.start_worker()

    def test_background_worker_drains_backlog(self) -> None:
        _, _, body = self.upload_dictionary("v1", DICT_V1, activate=True)
        v1 = json.loads(body)
        payloads = {}
        for i in range(5):
            data = SAMPLE + f"line-{i}\n".encode()
            status, _, body = self.upload_segment(f"s{i}.log", data)
            self.assertEqual(status, 201)
            payloads[json.loads(body)["number"]] = data

        _, _, body = self.upload_dictionary("v2", DICT_V2, activate=True)
        v2 = json.loads(body)

        deadline = time.time() + 30
        while time.time() < deadline:
            _, status = self.get_json("/api/status")
            if status["backlog"] == 0 and status["states"]["running"] == 0:
                break
            time.sleep(0.05)
        else:
            self.fail("worker did not drain the backlog")

        for number, data in payloads.items():
            row = self.service.get_segment(number)
            self.assertEqual(row["dictionary_id"], v2["id"])
            status, _, body = self.request("GET", f"/api/segments/{number}/download")
            self.assertEqual(status, 200)
            self.assertEqual(body, data)

    def test_worker_survives_and_resumes_after_restart(self) -> None:
        _, _, body = self.upload_dictionary("v1", DICT_V1, activate=True)
        v1 = json.loads(body)
        status, _, body = self.upload_segment("s.log", SAMPLE)
        number = json.loads(body)["number"]
        self.service.stop_worker()

        # Re-open the same data directory as a fresh process would.
        restarted = ArchiveService(
            Config(data_dir=self.tmp, worker_enabled=True, recompress_delay_ms=25)
        )
        try:
            restarted.start_worker()
            _, _, body = restarted_db_upload(restarted, "v2", DICT_V2)
            v2 = json.loads(body)
            restarted.activate_dictionary(v2["id"])
            self.assertTrue(restarted.wait_for_recompression(30))
            row = restarted.get_segment(number)
            self.assertEqual(row["dictionary_id"], v2["id"])
            _, path = restarted.spool_segment(number)
            try:
                self.assertEqual(path.read_bytes(), SAMPLE)
            finally:
                os.unlink(path)
        finally:
            restarted.stop_worker()


def restarted_db_upload(service: ArchiveService, name: str, data: bytes):
    dictionary = service.create_dictionary(name, data)
    return 201, {}, json.dumps(dictionary).encode()


if __name__ == "__main__":
    unittest.main()
