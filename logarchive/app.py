"""Threaded HTTP application for the log archive."""

from __future__ import annotations

import json
import logging
import mimetypes
import os
import re
import sys
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, urlparse

from . import multipart
from .archive import (
    SEGMENT_LIMIT_DEFAULT,
    ArchiveService,
    ConflictError,
    NotFoundError,
    ValidationError,
)
from .config import Config
from .storage import StorageError

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_DICTIONARY_UPLOAD = 64 * 1024  # small envelope around the 32 KiB limit

SEGMENT_PATH = re.compile(r"^/api/segments/(\d+)$")
SEGMENT_DOWNLOAD_PATH = re.compile(r"^/api/segments/(\d+)/download$")
DICTIONARY_ACTIVATE_PATH = re.compile(r"^/api/dictionaries/([0-9a-f]{32})/activate$")
DICTIONARY_PATH = re.compile(r"^/api/dictionaries/([0-9a-f]{32})$")


class HttpError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


class Handler(BaseHTTPRequestHandler):
    server_version = "LogArchive/1.0"
    protocol_version = "HTTP/1.1"
    service: ArchiveService
    # ------------------------------------------------------------- plumbing

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_error_json(
        self,
        status: int,
        code: str,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = {"error": {"code": code, "message": message}}
        if details:
            payload["error"]["details"] = details
        # The request body may not have been consumed (oversized or malformed
        # upload), so the connection cannot be reused safely.
        self.close_connection = True
        self._send_json(status, payload)

    def _send_empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_body(self, limit: int) -> bytes:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise HttpError(
                HTTPStatus.LENGTH_REQUIRED,
                "length_required",
                "Content-Length is required",
            )
        try:
            length = int(length_header)
        except ValueError:
            raise HttpError(HTTPStatus.BAD_REQUEST, "bad_request", "invalid Content-Length")
        if length < 0:
            raise HttpError(HTTPStatus.BAD_REQUEST, "bad_request", "invalid Content-Length")
        if length > limit:
            raise HttpError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "payload_too_large",
                f"request body exceeds {limit} bytes",
            )
        chunks = []
        remaining = length
        while remaining:
            chunk = self.rfile.read(min(remaining, 1024 * 1024))
            if not chunk:
                raise HttpError(
                    HTTPStatus.BAD_REQUEST, "bad_request", "request body ended early"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_multipart(
        self, *, limit: int, file_limit: int
    ) -> dict[str, multipart.Part]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise HttpError(HTTPStatus.BAD_REQUEST, "bad_request", "invalid Content-Length")
        if length <= 0:
            raise HttpError(HTTPStatus.BAD_REQUEST, "bad_request", "request body is empty")
        if length > limit:
            raise HttpError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "payload_too_large",
                f"request body exceeds {limit} bytes",
            )
        content_type = self.headers.get("Content-Type", "")
        try:
            return multipart.parse_multipart(
                multipart.LimitedReader(self.rfile, length),
                content_type,
                tmp_dir=self.service.tmp_dir,
                max_total_bytes=limit,
                max_file_bytes=file_limit,
            )
        except multipart.MultipartError as exc:
            raise HttpError(HTTPStatus.BAD_REQUEST, "invalid_multipart", str(exc))

    # ------------------------------------------------------------ HTTP verbs

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed("PUT")

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_allowed("PATCH")

    def _method_not_allowed(self, method: str) -> None:
        self.close_connection = True
        self._send_error_json(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "method_not_allowed",
            f"{method} is not supported",
        )

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if method == "GET":
                if self._serve_asset(path):
                    return
                if path == "/healthz":
                    self._send_json(HTTPStatus.OK, {"ok": True})
                    return
                if path == "/api/status":
                    self._send_json(HTTPStatus.OK, self.service.status())
                    return
                if path == "/api/dictionaries":
                    self._send_json(
                        HTTPStatus.OK, {"items": self.service.list_dictionaries()}
                    )
                    return
                if path == "/api/segments":
                    limit = self._int_param(query, "limit", SEGMENT_LIMIT_DEFAULT)
                    offset = self._int_param(query, "offset", 0)
                    self._send_json(
                        HTTPStatus.OK, self.service.list_segments(limit, offset)
                    )
                    return
                match = SEGMENT_DOWNLOAD_PATH.match(path)
                if match:
                    self._download(int(match.group(1)))
                    return
                match = SEGMENT_PATH.match(path)
                if match:
                    self._send_json(
                        HTTPStatus.OK, self.service.get_segment(int(match.group(1)))
                    )
                    return
            elif method == "POST":
                if path == "/api/dictionaries":
                    self._upload_dictionary()
                    return
                match = DICTIONARY_ACTIVATE_PATH.match(path)
                if match:
                    dictionary = self.service.activate_dictionary(match.group(1))
                    self._send_json(HTTPStatus.OK, dictionary)
                    return
                if path == "/api/segments":
                    self._upload_segment()
                    return
                if path == "/api/recompression/retry":
                    self._retry_recompression()
                    return
            elif method == "DELETE":
                match = DICTIONARY_PATH.match(path)
                if match:
                    self.service.delete_dictionary(match.group(1))
                    self._send_empty(HTTPStatus.NO_CONTENT)
                    return
            self._send_error_json(
                HTTPStatus.NOT_FOUND, "not_found", f"no route for {method} {path}"
            )
        except HttpError as exc:
            self._send_error_json(exc.status, exc.code, exc.message, exc.details)
        except NotFoundError as exc:
            self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", str(exc))
        except ValidationError as exc:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_input", str(exc))
        except ConflictError as exc:
            self._send_error_json(HTTPStatus.CONFLICT, exc.code, exc.message, exc.details)
        except (BrokenPipeError, ConnectionResetError):
            # The client went away (aborted upload or download).  Nothing to
            # report, and the connection is already unusable.
            log.info("client disconnected during %s %s", method, path)
            self.close_connection = True
        except (StorageError, OSError) as exc:
            log.error("storage failure on %s %s: %s", method, path, exc)
            try:
                self._send_error_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR, "storage_error", str(exc)
                )
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
        except Exception:
            log.error("unhandled error on %s %s\n%s", method, path, traceback.format_exc())
            try:
                self._send_error_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "unexpected server error",
                )
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True

    def _int_param(self, query: dict[str, list[str]], name: str, default: int) -> int:
        values = query.get(name)
        if not values:
            return default
        try:
            return int(values[0])
        except ValueError:
            raise HttpError(
                HTTPStatus.BAD_REQUEST, "invalid_input", f"{name} must be an integer"
            )

    # --------------------------------------------------------------- static

    def _serve_asset(self, path: str) -> bool:
        if path == "/":
            name = "index.html"
        elif path.startswith("/static/"):
            name = path[len("/static/") :]
        else:
            return False
        candidate = (STATIC_DIR / name).resolve()
        if STATIC_DIR.resolve() not in candidate.parents or not candidate.is_file():
            self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "asset not found")
            return True
        body = candidate.read_bytes()
        content_type, _ = mimetypes.guess_type(candidate.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        return True

    # ------------------------------------------------------------- handlers

    def _upload_dictionary(self) -> None:
        parts = self._read_multipart(
            limit=MAX_DICTIONARY_UPLOAD, file_limit=MAX_DICTIONARY_UPLOAD
        )
        try:
            payload = parts.get("dictionary") or parts.get("file")
            if not payload or (payload.path is None and payload.text is None):
                raise HttpError(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_input",
                    "a dictionary file field is required",
                )
            if payload.path is not None:
                data = payload.path.read_bytes()
            else:
                data = payload.text or b""
            name_part = parts.get("name")
            raw_name = ""
            if name_part and name_part.text:
                raw_name = name_part.text.decode("utf-8", "replace")
            elif payload.filename:
                raw_name = Path(payload.filename).stem
            dictionary = self.service.create_dictionary(raw_name, data)
            activate = parts.get("activate")
            if activate and activate.text and activate.text.strip().lower() in {
                b"1",
                b"true",
                b"yes",
                b"on",
            }:
                dictionary = self.service.activate_dictionary(dictionary["id"])
            self._send_json(HTTPStatus.CREATED, dictionary)
        finally:
            multipart.cleanup_parts(parts)

    def _upload_segment(self) -> None:
        limit = self.service.config.max_segment_bytes + 1024 * 1024
        parts = self._read_multipart(
            limit=limit, file_limit=self.service.config.max_segment_bytes
        )
        try:
            upload = parts.get("file") or parts.get("segment")
            if not upload or upload.path is None:
                raise HttpError(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_input",
                    "a log segment file is required",
                )
            expected = self.headers.get("X-Content-SHA256")
            segment = self.service.ingest_file(
                upload.path, upload.filename, expected_sha256=expected
            )
            self.send_response(HTTPStatus.CREATED)
            body = json.dumps(segment, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header(
                "Location", f"/api/segments/{segment['number']}"
            )
            self.end_headers()
            self.wfile.write(body)
        finally:
            multipart.cleanup_parts(parts)

    def _retry_recompression(self) -> None:
        length = self.headers.get("Content-Length")
        payload: dict[str, Any] = {}
        if length:
            body = self._read_body(64 * 1024)
            if body.strip():
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError:
                    raise HttpError(
                        HTTPStatus.BAD_REQUEST, "invalid_input", "body must be JSON"
                    )
        segment = payload.get("segment")
        if segment is not None:
            try:
                segment = int(segment)
            except (TypeError, ValueError):
                raise HttpError(
                    HTTPStatus.BAD_REQUEST, "invalid_input", "segment must be an integer"
                )
        result = self.service.retry_recompression(segment)
        self._send_json(HTTPStatus.ACCEPTED, result)

    def _download_bytes(self, row: dict[str, Any], temp_path: Path) -> None:
        size = os.path.getsize(temp_path)
        filename = row["filename"]
        ascii_name = re.sub(r"[^A-Za-z0-9._-]", "_", filename) or "segment.log"
        start, end, partial = 0, size - 1, False
        range_header = self.headers.get("Range")
        if range_header:
            parsed = _parse_single_range(range_header, size)
            if parsed is None:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            start, end = parsed
            partial = True

        length = end - start + 1 if size else 0
        self.send_response(HTTPStatus.PARTIAL_CONTENT if partial else HTTPStatus.OK)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header(
            "Content-Disposition",
            f"attachment; filename=\"{ascii_name}\"; "
            f"filename*=UTF-8''{quote(filename, safe='')}",
        )
        self.send_header("X-Segment-Sha256", row["original_sha256"])
        self.send_header("X-Segment-Number", str(row["number"]))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command == "HEAD":
            return
        with open(temp_path, "rb") as fh:
            if start:
                fh.seek(start)
            remaining = length
            while remaining > 0:
                chunk = fh.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

    def _download(self, number: int) -> None:
        if self.command == "HEAD":
            # Metadata is enough for a HEAD: avoid decompressing a possibly
            # large segment just to answer with headers.
            row = self.service.get_segment(number)
            self._download_headers(row, int(row["original_size"]))
            return
        row, temp_path = self.service.spool_segment(number)
        try:
            self._download_bytes(row, temp_path)
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    def _download_headers(self, row: dict[str, Any], size: int) -> None:
        filename = row["filename"]
        ascii_name = re.sub(r"[^A-Za-z0-9._-]", "_", filename) or "segment.log"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header(
            "Content-Disposition",
            f"attachment; filename=\"{ascii_name}\"; "
            f"filename*=UTF-8''{quote(filename, safe='')}",
        )
        self.send_header("X-Segment-Sha256", row["original_sha256"])
        self.send_header("X-Segment-Number", str(row["number"]))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()


class ArchiveServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], service: ArchiveService):
        self.service = service
        handler = type("BoundHandler", (Handler,), {"service": service})
        super().__init__(address, handler)


def _parse_single_range(header: str, size: int) -> Optional[tuple[int, int]]:
    """Parse one ``bytes=`` range.  Multi-range requests are rejected (None)."""
    value = header.strip()
    if not value.lower().startswith("bytes=") or size <= 0:
        return None
    spec = value[6:].strip()
    if "," in spec or "-" not in spec:
        return None
    first, _, last = spec.partition("-")
    try:
        if first == "":
            suffix = int(last)
            if suffix <= 0:
                return None
            start = max(0, size - suffix)
            end = size - 1
        else:
            start = int(first)
            end = int(last) if last else size - 1
    except ValueError:
        return None
    if start > end or start >= size:
        return None
    return start, min(end, size - 1)


def build_server(config: Optional[Config] = None) -> tuple[ArchiveServer, ArchiveService]:
    config = config or Config.from_env()
    service = ArchiveService(config)
    server = ArchiveServer((config.host, config.port), service)
    return server, service


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config.from_env()
    server, service = build_server(config)
    service.start_worker()
    host, port = server.server_address[:2]
    log.info("log archive listening on http://%s:%s (data=%s)", host, port, config.data_dir)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        server.server_close()
        service.stop_worker()
    return 0


if __name__ == "__main__":
    sys.exit(main())
