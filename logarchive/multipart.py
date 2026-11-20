"""Streaming multipart/form-data parser.

Segments can be large, so file parts are written straight to a temporary file
instead of being buffered in memory.  Only small text fields are kept as bytes.
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Optional

MAX_HEADER_BYTES = 64 * 1024
MAX_FIELD_BYTES = 64 * 1024
READ_CHUNK = 256 * 1024

_NAME_RE = re.compile(r';\s*name="((?:[^"\\]|\\.)*)"', re.IGNORECASE)
_FILENAME_RE = re.compile(r';\s*filename="((?:[^"\\]|\\.)*)"', re.IGNORECASE)
_FILENAME_STAR_RE = re.compile(r";\s*filename\*=UTF-8''([^;\s]+)", re.IGNORECASE)


class MultipartError(Exception):
    pass


class LimitedReader:
    """Expose at most *limit* bytes of an underlying binary stream.

    http.server's ``rfile`` is a raw socket file: reading past Content-Length
    blocks forever waiting for data that will never come.  Bounding reads to
    the declared body length turns the end of the body into a clean EOF.
    """

    def __init__(self, stream: BinaryIO, limit: int):
        self.stream = stream
        self.remaining = limit

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if size is None or size < 0:
            size = self.remaining
        # read1() (when available) returns as soon as some bytes arrive, so a
        # client that is still transmitting cannot stall the parser.
        method = getattr(self.stream, "read1", None) or self.stream.read
        data = method(min(size, self.remaining))
        self.remaining -= len(data)
        return data

    read1 = read


@dataclass
class Part:
    name: str
    filename: Optional[str]
    content_type: Optional[str]
    size: int
    text: Optional[bytes] = None
    path: Optional[Path] = None

    def cleanup(self) -> None:
        if self.path is not None:
            try:
                os.unlink(self.path)
            except OSError:
                pass
            self.path = None


def _unescape(value: str) -> str:
    return re.sub(r"\\(.)", r"\1", value)


def parse_boundary(content_type: str) -> bytes:
    if not content_type:
        raise MultipartError("missing Content-Type")
    if not content_type.lower().startswith("multipart/form-data"):
        raise MultipartError("expected multipart/form-data")
    match = re.search(r'boundary="?([^";]+)"?', content_type, re.IGNORECASE)
    if not match:
        raise MultipartError("multipart boundary is missing")
    boundary = match.group(1).strip()
    if not boundary or len(boundary) > 200:
        raise MultipartError("invalid multipart boundary")
    return boundary.encode("latin-1", "strict")


class _StreamReader:
    def __init__(self, stream: BinaryIO, tmp_dir: Path, max_total: int):
        self.stream = stream
        self.tmp_dir = tmp_dir
        self.max_total = max_total
        self.buffer = bytearray()
        self.total = 0
        self.eof = False

    def fill(self) -> bool:
        if self.eof:
            return False
        # read1() returns as soon as *some* bytes are available.  A plain
        # read(n) on a socket backed file object blocks until n bytes or EOF,
        # which would deadlock while the client is still sending.
        read = getattr(self.stream, "read1", None) or self.stream.read
        data = read(READ_CHUNK)
        if not data:
            self.eof = True
            return False
        self.total += len(data)
        if self.total > self.max_total:
            raise MultipartError("request body exceeds the configured limit")
        self.buffer.extend(data)
        return True

    def fill_until(self, marker: bytes, limit: int) -> None:
        # Compare the marker *position* to the limit, not the buffer length:
        # a single read may already contain body bytes beyond the headers.
        while True:
            index = self.buffer.find(marker)
            if index != -1:
                if index > limit:
                    raise MultipartError("multipart header section is too large")
                return
            if len(self.buffer) > limit:
                raise MultipartError("multipart header section is too large")
            if not self.fill():
                raise MultipartError("unexpected end of multipart body")

    def take(self, count: int) -> bytes:
        while len(self.buffer) < count:
            if not self.fill():
                raise MultipartError("unexpected end of multipart body")
        data = bytes(self.buffer[:count])
        del self.buffer[:count]
        return data


def _parse_headers(raw: bytes) -> tuple[str, Optional[str], Optional[str]]:
    text = raw.decode("latin-1")
    name = None
    filename = None
    content_type = None
    for line in text.split("\r\n"):
        if not line:
            continue
        header, _, value = line.partition(":")
        header = header.strip().lower()
        value = value.strip()
        if header == "content-disposition":
            match = _NAME_RE.search(value)
            if match:
                name = _unescape(match.group(1))
            match = _FILENAME_RE.search(value)
            if match:
                filename = _unescape(match.group(1))
            else:
                match = _FILENAME_STAR_RE.search(value)
                if match:
                    from urllib.parse import unquote

                    filename = unquote(match.group(1))
        elif header == "content-type":
            content_type = value
    if name is None:
        raise MultipartError("multipart part is missing a field name")
    return name, filename, content_type


def parse_multipart(
    stream: BinaryIO,
    content_type: str,
    *,
    tmp_dir: Path,
    max_total_bytes: int,
    max_file_bytes: int,
) -> dict[str, Part]:
    boundary = parse_boundary(content_type)
    reader = _StreamReader(stream, Path(tmp_dir), max_total_bytes)
    delimiter = b"--" + boundary
    separator = b"\r\n" + delimiter

    reader.fill_until(delimiter, MAX_HEADER_BYTES)
    index = reader.buffer.find(delimiter)
    del reader.buffer[: index + len(delimiter)]
    marker = reader.take(2)
    if marker == b"--":
        while reader.fill():
            pass
        return {}
    if marker != b"\r\n":
        raise MultipartError("malformed multipart preamble")

    parts: dict[str, Part] = {}
    while True:
        reader.fill_until(b"\r\n\r\n", MAX_HEADER_BYTES)
        header_end = reader.buffer.find(b"\r\n\r\n")
        raw_headers = bytes(reader.buffer[:header_end])
        del reader.buffer[: header_end + 4]

        name, filename, part_type = _parse_headers(raw_headers)

        target_path: Optional[Path] = None
        sink = None
        collected = bytearray()
        written = 0
        if filename is not None:
            fd, path = tempfile.mkstemp(prefix="upload-", dir=reader.tmp_dir)
            sink = os.fdopen(fd, "wb")
            target_path = Path(path)
        try:
            while True:
                search_at = reader.buffer.find(separator)
                if search_at != -1:
                    chunk = bytes(reader.buffer[:search_at])
                    del reader.buffer[: search_at + len(separator)]
                    if chunk:
                        written += len(chunk)
                        if written > max_file_bytes:
                            raise MultipartError("uploaded part is too large")
                        if sink:
                            sink.write(chunk)
                        else:
                            collected.extend(chunk)
                            if len(collected) > MAX_FIELD_BYTES:
                                raise MultipartError("text field is too large")
                    suffix = reader.take(2)
                    if suffix == b"--":
                        # Consume the optional epilogue so the request body is
                        # fully drained, then stop.
                        while reader.fill():
                            pass
                        finished = True
                    elif suffix == b"\r\n":
                        finished = False
                    else:
                        raise MultipartError("malformed multipart boundary")
                    break

                keep = len(separator) - 1
                if len(reader.buffer) > keep:
                    safe = len(reader.buffer) - keep
                    chunk = bytes(reader.buffer[:safe])
                    del reader.buffer[:safe]
                    written += len(chunk)
                    if written > max_file_bytes:
                        raise MultipartError("uploaded part is too large")
                    if sink:
                        sink.write(chunk)
                    else:
                        collected.extend(chunk)
                        if len(collected) > MAX_FIELD_BYTES:
                            raise MultipartError("text field is too large")
                if not reader.fill():
                    raise MultipartError("unexpected end of multipart body")

            if sink:
                sink.flush()
                os.fsync(sink.fileno())
                sink.close()
                sink = None
            parts[name] = Part(
                name=name,
                filename=filename,
                content_type=part_type,
                size=written,
                text=None if target_path else bytes(collected),
                path=target_path,
            )
            target_path = None
            if finished:
                break
        finally:
            if sink is not None:
                sink.close()
            if target_path is not None:
                try:
                    os.unlink(target_path)
                except OSError:
                    pass

    return parts


def cleanup_parts(parts: dict[str, Part]) -> None:
    for part in parts.values():
        part.cleanup()
