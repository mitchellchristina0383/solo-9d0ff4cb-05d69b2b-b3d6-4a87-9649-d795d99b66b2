"""Byte-level helpers: raw-DEFLATE with optional zlib preset dictionaries.

The archive stores the *original* bytes only as the decompressed form of a
raw-DEFLATE stream.  The dictionary that was used is recorded next to the
segment so the original bytes can always be reproduced exactly.
"""

from __future__ import annotations

import hashlib
import os
import zlib
from pathlib import Path
from typing import BinaryIO, Optional

CHUNK_SIZE = 1024 * 1024
COMPRESSION_LEVEL = 6
MAX_DICTIONARY_SIZE = 32_768  # zlib's preset-dictionary window limit


class StorageError(RuntimeError):
    pass


class DecompressionError(StorageError):
    pass


def sha256_file(path: os.PathLike[str] | str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def hash_stream(stream: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = stream.read(CHUNK_SIZE)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def compress_file(
    source: os.PathLike[str] | str,
    destination: os.PathLike[str] | str,
    dictionary: Optional[bytes],
    *,
    level: int = COMPRESSION_LEVEL,
) -> tuple[str, int]:
    """Compress *source* into *destination* as raw DEFLATE.

    Returns (sha256 of compressed bytes, compressed size).  The caller is
    responsible for installing the destination file atomically.
    """
    if dictionary is not None:
        if len(dictionary) > MAX_DICTIONARY_SIZE:
            raise StorageError("preset dictionary exceeds 32768 bytes")
        compressor = zlib.compressobj(
            level,
            zlib.DEFLATED,
            -15,
            8,
            zlib.Z_DEFAULT_STRATEGY,
            zdict=dictionary,
        )
    else:
        compressor = zlib.compressobj(level, zlib.DEFLATED, -15)

    digest = hashlib.sha256()
    written = 0
    with open(source, "rb") as src, open(destination, "wb") as dst:
        while True:
            chunk = src.read(CHUNK_SIZE)
            if not chunk:
                break
            out = compressor.compress(chunk)
            if out:
                dst.write(out)
                digest.update(out)
                written += len(out)
        out = compressor.flush()
        if out:
            dst.write(out)
            digest.update(out)
            written += len(out)
        dst.flush()
        os.fsync(dst.fileno())
    return digest.hexdigest(), written


def decompress_stream(
    source: BinaryIO,
    destination: os.PathLike[str] | str,
    dictionary: Optional[bytes],
) -> tuple[str, int, int]:
    """Decompress a raw-DEFLATE stream and verify structural integrity.

    Returns (sha256 of original bytes, original size, consumed compressed
    bytes).  Extra trailing bytes or a truncated stream are rejected.
    """
    if dictionary is not None:
        decompressor = zlib.decompressobj(-15, zdict=dictionary)
    else:
        decompressor = zlib.decompressobj(-15)

    digest = hashlib.sha256()
    written = 0
    consumed = 0
    with open(destination, "wb") as dst:
        while True:
            chunk = source.read(CHUNK_SIZE)
            if not chunk:
                break
            consumed += len(chunk)
            try:
                out = decompressor.decompress(chunk)
            except zlib.error as exc:
                raise DecompressionError(f"invalid DEFLATE stream: {exc}") from exc
            if out:
                dst.write(out)
                digest.update(out)
                written += len(out)
        try:
            out = decompressor.flush()
        except zlib.error as exc:
            raise DecompressionError(f"invalid DEFLATE stream: {exc}") from exc
        if out:
            dst.write(out)
            digest.update(out)
            written += len(out)
        dst.flush()
        os.fsync(dst.fileno())

    if not decompressor.eof:
        raise DecompressionError("truncated DEFLATE stream")
    if decompressor.unused_data:
        raise DecompressionError("trailing bytes after DEFLATE stream")
    return digest.hexdigest(), written, consumed


def decompress_file(
    source: os.PathLike[str] | str,
    destination: os.PathLike[str] | str,
    dictionary: Optional[bytes],
) -> tuple[str, int, int]:
    with open(source, "rb") as src:
        return decompress_stream(src, destination, dictionary)


def atomic_install(temp_path: os.PathLike[str] | str, final_path: os.PathLike[str] | str) -> None:
    """Atomically move a fully written temp file into its final location."""
    final = Path(final_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temp_path, final)
    try:
        dir_fd = os.open(final.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        # Directory fsync is not supported everywhere; durability of the file
        # contents was already enforced before the rename.
        pass


def remove_quietly(path: os.PathLike[str] | str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError:
        pass
