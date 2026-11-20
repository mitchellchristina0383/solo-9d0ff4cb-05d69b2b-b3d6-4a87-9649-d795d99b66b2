"""Application configuration, sourced from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _bool(value: str | None, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class Config:
    data_dir: Path
    host: str = "0.0.0.0"
    port: int = 8080
    max_segment_bytes: int = 512 * 1024 * 1024
    verify_writes: bool = True
    recompress_delay_ms: int = 100
    recompress_batch_size: int = 8
    worker_enabled: bool = True
    max_recompress_attempts: int = 5

    @classmethod
    def from_env(cls) -> "Config":
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        return cls(
            data_dir=data_dir,
            host=os.environ.get("HOST", "0.0.0.0"),
            port=_int(os.environ.get("PORT"), 8080),
            max_segment_bytes=_int(
                os.environ.get("MAX_SEGMENT_BYTES"), 512 * 1024 * 1024
            ),
            verify_writes=_bool(os.environ.get("VERIFY_WRITES"), True),
            recompress_delay_ms=max(0, _int(os.environ.get("RECOMPRESS_DELAY_MS"), 100)),
            recompress_batch_size=max(
                1, _int(os.environ.get("RECOMPRESS_BATCH_SIZE"), 8)
            ),
            worker_enabled=_bool(os.environ.get("WORKER_ENABLED"), True),
            max_recompress_attempts=max(
                1, _int(os.environ.get("MAX_RECOMPRESS_ATTEMPTS"), 5)
            ),
        )
