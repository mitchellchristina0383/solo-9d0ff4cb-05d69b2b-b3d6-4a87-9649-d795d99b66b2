"""Write-lock recovery test: block SQLite, then verify automatic resume.

The scenario behind the incident: a maintenance task holds the SQLite write
lock for a long time.  The background recompression worker hits
"database is locked" while committing a new compression result, and even the
failure bookkeeping cannot be written.  A healthy service must not require a
restart: as soon as the lock is released, the worker re-queues the interrupted
batch (preserving retry counters), finishes the conversion and keeps every
download byte-identical to the upload.

The script starts its own server process on a scratch data directory, uploads
segments, switches dictionaries, opens a second SQLite connection that holds a
write transaction for LOCK_HOLD_SECONDS, and then verifies that:

1. during the lock, segments remain downloadable with the original bytes;
2. the worker thread stays alive and the UI-visible state eventually resumes;
3. after the lock clears, every segment finishes on the new dictionary without
   a process restart and without spending retry attempts;
4. a dictionary activated during the recovery window wins: an older in-flight
   result can never overwrite the newer round's state.

    python3 scripts/lock_recovery_test.py
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.acceptance_walkthrough import (  # noqa: E402
    DICT_V1,
    DICT_V2,
    api,
    download,
    upload_dictionary,
    upload_segment,
)


def make_payload(index: int) -> bytes:
    chunk = "".join(
        f"timestamp=2026-11-20T00:{(i + index) % 60:02d}:00Z severity=INFO "
        f"service=api request_id=req-{index}-{i} endpoint=/v1/items/{i % 50} "
        f"status=200 user_id=u{i % 77} message=request completed\n"
        for i in range(6000)
    ).encode()
    return chunk


def hold_write_lock(db_path: Path, hold_seconds: float) -> None:
    """Open a separate connection and hold BEGIN IMMEDIATE for hold_seconds."""
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE dictionaries SET name = name")
        time.sleep(hold_seconds)
        conn.execute("COMMIT")
    finally:
        conn.close()


def main() -> int:
    port = int(os.environ.get("LOCK_TEST_PORT", "8098"))
    hold_seconds = float(os.environ.get("LOCK_HOLD_SECONDS", "6"))
    workdir = Path(os.environ.get("LOCK_TEST_DIR", "/tmp/logarchive-lock"))
    workdir.mkdir(parents=True, exist_ok=True)
    data = workdir / "data"
    if data.exists():
        shutil.rmtree(data)
    data.mkdir(parents=True)
    log_path = workdir / "server.log"
    url = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "DATA_DIR": str(data),
        "PORT": str(port),
        # Short busy timeout so the worker reaches its own retry loop quickly;
        # the service-level retry is what must carry it through the long lock.
        "DB_BUSY_TIMEOUT_SECONDS": "1",
        "RECOMPRESS_DELAY_MS": "0",
        "RECOMPRESS_BATCH_SIZE": "4",
        "LOG_LEVEL": "INFO",
    }

    def start() -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "logarchive.app"],
            cwd=str(ROOT),
            env=env,
            stdout=open(log_path, "ab"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        for _ in range(200):
            try:
                urllib.request.urlopen(url + "/healthz", timeout=1).read()
                return proc
            except Exception:
                if proc.poll() is not None:
                    raise SystemExit(f"server exited early; see {log_path}")
                time.sleep(0.1)
        raise SystemExit("server did not become healthy")

    failures: list[str] = []

    def check(condition: bool, label: str, detail: str = "") -> None:
        print(("PASS  " if condition else "FAIL  ") + label + (f"  {detail}" if detail else ""))
        if not condition:
            failures.append(label)

    proc = start()
    try:
        upload_dictionary(url, "v1", DICT_V1, activate=True)
        payloads = {}
        for i in range(4):
            payload = make_payload(i)
            path = workdir / f"seg-{i}.log"
            path.write_bytes(payload)
            segment = upload_segment(url, path, payload)
            payloads[segment["number"]] = payload
        print(f"uploaded {len(payloads)} segments")

        v2 = upload_dictionary(url, "v2", DICT_V2, activate=False)
        # Take the write lock *before* activation: every write the worker and
        # the API need (activation itself, claims, commits) now contends with
        # the maintenance transaction, exactly like the reported incident.
        print(f"holding the SQLite write lock for {hold_seconds:.0f}s ...")
        lock_thread = threading.Thread(
            target=hold_write_lock,
            args=(data / "archive.sqlite3", hold_seconds),
        )
        lock_thread.start()
        time.sleep(0.5)
        activated = threading.Event()

        def activate() -> None:
            api(url, "POST", f"/api/dictionaries/{v2['id']}/activate")
            activated.set()

        activate_thread = threading.Thread(target=activate)
        activate_thread.start()
        self_activating = activated.wait(hold_seconds + 15)
        check(self_activating, "dictionary activation completed once the lock cleared")
        activate_thread.join(5)

        # During the lock: downloads keep working and the server stays healthy.
        mid_ok = all(download(url, n) == p for n, p in payloads.items())
        check(mid_ok, "original bytes downloadable while the write lock is held")
        status = api(url, "GET", "/api/status")
        check(status["worker"]["enabled"], "worker still enabled during the lock")

        lock_thread.join(timeout=hold_seconds + 10)
        check(not lock_thread.is_alive(), "external lock released")
        print("write lock released; waiting for the worker to resume on its own")

        # Resume without any restart: the backlog must drain by itself.
        deadline = time.time() + 120
        final_status = None
        while time.time() < deadline:
            final_status = api(url, "GET", "/api/status")
            if final_status["backlog"] == 0 and final_status["states"]["running"] == 0:
                break
            time.sleep(0.2)
        check(final_status is not None and final_status["backlog"] == 0,
              "backlog drained after the lock cleared (no restart)")
        check(final_status is not None and final_status["states"]["failed"] == 0,
              "no segment entered the failed state",
              json.dumps(final_status["states"]) if final_status else "")
        check(final_status is not None and final_status["states"]["running"] == 0,
              "no segment stuck in running")

        target = api(url, "GET", "/api/status")["active_dictionary"]["id"]
        all_current = True
        attempts_clean = True
        bytes_match = True
        for number, payload in payloads.items():
            row = api(url, "GET", f"/api/segments/{number}")
            if row["dictionary_id"] != target or row["recompress_state"] != "current":
                all_current = False
            # Lock contention must never have been counted as a failure.
            if row["recompress_attempts"] != 0:
                attempts_clean = False
            if download(url, number) != payload:
                bytes_match = False
        check(all_current, "every segment converted to the active dictionary")
        check(attempts_clean, "retry counters stayed at zero through lock contention")
        check(bytes_match, "every download is byte-identical to the upload")

        # Second round: activate yet another dictionary during recovery and
        # prove an older in-flight write cannot overwrite the newer state.
        v3 = upload_dictionary(url, "v3", DICT_V2 + b"round-three", activate=False)
        api(url, "POST", f"/api/dictionaries/{v3['id']}/activate")
        deadline = time.time() + 120
        while time.time() < deadline:
            status = api(url, "GET", "/api/status")
            if status["backlog"] == 0 and status["states"]["running"] == 0:
                break
            time.sleep(0.2)
        round_three_ok = all(
            api(url, "GET", f"/api/segments/{n}")["dictionary_id"] == v3["id"]
            for n in payloads
        )
        check(round_three_ok, "newer activation round wins over stale in-flight work")
        check(
            all(download(url, n) == p for n, p in payloads.items()),
            "bytes still identical after the second round",
        )
    finally:
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.kill(proc.pid, signal.SIGKILL)

    print("\n" + ("ALL CHECKS PASSED" if not failures else f"FAILURES: {failures}"))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
