"""Hard-interruption test: SIGKILL the service mid-conversion and restart it.

Starts its own server process on a scratch data directory (so it never touches
a real deployment), uploads a large segment, switches dictionaries, waits until
the conversion is actually running, kills the process with SIGKILL, starts it
again and verifies that the segment is downloadable at every point and that the
conversion resumes and finishes.

    python3 scripts/interruption_test.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
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


def make_payload() -> bytes:
    chunk = "".join(
        f"timestamp=2026-11-20T00:{i % 60:02d}:00Z severity=INFO service=api "
        f"request_id=req-{i} endpoint=/v1/items/{i % 400} user_id=u{i % 1000} "
        f"message=request completed for user u{i % 1000}\n"
        for i in range(20000)
    ).encode()
    return chunk * 6


def main() -> int:
    port = int(os.environ.get("INTERRUPT_TEST_PORT", "8097"))
    workdir = Path(os.environ.get("INTERRUPT_TEST_DIR", "/tmp/logarchive-interrupt"))
    workdir.mkdir(parents=True, exist_ok=True)
    data = workdir / "data"
    if data.exists():
        import shutil

        shutil.rmtree(data)
    data.mkdir(parents=True)
    log_path = workdir / "server.log"
    url = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "DATA_DIR": str(data),
        "PORT": str(port),
        "RECOMPRESS_DELAY_MS": "0",
        "RECOMPRESS_BATCH_SIZE": "1",
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
        payload = make_payload()
        path = workdir / "big.log"
        path.write_bytes(payload)
        segment = upload_segment(url, path, payload)
        print(f"uploaded segment #{segment['number']} ({len(payload)} bytes)")

        upload_dictionary(url, "v2", DICT_V2, activate=True)
        row = None
        for _ in range(400):
            row = api(url, "GET", f"/api/segments/{segment['number']}")
            if row["recompress_state"] in ("running", "current"):
                break
            time.sleep(0.01)
        print(f"state before SIGKILL: {row['recompress_state']}")
        check(row["recompress_state"] == "running", "conversion in flight at kill time")

        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
        print("server killed with SIGKILL")

        proc = start()
        print("server restarted")
        time.sleep(0.2)
        check(
            download(url, segment["number"]) == payload,
            "segment downloadable immediately after restart",
        )

        status = None
        deadline = time.time() + 120
        while time.time() < deadline:
            status = api(url, "GET", "/api/status")
            if status["backlog"] == 0 and status["states"]["running"] == 0:
                break
            time.sleep(0.1)
        final = api(url, "GET", f"/api/segments/{segment['number']}")
        check(final["recompress_state"] == "current", "conversion finished after restart")
        target = api(url, "GET", "/api/status")["active_dictionary"]["id"]
        check(final["dictionary_id"] == target, "segment now uses the active dictionary")
        check(download(url, segment["number"]) == payload, "bytes identical after resume")
        check(status["states"]["failed"] == 0, "no failed conversions", json.dumps(status["states"]))
        check(status["backlog"] == 0, "backlog drained")
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
