"""Acceptance walkthrough: interleaved dictionary switch, uploads and restarts.

Run it against a live deployment, e.g.

    python3 scripts/acceptance_walkthrough.py --url http://127.0.0.1:8080

It creates two dictionaries and a handful of log segments, then exercises:

1. activation while uploads and downloads are in flight;
2. downloads of segments that are mid-conversion;
3. a hard restart in the middle of the recompression backlog;
4. byte-for-byte comparison of every download with the local source file;
5. dictionary deletion rules and post-deletion downloads.

The script never deletes user data: it only creates segments and the two
dictionaries it made itself, and it waits for the backlog to drain before
checking deletion rules.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

DICT_V1 = (
    b"timestamp= severity=INFO service=api-gateway user_id= request_id= "
    b"endpoint= method=GET status= duration_ms= message="
) * 3
DICT_V2 = (
    b"ts= level=INFO ERROR WARN svc=gateway uid= rid= path= verb= code= "
    b"latency_ms= msg= traceparent="
) * 3


def request(url, method, path, body=None, headers=None, timeout=120):
    req = urllib.request.Request(url.rstrip("/") + path, data=body, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def api(url, method, path, payload=None, timeout=120):
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if body else {}
    status, _, raw = request(url, method, path, body, headers, timeout)
    if status >= 400:
        raise RuntimeError(f"{method} {path} -> {status}: {raw.decode('utf-8', 'replace')}")
    return json.loads(raw) if raw and raw.lstrip().startswith(b"{") else None


def multipart(fields, files):
    boundary = f"----acc{uuid.uuid4().hex}"
    chunks = []
    for name, value in fields.items():
        chunks.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            + value.encode() + b"\r\n"
        )
    for name, (filename, data) in files.items():
        chunks.append(
            (f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; '
             f'filename="{filename}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()
            + data + b"\r\n"
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


def upload_segment(url, path: Path, payload: bytes):
    body, ctype = multipart({}, {"file": (path.name, payload)})
    headers = {
        "Content-Type": ctype,
        "Content-Length": str(len(body)),
        "X-Content-SHA256": hashlib.sha256(payload).hexdigest(),
    }
    status, _, raw = request(url, "POST", "/api/segments", body, headers)
    if status >= 400:
        raise RuntimeError(f"upload failed {status}: {raw.decode('utf-8', 'replace')}")
    return json.loads(raw)


def slow_upload(url, path: Path, payload: bytes, halfway) -> dict:
    """Upload a segment in two writes, calling *halfway()* between them.

    This guarantees the activation happens while the request body is still on
    the wire, which is the case the archive has to handle: a segment whose
    upload began before the switch but which is admitted afterwards.
    """
    body, ctype = multipart({}, {"file": (path.name, payload)})
    parsed = urllib.parse.urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=120)
    try:
        conn.putrequest("POST", "/api/segments")
        conn.putheader("Content-Type", ctype)
        conn.putheader("Content-Length", str(len(body)))
        conn.putheader("X-Content-SHA256", hashlib.sha256(payload).hexdigest())
        conn.endheaders()
        split = len(body) // 2
        conn.send(body[:split])
        halfway()
        conn.send(body[split:])
        response = conn.getresponse()
        raw = response.read()
        if response.status >= 400:
            raise RuntimeError(f"slow upload failed {response.status}: {raw.decode()}")
        return json.loads(raw)
    finally:
        conn.close()


def upload_dictionary(url, name: str, data: bytes, activate: bool):
    fields = {"name": name}
    if activate:
        fields["activate"] = "1"
    body, ctype = multipart(fields, {"dictionary": (f"{name}.dict", data)})
    status, _, raw = request(url, "POST", "/api/dictionaries", body,
                             {"Content-Type": ctype, "Content-Length": str(len(body))})
    if status >= 400:
        raise RuntimeError(f"dictionary upload failed {status}: {raw.decode()}")
    return json.loads(raw)


def download(url, number: int) -> bytes:
    status, _, raw = request(url, "GET", f"/api/segments/{number}/download")
    if status != 200:
        raise RuntimeError(f"download #{number} failed {status}")
    return raw


def make_segment(index: int) -> bytes:
    lines = []
    for i in range(4000):
        lines.append(
            f"timestamp=2026-11-20T00:{(i + index) % 60:02d}:00Z severity="
            f"{'ERROR' if i % 13 == 0 else 'INFO'} service=api-gateway "
            f"request_id=req-{index}-{i} endpoint=/v1/items/{i % 200} "
            f"status={500 if i % 101 == 0 else 200} user_id=u{(i * 7) % 999} "
            f"message=request completed for user u{(i * 7) % 999}\n"
        )
    return "".join(lines).encode()


def wait_drained(url, timeout=180.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = api(url, "GET", "/api/status")
        if status["backlog"] == 0 and status["states"]["running"] == 0:
            return status
        time.sleep(0.2)
    raise RuntimeError("backlog did not drain")


class Checker:
    def __init__(self):
        self.passed = []
        self.failed = []

    def check(self, ok, label, detail=""):
        if ok:
            self.passed.append(label)
            print(f"  PASS  {label}")
        else:
            self.failed.append(f"{label} {detail}")
            print(f"  FAIL  {label}  {detail}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("ARCHIVE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--docker", action="store_true",
                        help="restart via docker compose instead of assuming an external restart")
    parser.add_argument("--compose-file", default="docker-compose.yml")
    parser.add_argument("--restart-cmd", default="",
                        help="shell command that restarts the service")
    args = parser.parse_args()
    url = args.url
    checks = Checker()
    tmp = Path(tempfile.mkdtemp(prefix="acceptance-"))
    print(f"workdir: {tmp}")

    status = api(url, "GET", "/api/status")
    print(f"service reachable, {status['segments']} existing segment(s)")

    v1 = upload_dictionary(url, f"acc-v1-{uuid.uuid4().hex[:6]}", DICT_V1, activate=True)
    checks.check(v1["active"], "first dictionary activated")

    payloads = {}
    segments = {}
    for i in range(4):
        payload = make_segment(i)
        path = tmp / f"before-{i}.log"
        path.write_bytes(payload)
        segment = upload_segment(url, path, payload)
        payloads[segment["number"]] = payload
        segments[segment["number"]] = path
    checks.check(len(segments) == 4, "four segments ingested under v1")
    checks.check(
        all(api(url, "GET", f"/api/segments/{n}")["dictionary_id"] == v1["id"] for n in segments),
        "segments recorded with v1",
    )

    print("\n-- step 1: interleaved activation, upload and download --")
    v2 = upload_dictionary(url, f"acc-v2-{uuid.uuid4().hex[:6]}", DICT_V2, activate=False)
    during = {}
    errors = []

    def uploader(i):
        try:
            payload = make_segment(100 + i)
            path = tmp / f"during-{i}.log"
            path.write_bytes(payload)
            segment = upload_segment(url, path, payload)
            during[segment["number"]] = (payload, segment)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def downloader():
        for number in list(segments):
            try:
                if download(url, number) != payloads[number]:
                    errors.append(RuntimeError(f"download mismatch for #{number}"))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

    threads = [threading.Thread(target=uploader, args=(i,)) for i in range(4)]
    threads.append(threading.Thread(target=downloader))
    for t in threads:
        t.start()

    # One upload starts before the switch and is still transferring when the
    # new dictionary is activated.  It must be admitted under the new version.
    straddler_payload = make_segment(200)
    straddler_path = tmp / "straddler.log"
    straddler_path.write_bytes(straddler_payload)

    def activate_midway():
        api(url, "POST", f"/api/dictionaries/{v2['id']}/activate")
        print("      (v2 activated while an upload was in flight)")

    time.sleep(0.05)
    straddler = slow_upload(url, straddler_path, straddler_payload, activate_midway)
    for t in threads:
        t.join(120)
    checks.check(not errors, "no errors during interleaved upload/download", str(errors))
    checks.check(
        straddler["dictionary_id"] == v2["id"],
        "upload in flight during activation was stored with v2",
        f"got {straddler['dictionary_id']}",
    )
    checks.check(
        all(download(url, n) == payloads[n] for n in segments),
        "old segments downloadable during recompression",
    )
    payloads[straddler["number"]] = straddler_payload

    print("\n-- step 2: restart in the middle of the backlog --")
    mid = [n for n in segments if api(url, "GET", f"/api/segments/{n}")["recompress_state"] != "current"]
    if mid:
        print(f"segments still mid-conversion at restart: {mid}")
    if args.restart_cmd:
        print(f"restarting via: {args.restart_cmd}")
        subprocess.run(args.restart_cmd, shell=True, check=False)
    elif args.docker:
        subprocess.run(["docker", "compose", "-f", args.compose_file, "restart"], check=False)
    else:
        print("no restart command supplied; skipping the interruption step")
    if args.restart_cmd or args.docker:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                api(url, "GET", "/healthz")
                break
            except Exception:
                time.sleep(0.5)
        status = wait_drained(url)
        checks.check(
            all(api(url, "GET", f"/api/segments/{n}")["dictionary_id"] == v2["id"] for n in segments),
            "all pre-switch segments recompressed after restart",
        )
        checks.check(status["states"]["failed"] == 0, "no failed conversions after restart")

    status = wait_drained(url)
    checks.check(status["backlog"] == 0, "backlog drained")
    checks.check(
        all(
            api(url, "GET", f"/api/segments/{n}")["dictionary_id"] == v2["id"]
            for n in payloads
        ),
        "every segment ends on v2 after drain",
    )
    for number, payload in payloads.items():
        if download(url, number) != payload:
            checks.check(False, f"segment #{number} bytes unchanged after recompression")
            break
    else:
        checks.check(True, "all pre-switch segments byte-identical after recompression")

    print("\n-- step 3: dictionary deletion rules --")
    status, _, raw = request(url, "DELETE", f"/api/dictionaries/{v2['id']}")
    checks.check(status == 409, "active dictionary cannot be deleted", f"got {status}")
    status, _, raw = request(url, "DELETE", f"/api/dictionaries/{v1['id']}")
    checks.check(status == 204, "unreferenced old dictionary deleted", f"got {status}")
    checks.check(
        all(download(url, n) == payloads[n] for n in payloads),
        "segments still byte-identical after dictionary deletion",
    )

    print("\n-- summary --")
    print(f"{len(checks.passed)} passed, {len(checks.failed)} failed")
    for failure in checks.failed:
        print(f"  - {failure}")
    return 0 if not checks.failed else 1


if __name__ == "__main__":
    sys.exit(main())
