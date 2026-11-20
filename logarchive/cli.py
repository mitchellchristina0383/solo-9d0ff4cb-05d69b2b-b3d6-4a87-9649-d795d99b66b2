"""Small command-line client for the archive HTTP API.

Useful for scripted checks and for the acceptance walkthrough in the README.

    python3 -m logarchive.cli status
    python3 -m logarchive.cli dict-upload access-v2.dict --name v2 --activate
    python3 -m logarchive.cli upload seg-0001.log ...
    python3 -m logarchive.cli download 3 --out restored.log
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Iterable


def _base(args: argparse.Namespace) -> str:
    return args.url.rstrip("/")


def _request(
    args: argparse.Namespace, method: str, path: str, body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(
        _base(args) + path, data=body, method=method, headers=headers or {}
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def _json(args: argparse.Namespace, method: str, path: str, payload: Any = None) -> Any:
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    status, _, raw = _request(args, method, path, body, headers)
    if status >= 400:
        raise SystemExit(f"HTTP {status}: {raw.decode('utf-8', 'replace')}")
    return json.loads(raw) if raw else None


def _multipart(
    fields: dict[str, str], files: dict[str, tuple[str, bytes, str | None]]
) -> tuple[bytes, str]:
    boundary = f"----cli{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
            + value.encode()
            + b"\r\n"
        )
    for name, (filename, data, content_type) in files.items():
        ctype = content_type or "application/octet-stream"
        chunks.append(
            (
                f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; '
                f'filename="{filename}"\r\nContent-Type: {ctype}\r\n\r\n'
            ).encode()
            + data
            + b"\r\n"
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


def _post_multipart(
    args: argparse.Namespace, path: str, fields: dict[str, str],
    files: dict[str, tuple[str, bytes, str | None]], extra_headers: dict[str, str] | None = None,
) -> Any:
    body, content_type = _multipart(fields, files)
    headers = {"Content-Type": content_type, "Content-Length": str(len(body))}
    headers.update(extra_headers or {})
    status, _, raw = _request(args, "POST", path, body, headers)
    if status >= 400:
        raise SystemExit(f"HTTP {status}: {raw.decode('utf-8', 'replace')}")
    return json.loads(raw)


def cmd_status(args: argparse.Namespace) -> int:
    print(json.dumps(_json(args, "GET", "/api/status"), indent=2, ensure_ascii=False))
    return 0


def cmd_dict_list(args: argparse.Namespace) -> int:
    for item in _json(args, "GET", "/api/dictionaries")["items"]:
        flag = "*" if item["active"] else " "
        print(
            f"{flag} {item['id']}  {item['name']:<20} {item['size']:>6}B  "
            f"segments={item['segment_count']}  sha={item['sha256'][:12]}"
        )
    return 0


def cmd_dict_upload(args: argparse.Namespace) -> int:
    path = Path(args.file)
    data = path.read_bytes()
    fields = {"name": args.name or path.stem}
    if args.activate:
        fields["activate"] = "1"
    result = _post_multipart(args, "/api/dictionaries", fields,
                             {"dictionary": (path.name, data, "application/octet-stream")})
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def cmd_dict_activate(args: argparse.Namespace) -> int:
    print(json.dumps(_json(args, "POST", f"/api/dictionaries/{args.id}/activate"),
                     indent=2, ensure_ascii=False))
    return 0


def cmd_dict_delete(args: argparse.Namespace) -> int:
    status, _, raw = _request(args, "DELETE", f"/api/dictionaries/{args.id}")
    if status >= 400:
        print(f"HTTP {status}: {raw.decode('utf-8', 'replace')}", file=sys.stderr)
        return 1
    return 0


def cmd_upload(args: argparse.Namespace) -> int:
    for name in args.files:
        path = Path(name)
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        headers = {"X-Content-SHA256": digest} if not args.no_checksum else {}
        result = _post_multipart(
            args, "/api/segments", {},
            {"file": (path.name, data, mimetypes.guess_type(path.name)[0])},
            headers,
        )
        print(
            f"segment #{result['number']}  dict={result['dictionary_name'] or '-'}  "
            f"state={result['recompress_state']}  bytes={result['original_size']}"
        )
    return 0


def cmd_download(args: argparse.Namespace) -> int:
    status, headers, raw = _request(args, "GET", f"/api/segments/{args.number}/download")
    if status >= 400:
        raise SystemExit(f"HTTP {status}: {raw.decode('utf-8', 'replace')}")
    target = Path(args.out) if args.out else Path(
        f"segment-{args.number}.log"
    )
    target.write_bytes(raw)
    if args.expect_sha:
        actual = hashlib.sha256(raw).hexdigest()
        if actual != args.expect_sha:
            raise SystemExit(f"checksum mismatch: {actual} != {args.expect_sha}")
    print(f"wrote {len(raw)} bytes to {target}")
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        status = _json(args, "GET", "/api/status")
        if status["backlog"] == 0 and status["states"]["running"] == 0:
            print(json.dumps(status, indent=2, ensure_ascii=False))
            return 0
        time.sleep(0.5)
    print("timed out waiting for recompression", file=sys.stderr)
    return 1


def cmd_segments(args: argparse.Namespace) -> int:
    data = _json(args, "GET", f"/api/segments?limit={args.limit}&offset={args.offset}")
    for item in data["items"]:
        print(
            f"#{item['number']:<5} {item['filename']:<30} "
            f"dict={item['dictionary_name'] or '-':<12} "
            f"state={item['recompress_state'] or 'current':<8} "
            f"orig={item['original_size']} comp={item['compressed_size']}"
        )
    print(f"total={data['total']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="log archive client")
    parser.add_argument("--url", default=os.environ.get("ARCHIVE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--timeout", type=float, default=120.0)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("dict-list").set_defaults(func=cmd_dict_list)

    upload = sub.add_parser("dict-upload")
    upload.add_argument("file")
    upload.add_argument("--name")
    upload.add_argument("--activate", action="store_true")
    upload.set_defaults(func=cmd_dict_upload)

    activate = sub.add_parser("dict-activate")
    activate.add_argument("id")
    activate.set_defaults(func=cmd_dict_activate)

    delete = sub.add_parser("dict-delete")
    delete.add_argument("id")
    delete.set_defaults(func=cmd_dict_delete)

    segments = sub.add_parser("segments")
    segments.add_argument("--limit", type=int, default=50)
    segments.add_argument("--offset", type=int, default=0)
    segments.set_defaults(func=cmd_segments)

    up = sub.add_parser("upload")
    up.add_argument("files", nargs="+")
    up.add_argument("--no-checksum", action="store_true")
    up.set_defaults(func=cmd_upload)

    down = sub.add_parser("download")
    down.add_argument("number", type=int)
    down.add_argument("--out")
    down.add_argument("--expect-sha")
    down.set_defaults(func=cmd_download)

    wait = sub.add_parser("wait")
    wait.add_argument("--timeout", type=float, default=300.0)
    wait.set_defaults(func=cmd_wait)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
