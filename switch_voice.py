#!/usr/bin/env python3

import argparse
import json
import sys
import urllib.error
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a voice stream switch request to the RTSP control server.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Control server host")
    parser.add_argument("--port", type=int, required=True, help="Control server port")
    parser.add_argument("--wav", required=True, help="WAV file path relative to the media root")
    parser.add_argument("--id", required=True, dest="stream_id", help="Shared voice stream id")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    url = f"http://{args.host}:{args.port}/voice_stream/switch"
    payload = json.dumps({"id": args.stream_id, "file": args.wav}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            print(body)
            return 0
    except urllib.error.HTTPError as ex:
        body = ex.read().decode("utf-8", errors="replace")
        print(format_error_body(body, ex.code, ex.reason), file=sys.stderr)
        return 1
    except urllib.error.URLError as ex:
        print(f"request failed: {ex}", file=sys.stderr)
        return 1


def format_error_body(body: str, status_code: int, reason: str) -> str:
    if not body:
        return f"HTTP {status_code} {reason}"
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body
    if isinstance(payload, dict):
        message = str(payload.get("message", "")).strip()
        error = str(payload.get("error", "")).strip()
        if message and error:
            return f"{error}: {message}"
        if message:
            return message
        if error:
            return error
    return body


if __name__ == "__main__":
    raise SystemExit(main())
