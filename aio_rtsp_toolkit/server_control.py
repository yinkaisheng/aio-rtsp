from __future__ import annotations

import asyncio
import contextlib
import json
import urllib.parse
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional, Tuple, Type

import aio_sockets as aio


@dataclass
class HttpControlRequest:
    method: str
    target: str
    headers: Dict[str, str]
    body: bytes


class HttpControlServerHandler:
    def __init__(
        self,
        switch_voice_stream_file: Callable[[str, str], Awaitable[object]],
        *,
        inactive_error_types: Tuple[Type[BaseException], ...] = (),
    ) -> None:
        self._switch_voice_stream_file = switch_voice_stream_file
        self._inactive_error_types = inactive_error_types

    async def handle_client(self, sock: aio.TCPSocket) -> None:
        status_code = 500
        reason = "Internal Server Error"
        payload: Dict[str, object] = {
            "ok": False,
            "error": "internal_error",
            "message": "internal server error",
        }
        try:
            request = await self._read_control_request(sock)
            if request is None:
                return

            if request.method != "POST":
                status_code, reason, payload = 405, "Method Not Allowed", {
                    "ok": False,
                    "error": "method_not_allowed",
                    "message": "only POST is supported",
                }
            elif urllib.parse.urlsplit(request.target).path != "/voice_stream/switch":
                status_code, reason, payload = 404, "Not Found", {
                    "ok": False,
                    "error": "not_found",
                    "message": "unknown control endpoint",
                }
            else:
                parsed = self._parse_control_payload(request.headers, request.body)
                file_path = await self._switch_voice_stream_file(parsed["id"], parsed["file"])
                status_code = 200
                reason = "OK"
                payload = {
                    "ok": True,
                    "id": parsed["id"],
                    "file": getattr(file_path, "name", str(file_path)),
                }
        except FileNotFoundError as ex:
            status_code, reason, payload = 404, "Not Found", {
                "ok": False,
                "error": "file_not_found",
                "message": str(ex),
            }
        except self._inactive_error_types as ex:
            status_code, reason, payload = 400, "Bad Request", {
                "ok": False,
                "error": "stream_not_active",
                "message": str(ex),
            }
        except json.JSONDecodeError as ex:
            status_code, reason, payload = 400, "Bad Request", {
                "ok": False,
                "error": "invalid_json",
                "message": str(ex),
            }
        except ValueError as ex:
            status_code, reason, payload = 400, "Bad Request", {
                "ok": False,
                "error": "bad_request",
                "message": str(ex),
            }
        except asyncio.IncompleteReadError:
            return
        finally:
            with contextlib.suppress(Exception):
                await self._send_control_response(
                    sock,
                    status_code=status_code,
                    reason=reason,
                    body=json.dumps(payload).encode("utf-8"),
                    content_type="application/json",
                )
            with contextlib.suppress(Exception):
                await sock.close()

    async def _read_control_request(self, sock: aio.TCPSocket) -> Optional[HttpControlRequest]:
        request_line = await sock.recv_until(b"\n")
        if not request_line:
            return None
        try:
            method, target, _ = request_line.decode("utf-8").strip().split(" ", 2)
        except ValueError as ex:
            raise ValueError("malformed HTTP request line") from ex
        headers: Dict[str, str] = {}
        while True:
            line = await sock.recv_until(b"\n")
            if not line or line == b"\r\n":
                break
            decoded = line.decode("utf-8").rstrip("\r\n")
            if ":" not in decoded:
                continue
            key, value = decoded.split(":", 1)
            headers[key.strip().lower()] = value.strip()
        content_length = int(headers.get("content-length", "0") or "0")
        body = b""
        if content_length > 0:
            body = await sock.recv_exactly(content_length)
        return HttpControlRequest(
            method=method.upper(),
            target=target,
            headers=headers,
            body=body,
        )

    async def _send_control_response(
        self,
        sock: aio.TCPSocket,
        status_code: int,
        reason: str,
        body: bytes,
        content_type: str,
    ) -> None:
        response = [
            f"HTTP/1.1 {status_code} {reason}",
            "Connection: close",
            f"Content-Length: {len(body)}",
            f"Content-Type: {content_type}",
            "",
            "",
        ]
        await sock.send("\r\n".join(response).encode("utf-8") + body)

    def _parse_control_payload(self, headers: Dict[str, str], request_body: bytes) -> Dict[str, str]:
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type == "application/x-www-form-urlencoded":
            values = urllib.parse.parse_qs(request_body.decode("utf-8"), keep_blank_values=True)
            payload = {
                "id": (values.get("id", [""])[0] or "").strip(),
                "file": (values.get("file", [""])[0] or "").strip(),
            }
        else:
            parsed = json.loads((request_body or b"{}").decode("utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError("request body must be a JSON object")
            payload = {
                "id": str(parsed.get("id", "")).strip(),
                "file": str(parsed.get("file", "")).strip(),
            }
        if not payload["id"]:
            raise ValueError("control payload requires id")
        if not payload["file"]:
            raise ValueError("control payload requires file")
        return payload
