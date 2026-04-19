from __future__ import annotations

import asyncio
import struct
from typing import Dict, Optional

from .server_media import format_date_header
from .server_session_types import RtspBadRequestError, RtspRequest, _is_disconnect_error


DEFAULT_USER_AGENT = "python aio rtsp server"


class RtspProtocolMixin:
    async def _read_request(self) -> Optional[RtspRequest]:
        while not self.closed:
            try:
                first = await self.sock.recv_exactly(1)
            except asyncio.IncompleteReadError:
                return None
            if not first:
                return None
            if first == b"$":
                try:
                    header = await self.sock.recv_exactly(3)
                    channel = header[0]
                    payload_len = struct.unpack("!H", header[1:3])[0]
                    payload = b""
                    if payload_len > 0:
                        payload = await self.sock.recv_exactly(payload_len)
                    self._handle_interleaved_frame(channel, payload)
                except asyncio.IncompleteReadError:
                    return None
                continue
            try:
                raw = first + await self.sock.recv_until(b"\r\n\r\n")
            except asyncio.IncompleteReadError:
                return None
            except asyncio.LimitOverrunError as ex:
                raise RtspBadRequestError(f"request header too large: {ex.consumed} bytes")
            header_text = raw.decode("utf-8", errors="replace")
            lines = header_text.split("\r\n")
            if not lines or not lines[0]:
                raise RtspBadRequestError("empty request line")
            parts = lines[0].split(" ", 2)
            if len(parts) != 3:
                raise RtspBadRequestError(f"malformed request line: {lines[0]!r}")
            method, uri, version = parts
            if not version.startswith("RTSP/"):
                raise RtspBadRequestError(f"unsupported RTSP version: {version}")
            headers: Dict[str, str] = {}
            for line in lines[1:]:
                if not line or ":" not in line:
                    continue
                key, value = line.split(":", 1)
                headers[key.strip().lower()] = value.strip()
            content_length_text = headers.get("content-length", "0") or "0"
            try:
                content_length = int(content_length_text)
            except ValueError as ex:
                raise RtspBadRequestError(f"invalid Content-Length: {content_length_text!r}") from ex
            if content_length < 0:
                raise RtspBadRequestError("Content-Length must be greater than or equal to 0")
            body = b""
            if content_length > 0:
                body = await self.sock.recv_exactly(content_length)
            return RtspRequest(
                method=method.upper(),
                uri=uri,
                version=version,
                headers=headers,
                body=body,
            )
        return None

    async def _send_response(
        self,
        req_headers: Dict[str, str],
        status_code: int,
        reason: str,
        extra_headers: Optional[Dict[str, str]] = None,
        body: bytes = b"",
    ) -> None:
        headers = {
            "CSeq": req_headers.get("cseq", "0"),
            "Date": format_date_header(),
            "Server": DEFAULT_USER_AGENT,
        }
        if extra_headers:
            headers.update(extra_headers)
        headers["Content-Length"] = str(len(body)) if body else "0"
        lines = [f"RTSP/1.0 {status_code} {reason}"]
        lines.extend(f"{key}: {value}" for key, value in headers.items())
        payload = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8") + body
        async with self.write_lock:
            try:
                await self.sock.send(payload)
            except Exception as ex:
                if _is_disconnect_error(ex):
                    return
                raise
