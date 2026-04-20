from __future__ import annotations

import asyncio
import base64
import hashlib
import struct
import urllib.parse
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

from . import types
from .tcp_client_base import AsyncTCPClientBase, RequestTimeoutError, RequestInterruptedError
from .tick import Tick
from .media import H264Splicer, H265Splicer, create_audio_splicer, ensure_start_code
from .types import (
    HeaderMap,
    RTCP,
    RTP,
    SDPInfo,
    SocketAddress,
    RtspClientMsgType,
    RtspProtocolError,
    RtspTimeoutError,
)


@dataclass
class RtspRequest:
    method: str
    uri: str
    cseq: int
    headers: List[Tuple[str, str]]
    body: bytes = b""
    media_type: Optional[str] = None


@dataclass
class RtspInterleavedData:
    channel: int
    packet: Union[RTP, RTCP]


class RtspResponse:
    def __init__(self, status_code: int, headers: HeaderMap, body: str = "") -> None:
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self.sdp: Optional[SDPInfo] = None
        self.elapsed = 0

    def __str__(self) -> str:
        return f'{self.__class__.__name__}(status_code={self.status_code}, elapsed={self.elapsed})'


class RtspProtocol(AsyncTCPClientBase[RtspRequest, int]):
    def __init__(
        self,
        url: str,
        forward_address: Optional[Tuple[str, int]] = None,
        timeout: float = 4,
        log_type: int = 0,
        log_tag: str = "",
    ) -> None:
        parse_result = urllib.parse.urlparse(url)
        connect_host = parse_result.hostname
        connect_port = parse_result.port if parse_result.port else 554
        if forward_address:
            connect_host, connect_port = forward_address
        super().__init__(connect_host, connect_port, recv_size=65536, recv_timeout=timeout, stop_timeout=0.2, log_tag=log_tag)
        self.user_agent = "python aio rtsp client"
        self.timeout = timeout
        self.log_type = log_type
        self.tick = Tick()
        self.host = parse_result.hostname
        self.port = parse_result.port if parse_result.port else 554
        self.auth_method = ""
        self.auth_params = {}
        self.username = parse_result.username
        self.password = parse_result.password
        if self.username or self.password:
            slash_index = url.find("//")
            at_index = url.find("@")
            self.url = url[:slash_index + 2] + url[at_index + 1:]
        else:
            self.url = url
        self.content_base = ""
        self.local_addr: Optional[SocketAddress] = None
        self.rtsp_version = "RTSP/1.0"
        self.rtsp_version_b = self.rtsp_version.encode("utf-8")
        self.recv_buffer = bytearray()
        self.recv_buf_start = 0
        self.sdp: SDPInfo = {}
        self.cseq = 1
        self.session = ""
        self.session_timeout = 0.0
        self.video_rtp_channel = 0
        self.video_rtcp_channel = 1
        self.audio_rtp_channel = 2
        self.audio_rtcp_channel = 3
        self.media_channels = {
            self.video_rtp_channel: "video_rtp",
            self.video_rtcp_channel: "video_rtcp",
            self.audio_rtp_channel: "audio_rtp",
            self.audio_rtcp_channel: "audio_rtcp",
        }
        self.video_splicer: Optional[Union[H264Splicer, H265Splicer]] = None
        self.audio_splicer = None
        self._pending_methods: dict[int, str] = {}

    async def connect(self) -> None:
        if self.is_connected:
            return
        if self.log_type & RtspClientMsgType.ConnectResult:
            types.logger.info(f'{self.log_tag} connect {self.server_ip}:{self.server_port}')
        self.tick.reset()
        await asyncio.wait_for(super().connect(), timeout=self.timeout)
        if self._sock is not None:
            self.local_addr = self._sock.getsockname()
        if self.log_type & RtspClientMsgType.ConnectResult:
            types.logger.info(f'{self.log_tag} connected local_addr={self.local_addr}')

    def encode_message(self, message: RtspRequest) -> bytes:
        if not isinstance(message, RtspRequest):
            raise TypeError(f'unsupported message type: {type(message)!r}')
        lines = [f'{message.method} {message.uri} {self.rtsp_version}']
        for key, value in message.headers:
            lines.append(f'{key}: {value}')
        data = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8") + message.body
        if self.log_type & RtspClientMsgType.RTSP:
            types.logger.info(f'{self.log_tag} send:\n{data.decode("utf-8", errors="ignore")}')
        return data

    def feed_data(self, data: bytes) -> List[Union[RtspInterleavedData, RtspResponse]]:
        self.recv_buffer.extend(data)
        messages: List[Union[RtspInterleavedData, RtspResponse]] = []
        while True:
            recv_len = len(self.recv_buffer) - self.recv_buf_start
            if recv_len <= 0:
                break
            first_byte = self.recv_buffer[self.recv_buf_start]
            if first_byte == 0x24:
                if recv_len < 4:
                    break
                _, channel, length = struct.unpack("!BBH", self.recv_buffer[self.recv_buf_start:self.recv_buf_start + 4])
                if recv_len < 4 + length:
                    break
                packet_data = bytes(memoryview(self.recv_buffer)[self.recv_buf_start + 4:self.recv_buf_start + 4 + length])
                self.recv_buf_start += 4 + length
                if channel in (self.video_rtp_channel, self.audio_rtp_channel):
                    packet = self.parse_rtp(packet_data)
                else:
                    packet = RTCP()
                messages.append(RtspInterleavedData(channel, packet))
                self.shrink_buffer()
                continue
            response = self.try_parse_rtsp_response()
            if response is None:
                break
            messages.append(response)
            self.shrink_buffer()
        return messages

    def get_request_key(self, message: object) -> Optional[int]:
        if isinstance(message, RtspRequest):
            return message.cseq
        return None

    def get_response_key(self, message: object) -> Optional[int]:
        if isinstance(message, RtspResponse):
            cseq = message.headers.get("CSeq")
            if isinstance(cseq, int):
                return cseq
        return None

    def has_pending_method(self, method: str) -> bool:
        return any(pending == method for pending in self._pending_methods.values())

    def _update_session_from_header(self, session_header: Optional[str]) -> None:
        if not session_header:
            return
        parts = [part.strip() for part in str(session_header).split(";") if part.strip()]
        if not parts:
            return
        self.session = parts[0]
        for part in parts[1:]:
            key, _, value = part.partition("=")
            if key.strip().lower() != "timeout":
                continue
            try:
                self.session_timeout = float(value.strip())
            except ValueError:
                pass

    def _make_request(
        self,
        method: str,
        uri: str,
        headers: List[Tuple[str, str]],
        body: bytes = b"",
        media_type: Optional[str] = None,
    ) -> RtspRequest:
        cseq = self.cseq
        self.cseq += 1
        return RtspRequest(method, uri, cseq, headers, body=body, media_type=media_type)

    async def _send_request(self, request: RtspRequest, timeout: Optional[float] = None) -> Optional[RtspResponse]:
        self.tick.update()
        self._pending_methods[request.cseq] = request.method
        try:
            pending = await self.send_msg(
                request,
                timeout=timeout or self.timeout,
                expect_response=True,
                request_key=request.cseq,
            )
            assert pending is not None
            result = await pending.future
        except RequestTimeoutError as ex:
            raise RtspTimeoutError(
                f"RTSP receive timeout while waiting for RTSP response to {request.method} "
                f"(cseq={ex.request_key}, timeout={ex.timeout}s)",
                session_elapsed=self.tick.since_start(),
            ) from ex
        except RequestInterruptedError as ex:
            result = None
        finally:
            self._pending_methods.pop(request.cseq, None)
        if result is not None:
            result.elapsed = self.tick.since_last()
            self._update_session_from_header(result.headers.get("Session", self.session))
        return result

    def check_auth(self, rtsp_resp: RtspResponse) -> None:
        if rtsp_resp.status_code != 401 or not self.username or not self.password:
            return
        auths = rtsp_resp.headers.get("WWW-Authenticate")
        if auths is None:
            return
        if not isinstance(auths, list):
            auths = [auths]
        for auth in auths:
            parts = auth.split(None, 1)
            if parts[0] != "Digest":
                continue
            self.auth_method = "Digest"
            self.auth_params = {}
            if len(parts) == 2:
                for param in parts[1].split(","):
                    param = param.strip()
                    if "=" not in param:
                        continue
                    key, value = param.split("=", 1)
                    if value.startswith('"'):
                        value = value[1:-1]
                    self.auth_params[key] = value

    def generate_auth_header(self, method: str) -> Optional[str]:
        if self.auth_method == "Basic":
            auth = base64.b64encode(f'{self.username}:{self.password}'.encode()).decode()
            return f'Basic {auth}'
        if self.auth_method == "Digest":
            realm = self.auth_params["realm"]
            nonce = self.auth_params["nonce"]
            ha1 = hashlib.md5(f'{self.username}:{realm}:{self.password}'.encode()).hexdigest()
            ha2 = hashlib.md5(f'{method}:{self.url}'.encode()).hexdigest()
            response = hashlib.md5(f'{ha1}:{nonce}:{ha2}'.encode()).hexdigest()
            return f'Digest username="{self.username}", realm="{realm}", nonce="{nonce}", uri="{self.url}", response="{response}"'
        return None

    def _base_headers(
        self,
        method: str,
        include_session: bool = False,
        extra_headers: Optional[List[Tuple[str, str]]] = None,
    ) -> List[Tuple[str, str]]:
        headers = [("CSeq", str(self.cseq)), ("User-Agent", self.user_agent)]
        if include_session and self.session:
            headers.append(("Session", self.session))
        if extra_headers:
            headers.extend(extra_headers)
        auth = self.generate_auth_header(method)
        if auth:
            headers.append(("Authorization", auth))
        return headers

    async def options(self) -> Optional[RtspResponse]:
        return await self._send_request(self._make_request("OPTIONS", self.url, self._base_headers("OPTIONS")))

    async def describe(self) -> Optional[RtspResponse]:
        rtsp_resp = await self._send_request(
            self._make_request("DESCRIBE", self.url,
                               self._base_headers("DESCRIBE", extra_headers=[("Accept", "application/sdp")]))
        )
        if rtsp_resp is not None:
            if rtsp_resp.status_code == 200:
                self._parse_sdp(rtsp_resp)
        return rtsp_resp

    async def setup_media(self, media_type: str) -> Optional[RtspResponse]:
        media_sdp = self.sdp.get(media_type)
        if media_sdp is None:
            return None
        if media_type == "video":
            transport = f'{media_sdp["transport"]}/TCP;unicast;interleaved={self.video_rtp_channel}-{self.video_rtcp_channel}'
            # The first successful SETUP in a session typically negotiates the Session id.
            # Subsequent SETUPs (regardless of media type ordering) should include Session.
            include_session = bool(self.session)
        elif media_type == "audio":
            transport = f'{media_sdp["transport"]}/TCP;unicast;interleaved={self.audio_rtp_channel}-{self.audio_rtcp_channel}'
            # Audio may be the first (or only) SETUP request for audio-only resources.
            # Only include Session header when a session has already been negotiated.
            include_session = bool(self.session)
        else:
            raise ValueError(f'unsupported media_type: {media_type!r}')
        return await self._send_request(
            self._make_request(
                "SETUP",
                media_sdp["control"],
                self._base_headers("SETUP", include_session=include_session, extra_headers=[("Transport", transport)]),
                media_type=media_type,
            )
        )

    async def play(self) -> Optional[RtspResponse]:
        return await self._send_request(
            self._make_request("PLAY", self.url, self._base_headers(
                "PLAY", include_session=True, extra_headers=[("Range", "npt=0.000-")])))

    async def get_parameter(self) -> Optional[RtspResponse]:
        return await self._send_request(
            self._make_request("GET_PARAMETER", self.url, self._base_headers("GET_PARAMETER", include_session=True)))

    async def options_keepalive(self) -> Optional[RtspResponse]:
        return await self._send_request(self._make_request("OPTIONS", self.url, self._base_headers("OPTIONS")))

    async def teardown(self, timeout: float = 0.2) -> Optional[RtspResponse]:
        if not self.is_connected:
            return None
        request = self._make_request("TEARDOWN", self.url, self._base_headers("TEARDOWN", include_session=True))
        return await self._send_request(request, timeout=timeout)

    def try_parse_rtsp_response(self) -> Optional[RtspResponse]:
        if len(self.recv_buffer) == self.recv_buf_start:
            return None
        if not self.recv_buffer.startswith(self.rtsp_version_b, self.recv_buf_start):
            raise RtspProtocolError(
                f'invalid RTSP response start: {bytes(self.recv_buffer[self.recv_buf_start:self.recv_buf_start + 32])!r}',
                session_elapsed=self.tick.since_start(),
            )
        index = self.recv_buffer.find(b"\r\n\r\n", self.recv_buf_start)
        if index <= self.recv_buf_start:
            return None
        head = self.recv_buffer[self.recv_buf_start:index].decode("utf-8")
        resp_lines = head.splitlines()
        status_code = int(resp_lines[0].split()[1])
        headers: HeaderMap = {}
        for line in resp_lines[1:]:
            key, value = line.split(":", 1)
            value = value.lstrip()
            if key in ("CSeq", "Content-Length"):
                value = int(value)
            existing = headers.get(key)
            if existing is None:
                headers[key] = value
            elif isinstance(existing, list):
                existing.append(value)
            else:
                headers[key] = [existing, value]
        body_start = index + 4
        content_len = int(headers.get("Content-Length", 0) or 0)
        if len(self.recv_buffer) - body_start < content_len:
            return None
        body = ""
        if content_len > 0:
            body = self.recv_buffer[body_start:body_start + content_len].decode("utf-8")
        self.recv_buf_start = body_start + content_len
        if self.log_type & RtspClientMsgType.RTSP:
            types.logger.info(f'{self.log_tag} recv:\n{head}\n\n{body}')
        return RtspResponse(status_code, headers, body)

    def parse_rtp(self, rtp_data: bytes) -> RTP:
        version_padding_extension, mark_payload, sequence_number, timestamp, ssrc = struct.unpack("!BBHII", rtp_data[:12])
        version = (version_padding_extension >> 6) & 0b0011
        padding = (version_padding_extension >> 5) & 0x01
        extension = (version_padding_extension >> 4) & 0x01
        csic = version_padding_extension & 0b1111
        marker = mark_payload >> 7
        payload_type = mark_payload & 0b0111_1111
        padding_length = rtp_data[-1] if padding else 0
        payload = rtp_data[12:len(rtp_data) - padding_length]
        rtp = RTP(version, padding, extension, csic, marker, payload_type, sequence_number, timestamp, ssrc, payload)
        rtp.recv_tick = Tick.process_tick()
        return rtp

    def shrink_buffer(self, force: bool = False) -> None:
        if self.recv_buf_start > 102400 or (force and self.recv_buf_start > 0):
            self.recv_buffer = self.recv_buffer[self.recv_buf_start:]
            self.recv_buf_start = 0

    def _parse_sdp(self, rtsp_resp: RtspResponse) -> None:
        self.content_base = rtsp_resp.headers.get("Content-Base", "")
        self.sdp = {}
        media_info = None
        for line in rtsp_resp.body.splitlines():
            if line.startswith("m="):
                space_index = line.find(" ")
                media_type = line[2:space_index]
                self.sdp[media_type] = {}
                media_info = self.sdp[media_type]
                parts = line[space_index + 1:].split()
                media_info["transport"] = parts[1]
                media_info["payload"] = int(parts[2]) if parts[2].isdigit() else parts[2]
            elif line.startswith("a=control:"):
                control_url = line[10:]
                if control_url == "*":
                    control_url = self.content_base
                elif not control_url.startswith("rtsp://"):
                    control_url = f'{self.content_base}{control_url}'
                if media_info is None:
                    self.sdp["control"] = control_url
                else:
                    media_info["control"] = control_url
            elif line.startswith("a=rtpmap:"):
                parts = line[9:].split()
                media_info["rtpmap"] = int(parts[0])
                parts = parts[1].split("/")
                media_info["codec_name"] = parts[0]
                media_info["clock_rate"] = int(parts[1])
                if len(parts) == 3:
                    media_info["channel"] = int(parts[2])
                if media_info["codec_name"] == "H264":
                    self.video_splicer = H264Splicer(self.log_type, self.log_tag)
                elif media_info["codec_name"] == "H265":
                    self.video_splicer = H265Splicer(self.log_type, self.log_tag)
            elif line.startswith("a=fmtp:"):
                fmtp = {}
                for item in line[line.find(" "):].split(";"):
                    item = item.strip()
                    if not item or "=" not in item:
                        continue
                    key, value = item.split("=", 1)
                    key = key.lower()
                    if value.isdigit() and key not in ("config", "profile-level-id", "mode"):
                        value = int(value)
                    fmtp[key] = value
                media_info["fmtp"] = fmtp
                if isinstance(self.video_splicer, H265Splicer) and fmtp.get("sprop-max-don-diff", 0) > 0:
                    self.video_splicer.has_donl_field = True
            elif line.startswith("a=framerate:"):
                media_info["framerate"] = int(line[12:])
        video_sdp = self.sdp.get("video")
        fmtp = video_sdp.get("fmtp") if video_sdp else None
        if fmtp and video_sdp:
            sps_pps = fmtp.get("sprop-parameter-sets")
            if sps_pps:
                sps, pps = sps_pps.split(",")
                video_sdp["sps"] = ensure_start_code(base64.b64decode(sps))
                video_sdp["pps"] = ensure_start_code(base64.b64decode(pps))
        audio_sdp = self.sdp.get("audio")
        if audio_sdp and "codec_name" not in audio_sdp:
            payload = audio_sdp.get("payload")
            if payload == 8:
                audio_sdp["codec_name"] = "PCMA"
                audio_sdp["clock_rate"] = 8000
                audio_sdp["channel"] = 1
            elif payload == 0:
                audio_sdp["codec_name"] = "PCMU"
                audio_sdp["clock_rate"] = 8000
                audio_sdp["channel"] = 1
        self.audio_splicer = create_audio_splicer(audio_sdp, self.log_type, self.log_tag)
        rtsp_resp.sdp = self.sdp
