"""Async RTSP/TCP file server.

Serves local media files over RTSP interleaved TCP. Container video/audio tracks
are streamed directly when supported, and WAV input is resampled and encoded as
PCMA 8 kHz mono before being advertised and sent to RTSP clients."""

import asyncio
import base64
import binascii
import concurrent.futures
import contextlib
import random
import struct
import threading
import time
import traceback
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aio_sockets as aio
import av


H264CodecName = 'h264'
H265CodecName = 'h265'

ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".wav", ".aac"}
DEFAULT_USER_AGENT = "python aio rtsp server"
MAX_RTP_PAYLOAD = 1400
logger: aio.LoggerLike = aio.StdoutLogger()


def _is_disconnect_error(ex: BaseException) -> bool:
    return isinstance(ex, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError))


def format_date_header() -> str:
    return datetime.now(timezone.utc).strftime("%a, %b %d %Y %H:%M:%S GMT")


def normalize_codec_name(name: str) -> str:
    name = (name or "").lower()
    if name in ("h265", "hevc"):
        return H265CodecName
    if name in ("h264", "avc"):
        return H264CodecName
    if name in ("aac", "mpeg4-generic"):
        return "mpeg4-generic"
    return name


def split_annexb_nalus(data: bytes) -> List[bytes]:
    nalus: List[bytes] = []
    i = 0
    start = -1
    while i < len(data) - 3:
        if data[i:i + 3] == b"\x00\x00\x01":
            if start >= 0 and start < i:
                nalus.append(data[start:i])
            start = i + 3
            i += 3
            continue
        if i < len(data) - 4 and data[i:i + 4] == b"\x00\x00\x00\x01":
            if start >= 0 and start < i:
                nalus.append(data[start:i])
            start = i + 4
            i += 4
            continue
        i += 1
    if start >= 0 and start < len(data):
        nalus.append(data[start:])
    return [nalu for nalu in nalus if nalu]


def split_length_prefixed_nalus(data: bytes, nal_length_size: int) -> List[bytes]:
    nalus: List[bytes] = []
    offset = 0
    while offset + nal_length_size <= len(data):
        size = int.from_bytes(data[offset:offset + nal_length_size], "big")
        offset += nal_length_size
        if size <= 0 or offset + size > len(data):
            break
        nalus.append(data[offset:offset + size])
        offset += size
    return nalus


def parse_h264_avcc(extradata: bytes) -> Tuple[int, List[bytes], List[bytes]]:
    if len(extradata) < 7 or extradata[0] != 1:
        return 4, [], []
    nal_length_size = (extradata[4] & 0x03) + 1
    offset = 5
    sps_list: List[bytes] = []
    pps_list: List[bytes] = []
    sps_count = extradata[offset] & 0x1F
    offset += 1
    for _ in range(sps_count):
        if offset + 2 > len(extradata):
            return nal_length_size, sps_list, pps_list
        size = int.from_bytes(extradata[offset:offset + 2], "big")
        offset += 2
        sps_list.append(extradata[offset:offset + size])
        offset += size
    if offset >= len(extradata):
        return nal_length_size, sps_list, pps_list
    pps_count = extradata[offset]
    offset += 1
    for _ in range(pps_count):
        if offset + 2 > len(extradata):
            return nal_length_size, sps_list, pps_list
        size = int.from_bytes(extradata[offset:offset + 2], "big")
        offset += 2
        pps_list.append(extradata[offset:offset + size])
        offset += size
    return nal_length_size, sps_list, pps_list


def parse_h265_hvcc(extradata: bytes) -> Tuple[int, List[bytes], List[bytes], List[bytes]]:
    if len(extradata) < 23 or extradata[0] != 1:
        return 4, [], [], []
    nal_length_size = (extradata[21] & 0x03) + 1
    num_arrays = extradata[22]
    offset = 23
    vps_list: List[bytes] = []
    sps_list: List[bytes] = []
    pps_list: List[bytes] = []
    for _ in range(num_arrays):
        if offset + 3 > len(extradata):
            break
        nal_type = extradata[offset] & 0x3F
        count = int.from_bytes(extradata[offset + 1:offset + 3], "big")
        offset += 3
        for _ in range(count):
            if offset + 2 > len(extradata):
                break
            size = int.from_bytes(extradata[offset:offset + 2], "big")
            offset += 2
            if offset + size > len(extradata):
                break
            nalu = extradata[offset:offset + size]
            offset += size
            if nal_type == 32:
                vps_list.append(nalu)
            elif nal_type == 33:
                sps_list.append(nalu)
            elif nal_type == 34:
                pps_list.append(nalu)
    return nal_length_size, vps_list, sps_list, pps_list


def parse_parameter_sets(codec_name: str, extradata: bytes) -> Tuple[int, Dict[str, bytes]]:
    codec_name = normalize_codec_name(codec_name)
    nal_length_size = 4
    params: Dict[str, bytes] = {}
    if not extradata:
        return nal_length_size, params
    if codec_name == H264CodecName:
        if extradata.startswith(b"\x00\x00\x00\x01") or extradata.startswith(b"\x00\x00\x01"):
            nalus = split_annexb_nalus(extradata)
            for nalu in nalus:
                nal_type = nalu[0] & 0x1F
                if nal_type == 7 and "sps" not in params:
                    params["sps"] = nalu
                elif nal_type == 8 and "pps" not in params:
                    params["pps"] = nalu
        else:
            nal_length_size, sps_list, pps_list = parse_h264_avcc(extradata)
            if sps_list:
                params["sps"] = sps_list[0]
            if pps_list:
                params["pps"] = pps_list[0]
    elif codec_name == H265CodecName:
        if extradata.startswith(b"\x00\x00\x00\x01") or extradata.startswith(b"\x00\x00\x01"):
            nalus = split_annexb_nalus(extradata)
            for nalu in nalus:
                nal_type = (nalu[0] >> 1) & 0x3F
                if nal_type == 32 and "vps" not in params:
                    params["vps"] = nalu
                elif nal_type == 33 and "sps" not in params:
                    params["sps"] = nalu
                elif nal_type == 34 and "pps" not in params:
                    params["pps"] = nalu
        else:
            nal_length_size, vps_list, sps_list, pps_list = parse_h265_hvcc(extradata)
            if vps_list:
                params["vps"] = vps_list[0]
            if sps_list:
                params["sps"] = sps_list[0]
            if pps_list:
                params["pps"] = pps_list[0]
    return nal_length_size, params


def extract_nalus(codec_name: str, payload: bytes, nal_length_size: int) -> List[bytes]:
    if not payload:
        return []
    if normalize_codec_name(codec_name) in (H264CodecName, H265CodecName):
        nalus = split_length_prefixed_nalus(payload, nal_length_size)
        if nalus:
            return nalus
    if payload.startswith(b"\x00\x00\x00\x01") or payload.startswith(b"\x00\x00\x01"):
        return split_annexb_nalus(payload)
    return [payload]


def profile_level_id_from_sps(sps: bytes) -> str:
    if len(sps) < 4:
        return "000000"
    return sps[1:4].hex()


def to_base64(nalu: bytes) -> str:
    return base64.b64encode(nalu).decode("ascii")


def _iter_audio_frames(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def adts_to_asc(data: bytes) -> bytes:
    if len(data) < 7 or data[0] != 0xFF or (data[1] & 0xF0) != 0xF0:
        return b""
    profile = ((data[2] >> 6) & 0x03) + 1
    sample_rate_index = (data[2] >> 2) & 0x0F
    channels = ((data[2] & 0x01) << 2) | ((data[3] >> 6) & 0x03)
    value = ((profile & 0x1F) << 11) | ((sample_rate_index & 0x0F) << 7) | ((channels & 0x0F) << 3)
    return value.to_bytes(2, "big")


@dataclass
class MediaTrackInfo:
    media_type: str
    control: str
    codec_name: str
    payload_type: int
    clock_rate: int
    channels: int = 1
    fmtp: Optional[str] = None
    av_stream_index: Optional[int] = None
    nal_length_size: int = 4
    parameter_sets: Dict[str, bytes] = field(default_factory=dict)
    source_kind: str = "packet"


@dataclass
class MediaSource:
    file_path: Path
    tracks: List[MediaTrackInfo]
    session_name: str

    def build_sdp(self) -> str:
        lines = [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            f's=Session streamed by "{self.session_name}"',
            "t=0 0",
            "a=control:*",
            "a=range:npt=0-",
        ]
        for track in self.tracks:
            lines.append(f"m={track.media_type} 0 RTP/AVP {track.payload_type}")
            lines.append("c=IN IP4 0.0.0.0")
            if track.media_type == "video":
                lines.append(f"a=rtpmap:{track.payload_type} {track.codec_name.upper()}/{track.clock_rate}")
            elif track.codec_name == "mpeg4-generic":
                lines.append(
                    f"a=rtpmap:{track.payload_type} mpeg4-generic/{track.clock_rate}/{track.channels}"
                )
            elif track.codec_name == "pcma":
                lines.append("a=rtpmap:8 PCMA/8000/1")
            else:
                lines.append(
                    f"a=rtpmap:{track.payload_type} {track.codec_name.upper()}/{track.clock_rate}/{track.channels}"
                )
            if track.fmtp:
                lines.append(f"a=fmtp:{track.payload_type} {track.fmtp}")
            lines.append(f"a=control:{track.control}")
        return "\r\n".join(lines) + "\r\n"


@dataclass
class StreamTrackState:
    info: MediaTrackInfo
    rtp_channel: Optional[int] = None
    rtcp_channel: Optional[int] = None
    sequence_number: int = 1
    ssrc: int = field(default_factory=lambda: random.getrandbits(32))
    first_source_time: Optional[float] = None
    first_send_time: Optional[float] = None
    next_send_time: Optional[float] = None
    sent_codec_config: bool = False
    sample_cursor: int = 0
    logged_first_frame: bool = False
    media_time_offset: float = 0.0
    packet_time_cursor: float = 0.0
    packet_count: int = 0
    octet_count: int = 0
    last_rtp_timestamp: int = 0
    last_rtp_send_time: Optional[float] = None
    last_rtcp_send_time: Optional[float] = None


@dataclass
class QueuedMediaPacket:
    kind: str
    track_control: str
    data: bytes = b""
    media_time: float = 0.0
    duration_seconds: float = 0.0
    is_keyframe: bool = False
    sample_cursor: Optional[int] = None


@dataclass
class QueuedLoopState:
    kind: str
    track_control: str
    media_time_offset: float = 0.0
    packet_time_cursor: float = 0.0
    sample_cursor: Optional[int] = None


@dataclass
class QueuedWorkerSignal:
    kind: str
    message: str = ""


class RtspServerError(Exception):
    pass


class RtspBadRequestError(ValueError):
    pass


class RtspServerSession:
    def __init__(self, server: "RtspServer", sock: aio.TCPSocket):
        self.server = server
        self.sock = sock
        self.peername = sock.getpeername()
        self.session_id = f"{random.getrandbits(32):08X}"
        self.write_lock = asyncio.Lock()
        self.media_source: Optional[MediaSource] = None
        self.content_base: str = ""
        self.track_states: Dict[str, StreamTrackState] = {}
        self.stream_task: Optional[asyncio.Task] = None
        self.timeout_task: Optional[asyncio.Task] = None
        self.closed = False
        self.play_count = 0
        self.session_active = False
        self.last_activity_monotonic = time.monotonic()

    async def run(self) -> None:
        try:
            self.timeout_task = asyncio.create_task(
                self._watch_session_timeout(),
                name=f"rtsp-timeout-{self.session_id}",
            )
            while not self.closed:
                try:
                    request = await self._read_request()
                except RtspBadRequestError as ex:
                    logger.warning(f"{self.peername} bad request: {ex}")
                    with contextlib.suppress(Exception):
                        await self._send_response({}, 400, "Bad Request", body=str(ex).encode("utf-8"))
                    continue
                if request is None:
                    break
                method, uri, version, headers, body = request
                self._mark_activity()
                logger.info(f"{self.peername} {method} {uri}")
                try:
                    if method == "OPTIONS":
                        await self._handle_options(headers)
                    elif method == "DESCRIBE":
                        await self._handle_describe(uri, headers)
                    elif method == "SETUP":
                        await self._handle_setup(uri, headers)
                    elif method == "PLAY":
                        await self._handle_play(headers)
                    elif method == "PAUSE":
                        if not self._require_valid_session(headers):
                            await self._send_response(headers, 454, "Session Not Found")
                            continue
                        await self._stop_streaming()
                        await self._send_response(headers, 200, "OK", {"Session": self._session_header_value()})
                    elif method == "GET_PARAMETER":
                        if not self._require_valid_session(headers):
                            await self._send_response(headers, 454, "Session Not Found")
                            continue
                        await self._send_response(headers, 200, "OK", {"Session": self._session_header_value()})
                    elif method == "TEARDOWN":
                        if not self._require_valid_session(headers):
                            await self._send_response(headers, 454, "Session Not Found")
                            continue
                        await self._stop_streaming()
                        await self._send_response(headers, 200, "OK", {"Session": self._session_header_value()})
                        break
                    else:
                        await self._send_response(headers, 405, "Method Not Allowed")
                except FileNotFoundError as ex:
                    logger.warning(f"{self.peername} {method} not found: {ex}")
                    await self._send_response(headers, 404, "Not Found")
                except ValueError as ex:
                    logger.warning(f"{self.peername} {method} bad request: {ex}")
                    await self._send_response(headers, 400, "Bad Request", body=str(ex).encode("utf-8"))
                except RtspServerError as ex:
                    logger.warning(f"{self.peername} {method} media error: {ex}")
                    await self._send_response(headers, 415, "Unsupported Media", body=str(ex).encode("utf-8"))
                except Exception as ex:
                    if _is_disconnect_error(ex):
                        break
                    logger.error(f"{self.peername} {method} error: {ex!r}\n{traceback.format_exc()}")
                    with contextlib.suppress(Exception):
                        await self._send_response(headers, 500, "Internal Server Error")
        finally:
            await self.close()

    async def close(self) -> None:
        if self.closed:
            return
        if self.timeout_task is not None:
            current_task = asyncio.current_task()
            if self.timeout_task is not current_task:
                self.timeout_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self.timeout_task
            self.timeout_task = None
        await self._stop_streaming()
        await self._send_rtcp_bye()
        await self._close_transport()

    async def _close_transport(self) -> None:
        if self.closed:
            return
        self.closed = True
        logger.info(f"{self.peername} disconnected session={self.session_id}")
        with contextlib.suppress(BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            await self.sock.close()

    async def _read_request(self):
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
            return method.upper(), uri, version, headers, body
        return None

    def _require_valid_session(self, req_headers: Dict[str, str]) -> bool:
        session_header = req_headers.get("session", "")
        if not session_header:
            return False
        return session_header.split(";", 1)[0] == self.session_id

    def _mark_activity(self) -> None:
        self.last_activity_monotonic = time.monotonic()

    def _session_header_value(self) -> str:
        return f"{self.session_id};timeout={self.server.session_timeout}"

    async def _watch_session_timeout(self) -> None:
        interval = max(1.0, min(5.0, self.server.session_timeout / 2.0))
        while not self.closed:
            await asyncio.sleep(interval)
            if self.closed or not self.session_active:
                continue
            idle_seconds = time.monotonic() - self.last_activity_monotonic
            if idle_seconds < self.server.session_timeout:
                continue
            logger.info(
                f"{self.peername} session timeout session={self.session_id} "
                f"idle={idle_seconds:.1f}s timeout={self.server.session_timeout}s"
            )
            await self.close()
            return

    async def _handle_options(self, req_headers: Dict[str, str]) -> None:
        await self._send_response(
            req_headers,
            200,
            "OK",
            {"Public": "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN, GET_PARAMETER"},
        )

    async def _handle_describe(self, uri: str, req_headers: Dict[str, str]) -> None:
        resource_path = self.server.resolve_request_path(uri)
        self.play_count = self.server.parse_play_count(uri)
        self.media_source = self.server.load_media_source(resource_path)
        self.content_base = self.server.build_content_base(uri, req_headers)
        self.track_states = {track.control: StreamTrackState(track) for track in self.media_source.tracks}
        track_summary = ", ".join(
            f"{track.control}:{track.media_type}/{track.codec_name}/{track.clock_rate}"
            for track in self.media_source.tracks
        )
        logger.info(
            f"{self.peername} describe session={self.session_id} source={resource_path.name} "
            f"play_count={self.play_count} tracks={track_summary}"
        )
        sdp = self.media_source.build_sdp()
        await self._send_response(
            req_headers,
            200,
            "OK",
            {
                "Content-Base": self.content_base,
                "Content-Type": "application/sdp",
            },
            body=sdp.encode("utf-8"),
        )

    async def _handle_setup(self, uri: str, req_headers: Dict[str, str]) -> None:
        if self.media_source is None:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        session_header = req_headers.get("session")
        if session_header and session_header.split(";", 1)[0] != self.session_id:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        track_name = self.server.resolve_track_name(uri)
        state = self.track_states.get(track_name)
        if state is None:
            await self._send_response(req_headers, 404, "Not Found")
            return
        transport = req_headers.get("transport", "")
        channels = self.server.parse_interleaved_channels(transport)
        if channels is None:
            await self._send_response(req_headers, 461, "Unsupported Transport")
            return
        if channels[0] == channels[1]:
            await self._send_response(req_headers, 400, "Bad Request", body=b"interleaved RTP and RTCP channels must differ")
            return
        for other_state in self.track_states.values():
            if other_state.info.control == state.info.control:
                continue
            if channels[0] in (other_state.rtp_channel, other_state.rtcp_channel) or channels[1] in (
                other_state.rtp_channel,
                other_state.rtcp_channel,
            ):
                await self._send_response(req_headers, 400, "Bad Request", body=b"interleaved channels already in use")
                return
        state.rtp_channel, state.rtcp_channel = channels
        self.session_active = True
        self._mark_activity()
        logger.info(
            f"{self.peername} setup track={state.info.control} codec={state.info.codec_name} "
            f"rtp_channel={channels[0]} rtcp_channel={channels[1]}"
        )
        await self._send_response(
            req_headers,
            200,
            "OK",
            {
                "Transport": f"RTP/AVP/TCP;unicast;interleaved={channels[0]}-{channels[1]}",
                "Session": self._session_header_value(),
            },
        )

    async def _handle_play(self, req_headers: Dict[str, str]) -> None:
        if self.media_source is None:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        session_header = req_headers.get("session", "")
        if session_header.split(";", 1)[0] != self.session_id:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        if not any(state.rtp_channel is not None for state in self.track_states.values()):
            await self._send_response(req_headers, 455, "Method Not Valid In This State")
            return
        await self._stop_streaming()
        self._reset_stream_state()
        rtp_info = []
        for state in self.track_states.values():
            if state.rtp_channel is None:
                continue
            rtp_info.append(f"url={self.content_base}{state.info.control};seq=1;rtptime=0")
        await self._send_response(
            req_headers,
            200,
            "OK",
            {
                "Session": self._session_header_value(),
                "Range": "npt=0.000-",
                "RTP-Info": ",".join(rtp_info),
            },
        )
        active_tracks = [
            f"{state.info.control}:{state.info.codec_name}/rtp={state.rtp_channel}/rtcp={state.rtcp_channel}"
            for state in self.track_states.values()
            if state.rtp_channel is not None
        ]
        logger.info(
            f"{self.peername} play session={self.session_id} play_count={self.play_count} "
            f"tracks={', '.join(active_tracks)}"
        )
        self.stream_task = asyncio.create_task(self._stream_media(), name=f"rtsp-stream-{self.session_id}")

    def _reset_stream_state(self) -> None:
        for state in self.track_states.values():
            state.sequence_number = 1
            state.ssrc = random.getrandbits(32)
            state.first_source_time = None
            state.first_send_time = None
            state.next_send_time = None
            state.sent_codec_config = False
            state.sample_cursor = 0
            state.logged_first_frame = False
            state.media_time_offset = 0.0
            state.packet_time_cursor = 0.0
            state.packet_count = 0
            state.octet_count = 0
            state.last_rtp_timestamp = 0
            state.last_rtp_send_time = None
            state.last_rtcp_send_time = None

    async def _stop_streaming(self) -> None:
        if self.stream_task is None:
            return
        self.stream_task.cancel()
        try:
            await self.stream_task
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            logger.warning(f"{self.peername} stream task stopped with error: {ex!r}")
        self.stream_task = None

    async def _stream_media(self) -> None:
        suffix = self.media_source.file_path.suffix.lower()
        remaining_plays = self.play_count
        loop_index = 0
        while not self.closed:
            try:
                loop_index += 1
                logger.info(
                    f"{self.peername} stream loop start session={self.session_id} "
                    f"loop={loop_index} source={self.media_source.file_path.name}"
                )
                await self._stream_media_once(suffix)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                if _is_disconnect_error(ex):
                    break
                logger.error(f"{self.peername} stream error: {ex!r}\n{traceback.format_exc()}")
                break
            logger.info(
                f"{self.peername} stream loop end session={self.session_id} "
                f"loop={loop_index} source={self.media_source.file_path.name}"
            )
            if remaining_plays > 0:
                remaining_plays -= 1
                if remaining_plays == 0:
                    break
        if not self.closed and self.play_count > 0 and remaining_plays == 0:
            logger.info(f"{self.peername} play_count exhausted, closing session={self.session_id}")
            await self.close()

    async def _stream_media_once(self, suffix: str) -> None:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        stop_event = threading.Event()
        producer = asyncio.create_task(
            aio.to_thread(self._produce_media_packets, suffix, loop, queue, stop_event)
        )
        try:
            while True:
                item = await queue.get()
                if isinstance(item, QueuedWorkerSignal):
                    if item.kind == "done":
                        break
                    if item.kind == "error":
                        raise RtspServerError(item.message)
                    continue
                if isinstance(item, QueuedLoopState):
                    state = self.track_states.get(item.track_control)
                    if state is None:
                        continue
                    state.media_time_offset = item.media_time_offset
                    state.packet_time_cursor = item.packet_time_cursor
                    state.first_source_time = None
                    if item.sample_cursor is not None:
                        state.sample_cursor = item.sample_cursor
                    continue
                if not isinstance(item, QueuedMediaPacket):
                    continue
                state = self.track_states.get(item.track_control)
                if state is None or state.rtp_channel is None:
                    continue
                if item.kind == "video":
                    await self._send_video_packet(
                        state,
                        item.data,
                        item.media_time,
                        item.duration_seconds,
                        item.is_keyframe,
                    )
                elif item.kind == "aac":
                    await self._send_aac_packet(state, item.data, item.media_time, item.duration_seconds)
                elif item.kind == "pcma":
                    await self._send_raw_audio_packet(state, item.data, item.media_time, item.duration_seconds)
                elif item.kind == "wav_pcma":
                    await self._send_wav_audio_packet(state, item.data, item.sample_cursor or 0)
            await producer
        except asyncio.CancelledError:
            stop_event.set()
            raise
        finally:
            stop_event.set()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await producer

    def _threadsafe_queue_put(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        item,
        stop_event: threading.Event,
    ) -> bool:
        while not stop_event.is_set():
            future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            try:
                future.result(timeout=0.5)
                return True
            except concurrent.futures.TimeoutError:
                future.cancel()
                continue
            except Exception:
                future.cancel()
                return False
        return False

    def _produce_media_packets(
        self,
        suffix: str,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        try:
            if suffix == ".wav":
                self._produce_wav_packets(loop, queue, stop_event)
            else:
                self._produce_container_packets(loop, queue, stop_event)
        except Exception as ex:
            if not stop_event.is_set():
                self._threadsafe_queue_put(
                    loop,
                    queue,
                    QueuedWorkerSignal(kind="error", message=str(ex)),
                    stop_event,
                )
        finally:
            if not stop_event.is_set():
                self._threadsafe_queue_put(loop, queue, QueuedWorkerSignal(kind="done"), stop_event)

    def _produce_container_packets(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        stream_states = {
            state.info.av_stream_index: state
            for state in self.track_states.values()
            if state.rtp_channel is not None and state.info.av_stream_index is not None
        }
        if not stream_states:
            return
        loop_end_times: Dict[int, float] = {}
        first_packet_times: Dict[int, float] = {}
        with av.open(str(self.media_source.file_path), mode="r") as container:
            selected_streams = [container.streams[index] for index in stream_states]
            for packet in container.demux(selected_streams):
                if self.closed or stop_event.is_set():
                    return
                state = stream_states.get(packet.stream.index)
                if state is None or packet.size <= 0:
                    continue
                pts = packet.pts if packet.pts is not None else packet.dts
                if pts is None:
                    continue
                packet_time = float(pts * packet.time_base)
                duration_value = packet.duration
                if duration_value is None or duration_value <= 0:
                    average_rate = getattr(packet.stream, "average_rate", None) if state.info.media_type == "video" else None
                    if average_rate:
                        duration_seconds = float(1 / average_rate)
                    elif state.info.media_type == "audio" and state.info.codec_name == "mpeg4-generic":
                        duration_seconds = 1024 / float(state.info.clock_rate or 1)
                    else:
                        duration_seconds = 0.02
                else:
                    duration_seconds = float(duration_value * packet.time_base)
                first_packet_time = first_packet_times.setdefault(packet.stream.index, packet_time)
                relative_packet_time = max(0.0, packet_time - first_packet_time)
                media_time = state.media_time_offset + relative_packet_time
                if state.info.media_type == "video":
                    schedule_time = max(state.packet_time_cursor, media_time)
                    state.packet_time_cursor = schedule_time + max(duration_seconds, 0.0)
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.packet_time_cursor,
                    )
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="video",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=schedule_time,
                            duration_seconds=duration_seconds,
                            is_keyframe=bool(packet.is_keyframe),
                        ),
                        stop_event,
                    ):
                        return
                elif state.info.codec_name == "mpeg4-generic":
                    state.packet_time_cursor = max(state.packet_time_cursor, media_time + max(duration_seconds, 0.0))
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.packet_time_cursor,
                    )
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="aac",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=media_time,
                            duration_seconds=duration_seconds,
                        ),
                        stop_event,
                    ):
                        return
                elif state.info.codec_name == "pcma":
                    state.packet_time_cursor = max(state.packet_time_cursor, media_time + max(duration_seconds, 0.0))
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.packet_time_cursor,
                    )
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="pcma",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=media_time,
                            duration_seconds=duration_seconds,
                        ),
                        stop_event,
                    ):
                        return
        for stream_index, state in stream_states.items():
            end_time = loop_end_times.get(stream_index)
            if end_time is not None:
                if not self._threadsafe_queue_put(
                    loop,
                    queue,
                    QueuedLoopState(
                        kind="loop_state",
                        track_control=state.info.control,
                        media_time_offset=end_time,
                        packet_time_cursor=end_time,
                    ),
                    stop_event,
                ):
                    return

    def _produce_wav_packets(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        state = next((it for it in self.track_states.values() if it.rtp_channel is not None), None)
        if state is None:
            return
        packet_samples = 160
        resampler = av.AudioResampler(format="s16", layout="mono", rate=8000)
        encoder = av.CodecContext.create("pcm_alaw", "w")
        encoder.sample_rate = 8000
        encoder.layout = "mono"
        encoder.format = "s16"
        sample_cursor = state.sample_cursor
        with av.open(str(self.media_source.file_path), mode="r") as container:
            audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
            if audio_stream is None:
                raise RtspServerError("No audio stream found in wav file")
            for packet in container.demux([audio_stream]):
                if self.closed or stop_event.is_set():
                    return
                for decoded_frame in packet.decode():
                    for resampled_frame in _iter_audio_frames(resampler.resample(decoded_frame)):
                        for encoded_packet in encoder.encode(resampled_frame):
                            sample_cursor = self._enqueue_wav_chunks(
                                loop,
                                queue,
                                stop_event,
                                state.info.control,
                                bytes(encoded_packet),
                                packet_samples,
                                sample_cursor,
                            )
                            if sample_cursor < 0:
                                return
            for resampled_frame in _iter_audio_frames(resampler.resample(None)):
                for encoded_packet in encoder.encode(resampled_frame):
                    sample_cursor = self._enqueue_wav_chunks(
                        loop,
                        queue,
                        stop_event,
                        state.info.control,
                        bytes(encoded_packet),
                        packet_samples,
                        sample_cursor,
                    )
                    if sample_cursor < 0:
                        return
            for encoded_packet in encoder.encode(None):
                sample_cursor = self._enqueue_wav_chunks(
                    loop,
                    queue,
                    stop_event,
                    state.info.control,
                    bytes(encoded_packet),
                    packet_samples,
                    sample_cursor,
                )
                if sample_cursor < 0:
                    return
        self._threadsafe_queue_put(
            loop,
            queue,
            QueuedLoopState(
                kind="loop_state",
                track_control=state.info.control,
                sample_cursor=sample_cursor,
            ),
            stop_event,
        )

    def _enqueue_wav_chunks(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
        track_control: str,
        encoded: bytes,
        packet_samples: int,
        sample_cursor: int,
    ) -> int:
        offset = 0
        while offset < len(encoded):
            chunk = encoded[offset:offset + packet_samples]
            if not chunk:
                break
            offset += len(chunk)
            next_cursor = sample_cursor + len(chunk)
            if not self._threadsafe_queue_put(
                loop,
                queue,
                QueuedMediaPacket(
                    kind="wav_pcma",
                    track_control=track_control,
                    data=chunk,
                    sample_cursor=sample_cursor,
                ),
                stop_event,
            ):
                return -1
            sample_cursor = next_cursor
        return sample_cursor

    async def _send_video_packet(
        self,
        state: StreamTrackState,
        data: bytes,
        media_time: float,
        duration_seconds: float,
        is_keyframe: bool,
    ) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        codec_name = normalize_codec_name(state.info.codec_name)
        nalus = extract_nalus(codec_name, data, state.info.nal_length_size)
        if not nalus:
            return
        self._log_first_frame(state, timestamp, len(data))
        rtp_payloads: List[Tuple[bytes, int]] = []
        if is_keyframe and state.info.parameter_sets:
            for name in ("vps", "sps", "pps"):
                nalu = state.info.parameter_sets.get(name)
                if nalu:
                    rtp_payloads.extend(self._packetize_video_nalu(nalu, marker=0, codec_name=codec_name))
            state.sent_codec_config = True
        elif not state.sent_codec_config and state.info.parameter_sets:
            for name in ("vps", "sps", "pps"):
                nalu = state.info.parameter_sets.get(name)
                if nalu:
                    rtp_payloads.extend(self._packetize_video_nalu(nalu, marker=0, codec_name=codec_name))
            state.sent_codec_config = True
        for index, nalu in enumerate(nalus):
            marker = 1 if index == len(nalus) - 1 else 0
            rtp_payloads.extend(self._packetize_video_nalu(nalu, marker=marker, codec_name=codec_name))
        await self._send_paced_rtp_payloads(state, rtp_payloads, timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_aac_packet(self, state: StreamTrackState, data: bytes, media_time: float, duration_seconds: float) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        au_header = (len(data) << 3).to_bytes(2, "big")
        payload = struct.pack("!H", 16) + au_header + data
        await self._send_paced_rtp_payloads(state, [(payload, 1)], timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_raw_audio_packet(self, state: StreamTrackState, data: bytes, media_time: float, duration_seconds: float) -> None:
        timestamp = int(round(media_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        await self._send_paced_rtp_payloads(state, [(data, 1)], timestamp, media_time, max(duration_seconds, 0.0))

    async def _send_wav_audio_packet(self, state: StreamTrackState, data: bytes, sample_cursor: int) -> None:
        self._log_first_frame(state, sample_cursor, len(data))
        media_time = sample_cursor / float(state.info.clock_rate or 1)
        await self._sleep_until_media_time(state, media_time)
        await self._send_rtp_packet(state, data, timestamp=sample_cursor, marker=1)

    def _relative_source_time(self, state: StreamTrackState, value: float) -> float:
        if state.first_source_time is None:
            state.first_source_time = value
            return state.media_time_offset
        return state.media_time_offset + max(0.0, value - state.first_source_time)

    def _log_first_frame(self, state: StreamTrackState, timestamp: int, size: int) -> None:
        if state.logged_first_frame:
            return
        state.logged_first_frame = True
        logger.info(
            f"{self.peername} first {state.info.media_type} frame "
            f"codec={state.info.codec_name} track={state.info.control} "
            f"timestamp={timestamp} size={size}"
        )

    async def _sleep_until_media_time(self, state: StreamTrackState, media_time: float) -> None:
        now = time.perf_counter()
        if state.first_send_time is None:
            state.first_send_time = now
        target = state.first_send_time + max(0.0, media_time)
        if state.next_send_time is None:
            state.next_send_time = target
        else:
            state.next_send_time = max(state.next_send_time, target)
        delay = state.next_send_time - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def _send_paced_rtp_payloads(
        self,
        state: StreamTrackState,
        payloads: List[Tuple[bytes, int]],
        timestamp: int,
        media_time: float,
        duration_seconds: float,
    ) -> None:
        if not payloads:
            return
        await self._sleep_until_media_time(state, media_time)
        count = len(payloads)
        gap = duration_seconds / count if count > 1 and duration_seconds > 0 else 0.0
        for index, (payload, marker) in enumerate(payloads):
            if index > 0 and gap > 0:
                next_target = (state.next_send_time or time.perf_counter()) + gap
                delay = next_target - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                state.next_send_time = next_target
            await self._send_rtp_packet(state, payload, timestamp=timestamp, marker=marker)

    def _packetize_video_nalu(
        self,
        nalu: bytes,
        marker: int,
        codec_name: str,
    ) -> List[Tuple[bytes, int]]:
        payloads: List[Tuple[bytes, int]] = []
        if len(nalu) <= MAX_RTP_PAYLOAD:
            return [(nalu, marker)]
        if codec_name == H264CodecName:
            payloads.extend(self._packetize_h264_fu_a(nalu, marker))
        elif codec_name == H265CodecName:
            payloads.extend(self._packetize_h265_fu(nalu, marker))
        return payloads

    def _packetize_h264_fu_a(self, nalu: bytes, marker: int) -> List[Tuple[bytes, int]]:
        nal_header = nalu[0]
        fu_indicator = (nal_header & 0xE0) | 28
        nal_type = nal_header & 0x1F
        payload = memoryview(nalu)[1:]
        max_fragment = MAX_RTP_PAYLOAD - 2
        offset = 0
        packets: List[Tuple[bytes, int]] = []
        while offset < len(payload):
            size = min(max_fragment, len(payload) - offset)
            end = offset + size >= len(payload)
            fu_header = nal_type
            if offset == 0:
                fu_header |= 0x80
            if end:
                fu_header |= 0x40
            chunk = bytes([fu_indicator, fu_header]) + payload[offset:offset + size].tobytes()
            packets.append((chunk, 1 if end and marker else 0))
            offset += size
        return packets

    def _packetize_h265_fu(self, nalu: bytes, marker: int) -> List[Tuple[bytes, int]]:
        if len(nalu) < 3:
            return []
        nal_header0 = nalu[0]
        nal_header1 = nalu[1]
        nal_type = (nal_header0 >> 1) & 0x3F
        fu_indicator0 = (nal_header0 & 0x81) | (49 << 1)
        fu_indicator1 = nal_header1
        payload = memoryview(nalu)[2:]
        max_fragment = MAX_RTP_PAYLOAD - 3
        offset = 0
        packets: List[Tuple[bytes, int]] = []
        while offset < len(payload):
            size = min(max_fragment, len(payload) - offset)
            end = offset + size >= len(payload)
            fu_header = nal_type
            if offset == 0:
                fu_header |= 0x80
            if end:
                fu_header |= 0x40
            chunk = bytes([fu_indicator0, fu_indicator1, fu_header]) + payload[offset:offset + size].tobytes()
            packets.append((chunk, 1 if end and marker else 0))
            offset += size
        return packets

    async def _send_rtp_packet(self, state: StreamTrackState, payload: bytes, timestamp: int, marker: int) -> None:
        if state.rtp_channel is None:
            return
        header = struct.pack(
            "!BBHII",
            0x80,
            (0x80 if marker else 0x00) | (state.info.payload_type & 0x7F),
            state.sequence_number & 0xFFFF,
            timestamp & 0xFFFFFFFF,
            state.ssrc & 0xFFFFFFFF,
        )
        state.sequence_number = (state.sequence_number + 1) & 0xFFFF
        state.packet_count += 1
        state.octet_count += len(payload)
        state.last_rtp_timestamp = timestamp & 0xFFFFFFFF
        state.last_rtp_send_time = time.time()
        packet = header + payload
        await self._send_interleaved_frame(state.rtp_channel, packet)
        await self._maybe_send_rtcp_sr(state)

    async def _send_interleaved_frame(self, channel: int, payload: bytes) -> None:
        interleaved = b"$" + bytes([channel]) + struct.pack("!H", len(payload)) + payload
        async with self.write_lock:
            try:
                await self.sock.send(interleaved)
            except Exception as ex:
                if _is_disconnect_error(ex):
                    raise ConnectionResetError(*getattr(ex, "args", ())) from ex
                raise

    async def _maybe_send_rtcp_sr(self, state: StreamTrackState) -> None:
        if state.rtcp_channel is None or state.packet_count <= 0 or state.last_rtp_send_time is None:
            return
        now = time.time()
        if state.last_rtcp_send_time is not None and now - state.last_rtcp_send_time < 5.0:
            return
        ntp_seconds = now + 2208988800
        ntp_msw = int(ntp_seconds) & 0xFFFFFFFF
        ntp_lsw = int((ntp_seconds - int(ntp_seconds)) * (1 << 32)) & 0xFFFFFFFF
        packet = struct.pack(
            "!BBHIIIIII",
            0x80,
            200,
            6,
            state.ssrc & 0xFFFFFFFF,
            ntp_msw,
            ntp_lsw,
            state.last_rtp_timestamp & 0xFFFFFFFF,
            state.packet_count & 0xFFFFFFFF,
            state.octet_count & 0xFFFFFFFF,
        )
        await self._send_interleaved_frame(state.rtcp_channel, packet)
        state.last_rtcp_send_time = now
        logger.debug(
            f"{self.peername} sent rtcp sr track={state.info.control} "
            f"rtcp_channel={state.rtcp_channel} packets={state.packet_count} octets={state.octet_count} "
            f"rtp_timestamp={state.last_rtp_timestamp}"
        )

    async def _send_rtcp_bye(self) -> None:
        for state in self.track_states.values():
            if state.rtcp_channel is None:
                continue
            packet = struct.pack(
                "!BBHI",
                0x81,
                203,
                1,
                state.ssrc & 0xFFFFFFFF,
            )
            logger.info(
                f"{self.peername} sent rtcp bye track={state.info.control} "
                f"rtcp_channel={state.rtcp_channel} ssrc={state.ssrc:#x}"
            )
            with contextlib.suppress(Exception):
                await self._send_interleaved_frame(state.rtcp_channel, packet)

    def _handle_interleaved_frame(self, channel: int, payload: bytes) -> None:
        if not payload:
            return
        self._mark_activity()
        state = next((item for item in self.track_states.values() if item.rtcp_channel == channel), None)
        if state is None:
            return
        self._handle_rtcp_packet(state, payload)

    def _handle_rtcp_packet(self, state: StreamTrackState, payload: bytes) -> None:
        offset = 0
        while offset + 4 <= len(payload):
            v_p_count, packet_type, length_words = struct.unpack_from("!BBH", payload, offset)
            version = (v_p_count >> 6) & 0x03
            if version != 2:
                return
            packet_size = (length_words + 1) * 4
            if offset + packet_size > len(payload):
                return
            body = payload[offset + 4:offset + packet_size]
            if packet_type == 201 and len(body) >= 20:
                sender_ssrc, reportee_ssrc = struct.unpack_from("!II", body, 0)
                fraction_lost = body[8]
                highest_seq = struct.unpack_from("!I", body, 12)[0]
                jitter = struct.unpack_from("!I", body, 16)[0]
                logger.info(
                    f"{self.peername} rtcp rr track={state.info.control} "
                    f"sender_ssrc={sender_ssrc:#x} reportee_ssrc={reportee_ssrc:#x} "
                    f"fraction_lost={fraction_lost} highest_seq={highest_seq} jitter={jitter}"
                )
            elif packet_type == 202 and len(body) >= 4:
                source_ssrc = struct.unpack_from("!I", body, 0)[0]
                logger.info(f"{self.peername} rtcp sdes track={state.info.control} ssrc={source_ssrc:#x}")
            elif packet_type == 203 and len(body) >= 4:
                source_ssrc = struct.unpack_from("!I", body, 0)[0]
                logger.info(f"{self.peername} rtcp bye track={state.info.control} ssrc={source_ssrc:#x}")
            offset += packet_size

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
        if body:
            headers["Content-Length"] = str(len(body))
        else:
            headers["Content-Length"] = "0"
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


class RtspServer:
    def __init__(
        self,
        directory: str,
        host: str = "0.0.0.0",
        port: int = 8554,
        session_name: str = "aio-rtsp-toolkit server",
        session_timeout: int = 60,
    ):
        self.root_dir = Path(directory).expanduser().resolve()
        self.host = host
        self.port = port
        self.session_name = session_name
        self.session_timeout = max(15, int(session_timeout))
        self._server: Optional[asyncio.AbstractServer] = None
        self._media_source_cache: Dict[Tuple[Path, int, int], MediaSource] = {}

    async def start(self) -> "RtspServer":
        self._server = await aio.start_tcp_server(self._handle_client, self.host, self.port)
        logger.info(
            f"RTSP server listening on {self.host}:{self.port}, dir={self.root_dir}, "
            f"session_timeout={self.session_timeout}s"
        )
        return self

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_client(self, sock: aio.TCPSocket) -> None:
        session = RtspServerSession(self, sock)
        logger.info(f"{session.peername} connected")
        try:
            await session.run()
        except Exception as ex:
            if not _is_disconnect_error(ex):
                raise

    def resolve_request_path(self, uri: str) -> Path:
        parsed = urllib.parse.urlsplit(uri)
        relative = urllib.parse.unquote(parsed.path.lstrip("/"))
        resolved = (self.root_dir / relative).resolve()
        if self.root_dir not in resolved.parents and resolved != self.root_dir:
            raise FileNotFoundError(uri)
        if not resolved.is_file() or resolved.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise FileNotFoundError(uri)
        return resolved

    def resolve_track_name(self, uri: str) -> str:
        parsed = urllib.parse.urlsplit(uri)
        track_name = parsed.path.rstrip("/").split("/")[-1]
        return track_name.split(";", 1)[0]

    def build_content_base(self, uri: str, headers: Dict[str, str]) -> str:
        parsed = urllib.parse.urlsplit(uri)
        netloc = parsed.netloc.rsplit("@", 1)[-1]
        if not netloc:
            host_header = headers.get("host")
            if host_header:
                netloc = host_header
            else:
                netloc = f"{self.host}:{self.port}"
        path = parsed.path
        if not path.endswith("/"):
            path += "/"
        return f"rtsp://{netloc}{path}"

    def parse_play_count(self, uri: str) -> int:
        parsed = urllib.parse.urlsplit(uri)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if "play_count" not in params:
            return 1
        value = params.get("play_count", ["1"])[0].strip()
        if not value:
            return 1
        play_count = int(value)
        if play_count < 0:
            return 1
        return play_count

    def parse_interleaved_channels(self, transport: str) -> Optional[Tuple[int, int]]:
        lower = transport.lower()
        if "rtp/avp/tcp" not in lower:
            return None
        for part in transport.split(";"):
            if part.strip().lower().startswith("interleaved="):
                value = part.split("=", 1)[1]
                left, _, right = value.partition("-")
                if left.isdigit() and right.isdigit():
                    channels = (int(left), int(right))
                    if all(0 <= channel <= 255 for channel in channels):
                        return channels
                    raise ValueError("interleaved channel must be between 0 and 255")
        return None

    def load_media_source(self, file_path: Path) -> MediaSource:
        stat = file_path.stat()
        cache_key = (file_path, stat.st_mtime_ns, stat.st_size)
        cached = self._media_source_cache.get(cache_key)
        if cached is not None:
            return cached
        suffix = file_path.suffix.lower()
        if suffix == ".wav":
            media_source = self._load_wav_source(file_path)
        else:
            media_source = self._load_av_source(file_path)
        self._media_source_cache = {cache_key: media_source}
        return media_source

    def _load_wav_source(self, file_path: Path) -> MediaSource:
        with av.open(str(file_path), mode="r") as container:
            audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
            if audio_stream is None:
                raise RtspServerError("No audio stream found in wav file")
        track = MediaTrackInfo(
            media_type="audio",
            control="track1",
            codec_name="pcma",
            payload_type=8,
            clock_rate=8000,
            channels=1,
            source_kind="wav_pcm",
        )
        return MediaSource(file_path=file_path, tracks=[track], session_name=self.session_name)

    def _load_av_source(self, file_path: Path) -> MediaSource:
        tracks: List[MediaTrackInfo] = []
        with av.open(str(file_path), mode="r") as container:
            video_stream = next(
                (
                    stream
                    for stream in container.streams
                    if stream.type == "video" and normalize_codec_name(stream.codec_context.name) in (H264CodecName, H265CodecName)
                ),
                None,
            )
            audio_stream = next(
                (
                    stream
                    for stream in container.streams
                    if stream.type == "audio" and normalize_codec_name(stream.codec_context.name) in ("mpeg4-generic", "pcma")
                ),
                None,
            )
            if video_stream is None and audio_stream is None:
                raise RtspServerError("No supported H264/H265 video or AAC/PCMA audio track found")

            if video_stream is not None:
                codec_name = normalize_codec_name(video_stream.codec_context.name)
                nal_length_size, params = parse_parameter_sets(codec_name, bytes(video_stream.codec_context.extradata or b""))
                if codec_name == H264CodecName:
                    sps = params.get("sps", b"")
                    pps = params.get("pps", b"")
                    fmtp = None
                    if sps and pps:
                        fmtp = (
                            f"packetization-mode=1; profile-level-id={profile_level_id_from_sps(sps)}; "
                            f"sprop-parameter-sets={to_base64(sps)},{to_base64(pps)}"
                        )
                else:
                    vps = params.get("vps", b"")
                    sps = params.get("sps", b"")
                    pps = params.get("pps", b"")
                    fmtp = None
                    if vps and sps and pps:
                        fmtp = (
                            f"sprop-vps={to_base64(vps)}; "
                            f"sprop-sps={to_base64(sps)}; "
                            f"sprop-pps={to_base64(pps)}"
                        )
                tracks.append(
                    MediaTrackInfo(
                        media_type="video",
                        control=f"track{len(tracks) + 1}",
                        codec_name=codec_name,
                        payload_type=96,
                        clock_rate=90000,
                        fmtp=fmtp,
                        av_stream_index=video_stream.index,
                        nal_length_size=nal_length_size,
                        parameter_sets=params,
                    )
                )

            if audio_stream is not None:
                codec_name = normalize_codec_name(audio_stream.codec_context.name)
                payload_type = 104 if codec_name == "mpeg4-generic" else 8
                fmtp = None
                channels = audio_stream.codec_context.channels or 1
                sample_rate = audio_stream.codec_context.sample_rate or 8000
                if codec_name == "mpeg4-generic":
                    extradata = bytes(audio_stream.codec_context.extradata or b"")
                    if not extradata:
                        with av.open(str(file_path), mode="r") as audio_probe:
                            probe_stream = audio_probe.streams[audio_stream.index]
                            for packet in audio_probe.demux([probe_stream]):
                                payload = bytes(packet)
                                if payload:
                                    extradata = adts_to_asc(payload)
                                    if extradata:
                                        break
                    config = binascii.hexlify(extradata).decode("ascii")
                    if not config:
                        raise RtspServerError("AAC stream is missing AudioSpecificConfig extradata")
                    fmtp = (
                        "streamtype=5; profile-level-id=15; mode=AAC-hbr; "
                        f"config={config}; SizeLength=13; IndexLength=3; IndexDeltaLength=3"
                    )
                tracks.append(
                    MediaTrackInfo(
                        media_type="audio",
                        control=f"track{len(tracks) + 1}",
                        codec_name=codec_name,
                        payload_type=payload_type,
                        clock_rate=sample_rate,
                        channels=channels,
                        fmtp=fmtp,
                        av_stream_index=audio_stream.index,
                    )
                )

        return MediaSource(file_path=file_path, tracks=tracks, session_name=self.session_name)


async def serve(directory: str, host: str = "0.0.0.0", port: int = 8554, session_timeout: int = 60) -> None:
    server = RtspServer(directory=directory, host=host, port=port, session_timeout=session_timeout)
    try:
        await server.serve_forever()
    finally:
        await server.close()
