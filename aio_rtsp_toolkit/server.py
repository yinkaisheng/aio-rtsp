import asyncio
import base64
import binascii
import contextlib
import random
import struct
import time
import traceback
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aio_sockets as aio

try:
    import av
except Exception:  # pragma: no cover - optional dependency at runtime
    av = None


H264CodecName = 'h264'
H265CodecName = 'h265'
HEVCCodecName = 'hevc'

ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".wav", ".aac"}
DEFAULT_USER_AGENT = "python aio rtsp server"
MAX_RTP_PAYLOAD = 1400
logfunc = print


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


class RtspServerError(Exception):
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
        self.closed = False
        self.play_count = 0

    async def run(self) -> None:
        try:
            while not self.closed:
                request = await self._read_request()
                if request is None:
                    break
                method, uri, version, headers, body = request
                logfunc(f"{self.peername} {method} {uri}")
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
                        await self._stop_streaming()
                        await self._send_response(headers, 200, "OK", {"Session": self.session_id})
                    elif method == "GET_PARAMETER":
                        await self._send_response(headers, 200, "OK", {"Session": self.session_id})
                    elif method == "TEARDOWN":
                        await self._stop_streaming()
                        await self._send_response(headers, 200, "OK", {"Session": self.session_id})
                        break
                    else:
                        await self._send_response(headers, 405, "Method Not Allowed")
                except FileNotFoundError as ex:
                    logfunc(f"{self.peername} {method} not found: {ex}")
                    await self._send_response(headers, 404, "Not Found")
                except ValueError as ex:
                    logfunc(f"{self.peername} {method} bad request: {ex}")
                    await self._send_response(headers, 400, "Bad Request", body=str(ex).encode("utf-8"))
                except RtspServerError as ex:
                    logfunc(f"{self.peername} {method} media error: {ex}")
                    await self._send_response(headers, 415, "Unsupported Media", body=str(ex).encode("utf-8"))
                except Exception as ex:
                    if _is_disconnect_error(ex):
                        break
                    logfunc(f"{self.peername} {method} error: {ex!r}\n{traceback.format_exc()}")
                    with contextlib.suppress(Exception):
                        await self._send_response(headers, 500, "Internal Server Error")
        finally:
            await self.close()

    async def close(self) -> None:
        if self.closed:
            return
        await self._stop_streaming()
        await self._close_transport()

    async def _close_transport(self) -> None:
        if self.closed:
            return
        self.closed = True
        logfunc(f"{self.peername} disconnected")
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
                    payload_len = struct.unpack("!H", header[1:3])[0]
                    if payload_len > 0:
                        await self.sock.recv_exactly(payload_len)
                except asyncio.IncompleteReadError:
                    return None
                continue
            try:
                raw = first + await self.sock.recv_until(b"\r\n\r\n")
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                return None
            header_text = raw.decode("utf-8", errors="replace")
            lines = header_text.split("\r\n")
            if not lines or not lines[0]:
                return None
            parts = lines[0].split(" ", 2)
            if len(parts) != 3:
                return None
            method, uri, version = parts
            headers: Dict[str, str] = {}
            for line in lines[1:]:
                if not line or ":" not in line:
                    continue
                key, value = line.split(":", 1)
                headers[key.strip()] = value.strip()
            content_length = int(headers.get("Content-Length", "0") or "0")
            body = b""
            if content_length > 0:
                body = await self.sock.recv_exactly(content_length)
            return method.upper(), uri, version, headers, body
        return None

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
        session_header = req_headers.get("Session")
        if session_header and session_header.split(";", 1)[0] != self.session_id:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        track_name = self.server.resolve_track_name(uri)
        state = self.track_states.get(track_name)
        if state is None:
            await self._send_response(req_headers, 404, "Not Found")
            return
        transport = req_headers.get("Transport", "")
        channels = self.server.parse_interleaved_channels(transport)
        if channels is None:
            await self._send_response(req_headers, 461, "Unsupported Transport")
            return
        state.rtp_channel, state.rtcp_channel = channels
        await self._send_response(
            req_headers,
            200,
            "OK",
            {
                "Transport": f"RTP/AVP/TCP;unicast;interleaved={channels[0]}-{channels[1]}",
                "Session": f"{self.session_id};timeout=60",
            },
        )

    async def _handle_play(self, req_headers: Dict[str, str]) -> None:
        if self.media_source is None:
            await self._send_response(req_headers, 454, "Session Not Found")
            return
        session_header = req_headers.get("Session", "")
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
                "Session": self.session_id,
                "Range": "npt=0.000-",
                "RTP-Info": ",".join(rtp_info),
            },
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

    async def _stop_streaming(self) -> None:
        if self.stream_task is None:
            return
        self.stream_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self.stream_task
        self.stream_task = None

    async def _stream_media(self) -> None:
        suffix = self.media_source.file_path.suffix.lower()
        remaining_plays = self.play_count
        while not self.closed:
            try:
                if suffix == ".wav":
                    await self._stream_wav()
                else:
                    await self._stream_container()
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                if _is_disconnect_error(ex):
                    break
                logfunc(f"{self.peername} stream error: {ex!r}\n{traceback.format_exc()}")
                break
            if remaining_plays > 0:
                remaining_plays -= 1
                if remaining_plays == 0:
                    break
        if not self.closed and self.play_count > 0 and remaining_plays == 0:
            await self._close_transport()

    async def _stream_container(self) -> None:
        if av is None:
            raise RtspServerError("PyAV is required for mp4/mkv/aac streaming")
        stream_states = {
            state.info.av_stream_index: state
            for state in self.track_states.values()
            if state.rtp_channel is not None and state.info.av_stream_index is not None
        }
        if not stream_states:
            return
        loop_end_times: Dict[int, float] = {}
        with av.open(str(self.media_source.file_path), mode="r") as container:
            selected_streams = [container.streams[index] for index in stream_states]
            for packet in container.demux(selected_streams):
                if self.closed:
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
                if state.first_source_time is None:
                    relative_packet_time = 0.0
                else:
                    relative_packet_time = max(0.0, packet_time - state.first_source_time)
                if state.info.media_type == "video":
                    schedule_time = state.packet_time_cursor
                    state.packet_time_cursor += max(duration_seconds, 0.0)
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.packet_time_cursor,
                    )
                    await self._send_video_packet(
                        state,
                        bytes(packet),
                        schedule_time,
                        duration_seconds,
                        bool(packet.is_keyframe),
                    )
                elif state.info.codec_name == "mpeg4-generic":
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.media_time_offset + relative_packet_time + max(duration_seconds, 0.0),
                    )
                    await self._send_aac_packet(state, bytes(packet), packet_time, duration_seconds)
                elif state.info.codec_name == "pcma":
                    loop_end_times[packet.stream.index] = max(
                        loop_end_times.get(packet.stream.index, 0.0),
                        state.media_time_offset + relative_packet_time + max(duration_seconds, 0.0),
                    )
                    await self._send_raw_audio_packet(state, bytes(packet), packet_time, duration_seconds)
        for stream_index, state in stream_states.items():
            end_time = loop_end_times.get(stream_index)
            if end_time is not None:
                state.media_time_offset = end_time
            state.first_source_time = None
            state.packet_time_cursor = state.media_time_offset

    async def _stream_wav(self) -> None:
        if av is None:
            raise RtspServerError("PyAV is required for wav streaming")
        state = next((it for it in self.track_states.values() if it.rtp_channel is not None), None)
        if state is None:
            return
        packet_samples = 160
        resampler = av.AudioResampler(format="s16", layout="mono", rate=8000)
        encoder = av.CodecContext.create("pcm_alaw", "w")
        encoder.sample_rate = 8000
        encoder.layout = "mono"
        encoder.format = "s16"
        with av.open(str(self.media_source.file_path), mode="r") as container:
            audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
            if audio_stream is None:
                raise RtspServerError("No audio stream found in wav file")
            for packet in container.demux([audio_stream]):
                if self.closed:
                    return
                for decoded_frame in packet.decode():
                    for resampled_frame in _iter_audio_frames(resampler.resample(decoded_frame)):
                        for encoded_packet in encoder.encode(resampled_frame):
                            encoded = bytes(encoded_packet)
                            offset = 0
                            while offset < len(encoded):
                                chunk = encoded[offset:offset + packet_samples]
                                if not chunk:
                                    break
                                offset += len(chunk)
                                timestamp = state.sample_cursor
                                state.sample_cursor += len(chunk)
                                self._log_first_frame(state, timestamp, len(chunk))
                                await self._sleep_until_media_time(state, timestamp / float(state.info.clock_rate or 1))
                                await self._send_rtp_packet(state, chunk, timestamp=timestamp, marker=1)
            for resampled_frame in _iter_audio_frames(resampler.resample(None)):
                for encoded_packet in encoder.encode(resampled_frame):
                    encoded = bytes(encoded_packet)
                    offset = 0
                    while offset < len(encoded):
                        chunk = encoded[offset:offset + packet_samples]
                        if not chunk:
                            break
                        offset += len(chunk)
                        timestamp = state.sample_cursor
                        state.sample_cursor += len(chunk)
                        self._log_first_frame(state, timestamp, len(chunk))
                        await self._sleep_until_media_time(state, timestamp / float(state.info.clock_rate or 1))
                        await self._send_rtp_packet(state, chunk, timestamp=timestamp, marker=1)
            for encoded_packet in encoder.encode(None):
                encoded = bytes(encoded_packet)
                offset = 0
                while offset < len(encoded):
                    chunk = encoded[offset:offset + packet_samples]
                    if not chunk:
                        break
                    offset += len(chunk)
                    timestamp = state.sample_cursor
                    state.sample_cursor += len(chunk)
                    self._log_first_frame(state, timestamp, len(chunk))
                    await self._sleep_until_media_time(state, timestamp / float(state.info.clock_rate or 1))
                    await self._send_rtp_packet(state, chunk, timestamp=timestamp, marker=1)

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

    async def _send_aac_packet(self, state: StreamTrackState, data: bytes, packet_time: float, duration_seconds: float) -> None:
        source_time = self._relative_source_time(state, packet_time)
        timestamp = int(round(source_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        au_header = (len(data) << 3).to_bytes(2, "big")
        payload = struct.pack("!H", 16) + au_header + data
        await self._send_paced_rtp_payloads(state, [(payload, 1)], timestamp, source_time, max(duration_seconds, 0.0))

    async def _send_raw_audio_packet(self, state: StreamTrackState, data: bytes, packet_time: float, duration_seconds: float) -> None:
        source_time = self._relative_source_time(state, packet_time)
        timestamp = int(round(source_time * state.info.clock_rate))
        self._log_first_frame(state, timestamp, len(data))
        await self._send_paced_rtp_payloads(state, [(data, 1)], timestamp, source_time, max(duration_seconds, 0.0))

    def _relative_source_time(self, state: StreamTrackState, value: float) -> float:
        if state.first_source_time is None:
            state.first_source_time = value
            return state.media_time_offset
        return state.media_time_offset + max(0.0, value - state.first_source_time)

    def _log_first_frame(self, state: StreamTrackState, timestamp: int, size: int) -> None:
        if state.logged_first_frame:
            return
        state.logged_first_frame = True
        logfunc(
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
        packet = header + payload
        interleaved = b"$" + bytes([state.rtp_channel]) + struct.pack("!H", len(packet)) + packet
        async with self.write_lock:
            try:
                await self.sock.send(interleaved)
            except Exception as ex:
                if _is_disconnect_error(ex):
                    raise ConnectionResetError(*getattr(ex, "args", ())) from ex
                raise

    async def _send_response(
        self,
        req_headers: Dict[str, str],
        status_code: int,
        reason: str,
        extra_headers: Optional[Dict[str, str]] = None,
        body: bytes = b"",
    ) -> None:
        headers = {
            "CSeq": req_headers.get("CSeq", "0"),
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
    def __init__(self, directory: str, host: str = "0.0.0.0", port: int = 8554, session_name: str = "aio-rtsp-toolkit server"):
        self.root_dir = Path(directory).expanduser().resolve()
        self.host = host
        self.port = port
        self.session_name = session_name
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> "RtspServer":
        self._server = await aio.start_tcp_server(self._handle_client, self.host, self.port)
        logfunc(f"RTSP server listening on {self.host}:{self.port}, dir={self.root_dir}")
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
        logfunc(f"{session.peername} connected")
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
        return parsed.path.rstrip("/").split("/")[-1]

    def build_content_base(self, uri: str, headers: Dict[str, str]) -> str:
        parsed = urllib.parse.urlsplit(uri)
        host_header = headers.get("Host")
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
        value = params.get("play_count", ["0"])[0].strip()
        if not value:
            return 0
        play_count = int(value)
        if play_count < 0:
            raise ValueError("play_count must be greater than or equal to 0")
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
                    return int(left), int(right)
        return None

    def load_media_source(self, file_path: Path) -> MediaSource:
        suffix = file_path.suffix.lower()
        if suffix == ".wav":
            if av is None:
                raise RtspServerError("PyAV is required for wav streaming")
            return self._load_wav_source(file_path)
        if av is None:
            raise RtspServerError("PyAV is required for mp4/mkv/aac streaming")
        return self._load_av_source(file_path)

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


async def serve(directory: str, host: str = "0.0.0.0", port: int = 8554) -> None:
    server = RtspServer(directory=directory, host=host, port=port)
    try:
        await server.serve_forever()
    finally:
        await server.close()
