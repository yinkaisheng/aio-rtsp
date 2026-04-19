from __future__ import annotations

import asyncio
import contextlib
import random
import time
import traceback
from typing import Dict, Optional, TYPE_CHECKING

import aio_sockets as aio

from .server_media import MediaSource, MediaSourceError
from .server_protocol import RtspProtocolMixin
from .server_rtp import RtspRtpMixin
from .server_session_types import (
    PCMA_PACKET_SAMPLES,
    RtspBadRequestError,
    RtspServerError,
    StreamTrackState,
    _is_disconnect_error,
)
from .server_streaming import RtspStreamingMixin

if TYPE_CHECKING:
    from .server import RtspServer


class RtspServerSession(RtspStreamingMixin, RtspRtpMixin, RtspProtocolMixin):
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
        self.pre_fill_ms = 0
        self.session_active = False
        self.last_activity_monotonic = time.monotonic()
        self.voice_subscription: Optional[asyncio.Queue] = None

    @property
    def logger(self) -> aio.LoggerLike:
        return self.server.logger

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
                    self.logger.warning(f"{self.peername} bad request: {ex}")
                    with contextlib.suppress(Exception):
                        await self._send_response({}, 400, "Bad Request", body=str(ex).encode("utf-8"))
                    continue
                if request is None:
                    break
                method = request.method
                headers = request.headers
                self._mark_activity()
                self.logger.info(f"{self.peername} {method} {request.uri}")
                try:
                    if method == "OPTIONS":
                        await self._handle_options(headers)
                    elif method == "DESCRIBE":
                        await self._handle_describe(request.uri, headers)
                    elif method == "SETUP":
                        await self._handle_setup(request.uri, headers)
                    elif method == "PLAY":
                        await self._handle_play(headers)
                    elif method in {"PAUSE", "GET_PARAMETER", "TEARDOWN"}:
                        if await self._handle_session_control(method, headers):
                            break
                    else:
                        await self._send_response(headers, 405, "Method Not Allowed")
                except FileNotFoundError as ex:
                    self.logger.warning(f"{self.peername} {method} not found: {ex}")
                    await self._send_response(headers, 404, "Not Found")
                except ValueError as ex:
                    self.logger.warning(f"{self.peername} {method} bad request: {ex}")
                    await self._send_response(headers, 400, "Bad Request", body=str(ex).encode("utf-8"))
                except (RtspServerError, MediaSourceError) as ex:
                    self.logger.warning(f"{self.peername} {method} media error: {ex}")
                    await self._send_response(headers, 415, "Unsupported Media", body=str(ex).encode("utf-8"))
                except Exception as ex:
                    if _is_disconnect_error(ex):
                        break
                    self.logger.error(f"{self.peername} {method} error: {ex!r}\n{traceback.format_exc()}")
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
        self.logger.info(f"{self.peername} disconnected session={self.session_id}")
        with contextlib.suppress(BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            await self.sock.close()

    def _require_valid_session(self, req_headers: Dict[str, str]) -> bool:
        session_header = req_headers.get("session", "")
        return bool(session_header) and session_header.split(";", 1)[0] == self.session_id

    async def _handle_session_control(self, method: str, req_headers: Dict[str, str]) -> bool:
        if not self._require_valid_session(req_headers):
            await self._send_response(req_headers, 454, "Session Not Found")
            return False
        if method in {"PAUSE", "TEARDOWN"}:
            await self._stop_streaming()
        await self._send_response(req_headers, 200, "OK", {"Session": self._session_header_value()})
        return method == "TEARDOWN"

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
            self.logger.info(
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
        if self.server.is_voice_stream_request(uri):
            stream_id, play_time = self.server.parse_voice_stream_request(uri)
            voice_stream = await self.server.get_or_create_voice_stream(stream_id, play_time)
            self.play_count = 1
            self.media_source = voice_stream.media_source
            resource_label = f"voice_stream:{stream_id}"
        else:
            resource_path = self.server.resolve_request_path(uri)
            self.play_count = self.server.parse_play_count(uri)
            self.pre_fill_ms = self.server.parse_pre_fill_ms(uri)
            self.media_source = self.server.load_media_source(resource_path)
            resource_label = resource_path.name
        self.content_base = self.server.build_content_base(uri, req_headers)
        self.track_states = {track.control: StreamTrackState(track) for track in self.media_source.tracks}
        track_summary = ", ".join(
            f"{track.control}:{track.media_type}/{track.codec_name}/{track.clock_rate}"
            for track in self.media_source.tracks
        )
        self.logger.info(
            f"{self.peername} describe session={self.session_id} source={resource_label} "
            f"play_count={self.play_count} pre_fill_ms={self.pre_fill_ms} tracks={track_summary}"
        )
        await self._send_response(
            req_headers,
            200,
            "OK",
            {
                "Content-Base": self.content_base,
                "Content-Type": "application/sdp",
            },
            body=self.media_source.build_sdp().encode("utf-8"),
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
        channels = self.server.parse_interleaved_channels(req_headers.get("transport", ""))
        if channels is None:
            await self._send_response(req_headers, 461, "Unsupported Transport")
            return
        if channels[0] == channels[1]:
            await self._send_response(req_headers, 400, "Bad Request", body=b"interleaved RTP and RTCP channels must differ")
            return
        for other_state in self.track_states.values():
            if other_state.info.control == state.info.control:
                continue
            if channels[0] in (other_state.rtp_channel, other_state.rtcp_channel) or channels[1] in (other_state.rtp_channel, other_state.rtcp_channel):
                await self._send_response(req_headers, 400, "Bad Request", body=b"interleaved channels already in use")
                return
        state.rtp_channel, state.rtcp_channel = channels
        self.session_active = True
        self._mark_activity()
        self.logger.info(
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
        rtp_info = [
            f"url={self.content_base}{state.info.control};seq=1;rtptime=0"
            for state in self.track_states.values()
            if state.rtp_channel is not None
        ]
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
        self.logger.info(
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


__all__ = [
    "PCMA_PACKET_SAMPLES",
    "RtspBadRequestError",
    "RtspServerError",
    "RtspServerSession",
    "StreamTrackState",
    "_is_disconnect_error",
]
