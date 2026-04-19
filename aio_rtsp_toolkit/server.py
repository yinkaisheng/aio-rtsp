"""Async RTSP/TCP file server.

Serves local media files over RTSP interleaved TCP. Container video/audio tracks
are streamed directly when supported, and WAV input is resampled and encoded as
PCMA 8 kHz mono before being advertised and sent to RTSP clients."""

from __future__ import annotations

import asyncio
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aio_sockets as aio

from .server_control import HttpControlServerHandler
from .server_media import ALLOWED_EXTENSIONS, MediaSource, MediaSourceLoader
from .server_session import (
    PCMA_PACKET_SAMPLES,
    RtspServerError,
    RtspServerSession,
    _is_disconnect_error,
)
from .server_voice import SharedVoiceStream, VoiceStreamError


logger: aio.LoggerLike = aio.StdoutLogger()


class RtspServer:
    def __init__(
        self,
        directory: str,
        host: str = "0.0.0.0",
        port: int = 8554,
        session_name: str = "aio-rtsp-toolkit server",
        session_timeout: int = 60,
        control_host: Optional[str] = None,
        control_port: Optional[int] = None,
    ):
        self.root_dir = Path(directory).expanduser().resolve()
        self.host = host
        self.port = port
        self.session_name = session_name
        self.session_timeout = max(15, int(session_timeout))
        self.logger = logger
        self._server: Optional[asyncio.AbstractServer] = None
        self.control_host = control_host or host
        self.control_port = control_port
        self._control_server: Optional[asyncio.AbstractServer] = None
        self._control_handler = HttpControlServerHandler(
            self.switch_voice_stream_file,
            inactive_error_types=(RtspServerError, VoiceStreamError),
        )
        self._media_source_loader = MediaSourceLoader(self.session_name)
        self._voice_streams: Dict[str, SharedVoiceStream] = {}
        self._voice_streams_lock = asyncio.Lock()

    async def start(self) -> "RtspServer":
        self._server = await aio.start_tcp_server(self._handle_client, self.host, self.port)
        if self.control_port is not None:
            self._control_server = await aio.start_tcp_server(
                self._control_handler.handle_client,
                self.control_host,
                self.control_port,
            )
        self.logger.info(
            f"RTSP server listening on {self.host}:{self.port}, dir={self.root_dir}, "
            f"session_timeout={self.session_timeout}s"
        )
        if self._control_server is not None:
            self.logger.info(f"voice stream control listening on http://{self.control_host}:{self.control_port}")
        return self

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            if self._control_server is not None:
                async with self._control_server:
                    await asyncio.gather(
                        self._server.serve_forever(),
                        self._control_server.serve_forever(),
                    )
            else:
                await self._server.serve_forever()

    async def close(self) -> None:
        for stream in list(self._voice_streams.values()):
            await stream.stop("server shutdown")
        self._voice_streams.clear()
        if self._control_server is not None:
            self._control_server.close()
            await self._control_server.wait_closed()
            self._control_server = None
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_client(self, sock: aio.TCPSocket) -> None:
        session = RtspServerSession(self, sock)
        self.logger.info(f"{session.peername} connected")
        try:
            await session.run()
        except Exception as ex:
            if not _is_disconnect_error(ex):
                raise

    def resolve_request_path(self, uri: str) -> Path:
        relative, _ = self._split_media_request_path(uri)
        resolved = (self.root_dir / relative).resolve()
        if self.root_dir not in resolved.parents and resolved != self.root_dir:
            raise FileNotFoundError(uri)
        if not resolved.is_file() or resolved.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise FileNotFoundError(uri)
        return resolved

    def _split_media_request_path(self, uri: str) -> Tuple[str, List[str]]:
        parsed = urllib.parse.urlsplit(uri)
        parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
        media_parts: List[str] = []
        option_parts: List[str] = []
        media_found = False
        for part in parts:
            if not media_found:
                media_parts.append(part)
                if Path(part).suffix.lower() in ALLOWED_EXTENSIONS:
                    media_found = True
            else:
                option_parts.append(part)
        relative = "/".join(media_parts)
        return relative, option_parts

    def _parse_media_path_options(self, uri: str) -> Dict[str, str]:
        _, option_parts = self._split_media_request_path(uri)
        values: Dict[str, str] = {}
        for part in option_parts:
            lower = part.lower()
            if lower.startswith("play_count_"):
                values["play_count"] = part[len("play_count_"):].strip()
            elif lower.startswith("pre_fill_ms_"):
                values["pre_fill_ms"] = part[len("pre_fill_ms_"):].strip()
        return values

    def is_voice_stream_request(self, uri: str) -> bool:
        parsed = urllib.parse.urlsplit(uri)
        parts = [part for part in parsed.path.split("/") if part]
        return bool(parts) and parts[0] == "voice_stream"

    def parse_voice_stream_request(self, uri: str) -> Tuple[str, int]:
        parsed = urllib.parse.urlsplit(uri)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        parts = [part for part in parsed.path.split("/") if part]
        path_stream_id = ""
        path_play_time = ""
        if len(parts) >= 3 and parts[0] == "voice_stream":
            if parts[1].startswith("id_"):
                path_stream_id = parts[1][3:]
            if parts[2].startswith("play_time_"):
                path_play_time = parts[2][10:]

        stream_id = (params.get("id", [""])[0] or path_stream_id or "").strip()
        if not stream_id:
            raise ValueError("voice_stream requires id, use ?id=... or /id_<value>/")
        play_time_text = (params.get("play_time", [""])[0] or path_play_time or "").strip()
        if not play_time_text:
            raise ValueError("voice_stream requires play_time, use ?play_time=... or /play_time_<value>/")
        play_time = int(play_time_text)
        if play_time <= 0:
            raise ValueError("play_time must be greater than 0")
        return stream_id, play_time

    async def get_or_create_voice_stream(self, stream_id: str, play_time: int) -> SharedVoiceStream:
        async with self._voice_streams_lock:
            current = self._voice_streams.get(stream_id)
            if current is not None:
                if current.play_time != play_time:
                    self.logger.info(
                        f"voice stream id={stream_id} already active, keeping existing play_time={current.play_time}, "
                        f"ignoring requested play_time={play_time}"
                    )
                return current
            voice_stream = SharedVoiceStream(
                self,
                stream_id,
                play_time,
                packet_samples=PCMA_PACKET_SAMPLES,
                logger=self.logger,
            )
            self._voice_streams[stream_id] = voice_stream
        await voice_stream.start()
        self.logger.info(f"voice stream created id={stream_id} play_time={play_time}s")
        return voice_stream

    def get_voice_stream(self, stream_id: str) -> Optional[SharedVoiceStream]:
        return self._voice_streams.get(stream_id)

    async def remove_voice_stream(self, stream_id: str, stream: SharedVoiceStream) -> None:
        async with self._voice_streams_lock:
            current = self._voice_streams.get(stream_id)
            if current is stream:
                self._voice_streams.pop(stream_id, None)
                self.logger.info(f"voice stream removed id={stream_id}")

    def resolve_voice_file_path(self, file_name: str) -> Path:
        relative = urllib.parse.unquote((file_name or "").strip().lstrip("/"))
        if not relative:
            raise FileNotFoundError("empty file path")
        resolved = (self.root_dir / relative).resolve()
        if self.root_dir not in resolved.parents and resolved != self.root_dir:
            raise FileNotFoundError(file_name)
        if not resolved.is_file() or resolved.suffix.lower() != ".wav":
            raise FileNotFoundError(file_name)
        return resolved

    async def switch_voice_stream_file(self, stream_id: str, file_name: str) -> Path:
        voice_stream = self.get_voice_stream(stream_id)
        if voice_stream is None:
            raise RtspServerError(f"voice stream id={stream_id} is not active")
        file_path = self.resolve_voice_file_path(file_name)
        await voice_stream.switch_to_file(file_path)
        self.logger.info(f"voice stream switched id={stream_id} file={file_path.name}")
        return file_path

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
        path_options = self._parse_media_path_options(uri)
        if "play_count" not in params and "play_count" not in path_options:
            return 1
        value = (params.get("play_count", [""])[0] or path_options.get("play_count", "1")).strip()
        if not value:
            return 1
        play_count = int(value)
        if play_count < 0:
            return 1
        return play_count

    def parse_pre_fill_ms(self, uri: str) -> int:
        parsed = urllib.parse.urlsplit(uri)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        path_options = self._parse_media_path_options(uri)
        if "pre_fill_ms" not in params and "pre_fill_ms" not in path_options:
            return 0
        value = (params.get("pre_fill_ms", [""])[0] or path_options.get("pre_fill_ms", "0")).strip()
        if not value:
            return 0
        pre_fill_ms = int(value)
        if pre_fill_ms < 0:
            return 0
        return pre_fill_ms

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
        return self._media_source_loader.load(file_path)


async def serve(
    directory: str,
    host: str = "0.0.0.0",
    port: int = 8554,
    session_timeout: int = 60,
    control_host: Optional[str] = None,
    control_port: Optional[int] = None,
) -> None:
    server = RtspServer(
        directory=directory,
        host=host,
        port=port,
        session_timeout=session_timeout,
        control_host=control_host,
        control_port=control_port,
    )
    try:
        await server.serve_forever()
    finally:
        await server.close()
