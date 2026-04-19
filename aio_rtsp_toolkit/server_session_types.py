from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, Optional

from .server_media import MediaTrackInfo


PCMA_PACKET_MS = 40
PCMA_PACKET_SAMPLES = 8000 * PCMA_PACKET_MS // 1000


def _is_disconnect_error(ex: BaseException) -> bool:
    return isinstance(ex, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError))


def _iter_audio_frames(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


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
class RtspRequest:
    method: str
    uri: str
    version: str
    headers: Dict[str, str]
    body: bytes


class RtspServerError(Exception):
    pass


class RtspBadRequestError(ValueError):
    pass
