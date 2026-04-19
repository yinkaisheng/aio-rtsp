from __future__ import annotations

import asyncio
import enum
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union

import aio_sockets as aio


logger = aio.StdoutLogger()
SocketAddress = Tuple[str, int]
StopEventLike = Union[threading.Event, asyncio.Event]
HeaderValue = Union[str, int, list]
HeaderMap = Dict[str, HeaderValue]
SDPMediaInfo = Dict[str, Any]
SDPInfo = Dict[str, SDPMediaInfo]


class RtspClientMsgType:
    Exception = 1
    ConnectResult = 2
    Closed = 4
    RTSP = 8
    VideoFrame = 16
    AudioFrame = 32
    RTP = 64


DEFAULT_LOG_TYPE = (
    RtspClientMsgType.Exception
    | RtspClientMsgType.ConnectResult
    | RtspClientMsgType.Closed
    | RtspClientMsgType.RTSP
)


@dataclass(frozen=True)
class RtspEvent:
    event: str
    msg_type: int
    session_elapsed: float


@dataclass(frozen=True)
class ConnectResultEvent(RtspEvent):
    local_addr: Optional[SocketAddress] = None
    exception: Optional[Exception] = None
    elapsed: float = 0.0

    @property
    def success(self) -> bool:
        return self.exception is None and self.local_addr is not None


@dataclass(frozen=True)
class RtspMethodEvent(RtspEvent):
    method: str = ""
    response: Any = None
    media_type: Optional[str] = None

    @property
    def status_code(self) -> int:
        return self.response.status_code

    @property
    def elapsed(self) -> float:
        return self.response.elapsed


@dataclass(frozen=True)
class RtpPacketEvent(RtspEvent):
    channel: int = -1
    media_channel: str = ""
    rtp: Optional["RTP"] = None


@dataclass(frozen=True)
class VideoFrameEvent(RtspEvent):
    frame: Optional["VideoFrame"] = None

    @property
    def media_type(self) -> str:
        return "video"


@dataclass(frozen=True)
class AudioFrameEvent(RtspEvent):
    frame: Optional["AudioFrame"] = None

    @property
    def media_type(self) -> str:
        return "audio"


@dataclass(frozen=True)
class ClosedEvent(RtspEvent):
    reason: str = "closed"
    exception: Optional[Exception] = None

    @property
    def elapsed(self) -> float:
        return self.session_elapsed


class RtspError(Exception):
    def __init__(self, message: str, session_elapsed: Optional[float] = None):
        super().__init__(message)
        self.session_elapsed = session_elapsed


class RtspTimeoutError(RtspError):
    pass


class RtspProtocolError(RtspError):
    pass


class RtspResponseError(RtspError):
    def __init__(
        self,
        method: str,
        status_code: int,
        response: Any,
        media_type: Optional[str] = None,
        session_elapsed: Optional[float] = None,
    ):
        self.method = method
        self.status_code = status_code
        self.response = response
        self.media_type = media_type
        target = method if media_type is None else f'{method} {media_type}'
        super().__init__(f'RTSP {target} failed with status {status_code}', session_elapsed=session_elapsed)


class RTP:
    def __init__(
        self,
        version: int,
        padding: int,
        extension: int,
        csic: int,
        marker: int,
        payload_type: int,
        sequence_number: int,
        timestamp: int,
        ssrc: int,
        payload: Optional[bytes] = None,
    ):
        self.version = version
        self.padding = padding
        self.extension = extension
        self.csic = csic
        self.marker = marker
        self.payload_type = payload_type
        self.sequence_number = sequence_number
        self.timestamp = timestamp
        self.ssrc = ssrc
        self.payload = payload
        self.recv_tick = 0

    def __str__(self) -> str:
        return (
            f'{self.__class__.__name__}(timestamp={self.timestamp}, payload_type={self.payload_type}, '
            f'marker={self.marker}, seq={self.sequence_number}, '
            f'payload={" ".join(f"{v:02X}" for v in self.payload[:8])}.., recv_tick={self.recv_tick})'
        )


class RTCP:
    pass


class H264RTPNalUnitType(enum.IntEnum):
    Unkown = 0
    NonIDR = 1
    IDR = 5
    SEI = 6
    SPS = 7
    PPS = 8
    AUD = 9
    STAP_A = 24
    STAP_B = 25
    MTAP16 = 26
    MTAP24 = 27
    FUA = 28
    FUB = 29


class H265RTPNalUnitType(enum.IntEnum):
    BLA_W_LP = 16
    BLA_W_RADL = 17
    BLA_N_LP = 18
    IDR_W_RADL = 19
    IDR_N_LP = 20
    CRA_NUT = 21
    VPS_NUT = 32
    SPS_NUT = 33
    PPS_NUT = 34
    SUFFIX_SEI_NUT = 40
    AP = 48
    FU = 49
    PACI = 50


H264CodecName = "h264"
H265CodecName = "h265"
HEVCCodecName = "hevc"
START_BYTES = b"\x00\x00\x00\x01"
RTP_HEVC_DONL_FIELD_SIZE = 2
RTP_HEVC_DOND_FIELD_SIZE = 1


class SeqNo:
    def __init__(self, num: int = 0, tick: float = 0):
        self.num = num
        self.tick = tick


class VideoFrame:
    def __init__(
        self,
        codec_name_lower: str,
        nalu_type: int = 0,
        timestamp: int = 0,
        data: Optional[bytearray] = None,
        first_seq: Optional[SeqNo] = None,
        last_seq: Optional[SeqNo] = None,
    ):
        self.set_nalu_type(nalu_type, codec_name_lower)
        self.timestamp = timestamp
        self.is_corrupt = False
        self.first_seq = first_seq
        self.last_seq = last_seq
        self.miss_seq = None
        self.data = data

    def set_nalu_type(self, nalu_type: int, codec_name_lower: str) -> None:
        self.nalu_type = nalu_type
        if codec_name_lower == H264CodecName:
            self.is_key_frame = H264RTPNalUnitType.IDR.value <= nalu_type <= H264RTPNalUnitType.PPS.value
        elif codec_name_lower in (H265CodecName, HEVCCodecName):
            self.is_key_frame = H265RTPNalUnitType.BLA_W_LP.value <= nalu_type <= H265RTPNalUnitType.SUFFIX_SEI_NUT.value
        else:
            self.is_key_frame = False

    def __str__(self) -> str:
        seq = str(self.last_seq.num) if self.first_seq.num == self.last_seq.num else f'[{self.first_seq.num},{self.last_seq.num}]'
        miss = f' miss={self.miss_seq}' if self.miss_seq else ''
        recv_cost = self.last_seq.tick - self.first_seq.tick
        return f'{self.__class__.__name__}(timestamp={self.timestamp}, nalu_type={self.nalu_type}, size={len(self.data)}, seq={seq}{miss}, recv_cost={recv_cost:f})'


class AudioFrame:
    def __init__(
        self,
        codec_name_lower: str,
        timestamp: int = 0,
        data: bytes = b"",
        first_seq: Optional[SeqNo] = None,
        last_seq: Optional[SeqNo] = None,
        sample_rate: int = 0,
        channels: int = 0,
        sample_count: int = 0,
    ):
        self.codec_name_lower = codec_name_lower
        self.timestamp = timestamp
        self.data = data
        self.first_seq = first_seq
        self.last_seq = last_seq
        self.sample_rate = sample_rate
        self.channels = channels
        self.sample_count = sample_count
        self.is_corrupt = False
        self.extra = {}

    def __str__(self) -> str:
        if self.first_seq is None or self.last_seq is None:
            seq = '?'
            recv_cost = 0
        else:
            seq = str(self.last_seq.num) if self.first_seq.num == self.last_seq.num else f'[{self.first_seq.num},{self.last_seq.num}]'
            recv_cost = self.last_seq.tick - self.first_seq.tick
        corrupt = ' corrupt' if self.is_corrupt else ''
        return (
            f'{self.__class__.__name__}(codec={self.codec_name_lower}, timestamp={self.timestamp}, '
            f'size={len(self.data)}, sample_rate={self.sample_rate}, channels={self.channels}, '
            f'samples={self.sample_count}, seq={seq}{corrupt}, recv_cost={recv_cost:f})'
        )


PublicRtspEvent = Union[
    ConnectResultEvent,
    RtspMethodEvent,
    RtpPacketEvent,
    VideoFrameEvent,
    AudioFrameEvent,
    ClosedEvent,
]
