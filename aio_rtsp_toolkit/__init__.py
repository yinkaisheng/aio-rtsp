from importlib.metadata import PackageNotFoundError, version

from .tick import Tick
from .audio_codecs import (
    decode_g711_alaw_byte,
    decode_g711_mulaw_byte,
    decode_g711_to_pcm16_bytes,
)
from .client import (
    DEFAULT_LOG_TYPE,
    AACAudioSplicer,
    AudioFrame,
    AudioFrameEvent,
    ClosedEvent,
    ConnectResultEvent,
    H264CodecName,
    H265CodecName,
    HEVCCodecName,
    RTP,
    RtspClientMsgType,
    RtspConnectionError,
    RtspError,
    RtspEvent,
    RtspMethodEvent,
    RtspProtocolError,
    RtspResponse,
    RtspResponseError,
    RtspSession,
    RtspTimeoutError,
    RtpPacketEvent,
    VideoFrame,
    VideoFrameEvent,
    open_session,
)
from .server import RtspServer, RtspServerError, serve

try:
    __version__ = version("aio-rtsp-toolkit")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    'AACAudioSplicer',
    'AudioFrame',
    'AudioFrameEvent',
    'ClosedEvent',
    'ConnectResultEvent',
    'decode_g711_alaw_byte',
    'decode_g711_mulaw_byte',
    'decode_g711_to_pcm16_bytes',
    'DEFAULT_LOG_TYPE',
    'H264CodecName',
    'H265CodecName',
    'HEVCCodecName',
    'RTP',
    'RtpPacketEvent',
    'RtspClientMsgType',
    'RtspConnectionError',
    'RtspError',
    'RtspEvent',
    'RtspMethodEvent',
    'RtspProtocolError',
    'RtspResponse',
    'RtspResponseError',
    'RtspServer',
    'RtspServerError',
    'RtspSession',
    'RtspTimeoutError',
    'Tick',
    'VideoFrame',
    'VideoFrameEvent',
    '__version__',
    'open_session',
    'serve',
]
