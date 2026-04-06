#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# yinkaisheng@foxmail.com
import os
import sys
import time
from dataclasses import dataclass
from typing import (Any, AsyncGenerator, Callable, Deque, Dict, Generator, List, Iterable, Iterator, Optional, Sequence, Set, Tuple, Union)
import enum
import base64
import struct
import hashlib
import asyncio
import threading
import urllib.parse

import aio_sockets as aio
from .tick import Tick


logfunc = print

StopEventLike = Union[threading.Event, asyncio.Event]


class RtspClientMsgType:
    """Bit flags used to classify emitted data and control debug logging."""

    ConnectResult = 1
    RTSP = 2
    RTP = 4
    VideoFrame = 8
    AudioFrame = 16
    Closed = 32
    Exception = 64


@dataclass(frozen=True)
class RtspEvent:
    """Base class for all public session events.

    Attributes:
        event: Stable event name such as ``"rtsp_method"`` or ``"video_frame"``.
        msg_type: Bit flag from :class:`RtspClientMsgType`.
        session_elapsed: Process-local elapsed seconds since the session started.
    """

    event: str
    msg_type: int
    session_elapsed: float


@dataclass(frozen=True)
class ConnectResultEvent(RtspEvent):
    """Event emitted after the TCP connection attempt completes.

    Time values are local monotonic durations, not wall-clock timestamps.
    """

    local_addr: Any = None
    exception: Optional[Exception] = None
    elapsed: float = 0.0

    @property
    def success(self) -> bool:
        return self.exception is None and self.local_addr is not None


@dataclass(frozen=True)
class RtspMethodEvent(RtspEvent):
    """Event emitted for each RTSP method response.

    Time values are local monotonic durations, not wall-clock timestamps.
    """

    method: str = ''
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
    """Event emitted for each parsed interleaved RTP packet."""

    channel: int = -1
    media_channel: str = ''
    rtp: Optional['RTP'] = None


@dataclass(frozen=True)
class VideoFrameEvent(RtspEvent):
    """Event emitted for each assembled video frame."""

    frame: Optional['VideoFrame'] = None

    @property
    def media_type(self) -> str:
        return 'video'


@dataclass(frozen=True)
class AudioFrameEvent(RtspEvent):
    """Event emitted for each assembled audio frame."""

    frame: Optional['AudioFrame'] = None

    @property
    def media_type(self) -> str:
        return 'audio'


@dataclass(frozen=True)
class ClosedEvent(RtspEvent):
    """Event emitted when the session is shutting down."""

    reason: str = 'closed'

    @property
    def elapsed(self) -> float:
        return self.session_elapsed


class RtspError(Exception):
    """Base exception for public aio-rtsp API errors."""

    pass


class RtspConnectionError(RtspError):
    """Raised when the RTSP TCP connection cannot be established."""

    pass


class RtspTimeoutError(RtspError):
    """Raised when a connect, RTSP method, or RTP receive operation times out."""

    pass


class RtspProtocolError(RtspError):
    """Raised for malformed protocol data or transport-level failures."""

    pass


class RtspResponseError(RtspError):
    """Raised when an RTSP method returns a non-success response."""

    def __init__(self, method: str, status_code: int, response: 'RtspResponse', media_type: Optional[str] = None):
        self.method = method
        self.status_code = status_code
        self.response = response
        self.media_type = media_type
        target = method if media_type is None else f'{method} {media_type}'
        super().__init__(f'RTSP {target} failed with status {status_code}')


def get_session_elapsed(tick: Tick) -> float:
    """Return elapsed session time without mutating method timing state."""

    return round(Tick.get_tick() - tick.start_tick, 6)


class RTP:
    """Parsed RTP header fields and payload data."""

    def __init__(self, version: int, padding: int, extension: int, csic: int, marker: int, payload_type: int,
                 sequence_number: int, timestamp: int, ssrc: int, payload: bytes = None):
        #: RTP version number, typically 2.
        self.version = version
        #: Non-zero if the packet uses RTP padding.
        self.padding = padding
        #: Non-zero if an RTP extension header is present.
        self.extension = extension
        #: CSRC count from the RTP header.
        self.csic = csic
        #: Marker bit from the RTP header.
        self.marker = marker
        #: RTP payload type number.
        self.payload_type = payload_type
        #: RTP sequence number.
        self.sequence_number = sequence_number
        #: RTP timestamp in stream clock units.
        self.timestamp = timestamp
        #: Synchronization source identifier.
        self.ssrc = ssrc
        #: RTP payload bytes after header and padding removal.
        self.payload: bytes = payload
        #: Process-local receive time relative to process startup, derived from
        #: ``time.perf_counter()``.
        self.recv_tick = 0

    def __str__(self):
        return f'{self.__class__.__name__}(timestamp={self.timestamp}, payload_type={self.payload_type}, marker={self.marker}' \
               f', seq={self.sequence_number}, payload={" ".join(f"{v:02X}" for v in self.payload[:8])}, tick={self.recv_tick})'

class H264RTPNalUnitType(enum.IntEnum): # Network Abstraction Layer
    Unkown = 0
    NonIDR = 1
    IDR = 5     # Instantaneous Decoder Refresh Frame
    SEI = 6     # Supplemental Enhancement Information
    SPS = 7     # Sequence Parameter Set
    PPS = 8     # Picture Parameter Set
    AUD = 9     # Access Unit Delimiter
    STAP_A = 24 # Single-Time Aggregation Packet
    STAP_B = 25 # Multi-Time Aggregation Packets
    MTAP16 = 26
    MTAP24 = 27
    FUA = 28    # Fragmentation Unit A
    FUB = 29    # Fragmentation Unit B


class H265RTPNalUnitType(enum.IntEnum):
    TRAIL_N = 0
    TRAIL_R = 1
    TSA_N = 2
    TSA_R= 3
    STSA_N = 4
    STSA_R = 5
    RADL_N = 6
    RADL_R = 7
    RASL_N = 8
    RASL_R = 9
    RSV_VCL_N10 = 10
    RSV_VCL_R11 = 11
    RSV_VCL_N12 = 12
    RSV_VCL_R13 = 13
    RSV_VCL_N14 = 14
    RSV_VCL_R15 = 15
    BLA_W_LP = 16
    BLA_W_RADL = 17
    BLA_N_LP = 18
    IDR_W_RADL = 19
    IDR_N_LP = 20
    CRA_NUT = 21
    RSV_IRAP_VCL_22 = 22
    RSV_IRAP_VCL_23 = 23
    RSV_VCL24 = 24
    RSV_VCL25 = 25
    RSV_VCL26 = 26
    RSV_VCL27 = 27
    RSV_VCL28 = 28
    RSV_VCL29 = 29
    RSV_VCL30 = 30
    RSV_VCL31 = 31
    VPS_NUT = 32
    SPS_NUT = 33
    PPS_NUT = 34
    AUD = 35
    EOS_NUT = 36
    EOB_NUT = 37
    FD_NUT = 38
    PREFIX_SEI_NUT = 39
    SUFFIX_SEI_NUT = 40
    RSV_NVCL41 = 41
    RSV_NVCL42 = 42
    RSV_NVCL43 = 43
    RSV_NVCL44 = 44
    RSV_NVCL45 = 45
    RSV_NVCL46= 46
    RSV_NVCL47 = 47
    AP = 48     # Aggregation Packet
    FU = 49     # Fragmentation Unit
    PACI = 50   # PACI Packet


H264CodecName = 'h264'
H265CodecName = 'h265'
HEVCCodecName = 'hevc'
START_BYTES = b'\x00\x00\x00\x01'
RTP_HEVC_DONL_FIELD_SIZE = 2
RTP_HEVC_DOND_FIELD_SIZE = 1


class VideoFrameType(enum.IntEnum):
    Unkown = 0
    P = 1
    I = 5
    SEI = 6
    SPS = 7
    PPS = 8


class SeqNo:
    """Sequence number paired with process-local receive time."""

    def __init__(self, num: int = 0, tick: float = 0):
        self.num = num
        self.tick = tick


class VideoFrame:
    """Assembled raw video frame or standalone NAL unit.

    Attributes:
        timestamp: RTP timestamp for the frame.
        is_key_frame: Whether this NAL unit is treated as a key-frame unit.
        is_corrupt: Whether packet loss was detected while assembling the frame.
        first_seq: First RTP packet contributing to the frame, including local receive time.
        last_seq: Last RTP packet contributing to the frame, including local receive time.
        miss_seq: Missing RTP sequence numbers detected during assembly.
        data: Annex B byte stream.
    """

    def __init__(self, codec_name_lower: str, nalu_type: int = 0, timestamp: int = 0,
                 data: Union[bytes, bytearray] = None, first_seq: SeqNo = None, last_seq: SeqNo = None):
        self.set_nalu_type(nalu_type, codec_name_lower)
        self.timestamp: int = timestamp
        self.is_corrupt: bool = False
        self.first_seq: SeqNo = first_seq
        self.last_seq: SeqNo = last_seq
        self.miss_seq: List[int] = None
        self.data: Union[bytes,bytearray] = data

    def set_nalu_type(self, nalu_type, codec_name_lower: str) -> None:
        self.nalu_type = nalu_type
        if codec_name_lower == H264CodecName:
            self.is_key_frame: bool = H264RTPNalUnitType.IDR.value <= nalu_type <= H264RTPNalUnitType.PPS.value
        elif codec_name_lower == H265CodecName or codec_name_lower == HEVCCodecName:
            self.is_key_frame: bool = H265RTPNalUnitType.BLA_W_LP.value <= nalu_type <= H265RTPNalUnitType.SUFFIX_SEI_NUT.value
        else:
            self.is_key_frame: bool = False

    def __str__(self):
        seq = str(self.last_seq.num) if self.first_seq.num == self.last_seq.num else f'[{self.first_seq.num},{self.last_seq.num}]'
        miss = f' miss={self.miss_seq}' if self.miss_seq else ''
        return f'{self.__class__.__name__}(timestamp={self.timestamp}, nalu_type={self.nalu_type}, size={len(self.data)}' \
               f', seq={seq}{miss}, recv_cost={self.last_seq.tick-self.first_seq.tick:f})'


class AudioFrame:
    """Assembled audio frame with codec and sampling metadata."""

    def __init__(self, codec_name_lower: str, timestamp: int = 0, data: bytes = b'',
                 first_seq: SeqNo = None, last_seq: SeqNo = None, sample_rate: int = 0,
                 channels: int = 0, sample_count: int = 0):
        self.codec_name_lower = codec_name_lower
        self.timestamp = timestamp
        self.data = data
        self.first_seq = first_seq
        self.last_seq = last_seq
        self.sample_rate = sample_rate
        self.channels = channels
        self.sample_count = sample_count
        self.is_corrupt = False
        self.extra: Dict[str, Any] = {}

    def __str__(self):
        if self.first_seq is None or self.last_seq is None:
            seq = '?'
            recv_cost = 0
        else:
            seq = str(self.last_seq.num) if self.first_seq.num == self.last_seq.num else f'[{self.first_seq.num},{self.last_seq.num}]'
            recv_cost = self.last_seq.tick - self.first_seq.tick
        corrupt = ' corrupt' if self.is_corrupt else ''
        return f'{self.__class__.__name__}(codec={self.codec_name_lower}, timestamp={self.timestamp}, size={len(self.data)}' \
               f', sample_rate={self.sample_rate}, channels={self.channels}, samples={self.sample_count}, seq={seq}{corrupt}, recv_cost={recv_cost:f})'


def read_bits(data: bytes, bit_offset: int, bit_count: int) -> int:
    """Read an unsigned integer from a packed bitstream."""

    if bit_count <= 0:
        return 0
    value = 0
    for i in range(bit_count):
        pos = bit_offset + i
        value = (value << 1) | ((data[pos // 8] >> (7 - pos % 8)) & 0x01)
    return value


class AudioSplicer:
    """Base class for converting RTP payloads into audio frame objects."""

    def __init__(self, codec_name_lower: str, clock_rate: int = 0, channels: int = 1,
                 fmtp: Dict[str, Any] = None, log_type: int = 0):
        self.codec_name_lower = codec_name_lower
        self.clock_rate = clock_rate
        self.channels = channels or 1
        self.fmtp = fmtp or {}
        self.log_type = log_type

    def new_audio_frame(self, rtp: RTP, data: bytes, timestamp: int = None, sample_count: int = 0) -> AudioFrame:
        seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
        frame = AudioFrame(
            self.codec_name_lower,
            timestamp=rtp.timestamp if timestamp is None else timestamp,
            data=data,
            first_seq=seq,
            last_seq=seq,
            sample_rate=self.clock_rate,
            channels=self.channels,
            sample_count=sample_count,
        )
        return frame

    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        return []


class RawAudioSplicer(AudioSplicer):
    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        sample_count = len(rtp.payload) // max(self.channels, 1)
        return [self.new_audio_frame(rtp, rtp.payload, sample_count=sample_count)]


class AACAudioSplicer(AudioSplicer):
    """Splicer for MPEG4-GENERIC AAC RTP payloads."""

    def __init__(self, codec_name_lower: str, clock_rate: int = 0, channels: int = 1,
                 fmtp: Dict[str, Any] = None, log_type: int = 0):
        super().__init__(codec_name_lower, clock_rate, channels, fmtp, log_type)
        self.size_length = int(self.fmtp.get('sizelength', 13) or 13)
        self.index_length = int(self.fmtp.get('indexlength', 3) or 0)
        self.index_delta_length = int(self.fmtp.get('indexdeltalength', 3) or 0)
        self.cts_delta_length = int(self.fmtp.get('ctsdeltalength', 0) or 0)
        self.dts_delta_length = int(self.fmtp.get('dtsdeltalength', 0) or 0)
        self.random_access_indication = int(self.fmtp.get('randomaccessindication', 0) or 0)
        self.stream_state_indication = int(self.fmtp.get('streamstateindication', 0) or 0)

    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        if len(rtp.payload) < 2:
            return []
        au_headers_length_bits = struct.unpack('!H', rtp.payload[:2])[0]
        if au_headers_length_bits == 0:
            return [self.new_audio_frame(rtp, rtp.payload[2:], sample_count=1024)]

        au_headers_length_bytes = (au_headers_length_bits + 7) // 8
        payload_start = 2 + au_headers_length_bytes
        if len(rtp.payload) < payload_start:
            return []

        header_data = rtp.payload[2:payload_start]
        audio_data = rtp.payload[payload_start:]
        frames: List[AudioFrame] = []
        bit_offset = 0
        data_offset = 0
        index = 0
        while bit_offset < au_headers_length_bits:
            size = read_bits(header_data, bit_offset, self.size_length)
            bit_offset += self.size_length

            index_bits = self.index_length if index == 0 else self.index_delta_length
            if index_bits > 0:
                bit_offset += index_bits
            if self.cts_delta_length > 0:
                bit_offset += self.cts_delta_length
            if self.dts_delta_length > 0:
                bit_offset += self.dts_delta_length
            if self.random_access_indication > 0:
                bit_offset += 1
            if self.stream_state_indication > 0:
                bit_offset += self.stream_state_indication

            if size <= 0:
                index += 1
                continue

            chunk = audio_data[data_offset:data_offset + size]
            if len(chunk) < size:
                frame = self.new_audio_frame(rtp, chunk, sample_count=1024)
                frame.is_corrupt = True
                frames.append(frame)
                break
            frames.append(self.new_audio_frame(rtp, chunk, sample_count=1024))
            data_offset += size
            index += 1

        return frames


def create_audio_splicer(audio_sdp: Dict[str, Any], log_type: int = 0) -> Union[AudioSplicer, None]:
    """Create an audio splicer from parsed audio SDP information."""

    if not audio_sdp:
        return None
    codec_name_lower = str(audio_sdp.get('codec_name', '')).lower()
    clock_rate = int(audio_sdp.get('clock_rate', 0) or 0)
    channels = int(audio_sdp.get('channel', 1) or 1)
    fmtp = audio_sdp.get('fmtp', None)
    if codec_name_lower in ('pcma', 'g711a', 'g711-alaw'):
        return RawAudioSplicer('pcma', clock_rate or 8000, channels, fmtp, log_type)
    if codec_name_lower in ('pcmu', 'g711u', 'g711-mulaw'):
        return RawAudioSplicer('pcmu', clock_rate or 8000, channels, fmtp, log_type)
    if codec_name_lower in ('mpeg4-generic', 'aac'):
        return AACAudioSplicer('mpeg4-generic', clock_rate, channels, fmtp, log_type)
    if codec_name_lower in ('mp4a-latm', 'mpeg4-latm', 'aac_latm'):
        return RawAudioSplicer('mp4a-latm', clock_rate, channels, fmtp, log_type)
    return RawAudioSplicer(codec_name_lower, clock_rate, channels, fmtp, log_type)


def ensure_start_code(nalu: bytes) -> bytes:
    """Ensure a NAL unit is prefixed with a 4-byte Annex B start code."""

    if not nalu:
        return START_BYTES
    if nalu.startswith(START_BYTES):
        return nalu
    if nalu.startswith(b'\x00\x00\x01'):
        return b'\x00' + nalu
    return START_BYTES + nalu


class RtspResponse:
    """Parsed RTSP response and optional SDP payload."""

    def __init__(self, status_code: int, headers: Dict[str, str], body: str=''):
        #: RTSP response status code.
        self.status_code = status_code
        #: RTSP response headers.
        self.headers = headers
        #: RTSP response body text.
        self.body = body
        #: Parsed SDP content for DESCRIBE responses.
        self.sdp: Dict[str, Any] = None
        #: Process-local elapsed seconds for the method that produced this response.
        self.elapsed: float = 0

    def __str__(self):
        return f'{self.__class__.__name__}(status_code={self.status_code}, elapsed={self.elapsed})'


class VideoSplicer:
    """Base class for reconstructing video frames from RTP packets."""

    def __init__(self, log_type: int = 0):
        self.codec_name_lower = ''
        self._cur_frame_timestamp = None
        self._last_frame_end_seqno = None
        self.rtp_list: List[RTP] = []
        self.log_type = log_type

    def try_get_video_frame(self, rtp: RTP) -> Union[List[VideoFrame], None]:
        return None

    def parse_fu_payload_header(self, rtp: RTP) -> Tuple[int, int, int]:
        pass

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int):
        pass

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP):
        pass

    def splice_video_frame(self) -> VideoFrame:
        frame = VideoFrame(self.codec_name_lower, timestamp=self._cur_frame_timestamp, data=bytearray(START_BYTES))
        prev_seq: int = None
        miss_seq: List[int] = []
        rtp_count = len(self.rtp_list)
        for index, rtp in enumerate(self.rtp_list):
            start_bit, end_bit, nalu_type = self.parse_fu_payload_header(rtp)
            if index == 0:
                frame.first_seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
                frame.set_nalu_type(nalu_type, self.codec_name_lower)
                self.append_nalu_header_to_frame(frame, rtp, nalu_type)
                if not start_bit or end_bit:
                    miss_seq.append((rtp.sequence_number-1) if rtp.sequence_number > 0 else 0xFFFF)
                if rtp_count == 1:
                    frame.last_seq = frame.first_seq
                    if not end_bit:
                        miss_seq.append(0 if rtp.sequence_number == 0xFFFF else (rtp.sequence_number+1))
            elif index == rtp_count - 1:
                frame.last_seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
                if end_bit:
                    if prev_seq + 1 != rtp.sequence_number:
                        if prev_seq < rtp.sequence_number:
                            miss_seq.extend(range(prev_seq+1, rtp.sequence_number))
                        else:
                            if not (prev_seq == 0xFFFF and rtp.sequence_number == 0):
                                miss_seq.extend(range(prev_seq+1, 0xFFFF+1))
                                miss_seq.extend(range(0, rtp.sequence_number))
                else:
                    miss_seq.append((rtp.sequence_number+1) if rtp.sequence_number != 0xFFFF else 0)
            else:
                if prev_seq + 1 != rtp.sequence_number:
                    if prev_seq < rtp.sequence_number:
                        miss_seq.extend(range(prev_seq+1, rtp.sequence_number))
                    else:
                        if not (prev_seq == 0xFFFF and rtp.sequence_number == 0):
                            miss_seq.extend(range(prev_seq+1, 0xFFFF+1))
                            miss_seq.extend(range(0, rtp.sequence_number))
            self.append_fu_payload_to_frame(frame, rtp)
            prev_seq = rtp.sequence_number
        if miss_seq:
            frame.miss_seq = miss_seq
            frame.is_corrupt = True
        return frame

    def handle_rtp_fu(self, end_bit: int, rtp: RTP, frames: List[VideoFrame]):
        if self._cur_frame_timestamp is None:
            self._cur_frame_timestamp = rtp.timestamp
            self.rtp_list.append(rtp)
        else:
            if self._cur_frame_timestamp == rtp.timestamp:
                self.rtp_list.append(rtp)
            else:
                if len(self.rtp_list) > 0:
                    corrupted_frame = self.splice_video_frame()
                    frames.append(corrupted_frame)
                    self.rtp_list.clear()
                self._cur_frame_timestamp = rtp.timestamp
                self.rtp_list.append(rtp)
        if end_bit:
            frame = self.splice_video_frame()
            frames.append(frame)
            self.rtp_list.clear()

    def handle_rtp_not_fu(self, rtp:RTP, frames: List[VideoFrame]):
        if self._cur_frame_timestamp is None:
            self._cur_frame_timestamp = rtp.timestamp
        else:
            if self._cur_frame_timestamp != rtp.timestamp:
                if len(self.rtp_list) > 0:
                    corrupted_frame = self.splice_video_frame()
                    frames.append(corrupted_frame)
                    self.rtp_list.clear()
            self._cur_frame_timestamp = rtp.timestamp

    def get_frame_size(self, sps: bytes) -> Tuple[int, int]:
        # not implemented
        return 0, 0


class H264Splicer(VideoSplicer):
    """H.264 RTP payload splicer based on RFC 6184."""

    def __init__(self, log_type: int = 0):
        super().__init__(log_type)
        self.codec_name_lower = H264CodecName

    def try_get_video_frame(self, rtp: RTP) -> Union[List[VideoFrame], None]:
        '''
        see rfc3984
        +---------------+
        |0|1|2|3|4|5|6|7|
        +-+-+-+-+-+-+-+-+
        |F|NRI|  Type   |
        +---------------+
        https://ffmpeg.org/doxygen/4.4/rtpdec__h264_8c_source.html
        '''
        frames = []
        fu_indicator = rtp.payload[0]
        forbidden_bit = fu_indicator >> 7
        nri_type = fu_indicator >> 5 & 0b0011
        # NRI bits describe how important the NAL unit is to the decoder:
        # 00: loss does not affect decoding
        # 01: loss has low impact
        # 10: loss has medium impact
        # 11: loss has high impact
        nalu_type = fu_indicator & 0b0001_1111
        # NAL unit type summary:
        # 0: undefined
        # 1: non-IDR slice, usually part of a P-frame
        # 5: IDR slice, usually part of an I-frame
        # 6: SEI
        # 7: SPS
        # 8: PPS
        # 9: access unit delimiter
        # 24: STAP-A
        # 25: STAP-B
        # 26: MTAP16
        # 27: MTAP24
        # 28: FU-A
        # 29: FU-B
        if nalu_type == H264RTPNalUnitType.FUA.value: # 28
            #  0                   1                   2                   3
            #  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # | FU indicator  |   FU header   |                               |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               |
            # |                                                               |
            # |                         FU payload                            |
            # |                                                               |
            # |                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # |                               :...OPTIONAL RTP padding        |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            fu_header = rtp.payload[1]
            start_bit = fu_header >> 7
            end_bit = fu_header >> 6 & 0b0001
            original_nal = fu_header & 0b0001_1111 # 1: frame P, 5: frame I
            if self.log_type & RtspClientMsgType.RTP:
                logfunc(f'  video payload forbidden_bit={forbidden_bit}, nri={nri_type}, nal={nalu_type}, payload[1] fu_header start_bit={start_bit}, end_bit={end_bit}, org_nal={original_nal}')
            self.handle_rtp_fu(end_bit, rtp, frames)
        elif nalu_type == H264RTPNalUnitType.FUB.value: # 29
            #  0                   1                   2                   3
            #  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # | FU indicator  |   FU header   |               DON             |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-|
            # |                                                               |
            # |                         FU payload                            |
            # |                                                               |
            # |                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # |                               :...OPTIONAL RTP padding        |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            pass # not implemented
        else:
            if self.log_type & RtspClientMsgType.RTP:
                logfunc(f'  rtp payload[0] fu_indicator forbidden_bit={forbidden_bit}, nri={nri_type}, nal={nalu_type}')
            self.handle_rtp_not_fu(rtp, frames)
            seq_no = SeqNo(rtp.sequence_number, rtp.recv_tick)
            frame = VideoFrame(self.codec_name_lower, nalu_type, rtp.timestamp, bytearray(START_BYTES), seq_no, seq_no)
            frame.data.extend(rtp.payload)
            frames.append(frame)
        return frames

    def parse_fu_payload_header(self, rtp: RTP) -> Tuple[int, int, int]:
        fu_header = rtp.payload[1]
        start_bit = fu_header >> 7
        end_bit = fu_header >> 6 & 0b0001
        nal_unit_type = fu_header & 0b0001_1111
        return start_bit, end_bit, nal_unit_type

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int):
        frame.data.append((rtp.payload[0] & 0b1110_0000) | nalu_type)

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP):
        frame.data.extend(rtp.payload[2:]) # 1 byte fu-indicator and 1 byte fu-header


class H265Splicer(VideoSplicer):
    """H.265/HEVC RTP payload splicer based on RFC 7798."""

    def __init__(self, log_type: int = 0):
        super().__init__(log_type)
        self.codec_name_lower = H265CodecName
        self.has_donl_field = False

    def try_get_video_frame(self, rtp: RTP) -> Union[List[VideoFrame], None]:
        '''
        rfc7798
        +---------------+---------------+
        |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        |F|   Type    |  LayerId  | TID |
        +-------------+-----------------+
        https://ffmpeg.org/doxygen/4.4/rtpdec__hevc_8c_source.html
        '''
        frames = []
        forbidden_bit = rtp.payload[0] >> 7
        nal_uint_type = rtp.payload[0] >> 1 & 0b0011_1111 # 6 bits
        layer_id = ((rtp.payload[0] & 0b0001) << 5) | (rtp.payload[1] >> 3) # 6 bits, Required to be 0 in [HEVC]
        tid = rtp.payload[1] & 0b0111 # TID value of 0 is illegal to ensure that there is at least one bit in the NAL unit header equal to 1

        if nal_uint_type == H265RTPNalUnitType.FU.value: # 49
            #  0                   1                   2                   3
            #  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # |    PayloadHdr (Type=49)       |   FU header   | DONL (cond)   |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-|
            # | DONL (cond)   |                                               |
            # |-+-+-+-+-+-+-+-+                                               |
            # |                         FU payload                            |
            # |                                                               |
            # |                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            # |                               :...OPTIONAL RTP padding        |
            # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            fu_header = rtp.payload[2]
            start_bit = fu_header >> 7
            end_bit = fu_header >> 6 & 0b0001
            original_nalu = fu_header & 0b0011_1111 # [16,40] I frame, other P frame
            if self.log_type & RtspClientMsgType.RTP:
                logfunc(f'  video payload forbidden_bit={forbidden_bit}, nal={nal_uint_type}, layer_id={layer_id}, tid={tid}, fu_header start_bit={start_bit}, end_bit={end_bit}, org_nal={original_nalu}')
            self.handle_rtp_fu(end_bit, rtp, frames)
        else:
            if self.log_type & RtspClientMsgType.RTP:
                logfunc(f'  video payload forbidden_bit={forbidden_bit}, nal={nal_uint_type}, layer_id={layer_id}, tid={tid}')
            self.handle_rtp_not_fu(rtp, frames)
            if nal_uint_type == H265RTPNalUnitType.AP.value: # 48
                # 0                   1                   2                   3
                # 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |                          RTP Header                           |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |   PayloadHdr (Type=48)        |        NALU 1 DONL            |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |          NALU 1 Size          |            NALU 1 HDR         |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |                                                               |
                # |                 NALU 1 Data   . . .                           |
                # |                                                               |
                # +     . . .     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |               |  NALU 2 DOND  |          NALU 2 Size          |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |          NALU 2 HDR           |                               |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+          NALU 2 Data          |
                # |                                                               |
                # |        . . .                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                # |                               :...OPTIONAL RTP padding        |
                # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                if self.log_type & RtspClientMsgType.RTP:
                    logfunc(f'  video payload forbidden_bit={forbidden_bit}, nal={nal_uint_type}, layer_id={layer_id}, tid={tid}')
                index = 2 # playload header size
                payload_size = len(rtp.payload)
                if self.has_donl_field:
                    index += RTP_HEVC_DONL_FIELD_SIZE
                while True:
                    nalu_size = struct.unpack('!H', rtp.payload[index:index+2])[0]
                    index += 2
                    nalu_header = rtp.payload[index:index+2]
                    original_nal_uint_type = nalu_header[0] >> 1 & 0b0011_1111
                    if self.log_type & RtspClientMsgType.RTP:
                        logfunc(f'    AP nal={original_nal_uint_type}')
                    seq_no = SeqNo(rtp.sequence_number, rtp.recv_tick)
                    frame = VideoFrame(self.codec_name_lower, original_nal_uint_type, rtp.timestamp, bytearray(START_BYTES), seq_no, seq_no)
                    frame.data.extend(rtp.payload[index:index+nalu_size])
                    frames.append(frame)
                    index += nalu_size
                    if index >= payload_size:
                        break
                    if self.has_donl_field:
                        index += RTP_HEVC_DOND_FIELD_SIZE
            elif nal_uint_type == H265RTPNalUnitType.PACI.value: # 50
                pass # not implemented
            else: # not 48,49,50
                seq_no = SeqNo(rtp.sequence_number, rtp.recv_tick)
                frame = VideoFrame(self.codec_name_lower, nal_uint_type, rtp.timestamp, bytearray(START_BYTES), seq_no, seq_no)
                frame.data.extend(rtp.payload)
                frames.append(frame)
        return frames

    def parse_fu_payload_header(self, rtp: RTP) -> Tuple[int, int, int]:
        fu_header = rtp.payload[2]
        start_bit = fu_header >> 7
        end_bit = fu_header >> 6 & 0b0001
        nal_unit_type = fu_header & 0b0011_1111 # [16,40] I frame, other P frame
        return start_bit, end_bit, nal_unit_type

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int):
        frame.data.append((rtp.payload[0] & 0b1000_0001) | (nalu_type << 1))
        frame.data.append(rtp.payload[1])

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP):
        if self.has_donl_field: # skip 2 donl bytes
            frame.data.extend(rtp.payload[5:])
        else:
            frame.data.extend(rtp.payload[3:]) # 2 bytes payload header and 1 byte fu-header


class RtspClient:
    """Low-level RTSP client responsible for socket I/O and RTP parsing.

    Applications should usually consume :class:`RtspSession` instead of this
    lower-level helper directly.
    """

    def __init__(self, url: str, forward_address: aio.IPAddress = None, total_time: int = 10):
        #: Value sent in RTSP request headers.
        self.user_agent = 'python aio rtsp client'
        parse_result = urllib.parse.urlparse(url)
        self.host = parse_result.hostname
        self.port = parse_result.port if parse_result.port else 554
        self.auth_method = ''
        self.auth_params = {}
        self.username = parse_result.username
        self.password = parse_result.password
        if self.username or self.password:
            slash_index = url.find('//')
            at_index = url.find('@')
            self.url = url[:slash_index+2] + url[at_index+1:]
        else:
            self.url = url
        if forward_address:
            self.host, self.port = forward_address
        self.content_base: str = ''
        self.sock: aio.TCPSocket = None
        self.local_addr: Tuple[str, int] = None
        self.rtsp_version = 'RTSP/1.0'
        self.rtsp_version_b = self.rtsp_version.encode('utf-8')
        #: Parsed SDP keyed by media type after DESCRIBE succeeds.
        self.sdp: Dict[str, Any] = {}
        self.cseq = 1
        self.session: str = ''
        #: Interleaved channel numbers for RTSP-over-TCP transport.
        self.video_rtp_channel = 0
        self.video_rtcp_channel = 1
        self.audio_rtp_channel = 2
        self.audio_rtcp_channel = 3
        self.media_channels = {
            self.video_rtp_channel: 'video_rtp',
            self.video_rtcp_channel: 'video_rtcp',
            self.audio_rtp_channel: 'audio_rtp',
            self.audio_rtcp_channel: 'audio_rtcp',
        }
        self.timeout: float = 4
        self.total_time = total_time
        #: Receive buffer used for RTSP headers, RTSP bodies, and interleaved RTP packets.
        self.recv_buffer = bytearray()
        self.recv_buf_start = 0
        self.log_type = RtspClientMsgType.ConnectResult|RtspClientMsgType.RTSP
        self.tick = Tick()
        self.video_splicer: VideoSplicer = None
        self.audio_splicer: AudioSplicer = None
        self.rtps_before_play_response: List[Tuple[int, RTP]] = []

    async def connect(self) -> bool:
        """Open the RTSP TCP connection."""

        if self.log_type & RtspClientMsgType.ConnectResult:
            logfunc(f'connect {self.host}:{self.port}')
        self.tick.reset()

        self.sock = await asyncio.wait_for(aio.open_tcp_connection(self.host, self.port), timeout=self.timeout)
        self.local_addr = self.sock.getsockname()
        if self.log_type & RtspClientMsgType.ConnectResult:
            logfunc(f'connected local_addr={self.local_addr}')

    def check_auth(self, rtsp_resp: RtspResponse):
        """Parse a 401 authentication challenge from an RTSP response."""

        if rtsp_resp.status_code == 401:
            if not self.username or not self.password:
                return

            def parse_auth(auth: str):
                '''
                WWW-Authenticate: Basic realm="MERCURY IP-Camera"
                WWW-Authenticate: Digest realm="MERCURY IP-Camera", nonce="a697d0cb548de1b33a072820fb2732ad"
                '''
                parts = auth.split(maxsplit=1)
                self.auth_method = parts[0]
                if len(parts) == 2:
                    for param in parts[1].split(','):
                        param = param.strip()
                        key, value = param.split('=', maxsplit=1)
                        if value[0] == '"':
                            value = value[1:-1]
                        self.auth_params[key] = value

            auths = rtsp_resp.headers.get('WWW-Authenticate', None)
            if isinstance(auths, list):
                for auth in auths:
                    # if auth.startswith('Basic'):
                    #     parse_auth(auth)
                    if auth.startswith('Digest'):
                        parse_auth(auth)
            else:
                parse_auth(auths)

    def generate_digest_auth(self, realm: str, nonce: str, method: str, uri: str) -> str:
        ha1 = hashlib.md5(f'{self.username}:{realm}:{self.password}'.encode()).hexdigest()
        ha2 = hashlib.md5(f'{method}:{uri}'.encode()).hexdigest()
        response = hashlib.md5(f'{ha1}:{nonce}:{ha2}'.encode()).hexdigest()
        return f'username="{self.username}", realm="{realm}", nonce="{nonce}", uri="{uri}", response="{response}"'

    def generate_auth_header(self, method: str) -> str:
        if self.auth_method == 'Basic':
            auth = base64.b64encode(f'{self.username}:{self.password}'.encode()).decode()
            return f'\r\nAuthorization: Basic {auth}'
        elif self.auth_method == 'Digest':
            auth = self.generate_digest_auth(self.auth_params['realm'], self.auth_params['nonce'], method, self.url)
            return f'\r\nAuthorization: Digest {auth}'
        else:
            return ''

    async def options(self) -> RtspResponse:
        """Send an RTSP OPTIONS request."""

        auth_header = self.generate_auth_header('OPTIONS')
        content = f'OPTIONS {self.url} {self.rtsp_version}\r\n' \
            f'CSeq: {self.cseq}\r\n' \
            f'User-Agent: {self.user_agent}{auth_header}\r\n\r\n'
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(content)
        self.tick.update()
        await self.sock.send(content.encode('utf-8'))
        rtsp_resp = await self.wait_rstp_respone(timeout=self.timeout)
        rtsp_resp.elapsed = self.tick.since_last()
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                f'\nheaders: {rtsp_resp.headers}'
                f'\nbody: {rtsp_resp.body}')
        return rtsp_resp

    async def describe(self) -> RtspResponse:
        """Send an RTSP DESCRIBE request and parse SDP when available."""

        auth_header = self.generate_auth_header('DESCRIBE')
        self.cseq += 1
        content = f'DESCRIBE {self.url} {self.rtsp_version}\r\n'    \
            f'CSeq: {self.cseq}\r\n'    \
            f'User-Agent: {self.user_agent}\r\n'    \
            f'Accept: application/sdp{auth_header}\r\n\r\n'
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(content)
        self.tick.update()
        await self.sock.send(content.encode('utf-8'))
        rtsp_resp = await self.wait_rstp_respone(timeout=self.timeout)
        rtsp_resp.elapsed = self.tick.since_last()
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                f'\nheaders: {rtsp_resp.headers}'
                f'\nbody: {rtsp_resp.body}')
        if rtsp_resp.status_code != 200:
            return rtsp_resp
        self.content_base = rtsp_resp.headers.get('Content-Base', '')
        media_info = None
        '''
v=0
o=- 14665860 31787219 1 IN IP4 192.168.1.63
s=Session streamed by "MERCURY RTSP Server"
t=0 0
m=video 0 RTP/AVP 96
c=IN IP4 0.0.0.0
b=AS:4096
a=range:npt=0-
a=control:track1
a=rtpmap:96 H264/90000
a=fmtp:96 packetization-mode=1; profile-level-id=640016; sprop-parameter-sets=Z2QAFqzSAoC/5YQAAAMABAAAAwB6EA==,aOqPLA==
m=audio 0 RTP/AVP 8
a=rtpmap:8 PCMA/8000
a=control:track2
m=application/MERCURY 0 RTP/AVP smart/1/90000
a=rtpmap:95 MERCURY/90000
a=control:track3
        '''
        for line in rtsp_resp.body.splitlines():
            if line.startswith('m='):
                space_index= line.find(' ')
                mediaType = line[2:space_index] # video or audio or other
                self.sdp[mediaType] = {}
                media_info = self.sdp[mediaType]
                parts = line[space_index+1:].split() # 0 RTP/AVP 96
                media_info['transport'] = parts[1]
                media_info['payload'] = int(parts[2]) if parts[2].isdigit() else parts[2]
            elif line.startswith('a=control:'): # a=control:track1
                control_url = line[10:]
                if control_url == '*':
                    control_url = self.content_base
                elif not control_url.startswith('rtsp://'):
                    control_url = f'{self.content_base}{control_url}'
                if media_info is None:
                    self.sdp['control'] = control_url
                else:
                    media_info['control'] = control_url
            elif line.startswith('a=rtpmap:'):
                parts = line[9:].split()
                media_info['rtpmap'] = int(parts[0])
                parts = parts[1].split('/')
                media_info['codec_name'] = parts[0]
                media_info['clock_rate'] = int(parts[1])
                if len(parts) == 3:
                    media_info['channel'] = int(parts[2])
                if media_info['codec_name'] == 'H264':
                    self.video_splicer = H264Splicer(self.log_type)
                elif media_info['codec_name'] == 'H265':
                    self.video_splicer = H265Splicer(self.log_type)
            elif line.startswith('a=fmtp:'): # format parameters
                fmtp = {}
                for it in line[line.find(' '):].split(';'):
                    it = it.strip()
                    if not it or '=' not in it:
                        continue
                    key, value = it.split('=', 1)
                    key = key.lower()
                    if value.isdigit() and key not in ('config', 'profile-level-id', 'mode'):
                        value = int(value)
                    fmtp[key] = value
                media_info['fmtp'] = fmtp
                if isinstance(self.video_splicer, H265Splicer) and fmtp.get('sprop-max-don-diff', 0) > 0:
                    self.video_splicer.has_donl_field = True
            elif line.startswith('a=framerate:'):
                media_info['framerate'] = int(line[12:])
            # if sprop-max-don-diff > 0 in sdp, H265 has DONL field(2 bytes after playload header)
        video_sdp = self.sdp.get('video', None)
        fmtp = video_sdp.get('fmtp', None) if video_sdp else None
        if fmtp and video_sdp:
            sps_pps = fmtp.get('sprop-parameter-sets', None)
            if sps_pps:
                sps, pps = sps_pps.split(',')
                sps = base64.b64decode(sps)
                pps = base64.b64decode(pps)
                video_sdp['sps'] = ensure_start_code(sps)
                video_sdp['pps'] = ensure_start_code(pps)
        audio_sdp = self.sdp.get('audio', None)
        if audio_sdp and 'codec_name' not in audio_sdp:
            payload = audio_sdp.get('payload', None)
            if payload == 8:
                audio_sdp['codec_name'] = 'PCMA'
                audio_sdp['clock_rate'] = 8000
                audio_sdp['channel'] = 1
            elif payload == 0:
                audio_sdp['codec_name'] = 'PCMU'
                audio_sdp['clock_rate'] = 8000
                audio_sdp['channel'] = 1
        self.audio_splicer = create_audio_splicer(audio_sdp, self.log_type)
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(f'sdp:\n{self.sdp}')
        rtsp_resp.sdp = self.sdp
        return rtsp_resp

    async def setup(self, audio: bool = True, video: bool = True) -> Dict[str, RtspResponse]:
        assert audio or video, 'at least one stream must be enabled'
        auth_header = self.generate_auth_header('SETUP')
        rtsp_rtsps = {}
        video_sdp = self.sdp.get('video', None)
        if video_sdp and video:
            self.cseq += 1
            content = f'SETUP {video_sdp["control"]} {self.rtsp_version}\r\n'   \
                f'CSeq: {self.cseq}\r\n' \
                f'User-Agent: {self.user_agent}{auth_header}\r\n' \
                f'Transport: {video_sdp["transport"]}/TCP;unicast;interleaved={self.video_rtp_channel}-{self.video_rtcp_channel}\r\n\r\n'
            if self.log_type & RtspClientMsgType.RTSP:
                logfunc(content)
            self.tick.update()
            await self.sock.send(content.encode('utf-8'))
            rtsp_resp = await self.wait_rstp_respone(timeout=self.timeout)
            rtsp_resp.elapsed = self.tick.since_last()
            rtsp_rtsps['video'] = rtsp_resp
            if self.log_type & RtspClientMsgType.RTSP:
                logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                    f'\nheaders: {rtsp_resp.headers}'
                    f'\nbody: {rtsp_resp.body}')
            self.session = rtsp_resp.headers.get('Session', self.session).split(';')[0]
        audio_sdp = self.sdp.get('audio', None)
        if audio_sdp and audio:
            self.cseq += 1
            content = f'SETUP {audio_sdp["control"]} {self.rtsp_version}\r\n'   \
                f'CSeq: {self.cseq}\r\n' \
                f'User-Agent: {self.user_agent}\r\n' \
                f'Transport: {audio_sdp["transport"]}/TCP;unicast;interleaved={self.audio_rtp_channel}-{self.audio_rtcp_channel}\r\n'   \
                f'Session: {self.session}{auth_header}\r\n\r\n'

            if self.log_type & RtspClientMsgType.RTSP:
                logfunc(content)
            self.tick.update()
            await self.sock.send(content.encode('utf-8'))
            rtsp_resp = await self.wait_rstp_respone(timeout=self.timeout)
            rtsp_resp.elapsed = self.tick.since_last()
            rtsp_rtsps['audio'] = rtsp_resp
            if self.log_type & RtspClientMsgType.RTSP:
                logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                    f'\nheaders: {rtsp_resp.headers}'
                    f'\nbody: {rtsp_resp.body}')
            self.session = rtsp_resp.headers.get('Session', self.session).split(';')[0]
        return rtsp_rtsps

    async def play(self) -> RtspResponse:
        auth_header = self.generate_auth_header('PLAY')
        self.cseq += 1
        content = f'PLAY {self.url } {self.rtsp_version}\r\n'    \
            f'CSeq: {self.cseq}\r\n' \
            f'User-Agent: {self.user_agent}\r\n' \
            f'Session: {self.session}\r\n'  \
            f'Range: npt=0.000-{auth_header}\r\n\r\n'
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(content)
        self.tick.update()
        await self.sock.send(content.encode('utf-8'))

        # some servers may send rtp packets first then play response
        while True:
            ret = await self.try_get_rtp()
            if isinstance(ret, int):
                if ret == -1: # next buffer is not rtp
                    break
                await self.recv_some_to_buffer(timeout=self.timeout)
            else: # tuple
                # channel, rtp = ret
                # some servers send rtp before play response, we need to save them and handle after play response
                self.rtps_before_play_response.append(ret)

        rtsp_resp = await self.wait_rstp_respone(timeout=self.timeout)
        rtsp_resp.elapsed = self.tick.since_last()
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                f'\nheaders: {rtsp_resp.headers}'
                f'\nbody: {rtsp_resp.body}')
        return rtsp_resp

    async def teardown(self, wait_response: bool = True) -> None:
        '''
        After sending teardown, client is still receiving rtp packets.
        We need to get response util there is no rtp.
        If you want to stop immediately, set wait_response to False, it will ignore the left rtp packets and response.
        '''
        auth_header = self.generate_auth_header('TEARDOWN')
        self.cseq += 1
        content = f'TEARDOWN {self.url} {self.rtsp_version}\r\n'    \
            f'CSeq: {self.cseq}\r\n'    \
            f'User-Agent: {self.user_agent}\r\n'    \
            f'Session: {self.session}{auth_header}\r\n\r\n'
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(content)
        self.tick.update()
        await self.sock.send(content.encode('utf-8'))
        # handle recv rtp
        # handle teardown resp

    async def wait_teardown_response(self, timeout=0.5) -> RtspResponse:
        self.shrink_buffer(True)
        # logfunc(f'recv_buffer={self.recv_buffer}')
        rtsp_resp = await self.wait_rstp_respone(timeout=timeout)
        rtsp_resp.elapsed = self.tick.since_last()
        if self.log_type & RtspClientMsgType.RTSP:
            logfunc(f'{self.local_addr} recv:\nstatus code: {rtsp_resp.status_code}'
                f'\nheaders: {rtsp_resp.headers}'
                f'\nbody: {rtsp_resp.body}')
        return rtsp_resp

    async def close(self):
        if self.sock:
            await self.sock.close()

    async def wait_rstp_respone(self, timeout: float) -> RtspResponse:
        tick = time.perf_counter()
        while True:
            status_code, headers = self.try_parse_rtsp_response()
            if status_code is None:
                left_time = timeout - (time.perf_counter() - tick)
                if left_time <= 0:
                    raise RtspTimeoutError('RTSP receive timeout while waiting for RTSP response')
                await self.recv_some_to_buffer(timeout=left_time)
            else:
                rtsp_resp = RtspResponse(status_code, headers)
                content_len = headers.get('Content-Length', 0)
                if content_len > 0:
                    recv_len = len(self.recv_buffer) - self.recv_buf_start
                    if recv_len < content_len:
                        left_time = timeout - (time.perf_counter() - tick)
                        if left_time <= 0:
                            raise RtspTimeoutError('RTSP receive timeout while waiting for RTSP response')
                        await self.recv_exactly_to_buffer(content_len-recv_len, timeout=left_time)
                    rtsp_resp.body = self.recv_buffer[self.recv_buf_start:self.recv_buf_start + content_len].decode('utf-8')
                    self.recv_buf_start = self.recv_buf_start + content_len
                return rtsp_resp

    def try_parse_rtsp_response(self) -> Tuple[Union[int,None], Dict[str, str]]:
        if len(self.recv_buffer) == self.recv_buf_start:
            return None, None
        assert self.recv_buffer.startswith(self.rtsp_version_b, self.recv_buf_start), f'not valid response:{self.recv_buffer[self.recv_buf_start:]}'
        index = self.recv_buffer.find(b'\r\n\r\n', self.recv_buf_start)
        if index > self.recv_buf_start:
            resp_lines = self.recv_buffer[self.recv_buf_start:index].decode('utf-8').splitlines()
            resp_code = int(resp_lines[0].split()[1]) # RTSP/1.0 200 OK, RTSP/1.0 401 Unauthorized
            headers = {}
            for line in resp_lines[1:]:
                key, value = line.split(':', 1)
                value = value.lstrip()
                if key in ['CSeq', 'Content-Length']:
                    value = int(value)
                mvalue = headers.get(key, None)
                if mvalue is None:
                    headers[key] = value
                else:
                    if isinstance(mvalue, list):
                        mvalue.append(value)
                    else:
                        headers[key] = [mvalue, value]
            self.recv_buf_start = index + 4
            return resp_code, headers
        return None, None

    # async def recv_util(self, want_len: int, timeout: float):
    #     recv_len = len(self.recv_buffer) - self.recv_buf_start
    #     if recv_len >= want_len:
    #         return
    #     self.recv_exactly_to_buffer(want_len-recv_len, timeout=timeout)

    async def recv_some_to_buffer(self, timeout: float = 4) -> None:
            try:
                recv_data = await asyncio.wait_for(self.sock.recv(), timeout=timeout)
            except (TimeoutError, asyncio.TimeoutError) as ex:
                if self.log_type & RtspClientMsgType.Exception:
                    logfunc(f'recv timeout exception: {ex!r}')
                raise RtspTimeoutError('RTSP receive timeout while waiting for RTP data') from ex
            if not recv_data:
                if self.log_type & RtspClientMsgType.Exception:
                    logfunc('rtsp connection closed by peer')
                raise RtspConnectionError('RTSP connection closed by peer while waiting for RTP data')
            self.recv_buffer.extend(recv_data)

    async def recv_exactly_to_buffer(self, n, timeout: float = 4) -> None:
        try:
            recv_data = await asyncio.wait_for(self.sock.recv_exactly(n), timeout=timeout)
        except (TimeoutError, asyncio.TimeoutError) as ex:
            if self.log_type & RtspClientMsgType.Exception:
                logfunc(f'recv timeout exception: {ex!r}')
            raise RtspTimeoutError('RTSP receive timeout while waiting for RTP data') from ex
        if not recv_data:
            if self.log_type & RtspClientMsgType.Exception:
                logfunc('rtsp connection closed by peer')
            raise RtspConnectionError('RTSP connection closed by peer while waiting for RTP data')
        self.recv_buffer.extend(recv_data)

    async def try_get_rtp(self) -> Union[int, Tuple[int, RTP]]:
        recv_len = len(self.recv_buffer) - self.recv_buf_start
        if recv_len >= 4:
            magic, channel, length = struct.unpack('!BBH', self.recv_buffer[self.recv_buf_start:self.recv_buf_start+4])
            if magic == 0x24:# and channel in self.media_channels:
                # if self.verbose & RtspClientMsgType.RTP:
                #     logfunc(f' {magic} {channel} {length} {self.media_channels[channel]}')
                if (recv_len < 4 + length):
                    return recv_len
                rtp = bytes(memoryview(self.recv_buffer)[self.recv_buf_start+4:self.recv_buf_start+4+length])
                self.recv_buf_start += 4 + length
                if channel == self.video_rtp_channel or channel == self.audio_rtp_channel:
                    return channel, self.parse_rtp(rtp)
                else: # video_rtcp, audio_rtcp
                    return channel, None
            else:
                return -1
        return recv_len

    def parse_rtp(self, rtp_data: bytes) -> RTP:
        header_length = 12
        version_padding_extension, mark_payload, sequence_number, timestamp, ssrc = struct.unpack("!BBHII", rtp_data[:header_length])
        version = (version_padding_extension >> 6) & 0b0011
        padding = (version_padding_extension >> 5) & 0x01
        extension = (version_padding_extension >> 4) & 0x01
        csic = version_padding_extension & 0b1111
        marker = mark_payload >> 7
        payload_type = mark_payload & 0b0111_1111
        padding_length = 0
        if padding:
            padding_length = rtp_data[-1]
        payload = rtp_data[header_length:len(rtp_data) - padding_length]
        rtp = RTP(version, padding, extension, csic, marker, payload_type, sequence_number,
                        timestamp, ssrc, payload)
        rtp.recv_tick = Tick.process_tick()
        return rtp

    def shrink_buffer(self, force: bool = False):
        if self.recv_buf_start > 102400 or (force and self.recv_buf_start > 0):
            self.recv_buffer = self.recv_buffer[self.recv_buf_start:]
            self.recv_buf_start = 0


def _is_stop_requested(stop_event: Optional[StopEventLike]) -> bool:
    return bool(stop_event is not None and hasattr(stop_event, 'is_set') and stop_event.is_set())


class RtspSession:
    """High-level async RTSP session API.

    Use this class with ``async with`` and consume events from
    :meth:`iter_events` or :meth:`events`.

    Args:
        rtsp_url: RTSP URL to connect to.
        forward_address: Optional TCP target used instead of the parsed RTSP host.
        timeout: Timeout in seconds for connection, RTSP methods, and RTP receive waits.
        log_type: Bit flags from :class:`RtspClientMsgType` controlling low-level logging.
        enable_video: Whether to SETUP and emit video stream events.
        enable_audio: Whether to SETUP and emit audio stream events.

    Note:
        At least one of ``enable_video`` or ``enable_audio`` must be ``True``.
    """

    def __init__(self, rtsp_url: str, forward_address: aio.IPAddress = None, timeout: float = 4,
                 log_type: int = RtspClientMsgType.RTSP, enable_video: bool = True, enable_audio: bool = True):
        #: Original RTSP URL provided by the caller.
        self.rtsp_url = rtsp_url
        #: Optional TCP forward target used instead of the parsed RTSP host.
        self.forward_address = forward_address
        #: Timeout in seconds for connect, RTSP methods, and RTP receive waits.
        self.timeout = timeout
        #: Bit flags controlling low-level logging.
        self.log_type = log_type
        #: Whether to SETUP and emit video media events.
        self.enable_video = enable_video
        #: Whether to SETUP and emit audio media events.
        self.enable_audio = enable_audio
        self._client: Optional[RtspClient] = None
        self._closed = False

    async def __aenter__(self) -> 'RtspSession':
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @property
    def client(self) -> Optional[RtspClient]:
        """Expose the underlying low-level client after the session starts."""

        return self._client

    @property
    def sdp(self) -> Dict[str, Any]:
        """Return parsed SDP data if DESCRIBE has already succeeded."""

        if self._client is None:
            return {}
        return self._client.sdp

    async def close(self) -> None:
        """Close the underlying socket if the session is active."""

        if self._client is None or self._closed:
            return
        await self._client.close()
        self._closed = True

    async def iter_events(self, stop_event: Optional[StopEventLike] = None) -> AsyncGenerator[RtspEvent, None]:
        """Run the RTSP session and yield typed public events.

        Args:
            stop_event: Optional ``threading.Event`` or ``asyncio.Event`` used
                to request a graceful stop from outside the session loop.
        """

        if not self.enable_video and not self.enable_audio:
            raise ValueError('At least one of enable_video or enable_audio must be True')

        self._client = RtspClient(self.rtsp_url, self.forward_address)
        self._client.timeout = self.timeout
        self._client.log_type = self.log_type
        self._closed = False
        rtsp = self._client

        try:
            try:
                await rtsp.connect()
                ex = None
            except (TimeoutError, asyncio.TimeoutError) as ex:
                if self.log_type & RtspClientMsgType.Exception:
                    logfunc(f'RTSP connect timeout exception: {ex!r}')
                raise RtspTimeoutError(f'RTSP connect timed out after {self.timeout}s') from ex
            except Exception as ex:
                if self.log_type & RtspClientMsgType.Exception:
                    logfunc(f'RTSP connect exception: {ex!r}')
                raise RtspConnectionError(f'RTSP connect failed: {ex!r}') from ex

            connect_elapsed = rtsp.tick.since_last()
            yield ConnectResultEvent(
                event='connect_result',
                msg_type=RtspClientMsgType.ConnectResult,
                session_elapsed=get_session_elapsed(rtsp.tick),
                local_addr=rtsp.local_addr,
                exception=ex,
                elapsed=connect_elapsed,
            )

            rtsp_resp = await self._run_method('OPTIONS', rtsp.options)
            yield self._new_method_event('OPTIONS', rtsp_resp)

            rtsp.check_auth(rtsp_resp)
            if rtsp_resp.status_code == 401 and rtsp.auth_method and rtsp.username and rtsp.password:
                rtsp_resp = await self._run_method('OPTIONS', rtsp.options)
                yield self._new_method_event('OPTIONS', rtsp_resp)
            self._ensure_ok('OPTIONS', rtsp_resp)

            rtsp_resp = await self._run_method('DESCRIBE', rtsp.describe)
            yield self._new_method_event('DESCRIBE', rtsp_resp)

            rtsp.check_auth(rtsp_resp)
            if rtsp_resp.status_code == 401 and rtsp.auth_method and rtsp.username and rtsp.password:
                rtsp_resp = await self._run_method('DESCRIBE', rtsp.describe)
                yield self._new_method_event('DESCRIBE', rtsp_resp)
            self._ensure_ok('DESCRIBE', rtsp_resp)

            for media_type, setup_resp in (await self._run_method(
                'SETUP',
                rtsp.setup,
                audio=self.enable_audio,
                video=self.enable_video,
            )).items():
                yield self._new_method_event('SETUP', setup_resp, media_type=media_type)
                self._ensure_ok('SETUP', setup_resp, media_type)

            rtsp_resp = await self._run_method('PLAY', rtsp.play)
            yield self._new_method_event('PLAY', rtsp_resp)
            self._ensure_ok('PLAY', rtsp_resp)

            if rtsp.rtps_before_play_response:
                logfunc(f'got {len(rtsp.rtps_before_play_response)} rtp packets before play response')
                for channel, rtp in rtsp.rtps_before_play_response:
                    rtp.recv_tick = Tick.process_tick()
                    yield self._new_rtp_event(channel, rtp)
                rtsp.rtps_before_play_response.clear()

            async for event in self._recv_loop(stop_event=stop_event):
                yield event

            await rtsp.teardown()

            async for event in self._recv_loop(stop_event=stop_event, max_time=0.2):
                yield event

            rtsp_resp = await self._run_method('TEARDOWN', rtsp.wait_teardown_response, timeout=0.01)
            yield self._new_method_event('TEARDOWN', rtsp_resp)

            yield ClosedEvent(
                event='closed',
                msg_type=RtspClientMsgType.Closed,
                session_elapsed=get_session_elapsed(rtsp.tick),
            )
        finally:
            await self.close()

    async def _run_method(self, method: str, func: Callable, *args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except RtspError:
            raise
        except Exception as ex:
            raise RtspProtocolError(f'RTSP {method} failed: {ex!r}') from ex

    def _new_method_event(self, method: str, response: RtspResponse, media_type: Optional[str] = None) -> RtspMethodEvent:
        return RtspMethodEvent(
            event='rtsp_method',
            msg_type=RtspClientMsgType.RTSP,
            session_elapsed=get_session_elapsed(self._client.tick),
            method=method,
            response=response,
            media_type=media_type,
        )

    def _new_rtp_event(self, channel: int, rtp: Optional[RTP]) -> RtpPacketEvent:
        return RtpPacketEvent(
            event='rtp_packet',
            msg_type=RtspClientMsgType.RTP,
            session_elapsed=get_session_elapsed(self._client.tick),
            channel=channel,
            media_channel=self._client.media_channels[channel],
            rtp=rtp,
        )

    def _ensure_ok(self, method: str, response: RtspResponse, media_type: Optional[str] = None) -> None:
        if response.status_code != 200:
            raise RtspResponseError(method, response.status_code, response, media_type)

    async def _recv_loop(self, stop_event: Optional[StopEventLike] = None,
                         max_time: float = 0) -> AsyncGenerator[RtspEvent, None]:
        rtsp = self._client
        start_tick = time.perf_counter()
        while True:
            try:
                ret = await rtsp.try_get_rtp()
            except Exception as ex:
                if rtsp.log_type & RtspClientMsgType.Exception:
                    logfunc(f'rtp parse exception: {ex!r}')
                raise RtspProtocolError(f'Failed while parsing RTP: {ex!r}') from ex
            if isinstance(ret, int):
                if ret == -1:
                    break
                await rtsp.recv_some_to_buffer(timeout=max_time or self.timeout)
                continue

            channel, rtp = ret
            if rtsp.log_type & RtspClientMsgType.RTP:
                logfunc(f'{rtsp.media_channels[channel]}: {rtp}')
            yield self._new_rtp_event(channel, rtp)

            if self.enable_video and channel == rtsp.video_rtp_channel:
                for vframe in rtsp.video_splicer.try_get_video_frame(rtp):
                    if rtsp.log_type & RtspClientMsgType.VideoFrame:
                        logfunc(f'{vframe}')
                    yield VideoFrameEvent(
                        event='video_frame',
                        msg_type=RtspClientMsgType.VideoFrame,
                        session_elapsed=get_session_elapsed(rtsp.tick),
                        frame=vframe,
                    )
                rtsp.shrink_buffer()
            elif self.enable_audio and channel == rtsp.audio_rtp_channel:
                if rtsp.audio_splicer:
                    for aframe in rtsp.audio_splicer.try_get_audio_frames(rtp):
                        if rtsp.log_type & RtspClientMsgType.AudioFrame:
                            logfunc(f'{aframe}')
                        yield AudioFrameEvent(
                            event='audio_frame',
                            msg_type=RtspClientMsgType.AudioFrame,
                            session_elapsed=get_session_elapsed(rtsp.tick),
                            frame=aframe,
                        )
                rtsp.shrink_buffer()

            if max_time > 0:
                if time.perf_counter() - start_tick >= max_time:
                    break
            elif _is_stop_requested(stop_event):
                break


def open_session(rtsp_url: str, forward_address: aio.IPAddress = None, timeout: float = 4,
                 log_type: int = RtspClientMsgType.RTSP, enable_video: bool = True,
                 enable_audio: bool = True) -> RtspSession:
    """Convenience factory returning a :class:`RtspSession`.

    Args:
        rtsp_url: RTSP URL to connect to.
        forward_address: Optional TCP target used instead of the host and port
            parsed from ``rtsp_url``.

            This is useful when the RTSP URL should keep the camera's original
            address and RTSP path, but the actual TCP connection must go through
            a forwarded endpoint, relay host, or tunnel.

            Example:
                The camera URL is ``rtsp://192.168.1.122:554/stream2``.
                A computer on the same LAN as the camera is also reachable on a
                Tailscale address such as ``100.64.0.3`` and forwards traffic
                from that machine to the camera's RTSP port.

                Another computer on the virtual network can keep using the
                original RTSP URL:

                ``rtsp://192.168.1.122:554/stream2``

                but pass:

                ``forward_address=("100.64.0.3", 554)``

                so the TCP connection is opened to ``100.64.0.3:554`` while the
                RTSP request line still uses the original camera URL and path.

                In many deployments, connecting directly to
                ``rtsp://100.64.0.3:554/stream2`` may also work. ``forward_address``
                is mainly for cases where you want to preserve the original RTSP
                URL while routing the socket through a different network address.
        timeout: Timeout in seconds for connection, RTSP methods, and RTP receive waits.
        log_type: Bit flags from :class:`RtspClientMsgType` controlling low-level logging.
        enable_video: Whether to SETUP and emit video stream events.
        enable_audio: Whether to SETUP and emit audio stream events.

    Note:
        At least one of ``enable_video`` or ``enable_audio`` must be ``True``.
    """

    return RtspSession(
        rtsp_url,
        forward_address=forward_address,
        timeout=timeout,
        log_type=log_type,
        enable_video=enable_video,
        enable_audio=enable_audio,
    )
