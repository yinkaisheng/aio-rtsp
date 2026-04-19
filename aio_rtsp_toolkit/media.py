from __future__ import annotations

import struct
from typing import Any, Dict, List, Optional, Tuple

from . import types
from .types import (
    AudioFrame,
    H264CodecName,
    H264RTPNalUnitType,
    H265CodecName,
    H265RTPNalUnitType,
    HEVCCodecName,
    RTP,
    RTP_HEVC_DOND_FIELD_SIZE,
    RTP_HEVC_DONL_FIELD_SIZE,
    RtspClientMsgType,
    SeqNo,
    START_BYTES,
    VideoFrame,
)


def read_bits(data: bytes, bit_offset: int, bit_count: int) -> int:
    if bit_count <= 0:
        return 0
    value = 0
    for i in range(bit_count):
        pos = bit_offset + i
        value = (value << 1) | ((data[pos // 8] >> (7 - pos % 8)) & 0x01)
    return value


class AudioSplicer:
    def __init__(
        self,
        codec_name_lower: str,
        clock_rate: int = 0,
        channels: int = 1,
        fmtp: Optional[Dict[str, Any]] = None,
        log_type: int = 0,
        log_tag: str = "",
    ) -> None:
        self.codec_name_lower = codec_name_lower
        self.clock_rate = clock_rate
        self.channels = channels or 1
        self.fmtp = fmtp or {}
        self.log_type = log_type
        self.log_tag = log_tag

    def new_audio_frame(
        self,
        rtp: RTP,
        data: bytes,
        timestamp: Optional[int] = None,
        sample_count: int = 0,
    ) -> AudioFrame:
        seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
        return AudioFrame(
            self.codec_name_lower,
            timestamp=rtp.timestamp if timestamp is None else timestamp,
            data=data,
            first_seq=seq,
            last_seq=seq,
            sample_rate=self.clock_rate,
            channels=self.channels,
            sample_count=sample_count,
        )

    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        return []


class RawAudioSplicer(AudioSplicer):
    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        sample_count = len(rtp.payload) // max(self.channels, 1)
        return [self.new_audio_frame(rtp, rtp.payload, sample_count=sample_count)]


class AACAudioSplicer(AudioSplicer):
    def __init__(
        self,
        codec_name_lower: str,
        clock_rate: int = 0,
        channels: int = 1,
        fmtp: Optional[Dict[str, Any]] = None,
        log_type: int = 0,
        log_tag: str = "",
    ) -> None:
        super().__init__(codec_name_lower, clock_rate, channels, fmtp, log_type, log_tag)
        self.size_length = int(self.fmtp.get("sizelength", 13) or 13)
        self.index_length = int(self.fmtp.get("indexlength", 3) or 0)
        self.index_delta_length = int(self.fmtp.get("indexdeltalength", 3) or 0)
        self.cts_delta_length = int(self.fmtp.get("ctsdeltalength", 0) or 0)
        self.dts_delta_length = int(self.fmtp.get("dtsdeltalength", 0) or 0)
        self.random_access_indication = int(self.fmtp.get("randomaccessindication", 0) or 0)
        self.stream_state_indication = int(self.fmtp.get("streamstateindication", 0) or 0)

    def try_get_audio_frames(self, rtp: RTP) -> List[AudioFrame]:
        if len(rtp.payload) < 2:
            return []
        au_headers_length_bits = struct.unpack("!H", rtp.payload[:2])[0]
        if au_headers_length_bits == 0:
            return [self.new_audio_frame(rtp, rtp.payload[2:], sample_count=1024)]
        au_headers_length_bytes = (au_headers_length_bits + 7) // 8
        payload_start = 2 + au_headers_length_bytes
        if len(rtp.payload) < payload_start:
            return []
        header_data = rtp.payload[2:payload_start]
        audio_data = rtp.payload[payload_start:]
        frames = []
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


def create_audio_splicer(
    audio_sdp: Optional[Dict[str, Any]],
    log_type: int = 0,
    log_tag: str = "",
) -> Optional[AudioSplicer]:
    if not audio_sdp:
        return None
    codec_name_lower = str(audio_sdp.get("codec_name", "")).lower()
    clock_rate = int(audio_sdp.get("clock_rate", 0) or 0)
    channels = int(audio_sdp.get("channel", 1) or 1)
    fmtp = audio_sdp.get("fmtp")
    if codec_name_lower in ("pcma", "g711a", "g711-alaw"):
        return RawAudioSplicer("pcma", clock_rate or 8000, channels, fmtp, log_type, log_tag)
    if codec_name_lower in ("pcmu", "g711u", "g711-mulaw"):
        return RawAudioSplicer("pcmu", clock_rate or 8000, channels, fmtp, log_type, log_tag)
    if codec_name_lower in ("mpeg4-generic", "aac"):
        return AACAudioSplicer("mpeg4-generic", clock_rate, channels, fmtp, log_type, log_tag)
    if codec_name_lower in ("mp4a-latm", "mpeg4-latm", "aac_latm"):
        return RawAudioSplicer("mp4a-latm", clock_rate, channels, fmtp, log_type, log_tag)
    return RawAudioSplicer(codec_name_lower, clock_rate, channels, fmtp, log_type, log_tag)


def ensure_start_code(nalu: bytes) -> bytes:
    if not nalu:
        return START_BYTES
    if nalu.startswith(START_BYTES):
        return nalu
    if nalu.startswith(b"\x00\x00\x01"):
        return b"\x00" + nalu
    return START_BYTES + nalu


def _sequence_gap(prev_seq: int, cur_seq: int) -> List[int]:
    if prev_seq < cur_seq:
        return list(range(prev_seq + 1, cur_seq))
    if prev_seq == 0xFFFF and cur_seq == 0:
        return []
    return list(range(prev_seq + 1, 0xFFFF + 1)) + list(range(0, cur_seq))


class VideoSplicer:
    def __init__(self, log_type: int = 0, log_tag: str = "") -> None:
        self.codec_name_lower = ""
        self._cur_frame_timestamp = None
        self.rtp_list = []
        self.log_type = log_type
        self.log_tag = log_tag

    def try_get_video_frame(self, rtp: RTP) -> List[VideoFrame]:
        return []

    def parse_fu_payload_header(self, rtp: RTP) -> Tuple[int, int, int]:
        raise NotImplementedError

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int) -> None:
        raise NotImplementedError

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP) -> None:
        raise NotImplementedError

    def splice_video_frame(self) -> VideoFrame:
        frame = VideoFrame(self.codec_name_lower, timestamp=self._cur_frame_timestamp, data=bytearray(START_BYTES))
        prev_seq = None
        miss_seq = []
        rtp_count = len(self.rtp_list)
        for index, rtp in enumerate(self.rtp_list):
            start_bit, end_bit, nalu_type = self.parse_fu_payload_header(rtp)
            if index == 0:
                frame.first_seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
                frame.set_nalu_type(nalu_type, self.codec_name_lower)
                self.append_nalu_header_to_frame(frame, rtp, nalu_type)
                if not start_bit or end_bit:
                    miss_seq.append((rtp.sequence_number - 1) if rtp.sequence_number > 0 else 0xFFFF)
                if rtp_count == 1:
                    frame.last_seq = frame.first_seq
                    if not end_bit:
                        miss_seq.append(0 if rtp.sequence_number == 0xFFFF else (rtp.sequence_number + 1))
            elif index == rtp_count - 1:
                frame.last_seq = SeqNo(rtp.sequence_number, rtp.recv_tick)
                if end_bit:
                    if prev_seq + 1 != rtp.sequence_number:
                        miss_seq.extend(_sequence_gap(prev_seq, rtp.sequence_number))
                else:
                    miss_seq.append((rtp.sequence_number + 1) if rtp.sequence_number != 0xFFFF else 0)
            else:
                if prev_seq + 1 != rtp.sequence_number:
                    miss_seq.extend(_sequence_gap(prev_seq, rtp.sequence_number))
            self.append_fu_payload_to_frame(frame, rtp)
            prev_seq = rtp.sequence_number
        if miss_seq:
            frame.miss_seq = miss_seq
            frame.is_corrupt = True
        return frame

    def handle_rtp_fu(self, end_bit: int, rtp: RTP, frames: List[VideoFrame]) -> None:
        if self._cur_frame_timestamp is None:
            self._cur_frame_timestamp = rtp.timestamp
            self.rtp_list.append(rtp)
        else:
            if self._cur_frame_timestamp == rtp.timestamp:
                self.rtp_list.append(rtp)
            else:
                if self.rtp_list:
                    frames.append(self.splice_video_frame())
                    self.rtp_list[:] = []
                self._cur_frame_timestamp = rtp.timestamp
                self.rtp_list.append(rtp)
        if end_bit:
            frames.append(self.splice_video_frame())
            self.rtp_list[:] = []

    def handle_rtp_not_fu(self, rtp: RTP, frames: List[VideoFrame]) -> None:
        if self._cur_frame_timestamp is None:
            self._cur_frame_timestamp = rtp.timestamp
        else:
            if self._cur_frame_timestamp != rtp.timestamp and self.rtp_list:
                frames.append(self.splice_video_frame())
                self.rtp_list[:] = []
            self._cur_frame_timestamp = rtp.timestamp


class H264Splicer(VideoSplicer):
    def __init__(self, log_type: int = 0, log_tag: str = "") -> None:
        super().__init__(log_type, log_tag)
        self.codec_name_lower = H264CodecName

    def try_get_video_frame(self, rtp: RTP) -> List[VideoFrame]:
        frames: List[VideoFrame] = []
        fu_indicator = rtp.payload[0]
        forbidden_bit = fu_indicator >> 7
        nri_type = fu_indicator >> 5 & 0b0011
        nalu_type = fu_indicator & 0b0001_1111
        if nalu_type == H264RTPNalUnitType.FUA.value:
            fu_header = rtp.payload[1]
            start_bit = fu_header >> 7
            end_bit = fu_header >> 6 & 0b0001
            original_nal = fu_header & 0b0001_1111
            if self.log_type & RtspClientMsgType.RTP:
                types.logger.info(
                    f'{self.log_tag} video payload forbidden_bit={forbidden_bit}, nri={nri_type}, '
                    f'nal={nalu_type}, start_bit={start_bit}, end_bit={end_bit}, org_nal={original_nal}'
                )
            self.handle_rtp_fu(end_bit, rtp, frames)
        else:
            if self.log_type & RtspClientMsgType.RTP:
                types.logger.info(f'{self.log_tag} rtp payload forbidden_bit={forbidden_bit}, nri={nri_type}, nal={nalu_type}')
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

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int) -> None:
        frame.data.append((rtp.payload[0] & 0b1110_0000) | nalu_type)

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP) -> None:
        frame.data.extend(rtp.payload[2:])


class H265Splicer(VideoSplicer):
    def __init__(self, log_type: int = 0, log_tag: str = "") -> None:
        super().__init__(log_type, log_tag)
        self.codec_name_lower = H265CodecName
        self.has_donl_field = False

    def try_get_video_frame(self, rtp: RTP) -> List[VideoFrame]:
        frames: List[VideoFrame] = []
        forbidden_bit = rtp.payload[0] >> 7
        nal_unit_type = rtp.payload[0] >> 1 & 0b0011_1111
        layer_id = ((rtp.payload[0] & 0b0001) << 5) | (rtp.payload[1] >> 3)
        tid = rtp.payload[1] & 0b0111
        if nal_unit_type == H265RTPNalUnitType.FU.value:
            fu_header = rtp.payload[2]
            start_bit = fu_header >> 7
            end_bit = fu_header >> 6 & 0b0001
            original_nalu = fu_header & 0b0011_1111
            if self.log_type & RtspClientMsgType.RTP:
                types.logger.info(
                    f'{self.log_tag} video payload forbidden_bit={forbidden_bit}, nal={nal_unit_type}, '
                    f'layer_id={layer_id}, tid={tid}, start_bit={start_bit}, end_bit={end_bit}, org_nal={original_nalu}'
                )
            self.handle_rtp_fu(end_bit, rtp, frames)
        else:
            if self.log_type & RtspClientMsgType.RTP:
                types.logger.info(
                    f'{self.log_tag} video payload forbidden_bit={forbidden_bit}, nal={nal_unit_type}, '
                    f'layer_id={layer_id}, tid={tid}'
                )
            self.handle_rtp_not_fu(rtp, frames)
            if nal_unit_type == H265RTPNalUnitType.AP.value:
                index = 2
                payload_size = len(rtp.payload)
                if self.has_donl_field:
                    index += RTP_HEVC_DONL_FIELD_SIZE
                while True:
                    nalu_size = struct.unpack("!H", rtp.payload[index:index + 2])[0]
                    index += 2
                    nalu_header = rtp.payload[index:index + 2]
                    original_nal_unit_type = nalu_header[0] >> 1 & 0b0011_1111
                    seq_no = SeqNo(rtp.sequence_number, rtp.recv_tick)
                    frame = VideoFrame(self.codec_name_lower, original_nal_unit_type, rtp.timestamp, bytearray(START_BYTES), seq_no, seq_no)
                    frame.data.extend(rtp.payload[index:index + nalu_size])
                    frames.append(frame)
                    index += nalu_size
                    if index >= payload_size:
                        break
                    if self.has_donl_field:
                        index += RTP_HEVC_DOND_FIELD_SIZE
            else:
                seq_no = SeqNo(rtp.sequence_number, rtp.recv_tick)
                frame = VideoFrame(self.codec_name_lower, nal_unit_type, rtp.timestamp, bytearray(START_BYTES), seq_no, seq_no)
                frame.data.extend(rtp.payload)
                frames.append(frame)
        return frames

    def parse_fu_payload_header(self, rtp: RTP) -> Tuple[int, int, int]:
        fu_header = rtp.payload[2]
        start_bit = fu_header >> 7
        end_bit = fu_header >> 6 & 0b0001
        nal_unit_type = fu_header & 0b0011_1111
        return start_bit, end_bit, nal_unit_type

    def append_nalu_header_to_frame(self, frame: VideoFrame, rtp: RTP, nalu_type: int) -> None:
        frame.data.append((rtp.payload[0] & 0b1000_0001) | (nalu_type << 1))
        frame.data.append(rtp.payload[1])

    def append_fu_payload_to_frame(self, frame: VideoFrame, rtp: RTP) -> None:
        if self.has_donl_field:
            frame.data.extend(rtp.payload[5:])
        else:
            frame.data.extend(rtp.payload[3:])
