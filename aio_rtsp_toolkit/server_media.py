from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import av

from .types import H264CodecName, H265CodecName


ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".wav", ".aac"}
PCMA_CLOCK_RATE = 8000


class MediaSourceError(Exception):
    pass


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
    stream_kind: str = "file"
    stream_id: str = ""

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


class MediaSourceLoader:
    def __init__(self, session_name: str) -> None:
        self.session_name = session_name
        self._cache: Dict[Tuple[Path, int, int], MediaSource] = {}

    def load(self, file_path: Path) -> MediaSource:
        stat = file_path.stat()
        cache_key = (file_path, stat.st_mtime_ns, stat.st_size)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        suffix = file_path.suffix.lower()
        if suffix == ".wav":
            media_source = self._load_wav_source(file_path)
        else:
            media_source = self._load_av_source(file_path)
        self._cache = {cache_key: media_source}
        return media_source

    def _load_wav_source(self, file_path: Path) -> MediaSource:
        with av.open(str(file_path), mode="r") as container:
            audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
            if audio_stream is None:
                raise MediaSourceError("No audio stream found in wav file")
        track = MediaTrackInfo(
            media_type="audio",
            control="track1",
            codec_name="pcma",
            payload_type=8,
            clock_rate=PCMA_CLOCK_RATE,
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
                    if stream.type == "video"
                    and normalize_codec_name(stream.codec_context.name) in (H264CodecName, H265CodecName)
                ),
                None,
            )
            audio_stream = next(
                (
                    stream
                    for stream in container.streams
                    if stream.type == "audio"
                    and normalize_codec_name(stream.codec_context.name) in ("mpeg4-generic", "pcma")
                ),
                None,
            )
            if video_stream is None and audio_stream is None:
                raise MediaSourceError("No supported H264/H265 video or AAC/PCMA audio track found")

            if video_stream is not None:
                codec_name = normalize_codec_name(video_stream.codec_context.name)
                nal_length_size, params = parse_parameter_sets(
                    codec_name, bytes(video_stream.codec_context.extradata or b"")
                )
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
                        raise MediaSourceError("AAC stream is missing AudioSpecificConfig extradata")
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
