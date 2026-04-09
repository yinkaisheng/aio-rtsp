"""Optional audio decode helpers for aio-rtsp-toolkit."""

import fractions
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import av
import numpy

from .audio_codecs import decode_g711_to_pcm16_bytes
from .tick import Tick
from .client import logger


def get_av_audio_codec_params(audio_sdp: Dict[str, Any]) -> Tuple[str, Optional[bytes]]:
    codec_name_lower = str(audio_sdp.get('codec_name', '')).lower()
    fmtp = audio_sdp.get('fmtp', {}) or {}
    config = fmtp.get('config', None)
    extradata = None
    if isinstance(config, str) and config:
        config = config.strip()
        if len(config) % 2 == 1:
            config = '0' + config
        extradata = bytes.fromhex(config)
    elif isinstance(config, int):
        hex_config = f'{config:X}'
        if len(hex_config) % 2 == 1:
            hex_config = '0' + hex_config
        extradata = bytes.fromhex(hex_config)

    if codec_name_lower in ('pcma', 'g711a', 'g711-alaw'):
        return 'pcm_alaw', None
    if codec_name_lower in ('pcmu', 'g711u', 'g711-mulaw'):
        return 'pcm_mulaw', None
    if codec_name_lower in ('mpeg4-generic', 'aac'):
        return 'aac', extradata
    if codec_name_lower in ('mp4a-latm', 'mpeg4-latm', 'aac_latm'):
        return 'aac_latm', extradata
    return '', extradata


def get_frame_channel_count(frame, fallback: int = 1) -> int:
    try:
        return len(frame.layout.channels)
    except Exception:
        pass
    try:
        return frame.layout.nb_channels
    except Exception:
        pass
    try:
        arr = frame.to_ndarray()
        if arr.ndim == 1:
            return fallback
        if arr.ndim == 2:
            return arr.shape[0] if arr.shape[0] <= 8 else fallback
    except Exception:
        pass
    return fallback


def normalize_audio_array(arr, channel_count: int):
    if arr.ndim == 1:
        return arr.reshape(-1, channel_count)
    if arr.ndim == 2:
        if arr.shape[1] == channel_count:
            return numpy.ascontiguousarray(arr)
        if arr.shape[0] == channel_count:
            return numpy.ascontiguousarray(arr.T)
        if arr.shape[0] == 1:
            return numpy.ascontiguousarray(arr.reshape(-1, channel_count))
    return numpy.ascontiguousarray(arr.reshape(-1, channel_count))


@dataclass
class PCMFrame:
    """Decoded PCM audio chunk."""

    pcm_bytes: bytes
    sample_rate: int
    channels: int
    samples_per_channel: int
    sample_format: str = 's16le'


class AudioPCMDecoder:
    """Decode audio frames into PCM ``s16le`` chunks."""

    def __init__(self, output_sample_rate: Optional[int] = None,
                 session_id: Optional[str] = None,
                 log_prefix: str = ''):
        self.numpy = numpy
        self.session_id = session_id or ''
        self.log_prefix = log_prefix or ''
        self.log_tag = ''
        if self.session_id:
            self.log_tag = f'[{self.log_prefix}|{self.session_id}] ' if self.log_prefix else f'[{self.session_id}] '
        self.codec = None
        self.codec_name = ''
        self.source_codec_name = ''
        self.sample_rate = 0
        self.channels = 1
        self.time_base = fractions.Fraction(1, 8000)
        self.output_sample_rate = int(output_sample_rate or 0)
        self.resampler = None
        self.resampler_key = None
        self.logged_first_pcm_frame = False

    def configure(self, audio_sdp: Dict[str, Any]) -> bool:
        av_codec_name, extradata = get_av_audio_codec_params(audio_sdp)
        if not av_codec_name:
            codec_name = audio_sdp.get('codec_name', '')
            logger.warning(f'{self.log_tag}{Tick.process_tick()} Audio codec is not supported for PCM decode: {codec_name}')
            return False
        self.codec_name = av_codec_name
        self.source_codec_name = str(audio_sdp.get('codec_name', '')).lower()
        if av_codec_name in ('pcm_alaw', 'pcm_mulaw'):
            self.codec = None
        else:
            self.codec = av.CodecContext.create(av_codec_name, 'r')
            if extradata:
                self.codec.extradata = extradata
        self.sample_rate = int(audio_sdp.get('clock_rate', 8000) or 8000)
        self.channels = int(audio_sdp.get('channel', 1) or 1)
        self.time_base = fractions.Fraction(1, self.sample_rate)
        output_sample_rate = self.output_sample_rate or self.sample_rate
        logger.info(
            f'{self.log_tag}{Tick.process_tick()} Audio ready: codec={audio_sdp.get("codec_name")}, decoder={av_codec_name}'
            f', sample_rate={self.sample_rate}, channels={self.channels}, output_sample_rate={output_sample_rate}'
        )
        return True

    def feed(self, audio_frame) -> List[PCMFrame]:
        if not audio_frame.data:
            return []

        if self.codec_name == 'pcm_alaw':
            return self._feed_g711(audio_frame.data, is_alaw=True)
        if self.codec_name == 'pcm_mulaw':
            return self._feed_g711(audio_frame.data, is_alaw=False)
        if self.codec is None:
            return []

        packet = av.Packet(audio_frame.data)
        packet.pts = packet.dts = audio_frame.timestamp
        packet.time_base = self.time_base
        try:
            decoded_frames = self.codec.decode(packet)
        except Exception as ex:
            logger.error(f'{self.log_tag}{Tick.process_tick()} Audio decode error timestamp={audio_frame.timestamp}, ex={ex!r}')
            return []

        pcm_frames: List[PCMFrame] = []
        for decoded_frame in decoded_frames:
            pcm_frames.extend(self._decode_frame(decoded_frame))
        return pcm_frames

    def _feed_g711(self, data: bytes, is_alaw: bool) -> List[PCMFrame]:
        try:
            pcm_bytes = decode_g711_to_pcm16_bytes(data, is_alaw=is_alaw)
        except Exception as ex:
            codec_label = 'pcma' if is_alaw else 'pcmu'
            logger.error(f'{self.log_tag}{Tick.process_tick()} Audio decode error codec={codec_label}, ex={ex!r}')
            return []

        pcm = self.numpy.frombuffer(pcm_bytes, dtype=self.numpy.int16)
        if pcm.size == 0:
            return []
        pcm = normalize_audio_array(pcm, self.channels)
        sample_rate = self.sample_rate or 8000
        if self.output_sample_rate and self.output_sample_rate != sample_rate:
            pcm = self._resample_pcm_array(pcm, sample_rate, self.channels, self.output_sample_rate)
            sample_rate = self.output_sample_rate
        return [self._pcm_frame_from_array(pcm, sample_rate, self.channels)]

    def _decode_frame(self, decoded_frame) -> List[PCMFrame]:
        sample_rate = decoded_frame.sample_rate or self.sample_rate or 8000
        channel_count = get_frame_channel_count(decoded_frame, self.channels)
        layout_name = getattr(decoded_frame.layout, 'name', None)
        if not layout_name:
            layout_name = 'mono' if channel_count == 1 else 'stereo'
        output_sample_rate = self.output_sample_rate or sample_rate
        resampler_key = (sample_rate, channel_count, layout_name, output_sample_rate)
        if self.resampler is None or self.resampler_key != resampler_key:
            self.resampler = av.AudioResampler(format='s16', layout=layout_name, rate=output_sample_rate)
            self.resampler_key = resampler_key

        resampled = self.resampler.resample(decoded_frame)
        if not isinstance(resampled, list):
            resampled = [resampled]

        pcm_frames: List[PCMFrame] = []
        for frame in resampled:
            frame_channels = get_frame_channel_count(frame, channel_count)
            pcm = normalize_audio_array(frame.to_ndarray(), frame_channels)
            pcm_frames.append(self._pcm_frame_from_array(pcm, frame.sample_rate or output_sample_rate, frame_channels))
        return pcm_frames

    def _resample_pcm_array(self, pcm, sample_rate: int, channels: int, output_sample_rate: int):
        layout_name = 'mono' if channels == 1 else 'stereo'
        resampler_key = (sample_rate, channels, layout_name, output_sample_rate, 'pcm-array')
        if self.resampler is None or self.resampler_key != resampler_key:
            self.resampler = av.AudioResampler(format='s16', layout=layout_name, rate=output_sample_rate)
            self.resampler_key = resampler_key
        frame = av.AudioFrame.from_ndarray(pcm.T, format='s16', layout=layout_name)
        frame.sample_rate = sample_rate
        resampled = self.resampler.resample(frame)
        if not isinstance(resampled, list):
            resampled = [resampled]
        arrays = []
        for out_frame in resampled:
            arrays.append(normalize_audio_array(out_frame.to_ndarray(), channels))
        if not arrays:
            return self.numpy.empty((0, channels), dtype=self.numpy.int16)
        return self.numpy.ascontiguousarray(self.numpy.concatenate(arrays, axis=0))

    def _pcm_frame_from_array(self, pcm, sample_rate: int, channels: int) -> PCMFrame:
        pcm = self.numpy.ascontiguousarray(pcm, dtype=self.numpy.int16)
        samples_per_channel = int(pcm.shape[0]) if pcm.ndim >= 1 else 0
        if not self.logged_first_pcm_frame and samples_per_channel > 0:
            self.logged_first_pcm_frame = True
            duration_ms = samples_per_channel * 1000.0 / float(sample_rate or 1)
            logger.info(
                f'{self.log_tag}{Tick.process_tick()} First decoded audio PCM frame: '
                f'codec={self.source_codec_name or self.codec_name}, sample_rate={sample_rate}, '
                f'channels={channels}, samples_per_channel={samples_per_channel}, duration_ms={duration_ms:.2f}'
            )
        return PCMFrame(
            pcm_bytes=pcm.tobytes(),
            sample_rate=sample_rate,
            channels=channels,
            samples_per_channel=samples_per_channel,
        )

    def close(self) -> None:
        self.codec = None
        self.resampler = None
        self.resampler_key = None
        self.source_codec_name = ''
        self.logged_first_pcm_frame = False
