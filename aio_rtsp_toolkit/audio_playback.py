"""Optional audio playback helpers for aio-rtsp-toolkit.

This module is intentionally separate from the RTSP protocol core. It depends
on optional runtime packages for playback and, for some codecs, decoding.
"""

import fractions
import queue
import threading
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING

from .audio_codecs import decode_g711_to_pcm16_bytes
from .tick import Tick

if TYPE_CHECKING:
    import av
    import numpy
    import sounddevice as sd


def _missing_dependency_error(feature: str, package_name: str, extra_name: str) -> RuntimeError:
    return RuntimeError(
        f"{feature} requires the optional dependency '{package_name}'. "
        f"Install it with: pip install aio-rtsp-toolkit[{extra_name}]"
    )


def _require_numpy():
    try:
        import numpy
    except ImportError as ex:
        raise _missing_dependency_error('Audio playback', 'numpy', 'audio') from ex
    return numpy


def _require_sounddevice():
    try:
        import sounddevice as sd
    except ImportError as ex:
        raise _missing_dependency_error('Audio playback', 'sounddevice', 'audio') from ex
    return sd


def _require_av():
    try:
        import av
    except ImportError as ex:
        raise _missing_dependency_error('AAC/LATM audio playback', 'av', 'decode') from ex
    return av


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
    numpy = _require_numpy()
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


class SoundDeviceAudioPlayer:
    """Decode and play audio frames through ``sounddevice``.

    ``numpy`` and ``sounddevice`` are required for all playback.
    ``av`` is required only for codecs that need packet decoding such as AAC.
    G.711 A-law and mu-law are decoded internally and do not require ``av``.
    """

    def __init__(self, logfunc: Optional[Callable[[str], None]] = None):
        self.logfunc = logfunc or (lambda msg: None)
        self.numpy = _require_numpy()
        self.sd = _require_sounddevice()
        self.codec = None
        self.codec_name = ''
        self.source_codec_name = ''
        self.sample_rate = 0
        self.channels = 1
        self.time_base = fractions.Fraction(1, 8000)
        self.output_stream = None
        self.output_sample_rate = 0
        self.output_channels = 0
        self.resampler = None
        self.resampler_key = None
        self.pcm_queue: queue.Queue = queue.Queue(maxsize=128)
        self.pending_chunk = None
        self.pending_offset = 0
        self.queue_lock = threading.Lock()

    def configure(self, audio_sdp: Dict[str, Any]) -> bool:
        av_codec_name, extradata = get_av_audio_codec_params(audio_sdp)
        if not av_codec_name:
            codec_name = audio_sdp.get('codec_name', '')
            self.logfunc(f'{Tick.process_tick()} Audio codec is not supported for playback: {codec_name}')
            return False
        self.codec_name = av_codec_name
        self.source_codec_name = str(audio_sdp.get('codec_name', '')).lower()
        if av_codec_name in ('pcm_alaw', 'pcm_mulaw'):
            self.codec = None
        else:
            av = _require_av()
            self.codec = av.CodecContext.create(av_codec_name, 'r')
            if extradata:
                self.codec.extradata = extradata
        self.sample_rate = int(audio_sdp.get('clock_rate', 8000) or 8000)
        self.channels = int(audio_sdp.get('channel', 1) or 1)
        self.time_base = fractions.Fraction(1, self.sample_rate)
        self.logfunc(
            f'{Tick.process_tick()} Audio ready: codec={audio_sdp.get("codec_name")}, decoder={av_codec_name}'
            f', sample_rate={self.sample_rate}, channels={self.channels}'
        )
        return True

    def feed(self, audio_frame) -> None:
        if not audio_frame.data:
            return

        if self.codec_name == 'pcm_alaw':
            self._feed_g711(audio_frame.data, is_alaw=True)
            return
        if self.codec_name == 'pcm_mulaw':
            self._feed_g711(audio_frame.data, is_alaw=False)
            return
        if self.codec is None:
            return

        av = _require_av()
        packet = av.Packet(audio_frame.data)
        packet.pts = packet.dts = audio_frame.timestamp
        packet.time_base = self.time_base
        try:
            decoded_frames = self.codec.decode(packet)
        except Exception as ex:
            self.logfunc(f'{Tick.process_tick()} Audio decode error timestamp={audio_frame.timestamp}, ex={ex!r}')
            return

        for decoded_frame in decoded_frames:
            self._play_frame(decoded_frame)

    def _feed_g711(self, data: bytes, is_alaw: bool) -> None:
        try:
            pcm_bytes = decode_g711_to_pcm16_bytes(data, is_alaw=is_alaw)
        except Exception as ex:
            codec_label = 'pcma' if is_alaw else 'pcmu'
            self.logfunc(f'{Tick.process_tick()} Audio decode error codec={codec_label}, ex={ex!r}')
            return

        pcm = self.numpy.frombuffer(pcm_bytes, dtype=self.numpy.int16)
        if pcm.size == 0:
            return
        pcm = normalize_audio_array(pcm, self.channels)
        self._ensure_output(self.sample_rate or 8000, self.channels)
        self._enqueue_pcm(pcm)

    def _play_frame(self, decoded_frame) -> None:
        av = _require_av()
        sample_rate = decoded_frame.sample_rate or self.sample_rate or 8000
        channel_count = get_frame_channel_count(decoded_frame, self.channels)
        layout_name = getattr(decoded_frame.layout, 'name', None)
        if not layout_name:
            layout_name = 'mono' if channel_count == 1 else 'stereo'
        resampler_key = (sample_rate, channel_count, layout_name)
        if self.resampler is None or self.resampler_key != resampler_key:
            self.resampler = av.AudioResampler(format='s16', layout=layout_name, rate=sample_rate)
            self.resampler_key = resampler_key

        resampled = self.resampler.resample(decoded_frame)
        if not isinstance(resampled, list):
            resampled = [resampled]

        for frame in resampled:
            frame_channels = get_frame_channel_count(frame, channel_count)
            pcm = normalize_audio_array(frame.to_ndarray(), frame_channels)
            self._ensure_output(frame.sample_rate or sample_rate, frame_channels)
            self._enqueue_pcm(pcm)

    def _ensure_output(self, sample_rate: int, channels: int) -> None:
        if self.output_stream is not None and self.output_sample_rate == sample_rate and self.output_channels == channels:
            return
        self._close_output()
        self.output_stream = self.sd.OutputStream(
            samplerate=sample_rate,
            channels=channels,
            dtype='int16',
            callback=self._audio_callback,
            blocksize=0,
        )
        self.output_stream.start()
        self.output_sample_rate = sample_rate
        self.output_channels = channels

    def close(self) -> None:
        with self.queue_lock:
            self.pending_chunk = None
            self.pending_offset = 0
            while not self.pcm_queue.empty():
                try:
                    self.pcm_queue.get_nowait()
                except queue.Empty:
                    break
        self._close_output()
        self.codec = None
        self.resampler = None
        self.resampler_key = None
        self.source_codec_name = ''

    def _enqueue_pcm(self, pcm) -> None:
        if pcm.size == 0:
            return
        chunk = self.numpy.ascontiguousarray(pcm)
        while True:
            try:
                self.pcm_queue.put_nowait(chunk)
                return
            except queue.Full:
                try:
                    self.pcm_queue.get_nowait()
                except queue.Empty:
                    return

    def _audio_callback(self, outdata, frames, time_info, status) -> None:
        if status:
            self.logfunc(f'{Tick.process_tick()} Audio callback status: {status}')
        outdata.fill(0)
        frame_offset = 0
        channels = outdata.shape[1] if outdata.ndim == 2 else self.output_channels or 1
        with self.queue_lock:
            while frame_offset < frames:
                if self.pending_chunk is None:
                    try:
                        self.pending_chunk = self.pcm_queue.get_nowait()
                        self.pending_offset = 0
                    except queue.Empty:
                        break
                chunk = self.pending_chunk
                available = len(chunk) - self.pending_offset
                if available <= 0:
                    self.pending_chunk = None
                    self.pending_offset = 0
                    continue
                copy_count = min(frames - frame_offset, available)
                outdata[frame_offset:frame_offset + copy_count, :channels] = chunk[
                    self.pending_offset:self.pending_offset + copy_count, :channels
                ]
                frame_offset += copy_count
                self.pending_offset += copy_count
                if self.pending_offset >= len(chunk):
                    self.pending_chunk = None
                    self.pending_offset = 0

    def _close_output(self) -> None:
        if self.output_stream is None:
            return
        try:
            self.output_stream.stop()
        finally:
            self.output_stream.close()
            self.output_stream = None
            self.output_sample_rate = 0
            self.output_channels = 0
