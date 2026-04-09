"""Optional audio playback helpers for aio-rtsp-toolkit."""

import queue
import threading
from typing import List, Optional

import sounddevice as sd

from .audio_decode import AudioPCMDecoder, PCMFrame, normalize_audio_array
from .client import logger
from .tick import Tick


class SoundDeviceAudioPlayer(AudioPCMDecoder):
    """Decode PCM and play it through ``sounddevice``."""

    def __init__(self, output_sample_rate: Optional[int] = None,
                 session_id: Optional[str] = None,
                 log_prefix: str = ''):
        super().__init__(
            output_sample_rate=output_sample_rate,
            session_id=session_id,
            log_prefix=log_prefix,
        )
        self.sd = sd
        self.output_stream = None
        self.active_output_sample_rate = 0
        self.active_output_channels = 0
        self.pcm_queue: queue.Queue = queue.Queue(maxsize=128)
        self.pending_chunk = None
        self.pending_offset = 0
        self.queue_lock = threading.Lock()

    def feed(self, audio_frame) -> List[PCMFrame]:
        pcm_frames = super().feed(audio_frame)
        for pcm_frame in pcm_frames:
            pcm = self.numpy.frombuffer(pcm_frame.pcm_bytes, dtype=self.numpy.int16)
            if pcm.size == 0:
                continue
            pcm = normalize_audio_array(pcm, pcm_frame.channels)
            self._ensure_output(pcm_frame.sample_rate, pcm_frame.channels)
            self._enqueue_pcm(pcm)
        return pcm_frames

    def _ensure_output(self, sample_rate: int, channels: int) -> None:
        if self.output_stream is not None and self.active_output_sample_rate == sample_rate and self.active_output_channels == channels:
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
        self.active_output_sample_rate = sample_rate
        self.active_output_channels = channels

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
        super().close()

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
            logger.warning(f'{self.log_tag}{Tick.process_tick()} Audio callback status: {status}')
        outdata.fill(0)
        frame_offset = 0
        channels = outdata.shape[1] if outdata.ndim == 2 else self.active_output_channels or 1
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
            self.active_output_sample_rate = 0
            self.active_output_channels = 0
