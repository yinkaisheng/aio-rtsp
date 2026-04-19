from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, TYPE_CHECKING

import aio_sockets as aio
import av

from .server_media import MediaSource, MediaTrackInfo, PCMA_CLOCK_RATE

if TYPE_CHECKING:
    from .server import RtspServer


@dataclass
class QueuedWorkerSignal:
    kind: str
    message: str = ""


@dataclass
class VoiceStreamChunk:
    data: bytes


@dataclass
class VoiceStreamEnd:
    reason: str = ""


class VoiceStreamError(Exception):
    pass


def _iter_audio_frames(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class SharedVoiceStream:
    def __init__(
        self,
        server: "RtspServer",
        stream_id: str,
        play_time: int,
        *,
        packet_samples: int,
        logger: aio.LoggerLike,
    ) -> None:
        self.server = server
        self.stream_id = stream_id
        self.play_time = max(1, int(play_time))
        self.packet_samples = packet_samples
        self.logger = logger
        self.track = MediaTrackInfo(
            media_type="audio",
            control="track1",
            codec_name="pcma",
            payload_type=8,
            clock_rate=PCMA_CLOCK_RATE,
            channels=1,
            source_kind="shared_voice",
        )
        self.media_source = MediaSource(
            file_path=self.server.root_dir / f"voice_stream_{stream_id}.wav",
            tracks=[self.track],
            session_name=self.server.session_name,
            stream_kind="shared_voice",
            stream_id=stream_id,
        )
        self._subscribers: Set[asyncio.Queue] = set()
        self._task_lock = asyncio.Lock()
        self._producer_task: Optional[asyncio.Task] = None
        self._ended = False
        self._playback_started_tick: Optional[float] = None
        self._published_samples = 0

    async def start(self) -> None:
        await self._replace_producer(self._run_silence())

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        if self._ended:
            queue.put_nowait(VoiceStreamEnd("stream already ended"))
            return queue
        self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)
        if not self._subscribers and not self._ended:
            await self.stop("no subscribers")

    async def switch_to_file(self, file_path: Path) -> None:
        if self._ended:
            raise VoiceStreamError(f"voice stream id={self.stream_id} is not active")
        await self._replace_producer(self._run_wav_file(file_path))

    async def stop(self, reason: str = "") -> None:
        if self._ended:
            return
        self._ended = True
        async with self._task_lock:
            if self._producer_task is not None:
                current_task = asyncio.current_task()
                producer_task = self._producer_task
                if producer_task is not current_task:
                    producer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await producer_task
                self._producer_task = None
        end_event = VoiceStreamEnd(reason=reason)
        for queue in list(self._subscribers):
            queue.put_nowait(end_event)
        self._subscribers.clear()
        await self.server.remove_voice_stream(self.stream_id, self)

    async def _replace_producer(self, producer) -> None:
        async with self._task_lock:
            if self._producer_task is not None:
                self._producer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._producer_task
            self._producer_task = asyncio.create_task(
                producer,
                name=f"voice-stream-{self.stream_id}",
            )

    def _publish(self, item) -> None:
        if self._ended:
            return
        for queue in list(self._subscribers):
            queue.put_nowait(item)

    async def _run_silence(self) -> None:
        silence_packet = bytes([0xD5]) * self.packet_samples
        deadline = time.monotonic() + self.play_time
        try:
            while not self._ended:
                if time.monotonic() >= deadline:
                    break
                await self._wait_for_publish_time(len(silence_packet))
                self._publish(VoiceStreamChunk(silence_packet))
                self._published_samples += len(silence_packet)
        except asyncio.CancelledError:
            raise
        if not self._ended:
            await self.stop(f"voice stream id={self.stream_id} timed out after {self.play_time}s")

    async def _run_wav_file(self, file_path: Path) -> None:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        stop_event = threading.Event()
        producer = asyncio.create_task(
            aio.to_thread(self._produce_wav_chunks, loop, queue, stop_event, file_path),
            name=f"voice-stream-file-{self.stream_id}",
        )
        try:
            while not self._ended:
                item = await queue.get()
                if isinstance(item, QueuedWorkerSignal):
                    if item.kind == "done":
                        break
                    if item.kind == "error":
                        raise VoiceStreamError(item.message)
                    continue
                if not isinstance(item, bytes):
                    continue
                await self._wait_for_publish_time(len(item))
                self._publish(VoiceStreamChunk(item))
                self._published_samples += len(item)
            await producer
        except asyncio.CancelledError:
            stop_event.set()
            raise
        finally:
            stop_event.set()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await producer
        if not self._ended:
            await self.stop(f"voice stream id={self.stream_id} finished file {file_path.name}")

    def _produce_wav_chunks(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
        file_path: Path,
    ) -> None:
        try:
            resampler = av.AudioResampler(format="s16", layout="mono", rate=8000)
            encoder = av.CodecContext.create("pcm_alaw", "w")
            encoder.sample_rate = PCMA_CLOCK_RATE
            encoder.layout = "mono"
            encoder.format = "s16"
            with av.open(str(file_path), mode="r") as container:
                audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
                if audio_stream is None:
                    raise VoiceStreamError("No audio stream found in wav file")
                for packet in container.demux([audio_stream]):
                    if stop_event.is_set():
                        return
                    for decoded_frame in packet.decode():
                        for resampled_frame in _iter_audio_frames(resampler.resample(decoded_frame)):
                            for encoded_packet in encoder.encode(resampled_frame):
                                if not self._enqueue_live_chunks(
                                    loop, queue, stop_event, bytes(encoded_packet), self.packet_samples
                                ):
                                    return
                for resampled_frame in _iter_audio_frames(resampler.resample(None)):
                    for encoded_packet in encoder.encode(resampled_frame):
                        if not self._enqueue_live_chunks(
                            loop, queue, stop_event, bytes(encoded_packet), self.packet_samples
                        ):
                            return
                for encoded_packet in encoder.encode(None):
                    if not self._enqueue_live_chunks(
                        loop, queue, stop_event, bytes(encoded_packet), self.packet_samples
                    ):
                        return
        except Exception as ex:
            if not stop_event.is_set():
                self._threadsafe_queue_put(loop, queue, QueuedWorkerSignal(kind="error", message=str(ex)), stop_event)
        finally:
            if not stop_event.is_set():
                self._threadsafe_queue_put(loop, queue, QueuedWorkerSignal(kind="done"), stop_event)

    def _enqueue_live_chunks(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
        encoded: bytes,
        packet_samples: int,
    ) -> bool:
        offset = 0
        while offset < len(encoded):
            chunk = encoded[offset:offset + packet_samples]
            if not chunk:
                break
            offset += len(chunk)
            if not self._threadsafe_queue_put(loop, queue, bytes(chunk), stop_event):
                return False
        return True

    def _threadsafe_queue_put(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        item,
        stop_event: threading.Event,
    ) -> bool:
        while not stop_event.is_set():
            future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            try:
                future.result(timeout=0.1)
                return True
            except asyncio.TimeoutError:
                future.cancel()
                continue
            except Exception:
                future.cancel()
                return False
        return False

    async def _wait_for_publish_time(self, sample_count: int) -> None:
        if sample_count <= 0:
            return
        if self._playback_started_tick is None:
            self._playback_started_tick = time.perf_counter()
            return
        target = self._playback_started_tick + (self._published_samples / float(PCMA_CLOCK_RATE))
        delay = target - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
