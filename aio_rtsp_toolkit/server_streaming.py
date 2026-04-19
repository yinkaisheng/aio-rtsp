from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import threading
import traceback
from typing import Dict

import aio_sockets as aio
import av

from .server_media import PCMA_CLOCK_RATE, normalize_codec_name
from .server_session_types import (
    PCMA_PACKET_SAMPLES,
    QueuedLoopState,
    QueuedMediaPacket,
    RtspServerError,
    _is_disconnect_error,
    _iter_audio_frames,
)
from .server_voice import QueuedWorkerSignal, VoiceStreamChunk, VoiceStreamEnd


QUEUE_PUT_POLL_INTERVAL = 0.05


class RtspStreamingMixin:
    async def _stop_streaming(self) -> None:
        if self.media_source is not None and self.media_source.stream_kind == "shared_voice" and self.voice_subscription is not None:
            voice_stream = self.server.get_voice_stream(self.media_source.stream_id)
            if voice_stream is not None:
                await voice_stream.unsubscribe(self.voice_subscription)
            self.voice_subscription = None
        if self.stream_task is None:
            return
        self.stream_task.cancel()
        try:
            await self.stream_task
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            self.logger.warning(f"{self.peername} stream task stopped with error: {ex!r}")
        self.stream_task = None

    async def _stream_media(self) -> None:
        if self.media_source is not None and self.media_source.stream_kind == "shared_voice":
            await self._stream_shared_voice()
            return
        suffix = self.media_source.file_path.suffix.lower()
        remaining_plays = self.play_count
        loop_index = 0
        while not self.closed:
            try:
                loop_index += 1
                self.logger.info(
                    f"{self.peername} stream loop start session={self.session_id} "
                    f"loop={loop_index} source={self.media_source.file_path.name}"
                )
                if loop_index == 1 and self.pre_fill_ms > 0:
                    await self._send_pre_fill_silence()
                await self._stream_media_once(suffix)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                if _is_disconnect_error(ex):
                    break
                self.logger.error(f"{self.peername} stream error: {ex!r}\n{traceback.format_exc()}")
                break
            self.logger.info(
                f"{self.peername} stream loop end session={self.session_id} "
                f"loop={loop_index} source={self.media_source.file_path.name}"
            )
            if remaining_plays > 0:
                remaining_plays -= 1
                if remaining_plays == 0:
                    break
        if not self.closed and self.play_count > 0 and remaining_plays == 0:
            self.logger.info(f"{self.peername} play_count exhausted, closing session={self.session_id}")
            await self.close()

    async def _stream_shared_voice(self) -> None:
        if self.media_source is None:
            return
        state = next((item for item in self.track_states.values() if item.rtp_channel is not None), None)
        if state is None:
            return
        voice_stream = self.server.get_voice_stream(self.media_source.stream_id)
        if voice_stream is None:
            raise RtspServerError(f"voice stream id={self.media_source.stream_id} is not active")
        self.voice_subscription = voice_stream.subscribe()
        sample_cursor = 0
        try:
            while True:
                item = await self.voice_subscription.get()
                if isinstance(item, VoiceStreamEnd):
                    self.logger.info(
                        f"{self.peername} voice stream end session={self.session_id} "
                        f"id={self.media_source.stream_id} reason={item.reason}"
                    )
                    break
                if isinstance(item, VoiceStreamChunk):
                    await self._send_live_audio_packet(state, item.data, sample_cursor)
                    sample_cursor += len(item.data)
        finally:
            if self.media_source is not None and self.voice_subscription is not None:
                current_stream = self.server.get_voice_stream(self.media_source.stream_id)
                if current_stream is not None:
                    await current_stream.unsubscribe(self.voice_subscription)
                self.voice_subscription = None
        if not self.closed:
            await self.close()

    async def _send_pre_fill_silence(self) -> None:
        if self.media_source is None or self.pre_fill_ms <= 0:
            return
        state = next((item for item in self.track_states.values() if item.rtp_channel is not None), None)
        if state is None:
            return
        if not self._supports_pre_fill():
            self.logger.info(
                f"{self.peername} pre_fill ignored session={self.session_id} "
                f"source={self.media_source.file_path.name} reason=unsupported_media"
            )
            return
        total_samples = int(round(self.pre_fill_ms * state.info.clock_rate / 1000.0))
        if total_samples <= 0:
            return
        self.logger.info(
            f"{self.peername} pre_fill start session={self.session_id} "
            f"milliseconds={self.pre_fill_ms} samples={total_samples}"
        )
        sample_cursor = state.sample_cursor
        remaining = total_samples
        while remaining > 0 and not self.closed:
            chunk_samples = min(PCMA_PACKET_SAMPLES, remaining)
            await self._send_wav_audio_packet(state, bytes([0xD5]) * chunk_samples, sample_cursor)
            sample_cursor += chunk_samples
            remaining -= chunk_samples
        state.sample_cursor = sample_cursor
        state.media_time_offset = sample_cursor / float(state.info.clock_rate or 1)
        state.packet_time_cursor = state.media_time_offset

    def _supports_pre_fill(self) -> bool:
        if self.media_source is None or self.media_source.stream_kind != "file":
            return False
        if len(self.media_source.tracks) != 1:
            return False
        track = self.media_source.tracks[0]
        return track.media_type == "audio" and normalize_codec_name(track.codec_name) == "pcma"

    async def _stream_media_once(self, suffix: str) -> None:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        stop_event = threading.Event()
        producer = asyncio.create_task(aio.to_thread(self._produce_media_packets, suffix, loop, queue, stop_event))
        try:
            while True:
                item = await queue.get()
                if isinstance(item, QueuedWorkerSignal):
                    if item.kind == "done":
                        break
                    if item.kind == "error":
                        raise RtspServerError(item.message)
                    continue
                if isinstance(item, QueuedLoopState):
                    state = self.track_states.get(item.track_control)
                    if state is None:
                        continue
                    state.media_time_offset = item.media_time_offset
                    state.packet_time_cursor = item.packet_time_cursor
                    state.first_source_time = None
                    if item.sample_cursor is not None:
                        state.sample_cursor = item.sample_cursor
                    continue
                if not isinstance(item, QueuedMediaPacket):
                    continue
                state = self.track_states.get(item.track_control)
                if state is None or state.rtp_channel is None:
                    continue
                if item.kind == "video":
                    await self._send_video_packet(state, item.data, item.media_time, item.duration_seconds, item.is_keyframe)
                elif item.kind == "aac":
                    await self._send_aac_packet(state, item.data, item.media_time, item.duration_seconds)
                elif item.kind == "pcma":
                    await self._send_raw_audio_packet(state, item.data, item.media_time, item.duration_seconds)
                elif item.kind == "wav_pcma":
                    await self._send_wav_audio_packet(state, item.data, item.sample_cursor or 0)
            await producer
        except asyncio.CancelledError:
            stop_event.set()
            raise
        finally:
            stop_event.set()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await producer

    def _threadsafe_queue_put(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, item, stop_event: threading.Event) -> bool:
        while not stop_event.is_set():
            future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            try:
                future.result(timeout=QUEUE_PUT_POLL_INTERVAL)
                return True
            except concurrent.futures.TimeoutError:
                future.cancel()
                continue
            except Exception:
                future.cancel()
                return False
        return False

    def _produce_media_packets(
        self,
        suffix: str,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        try:
            if suffix == ".wav":
                self._produce_wav_packets(loop, queue, stop_event)
            else:
                self._produce_container_packets(loop, queue, stop_event)
        except Exception as ex:
            if not stop_event.is_set():
                self._threadsafe_queue_put(loop, queue, QueuedWorkerSignal(kind="error", message=str(ex)), stop_event)
        finally:
            if not stop_event.is_set():
                self._threadsafe_queue_put(loop, queue, QueuedWorkerSignal(kind="done"), stop_event)

    def _produce_container_packets(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        stream_states = {
            state.info.av_stream_index: state
            for state in self.track_states.values()
            if state.rtp_channel is not None and state.info.av_stream_index is not None
        }
        if not stream_states:
            return
        loop_end_times: Dict[int, float] = {}
        first_packet_times: Dict[int, float] = {}
        with av.open(str(self.media_source.file_path), mode="r") as container:
            selected_streams = [container.streams[index] for index in stream_states]
            for packet in container.demux(selected_streams):
                if self.closed or stop_event.is_set():
                    return
                state = stream_states.get(packet.stream.index)
                if state is None or packet.size <= 0:
                    continue
                pts = packet.pts if packet.pts is not None else packet.dts
                if pts is None:
                    continue
                packet_time = float(pts * packet.time_base)
                duration_value = packet.duration
                if duration_value is None or duration_value <= 0:
                    average_rate = getattr(packet.stream, "average_rate", None) if state.info.media_type == "video" else None
                    if average_rate:
                        duration_seconds = float(1 / average_rate)
                    elif state.info.media_type == "audio" and state.info.codec_name == "mpeg4-generic":
                        duration_seconds = 1024 / float(state.info.clock_rate or 1)
                    else:
                        duration_seconds = 0.02
                else:
                    duration_seconds = float(duration_value * packet.time_base)
                first_packet_time = first_packet_times.setdefault(packet.stream.index, packet_time)
                relative_packet_time = max(0.0, packet_time - first_packet_time)
                media_time = state.media_time_offset + relative_packet_time
                if state.info.media_type == "video":
                    schedule_time = max(state.packet_time_cursor, media_time)
                    state.packet_time_cursor = schedule_time + max(duration_seconds, 0.0)
                    loop_end_times[packet.stream.index] = max(loop_end_times.get(packet.stream.index, 0.0), state.packet_time_cursor)
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="video",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=schedule_time,
                            duration_seconds=duration_seconds,
                            is_keyframe=bool(packet.is_keyframe),
                        ),
                        stop_event,
                    ):
                        return
                elif state.info.codec_name == "mpeg4-generic":
                    state.packet_time_cursor = max(state.packet_time_cursor, media_time + max(duration_seconds, 0.0))
                    loop_end_times[packet.stream.index] = max(loop_end_times.get(packet.stream.index, 0.0), state.packet_time_cursor)
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="aac",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=media_time,
                            duration_seconds=duration_seconds,
                        ),
                        stop_event,
                    ):
                        return
                elif state.info.codec_name == "pcma":
                    state.packet_time_cursor = max(state.packet_time_cursor, media_time + max(duration_seconds, 0.0))
                    loop_end_times[packet.stream.index] = max(loop_end_times.get(packet.stream.index, 0.0), state.packet_time_cursor)
                    if not self._threadsafe_queue_put(
                        loop,
                        queue,
                        QueuedMediaPacket(
                            kind="pcma",
                            track_control=state.info.control,
                            data=bytes(packet),
                            media_time=media_time,
                            duration_seconds=duration_seconds,
                        ),
                        stop_event,
                    ):
                        return
        for stream_index, state in stream_states.items():
            end_time = loop_end_times.get(stream_index)
            if end_time is not None and not self._threadsafe_queue_put(
                loop,
                queue,
                QueuedLoopState(
                    kind="loop_state",
                    track_control=state.info.control,
                    media_time_offset=end_time,
                    packet_time_cursor=end_time,
                ),
                stop_event,
            ):
                return

    def _produce_wav_packets(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
    ) -> None:
        state = next((it for it in self.track_states.values() if it.rtp_channel is not None), None)
        if state is None:
            return
        resampler = av.AudioResampler(format="s16", layout="mono", rate=PCMA_CLOCK_RATE)
        encoder = av.CodecContext.create("pcm_alaw", "w")
        encoder.sample_rate = PCMA_CLOCK_RATE
        encoder.layout = "mono"
        encoder.format = "s16"
        sample_cursor = state.sample_cursor
        with av.open(str(self.media_source.file_path), mode="r") as container:
            audio_stream = next((stream for stream in container.streams if stream.type == "audio"), None)
            if audio_stream is None:
                raise RtspServerError("No audio stream found in wav file")
            for packet in container.demux([audio_stream]):
                if self.closed or stop_event.is_set():
                    return
                for decoded_frame in packet.decode():
                    for resampled_frame in _iter_audio_frames(resampler.resample(decoded_frame)):
                        for encoded_packet in encoder.encode(resampled_frame):
                            sample_cursor = self._enqueue_wav_chunks(
                                loop, queue, stop_event, state.info.control, bytes(encoded_packet), sample_cursor
                            )
                            if sample_cursor < 0:
                                return
            for resampled_frame in _iter_audio_frames(resampler.resample(None)):
                for encoded_packet in encoder.encode(resampled_frame):
                    sample_cursor = self._enqueue_wav_chunks(
                        loop, queue, stop_event, state.info.control, bytes(encoded_packet), sample_cursor
                    )
                    if sample_cursor < 0:
                        return
            for encoded_packet in encoder.encode(None):
                sample_cursor = self._enqueue_wav_chunks(
                    loop, queue, stop_event, state.info.control, bytes(encoded_packet), sample_cursor
                )
                if sample_cursor < 0:
                    return
        self._threadsafe_queue_put(
            loop,
            queue,
            QueuedLoopState(kind="loop_state", track_control=state.info.control, sample_cursor=sample_cursor),
            stop_event,
        )

    def _enqueue_wav_chunks(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue,
        stop_event: threading.Event,
        track_control: str,
        encoded: bytes,
        sample_cursor: int,
    ) -> int:
        offset = 0
        while offset < len(encoded):
            chunk = encoded[offset:offset + PCMA_PACKET_SAMPLES]
            if not chunk:
                break
            offset += len(chunk)
            next_cursor = sample_cursor + len(chunk)
            if not self._threadsafe_queue_put(
                loop,
                queue,
                QueuedMediaPacket(kind="wav_pcma", track_control=track_control, data=chunk, sample_cursor=sample_cursor),
                stop_event,
            ):
                return -1
            sample_cursor = next_cursor
        return sample_cursor
