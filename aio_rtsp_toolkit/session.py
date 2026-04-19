from __future__ import annotations

import asyncio
import itertools
import time
import traceback
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Union, cast

from . import types
from .protocol import RtspInterleavedData, RtspProtocol, RtspResponse
from .types import (
    DEFAULT_LOG_TYPE,
    AudioFrameEvent,
    ClosedEvent,
    ConnectResultEvent,
    PublicRtspEvent,
    RtpPacketEvent,
    SocketAddress,
    StopEventLike,
    RtspClientMsgType,
    RtspError,
    RtspEvent,
    RtspMethodEvent,
    RtspProtocolError,
    RtspResponseError,
    RtspTimeoutError,
    VideoFrameEvent,
)
from .tcp_client_base import ConnectionClosed


_QUEUE_EOF = object()
_session_id_counter = itertools.count(1)
InternalQueueItem = Union[PublicRtspEvent, object]


class RtspSession:
    def __init__(
        self,
        rtsp_url: str,
        forward_address: Optional[SocketAddress] = None,
        timeout: float = 4,
        session_id: Optional[str] = None,
        enable_video: bool = True,
        enable_audio: bool = True,
        log_type: int = DEFAULT_LOG_TYPE,
        log_prefix: str = '',
    ) -> None:
        self.rtsp_url = rtsp_url
        self.forward_address = forward_address
        self.timeout = timeout
        self.log_type = log_type
        self.session_id = session_id or 's%04d' % next(_session_id_counter)
        self.log_tag = f'[{log_prefix}|{self.session_id}]' if log_prefix else f'[{self.session_id}]'
        self.enable_video = enable_video
        self.enable_audio = enable_audio
        self._client: Optional[RtspProtocol] = None
        self._closed = False
        self._event_queue: Optional[asyncio.Queue] = None
        self._event_receiver_task: Optional[asyncio.Task] = None
        self._stop_task: Optional[asyncio.Task] = None
        self._keep_alive_task: Optional[asyncio.Task] = None
        self._event_receiver_exception: Optional[BaseException] = None
        self._stop_event: asyncio.Event = None
        self._played = False
        self._close_reason: Optional[str] = None
        self._close_exception: Optional[Exception] = None
        self._play_buffered_events: List[RtspEvent] = []

    async def __aenter__(self) -> "RtspSession":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    @property
    def client(self) -> Optional[RtspProtocol]:
        return self._client

    @property
    def sdp(self) -> dict:
        if self._client is None:
            return {}
        return self._client.sdp

    async def close(self) -> None:
        """
        Force the session to stop and release resources immediately.

        This path does not send RTSP TEARDOWN. It only stops background tasks and closes
        the underlying RTSP connection. Prefer stopping iter_events() through the caller's
        stop_event when a graceful shutdown is desired; that normal stop path may send
        TEARDOWN before the connection is closed.
        """
        if self._closed:
            return
        self._closed = True
        if self._stop_event is not None:
            self._stop_event.set()

        stop_task = self._stop_task
        self._stop_task = None
        keep_alive_task = self._keep_alive_task
        self._keep_alive_task = None
        event_receiver_task = self._event_receiver_task
        self._event_receiver_task = None

        await self._cancel_task(stop_task)
        await self._cancel_task(keep_alive_task)
        await self._cancel_task(event_receiver_task)

        if self._client is None:
            return
        if self.log_type & RtspClientMsgType.Closed:
            types.logger.info(f'{self.log_tag} close RTSP connection'
                              f', ex={self._event_receiver_exception!r}')
        await self._client.close(reason='rtsp session closed')
        self._client = None

    async def iter_events(self, stop_event: StopEventLike) -> AsyncIterator[PublicRtspEvent]:
        """
        Run the RTSP session and yield public events until the session finishes.

        The caller should stop the session by setting stop_event. This is the recommended
        shutdown path because it allows the session to try sending RTSP TEARDOWN before the
        connection is closed. Calling close() directly skips TEARDOWN and force-closes the
        session resources.
        """
        if not self.enable_video and not self.enable_audio:
            raise ValueError('At least one of enable_video or enable_audio must be True')
        self._client = RtspProtocol(self.rtsp_url, self.forward_address, self.timeout, self.log_type, self.log_tag)
        self._closed = False
        self._event_receiver_exception = None
        self._close_reason = None
        self._close_exception = None
        self._event_queue = asyncio.Queue()  # type: asyncio.Queue[InternalQueueItem]
        self._played = False
        self._play_buffered_events[:] = []
        user_stop_event = stop_event
        self._stop_event = asyncio.Event()
        rtsp = self._client

        async def handle_user_stop(rtsp: RtspProtocol,
                                   user_stop_event: StopEventLike,
                                   client_stop_event: asyncio.Event):
            if isinstance(user_stop_event, asyncio.Event):
                while True:
                    try:
                        await asyncio.wait_for(user_stop_event.wait(), timeout=0.1)
                        break
                    except asyncio.TimeoutError:
                        pass
                    if rtsp._closed:
                        client_stop_event.set()
                        return
            else: # threading.Event
                while True:
                    await asyncio.sleep(0.1)
                    if rtsp._closed:
                        client_stop_event.set()
                        return
                    if user_stop_event.is_set():
                        break

            client_stop_event.set()
            teardown_resp = None
            if self._played:
                try:
                    teardown_resp =  await self._run_method('TEARDOWN', rtsp.teardown, 0.2)
                except:
                    pass
            return teardown_resp

        self._stop_task = asyncio.create_task(handle_user_stop(rtsp, user_stop_event, self._stop_event))

        try:
            connect_exception: Optional[Exception] = None
            try:
                await rtsp.connect()
            except asyncio.TimeoutError:
                connect_exception = RtspTimeoutError(
                    f'RTSP connect timed out after {self.timeout}s',
                    session_elapsed=rtsp.tick.since_start(),
                )
            except Exception as ex:
                connect_exception = ex
            connect_elapsed = rtsp.tick.since_last()
            yield ConnectResultEvent(
                event='connect_result',
                msg_type=RtspClientMsgType.ConnectResult,
                session_elapsed=rtsp.tick.since_start(),
                local_addr=rtsp.local_addr,
                exception=connect_exception,
                elapsed=connect_elapsed,
            )
            if connect_exception is not None or rtsp.local_addr is None:
                rtsp._closed = True
                return

            rtsp.start_sock_recv_task()
            self._event_receiver_task = asyncio.create_task(self._event_bridge_loop())
            rtsp_resp = None

            for i in range(1):
                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                rtsp_resp = await self._run_method('OPTIONS', rtsp.options)
                if rtsp_resp is None:
                    break
                yield self._new_method_event('OPTIONS', rtsp_resp)

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                rtsp.check_auth(rtsp_resp)
                if rtsp_resp.status_code == 401 and rtsp.auth_method and rtsp.username and rtsp.password:
                    rtsp_resp = await self._run_method('OPTIONS', rtsp.options)
                    if rtsp_resp is None:
                        break
                    yield self._new_method_event('OPTIONS', rtsp_resp)
                    self._ensure_ok('OPTIONS', rtsp_resp)

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                rtsp_resp = await self._run_method('DESCRIBE', rtsp.describe)
                if rtsp_resp is None:
                    break
                yield self._new_method_event('DESCRIBE', rtsp_resp)

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                rtsp.check_auth(rtsp_resp)
                if rtsp_resp.status_code == 401 and rtsp.auth_method and rtsp.username and rtsp.password:
                    rtsp_resp = await self._run_method('DESCRIBE', rtsp.describe)
                    if rtsp_resp is None:
                        break
                    yield self._new_method_event('DESCRIBE', rtsp_resp)

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                if self.enable_video:
                    rtsp_resp = await self._run_method('SETUP', rtsp.setup_media, 'video')
                    if rtsp_resp is None:
                        break
                    yield self._new_method_event('SETUP', rtsp_resp, media_type='video')
                    self._ensure_ok('SETUP', rtsp_resp, 'video')

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                if self.enable_audio:
                    rtsp_resp = await self._run_method('SETUP', rtsp.setup_media, 'audio')
                    if rtsp_resp is None:
                        break
                    yield self._new_method_event('SETUP', rtsp_resp, media_type='audio')
                    self._ensure_ok('SETUP', rtsp_resp, 'audio')

                if user_stop_event.is_set() or self._stop_event.is_set():
                    break
                rtsp_resp = await self._run_method('PLAY', rtsp.play)
                if rtsp_resp is None:
                    break
                yield self._new_method_event('PLAY', rtsp_resp)
                self._ensure_ok('PLAY', rtsp_resp)
                self._played = True

                if rtsp.session_timeout > 0:
                    self._keep_alive_task = asyncio.create_task(self._run_keep_alive())

                for buffered_event in self._drain_buffered_events_before_play():
                    yield buffered_event

            async for event in self._get_event_loop():
                yield event

            stop_task = self._stop_task
            teardown_resp = await stop_task if stop_task is not None else None
            if teardown_resp is not None:
                yield self._new_method_event('TEARDOWN', teardown_resp)
        finally:
            self._played = False
            await self.close()

        yield ClosedEvent(
            event='closed',
            msg_type=RtspClientMsgType.Closed,
            session_elapsed=rtsp.tick.since_start(),
            reason=self._close_reason or 'closed',
            exception=self._close_exception,
        )

    async def _run_keep_alive(self) -> None:
        timeout = max(self._client.session_timeout - 5, 10)
        while True:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout)
                return
            except asyncio.TimeoutError  as ex:
                pass

            try:
                rtsp_resp = await self._run_method('GET_PARAMETER', self._client.get_parameter)
                if rtsp_resp is not None:
                    if rtsp_resp.status_code == 405:
                        rtsp_resp = await self._run_method('OPTIONS', self._client.options_keepalive)
            except Exception as ex:
                types.logger.error(f'ex={ex!r}')

    async def _event_bridge_loop(self) -> None:
        try:
            async for event in self._client.events():
                if isinstance(event, RtspInterleavedData):
                    for output in self._build_rtp_events(event.channel, event.packet):
                        self._emit_event(output)
                elif isinstance(event, ConnectionClosed):
                    if self.log_type & RtspClientMsgType.Closed:
                        call_stack = ''.join(traceback.format_exception(type(event.exception), event.exception,
                            event.exception.__traceback__)) if event.exception else ''
                        types.logger.info(f'{self.log_tag} ConnectionClosed reason={event.reason}'
                                          f', ex={event.exception!r}\n{call_stack}')
                    self._close_reason = event.reason
                    if event.exception is not None:
                        self._close_exception = event.exception
                    break
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            self._event_receiver_exception = ex
        finally:
            if self._event_queue is not None:
                await self._event_queue.put(_QUEUE_EOF)

    async def _run_method(
        self,
        method: str,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> object:
        '''return RtspResponse or None, raise '''
        try:
            return await func(*args, **kwargs)
        except RtspError:
            raise
        except Exception as ex:
            if self.log_type & RtspClientMsgType.Exception:
                types.logger.error(f'{self.log_tag} RTSP {method} ex={ex!r}\n{traceback.format_exc()}')
            raise RtspProtocolError(f'RTSP {method} failed: {ex!r}', session_elapsed=self._client.tick.since_start())

    async def _cancel_task(self, task: Optional[asyncio.Task]) -> None:
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _new_method_event(
        self,
        method: str,
        response: RtspResponse,
        media_type: Optional[str] = None,
    ) -> RtspMethodEvent:
        return RtspMethodEvent(
            event='rtsp_method',
            msg_type=RtspClientMsgType.RTSP,
            session_elapsed=self._client.tick.since_start(),
            method=method,
            response=response,
            media_type=media_type,
        )

    def _new_rtp_event(self, channel: int, rtp: object) -> RtpPacketEvent:
        return RtpPacketEvent(
            event='rtp_packet',
            msg_type=RtspClientMsgType.RTP,
            session_elapsed=self._client.tick.since_start(),
            channel=channel,
            media_channel=self._client.media_channels[channel],
            rtp=rtp,
        )

    def _ensure_ok(self, method: str, response: RtspResponse, media_type: Optional[str] = None) -> None:
        if response.status_code != 200:
            raise RtspResponseError(method, response.status_code, response, media_type, session_elapsed=self._client.tick.since_start())

    async def _get_event_loop(self) -> AsyncIterator[PublicRtspEvent]:
        while True:
            event = await self._event_queue.get()
            if event is _QUEUE_EOF:
                break
            yield cast(PublicRtspEvent, event)

    def _is_disconnect_close(self) -> bool:
        return self._close_reason is not None or self._close_exception is not None

    def _emit_event(self, event: PublicRtspEvent) -> None:
        assert self._event_queue is not None
        if self._client is not None and self._client.has_pending_method('PLAY'):
            self._play_buffered_events.append(event)
            return
        self._event_queue.put_nowait(event)

    def _drain_buffered_events_before_play(self) -> List[PublicRtspEvent]:
        events = list(self._play_buffered_events)
        self._play_buffered_events[:] = []
        return events

    def _build_rtp_events(self, channel: int, rtp: object) -> List[PublicRtspEvent]:
        rtsp = self._client
        events: List[PublicRtspEvent] = [self._new_rtp_event(channel, rtp)]
        if self.enable_video and channel == rtsp.video_rtp_channel and rtsp.video_splicer:
            for vframe in rtsp.video_splicer.try_get_video_frame(rtp):
                events.append(VideoFrameEvent(
                    event='video_frame',
                    msg_type=RtspClientMsgType.VideoFrame,
                    session_elapsed=rtsp.tick.since_start(),
                    frame=vframe,
                ))
        elif self.enable_audio and channel == rtsp.audio_rtp_channel and rtsp.audio_splicer:
            for aframe in rtsp.audio_splicer.try_get_audio_frames(rtp):
                events.append(AudioFrameEvent(
                    event='audio_frame',
                    msg_type=RtspClientMsgType.AudioFrame,
                    session_elapsed=rtsp.tick.since_start(),
                    frame=aframe,
                ))
        return events


def open_session(
    rtsp_url: str,
    forward_address: Optional[SocketAddress] = None,
    timeout: float = 4,
    session_id: Optional[str] = None,
    enable_video: bool = True,
    enable_audio: bool = True,
    log_type: int = DEFAULT_LOG_TYPE,
    log_prefix: str = '',
) -> RtspSession:
    return RtspSession(
        rtsp_url,
        forward_address=forward_address,
        timeout=timeout,
        log_type=log_type,
        log_prefix=log_prefix,
        session_id=session_id,
        enable_video=enable_video,
        enable_audio=enable_audio,
    )
