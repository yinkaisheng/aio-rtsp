from __future__ import annotations

import asyncio
import socket
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Dict, Generic, Hashable, Iterable, List, Optional, TypeVar, Union, cast

import aio_sockets as aio

"""
Generic TCP client base class.

Event semantics:
1. events() yields only unsolicited server-push messages, plus any later reply to a message
   that was sent with expect_response=False. Such a later reply is treated as an
   unsolicited push message.
2. For send_msg(..., expect_response=True, timeout=...), the returned PendingRequest.future
   resolves to the matching response, raises RequestTimeoutError on timeout, or raises
   RequestInterruptedError on close. Both exception types expose the original
   request_message and request_key; RequestTimeoutError also exposes timeout, and
   RequestInterruptedError also exposes reason.
3. After a request times out, any later response for the same request_key is discarded.
4. When the connection closes, events() emits one ConnectionClosed event, and all unfinished
   pending requests fail as RequestInterruptedError.
5. When awaiting a pending request raises RequestInterruptedError, business code usually only
   needs to stop the current flow and return early. The library has already started internal
   connection cleanup, and events() will later emit ConnectionClosed. Business code normally
   does not need to call close() again just because of that interruption.

Subclasses must implement at least:
1. encode_message: encode a business message into bytes
2. feed_data: split incoming TCP byte stream data into business message objects
3. get_request_key: extract the correlation key from a client request that expects a response
4. get_response_key: extract the correlation key from a server response

Correlation key usage:
1. It matches a client request with its corresponding server response.
2. Internally it is used as the key in _pending_requests for timeout tracking and response matching.
3. It does not need to be globally unique forever, but it must be unique among all currently
   pending requests.
4. If two unfinished requests use the same correlation key, the base class cannot decide which
   request owns the response, so duplicate keys are rejected.
5. A correlation key may be reused only after the previous request has completed, timed out,
   or been finished by disconnect.

Typical examples:
1. RTSP can use CSeq directly as the correlation key.
2. A custom binary protocol can use seq_id / request_id / transaction_id as the correlation key.

Minimal usage example:

    class DemoClient(AsyncTCPClientBase):
        def __init__(self, server_ip, server_port):
            super().__init__(server_ip, server_port)
            self._buffer = bytearray()

        def encode_message(self, message):
            return (message["payload"] + "\\n").encode("utf-8")

        def feed_data(self, data):
            self._buffer.extend(data)
            messages = []
            while True:
                pos = self._buffer.find(b"\\n")
                if pos < 0:
                    break
                line = bytes(self._buffer[:pos])
                del self._buffer[:pos + 1]
                text = line.decode("utf-8")
                if text.startswith("resp:"):
                    seq = int(text.split(":", 2)[1])
                    messages.append({"kind": "resp", "seq": seq, "raw": text})
                else:
                    messages.append({"kind": "notify", "raw": text})
            return messages

        def get_request_key(self, message):
            return message.get("seq")

        def get_response_key(self, message):
            if message.get("kind") == "resp":
                return message.get("seq")
            return None

    async def main():
        client = DemoClient("127.0.0.1", 9000)
        await client.connect()

        # Fire-and-forget message. Any later response is delivered through events().
        await client.send_msg({"payload": "notify:hello"}, expect_response=False)

        # Request-response message. The caller awaits the future directly.
        pending = await client.send_msg({"seq": 1, "payload": "req:1"}, expect_response=True, timeout=5)
        assert pending is not None
        try:
            result = await pending.future
            print("request result:", result)
        except RequestTimeoutError as ex:
            print("request timed out:", ex.request_key, ex.timeout, ex.request_message)
        except RequestInterruptedError as ex:
            print("request interrupted:", ex.request_key, ex.reason, ex.request_message)

        async for event in client.events():
            print(event)
"""


_STREAM_CLOSED = object()

MessageT = TypeVar("MessageT")
KeyT = TypeVar("KeyT", bound=Hashable)


@dataclass
class ConnectionClosed:
    reason: str
    exception: Optional[Exception] = None


class RequestTimeoutError(asyncio.TimeoutError, Generic[MessageT, KeyT]):
    def __init__(self, request_message: MessageT, request_key: KeyT, timeout: float) -> None:
        self.request_message = request_message
        self.request_key = request_key
        self.timeout = timeout
        super().__init__(f"request {request_key!r} timed out after {timeout}s")


class RequestInterruptedError(Exception, Generic[MessageT, KeyT]):
    def __init__(self, request_message: MessageT, request_key: KeyT, reason: str) -> None:
        self.request_message = request_message
        self.request_key = request_key
        self.reason = reason
        super().__init__(f"request {request_key!r} interrupted: {reason}")


@dataclass
class PendingRequest(Generic[MessageT, KeyT]):
    message: MessageT
    request_key: KeyT
    timeout: float
    timeout_task: asyncio.Task
    future: asyncio.Future
    created_at: float = field(default_factory=time.monotonic)


class AsyncTCPClientBase(ABC, Generic[MessageT, KeyT]):
    def __init__(
        self,
        server_ip: str,
        server_port: int,
        *,
        recv_size: int = 8192,
        late_response_ttl: float = 60.0,
        recv_timeout: float = None,
        stop_timeout: float = 0.2,
        event_queue_maxsize: int = 0,
        log_tag: str = ''
    ) -> None:
        self.server_ip = server_ip
        self.server_port = server_port
        self.recv_size = recv_size
        self.late_response_ttl = late_response_ttl
        self.recv_timeout = recv_timeout
        self.stop_timeout = stop_timeout
        self.log_tag = log_tag

        self._sock: Optional[aio.TCPSocket] = None
        self._sock_recv_task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue = aio.Queue(maxsize=event_queue_maxsize)
        self._pending_requests: Dict[KeyT, PendingRequest[MessageT, KeyT]] = {}
        self._expired_request_keys: Dict[KeyT, float] = {}
        self._stream_finished = False
        self._closed = False

    @property
    def is_connected(self) -> bool:
        return self._sock is not None and not self._closed

    async def connect(self) -> None:
        """
        Establish the TCP connection.

        Returns immediately if the client is already connected. Any connection failure from
        the underlying socket layer propagates to the caller.
        """
        if self.is_connected:
            return
        family = socket.AF_INET6 if ":" in self.server_ip else socket.AF_INET
        self._sock = await aio.open_tcp_connection(
            self.server_ip,
            self.server_port,
            family=family,
        )
        self._closed = False
        self._stream_finished = False

    def start_sock_recv_task(self) -> asyncio.Task:
        """
        Ensure the background recv task is running and return it.

        The task is reused while still alive. It normally finishes when the socket closes or
        recv processing fails.
        """
        if self._sock_recv_task is not None and not self._sock_recv_task.done():
            return self._sock_recv_task
        self._sock_recv_task = aio.create_task(self._sock_recv_loop())
        return self._sock_recv_task

    async def send_msg(
        self,
        message: MessageT,
        *,
        expect_response: bool = False,
        timeout: Optional[float] = None,
        request_key: Optional[KeyT] = None,
    ) -> Optional[PendingRequest[MessageT, KeyT]]:
        """
        Send one business message.

        If expect_response=True, timeout is required and the returned PendingRequest.future
        must be awaited for the final result. Returns a PendingRequest. That future resolves
        to the normal response, raises RequestTimeoutError on timeout, or raises
        RequestInterruptedError on interruption. RequestTimeoutError is also an
        asyncio.TimeoutError subclass.

        When awaiting pending.future raises RequestInterruptedError, the library has already
        started internal connection cleanup for that disconnect/interruption path, and
        events() will later emit ConnectionClosed. Business code usually only needs to stop
        the current flow and return early, instead of calling close() again just because
        that await was interrupted.

        If expect_response=False, timeout must be None. Returns None. Any later response for
        the message is delivered only through events().

        Raises:
            RuntimeError: The client is not connected, or the socket becomes unavailable
                before the write starts.
            ValueError: The expect_response / timeout / request_key combination is invalid.
            Exception: encode_message() or the underlying socket send may raise. On send
                failure, the client closes the connection before re-raising the exception.
        """
        if not self.is_connected:
            raise RuntimeError("client is not connected; call connect() before send_msg()")

        # if self._sock_recv_task is None or self._sock_recv_task.done():
        #     self.start_sock_recv_task()

        if expect_response:
            if timeout is None:
                raise ValueError("timeout is required when expect_response=True")
            if request_key is None:
                request_key = self.get_request_key(message)
            if request_key is None:
                raise ValueError("request_key is required for messages expecting a response")
            if request_key in self._pending_requests:
                raise ValueError(f"duplicate pending request_key: {request_key!r}")
        elif timeout is not None:
            raise ValueError("timeout must be None when expect_response=False")

        payload = self.encode_message(message)
        pending = None

        if expect_response:
            future = asyncio.get_running_loop().create_future()
            timeout_task = aio.create_task(self._request_timeout_loop(request_key, timeout))
            pending = PendingRequest(
                message=message,
                request_key=cast(KeyT, request_key),
                timeout=timeout,
                timeout_task=timeout_task,
                future=future,
            )
            self._pending_requests[request_key] = pending

        try:
            async with self._send_lock:
                sock = self._sock
                if sock is None:
                    raise RuntimeError("client socket is closed before send") # todo
                await sock.send(payload)
        except Exception as ex:
            await self.close(reason="send failed", exception=ex)
            raise

        return pending

    async def close(self, *, reason: str = "client closed connection", exception: Optional[Exception] = None) -> None:
        """
        Close the connection and finish the event stream.

        All unfinished pending requests fail as RequestInterruptedError with the same
        reason. After cleanup, events() emits one ConnectionClosed event carrying reason
        and exception, then terminates.
        """
        if self._closed:
            return

        self._closed = True
        sock = self._sock
        self._sock = None

        if sock is not None:
            aio.aio_sockets.logger.info(f'{self.log_tag} close sock')
            try:
                await sock.close()
            except Exception:
                pass

        sock_recv_task = self._sock_recv_task
        self._sock_recv_task = None
        if sock_recv_task is not None:
            try:
                await sock_recv_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        await self._finish_pending_requests_on_disconnect(reason)
        await self._emit_event(ConnectionClosed(reason=reason, exception=exception))
        await self._finish_event_stream()

    async def graceful_close(
        self,
        stop_message: Optional[MessageT] = None,
        *,
        timeout: Optional[float] = None,
        request_key: Optional[KeyT] = None,
    ) -> None:
        """
        Optionally send a final request-response message, wait up to timeout seconds for its
        result, then close the connection.

        The send phase follows the same validation and exception behavior as send_msg().
        """
        if stop_message is None:
            await self.close(reason="client requested close")
            return

        wait_timeout = self.stop_timeout if timeout is None else timeout
        pending = await self.send_msg(
            stop_message,
            timeout=wait_timeout,
            expect_response=True,
            request_key=request_key,
        )

        ret = None
        try:
            ret = await asyncio.wait_for(asyncio.shield(pending.future), timeout=wait_timeout + 0.05)
        except Exception:
            pass

        await self.close(reason="graceful close finished")
        return ret

    async def events(self) -> AsyncIterator[Any]:
        """
        Yield business events in order.

        This API does not actively raise disconnect exceptions. Disconnect semantics are
        expressed through events such as ConnectionClosed.
        """
        while True:
            event = await self._event_queue.get()
            if event is _STREAM_CLOSED:
                break
            yield event

    async def _sock_recv_loop(self) -> None:
        exception = None  # type: Optional[Exception]
        reason = ''
        sock = self._sock
        try:
            while True:
                if self.recv_timeout is None:
                    data = await sock.recv(self.recv_size)
                else:
                    try:
                        data = await sock.recv_timeout(self.recv_size, timeout=self.recv_timeout)
                    except asyncio.TimeoutError:
                        reason = f'socket receive timed out after {self.recv_timeout}s'
                        break
                if not data:
                    aio.aio_sockets.logger.info(f'{self.log_tag} recv None, connection closed by peer')
                    reason = 'server closed connection' # not real, if user call close
                    break
                for message in self.feed_data(data):
                    await self._handle_incoming_message(message)
        except asyncio.CancelledError as ex:
            # Defensive path: this task is normally expected to finish from socket close or
            # recv failure, but external task cancellation is still possible.
            exception = ex
            reason = 'recv loop cancelled'
            raise
        except Exception as ex:
            exception = ex
            reason = f'recv loop failed with ex={ex!r}'
        finally:
            aio.aio_sockets.logger.info(f'{self.log_tag} close sock')
            try:
                await sock.close()
            except Exception:
                pass
        if self._closed:
            return # user has called close
        self._closed = True
        self._sock = None
        await self._finish_pending_requests_on_disconnect(reason)
        await self._emit_event(ConnectionClosed(reason=reason, exception=exception))
        await self._finish_event_stream()

    async def _handle_incoming_message(self, message: object) -> None:
        self._purge_expired_request_keys()

        response_key = self.get_response_key(message)
        if response_key is None:
            await self._emit_event(message)
            return

        pending = self._pending_requests.pop(response_key, None)
        if pending is not None:
            pending.timeout_task.cancel()
            if not pending.future.done():
                pending.future.set_result(message)
            return

        if response_key in self._expired_request_keys:
            return

        await self._emit_event(message)

    async def _request_timeout_loop(self, request_key: KeyT, timeout: float) -> None:
        try:
            await asyncio.sleep(timeout)
            pending = self._pending_requests.pop(request_key, None)
            if pending is None:
                return

            self._expired_request_keys[request_key] = time.monotonic()
            if not pending.future.done():
                pending.future.set_exception(
                    RequestTimeoutError(
                        request_message=pending.message,
                        request_key=request_key,
                        timeout=timeout,
                    )
                )
        except asyncio.CancelledError:
            # Expected path: matching response or disconnect cancels the timeout task.
            raise

    async def _finish_pending_requests_on_disconnect(self, reason: str) -> None:
        for pending in list(self._pending_requests.values()):
            pending.timeout_task.cancel()
            if not pending.future.done():
                pending.future.set_exception(
                    RequestInterruptedError(
                        request_message=pending.message,
                        request_key=pending.request_key,
                        reason=reason,
                    )
                )
        self._pending_requests.clear()

    async def _emit_event(self, event: Any) -> None:
        if self._stream_finished:
            return
        await self._event_queue.put(event)

    async def _finish_event_stream(self) -> None:
        if self._stream_finished:
            return
        self._stream_finished = True
        await self._event_queue.put(_STREAM_CLOSED)

    def _purge_expired_request_keys(self) -> None:
        if not self._expired_request_keys:
            return
        now = time.monotonic()
        expired_keys = [
            key
            for key, created_at in self._expired_request_keys.items()
            if now - created_at >= self.late_response_ttl
        ]  # type: List[KeyT]
        for key in expired_keys:
            self._expired_request_keys.pop(key, None)

    @abstractmethod
    def encode_message(self, message: MessageT) -> bytes:
        """
        Encode one business message into bytes.

        Any exception raised here propagates from send_msg(). If the later socket send fails,
        the connection is closed and the send exception is re-raised.
        """
        raise NotImplementedError

    @abstractmethod
    def feed_data(self, data: bytes) -> Iterable[Any]:
        """
        Parse received bytes into business messages.

        Each returned message is passed to get_response_key(). If it is recognized as a
        response to a pending request, it is consumed internally and completes the matching
        PendingRequest.future. Otherwise it is emitted to callers through events().

        Any exception raised here is treated as a recv-loop failure: the connection is closed,
        and the original exception is attached to the resulting ConnectionClosed event.
        """
        raise NotImplementedError

    @abstractmethod
    def get_request_key(self, message: MessageT) -> Optional[KeyT]:
        """
        Return the correlation key for a client request that expects a response.

        This key is used to match the later response and is also used as the index in the
        pending-request table. It does not need to be globally unique, but it must be unique
        within the set of currently unfinished requests.

        Any exception raised here propagates from send_msg().

        Return None for messages that do not expect a response.
        """
        raise NotImplementedError

    @abstractmethod
    def get_response_key(self, message: object) -> Optional[KeyT]:
        """
        Extract the correlation key from a server response.

        The returned value must match the key produced by get_request_key() for the
        corresponding request, so the base class can find the original request, cancel its
        timeout task, and emit the response into the event stream.

        Any exception raised here is treated as a recv-loop failure: the connection is closed,
        and the original exception is attached to the resulting ConnectionClosed event.

        Return None for non-response messages.
        """
        raise NotImplementedError
