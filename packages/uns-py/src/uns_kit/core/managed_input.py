"""Single-owner, bounded MQTT input. Failed work is retained, never replayed."""

from __future__ import annotations

import asyncio
import contextlib
from copy import deepcopy
from typing import Any, Awaitable, Callable


class ManagedInputError(RuntimeError):
    pass


class ManagedMqttInput:
    def __init__(self, client: Any, *, max_pending: int = 64, max_bytes: int = 8 * 1024 * 1024, max_message_bytes: int = 1024 * 1024) -> None:
        for name, value in (("max_pending_inputs", max_pending), ("max_input_bytes", max_bytes), ("max_input_message_bytes", max_message_bytes)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self.client = client
        self._limit = max_pending
        self._byte_limit = max_bytes
        self._message_limit = max_message_bytes
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_pending)
        self._changed = asyncio.Event()
        self._ready = asyncio.Event()
        self._transport_lock = asyncio.Lock()
        self._handler: Callable[[Any], Awaitable[None]] | None = None
        self._supervisor: asyncio.Task | None = None
        self._consumer: asyncio.Task | None = None
        self._drain_task: asyncio.Task | None = None
        self._session: Any = None
        self._session_disconnected = False
        self._releasing = False
        self._sealed = False
        self._failure: BaseException | None = None
        self._active_message: Any = None
        self._failed_message: Any = None
        self._pending = 0
        self._bytes = 0
        self._topics: list[str] = []
        self._qos = 1

        owner = self

        class IncomingQueue(asyncio.Queue):
            # aiomqtt's public queue_type hook: retain data in the SDK queue,
            # not in a second unbounded library queue. The empty queue iterator
            # is used only to observe transport disconnects.
            def put_nowait(self, message: Any) -> None:
                if not owner._admit(message):
                    raise asyncio.QueueFull

        client._managed_queue_type = IncomingQueue
        client._managed_input_owner = self

    def set_handler(self, handler: Callable[[Any], Awaitable[None]]) -> None:
        if not callable(handler) or self._handler is not None or self._releasing:
            raise ManagedInputError("Managed input requires one handler registered before startup")
        self._handler = handler

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._ready.clear()
        self._changed.set()

    def _admit(self, message: Any) -> bool:
        size = len(message.payload or b"") + len(str(message.topic).encode())
        if self._sealed or self._supervisor is None:
            self._fail(ManagedInputError("MQTT input arrived outside its subscription boundary"))
            return False
        if size > self._message_limit or self._pending >= self._limit or self._bytes + size > self._byte_limit:
            self._fail(ManagedInputError("Managed MQTT input capacity exceeded; source reconciliation required"))
            return False
        self._queue.put_nowait((message, size))
        self._pending += 1
        self._bytes += size
        self._changed.set()
        return True

    @property
    def ready(self) -> bool:
        return self._ready.is_set() and not self._releasing and self._failure is None

    @property
    def pending(self) -> int:
        return self._pending

    def status(self) -> dict[str, Any]:
        return {
            "managed": True, "ready": self.ready, "pending_messages": self._pending,
            "pending_bytes": self._bytes, "failed": self._failure is not None,
            "releasing": self._releasing, "sealed": self._sealed,
        }

    @property
    def failed_message(self) -> Any:
        return self._failed_message

    async def start(self, topics: str | list[str], *, qos: int = 1, timeout: float | None = 30.0) -> None:
        normalized = [topics] if isinstance(topics, str) else list(topics)
        if not normalized or len(normalized) > 64 or any(not isinstance(topic, str) or not topic or len(topic.encode()) > 512 or "\x00" in topic for topic in normalized):
            raise ValueError("Managed input requires 1-64 nonempty bounded MQTT filters")
        if type(qos) is not int or qos not in (0, 1, 2):
            raise ValueError("MQTT input QoS must be 0, 1 or 2")
        for topic in normalized:
            parts = topic.split("/")
            if any("+" in part and part != "+" or "#" in part and (part != "#" or index != len(parts) - 1) for index, part in enumerate(parts)):
                raise ValueError("Invalid MQTT subscription filter")
        if self._releasing or self._handler is None:
            raise ManagedInputError("Register the managed handler before starting a live input")
        if self._supervisor is not None:
            if normalized != self._topics or qos != self._qos:
                raise ManagedInputError("A managed input cannot change its admitted source")
        else:
            self._topics, self._qos = normalized, qos
            self._consumer = asyncio.create_task(self._consume())
            self._supervisor = asyncio.create_task(self._supervise())
        await asyncio.wait_for(self._wait_ready(), timeout=timeout)

    async def _wait_ready(self) -> None:
        while not self._ready.is_set():
            if self._failure is not None:
                raise ManagedInputError("Managed MQTT input is unconfirmed") from self._failure
            if self._releasing:
                raise ManagedInputError("Managed MQTT input is releasing")
            self._changed.clear()
            await self._changed.wait()

    async def _supervise(self) -> None:
        try:
            while not self._releasing and self._failure is None:
                await self.client._ensure_connected()
                async with self._transport_lock:
                    if self._releasing:
                        return
                    self._session = self.client._client
                    self._session_disconnected = False
                    await self._session.subscribe([(topic, self._qos) for topic in self._topics])
                    self._ready.set()
                    self._changed.set()
                try:
                    # All messages enter through IncomingQueue above. This
                    # iterator waits only for disconnection; it owns no payload.
                    await self._session.messages.__anext__()
                    self._fail(ManagedInputError("Unexpected second MQTT input queue"))
                    return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._session_disconnected = True
                    self._ready.clear()
                    if self.client._client is self._session:
                        self.client._connected.clear()
                    with contextlib.suppress(Exception):
                        await self._session.__aexit__(None, None, None)
                    if not self._releasing:
                        await asyncio.sleep(self.client.reconnect_interval)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(error)

    async def _consume(self) -> None:
        while True:
            self._changed.clear()
            if self._failure is not None:
                return
            if self._queue.empty():
                if self._sealed:
                    return
                await self._changed.wait()
                continue
            message, size = self._queue.get_nowait()
            self._active_message = message
            try:
                assert self._handler is not None
                await self._handler(deepcopy(message))
            except BaseException as error:
                self._failed_message = message
                self._fail(error)
                return
            self._active_message = None
            self._pending -= 1
            self._bytes -= size
            self._queue.task_done()

    def begin_release(self) -> None:
        self._releasing = True
        self._ready.clear()
        self._changed.set()

    async def drain(self, timeout: float | None = 30.0) -> None:
        self.begin_release()
        if self._drain_task is None:
            self._drain_task = asyncio.create_task(self._finish())
            self._drain_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        task = self._drain_task
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except Exception:
            if task.done() and self._failure is None and self._drain_task is task:
                self._drain_task = None
            raise

    async def _finish(self) -> None:
        async with self._transport_lock:
            if self._session is not None and not self._session_disconnected:
                await self._session.unsubscribe(self._topics)
            self._sealed = True
            self._changed.set()
        if self._supervisor is not None and not self._supervisor.done():
            self._supervisor.cancel()
            await asyncio.gather(self._supervisor, return_exceptions=True)
        if self._consumer is not None:
            await asyncio.shield(self._consumer)
        if self._failure is not None:
            raise ManagedInputError("Managed MQTT input is unconfirmed; original work retained") from self._failure
