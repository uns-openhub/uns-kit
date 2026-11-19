"""Independent QoS-1 admission and a single fenced journal consumer."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import ssl
import threading
from typing import Any, Awaitable, Callable

from .async_worker import BoundedAsyncWorker
from .durable_journal import DurableInputJournal, JournalError, JournalRecord, bounded_json, identifier


class DurableMqttReceiver:
    """Commit input before MQTT ACK, independent of publisher ownership.

    Uses public paho manual_ack (aiomqtt's automatic ACK cannot establish this
    boundary). Capacity/storage failures disconnect without ACK and stay latched
    until explicit resume. Persistent broker sessions are required for redelivery.
    """

    def __init__(self, journal: DurableInputJournal, host: str, topics: list[str], *,
                 client_id: str, port: int = 1883, username: str | None = None,
                 password: str | None = None, tls: bool = False) -> None:
        from paho.mqtt.client import CallbackAPIVersion, Client, MQTTv311
        identifier(client_id)
        if not isinstance(host, str) or not host or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("Invalid MQTT endpoint")
        if not topics or len(topics) > 64:
            raise ValueError("Receiver requires 1-64 MQTT filters")
        for topic in topics:
            if not isinstance(topic, str) or not topic or len(topic.encode()) > 512 or "\x00" in topic:
                raise ValueError("Invalid MQTT filter")
            parts = topic.split("/")
            if any("+" in part and part != "+" or "#" in part and (part != "#" or index != len(parts) - 1) for index, part in enumerate(parts)):
                raise ValueError("Invalid MQTT filter")
        self.journal, self.host, self.port = journal, host, port
        self._topics, self._identity = list(topics), client_id
        self._binding = hashlib.sha256(bounded_json({"host": host, "port": port, "tls": tls,
            "topics": sorted(topics), "clientId": client_id})).hexdigest()
        self._client = Client(CallbackAPIVersion.VERSION2, client_id=client_id,
                              clean_session=False, protocol=MQTTv311, manual_ack=True)
        if username is not None:
            self._client.username_pw_set(username, password)
        if tls:
            self._client.tls_set_context(ssl.create_default_context())
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_message = self._on_message
        self._ready = threading.Event()
        self._failed: BaseException | None = None
        self._started = False
        self._control = asyncio.Lock()
        self._start_task: asyncio.Task | None = None
        self._resume_task: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None
        self._stopping = False

    def _on_connect(self, client: Any, userdata: Any, flags: Any, reason: Any, properties: Any) -> None:
        if reason.is_failure:
            self._failed = JournalError("MQTT durable receiver connection rejected")
            return
        if not flags.session_present:
            with self.journal._transaction() as db:
                db.execute("DELETE FROM deliveries")
        client.subscribe([(topic, 1) for topic in self._topics])

    def _on_subscribe(self, client: Any, userdata: Any, mid: int, reasons: Any, properties: Any) -> None:
        if any(reason.is_failure or reason.value != 1 for reason in reasons):
            self._failed = JournalError("Durable receiver requires acknowledged QoS 1 subscriptions")
            client.disconnect()
        else:
            self._ready.set()

    def _on_disconnect(self, client: Any, userdata: Any, flags: Any, reason: Any, properties: Any) -> None:
        self._ready.clear()

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        try:
            if self._failed is not None or message.qos != 1:
                raise JournalError("Durable admission requires QoS 1 source publications")
            self.journal.admit(message.topic, bytes(message.payload), mid=message.mid,
                               duplicate=bool(message.dup), retained=bool(message.retain))
            if client.ack(message.mid, message.qos) != 0:
                raise JournalError("MQTT ACK is unconfirmed; retained admission may be redelivered")
        except BaseException as error:
            self._failed = error
            self._ready.clear()
            client.disconnect()

    async def start(self, *, timeout: float = 10.0) -> None:
        if self._stopping:
            raise JournalError("Receiver is stopping or stopped; create a new receiver")
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._start(timeout))
            self._start_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        await asyncio.shield(self._start_task)

    async def _start(self, timeout: float) -> None:
        async with self._control:
            if self._started:
                if self._failed is not None:
                    raise JournalError("Receiver is paused; review failure and call resume") from self._failed
                return
            self.journal.acquire_receiver(self._identity)
            try:
                with self.journal._transaction() as db:
                    previous = db.execute("SELECT value FROM metadata WHERE key='receiver_source'").fetchone()
                    if previous and previous[0] != self._binding:
                        raise JournalError("Receiver broker/source binding changed")
                    db.execute("INSERT OR REPLACE INTO metadata VALUES ('receiver_source',?)", (self._binding,))
                self._started = True
                await asyncio.to_thread(self._client.connect, self.host, self.port, 30)
                self._client.loop_start()
                await self._wait_ready(timeout)
            except BaseException:
                if not self._started:
                    self.journal.release_receiver()
                # If network setup started, keep ownership for explicit stop;
                # cancellation must not permit a second receiver during connect.
                raise

    async def _wait_ready(self, timeout: float) -> None:
        async def wait() -> None:
            while not self._ready.is_set():
                if self._failed is not None:
                    raise JournalError("Durable MQTT receiver is paused") from self._failed
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), timeout)

    async def resume(self, *, timeout: float = 10.0) -> None:
        """Retry transport/admission only; never retry a business callback."""
        if self._stopping:
            raise JournalError("Receiver is stopping or stopped")
        if self._start_task is not None:
            await asyncio.gather(asyncio.shield(self._start_task), return_exceptions=True)
        if self._resume_task is None or self._resume_task.done():
            self._resume_task = asyncio.create_task(self._resume(timeout))
            self._resume_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        await asyncio.shield(self._resume_task)

    async def _resume(self, timeout: float) -> None:
        async with self._control:
            if not self._started:
                raise JournalError("Start receiver before resume")
            await asyncio.to_thread(self._client.loop_stop)
            self._failed = None
            self._ready.clear()
            await asyncio.to_thread(self._client.reconnect)
            self._client.loop_start()
            await self._wait_ready(timeout)

    def status(self) -> dict[str, Any]:
        return {"ready": self._ready.is_set() and self._failed is None, "paused": self._failed is not None}

    async def stop(self) -> None:
        self._stopping = True
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
            self._stop_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        await asyncio.shield(self._stop_task)

    async def _stop(self) -> None:
        for task in (self._start_task, self._resume_task):
            if task is not None:
                await asyncio.gather(asyncio.shield(task), return_exceptions=True)
        async with self._control:
            if self._started:
                self._client.disconnect()
                await asyncio.to_thread(self._client.loop_stop)
                self._started = False
                self._ready.clear()
                self.journal.release_receiver()


class JournalInput:
    """Single serial handler; checkpoint follows callback AND output flush.

    Pending disk records are not yet admitted to the application. Release stops
    taking new records, finishes its one admitted callback and leaves the rest
    for a confirmed next owner. Unknown work blocks restart, not automatic replay.
    """

    def __init__(self, journal: DurableInputJournal, handler: Callable[[JournalRecord], Awaitable[None]], *,
                 owner_id: str, active: Callable[[], bool] = lambda: True,
                 flush: Callable[[], Awaitable[None]] | None = None) -> None:
        if not callable(handler) or not callable(active) or (flush is not None and not callable(flush)):
            raise TypeError("Invalid journal input callbacks")
        self.journal, self.owner_id = journal, identifier(owner_id)
        self._handler, self._active, self._flush = handler, active, flush
        self._worker = BoundedAsyncWorker(max_pending=4)
        self._task: asyncio.Task | None = None
        self._failure: BaseException | None = None
        self._releasing = False
        self._owns = False
        self._closed = False
        self._stop_task: asyncio.Task | None = None
        self._start_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._closed or self._releasing:
            raise JournalError("Journal input is already started or releasing")
        if self._active() is not True:
            raise JournalError("Journal input has no active owner")
        if self._start_task is None:
            self._start_task = asyncio.create_task(self._start())
            self._start_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        await asyncio.shield(self._start_task)

    async def _start(self) -> None:
        await self._worker.run(lambda: self.journal.acquire_consumer(self.owner_id))
        self._owns = True
        if not self._releasing:
            self._start_owned()

    def _start_owned(self) -> None:
        self._task = asyncio.create_task(self._consume())
        self._task.add_done_callback(lambda task: None if task.cancelled() else task.exception())

    async def _consume(self) -> None:
        while not self._releasing:
            if self._active() is not True:
                await asyncio.sleep(0.02)
                continue
            record = await self._worker.run(lambda: self.journal.take(self.owner_id))
            if record is None:
                await asyncio.sleep(0.02)
                continue
            try:
                result = self._handler(record)
                if not inspect.isawaitable(result):
                    raise TypeError("Journal handler must be asynchronous")
                await result
                if self._flush is not None:
                    await self._flush()
                await self._worker.run(lambda: self.journal.settle(self.owner_id, record.id, confirmed=True))
            except BaseException as error:
                self._failure = error
                await self._worker.run(lambda: self.journal.settle(self.owner_id, record.id, confirmed=False))
                return

    def begin_release(self) -> None:
        self._releasing = True

    async def drain(self, timeout: float | None = 30.0) -> None:
        self.begin_release()
        async def wait() -> None:
            if self._start_task is not None:
                await asyncio.shield(self._start_task)
            if self._task is not None:
                await asyncio.shield(self._task)
        await asyncio.wait_for(wait(), timeout)
        if self._failure is not None:
            raise JournalError("Journal input effect remains unconfirmed") from self._failure

    async def _release_ownership(self, *, transferred: bool = False) -> None:
        if self._owns:
            await self._worker.run(lambda: self.journal.release_consumer(self.owner_id, transferred=transferred))
            self._owns = False

    async def stop(self, *, timeout: float | None = 30.0) -> None:
        if self._stop_task is None:
            async def finish() -> None:
                await self.drain(timeout)
                await self._release_ownership()
                await self._worker.close()
                self._closed = True
            self._stop_task = asyncio.create_task(finish())
            self._stop_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        task = self._stop_task
        try:
            await asyncio.shield(task)
        except Exception:
            if self._stop_task is task:
                self._stop_task = None
            raise
