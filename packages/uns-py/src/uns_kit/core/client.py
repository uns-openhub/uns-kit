from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import socket
from typing import AsyncIterator, List, Optional
import uuid

import aiomqtt

from .packet import UnsPacket
from .topic_builder import TopicBuilder

MqttError = aiomqtt.MqttError


class UnsMqttClient:
    _exception_handler_installed = False
    _exception_handler_loop_id: int | None = None

    def __init__(
        self,
        host: str,
        *,
        topic_builder: TopicBuilder,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        tls: bool = False,
        keepalive: int = 60,
        clean_session: bool = True,
        reconnect_interval: float = 2.0,
        max_reconnect_interval: float = 30.0,
        instance_name: Optional[str] = None,
        publisher_active: Optional[bool] = None,
        subscriber_active: Optional[bool] = None,
        stats_interval: float = 60.0,
        enable_status: bool = True,
    ):
        self.host = host
        self.port = port
        self.username = username if username not in ("", None) else None
        self.password = password if password not in ("", None) else None
        if client_id:
            self.client_id = client_id
        else:
            base = topic_builder.process_name
            if instance_name:
                base = f"{base}-{instance_name}"
            self.client_id = f"{base}-{uuid.uuid4().hex[:8]}"
        self.tls = tls
        self.keepalive = keepalive
        self.clean_session = clean_session
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_interval = max_reconnect_interval
        self.topic_builder = topic_builder
        self.instance_name = instance_name
        self.publisher_active = publisher_active
        self.subscriber_active = subscriber_active
        self.stats_interval = stats_interval
        self.enable_status = enable_status
        self._client: Optional[aiomqtt.Client] = None
        self._status_task: Optional[asyncio.Task] = None
        self._stats_task: Optional[asyncio.Task] = None
        self._connected = asyncio.Event()
        self._closing = False
        self._managed_input_owner = None
        self._managed_queue_type = None
        self._connect_lock = asyncio.Lock()
        self._published_message_count = 0
        self._published_message_bytes = 0
        self._subscribed_message_count = 0
        self._subscribed_message_bytes = 0

        if self.instance_name:
            self.status_topic = self.topic_builder.instance_status_topic(self.instance_name)
        else:
            self.status_topic = self.topic_builder.process_status_topic

    def _install_exception_handler(self) -> None:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        if UnsMqttClient._exception_handler_installed and UnsMqttClient._exception_handler_loop_id == loop_id:
            return
        original = loop.get_exception_handler()

        def handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
            exc = context.get("exception")
            message = context.get("message", "")
            if isinstance(exc, MqttError) and "Unexpected disconnect" in str(exc):
                # Swallow noisy disconnect futures; reconnect logic handles it.
                return
            if isinstance(exc, MqttError) and "Future exception was never retrieved" in message:
                return
            if original:
                original(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(handler)
        UnsMqttClient._exception_handler_installed = True
        UnsMqttClient._exception_handler_loop_id = loop_id

    async def _connect_once(self) -> None:
        await self._probe_socket()
        will = aiomqtt.Will(
            topic=f"{self.status_topic}alive",
            payload=b"",
            qos=0,
            retain=True,
        )

        tls_context = None
        if self.tls:
            try:
                import ssl

                tls_context = ssl.create_default_context()
            except Exception:
                tls_context = None

        # aiomqtt supports async connect/disconnect directly.
        kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            # aiomqtt uses `identifier` for MQTT clientId.
            "identifier": self.client_id,
            "keepalive": self.keepalive,
            "will": will,
        }
        if tls_context is not None:
            kwargs["tls_context"] = tls_context
        # aiomqtt v2.x supports clean_session; keep a fallback for API changes.
        kwargs["clean_session"] = self.clean_session
        if self._managed_queue_type is not None:
            kwargs["queue_type"] = self._managed_queue_type
        try:
            client = aiomqtt.Client(**kwargs)
        except TypeError:
            kwargs.pop("clean_session", None)
            client = aiomqtt.Client(**kwargs)

        # aiomqtt does not expose connect()/disconnect() methods; the client is
        # an async context manager.
        await client.__aenter__()
        self._client = client
        self._connected.set()

    async def _probe_socket(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            def probe() -> None:
                with socket.create_connection((self.host, self.port or 1883), timeout=2):
                    pass
            await loop.run_in_executor(None, probe)
        except Exception as exc:
            raise aiomqtt.MqttError(f"Cannot reach MQTT broker at {self.host}:{self.port or 1883}") from exc

    async def _ensure_connected(self) -> None:
        if self._connected.is_set():
            return
        async with self._connect_lock:
            if self._connected.is_set():
                return
            self._install_exception_handler()
            backoff = self.reconnect_interval
            while not self._closing:
                try:
                    await self._connect_once()
                    if self.enable_status:
                        if not self._status_task or self._status_task.done():
                            self._status_task = asyncio.create_task(self._publish_status_loop())
                            self._status_task.add_done_callback(self._handle_task_error)
                        if not self._stats_task or self._stats_task.done():
                            self._stats_task = asyncio.create_task(self._publish_stats_loop())
                            self._stats_task.add_done_callback(self._handle_task_error)
                    return
                except aiomqtt.MqttError:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.max_reconnect_interval)

    def _handle_task_error(self, task: asyncio.Task) -> None:
        try:
            task.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            self._connected.clear()

    async def connect(self) -> None:
        await self._ensure_connected()

    async def close(self) -> None:
        self._closing = True
        if self._status_task:
            self._status_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._status_task
        if self._stats_task:
            self._stats_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stats_task
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
        self._connected.clear()

    async def publish_raw(self, topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        await self._ensure_connected()
        for attempt in range(2):
            try:
                assert self._client
                payload_bytes = payload.encode() if isinstance(payload, str) else payload
                self._published_message_count += 1
                self._published_message_bytes += len(payload_bytes)
                await self._client.publish(topic, payload_bytes, qos=qos, retain=retain)
                return
            except aiomqtt.MqttError:
                self._connected.clear()
                if attempt == 0:
                    await self._ensure_connected()
                else:
                    raise

    async def publish_once_raw(self, topic: str, payload: str | bytes, *, qos: int = 1, retain: bool = False) -> None:
        """One broker attempt; a missing ACK is unknown, never an automatic replay."""
        await self._ensure_connected()
        assert self._client is not None
        data = payload.encode() if isinstance(payload, str) else payload
        self._published_message_count += 1
        self._published_message_bytes += len(data)
        try:
            await self._client.publish(topic, data, qos=qos, retain=retain)
        except aiomqtt.MqttError:
            self._connected.clear()
            raise

    async def publish_packet(self, topic: str, packet: dict, *, qos: int = 0, retain: bool = False) -> None:
        payload = UnsPacket.to_json(packet)
        await self.publish_raw(topic, payload, qos=qos, retain=retain)

    @asynccontextmanager
    async def messages(self, topics: str | List[str]) -> AsyncIterator[AsyncIterator[aiomqtt.Message]]:
        if self._managed_input_owner is not None:
            raise RuntimeError("Managed MQTT input owns this client's subscription and reader")
        await self._ensure_connected()
        assert self._client
        # aiomqtt exposes a single async iterator at `client.messages` (not a context manager).
        # Messages yielded are already filtered by the active subscriptions.
        messages = self._client.messages

        try:
            if isinstance(topics, str):
                await self._client.subscribe(topics)
            else:
                for t in topics:
                    await self._client.subscribe(t)

            async def wrapped() -> AsyncIterator[aiomqtt.Message]:
                try:
                    async for msg in messages:
                        self._subscribed_message_count += 1
                        self._subscribed_message_bytes += len(msg.payload or b"")
                        yield msg
                except aiomqtt.MqttError:
                    self._connected.clear()
                    return
                except asyncio.CancelledError:
                    return

            yield wrapped()
        except aiomqtt.MqttError:
            self._connected.clear()
            raise
        finally:
            # Best-effort unsubscribe; failure isn't fatal (e.g. disconnect while shutting down).
            with contextlib.suppress(Exception):
                await self._client.unsubscribe(topics)

    async def resilient_messages(self, topics: str | List[str]) -> AsyncIterator[aiomqtt.Message]:
        """
        Async generator that keeps the subscription alive across disconnects.
        """
        if self._managed_input_owner is not None:
            raise RuntimeError("Managed MQTT input owns this client's subscription and reader")
        while not self._closing:
            await self._ensure_connected()
            try:
                async with self.messages(topics) as msgs:
                    async for msg in msgs:
                        yield msg
            except aiomqtt.MqttError:
                self._connected.clear()
                await asyncio.sleep(self.reconnect_interval)
                continue
            except Exception:
                self._connected.clear()
                await asyncio.sleep(self.reconnect_interval)
                continue

    async def _publish_status_loop(self) -> None:
        uptime_topic = f"{self.status_topic}uptime"
        alive_topic = f"{self.status_topic}alive"
        publisher_topic = f"{self.status_topic}t-publisher-active"
        subscriber_topic = f"{self.status_topic}t-subscriber-active"
        start = asyncio.get_event_loop().time()
        try:
            while not self._closing:
                try:
                    now = asyncio.get_event_loop().time()
                    uptime_minutes = int((now - start) / 60)
                    time = datetime.now(timezone.utc)
                    alive_packet = UnsPacket.data(value=1, uom="bit", time=time)
                    uptime_packet = UnsPacket.data(value=uptime_minutes, uom="min", time=time)
                    await self.publish_raw(alive_topic, UnsPacket.to_json(alive_packet), qos=0, retain=False)
                    await self.publish_raw(uptime_topic, UnsPacket.to_json(uptime_packet), qos=0, retain=False)
                    if self.publisher_active is not None:
                        publisher_packet = UnsPacket.data(value=1 if self.publisher_active else 0, uom="bit", time=time)
                        await self.publish_raw(publisher_topic, UnsPacket.to_json(publisher_packet), qos=0, retain=False)
                    if self.subscriber_active is not None:
                        subscriber_packet = UnsPacket.data(value=1 if self.subscriber_active else 0, uom="bit", time=time)
                        await self.publish_raw(subscriber_topic, UnsPacket.to_json(subscriber_packet), qos=0, retain=False)
                except aiomqtt.MqttError:
                    self._connected.clear()
                except Exception:
                    self._connected.clear()
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async def _publish_stats_loop(self) -> None:
        published_count_topic = f"{self.status_topic}published-message-count"
        published_bytes_topic = f"{self.status_topic}published-message-bytes"
        subscribed_count_topic = f"{self.status_topic}subscribed-message-count"
        subscribed_bytes_topic = f"{self.status_topic}subscribed-message-bytes"
        try:
            while not self._closing:
                try:
                    time = datetime.now(timezone.utc)
                    published_count_packet = UnsPacket.data(value=self._published_message_count, time=time)
                    published_bytes_packet = UnsPacket.data(
                        value=round(self._published_message_bytes / 1024),
                        uom="kB",
                        time=time,
                    )
                    subscribed_count_packet = UnsPacket.data(value=self._subscribed_message_count, time=time)
                    subscribed_bytes_packet = UnsPacket.data(
                        value=round(self._subscribed_message_bytes / 1024),
                        uom="kB",
                        time=time,
                    )
                    await self.publish_raw(published_count_topic, UnsPacket.to_json(published_count_packet), qos=0, retain=False)
                    await self.publish_raw(published_bytes_topic, UnsPacket.to_json(published_bytes_packet), qos=0, retain=False)
                    await self.publish_raw(subscribed_count_topic, UnsPacket.to_json(subscribed_count_packet), qos=0, retain=False)
                    await self.publish_raw(subscribed_bytes_topic, UnsPacket.to_json(subscribed_bytes_packet), qos=0, retain=False)
                    self._published_message_count = 0
                    self._published_message_bytes = 0
                    self._subscribed_message_count = 0
                    self._subscribed_message_bytes = 0
                except aiomqtt.MqttError:
                    self._connected.clear()
                except Exception:
                    self._connected.clear()
                await asyncio.sleep(self.stats_interval)
        except asyncio.CancelledError:
            pass
