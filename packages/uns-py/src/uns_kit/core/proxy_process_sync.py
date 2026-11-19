from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import queue
import threading
from contextlib import AbstractContextManager
from collections.abc import Coroutine
from typing import Any, Callable, Iterator, Mapping, Optional, TypeVar

from .proxy_process import UnsMqttProxy, UnsParameters, UnsProcessParameters, UnsProxyProcess
from .uns_mqtt_proxy import MessageMode

T = TypeVar("T")


class _LoopThread:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="uns-kit-sync-loop", daemon=True)
        self._closed = False
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def run(self, coroutine: Coroutine[Any, Any, T], timeout: Optional[float] = None) -> T:
        if self._closed:
            raise RuntimeError("Sync UNS loop is closed.")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Timed out waiting for async UNS operation.") from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        future = asyncio.run_coroutine_threadsafe(self._shutdown_pending_tasks(), self._loop)
        with contextlib.suppress(Exception):
            future.result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()

    async def _shutdown_pending_tasks(self) -> None:
        current = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks(self._loop) if task is not current and not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class _SyncSubscriptionIterator(Iterator[Any]):
    _SENTINEL = object()

    def __init__(self, client: Any, loop_thread: _LoopThread, topics: str | list[str], *, resilient: bool) -> None:
        self._client = client
        self._loop_thread = loop_thread
        self._topics = topics
        self._resilient = resilient
        self._queue: queue.Queue[Any] = queue.Queue()
        self._closed = False
        self._task = self._loop_thread.run(self._create_task())

    async def _create_task(self) -> asyncio.Task[None]:
        return asyncio.create_task(self._pump_messages())

    async def _pump_messages(self) -> None:
        try:
            async def emit_message(message: Any) -> None:
                self._queue.put(message)

            if self._resilient:
                async for message in self._client.resilient_messages(self._topics):  # type: ignore[attr-defined]
                    await emit_message(message)
            else:
                async with self._client.messages(self._topics) as messages:  # type: ignore[attr-defined]
                    async for message in messages:
                        await emit_message(message)
        except asyncio.CancelledError:
            pass
        finally:
            self._queue.put(self._SENTINEL)

    def __next__(self) -> Any:
        item = self._queue.get()
        if item is self._SENTINEL:
            raise StopIteration
        return item

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._task.cancel()
        try:
            self._loop_thread.run(self._await_task())
        except Exception:
            pass

    async def _await_task(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


class SyncMessagesContext(AbstractContextManager["_SyncSubscriptionIterator"]):
    def __init__(self, client: Any, loop_thread: _LoopThread, topics: str | list[str], *, resilient: bool = False) -> None:
        self._iterator = _SyncSubscriptionIterator(client, loop_thread, topics, resilient=resilient)

    def __enter__(self) -> _SyncSubscriptionIterator:
        return self._iterator

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._iterator.close()


class SyncSubscription:
    def __init__(
        self,
        iterator: _SyncSubscriptionIterator,
        on_message: Callable[[Any], Any],
    ) -> None:
        self._iterator = iterator
        self._on_message = on_message
        self._closed = False
        self._thread = threading.Thread(target=self._run, name="uns-kit-sync-subscription", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            for message in self._iterator:
                self._on_message(message)
        except Exception:
            return

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._iterator.close()
        self._thread.join(timeout=5)

    def unsubscribe(self) -> None:
        self.close()


class UnsMqttProxySync:
    def __init__(self, proxy: UnsMqttProxy, loop_thread: _LoopThread) -> None:
        self._proxy = proxy
        self._loop_thread = loop_thread
        self.client = proxy.client
        self.topic_builder = proxy.topic_builder
        self.instance_status_topic = proxy.instance_status_topic

    def publish_message(self, topic: str, payload: str | bytes, *, timeout: Optional[float] = None) -> None:
        self._loop_thread.run(self._proxy.publish_message(topic, payload), timeout=timeout)

    def publish_packet(self, topic: str, packet: dict[str, Any], *, timeout: Optional[float] = None) -> None:
        self._loop_thread.run(self._proxy.publish_packet(topic, packet), timeout=timeout)

    def publish_mqtt_message(
        self,
        mqtt_message: dict[str, Any],
        mode: MessageMode = MessageMode.RAW,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        self._loop_thread.run(self._proxy.publish_mqtt_message(mqtt_message, mode), timeout=timeout)

    def drain_publishes(self, *, timeout: Optional[float] = None) -> None:
        self._loop_thread.run(self._proxy.drain_publishes(timeout=timeout), timeout=timeout)

    def flush(self, *, timeout: Optional[float] = None) -> None:
        self._loop_thread.run(self._proxy.flush(timeout=timeout), timeout=timeout)

    def stop(
        self,
        *,
        drain: bool = True,
        timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S,
    ) -> None:
        self._loop_thread.run(self._proxy.stop(drain=drain, timeout=timeout), timeout=timeout)

    def close(
        self,
        *,
        drain: bool = True,
        timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S,
    ) -> None:
        self._loop_thread.run(self._proxy.close(drain=drain, timeout=timeout), timeout=timeout)

    def messages(self, topics: str | list[str]) -> SyncMessagesContext:
        return SyncMessagesContext(self.client, self._loop_thread, topics, resilient=False)

    def resilient_messages(self, topics: str | list[str]) -> Iterator[Any]:
        return _SyncSubscriptionIterator(self.client, self._loop_thread, topics, resilient=True)

    def subscribe(
        self,
        topics: str | list[str],
        *,
        on_message: Callable[[Any], Any],
        resilient: bool = True,
    ) -> SyncSubscription:
        iterator = _SyncSubscriptionIterator(self.client, self._loop_thread, topics, resilient=resilient)
        return SyncSubscription(iterator, on_message)


class UnsProxyProcessSync:
    """
    Blocking facade for sync-only integrations.
    Runs the existing async UNS runtime on a private background event loop.
    """

    def __init__(
        self,
        host: str,
        process_parameters: UnsProcessParameters | Mapping[str, Any],
        process_name: Optional[str] = None,
        activate_delay_s: float = 10.0,
    ) -> None:
        self._loop_thread = _LoopThread()

        async def create_process() -> UnsProxyProcess:
            return UnsProxyProcess(
                host,
                process_parameters,
                process_name=process_name,
                activate_delay_s=activate_delay_s,
            )

        self._process = self._loop_thread.run(create_process())

    def get_process_name(self) -> str:
        return self._process.get_process_name()

    def start(self, *, timeout: Optional[float] = None) -> None:
        self._loop_thread.run(self._process.start(), timeout=timeout)

    def stop(
        self,
        *,
        drain: bool = True,
        timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S,
    ) -> None:
        try:
            self._loop_thread.run(self._process.stop(drain=drain, timeout=timeout), timeout=timeout)
        finally:
            self._loop_thread.close()

    def close(
        self,
        *,
        drain: bool = True,
        timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S,
    ) -> None:
        self.stop(drain=drain, timeout=timeout)

    def set_active(self, active: bool) -> None:
        async def update() -> None:
            self._process.set_active(active)
        self._loop_thread.run(update())

    def create_mqtt_proxy(
        self,
        instance_name: str,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls: Optional[bool] = None,
        client_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> UnsMqttProxySync:
        proxy = self._loop_thread.run(
            self._process.create_mqtt_proxy(
                instance_name,
                host=host,
                port=port,
                username=username,
                password=password,
                tls=tls,
                client_id=client_id,
            ),
            timeout=timeout,
        )
        return UnsMqttProxySync(proxy, self._loop_thread)

    def create_mqtt_proxy_sync(self, instance_name: str, **kwargs: Any) -> UnsMqttProxySync:
        return self.create_mqtt_proxy(instance_name, **kwargs)

    def create_uns_mqtt_proxy(
        self,
        mqtt_host: str,
        instance_name: str,
        instance_mode: str = "wait",
        handover: bool = True,
        uns_parameters: Optional[UnsParameters | Mapping[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> UnsMqttProxySync:
        parameters = UnsParameters.from_mapping(uns_parameters) if isinstance(uns_parameters, Mapping) else uns_parameters
        if parameters is not None and parameters.managed_input:
            raise ValueError("Managed MQTT input requires the asynchronous UnsProxyProcess API")
        proxy = self._loop_thread.run(
            self._process.create_uns_mqtt_proxy(
                mqtt_host,
                instance_name,
                instance_mode=instance_mode,
                handover=handover,
                uns_parameters=uns_parameters,
            ),
            timeout=timeout,
        )
        return UnsMqttProxySync(proxy, self._loop_thread)

    def create_uns_mqtt_proxy_sync(
        self,
        mqtt_host: str,
        instance_name: str,
        instance_mode: str = "wait",
        handover: bool = True,
        uns_parameters: Optional[UnsParameters | Mapping[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> UnsMqttProxySync:
        return self.create_uns_mqtt_proxy(
            mqtt_host,
            instance_name,
            instance_mode=instance_mode,
            handover=handover,
            uns_parameters=uns_parameters,
            timeout=timeout,
        )

    def createMqttProxy(self, instanceName: str, **kwargs: Any) -> UnsMqttProxySync:
        return self.create_mqtt_proxy(instanceName, **kwargs)

    def createMqttProxySync(self, instanceName: str, **kwargs: Any) -> UnsMqttProxySync:
        return self.create_mqtt_proxy(instanceName, **kwargs)

    def createUnsMqttProxy(
        self,
        mqttHost: str,
        instanceName: str,
        instanceMode: str = "wait",
        handover: bool = True,
        unsParameters: Optional[UnsParameters | Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> UnsMqttProxySync:
        return self.create_uns_mqtt_proxy(
            mqttHost,
            instanceName,
            instance_mode=instanceMode,
            handover=handover,
            uns_parameters=unsParameters,
            **kwargs,
        )

    def createUnsMqttProxySync(
        self,
        mqttHost: str,
        instanceName: str,
        instanceMode: str = "wait",
        handover: bool = True,
        unsParameters: Optional[UnsParameters | Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> UnsMqttProxySync:
        return self.create_uns_mqtt_proxy(
            mqttHost,
            instanceName,
            instance_mode=instanceMode,
            handover=handover,
            uns_parameters=unsParameters,
            **kwargs,
        )

    def __enter__(self) -> "UnsProxyProcessSync":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()
