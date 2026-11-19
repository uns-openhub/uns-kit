from __future__ import annotations

import asyncio
import inspect
import json
import math
import uuid
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Protocol, TypeVar, cast

from ..api.proxy import ApiProxyOptions, UnsApiProxy
from ..cron.proxy import CronProxyOptions, CronScheduleInput, UnsCronProxy
from ..version import __version__
from .client import UnsMqttClient
from .lifecycle import ProcessLifecycleHooks, ProcessShutdownError
from .runtime_metadata import RUNTIME_METADATA
from .service_metadata import UnsServiceMetadataInput, build_uns_service_metadata
from .status_monitor import StatusMonitor
from .topic_builder import TopicBuilder
from .uns_mqtt_proxy import UnsMqttProxy
from .durable_input import JournalInput
from .durable_journal import DurableInputJournal


def _pick(mapping: Mapping[str, Any], snake: str, camel: str) -> Any:
    if snake in mapping:
        return mapping.get(snake)
    return mapping.get(camel)


def _resolve_tls(mqtt_ssl: Optional[bool]) -> bool:
    return bool(mqtt_ssl)


def _resolve_port(port: Optional[int]) -> int:
    return int(port) if port is not None else 1883


def _opt_in(value: Any) -> bool:
    if value is None:
        return False
    if type(value) is not bool:
        raise ValueError("Managed runtime options require an explicit boolean")
    return value


def _backpressure(value: Any) -> Any:
    if isinstance(value, Mapping):
        if set(value) != {"maxWaiting"}:
            raise ValueError("publishBackpressure requires maxWaiting")
        return value["maxWaiting"]
    return value


class _Stoppable(Protocol):
    async def stop(self) -> None: ...


_StoppableT = TypeVar("_StoppableT", bound=_Stoppable)


async def _stop_owned(proxies: list[_StoppableT], errors: list[BaseException]) -> None:
    for proxy in list(proxies):
        try:
            await proxy.stop()
            proxies.remove(proxy)
        except Exception as error:
            errors.append(error)


@dataclass
class UnsParameters:
    username: Optional[str] = None
    password: Optional[str] = None
    mqtt_ssl: Optional[bool] = None
    client_id: Optional[str] = None
    port: Optional[int] = None
    keepalive: Optional[int] = None
    clean: Optional[bool] = None
    reconnect_period: Optional[int] = None
    publish_concurrency: Optional[int] = None
    max_pending_publishes: Optional[int] = None
    managed_input: bool = False
    mqtt_sub_to_topics: list[str] | None = None
    max_pending_inputs: int = 64
    max_input_bytes: int = 8 * 1024 * 1024
    max_input_message_bytes: int = 1024 * 1024
    publish_backpressure: int | None = None
    publish_qos: int = 0

    @staticmethod
    def from_mapping(mapping: Mapping[str, Any]) -> "UnsParameters":
        return UnsParameters(
            username=mapping.get("username"),
            password=mapping.get("password"),
            mqtt_ssl=_pick(mapping, "mqtt_ssl", "mqttSSL"),
            client_id=_pick(mapping, "client_id", "clientId"),
            port=mapping.get("port"),
            keepalive=mapping.get("keepalive"),
            clean=mapping.get("clean"),
            reconnect_period=_pick(mapping, "reconnect_period", "reconnectPeriod"),
            publish_concurrency=_pick(mapping, "publish_concurrency", "publishConcurrency"),
            max_pending_publishes=_pick(mapping, "max_pending_publishes", "maxPendingPublishes"),
            managed_input=_opt_in(_pick(mapping, "managed_input", "managedInput")),
            mqtt_sub_to_topics=_pick(mapping, "mqtt_sub_to_topics", "mqttSubToTopics"),
            max_pending_inputs=mapping.get("max_pending_inputs", mapping.get("maxPendingInputs", 64)),
            max_input_bytes=mapping.get("max_input_bytes", mapping.get("maxInputBytes", 8 * 1024 * 1024)),
            max_input_message_bytes=mapping.get("max_input_message_bytes", mapping.get("maxInputMessageBytes", 1024 * 1024)),
            publish_backpressure=_backpressure(_pick(mapping, "publish_backpressure", "publishBackpressure")),
            publish_qos=mapping.get("publish_qos", mapping.get("publishQos", 0)),
        )


@dataclass
class UnsProcessParameters:
    process_name: str
    username: Optional[str] = None
    password: Optional[str] = None
    mqtt_ssl: Optional[bool] = None
    client_id: Optional[str] = None
    port: Optional[int] = None
    keepalive: Optional[int] = None
    clean: Optional[bool] = None
    reconnect_period: Optional[int] = None
    package_name: Optional[str] = None
    package_version: Optional[str] = None
    managed_ownership: bool = False

    @staticmethod
    def from_mapping(mapping: Mapping[str, Any]) -> "UnsProcessParameters":
        process_name = _pick(mapping, "process_name", "processName")
        if not process_name:
            raise ValueError("UnsProxyProcess requires process_name/processName.")
        return UnsProcessParameters(
            process_name=str(process_name),
            username=mapping.get("username"),
            password=mapping.get("password"),
            mqtt_ssl=_pick(mapping, "mqtt_ssl", "mqttSSL"),
            client_id=_pick(mapping, "client_id", "clientId"),
            port=mapping.get("port"),
            keepalive=mapping.get("keepalive"),
            clean=mapping.get("clean"),
            reconnect_period=_pick(mapping, "reconnect_period", "reconnectPeriod"),
            package_name=_pick(mapping, "package_name", "packageName"),
            package_version=_pick(mapping, "package_version", "packageVersion"),
            managed_ownership=_opt_in(_pick(mapping, "managed_ownership", "managedOwnership")),
        )


class UnsProxyProcess:
    """
    Minimal Python equivalent of the TS UnsProxyProcess.
    Manages process-level status publishing and creates instance proxies.
    """

    def __init__(
        self,
        host: str,
        process_parameters: UnsProcessParameters | Mapping[str, Any],
        process_name: Optional[str] = None,
        activate_delay_s: float = 10.0,
    ) -> None:
        if isinstance(process_parameters, UnsProcessParameters):
            self.process_parameters = process_parameters
            if process_name:
                self.process_parameters.process_name = process_name
        else:
            self.process_parameters = UnsProcessParameters.from_mapping(process_parameters)
            if process_name:
                self.process_parameters.process_name = process_name

        self.process_name = self.process_parameters.process_name
        self.process_parameters.managed_ownership = _opt_in(self.process_parameters.managed_ownership)
        self.process_parameters.package_name = (
            self.process_parameters.package_name
            or RUNTIME_METADATA.package_name
            or "uns-kit"
        )
        self.process_parameters.package_version = (
            self.process_parameters.package_version
            or RUNTIME_METADATA.package_version
            or __version__
        )
        self.process_id = uuid.uuid4().hex
        self.active = False
        self._activate_delay_s = activate_delay_s
        self.topic_builder = TopicBuilder(
            self.process_parameters.package_name,
            self.process_parameters.package_version,
            self.process_name,
        )
        if self.process_parameters.client_id:
            process_client_id = f"{self.process_parameters.client_id}-{self.process_id}"
        else:
            process_client_id = f"{self.process_name}-{self.process_id}"
        reconnect_interval_s = (
            (self.process_parameters.reconnect_period / 1000)
            if self.process_parameters.reconnect_period is not None
            else 2.0
        )
        self._client = UnsMqttClient(
            host,
            port=_resolve_port(self.process_parameters.port),
            username=self.process_parameters.username or None,
            password=self.process_parameters.password or None,
            tls=_resolve_tls(self.process_parameters.mqtt_ssl),
            client_id=process_client_id,
            keepalive=self.process_parameters.keepalive or 60,
            clean_session=True if self.process_parameters.clean is None else self.process_parameters.clean,
            topic_builder=self.topic_builder,
            enable_status=False,
            reconnect_interval=reconnect_interval_s,
        )
        self._status_monitor = StatusMonitor(self._client, self.topic_builder, lambda: self.active)
        self._proxies: List[UnsMqttProxy] = []
        self._api_proxies: List[UnsApiProxy] = []
        self._cron_proxies: List[UnsCronProxy] = []
        self._activate_task: Optional[asyncio.Task] = None
        self._stop_requested = False
        self._stop_task: Optional[asyncio.Task[None]] = None
        self._closed_resources: set[str] = set()
        self._lifecycle_participants: list[ProcessLifecycleHooks] = []
        self._released_participants: set[int] = set()
        self._application_drain_task: Optional[asyncio.Task[None]] = None
        self._journal_inputs: list[JournalInput] = []
        self._journal_handover: Any = None
        self._handover_released = False
        self._handover_passive = False
        self._handover_action_task: asyncio.Task | None = None

    def get_process_name(self) -> str:
        return self.process_name

    async def start(self) -> None:
        self._ensure_not_stopping()
        await self._client.connect()
        await self._status_monitor.start()
        if not self.process_parameters.managed_ownership and (self._activate_task is None or self._activate_task.done()):
            self._activate_task = asyncio.create_task(self._activate_after_delay())

    async def stop(self, *, drain: bool = True, timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S) -> None:
        if timeout is not None and (isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Shutdown timeout must be a positive finite number of seconds or None")
        if self._stop_task is None:
            self._stop_requested = True
            self.active = False
            self._begin_owned_release()
            self._stop_task = asyncio.create_task(self._perform_stop(drain=drain, timeout=timeout))
            self._stop_task.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        task = self._stop_task
        try:
            # A disconnected caller must not cancel application IO or cleanup.
            await asyncio.shield(task)
        except Exception:
            if self._stop_task is task:
                self._stop_task = None
            raise

    async def shutdown(self, *, drain: bool = True, timeout: Optional[float] = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_S) -> None:
        await self.stop(drain=drain, timeout=timeout)

    def is_active(self) -> bool:
        return self.active and not self._stop_requested and not self._handover_passive

    def isActive(self) -> bool:
        return self.is_active()

    def register_lifecycle_participant(self, hooks: ProcessLifecycleHooks) -> None:
        self._ensure_not_stopping()
        if not isinstance(hooks, ProcessLifecycleHooks):
            raise TypeError("Expected ProcessLifecycleHooks")
        self._lifecycle_participants.append(hooks)

    async def publish_service_metadata(self, metadata: UnsServiceMetadataInput) -> dict[str, Any]:
        self._ensure_not_stopping()
        resolved = build_uns_service_metadata(
            metadata,
            process_name=self.process_name,
            process_id=self.process_id,
            package_name=self.process_parameters.package_name,
            package_version=self.process_parameters.package_version,
        )
        payload = json.dumps(resolved, separators=(",", ":"), allow_nan=False)
        await self._client.publish_raw(self.topic_builder.service_metadata_topic(), payload, retain=True)
        return resolved

    async def publishServiceMetadata(self, metadata: UnsServiceMetadataInput) -> dict[str, Any]:
        return await self.publish_service_metadata(metadata)

    def _ensure_not_stopping(self) -> None:
        if self._stop_requested or self._handover_released:
            raise RuntimeError("UNS process is stopping or stopped")

    def _begin_owned_release(self) -> None:
        for api_proxy in self._api_proxies:
            begin = getattr(api_proxy, "begin_release", None)
            if begin is not None:
                begin()
        for proxy in self._proxies:
            begin = getattr(proxy, "begin_input_release", None)
            if begin is not None:
                begin()
        for source in self._journal_inputs:
            source.begin_release()

    async def _drain_application(self) -> None:
        async def drain_participant(hooks: ProcessLifecycleHooks) -> None:
            await hooks.drain()

        results = await asyncio.gather(
            *(drain_participant(hooks) for hooks in self._lifecycle_participants),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise ProcessShutdownError(errors)

    async def _perform_stop(self, *, drain: bool, timeout: Optional[float]) -> None:
        if self._handover_action_task is not None and not self._handover_action_task.done():
            await asyncio.wait_for(asyncio.shield(asyncio.gather(self._handover_action_task, return_exceptions=True)), timeout)
        if self._activate_task and not self._activate_task.done():
            self._activate_task.cancel()
            try:
                await self._activate_task
            except asyncio.CancelledError:
                pass
        await self._drain_owned_work(timeout)
        errors: list[BaseException] = []
        await _stop_owned(self._journal_inputs, errors)
        # Attempt every independent cleanup, preserving failures for the caller.
        await _stop_owned(self._api_proxies, errors)
        await _stop_owned(self._cron_proxies, errors)
        for proxy in list(self._proxies):
            try:
                await proxy.close(drain=drain, timeout=timeout)
                self._proxies.remove(proxy)
            except Exception as error:
                errors.append(error)
        for resource, method in ((self._status_monitor, "stop"), (self._client, "close")):
            if method in self._closed_resources:
                continue
            try:
                await getattr(resource, method)()
                self._closed_resources.add(method)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ProcessShutdownError(errors)
        self._proxies.clear()
        self._api_proxies.clear()
        self._cron_proxies.clear()

    async def _drain_owned_work(self, timeout: Optional[float]) -> None:
        errors: list[BaseException] = []
        for index, hooks in enumerate(self._lifecycle_participants):
            if index in self._released_participants:
                continue
            try:
                if hooks.on_release is not None:
                    result = hooks.on_release()
                    if inspect.isawaitable(result):
                        if inspect.iscoroutine(result):
                            result.close()
                        raise TypeError("Lifecycle on_release must be synchronous")
                self._released_participants.add(index)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ProcessShutdownError(errors)
        drains = []
        for proxy in self._proxies:
            method = getattr(proxy, "drain_input_for_stop", None)
            if method is not None:
                drains.append(method(timeout=timeout))
        for api_proxy in self._api_proxies:
            method = getattr(api_proxy, "drain_requests", None)
            if method is not None:
                drains.append(method(timeout=timeout))
        for source in self._journal_inputs:
            drains.append(source.drain(timeout=timeout))
        if drains:
            results = await asyncio.gather(*drains, return_exceptions=True)
            errors.extend(result for result in results if isinstance(result, BaseException))
            if errors:
                raise ProcessShutdownError(errors)
        if self._lifecycle_participants:
            if self._application_drain_task is None:
                self._application_drain_task = asyncio.create_task(self._drain_application())
                # Retrieve a late failure even when a timed-out caller has left.
                self._application_drain_task.add_done_callback(
                    lambda task: None if task.cancelled() else task.exception()
                )
            await asyncio.wait_for(asyncio.shield(self._application_drain_task), timeout=timeout)

    def set_active(self, active: bool) -> None:
        self._ensure_not_stopping()
        if active and self._journal_handover is not None:
            raise RuntimeError("Use JournalHandover.start/activate for fenced ownership")
        self.active = active
        if not active and self.process_parameters.managed_ownership:
            for proxy in self._proxies:
                proxy.begin_input_release()
                task = asyncio.create_task(proxy.drain_input_for_stop())
                task.add_done_callback(lambda result: None if result.cancelled() else result.exception())
            for source in self._journal_inputs:
                source.begin_release()

    def create_journal_input(self, journal: DurableInputJournal, handler: Any, *, owner_id: str) -> JournalInput:
        self._ensure_not_stopping()
        if self._journal_inputs or self._journal_handover is not None:
            raise RuntimeError("This runtime supports one durable journal source")
        if any(proxy._publish_qos != 1 or not proxy._strict_publish_drain for proxy in self._proxies):
            raise ValueError("Declare strict QoS 1 outputs before creating a journal input")

        async def flush() -> None:
            for proxy in self._proxies:
                if proxy._publish_qos != 1 or not proxy._strict_publish_drain:
                    raise RuntimeError("Journal outputs require strict QoS 1 publish drain")
                await proxy.flush()

        source = JournalInput(journal, handler, owner_id=owner_id, active=self.is_active, flush=flush)
        self._journal_inputs.append(source)
        return source

    async def _activate_after_delay(self) -> None:
        await asyncio.sleep(self._activate_delay_s)
        if not self.active:
            self.active = True

    async def create_mqtt_proxy(
        self,
        instance_name: str,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls: Optional[bool] = None,
        client_id: Optional[str] = None,
    ) -> UnsMqttProxy:
        self._ensure_not_stopping()
        params = UnsParameters(
            username=username,
            password=password,
            mqtt_ssl=tls,
            client_id=client_id,
            port=port,
        )
        return await self.create_uns_mqtt_proxy(
            host or self._client.host,
            instance_name,
            "wait",
            True,
            params,
        )

    async def create_uns_mqtt_proxy(
        self,
        mqtt_host: str,
        instance_name: str,
        instance_mode: str = "wait",
        handover: bool = True,
        uns_parameters: Optional[UnsParameters | Mapping[str, Any]] = None,
    ) -> UnsMqttProxy:
        self._ensure_not_stopping()
        if self._journal_handover is not None:
            raise RuntimeError("Declare all proxies before sealing handover contracts")
        # Kept for TS API parity. Python runtime currently does not implement handover manager semantics.
        if instance_mode == "force":
            self.active = True
        _ = handover

        params = (
            UnsParameters.from_mapping(uns_parameters)
            if isinstance(uns_parameters, Mapping)
            else (uns_parameters or UnsParameters())
        )
        if self._journal_inputs and params.publish_qos != 1:
            raise ValueError("Journal-owned output requires publish_qos=1")

        if params.client_id:
            resolved_client_id = params.client_id
        elif self.process_parameters.client_id:
            resolved_client_id = f"{self.process_parameters.client_id}-{instance_name}-{self.process_id}"
        else:
            resolved_client_id = f"{self.process_name}-{instance_name}-{self.process_id}"

        resolved_host = mqtt_host
        if not resolved_host:
            raise ValueError("mqtt_host is required.")

        reconnect_interval_s = (
            (params.reconnect_period / 1000)
            if params.reconnect_period is not None
            else 2.0
        )
        proxy = UnsMqttProxy(
            resolved_host,
            process_name=self.process_name,
            instance_name=instance_name,
            package_name=cast(str, self.process_parameters.package_name),
            package_version=cast(str, self.process_parameters.package_version),
            port=_resolve_port(params.port if params.port is not None else self.process_parameters.port),
            username=params.username if params.username is not None else self.process_parameters.username,
            password=params.password if params.password is not None else self.process_parameters.password,
            tls=_resolve_tls(params.mqtt_ssl if params.mqtt_ssl is not None else self.process_parameters.mqtt_ssl),
            client_id=resolved_client_id,
            keepalive=params.keepalive if params.keepalive is not None else (self.process_parameters.keepalive or 60),
            clean_session=params.clean if params.clean is not None else (
                True if self.process_parameters.clean is None else self.process_parameters.clean
            ),
            reconnect_interval=reconnect_interval_s,
            publish_concurrency=params.publish_concurrency if params.publish_concurrency is not None else 32,
            max_pending_publishes=params.max_pending_publishes if params.max_pending_publishes is not None else 1000,
            publish_backpressure=params.publish_backpressure if params.publish_backpressure is not None else (32 if params.max_pending_publishes is None or params.max_pending_publishes > 0 else None),
            managed_input=params.managed_input,
            mqtt_sub_to_topics=params.mqtt_sub_to_topics,
            max_pending_inputs=params.max_pending_inputs,
            max_input_bytes=params.max_input_bytes,
            max_input_message_bytes=params.max_input_message_bytes,
            input_ownership_gate=lambda: not self._stop_requested and (not self.process_parameters.managed_ownership or self.is_active()),
            strict_publish_drain=True,
            publish_qos=params.publish_qos,
        )
        self._proxies.append(proxy)
        await proxy.connect()
        self._ensure_not_stopping()
        return proxy

    async def createUnsMqttProxy(
        self,
        mqttHost: str,
        instanceName: str,
        instanceMode: str = "wait",
        handover: bool = True,
        unsParameters: Optional[UnsParameters | Mapping[str, Any]] = None,
    ) -> UnsMqttProxy:
        return await self.create_uns_mqtt_proxy(
            mqttHost,
            instanceName,
            instanceMode,
            handover,
            unsParameters,
        )

    async def createMqttProxy(
        self,
        instanceName: str,
        **kwargs: Any,
    ) -> UnsMqttProxy:
        return await self.create_mqtt_proxy(instanceName, **kwargs)

    async def create_api_proxy(
        self,
        instance_name: str,
        options: Optional[ApiProxyOptions | Mapping[str, Any]] = None,
    ) -> UnsApiProxy:
        self._ensure_not_stopping()
        if self._journal_handover is not None:
            raise RuntimeError("Declare all APIs before sealing handover contracts")
        proxy = UnsApiProxy(
            self._client,
            process_name=self.process_name,
            instance_name=instance_name,
            topic_builder=self.topic_builder,
            options=options,
        )
        proxy.set_ownership_gate(lambda: not self._stop_requested and (not self.process_parameters.managed_ownership or self.is_active()))
        self._api_proxies.append(proxy)
        await proxy.start()
        self._ensure_not_stopping()
        return proxy

    async def createApiProxy(
        self,
        instanceName: str,
        options: Optional[ApiProxyOptions | Mapping[str, Any]] = None,
    ) -> UnsApiProxy:
        return await self.create_api_proxy(instanceName, options)

    async def create_cron_proxy(
        self,
        cron_input: CronScheduleInput,
        options: Optional[CronProxyOptions | Mapping[str, Any]] = None,
    ) -> UnsCronProxy:
        self._ensure_not_stopping()
        normalized_options = dict(options) if isinstance(options, Mapping) else options
        if self._journal_handover is not None:
            raise RuntimeError("Declare cron before sealing handover contracts")
        proxy = UnsCronProxy(cron_input, normalized_options, active=lambda: not self._stop_requested and (not self.process_parameters.managed_ownership or self.is_active()))
        await proxy.start()
        self._cron_proxies.append(proxy)
        return proxy

    async def createCrontabProxy(
        self,
        cronInput: CronScheduleInput,
        options: Optional[CronProxyOptions | Mapping[str, Any]] = None,
    ) -> UnsCronProxy:
        return await self.create_cron_proxy(cronInput, options)
