from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from collections.abc import Awaitable
from typing import Any, Dict, Optional

from .client import UnsMqttClient
from .logger import get_logger
from .packet import UnsPacket, isoformat
from .proxy import UnsProxy
from .topic_builder import TopicBuilder

logger = get_logger(__name__)


class MessageMode(str, Enum):
    RAW = "raw"
    # Deprecated: producer-side delta calculation loses state across restarts.
    DELTA = "delta"
    # Deprecated: producer-side delta calculation loses state across restarts.
    BOTH = "both"


@dataclass
class LastValueEntry:
    value: Any
    uom: Optional[str]
    timestamp: datetime


@dataclass
class QueuedPublish:
    topic: str
    payload: str | bytes


class UnsMqttProxy(UnsProxy):
    DEFAULT_DRAIN_TIMEOUT_S = 30.0

    def __init__(
        self,
        host: str,
        *,
        process_name: str,
        instance_name: str,
        package_name: str = "uns-kit",
        package_version: str = "0.0.1",
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls: bool = False,
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        reconnect_interval: float = 2.0,
        max_reconnect_interval: float = 30.0,
        publish_concurrency: int = 32,
        max_pending_publishes: Optional[int] = None,
    ) -> None:
        self.topic_builder = TopicBuilder(package_name, package_version, process_name)
        self.instance_status_topic = self.topic_builder.instance_status_topic(instance_name)
        self.client = UnsMqttClient(
            host,
            port=port,
            username=username,
            password=password,
            tls=tls,
            client_id=client_id,
            keepalive=keepalive,
            clean_session=clean_session,
            reconnect_interval=reconnect_interval,
            max_reconnect_interval=max_reconnect_interval,
            topic_builder=self.topic_builder,
            instance_name=instance_name,
            publisher_active=True,
            subscriber_active=True,
        )
        super().__init__(self.client, self.instance_status_topic, instance_name)
        self._last_values: Dict[str, LastValueEntry] = {}
        self._sequence_ids: Dict[str, int] = {}
        self._delta_mode_deprecation_warned = False
        self._publish_concurrency = max(1, publish_concurrency)
        self._max_pending_publishes = (
            None if max_pending_publishes is None or max_pending_publishes <= 0 else max_pending_publishes
        )
        if self._max_pending_publishes is None:
            self._publish_queue: asyncio.Queue[QueuedPublish | None] = asyncio.Queue()
        else:
            self._publish_queue = asyncio.Queue(maxsize=self._max_pending_publishes)
        self._publish_workers: list[asyncio.Task[None]] = []
        self._publish_workers_started = False
        self._publish_workers_stop_requested = False
        self._pending_enqueue_operations = 0
        self._pending_publish_completions = 0
        self._drain_condition = asyncio.Condition()
        self._publish_worker_failure: Exception | None = None

    async def connect(self) -> None:
        await self.client.connect()
        self._ensure_publish_workers_started()
        await self.start()

    async def stop(self, *, drain: bool = True, timeout: Optional[float] = DEFAULT_DRAIN_TIMEOUT_S) -> None:
        await self._stop_publish_workers(drain=drain, timeout=timeout)
        await super().stop()

    async def close(self, *, drain: bool = True, timeout: Optional[float] = DEFAULT_DRAIN_TIMEOUT_S) -> None:
        await self.stop(drain=drain, timeout=timeout)
        await self.client.close()

    async def publish_message(self, topic: str, payload: str | bytes) -> None:
        await self._track_enqueue_operation(self._enqueue_publish(topic, payload))

    async def publish_packet(self, topic: str, packet: Dict[str, Any]) -> None:
        await self._track_enqueue_operation(self._enqueue_publish(topic, UnsPacket.to_json(packet)))

    async def publish_mqtt_message(self, mqtt_message: Dict[str, Any], mode: MessageMode = MessageMode.RAW) -> None:
        await self._track_enqueue_operation(self._publish_mqtt_message_impl(mqtt_message, mode))

    async def drain_publishes(self, *, timeout: Optional[float] = None) -> None:
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            async with self._drain_condition:
                if self._pending_enqueue_operations == 0 and self._pending_publish_completions == 0:
                    return
                self._raise_if_drain_blocked()
                if deadline is None:
                    await self._drain_condition.wait()
                    continue
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for pending MQTT publishes to drain.")
                await asyncio.wait_for(self._drain_condition.wait(), timeout=remaining)

    async def flush(self, *, timeout: Optional[float] = None) -> None:
        await self.drain_publishes(timeout=timeout)

    async def _publish_mqtt_message_impl(self, mqtt_message: Dict[str, Any], mode: MessageMode = MessageMode.RAW) -> None:
        attrs = mqtt_message.get("attributes")
        if attrs is None:
            raise ValueError("mqtt_message must include attributes")
        if not isinstance(attrs, list):
            attrs = [attrs]

        base_topic = mqtt_message.get("topic", "")
        asset = mqtt_message.get("asset")
        asset_description = mqtt_message.get("assetDescription")
        asset_stable_entity_id = mqtt_message.get("assetStableEntityId")
        asset_display_name = mqtt_message.get("assetDisplayName")
        asset_identity_proof = mqtt_message.get("assetIdentityProof")
        asset_provider_identity = mqtt_message.get("assetProviderIdentity")
        asset_provider_identity_proof = mqtt_message.get("assetProviderIdentityProof")
        has_asset_identity_metadata = any(
            value is not None
            for value in (asset_stable_entity_id, asset_display_name, asset_identity_proof)
        )
        has_asset_provider_identity_metadata = any(
            value is not None
            for value in (asset_provider_identity, asset_provider_identity_proof)
        )
        if has_asset_identity_metadata and has_asset_provider_identity_metadata:
            raise ValueError(
                "Stable Asset identity metadata and provider candidate metadata are mutually exclusive."
            )
        if has_asset_identity_metadata:
            if (
                not isinstance(asset_stable_entity_id, str)
                or not asset_stable_entity_id.strip()
                or not isinstance(asset_identity_proof, str)
                or not asset_identity_proof.strip()
                or (
                    asset_display_name is not None
                    and (not isinstance(asset_display_name, str) or not asset_display_name.strip())
                )
            ):
                raise ValueError(
                    "Asset identity metadata requires assetStableEntityId and assetIdentityProof; "
                    "assetDisplayName must be non-empty when supplied."
                )
        if has_asset_provider_identity_metadata:
            required_provider_fields = ("providerId", "externalSystem", "externalType", "externalId")
            if (
                not isinstance(asset_provider_identity, dict)
                or not isinstance(asset_provider_identity_proof, str)
                or not asset_provider_identity_proof.strip()
                or any(
                    not isinstance(asset_provider_identity.get(field), str)
                    or not asset_provider_identity[field].strip()
                    for field in required_provider_fields
                )
            ):
                raise ValueError(
                    "Provider Asset identity metadata requires providerId, externalSystem, "
                    "externalType, externalId, and assetProviderIdentityProof."
                )
        object_type = mqtt_message.get("objectType")
        object_type_description = mqtt_message.get("objectTypeDescription")
        object_id = mqtt_message.get("objectId")

        for attr in attrs:
            attribute = attr.get("attribute")
            if attribute is None:
                raise ValueError("attribute is required")
            description = attr.get("description")
            tags = attr.get("tags")
            attribute_needs_persistence = attr.get("attributeNeedsPersistence")
            if attribute_needs_persistence is None:
                attribute_needs_persistence = True
            value_type = attr.get("valueType")
            presentation_kind = attr.get("presentationKind")
            default_aggregation = attr.get("defaultAggregation")
            counter_reset_policy = attr.get("counterResetPolicy")
            table_columns = attr.get("tableColumns")
            validity_mode = attr.get("validityMode")
            expected_interval_ms = attr.get("expectedIntervalMs")
            lifecycle_end_value = attr.get("lifecycleEndValue")

            message = attr.get("message")
            if message is None:
                if "data" in attr:
                    message = {
                        "data": attr["data"],
                        **({"createdAt": attr["createdAt"]} if attr.get("createdAt") else {}),
                        **({"expiresAt": attr["expiresAt"]} if attr.get("expiresAt") else {}),
                    }
                elif "table" in attr:
                    message = {
                        "table": attr["table"],
                        **({"createdAt": attr["createdAt"]} if attr.get("createdAt") else {}),
                        **({"expiresAt": attr["expiresAt"]} if attr.get("expiresAt") else {}),
                    }
                else:
                    raise ValueError("Attribute entry must include exactly one of data/table/message")

            packet = UnsPacket.from_message(message)

            msg = {
                "topic": base_topic,
                "asset": asset,
                "assetDescription": asset_description,
                **(
                    {
                        "assetStableEntityId": asset_stable_entity_id.strip().lower(),
                        **(
                            {"assetDisplayName": asset_display_name.strip()}
                            if asset_display_name is not None
                            else {}
                        ),
                        "assetIdentityProof": asset_identity_proof.strip(),
                    }
                    if has_asset_identity_metadata
                    else {}
                ),
                **(
                    {
                        "assetProviderIdentity": {
                            "providerId": asset_provider_identity["providerId"].strip().lower(),
                            "externalSystem": asset_provider_identity["externalSystem"].strip().lower(),
                            "externalType": asset_provider_identity["externalType"].strip().lower(),
                            "externalId": asset_provider_identity["externalId"].strip(),
                        },
                        "assetProviderIdentityProof": asset_provider_identity_proof.strip(),
                    }
                    if has_asset_provider_identity_metadata
                    else {}
                ),
                "objectType": object_type,
                "objectTypeDescription": object_type_description,
                "objectId": object_id,
                "attribute": attribute,
                "description": description or attribute,
                "tags": tags,
                "attributeNeedsPersistence": attribute_needs_persistence,
                "valueType": value_type,
                "presentationKind": presentation_kind,
                "defaultAggregation": default_aggregation,
                "counterResetPolicy": counter_reset_policy,
                "tableColumns": table_columns,
                "packet": packet,
            }

            if validity_mode is not None:
                msg["validityMode"] = validity_mode
            if expected_interval_ms is not None:
                msg["expectedIntervalMs"] = expected_interval_ms
            if lifecycle_end_value is not None:
                msg["lifecycleEndValue"] = lifecycle_end_value

            if mode == MessageMode.RAW:
                await self._process_and_publish(msg, value_is_cumulative=False)
            elif mode == MessageMode.DELTA:
                self._warn_deprecated_delta_mode(mode)
                delta_msg = dict(msg)
                delta_msg["attribute"] = f"{attribute}-delta"
                delta_msg["description"] = f"{msg['description']} (delta)"
                await self._process_and_publish(delta_msg, value_is_cumulative=True)
            elif mode == MessageMode.BOTH:
                self._warn_deprecated_delta_mode(mode)
                await self._process_and_publish(msg, value_is_cumulative=False)
                delta_msg = dict(msg)
                delta_msg["attribute"] = f"{attribute}-delta"
                delta_msg["description"] = f"{msg['description']} (delta)"
                await self._process_and_publish(delta_msg, value_is_cumulative=True)

    def _warn_deprecated_delta_mode(self, mode: MessageMode) -> None:
        if self._delta_mode_deprecation_warned:
            return
        self._delta_mode_deprecation_warned = True
        logger.warning(
            "MessageMode.%s is deprecated: producer-side delta calculation loses previous-value state "
            "across service restarts. Publish raw cumulative counter values and request delta/rate from "
            "Datahub history APIs.",
            mode.value,
        )

    def _resolve_object_identity(self, msg: Dict[str, Any]) -> None:
        topic = msg.get("topic", "")
        provided_type = msg.get("objectType")
        provided_id = msg.get("objectId")
        provided_asset = msg.get("asset")

        parts = [p for p in topic.split("/") if p]
        parsed_type = parts[-2] if len(parts) >= 2 else None
        parsed_id = parts[-1] if len(parts) >= 1 else None
        parsed_asset = parts[-3] if len(parts) >= 3 else None

        msg["objectType"] = provided_type or parsed_type
        msg["objectId"] = provided_id or parsed_id or "main"
        msg["asset"] = provided_asset or parsed_asset

    def _normalize_topic(self, topic: str) -> str:
        return topic if topic.endswith("/") else f"{topic}/"

    def _ensure_publish_workers_started(self) -> None:
        if self._publish_workers_started:
            return
        self._publish_workers_started = True
        self._publish_workers_stop_requested = False
        self._publish_worker_failure = None
        self._publish_workers = [
            asyncio.create_task(self._publish_worker(), name=f"{self._instance_name}-publish-{index}")
            for index in range(self._publish_concurrency)
        ]
        for task in self._publish_workers:
            task.add_done_callback(self._handle_publish_worker_done)

    async def _stop_publish_workers(
        self,
        *,
        drain: bool = True,
        timeout: Optional[float] = DEFAULT_DRAIN_TIMEOUT_S,
    ) -> None:
        if not self._publish_workers_started:
            return
        if drain:
            await self.flush(timeout=timeout)
            self._publish_workers_stop_requested = True
            for _ in self._publish_workers:
                await self._publish_queue.put(None)
        else:
            self._publish_workers_stop_requested = True
            await self._discard_queued_publishes()
            for task in self._publish_workers:
                task.cancel()
        await asyncio.gather(*self._publish_workers, return_exceptions=True)
        self._publish_workers = []
        self._publish_workers_started = False

    async def _publish_worker(self) -> None:
        while True:
            item = await self._publish_queue.get()
            try:
                if item is None:
                    return
                await self.client.publish_raw(item.topic, item.payload)
            except Exception as exc:
                logger.exception("Error publishing message to topic %s", item.topic if item else "<shutdown>")
                await self.event.emit(
                    "error",
                    {
                        "topic": item.topic if item else None,
                        "payload": item.payload if item else None,
                        "error": exc,
                    },
                )
            finally:
                if item is not None:
                    await self._mark_publish_completed()
                self._publish_queue.task_done()

    async def _enqueue_publish(self, topic: str, payload: str | bytes) -> None:
        self._ensure_publish_workers_started()
        try:
            self._publish_queue.put_nowait(QueuedPublish(topic=topic, payload=payload))
        except asyncio.QueueFull as exc:
            queue_limit = self._max_pending_publishes if self._max_pending_publishes is not None else "unbounded"
            raise RuntimeError(f"{self._instance_name} - Publisher queue is full ({queue_limit}).") from exc
        await self._mark_publish_accepted()

    async def _process_and_publish(self, msg: Dict[str, Any], *, value_is_cumulative: bool) -> None:
        self._resolve_object_identity(msg)
        base_topic = self._normalize_topic(msg.get("topic", ""))

        packet = msg["packet"]
        message = packet.get("message", {})
        data = message.get("data")
        table = message.get("table")

        attribute_type = "Data" if data is not None else "Table" if table is not None else None
        data_group = ""
        if isinstance(data, dict):
            data_group = data.get("dataGroup") or ""
        if isinstance(table, dict):
            data_group = table.get("dataGroup") or ""

        await self.register_unique_topic(
            {
                "timestamp": isoformat(datetime.now(timezone.utc)),
                "topic": base_topic,
                "asset": msg.get("asset"),
                "assetDescription": msg.get("assetDescription"),
                **(
                    {
                        "assetStableEntityId": msg["assetStableEntityId"],
                        **(
                            {"assetDisplayName": msg["assetDisplayName"]}
                            if msg.get("assetDisplayName") is not None
                            else {}
                        ),
                        "assetIdentityProof": msg["assetIdentityProof"],
                    }
                    if msg.get("assetStableEntityId") is not None
                    else {}
                ),
                **(
                    {
                        "assetProviderIdentity": msg["assetProviderIdentity"],
                        "assetProviderIdentityProof": msg["assetProviderIdentityProof"],
                    }
                    if msg.get("assetProviderIdentity") is not None
                    else {}
                ),
                "objectType": msg.get("objectType"),
                "objectTypeDescription": msg.get("objectTypeDescription"),
                "objectId": msg.get("objectId"),
                "attribute": msg.get("attribute"),
                "attributeType": attribute_type,
                "description": msg.get("description"),
                "tags": msg.get("tags"),
                "attributeNeedsPersistence": msg.get("attributeNeedsPersistence"),
                "valueType": msg.get("valueType"),
                "presentationKind": msg.get("presentationKind"),
                "defaultAggregation": msg.get("defaultAggregation"),
                "counterResetPolicy": msg.get("counterResetPolicy"),
                "tableColumns": msg.get("tableColumns"),
                "dataGroup": data_group,
                **({"validityMode": msg.get("validityMode")} if msg.get("validityMode") is not None else {}),
                **(
                    {"expectedIntervalMs": msg.get("expectedIntervalMs")}
                    if msg.get("expectedIntervalMs") is not None
                    else {}
                ),
                **({"lifecycleEndValue": msg.get("lifecycleEndValue")} if msg.get("lifecycleEndValue") is not None else {}),
            }
        )

        publish_topic = (
            f"{base_topic}"
            f"{msg.get('asset') + '/' if msg.get('asset') else ''}"
            f"{msg.get('objectType') + '/' if msg.get('objectType') else ''}"
            f"{msg.get('objectId') + '/' if msg.get('objectId') else ''}"
            f"{msg.get('attribute')}"
        )

        seq_id = self._sequence_ids.get(base_topic, 0)
        self._sequence_ids[base_topic] = seq_id + 1
        packet["sequenceId"] = seq_id

        if isinstance(data, dict):
            time_value = data.get("time")
            if not time_value:
                time_value = UnsPacket.data(value=0)["message"]["data"]["time"]
                data["time"] = time_value
            current_time = datetime.fromisoformat(time_value.replace("Z", "+00:00"))
            new_value = data.get("value")
            new_uom = data.get("uom")
            last = self._last_values.get(publish_topic)
            if last:
                interval_ms = int((current_time - last.timestamp).total_seconds() * 1000)
                packet["interval"] = interval_ms
                if value_is_cumulative and isinstance(new_value, (int, float)) and isinstance(last.value, (int, float)):
                    delta = new_value - last.value
                    data["value"] = delta
                    data["time"] = isoformat(current_time)
                self._last_values[publish_topic] = LastValueEntry(new_value, new_uom, current_time)
                await self._enqueue_publish(publish_topic, UnsPacket.to_json(packet))
            else:
                self._last_values[publish_topic] = LastValueEntry(new_value, new_uom, current_time)
                # For delta mode with no previous value, skip to avoid bogus delta; otherwise publish.
                if not value_is_cumulative:
                    await self._enqueue_publish(publish_topic, UnsPacket.to_json(packet))
        elif isinstance(table, dict):
            await self._enqueue_publish(publish_topic, UnsPacket.to_json(packet))
        else:
            raise ValueError("packet.message must include data or table")

    async def _track_enqueue_operation(self, operation: Awaitable[None]) -> None:
        await self._increment_pending_enqueues()
        try:
            await operation
        finally:
            await self._decrement_pending_enqueues()

    async def _increment_pending_enqueues(self) -> None:
        async with self._drain_condition:
            self._pending_enqueue_operations += 1
            self._drain_condition.notify_all()

    async def _decrement_pending_enqueues(self) -> None:
        async with self._drain_condition:
            if self._pending_enqueue_operations > 0:
                self._pending_enqueue_operations -= 1
            self._drain_condition.notify_all()

    async def _mark_publish_accepted(self) -> None:
        async with self._drain_condition:
            self._pending_publish_completions += 1
            self._drain_condition.notify_all()

    async def _mark_publish_completed(self) -> None:
        async with self._drain_condition:
            if self._pending_publish_completions > 0:
                self._pending_publish_completions -= 1
            self._drain_condition.notify_all()

    async def _discard_queued_publishes(self) -> None:
        discarded = 0
        while True:
            try:
                item = self._publish_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is not None:
                discarded += 1
            self._publish_queue.task_done()
        if discarded == 0:
            return
        async with self._drain_condition:
            self._pending_publish_completions = max(0, self._pending_publish_completions - discarded)
            self._drain_condition.notify_all()

    def _handle_publish_worker_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            failure: Exception | None = RuntimeError("MQTT publish worker stopped before pending publishes drained.")
        else:
            failure = task.exception()
        if failure is not None and not self._publish_workers_stop_requested and self._publish_worker_failure is None:
            self._publish_worker_failure = failure
        if not task.cancelled() or self._publish_worker_failure is not None:
            asyncio.create_task(self._notify_drain_waiters())

    async def _notify_drain_waiters(self) -> None:
        async with self._drain_condition:
            self._drain_condition.notify_all()

    def _raise_if_drain_blocked(self) -> None:
        if self._publish_worker_failure is not None:
            raise RuntimeError("MQTT publish drain aborted because a publish worker exited unexpectedly.") from self._publish_worker_failure
