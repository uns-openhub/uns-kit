from __future__ import annotations

import asyncio
import pytest

from uns_kit.core.uns_mqtt_proxy import UnsMqttProxy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persistence_metadata", "expected"),
    [
        ({}, True),
        ({"attributeNeedsPersistence": None}, True),
        ({"attributeNeedsPersistence": True}, True),
        ({"attributeNeedsPersistence": False}, False),
    ],
)
async def test_publish_mqtt_message_defaults_persistence_to_true(
    persistence_metadata: dict[str, bool | None],
    expected: bool,
) -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
    )

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        return None

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    await proxy.publish_mqtt_message(
        {
            "topic": "test/site",
            "attributes": {
                "attribute": "temperature",
                "data": {"value": 21.5},
                **persistence_metadata,
            },
        }
    )

    produced_topic = next(iter(proxy._produced_topics.values()))
    assert produced_topic["attributeNeedsPersistence"] is expected
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_publish_mqtt_message_carries_and_rotates_asset_identity_proof() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
    )

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        return None

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]
    request = {
        "topic": "enterprise/site-a/line-4",
        "asset": "PACKER-07",
        "assetStableEntityId": "11111111-1111-4111-8111-111111111111",
        "assetDisplayName": "Packer 07",
        "assetIdentityProof": "proof-1",
        "attributes": {"attribute": "state", "data": {"value": "READY"}},
    }

    await proxy.publish_mqtt_message(request)
    request["assetIdentityProof"] = "proof-2"
    await proxy.publish_mqtt_message(request)

    produced_topic = next(iter(proxy._produced_topics.values()))
    assert produced_topic["assetStableEntityId"] == "11111111-1111-4111-8111-111111111111"
    assert produced_topic["assetDisplayName"] == "Packer 07"
    assert produced_topic["assetIdentityProof"] == "proof-2"
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_publish_mqtt_message_rejects_partial_asset_identity_metadata() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
    )
    with pytest.raises(ValueError, match="requires assetStableEntityId and assetIdentityProof"):
        await proxy.publish_mqtt_message(
            {
                "topic": "enterprise/site-a/line-4",
                "asset": "PACKER-07",
                "assetDisplayName": "Packer 07",
                "attributes": {"attribute": "state", "data": {"value": "READY"}},
            }
        )
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_publish_message_uses_bounded_concurrency() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=3,
        max_pending_publishes=100,
    )

    current = 0
    max_observed = 0

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        nonlocal current, max_observed
        current += 1
        max_observed = max(max_observed, current)
        await asyncio.sleep(0.02)
        current -= 1

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    for index in range(8):
        await proxy.publish_message(f"test/{index}", f"payload-{index}")

    await proxy._stop_publish_workers()

    assert max_observed > 1
    assert max_observed <= 3


@pytest.mark.asyncio
async def test_publish_message_allows_unbounded_queue_configuration() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=2,
        max_pending_publishes=0,
    )

    published: list[str] = []

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        await asyncio.sleep(0.01)
        published.append(topic)

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    for index in range(20):
        await proxy.publish_message(f"test/{index}", f"payload-{index}")

    await proxy._stop_publish_workers()

    assert len(published) == 20


@pytest.mark.asyncio
async def test_publish_message_rejects_when_bounded_queue_is_full() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=1,
        max_pending_publishes=2,
    )

    release_publish = asyncio.Event()

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        await release_publish.wait()

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    await proxy.publish_message("test/0", "payload-0")
    await proxy.publish_message("test/1", "payload-1")

    with pytest.raises(RuntimeError, match="Publisher queue is full"):
        await proxy.publish_message("test/2", "payload-2")

    release_publish.set()
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_flush_waits_for_delayed_async_publish_completions() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=2,
        max_pending_publishes=10,
    )

    release_publish = asyncio.Event()
    published: list[str] = []

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        await release_publish.wait()
        published.append(topic)

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    await proxy.publish_message("test/0", "payload-0")
    await proxy.publish_message("test/1", "payload-1")

    flush_task = asyncio.create_task(proxy.flush(timeout=1.0))
    await asyncio.sleep(0)
    assert not flush_task.done()

    release_publish.set()
    await flush_task

    assert sorted(published) == ["test/0", "test/1"]
    assert proxy._pending_publish_completions == 0
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_close_waits_for_enqueued_publishes_by_default() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=2,
        max_pending_publishes=100,
    )

    published: list[str] = []

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        await asyncio.sleep(0.01)
        published.append(topic)

    async def fake_client_close() -> None:
        return None

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]
    proxy.client.close = fake_client_close  # type: ignore[method-assign]

    for index in range(5):
        await proxy.publish_message(f"test/{index}", f"payload-{index}")

    await proxy.close()

    assert len(published) == 5


@pytest.mark.asyncio
async def test_publish_failure_emits_error_event_and_allows_flush_to_finish() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=1,
        max_pending_publishes=10,
    )

    failures: list[dict[str, object]] = []

    async def on_error(payload: dict[str, object]) -> None:
        failures.append(payload)

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        raise RuntimeError("broker rejected publish")

    proxy.event.on("error", on_error)
    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    await proxy.publish_message("test/failure", "payload")
    await proxy.flush(timeout=1.0)

    assert proxy._pending_publish_completions == 0
    assert len(failures) == 1
    assert failures[0]["topic"] == "test/failure"
    assert isinstance(failures[0]["error"], RuntimeError)
    await proxy._stop_publish_workers()


@pytest.mark.asyncio
async def test_flush_does_not_hang_if_publish_worker_exits() -> None:
    proxy = UnsMqttProxy(
        "localhost",
        process_name="test-process",
        instance_name="test-instance",
        publish_concurrency=1,
        max_pending_publishes=10,
    )

    entered_publish = asyncio.Event()

    async def fake_publish_raw(topic: str, payload: str | bytes, *, qos: int = 0, retain: bool = False) -> None:
        entered_publish.set()
        await asyncio.sleep(10)

    proxy.client.publish_raw = fake_publish_raw  # type: ignore[method-assign]

    await proxy.publish_message("test/0", "payload-0")
    await proxy.publish_message("test/1", "payload-1")
    await entered_publish.wait()

    worker = proxy._publish_workers[0]
    worker.cancel()

    with pytest.raises(RuntimeError, match="publish worker exited unexpectedly"):
        await proxy.flush(timeout=1.0)

    await proxy._stop_publish_workers(drain=False)
