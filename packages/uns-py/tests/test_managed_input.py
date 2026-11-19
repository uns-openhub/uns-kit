from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from uns_kit import ManagedInputError
from uns_kit.core.managed_input import ManagedMqttInput


class Session:
    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []
        self.unsubscribe_ack = asyncio.Event()
        self.unsubscribe_ack.set()
        self.disconnected = asyncio.Event()
        self.messages = self

    async def subscribe(self, topics):
        self.subscribed.append(topics)

    async def unsubscribe(self, topics):
        self.unsubscribed.append(topics)
        await self.unsubscribe_ack.wait()

    async def __anext__(self):
        await self.disconnected.wait()
        raise ConnectionError("synthetic disconnect")


def managed(**kwargs):
    client = SimpleNamespace(_client=Session(), _connected=asyncio.Event(), reconnect_interval=0.01)

    async def connect():
        client._connected.set()

    client._ensure_connected = connect
    owner = ManagedMqttInput(client, **kwargs)
    queue = client._managed_queue_type()
    return owner, client._client, queue


@pytest.mark.asyncio
async def test_unsubscribe_boundary_drains_serial_input_and_reuses_timed_out_drain() -> None:
    owner, session, queue = managed(max_pending=2)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def handler(message):
        calls.append(message.payload)
        entered.set()
        await release.wait()

    owner.set_handler(handler)
    await owner.start("example/frames")
    queue.put_nowait(SimpleNamespace(topic="example/frames", payload=b"one"))
    await entered.wait()
    session.unsubscribe_ack.clear()
    with pytest.raises(TimeoutError):
        await owner.drain(0.01)
    queue.put_nowait(SimpleNamespace(topic="example/frames", payload=b"two"))
    assert owner.pending == 2 and calls == [b"one"]
    session.unsubscribe_ack.set()
    await asyncio.sleep(0)
    assert not owner._drain_task.done()
    release.set()
    await owner.drain()
    await owner.drain()
    assert calls == [b"one", b"two"] and owner.pending == 0
    assert session.unsubscribed == [["example/frames"]]
    assert not owner.ready


@pytest.mark.asyncio
async def test_failed_handler_retains_original_input_and_never_replays() -> None:
    owner, _, queue = managed()
    original = SimpleNamespace(topic="warehouse/pressure", payload=b"original")
    calls = []

    async def handler(message):
        calls.append(message)
        message.payload = b"application-mutated-copy"
        raise ValueError("synthetic partial effect")

    owner.set_handler(handler)
    await owner.start("warehouse/pressure")
    queue.put_nowait(original)
    await asyncio.sleep(0)
    for _ in range(2):
        with pytest.raises(ManagedInputError, match="unconfirmed"):
            await owner.drain()
    assert len(calls) == 1 and owner.failed_message is original and owner.pending == 1
    assert owner.failed_message.payload == b"original"


@pytest.mark.asyncio
async def test_overflow_is_bounded_and_cannot_report_clean_stop() -> None:
    owner, _, queue = managed(max_pending=1)
    release = asyncio.Event()
    owner.set_handler(lambda message: release.wait())
    await owner.start("example/frames")
    queue.put_nowait(SimpleNamespace(topic="example/frames", payload=b"one"))
    await asyncio.sleep(0)
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(SimpleNamespace(topic="example/frames", payload=b"overflow"))
    assert owner.pending == 1
    release.set()
    with pytest.raises(ManagedInputError):
        await owner.drain()


@pytest.mark.asyncio
async def test_cancelled_drain_waiter_does_not_cancel_accepted_handler() -> None:
    owner, _, queue = managed()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def handler(message):
        entered.set()
        await finish.wait()

    owner.set_handler(handler)
    await owner.start("example/input")
    queue.put_nowait(SimpleNamespace(topic="example/input", payload=b"work"))
    await entered.wait()
    draining = asyncio.create_task(owner.drain())
    await asyncio.sleep(0)
    draining.cancel()
    with pytest.raises(asyncio.CancelledError):
        await draining
    assert not owner._consumer.done()
    finish.set()
    await owner.drain()


@pytest.mark.asyncio
async def test_one_handler_and_immutable_source() -> None:
    owner, _, _ = managed()
    owner.set_handler(lambda message: asyncio.sleep(0))
    with pytest.raises(ManagedInputError):
        owner.set_handler(lambda message: asyncio.sleep(0))
    await owner.start("example/input")
    with pytest.raises(ManagedInputError):
        await owner.start("another/input")
    await owner.drain()


@pytest.mark.asyncio
async def test_managed_proxy_refuses_competing_legacy_readers() -> None:
    from uns_kit import UnsMqttProxy
    proxy = UnsMqttProxy("unused", process_name="fixture", instance_name="input", managed_input=True)
    with pytest.raises(RuntimeError, match="owns"):
        async with proxy.client.messages("example/source"):
            pass
    with pytest.raises(RuntimeError, match="owns"):
        await proxy.client.resilient_messages("example/source").__anext__()
    await proxy.close()


@pytest.mark.asyncio
async def test_byte_capacity_is_bounded_independently_of_message_count() -> None:
    owner, _, queue = managed(max_pending=10, max_bytes=3, max_message_bytes=3)
    owner.set_handler(lambda message: asyncio.sleep(0))
    await owner.start("t")
    queue.put_nowait(SimpleNamespace(topic="t", payload=b"x"))
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(SimpleNamespace(topic="t", payload=b"y"))
    assert owner.pending == 1
    with pytest.raises(ManagedInputError):
        await owner.drain()


@pytest.mark.asyncio
async def test_invalid_filters_do_not_start_a_subscription() -> None:
    owner, session, _ = managed()
    owner.set_handler(lambda message: asyncio.sleep(0))
    for topic in ("example/#/suffix", "example/bad+"):
        with pytest.raises(ValueError):
            await owner.start(topic)
    assert session.subscribed == []
    await owner.drain()


def test_sync_facade_rejects_managed_input_before_connecting() -> None:
    from uns_kit import UnsProxyProcessSync
    process = UnsProxyProcessSync("unused", {"processName": "sync-fixture"})
    try:
        with pytest.raises(ValueError, match="asynchronous"):
            process.create_uns_mqtt_proxy("unused", "input", uns_parameters={"managedInput": True})
    finally:
        process.stop()
