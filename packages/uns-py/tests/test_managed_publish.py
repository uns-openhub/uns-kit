from __future__ import annotations

import asyncio

import pytest

from uns_kit import UnsMqttProxy


@pytest.mark.asyncio
async def test_bounded_capacity_waits_fifo_without_repeating_publications() -> None:
    proxy = UnsMqttProxy("unused", process_name="fixture", instance_name="output", publish_concurrency=1, max_pending_publishes=1, publish_backpressure=1)
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []

    async def publish(topic, payload):
        entered.set()
        await release.wait()
        effects.append(payload)

    proxy.client.publish_raw = publish
    try:
        await proxy.publish_message("example/out", "first")
        await entered.wait()
        await proxy.publish_message("example/out", "second")
        third = asyncio.create_task(proxy.publish_message("example/out", "third"))
        while proxy._capacity_waiters != 1:
            await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="waiter limit"):
            await proxy.publish_message("example/out", "not-admitted")
        assert not third.done()
        release.set()
        await third
        await proxy.stop()
        assert effects == ["first", "second", "third"]
        with pytest.raises(RuntimeError, match="admission is closed"):
            await proxy.publish_message("example/out", "late")
    finally:
        release.set()
        await proxy.stop()


@pytest.mark.asyncio
async def test_strict_output_failure_stays_unconfirmed_after_queue_empties() -> None:
    proxy = UnsMqttProxy("unused", process_name="fixture", instance_name="output", strict_publish_drain=True)
    failed = asyncio.Event()

    async def publish(topic, payload):
        failed.set()
        raise ConnectionError("synthetic publish failure")

    proxy.client.publish_raw = publish
    await proxy.publish_message("example/out", "original")
    await failed.wait()
    for _ in range(2):
        with pytest.raises(RuntimeError, match="unconfirmed"):
            await proxy.flush()
    with pytest.raises(RuntimeError, match="unconfirmed"):
        await proxy.stop(drain=False)
    # Local fixture teardown only, after proving no clean SDK stop is allowed.
    await proxy._stop_publish_workers(drain=False)
