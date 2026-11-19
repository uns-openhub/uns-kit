from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from uns_kit import ProcessLifecycleHooks, ProcessShutdownError, ProcessWork, UnsProxyProcess


def runtime():
    process = UnsProxyProcess("localhost", {"processName": "fixture"})
    process._client = SimpleNamespace(connect=AsyncMock(), close=AsyncMock(), publish_raw=AsyncMock())
    process._status_monitor = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    return process


@pytest.mark.asyncio
async def test_stop_releases_all_sources_then_drains_application_before_output() -> None:
    process = runtime()
    work = ProcessWork(process.is_active)
    process.set_active(True)
    finish = asyncio.Event()
    accepted = work.track(finish.wait())
    output = SimpleNamespace(close=AsyncMock())
    process._proxies.append(output)
    release = Mock(side_effect=work.pause)
    drain = AsyncMock(side_effect=work.drain)
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=drain, on_release=release))
    await process.start()
    stops = [asyncio.create_task(process.stop()) for _ in range(2)]
    while not drain.called:
        await asyncio.sleep(0)
    assert not process.is_active()
    assert await work.run(lambda: asyncio.sleep(0, result="unexpected")) is None
    output.close.assert_not_called()
    finish.set()
    await accepted
    await asyncio.gather(*stops)
    await process.shutdown()
    release.assert_called_once()
    drain.assert_awaited_once()
    output.close.assert_awaited_once()
    process._client.close.assert_awaited_once()
    assert process._activate_task.cancelled()


@pytest.mark.asyncio
async def test_drain_timeout_preserves_work_and_retry_awaits_the_same_drain() -> None:
    process = runtime()
    finish = asyncio.Event()
    release = Mock()
    drain = AsyncMock(side_effect=finish.wait)
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=drain, on_release=release))
    with pytest.raises(TimeoutError):
        await process.stop(timeout=0.01)
    assert not process._application_drain_task.done()
    process._client.close.assert_not_called()
    finish.set()
    await process.stop()
    drain.assert_awaited_once()
    release.assert_called_once()
    process._client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_drain_blocks_transport_close_and_reports_all_participants() -> None:
    process = runtime()
    broken = AsyncMock(side_effect=ValueError("synthetic effect failure"))
    other = AsyncMock()
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=broken))
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=other))
    for _ in range(2):
        with pytest.raises(ProcessShutdownError) as caught:
            await process.stop()
        assert isinstance(caught.value.errors[0], ValueError)
    broken.assert_awaited_once()
    other.assert_awaited_once()
    process._client.close.assert_not_called()


@pytest.mark.asyncio
async def test_caller_cancellation_does_not_cancel_cleanup() -> None:
    process = runtime()
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def drain():
        entered.set()
        await finish.wait()

    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=drain))
    stop = asyncio.create_task(process.stop())
    await entered.wait()
    stop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop
    assert not process._application_drain_task.done()
    finish.set()
    await process.stop()
    process._client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_transport_cleanup_attempts_every_resource_and_retries_only_failed_closes() -> None:
    process = runtime()
    broken = SimpleNamespace(stop=AsyncMock(side_effect=ValueError("synthetic API failure")))
    cron = SimpleNamespace(stop=AsyncMock())
    output = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("synthetic output failure")))
    process._api_proxies.append(broken)
    process._cron_proxies.append(cron)
    process._proxies.append(output)
    with pytest.raises(ProcessShutdownError) as caught:
        await process.stop()
    assert len(caught.value.errors) == 2
    broken.stop.side_effect = None
    output.close.side_effect = None
    await process.stop()
    assert broken.stop.await_count == 2
    cron.stop.assert_awaited_once()
    assert output.close.await_count == 2
    process._status_monitor.stop.assert_awaited_once()
    process._client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_release_failures_do_not_skip_other_release_hooks_or_close_outputs() -> None:
    process = runtime()
    broken = Mock(side_effect=ValueError("cannot stop source"))
    other = Mock()
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=AsyncMock(), on_release=broken))
    process.register_lifecycle_participant(ProcessLifecycleHooks(drain=AsyncMock(), on_release=other))
    with pytest.raises(ProcessShutdownError):
        await process.stop()
    other.assert_called_once()
    process._client.close.assert_not_called()
    broken.side_effect = None
    broken.return_value = None
    await process.stop()
    other.assert_called_once()


@pytest.mark.asyncio
async def test_stopped_owner_rejects_new_resources_and_activation() -> None:
    process = runtime()
    await process.stop()
    for action in (
        process.start,
        lambda: process.create_mqtt_proxy("later"),
        lambda: process.create_api_proxy("later"),
        lambda: process.create_cron_proxy("* * * * *"),
    ):
        with pytest.raises(RuntimeError, match="stopping or stopped"):
            await action()
    with pytest.raises(RuntimeError):
        process.set_active(True)
    with pytest.raises(RuntimeError):
        process.register_lifecycle_participant(ProcessLifecycleHooks(drain=AsyncMock()))


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
async def test_invalid_timeout_does_not_start_shutdown(timeout) -> None:
    process = runtime()
    with pytest.raises(ValueError):
        await process.stop(timeout=timeout)
    assert not process._stop_requested
