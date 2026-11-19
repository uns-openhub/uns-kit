from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from uns_kit import AsyncUnsClient, ClientError, HttpRequestDrainError, ProcessShutdownError, UnsProxyProcess


async def wait_event(event):
    assert await asyncio.to_thread(event.wait, 2)


def runtime():
    process = UnsProxyProcess("unused", {"processName": "http-fixture", "managedOwnership": True})
    process._client = SimpleNamespace(publish_raw=AsyncMock(), close=AsyncMock())
    process._status_monitor = SimpleNamespace(stop=AsyncMock())
    return process


@pytest.mark.asyncio
async def test_real_http_owner_gates_and_process_stop_waits_before_output_cleanup() -> None:
    process = runtime()
    api = await process.create_api_proxy("api")
    entered, release = threading.Event(), threading.Event()
    effects = []

    async def handle(event):
        entered.set()
        await asyncio.to_thread(release.wait, 2)
        effects.append("done")
        event.res.json({"ok": True})

    api.event.on("apiPostEvent", handle)
    await api.post("example/site", "asset", "equipment", "main", "run")
    path = "example/site/asset/equipment/main/run"
    output = SimpleNamespace(close=AsyncMock())
    process._proxies.append(output)
    try:
        async with AsyncUnsClient(f"http://127.0.0.1:{api._port}") as client:
            with pytest.raises(ClientError) as caught:
                await client.post(path, authorize=False)
            assert caught.value.status_code == 503 and effects == []
            process.set_active(True)
            request = asyncio.create_task(client.post(path, authorize=False))
            await wait_event(entered)
            with pytest.raises(ProcessShutdownError) as caught:
                await process.stop(timeout=0.01)
            assert any(isinstance(error, TimeoutError) for error in caught.value.errors)
            stop = asyncio.create_task(process.stop(timeout=2))
            output.close.assert_not_called()
            # A separate connection verifies admission closed immediately.
            async with AsyncUnsClient(f"http://127.0.0.1:{api._port}") as rejected:
                with pytest.raises(ClientError) as caught:
                    await rejected.post(path, authorize=False)
                assert caught.value.status_code == 503
            release.set()
            assert await request == {"ok": True}
            await stop
        output.close.assert_awaited_once()
        assert effects == ["done"]
    finally:
        release.set()
        await process.stop()


async def force_fixture_exit(api):
    # Test-only cleanup after asserting an intentionally unconfirmed operation.
    # The SDK must refuse clean Stop in that state.
    api._server.should_exit = True
    await asyncio.to_thread(api._server_thread.join, 2)
    if api._status_task:
        api._status_task.cancel()
        await asyncio.gather(api._status_task, return_exceptions=True)
    await api._service_authority.close()
    await api._jwt_worker.close()
    await super(type(api), api).stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("method,read_only", [("POST", True), ("GET", True), ("POST", False), ("GET", False)])
async def test_disconnected_readers_settle_but_undeclared_effects_remain_unknown(method, read_only) -> None:
    process = runtime()
    process.set_active(True)
    api = await process.create_api_proxy("api")
    entered, release = threading.Event(), threading.Event()
    effects = []

    async def handle(event):
        async def work():
            entered.set()
            await asyncio.to_thread(release.wait, 2)
            effects.append("settled")
        if read_only:
            await api.run_read_only_request(event, work)
        else:
            await work()
        event.res.json({"ok": True})

    api.event.on("apiPostEvent" if method == "POST" else "apiGetEvent", handle)
    await (api.post if method == "POST" else api.get)("example/independent", "asset", "service", "main", "lookup")
    path = "/api/example/independent/asset/service/main/lookup"
    reader, writer = await asyncio.open_connection("127.0.0.1", api._port)
    try:
        writer.write(f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode())
        await writer.drain()
        await wait_event(entered)
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.02)
        api.begin_release()
        with pytest.raises(TimeoutError):
            await api.drain_requests(0.01)
        release.set()
        if read_only:
            await api.drain_requests()
            await process.stop()
        else:
            for _ in range(2):
                with pytest.raises(HttpRequestDrainError, match="without confirmation"):
                    await api.drain_requests()
        assert effects == ["settled"]
    finally:
        release.set()
        writer.close()
        if read_only:
            await process.stop()
        else:
            await force_fixture_exit(api)
