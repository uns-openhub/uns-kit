from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from uns_kit.api.request_lifecycle import HttpRequestDrainError, RequestLifecycle


def connection():
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "http.request", "body": b"", "more_body": False})
    outgoing = []

    async def send(message):
        outgoing.append(message)

    return incoming, outgoing, send


@pytest.mark.asyncio
async def test_gate_and_capacity_reject_before_handler_admission() -> None:
    lifecycle = RequestLifecycle(max_pending=1)
    owned = False
    lifecycle.set_gate(lambda: owned)
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def app(scope, receive, send):
        calls.append(1)
        entered.set()
        await finish.wait()
        await send({"type": "http.response.body", "body": b"done"})

    wrapped = lifecycle.middleware(app)
    incoming, outgoing, send = connection()
    await wrapped({"type": "http"}, incoming.get, send)
    assert outgoing[0]["status"] == 503 and calls == []
    owned = True
    active = asyncio.create_task(wrapped({"type": "http"}, incoming.get, send))
    await entered.wait()
    other, denied, other_send = connection()
    await wrapped({"type": "http"}, other.get, other_send)
    assert denied[0]["status"] == 503 and calls == [1]
    owned = False
    with pytest.raises(TimeoutError):
        await lifecycle.drain(0.01)
    finish.set()
    await active
    await lifecycle.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [True, False])
async def test_cancelled_transport_keeps_work_owned_and_unknown_effects_latched(read_only) -> None:
    lifecycle = RequestLifecycle()
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def app(scope, receive, send):
        async def work():
            entered.set()
            await release.wait()
            completed.append(1)
        if read_only:
            await lifecycle.run_read_only(SimpleNamespace(scope=scope), work)
        else:
            await work()
        await send({"type": "http.response.body", "body": b"done"})

    incoming, _, send = connection()
    transport = asyncio.create_task(lifecycle.middleware(app)({"type": "http"}, incoming.get, send))
    await entered.wait()
    transport.cancel()
    with pytest.raises(asyncio.CancelledError):
        await transport
    lifecycle.begin_release()
    with pytest.raises(TimeoutError):
        await lifecycle.drain(0.01)
    release.set()
    if read_only:
        await lifecycle.drain()
    else:
        for _ in range(2):
            with pytest.raises(HttpRequestDrainError, match="without confirmation"):
                await lifecycle.drain()
    assert completed == [1]


@pytest.mark.asyncio
async def test_response_finish_does_not_release_work_early() -> None:
    lifecycle = RequestLifecycle()
    response_sent, finish = asyncio.Event(), asyncio.Event()

    async def app(scope, receive, send):
        await send({"type": "http.response.body", "body": b"accepted"})
        response_sent.set()
        await finish.wait()

    incoming, _, send = connection()
    transport = asyncio.create_task(lifecycle.middleware(app)({"type": "http"}, incoming.get, send))
    await response_sent.wait()
    with pytest.raises(TimeoutError):
        await lifecycle.drain(0.01)
    finish.set()
    await transport
    await lifecycle.drain()


@pytest.mark.asyncio
async def test_read_declaration_rejects_duplicate_without_running_second_factory() -> None:
    lifecycle = RequestLifecycle()
    second_ran = []

    async def app(scope, receive, send):
        context = SimpleNamespace(scope=scope)

        async def read():
            with pytest.raises(HttpRequestDrainError, match="once"):
                await lifecycle.run_read_only(context, lambda: second_ran.append(1))
        await lifecycle.run_read_only(context, read)
        await send({"type": "http.response.body", "body": b"done"})

    incoming, _, send = connection()
    await lifecycle.middleware(app)({"type": "http"}, incoming.get, send)
    await lifecycle.drain()
    assert second_ran == []


@pytest.mark.asyncio
async def test_an_async_gate_is_not_truthy_ownership() -> None:
    lifecycle = RequestLifecycle()
    calls = []

    async def invalid_gate():
        return True

    lifecycle.set_gate(invalid_gate)
    async def app(scope, receive, send):
        calls.append(1)
    incoming, outgoing, send = connection()
    await lifecycle.middleware(app)({"type": "http"}, incoming.get, send)
    assert outgoing[0]["status"] == 503 and calls == []
