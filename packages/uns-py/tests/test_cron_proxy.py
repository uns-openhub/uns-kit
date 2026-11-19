from __future__ import annotations

import asyncio

import pytest

from uns_kit.cron import UnsCronProxy


@pytest.mark.asyncio
async def test_cron_proxy_emits_cron_events() -> None:
    proxy = UnsCronProxy("*/1 * * * * *")
    events: list[dict[str, str]] = []
    received = asyncio.Event()

    async def handle(event) -> None:
        events.append(event)
        received.set()

    proxy.event.on("cronEvent", handle)
    await proxy.start()

    try:
        await asyncio.wait_for(received.wait(), timeout=2.5)
    finally:
        await proxy.stop()

    assert events
    assert events[0]["cronExpression"] == "*/1 * * * * *"


@pytest.mark.asyncio
async def test_cron_admission_can_close_without_cancelling_an_accepted_handler() -> None:
    admitted = True
    entered = asyncio.Event()
    finish = asyncio.Event()
    calls = 0
    proxy = UnsCronProxy("* * * * *", active=lambda: admitted)

    async def handle(event):
        nonlocal calls
        calls += 1
        entered.set()
        await finish.wait()

    proxy.event.on("cronEvent", handle)
    accepted = asyncio.create_task(proxy._emit_event(event_name=None, cron_expression="* * * * *"))
    await entered.wait()
    admitted = False
    await proxy._emit_event(event_name=None, cron_expression="* * * * *")
    assert calls == 1
    assert not accepted.done()
    finish.set()
    await accepted
