from __future__ import annotations

import asyncio

import pytest

from uns_kit import ProcessWork


@pytest.mark.asyncio
async def test_pause_gates_new_work_but_drains_already_accepted_input() -> None:
    finish = asyncio.Event()
    work = ProcessWork()
    accepted = work.track(finish.wait())
    work.pause()
    assert await work.run(lambda: asyncio.sleep(0, result="unexpected")) is None
    draining = asyncio.create_task(work.drain())
    await asyncio.sleep(0)
    assert not draining.done()
    # A transport-delivered input is still accepted after pause.
    assert await work.track(asyncio.sleep(0, result="accepted")) == "accepted"
    finish.set()
    await accepted
    await draining


@pytest.mark.asyncio
async def test_failed_or_cancelled_work_stays_unconfirmed_after_success() -> None:
    for cancel in (False, True):
        work = ProcessWork()

        async def effect() -> None:
            if cancel:
                raise asyncio.CancelledError()
            raise ValueError("synthetic failure")

        with pytest.raises((ValueError, asyncio.CancelledError)):
            await work.track(effect())
        await work.track(asyncio.sleep(0))
        for _ in range(2):
            with pytest.raises(RuntimeError, match="unconfirmed"):
                await work.drain()


@pytest.mark.asyncio
async def test_standby_does_not_invoke_background_factory() -> None:
    def unexpected():
        raise AssertionError("Standby must not start application work")

    assert await ProcessWork(lambda: False).run(unexpected) is None


@pytest.mark.asyncio
async def test_synchronous_factory_failure_is_owned() -> None:
    work = ProcessWork()

    def failing():
        raise ValueError("factory failed")

    with pytest.raises(ValueError):
        await work.run(failing)
    with pytest.raises(RuntimeError, match="unconfirmed"):
        await work.drain()


@pytest.mark.asyncio
async def test_cancelling_drain_or_background_caller_does_not_cancel_effect() -> None:
    work = ProcessWork()
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def effect():
        entered.set()
        await finish.wait()
        return "done"

    caller = asyncio.create_task(work.run(effect))
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    draining = asyncio.create_task(work.drain())
    await asyncio.sleep(0)
    draining.cancel()
    with pytest.raises(asyncio.CancelledError):
        await draining
    assert len(work._pending) == 1
    assert not next(iter(work._pending)).cancelled()
    finish.set()
    await work.drain()
