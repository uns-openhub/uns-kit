from __future__ import annotations

import asyncio
import threading

import pytest

from uns_kit.core.async_worker import AsyncCapacityError, AsyncClientClosedError, BoundedAsyncWorker


@pytest.mark.asyncio
async def test_cancelled_waiter_keeps_capacity_and_close_waits_for_real_io() -> None:
    worker = BoundedAsyncWorker(max_pending=1)
    entered, release = threading.Event(), threading.Event()
    effects = []

    def operation():
        entered.set()
        release.wait(2)
        effects.append("applied")
        return 42

    call = asyncio.create_task(worker.run(operation))
    close = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        with pytest.raises(AsyncCapacityError):
            await worker.run(lambda: effects.append("unexpected"))
        close = asyncio.create_task(worker.close(lambda: effects.append("closed")))
        await asyncio.sleep(0)
        assert not close.done()
        with pytest.raises(AsyncClientClosedError):
            await worker.run(lambda: None)
    finally:
        release.set()
        if close is not None:
            await close
        await worker.close()
    assert effects == ["applied", "closed"]


@pytest.mark.asyncio
async def test_one_worker_keeps_affinity_and_serial_order() -> None:
    worker = BoundedAsyncWorker()
    calls = []
    try:
        def operation(index):
            calls.append((index, threading.get_ident()))
            return index

        assert await asyncio.gather(*(worker.run(lambda index=index: operation(index)) for index in range(20))) == list(range(20))
    finally:
        await worker.close(lambda: calls.append((20, threading.get_ident())))
    assert [index for index, _ in calls] == list(range(21))
    assert len({thread for _, thread in calls}) == 1
    assert calls[0][1] != threading.get_ident()


@pytest.mark.asyncio
async def test_close_can_be_awaited_from_another_event_loop() -> None:
    worker = BoundedAsyncWorker()
    assert await worker.run(lambda: 42) == 42
    await asyncio.to_thread(lambda: asyncio.run(worker.close()))
    await worker.close()


@pytest.mark.asyncio
async def test_failure_does_not_skip_cleanup_or_get_replayed() -> None:
    worker = BoundedAsyncWorker()
    calls = []

    def fail():
        calls.append("effect")
        raise ConnectionError("outcome unknown")

    with pytest.raises(ConnectionError):
        await worker.run(fail)
    await worker.close(lambda: calls.append("closed"))
    assert calls == ["effect", "closed"]


@pytest.mark.asyncio
async def test_cancelling_close_waiter_does_not_cancel_or_repeat_cleanup() -> None:
    worker = BoundedAsyncWorker()
    entered, release = threading.Event(), threading.Event()
    cleanup = []

    def hold():
        entered.set()
        release.wait(2)

    operation = asyncio.create_task(worker.run(hold))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        closing = asyncio.create_task(worker.close(lambda: cleanup.append("closed")))
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert cleanup == []
    finally:
        release.set()
        await operation
        await worker.close()
    assert cleanup == ["closed"]
