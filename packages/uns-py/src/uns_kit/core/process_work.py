"""Ownership of accepted application work, independent of transport queues."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class ProcessWork:
    """Track work on one event loop. Failures remain latched after reconnect.

    ``run`` gates new background work; ``track`` always owns already accepted
    input, even after pause. Neither method provides persistence or retries.
    """

    def __init__(self, active: Callable[[], bool] = lambda: True) -> None:
        self._active = active
        self._pending: set[asyncio.Future] = set()
        self._failed = False
        self._paused = False

    def track(self, work: Awaitable[T]) -> asyncio.Future[T]:
        task = asyncio.ensure_future(work)
        self._pending.add(task)
        task.add_done_callback(self._settled)
        return task

    def _settled(self, task: asyncio.Future) -> None:
        if task.cancelled() or task.exception() is not None:
            self._failed = True
        self._pending.discard(task)

    async def run(self, work: Callable[[], Awaitable[T]]) -> T | None:
        if self._paused or not self._active():
            return None

        async def invoke() -> T:
            return await work()

        return await asyncio.shield(self.track(invoke()))

    def pause(self) -> None:
        self._paused = True

    def fail(self) -> None:
        self._failed = True

    async def drain(self) -> None:
        while self._pending:
            await asyncio.shield(asyncio.gather(*self._pending, return_exceptions=True))
        if self._failed:
            raise RuntimeError("Accepted application work is unconfirmed")
