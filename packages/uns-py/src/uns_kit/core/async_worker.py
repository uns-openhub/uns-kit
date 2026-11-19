"""Bounded offload with resource affinity and honest cancellation semantics."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class AsyncCapacityError(RuntimeError):
    """The operation was rejected before it entered the worker queue."""


class AsyncClientClosedError(RuntimeError):
    """The resource has closed admission for new operations."""


class BoundedAsyncWorker:
    """Keep accepted operations owned until underlying IO actually finishes.

    One worker gives database drivers thread affinity. Futures are concurrent
    futures so teardown can also be awaited from another event loop, as used
    by the API server's own thread. Cancelling a caller never cancels its IO.
    """

    def __init__(self, *, max_pending: int = 32, workers: int = 1) -> None:
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be a positive integer")
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="uns-sdk-io")
        self._max_pending = max_pending
        self._lock = threading.Lock()
        self._pending: set[Future[Any]] = set()
        self._closing = False
        self._close_future: Future[None] | None = None

    @property
    def pending(self) -> int:
        with self._lock:
            return len(self._pending)

    def _settled(self, future: Future[Any]) -> None:
        with self._lock:
            self._pending.discard(future)

    @staticmethod
    async def _await(future: Future[T]) -> T:
        wrapped = asyncio.wrap_future(future)
        # A cancelled waiter can leave an underlying failure without a waiter.
        wrapped.add_done_callback(lambda result: None if result.cancelled() else result.exception())
        return await asyncio.shield(wrapped)

    async def run(self, operation: Callable[[], T]) -> T:
        with self._lock:
            if self._closing:
                raise AsyncClientClosedError("Async client is closing or closed")
            if len(self._pending) >= self._max_pending:
                raise AsyncCapacityError("Async client admission capacity exceeded")
            future = self._executor.submit(operation)
            self._pending.add(future)
        # Register outside the lock: an already-finished future calls inline.
        future.add_done_callback(self._settled)
        return await self._await(future)

    async def close(self, cleanup: Callable[[], None] | None = None) -> None:
        with self._lock:
            if self._close_future is None:
                self._closing = True
                accepted = tuple(self._pending)

                def finish() -> None:
                    for future in accepted:
                        try:
                            future.result()
                        except BaseException:
                            # Operation errors belong to their original callers.
                            pass
                    if cleanup is not None:
                        cleanup()

                self._close_future = self._executor.submit(finish)
                self._executor.shutdown(wait=False)
            future = self._close_future
        await self._await(future)
