"""ASGI request ownership, including work after transport cancellation."""

from __future__ import annotations

import asyncio
import inspect
import threading
from concurrent.futures import Future
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable

_SCOPE_KEY = "uns.http.accepted-request"
_CURRENT: ContextVar[Any] = ContextVar("uns_http_request", default=None)


class HttpRequestDrainError(RuntimeError):
    pass


@dataclass(eq=False)
class _Request:
    owner: RequestLifecycle
    done: Future[None] = field(default_factory=Future)
    read_only: bool = False
    read_pending: bool = False
    application_done: bool = False
    transport_ended: bool = False
    response_finished: bool = False


class RequestLifecycle:
    def __init__(self, *, max_pending: int = 256) -> None:
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending_requests must be a positive integer")
        self._limit = max_pending
        self._lock = threading.Lock()
        self._requests: set[_Request] = set()
        self._failed = False
        self._closing = False
        self._gate: Callable[[], bool] | None = None

    def set_gate(self, gate: Callable[[], bool]) -> None:
        if not callable(gate):
            raise TypeError("Ownership gate must be callable")
        with self._lock:
            self._gate = gate

    def begin_release(self) -> None:
        with self._lock:
            self._closing = True

    def _admit(self) -> tuple[_Request | None, str]:
        with self._lock:
            try:
                value = True if self._gate is None else self._gate()
                if inspect.iscoroutine(value):
                    value.close()
                owned = value is True
            except Exception:
                owned = False
            if self._closing or not owned:
                return None, "RUNTIME_PASSIVE"
            if len(self._requests) >= self._limit:
                return None, "RUNTIME_BUSY"
            request = _Request(self)
            self._requests.add(request)
            return request, ""

    def _disconnect(self, request: _Request) -> None:
        with self._lock:
            request.transport_ended = True
            if not request.response_finished and not request.read_only:
                self._failed = True

    def _settle(self, request: _Request, *, error: bool = False) -> None:
        with self._lock:
            if error and not request.read_only:
                self._failed = True
            complete = request.application_done and not request.read_pending
            if complete:
                self._requests.discard(request)
        if complete and not request.done.done():
            request.done.set_result(None)

    async def drain(self, timeout: float | None = 30.0) -> None:
        current = _CURRENT.get()
        if current is not None and current.owner is self:
            raise HttpRequestDrainError("An accepted HTTP handler cannot drain itself")

        async def wait() -> None:
            while True:
                with self._lock:
                    accepted = [request.done for request in self._requests]
                    failed = self._failed
                if not accepted:
                    if failed:
                        raise HttpRequestDrainError("An accepted HTTP request ended without confirmation")
                    return
                await asyncio.shield(asyncio.gather(*(asyncio.wrap_future(future) for future in accepted)))

        await asyncio.wait_for(wait(), timeout=timeout)

    async def run_read_only(self, context: Any, operation: Callable[[], Any]) -> Any:
        request_object = getattr(context, "req", context)
        request = request_object.scope.get(_SCOPE_KEY)
        with self._lock:
            if not isinstance(request, _Request) or request.owner is not self or request not in self._requests:
                raise HttpRequestDrainError("HTTP request was not admitted by this runtime")
            if request.transport_ended or request.read_only or request.application_done:
                raise HttpRequestDrainError("Declare HTTP read-only work once before the response ends")
            request.read_only = True
            request.read_pending = True

        async def run() -> Any:
            try:
                result = operation()
                return await result if inspect.isawaitable(result) else result
            finally:
                with self._lock:
                    request.read_pending = False
                self._settle(request)

        task = asyncio.create_task(run())
        task.add_done_callback(lambda result: None if result.cancelled() else result.exception())
        return await asyncio.shield(task)

    def middleware(self, app: Any) -> Any:
        owner = self

        async def managed(scope: Any, receive: Any, send: Any) -> None:
            if scope["type"] != "http":
                await app(scope, receive, send)
                return
            request, reason = owner._admit()
            if request is None:
                from starlette.responses import JSONResponse
                await JSONResponse({"error": reason}, status_code=503, headers={"Retry-After": "1"})(scope, receive, send)
                return
            scope[_SCOPE_KEY] = request
            incoming: asyncio.Queue = asyncio.Queue(maxsize=1)

            async def pump() -> None:
                try:
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            owner._disconnect(request)
                        await incoming.put(message)
                        if message["type"] == "http.disconnect":
                            return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    owner._disconnect(request)
                    await incoming.put({"type": "http.disconnect"})

            async def tracked_send(message: Any) -> None:
                try:
                    await send(message)
                except BaseException:
                    owner._disconnect(request)
                    raise
                if message["type"] == "http.response.body" and not message.get("more_body", False):
                    with owner._lock:
                        request.response_finished = True
                        request.transport_ended = True

            pump_task = asyncio.create_task(pump())

            async def run() -> None:
                token = _CURRENT.set(request)
                failed = False
                try:
                    await app(scope, incoming.get, tracked_send)
                    if not request.response_finished:
                        failed = True
                except BaseException:
                    failed = True
                    raise
                finally:
                    _CURRENT.reset(token)
                    pump_task.cancel()
                    await asyncio.gather(pump_task, return_exceptions=True)
                    with owner._lock:
                        request.application_done = True
                    owner._settle(request, error=failed)

            task = asyncio.create_task(run())
            task.add_done_callback(lambda result: None if result.cancelled() else result.exception())
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                owner._disconnect(request)
                raise

        return managed


class RequestOwnershipMiddleware:
    def __init__(self, app: Any, lifecycle: RequestLifecycle) -> None:
        self._app = lifecycle.middleware(app)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self._app(scope, receive, send)
