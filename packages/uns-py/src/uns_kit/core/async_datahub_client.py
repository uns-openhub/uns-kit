"""Awaitable access to the existing UNS client contracts, without blocking asyncio."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any

from .async_worker import BoundedAsyncWorker
from .auth_client import AuthClient
from .datahub_client import BatchRangeResponse, ClientError, RangeResult, RawResponse, UnsClient
from .http_transport import bounded_http_request, http_origin
from .service_token_provider import AccessTokenProvider, ServiceTokenProvider


class _AsyncTransportClient(UnsClient):
    def __init__(self, *args: Any, max_response_bytes: int, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._origin = http_origin(self.api_url)
        self._max_response_bytes = max_response_bytes

    def ensure_token(self) -> str:
        if self.manual_access_token:
            return self.manual_access_token
        if self.token_provider is not None:
            token = self.token_provider.get_access_token()
            if not token:
                raise RuntimeError("Service credential unavailable; review Service access")
            return token
        if self.auth_client is not None:
            return self.auth_client.get_access_token()
        raise RuntimeError("Service credential unavailable; review Service access")

    def _request(self, method: str, url: str, *, body: dict[str, Any] | None = None, token: str | None = None) -> RawResponse:
        if http_origin(url) != self._origin:
            raise ClientError("Async UNS requests must stay on the configured origin")
        return bounded_http_request(method, url, token=token, body=body, timeout=self.timeout, max_bytes=self._max_response_bytes)


class AsyncUnsClient:
    """Bounded, serialized offload of the shared synchronous endpoint helpers.

    Token providers are read inside the worker on every authorized request.
    A missing service credential never invokes interactive login. A cancelled
    caller stops waiting; IO can still finish, and close waits for accepted IO.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_base_path: str = "/api",
        token: str | None = None,
        token_provider: AccessTokenProvider | None = None,
        auth_client: AuthClient | None = None,
        timeout: float = 10.0,
        max_pending: int = 32,
        max_response_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("HTTP timeout must be positive finite seconds")
        if type(max_response_bytes) is not int or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        if token is None and token_provider is None and auth_client is None:
            token_provider = ServiceTokenProvider()
        self._client = _AsyncTransportClient(
            base_url, api_base_path=api_base_path, token=token, token_provider=token_provider,
            auth_client=auth_client, timeout=timeout, max_response_bytes=max_response_bytes,
        )
        self._worker = BoundedAsyncWorker(max_pending=max_pending)

    async def ensure_token(self) -> str:
        return await self._worker.run(self._client.ensure_token)

    async def set_token(self, token: str | None) -> None:
        await self._worker.run(lambda: self._client.set_token(token))

    async def get(self, endpoint: str, params: dict[str, Any] | None = None, *, base_url: str | None = None, authorize: bool = True) -> RawResponse:
        snapshot = deepcopy(params)
        return await self._worker.run(lambda: self._client.get(endpoint, snapshot, base_url=base_url, authorize=authorize))

    async def get_data(self, endpoint: str, params: dict[str, Any] | None = None, *, base_url: str | None = None, authorize: bool = True) -> RawResponse:
        return await self.get(endpoint, params, base_url=base_url, authorize=authorize)

    async def post(self, endpoint: str, data: dict[str, Any] | None = None, *, base_url: str | None = None, authorize: bool = True) -> dict[str, Any]:
        snapshot = deepcopy(data)
        return await self._worker.run(lambda: self._client.post(endpoint, snapshot, base_url=base_url, authorize=authorize))

    async def last_value(self, topics: str | list[str], *, token: str | None = None) -> dict[str, dict[str, Any]] | None:
        snapshot = list(topics) if isinstance(topics, list) else topics
        return await self._worker.run(lambda: self._client.last_value(snapshot, token=token))

    async def get_attribute_data(self, topic_path: str, *, token: str | None = None, **query: Any) -> RangeResult | None:
        snapshot = deepcopy(query)
        return await self._worker.run(lambda: self._client.get_attribute_data(topic_path, token=token, **snapshot))

    async def history(self, topics: str | list[str], *, token: str | None = None, **query: Any) -> BatchRangeResponse | None:
        topic_snapshot = list(topics) if isinstance(topics, list) else topics
        query_snapshot = deepcopy(query)
        return await self._worker.run(lambda: self._client.history(topic_snapshot, token=token, **query_snapshot))

    async def close(self) -> None:
        await self._worker.close()

    async def aclose(self) -> None:
        await self.close()

    async def __aenter__(self) -> AsyncUnsClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
