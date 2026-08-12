from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
import threading
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Optional, Sequence, TYPE_CHECKING, TypedDict, Unpack

from .auth_client import AuthClient
from .service_token_provider import AccessTokenProvider

if TYPE_CHECKING:
    import pandas as pd


class RangeQuery(TypedDict, total=False):
    table: str
    from_: str
    to: str
    timeField: Literal["auto", "timestamp", "interval"]
    limit: int
    maxPoints: int
    bucketMs: int
    aggregate: Literal["avg", "min", "max", "last", "sum", "count"]
    column: str
    summaryOnly: bool
    dedupe: bool

@dataclass(frozen=True)
class LastValueResult:
    topic: str
    value: Any
    values: Optional[dict[str, Any]]
    uom: Optional[str]
    timestamp: Optional[str]
    data_group: Optional[str]
    age_ms: Optional[float]
    source: str

    @staticmethod
    def from_mapping(value: Mapping[str, Any]) -> "LastValueResult":
        raw_values = value.get("values")
        return LastValueResult(
            topic=str(value.get("topic") or ""),
            value=value.get("value"),
            values=dict(raw_values) if isinstance(raw_values, Mapping) else None,
            uom=value.get("uom") if isinstance(value.get("uom"), str) else None,
            timestamp=value.get("timestamp") if isinstance(value.get("timestamp"), str) else None,
            data_group=value.get("dataGroup") if isinstance(value.get("dataGroup"), str) else None,
            age_ms=float(value["ageMs"]) if isinstance(value.get("ageMs"), (int, float)) else None,
            source=str(value.get("source") or "miss"),
        )

    @property
    def hit(self) -> bool:
        return self.source == "cache"

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "value": self.value,
            "values": self.values,
            "uom": self.uom,
            "timestamp": self.timestamp,
            "dataGroup": self.data_group,
            "ageMs": self.age_ms,
            "source": self.source,
        }


@dataclass(frozen=True)
class RangeResult:
    data: list[list[Any]]
    stats: Optional[dict[str, Any]]

    @staticmethod
    def from_mapping(value: Mapping[str, Any]) -> "RangeResult":
        raw_data = value.get("data")
        rows = [list(row) for row in raw_data if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray))] if isinstance(raw_data, list) else []
        raw_stats = value.get("stats")
        stats = dict(raw_stats) if isinstance(raw_stats, Mapping) else None
        return RangeResult(data=rows, stats=stats)

    @property
    def columns(self) -> list[dict[str, Any]]:
        raw = self.stats.get("raw") if isinstance(self.stats, Mapping) else None
        columns = raw.get("columns") if isinstance(raw, Mapping) else None
        return [dict(column) for column in columns] if isinstance(columns, list) else []

    def records(self) -> list[dict[str, Any]]:
        columns = self.columns
        if not columns:
            return [{str(index): cell for index, cell in enumerate(row)} for row in self.data]
        return [
            {(columns[index].get("name") or str(index)): cell for index, cell in enumerate(row)}
            for row in self.data
        ]

    def to_dataframe(self) -> "pd.DataFrame":
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - exercised via stubbed import in tests
            raise RuntimeError("pandas is required for to_dataframe(). Install pandas first.") from exc
        column_names = [str(column.get("name") or index) for index, column in enumerate(self.columns)]
        return pd.DataFrame(self.data, columns=column_names or None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": [list(row) for row in self.data],
            "stats": dict(self.stats) if isinstance(self.stats, Mapping) else None,
        }


@dataclass(frozen=True)
class BatchRangeTopicResult(RangeResult):
    topic: str
    error: Optional[str]

    @staticmethod
    def from_mapping(value: Mapping[str, Any]) -> "BatchRangeTopicResult":
        base = RangeResult.from_mapping(value)
        return BatchRangeTopicResult(
            topic=str(value.get("topic") or ""),
            error=value.get("error") if isinstance(value.get("error"), str) else None,
            data=base.data,
            stats=base.stats,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "error": self.error,
            "data": [list(row) for row in self.data],
            "stats": dict(self.stats) if isinstance(self.stats, Mapping) else None,
        }


@dataclass(frozen=True)
class BatchRangeResponse:
    results: list[BatchRangeTopicResult]
    stats: Optional[dict[str, Any]]

    @staticmethod
    def from_mapping(value: Mapping[str, Any]) -> "BatchRangeResponse":
        raw_results = value.get("results")
        results = [
            BatchRangeTopicResult.from_mapping(item)
            for item in raw_results
            if isinstance(raw_results, list) and isinstance(item, Mapping)
        ]
        raw_stats = value.get("stats")
        stats = dict(raw_stats) if isinstance(raw_stats, Mapping) else None
        return BatchRangeResponse(results=results, stats=stats)

    @property
    def by_topic(self) -> dict[str, dict[str, Any]]:
        return {result.topic: result.to_dict() for result in self.results}

    def to_dict(self) -> dict[str, Any]:
        return {
            "results": [result.to_dict() for result in self.results],
            "stats": dict(self.stats) if isinstance(self.stats, Mapping) else None,
        }


class ClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class RawResponse:
    status_code: int
    headers: dict[str, str]
    content: bytes

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text) if self.content else None


class UnsClient:
    def __init__(
        self,
        base_url: str,
        *,
        api_base_path: str = "/api",
        token: Optional[str] = None,
        token_provider: Optional[AccessTokenProvider] = None,
        auth_client: Optional[AuthClient] = None,
        timeout: float = 10.0,
    ) -> None:
        self.api_base_path = self._normalize_base_path(api_base_path)
        stripped_base_url = base_url.rstrip("/")
        if self.api_base_path and stripped_base_url.endswith(self.api_base_path):
            self.api_url = stripped_base_url
            self.base_url = stripped_base_url[: -len(self.api_base_path)].rstrip("/")
        else:
            self.base_url = stripped_base_url
            self.api_url = f"{stripped_base_url}{self.api_base_path}"
        self.manual_access_token = token
        self.access_token = token
        self.token_provider = token_provider
        self.timeout = timeout
        self.auth_client = auth_client
        self._opener = urllib.request.build_opener()

    def set_token(self, token: Optional[str]) -> None:
        self.manual_access_token = token
        self.access_token = token

    def ensure_token(self) -> Optional[str]:
        if self.manual_access_token:
            return self.manual_access_token
        if self.token_provider:
            service_token = self.token_provider.get_access_token()
            if service_token:
                self.access_token = service_token
                return service_token
        try:
            if self.auth_client is None:
                self.auth_client = AuthClient.create()
            self.access_token = self.auth_client.get_access_token()
            return self.access_token

        except Exception as exc:
            raise RuntimeError("No access token available, please login again.") from exc

    def get(self, endpoint: str, params: Optional[dict[str, Any]] = None, *, base_url: Optional[str] = None, authorize: bool = True) -> RawResponse:
        token = self.ensure_token() if authorize else None
        query = urllib.parse.urlencode(params or {})
        suffix = f"?{query}" if query else ""
        return self._request(
            "GET",
            f"{self._build_url(endpoint, base_url=base_url)}{suffix}",
            token=token,
        )

    def post(self, endpoint: str, data: Optional[dict[str, Any]] = None, *, base_url: Optional[str] = None, authorize: bool = True) -> dict[str, Any]:
        token = self.ensure_token() if authorize else None
        return self._request_json(
            "POST",
            self._build_url(endpoint, base_url=base_url),
            body=data or {},
            token=token,
        )

    def get_data(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        *,
        base_url: Optional[str] = None,
        authorize: bool = True,
    ) -> RawResponse:
        return self.get(endpoint, params, base_url=base_url, authorize=authorize)

    def last_value(self, topics: str | list[str], *, token: Optional[str] = None) -> Optional[dict[str, dict[str, Any]]]:
        topic_list = [topics] if isinstance(topics, str) else topics
        if not topic_list:
            raise ValueError("topics must contain at least one topic.")
        if len(topic_list) > 500:
            raise ValueError("Maximum 500 topics per request.")
        effective_token = token
        if effective_token is None:
            effective_token = self.ensure_token()
        try:
            payload = self._request_json(
                "POST",
                self._build_url("catchall/batch/last"),
                body={"topics": topic_list},
                token=effective_token,
            )
        except ClientError as exc:
            if exc.status_code == 404:
                return None
            raise
        raw_results = payload.get("results")
        if not isinstance(raw_results, list):
            raise ClientError("Last-value response did not include a results array.")
        results = [
            LastValueResult.from_mapping(item)
            for item in raw_results
            if isinstance(item, Mapping)
        ]
        return {result.topic: result.to_dict() for result in results}

    def get_attribute_data(
        self,
        topic_path: str,
        *,
        token: Optional[str] = None,
        **query: Unpack[RangeQuery],
    ) -> Optional[RangeResult]:
        effective_token = token
        if effective_token is None:
            effective_token = self.ensure_token()
        encoded_topic_path = urllib.parse.quote(topic_path, safe="")
        url = f"{self._build_url(f'catchall/{encoded_topic_path}')}{self._build_query_suffix(self._normalize_range_query(query))}"
        try:
            payload = self._request_json("GET", url, token=effective_token)
        except ClientError as exc:
            if exc.status_code == 404:
                return None
            raise
        return RangeResult.from_mapping(payload)

    def history(
        self,
        topics: str | list[str],
        *,
        token: Optional[str] = None,
        **query: Unpack[RangeQuery],
    ) -> Optional[BatchRangeResponse]:
        topic_list = [topics] if isinstance(topics, str) else topics
        if not topic_list:
            raise ValueError("topics must contain at least one topic.")
        if len(topic_list) > 500:
            raise ValueError("Maximum 500 topics per request.")
        effective_token = token
        if effective_token is None:
            effective_token = self.ensure_token()
        try:
            payload = self._request_json(
                "POST",
                self._build_url("catchall/batch/range"),
                body={"topics": topic_list, **self._normalize_range_query(query)},
                token=effective_token,
            )
        except ClientError as exc:
            if exc.status_code == 404:
                return None
            raise
        raw_results = payload.get("results")
        if not isinstance(raw_results, list):
            raise ClientError("Batch-range response did not include a results array.")
        return BatchRangeResponse.from_mapping(payload)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: Optional[dict[str, Any]] = None,
        token: Optional[str] = None,
    ) -> dict[str, Any]:
        response = self._request(method, url, body=body, token=token)
        decoded = response.text
        if not decoded:
            return {}
        try:
            parsed = json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise ClientError("UNS response was not valid JSON.") from exc
        if not isinstance(parsed, dict):
            raise ClientError("UNS response must be a JSON object.")
        return parsed

    def _request(
        self,
        method: str,
        url: str,
        *,
        body: Optional[dict[str, Any]] = None,
        token: Optional[str] = None,
    ) -> RawResponse:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "*/*",
                **({"Content-Type": "application/json"} if data is not None else {}),
                **({"Authorization": f"Bearer {token}"} if token else {}),
            },
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                content = response.read()
                status_code = getattr(response, "status", response.getcode())
                headers = dict(response.headers.items())
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            raise ClientError(
                f"UNS request failed with HTTP {exc.code}: {message}",
                status_code=exc.code,
            ) from exc
        except urllib.error.URLError as exc:
            raise ClientError(f"UNS request failed: {exc.reason}") from exc

        return RawResponse(status_code=status_code, headers=headers, content=content)

    def _build_url(self, endpoint: str, *, base_url: Optional[str] = None) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        root = (base_url or self.api_url).rstrip("/")
        path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        if base_url is None and self.api_base_path and (
            path == self.api_base_path or path.startswith(f"{self.api_base_path}/")
        ):
            path = path[len(self.api_base_path):] or "/"
        return f"{root}{path}"

    def _build_query_suffix(self, params: Mapping[str, Any]) -> str:
        filtered = {key: value for key, value in params.items() if value is not None}
        if not filtered:
            return ""
        return f"?{urllib.parse.urlencode(filtered)}"

    def _normalize_range_query(self, query: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(query)
        if "from_" in normalized:
            normalized["from"] = normalized.pop("from_")
        return normalized

    @staticmethod
    def _normalize_base_path(value: str) -> str:
        stripped = value.strip()
        if not stripped:
            return ""
        return stripped if stripped.startswith("/") else f"/{stripped}"


class UnsClientManager:
    """Process-local registry for sharing configured :class:`UnsClient` instances."""

    def __init__(self) -> None:
        self._clients: dict[str, UnsClient] = {}
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        base_url: str,
        *,
        api_base_path: str = "/api",
        token: Optional[str] = None,
        token_provider: Optional[AccessTokenProvider] = None,
        auth_client: Optional[AuthClient] = None,
        timeout: float = 10.0,
    ) -> UnsClient:
        client = UnsClient(
            base_url,
            api_base_path=api_base_path,
            token=token,
            token_provider=token_provider,
            auth_client=auth_client,
            timeout=timeout,
        )
        with self._lock:
            self._clients[_normalize_client_name(name)] = client
        return client

    def get(self, name: str = "default") -> UnsClient:
        normalized_name = _normalize_client_name(name)
        with self._lock:
            client = self._clients.get(normalized_name)
        if client is None:
            raise RuntimeError(
                f"UNS client '{normalized_name}' is not registered. "
                "Call register_uns_client() during application startup."
            )
        return client

    def delete(self, name: str = "default") -> None:
        with self._lock:
            self._clients.pop(_normalize_client_name(name), None)

    def clear(self) -> None:
        with self._lock:
            self._clients.clear()


def _normalize_client_name(name: str) -> str:
    normalized_name = name.strip()
    if not normalized_name:
        raise ValueError("UNS client name must not be empty.")
    return normalized_name


_default_uns_client_manager = UnsClientManager()


def register_uns_client(
    base_url: str,
    *,
    name: str = "default",
    api_base_path: str = "/api",
    token: Optional[str] = None,
    token_provider: Optional[AccessTokenProvider] = None,
    auth_client: Optional[AuthClient] = None,
    timeout: float = 10.0,
) -> UnsClient:
    """Create or replace a named process-local UNS REST client."""
    return _default_uns_client_manager.register(
        name,
        base_url,
        api_base_path=api_base_path,
        token=token,
        token_provider=token_provider,
        auth_client=auth_client,
        timeout=timeout,
    )


def get_uns_client(name: str = "default") -> UnsClient:
    """Return a named client previously registered during application startup."""
    return _default_uns_client_manager.get(name)


def delete_uns_client(name: str = "default") -> None:
    """Remove a named client from the process-local registry."""
    _default_uns_client_manager.delete(name)
