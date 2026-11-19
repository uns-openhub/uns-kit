"""Async database surface with one bounded, thread-affine worker per client."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, cast

from ..core.async_worker import BoundedAsyncWorker
from .client import create_database_client
from .schema import DatabaseConnectionConfig, SqliteDatabaseConfig, normalize_database_config
from .types import DatabaseClient, DatabaseExecuteResult, DatabaseQueryResult, SqlParams


class AsyncDatabaseClient:
    """Construct, use and close all driver resources on the same worker thread.

    Operations are serialized; max_pending bounds active plus queued work.
    Cancellation does not mean SQL was rolled back. close waits for accepted
    work; no failed SQL operation is automatically retried.
    """

    def __init__(self, config: DatabaseConnectionConfig, *, name: str | None = None, max_pending: int = 32) -> None:
        self._config = normalize_database_config(deepcopy(dict(config)))
        self.dialect = self._config["dialect"]
        self.name = name
        self.sql_dir = str(Path(self._config["sqlDir"]).resolve()) if self._config.get("sqlDir") else None
        # Freeze relative filenames/directories at construction, not first IO.
        if self.sql_dir is not None:
            self._config["sqlDir"] = self.sql_dir
        if self.dialect == "sqlite":
            sqlite_config = cast(SqliteDatabaseConfig, self._config)
            if sqlite_config["filename"] != ":memory:":
                sqlite_config["filename"] = str(Path(sqlite_config["filename"]).resolve())
        self._client: DatabaseClient | None = None
        self._worker = BoundedAsyncWorker(max_pending=max_pending)

    def _get_client(self) -> DatabaseClient:
        if self._client is None:
            self._client = create_database_client(self._config, name=self.name)
        return self._client

    async def query(self, sql_text: str, params: SqlParams | None = None) -> DatabaseQueryResult:
        snapshot = deepcopy(params)
        return await self._worker.run(lambda: self._get_client().query(sql_text, snapshot))

    async def execute(self, sql_text: str, params: SqlParams | None = None) -> DatabaseExecuteResult:
        snapshot = deepcopy(params)
        return await self._worker.run(lambda: self._get_client().execute(sql_text, snapshot))

    async def query_file(self, file_path: str, params: SqlParams | None = None) -> DatabaseQueryResult:
        snapshot = deepcopy(params)
        return await self._worker.run(lambda: self._get_client().query_file(file_path, snapshot))

    async def execute_file(self, file_path: str, params: SqlParams | None = None) -> DatabaseExecuteResult:
        snapshot = deepcopy(params)
        return await self._worker.run(lambda: self._get_client().execute_file(file_path, snapshot))

    def _close(self) -> None:
        if self._client is not None:
            self._client.close()

    async def close(self) -> None:
        await self._worker.close(self._close)

    async def aclose(self) -> None:
        await self.close()

    async def __aenter__(self) -> AsyncDatabaseClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


def create_async_database_client(config: DatabaseConnectionConfig, *, name: str | None = None, max_pending: int = 32) -> AsyncDatabaseClient:
    """Create a lazy async client. Driver setup happens on the first awaited IO."""
    return AsyncDatabaseClient(config, name=name, max_pending=max_pending)
