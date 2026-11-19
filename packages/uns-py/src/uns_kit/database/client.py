from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Callable

from ..core.logger import get_logger
from .params import compile_named_params
from .schema import DatabaseConnectionConfig, normalize_database_config
from .sql import load_sql_file
from .types import DatabaseAdapter, DatabaseClient, DatabaseDialect, DatabaseExecuteResult, DatabaseQueryResult, SqlParams

logger = get_logger(__name__)


class _DatabaseClientImpl:
    def __init__(
        self,
        adapter_factory: Callable[[], DatabaseAdapter],
        dialect: DatabaseDialect,
        *,
        name: str | None = None,
        sql_dir: str | None = None,
    ) -> None:
        self._adapter_factory = adapter_factory
        self._adapter: DatabaseAdapter | None = None
        self.dialect = dialect
        self.name = name
        self.sql_dir = sql_dir

    def _get_adapter(self) -> DatabaseAdapter:
        if self._adapter is None:
            self._adapter = self._adapter_factory()
        return self._adapter

    def query(self, sql_text: str, params: SqlParams | None = None) -> DatabaseQueryResult:
        statement = compile_named_params(self.dialect, sql_text, params)
        return self._get_adapter().query(statement)

    def execute(self, sql_text: str, params: SqlParams | None = None) -> DatabaseExecuteResult:
        statement = compile_named_params(self.dialect, sql_text, params)
        return self._get_adapter().execute(statement)

    def query_file(self, file_path: str, params: SqlParams | None = None) -> DatabaseQueryResult:
        return self.query(load_sql_file(file_path, base_dir=self.sql_dir), params)

    def execute_file(self, file_path: str, params: SqlParams | None = None) -> DatabaseExecuteResult:
        return self.execute(load_sql_file(file_path, base_dir=self.sql_dir), params)

    def close(self) -> None:
        adapter = self._adapter
        self._adapter = None
        if adapter is None:
            return
        adapter.close()


class _SqliteAdapter:
    dialect: DatabaseDialect = "sqlite"

    def __init__(self, config: dict[str, Any]) -> None:
        filename = str(config["filename"])
        timeout_ms = int(config.get("timeoutMs", 5000))
        readonly = bool(config.get("readonly", False))
        file_must_exist = bool(config.get("fileMustExist", False))
        path = Path(filename)
        if file_must_exist and not path.exists():
            raise FileNotFoundError(f"SQLite database file does not exist: {filename}")
        if readonly:
            uri = f"file:{path.resolve().as_posix()}?mode=ro"
            self._connection = sqlite3.connect(uri, uri=True, timeout=timeout_ms / 1000)
        else:
            self._connection = sqlite3.connect(str(path), timeout=timeout_ms / 1000)
        self._connection.row_factory = sqlite3.Row

    def query(self, statement) -> DatabaseQueryResult:
        cursor = self._connection.execute(statement.text, statement.values)
        rows = [dict(row) for row in cursor.fetchall()]
        return DatabaseQueryResult(rows=rows, row_count=len(rows))

    def execute(self, statement) -> DatabaseExecuteResult:
        cursor = self._connection.execute(statement.text, statement.values)
        self._connection.commit()
        return DatabaseExecuteResult(row_count=cursor.rowcount if cursor.rowcount != -1 else 0)

    def close(self) -> None:
        self._connection.close()


class _PgAdapter:
    dialect: DatabaseDialect = "pg"

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                'Postgres support is not available. Install the database extra with a usable psycopg runtime, '
                'for example `pip install "uns-kit[database]"` or `pip install "psycopg[binary]"`. '
                f"Original import error: {exc}"
            ) from exc
        self._psycopg = psycopg
        self._connection_kwargs = {
            "host": config["host"],
            "port": config.get("port", 5432),
            "dbname": config["database"],
            "user": config["user"],
            "password": config.get("password"),
        }
        self._connection = None

    def _get_connection(self):
        if self._connection is None or getattr(self._connection, "closed", False):
            self._connection = self._psycopg.connect(**self._connection_kwargs)
        return self._connection

    def _drop_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    def _run_with_connection(self, operation):
        connection = self._get_connection()
        try:
            return operation(connection)
        except Exception:
            # The server may have committed before disconnecting. Never replay
            # this SQL; only a separately requested operation may reconnect.
            self._drop_connection()
            raise

    def query(self, statement) -> DatabaseQueryResult:
        def operation(connection) -> DatabaseQueryResult:
            with connection.cursor() as cursor:
                cursor.execute(statement.text, statement.values)
                columns = [description.name for description in (cursor.description or [])]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return DatabaseQueryResult(rows=rows, row_count=len(rows))

        return self._run_with_connection(operation)

    def execute(self, statement) -> DatabaseExecuteResult:
        def operation(connection) -> DatabaseExecuteResult:
            with connection.cursor() as cursor:
                cursor.execute(statement.text, statement.values)
                connection.commit()
                return DatabaseExecuteResult(row_count=cursor.rowcount if cursor.rowcount != -1 else 0)

        return self._run_with_connection(operation)

    def close(self) -> None:
        self._drop_connection()


class _OracleAdapter:
    dialect: DatabaseDialect = "oracle"

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import oracledb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError('Oracle support requires `pip install "uns-kit[database]"`.') from exc
        self._oracledb = oracledb
        connect_string = config.get("connectString")
        if not connect_string:
            host = config["host"]
            port = int(config.get("port", 1521))
            service_name = config.get("serviceName")
            sid = config.get("sid")
            connect_string = f"{host}:{port}/{service_name or sid}"
        self._connection_kwargs = {
            "user": config["user"],
            "password": config.get("password"),
            "dsn": connect_string,
        }
        self._connection = None

    def _get_connection(self):
        if self._connection is None:
            self._connection = self._oracledb.connect(**self._connection_kwargs)
        return self._connection

    def _drop_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:
            pass

    def _run_with_connection(self, operation):
        connection = self._get_connection()
        try:
            return operation(connection)
        except Exception:
            self._drop_connection()
            raise

    def query(self, statement) -> DatabaseQueryResult:
        def operation(connection) -> DatabaseQueryResult:
            with connection.cursor() as cursor:
                cursor.execute(statement.text, statement.values)
                columns = [column[0] for column in (cursor.description or [])]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return DatabaseQueryResult(rows=rows, row_count=len(rows))

        return self._run_with_connection(operation)

    def execute(self, statement) -> DatabaseExecuteResult:
        def operation(connection) -> DatabaseExecuteResult:
            with connection.cursor() as cursor:
                cursor.execute(statement.text, statement.values)
                connection.commit()
                return DatabaseExecuteResult(row_count=cursor.rowcount if cursor.rowcount != -1 else 0)

        return self._run_with_connection(operation)

    def close(self) -> None:
        self._drop_connection()


def create_database_client(
    config: DatabaseConnectionConfig,
    *,
    name: str | None = None,
) -> DatabaseClient:
    normalized = normalize_database_config(dict(config))
    sql_dir = str((Path.cwd() / normalized["sqlDir"]).resolve()) if normalized.get("sqlDir") else None
    logger.info(
        "uns-database - Creating database client: %s (%s)",
        name or "unnamed",
        normalized["dialect"],
    )
    dialect = normalized["dialect"]
    if dialect == "sqlite":
        return _DatabaseClientImpl(lambda: _SqliteAdapter(normalized), dialect, name=name, sql_dir=sql_dir)
    if dialect == "pg":
        return _DatabaseClientImpl(lambda: _PgAdapter(normalized), dialect, name=name, sql_dir=sql_dir)
    if dialect == "oracle":
        return _DatabaseClientImpl(lambda: _OracleAdapter(normalized), dialect, name=name, sql_dir=sql_dir)
    raise ValueError(f"Unsupported database dialect: {dialect}")


class DatabaseManager:
    def __init__(self) -> None:
        self._configs: dict[str, DatabaseConnectionConfig] = {}
        self._clients: dict[str, DatabaseClient] = {}

    def register(self, name: str, config: DatabaseConnectionConfig) -> "DatabaseManager":
        self._configs[name] = config
        if name in self._clients:
            self._clients[name].close()
            del self._clients[name]
        logger.info("uns-database - Registered database: %s (%s)", name, config["dialect"])
        return self

    def has(self, name: str) -> bool:
        return name in self._configs

    def get_names(self) -> list[str]:
        return list(self._configs)

    def get(self, name: str) -> DatabaseClient:
        if name in self._clients:
            return self._clients[name]
        config = self._configs.get(name)
        if config is None:
            raise ValueError(f"Database connection '{name}' is not configured.")
        client = create_database_client(config, name=name)
        self._clients[name] = client
        return client

    def close(self, name: str) -> None:
        client = self._clients.get(name)
        if client is not None:
            client.close()

    def close_all(self) -> None:
        for client in list(self._clients.values()):
            client.close()


_default_database_manager = DatabaseManager()


def register_database(name: str, config: DatabaseConnectionConfig) -> DatabaseManager:
    return _default_database_manager.register(name, config)


def get_database_manager() -> DatabaseManager:
    return _default_database_manager


def get_database(name: str) -> DatabaseClient:
    return _default_database_manager.get(name)
