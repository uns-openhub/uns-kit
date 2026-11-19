from __future__ import annotations

from .client import DatabaseManager, create_database_client, get_database, get_database_manager, register_database
from .async_client import AsyncDatabaseClient, create_async_database_client
from .params import ORACLE_IN_LIST_LIMIT, compile_named_params
from .schema import (
    DatabaseConnectionConfig,
    OracleDatabaseConfig,
    PostgresDatabaseConfig,
    SqliteDatabaseConfig,
    database_connection_schema,
    database_project_extras_schema,
    databases_config_schema,
    normalize_database_config,
    oracle_database_schema,
    postgres_database_schema,
    sqlite_database_schema,
)
from .sql import load_sql_file, resolve_sql_file_path
from .types import CompiledSqlStatement, DatabaseClient, DatabaseDialect, DatabaseExecuteResult, DatabaseQueryResult, SqlParams

__all__ = [
    "DatabaseDialect",
    "SqlParams",
    "CompiledSqlStatement",
    "DatabaseQueryResult",
    "DatabaseExecuteResult",
    "DatabaseClient",
    "AsyncDatabaseClient",
    "create_async_database_client",
    "PostgresDatabaseConfig",
    "SqliteDatabaseConfig",
    "OracleDatabaseConfig",
    "DatabaseConnectionConfig",
    "postgres_database_schema",
    "sqlite_database_schema",
    "oracle_database_schema",
    "database_connection_schema",
    "databases_config_schema",
    "database_project_extras_schema",
    "normalize_database_config",
    "ORACLE_IN_LIST_LIMIT",
    "compile_named_params",
    "load_sql_file",
    "resolve_sql_file_path",
    "create_database_client",
    "DatabaseManager",
    "register_database",
    "get_database_manager",
    "get_database",
]
