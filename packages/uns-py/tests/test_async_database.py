from __future__ import annotations

import asyncio
import sys
import threading
from types import SimpleNamespace

import pytest

from uns_kit import AsyncClientClosedError, create_async_database_client


@pytest.mark.asyncio
async def test_actual_sqlite_named_params_files_and_concurrent_admission(tmp_path) -> None:
    (tmp_path / "read.sql").write_text("select id, value from measurements where id in (:ids) order by id")
    async with create_async_database_client({"dialect": "sqlite", "filename": str(tmp_path / "db.sqlite"), "sqlDir": str(tmp_path)}) as client:
        await client.execute("create table measurements(id integer primary key, value text)")
        await asyncio.gather(*(client.execute("insert into measurements values (:id, :value)", {"id": index, "value": str(index)}) for index in range(12)))
        result = await client.query_file("read.sql", {"ids": [1, 5, 9]})
        assert result.row_count == 3
        assert result.rows == [{"id": index, "value": str(index)} for index in [1, 5, 9]]
    with pytest.raises(AsyncClientClosedError):
        await client.query("select 1")
    # Opening the committed database with a separate owner preserves the rows.
    async with create_async_database_client({"dialect": "sqlite", "filename": str(tmp_path / "db.sqlite")}) as reader:
        assert (await reader.query("select count(*) as n from measurements")).rows == [{"n": 12}]


@pytest.mark.asyncio
async def test_sqlite_io_does_not_block_loop_and_cancel_does_not_close_connection_early() -> None:
    client = create_async_database_client({"dialect": "sqlite", "filename": ":memory:"})
    entered, release = threading.Event(), threading.Event()

    def hold():
        entered.set()
        release.wait(2)
        return 42

    await client._worker.run(lambda: client._get_client()._get_adapter()._connection.create_function("hold", 0, hold))
    call = asyncio.create_task(client.query("select hold() as value"))
    closing = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        # Reaching this await while SQL is blocked proves the event loop is free.
        await asyncio.sleep(0)
        assert not call.done()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        closing = asyncio.create_task(client.close())
        await asyncio.sleep(0)
        assert not closing.done()
    finally:
        release.set()
        if closing is not None:
            await closing
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["pg", "oracle"])
async def test_driver_failure_never_replays_sql_and_next_request_reconnects(monkeypatch, dialect) -> None:
    connections = []
    effects = []

    class Cursor:
        rowcount = 1
        description = [SimpleNamespace(name="answer")] if dialect == "pg" else [("answer",)]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            if sql.startswith("insert"):
                effects.append(sql)

        def fetchall(self):
            return [(42,)]

    class Connection:
        closed = False

        def cursor(self):
            return Cursor()

        def commit(self):
            raise ConnectionError("server closed the connection unexpectedly after possible commit")

        def close(self):
            self.closed = True

    def connect(**kwargs):
        connection = Connection()
        connections.append(connection)
        return connection

    monkeypatch.setitem(sys.modules, "psycopg" if dialect == "pg" else "oracledb", SimpleNamespace(connect=connect))
    config = {"dialect": dialect, "host": "unused", "database": "fixture", "user": "fixture", "serviceName": "fixture"}
    async with create_async_database_client(config) as client:
        with pytest.raises(ConnectionError):
            await client.execute("insert into target values (:value)", {"value": 1})
        assert len(effects) == 1 and len(connections) == 1
        assert connections[0].closed
        assert (await client.query("select 42 as answer")).rows == [{"answer": 42}]
        assert len(connections) == 2
    assert connections[1].closed
