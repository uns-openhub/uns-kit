from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import threading
import sqlite3
from pathlib import Path

import pytest

from uns_kit import DurableInputJournal, DurableMqttReceiver, JournalCapacityError, JournalError, JournalInput, JournalOwnershipError
from uns_kit.core.durable_journal import bounded_json


def open_journal(tmp_path, **options):
    return DurableInputJournal(tmp_path.resolve() / "journal", journal_id="inspection-frames", contract="frames-v1", **options)


def test_commit_reopen_and_delivery_rededup_without_payload_loss(tmp_path):
    journal = open_journal(tmp_path, create=True)
    journal.acquire_receiver("receiver-1")
    record_id = journal.admit("inspection/frames", b"original\x00\xff", mid=7)
    assert journal.admit("inspection/frames", b"original\x00\xff", mid=7, duplicate=True) == record_id
    journal.close()
    reopened = open_journal(tmp_path)
    reopened.acquire_consumer("worker-a")
    record = reopened.take("worker-a")
    assert record.id == record_id and record.payload == b"original\x00\xff" and record.received_at > 0
    reopened.settle("worker-a", record_id, confirmed=True)
    assert reopened.status()["pending"] == 0
    reopened.acquire_receiver("receiver-1")
    assert reopened.admit("inspection/frames", b"original\x00\xff", mid=7, duplicate=True) == record_id
    assert reopened.status()["pending"] == 0
    with pytest.raises(JournalError, match="Ambiguous"):
        reopened.admit("inspection/frames", b"different", mid=7, duplicate=True)
    assert reopened.admit("inspection/frames", b"different", mid=7) > record_id
    reopened.release_consumer("worker-a")
    reopened.close()


@pytest.mark.parametrize("capacity", [{"max_records": 1}, {"max_bytes": 6}, {"max_message_bytes": 6}])
def test_capacity_never_prunes_pending_input(tmp_path, capacity):
    journal = open_journal(tmp_path, create=True, **capacity)
    journal.acquire_receiver("receiver")
    journal.admit("x", b"12345", mid=1)
    with pytest.raises(JournalCapacityError):
        journal.admit("x", b"678901", mid=2)
    assert journal.status()["pending"] == 1 and journal.status()["payload_bytes"] == 6
    journal.close()


def test_identity_and_kernel_roles_are_exclusive_across_handles(tmp_path):
    one = open_journal(tmp_path, create=True)
    two = open_journal(tmp_path)
    one.acquire_receiver("receiver")
    one.acquire_consumer("worker")
    with pytest.raises(JournalOwnershipError):
        two.acquire_receiver("receiver")
    with pytest.raises(JournalOwnershipError):
        two.acquire_consumer("worker")
    one.release_receiver()
    with pytest.raises(JournalOwnershipError, match="identity"):
        two.acquire_receiver("different-receiver")
    two.acquire_receiver("receiver")
    one.release_consumer("worker")
    two.acquire_consumer("worker-b")
    two.release_consumer("worker-b")
    one.close()
    two.close()
    with pytest.raises(JournalError, match="differ"):
        DurableInputJournal(tmp_path.resolve() / "journal", journal_id="other", contract="frames-v1")


def test_private_nonsymlink_storage_and_unknown_protocol_fail_closed(tmp_path):
    journal = open_journal(tmp_path, create=True)
    journal._db.execute("UPDATE metadata SET value='999' WHERE key='protocol'")
    journal.close()
    with pytest.raises(JournalError, match="protocol"):
        open_journal(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "journal", target_is_directory=True)
    with pytest.raises(JournalError, match="symlink"):
        DurableInputJournal(link, journal_id="inspection-frames", contract="frames-v1")
    public = tmp_path / "public"
    public.mkdir(mode=0o755)
    with pytest.raises(JournalError, match="private"):
        DurableInputJournal(public, journal_id="a", contract="b", create=True)


def test_foreign_database_is_rejected_without_creating_a_wal(tmp_path):
    directory = tmp_path.resolve() / "journal"
    directory.mkdir(mode=0o700)
    database = directory / "input.sqlite3"
    db = sqlite3.connect(database)
    db.execute("CREATE TABLE foreign_data(value TEXT)")
    db.close()
    database.chmod(0o600)
    original = database.read_bytes()
    with pytest.raises(JournalError, match="supported"):
        open_journal(tmp_path)
    assert database.read_bytes() == original
    assert not Path(str(database) + "-wal").exists()


@pytest.mark.parametrize("value", [{"x": float("nan")}, {"x": {1, 2}}, {"__proto__": 1}, "x" * (768 * 1024)])
def test_state_is_bounded_plain_json(value):
    with pytest.raises((ValueError, JournalError)):
        bounded_json(value)


def test_sigkill_retains_running_original_and_requires_explicit_resolution(tmp_path):
    journal = open_journal(tmp_path, create=True)
    journal.acquire_receiver("receiver")
    record_id = journal.admit("inspection/frames", b"do-not-replay", mid=1)
    journal.release_receiver()
    marker = tmp_path / "running"
    code = """
import signal, sys
from pathlib import Path
from uns_kit import DurableInputJournal
j = DurableInputJournal(sys.argv[1], journal_id='inspection-frames', contract='frames-v1')
j.acquire_consumer('launch-a')
assert j.take('launch-a') is not None
Path(sys.argv[2]).write_text('running')
signal.pause()
"""
    child = subprocess.Popen([sys.executable, "-c", code, str(journal.directory), str(marker)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")})
    try:
        deadline = time.monotonic() + 5
        while not marker.exists():
            assert child.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.01)
        child.kill()
        child.wait(timeout=3)
        with pytest.raises(JournalError, match="unconfirmed"):
            journal.acquire_consumer("launch-a")
        journal.reconcile(record_id, resolution_id="review-1", decision="retry")
        journal.reconcile(record_id, resolution_id="review-1", decision="retry")
        with pytest.raises(JournalError, match="reused"):
            journal.reconcile(record_id, resolution_id="review-1", decision="completed")
        journal.acquire_consumer("launch-a")
        original = journal.take("launch-a")
        assert original.id == record_id and original.payload == b"do-not-replay"
        journal.settle("launch-a", record_id, confirmed=True)
        journal.release_consumer("launch-a")
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        journal.close()


@pytest.mark.asyncio
async def test_timeout_and_cancel_waiters_preserve_callback_then_checkpoint(tmp_path):
    journal = open_journal(tmp_path, create=True)
    journal.acquire_receiver("receiver")
    journal.admit("inspection/frames", b"accepted", mid=1)
    entered, release, flushed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def handle(record):
        calls.append(record.id)
        entered.set()
        await release.wait()

    async def flush():
        assert journal.status()["running"] == 1
        flushed.set()

    source = JournalInput(journal, handle, owner_id="worker", flush=flush)
    await source.start()
    await asyncio.wait_for(entered.wait(), 2)
    with pytest.raises(TimeoutError):
        await source.drain(0.01)
    waiter = asyncio.create_task(source.drain())
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    assert not source._task.done() and journal.status()["running"] == 1
    release.set()
    await source.stop()
    assert flushed.is_set() and calls == [1] and journal.status()["running"] == 0
    journal.close()


@pytest.mark.asyncio
async def test_unknown_output_does_not_checkpoint_or_replay(tmp_path):
    journal = open_journal(tmp_path, create=True)
    journal.acquire_receiver("receiver")
    record_id = journal.admit("inspection/frames", b"original", mid=1)
    called = asyncio.Event()

    async def handle(record):
        called.set()

    async def flush():
        raise RuntimeError("unconfirmed publication")

    source = JournalInput(journal, handle, owner_id="worker", flush=flush)
    await source.start()
    await asyncio.wait_for(called.wait(), 2)
    for _ in range(2):
        with pytest.raises(JournalError, match="unconfirmed"):
            await source.stop()
    assert journal.status()["unknown"] == 1
    journal.close()  # Test teardown, not a clean application drain.
    await source._worker.close()
    reopened = open_journal(tmp_path)
    with pytest.raises(JournalError, match="unconfirmed"):
        reopened.acquire_consumer("worker")
    reopened.reconcile(record_id, resolution_id="review-completed", decision="completed")
    reopened.acquire_consumer("worker")
    assert reopened.take("worker") is None
    reopened.release_consumer("worker")
    reopened.close()


@pytest.mark.asyncio
async def test_cancelled_consumer_start_retains_claim_until_stop_awaits_it(tmp_path):
    journal = open_journal(tmp_path, create=True)
    entered, release = threading.Event(), threading.Event()
    acquire = journal.acquire_consumer
    def slow_claim(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return acquire(*args, **kwargs)
    journal.acquire_consumer = slow_claim
    async def handler(record):
        pytest.fail("Stop must fence input before a late claim finishes")
    source = JournalInput(journal, handler, owner_id="worker")
    start = asyncio.create_task(source.start())
    assert await asyncio.to_thread(entered.wait, 2)
    start.cancel()
    await asyncio.gather(start, return_exceptions=True)
    stop = asyncio.create_task(source.stop())
    await asyncio.sleep(0.01)
    assert not stop.done()
    release.set()
    await stop
    other = open_journal(tmp_path)
    other.acquire_consumer("new-owner")
    other.release_consumer("new-owner")
    other.close()
    journal.close()


@pytest.mark.asyncio
async def test_cancelled_receiver_connect_does_not_release_its_session_early(tmp_path):
    from types import SimpleNamespace
    journal = open_journal(tmp_path, create=True)
    receiver = DurableMqttReceiver(journal, "fixture", ["example/source"], client_id="receiver")
    entered, release = threading.Event(), threading.Event()
    def connect(*args):
        entered.set()
        assert release.wait(3)
    receiver._client = SimpleNamespace(connect=connect, loop_start=receiver._ready.set,
        loop_stop=lambda: None, disconnect=lambda: None)
    start = asyncio.create_task(receiver.start())
    assert await asyncio.to_thread(entered.wait, 2)
    start.cancel()
    await asyncio.gather(start, return_exceptions=True)
    other = open_journal(tmp_path)
    with pytest.raises(JournalOwnershipError):
        other.acquire_receiver("receiver")
    stop = asyncio.create_task(receiver.stop())
    await asyncio.sleep(0.01)
    assert not stop.done()
    release.set()
    await stop
    other.acquire_receiver("receiver")
    other.release_receiver()
    other.close()
    journal.close()


@pytest.mark.asyncio
async def test_qos_one_output_never_replays_an_unknown_broker_attempt():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import aiomqtt
    from uns_kit import UnsMqttProxy
    proxy = UnsMqttProxy("fixture", process_name="one-attempt", instance_name="output", publish_qos=1)
    transport = SimpleNamespace(publish=AsyncMock(side_effect=aiomqtt.MqttError("ACK unavailable")))
    proxy.client._client = transport
    proxy.client._connected.set()
    with pytest.raises(aiomqtt.MqttError):
        await proxy.client.publish_once_raw("example/output", b"effect", qos=1)
    transport.publish.assert_awaited_once_with("example/output", b"effect", qos=1, retain=False)


def test_process_rejects_qos_zero_output_before_admitting_business_input(tmp_path):
    from uns_kit import UnsMqttProxy, UnsProxyProcess
    journal = open_journal(tmp_path, create=True)
    runtime = UnsProxyProcess("fixture", {"processName": "qos-check", "managedOwnership": True})
    runtime._proxies.append(UnsMqttProxy("fixture", process_name="qos-check", instance_name="unsafe"))
    async def handle(record):
        pytest.fail("Invalid output configuration must fail before any callback")
    try:
        with pytest.raises(ValueError, match="QoS 1"):
            runtime.create_journal_input(journal, handle, owner_id="worker")
        assert runtime._journal_inputs == []
    finally:
        journal.close()
