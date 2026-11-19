from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from pathlib import Path

import pytest

from uns_kit import ApplicationHandoverState, DurableInputJournal, JournalError, JournalHandover, UnsMqttProxy, UnsProxyProcess
from uns_kit.core.uns_mqtt_proxy import LastValueEntry


def journal(tmp_path, create=False):
    return DurableInputJournal(tmp_path.resolve() / "journal", journal_id="example-stream", contract="transform-v1", create=create)


def owner(storage, owner_id, *, state=None, handler=None, contract="window-v1", import_hook=None):
    runtime = UnsProxyProcess("fixture", {"processName": "generic-transformer", "managedOwnership": True})
    runtime._client = SimpleNamespace(close=AsyncMock())
    runtime._status_monitor = SimpleNamespace(stop=AsyncMock())
    output = UnsMqttProxy("fixture", process_name="generic-transformer", instance_name="output", publish_qos=1, strict_publish_drain=True)
    output.client.close = AsyncMock()
    runtime._proxies.append(output)
    data = state if state is not None else {"rows": [], "total": 0}

    async def default_handler(record):
        data["rows"].append(record.payload.decode())

    source = runtime.create_journal_input(storage, handler or default_handler, owner_id=owner_id)

    def validate(value):
        if type(value) is not dict or set(value) != {"rows", "total"} or type(value["rows"]) is not list or type(value["total"]) is not int:
            raise JournalError("Invalid business state")

    def restore(value):
        if import_hook is not None:
            return import_hook(value)
        data.clear()
        data.update(value)

    handover = JournalHandover(runtime, source, proxy_contracts={"output": "raw-v1"}, application_state=ApplicationHandoverState(contract, lambda: data, validate, restore))
    return SimpleNamespace(runtime=runtime, source=source, output=output, handover=handover, data=data)


async def cleanup(*owners):
    for item in owners:
        try:
            await item.runtime.stop()
        finally:
            item.source.journal.close()
            await item.source._worker.close()


@pytest.mark.asyncio
async def test_contract_mismatch_refuses_before_release(tmp_path):
    a = owner(journal(tmp_path, True), "source")
    b = owner(journal(tmp_path), "target", contract="different-v1")
    try:
        await a.handover.start()
        with pytest.raises(JournalError, match="differ"):
            await a.handover.prepare(b.handover.offer("move-1"))
        assert a.runtime.is_active() and a.handover.inspect_operation("move-1") is None
        assert not a.source._releasing
    finally:
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_hot_state_transfer_is_bound_restored_once_and_source_cannot_reactivate(tmp_path):
    imported = []
    data = {"rows": ["old"], "total": 17}
    a = owner(journal(tmp_path, True), "source", state=data)
    b = owner(journal(tmp_path), "target", import_hook=lambda value: imported.append(value))
    try:
        await a.handover.start()
        now = datetime.now(timezone.utc)
        a.output._last_values["plant/counter"] = LastValueEntry(17, None, now)
        a.output._sequence_ids["plant/counter"] = 91
        offer = b.handover.offer("move-1")
        receipt = await a.handover.prepare(offer)
        assert receipt == await a.handover.prepare(offer)
        assert not a.runtime.is_active() and not b.runtime.is_active()
        with pytest.raises(RuntimeError):
            a.runtime.set_active(True)
        with pytest.raises(RuntimeError, match="output admission"):
            await a.output.publish_message("unrelated", b"cannot-write")
        bad = {**receipt, "stateDigest": "0" * 64}
        with pytest.raises(JournalError):
            await b.handover.activate(bad)
        await b.handover.activate(receipt)
        await b.handover.activate(receipt)
        assert b.runtime.is_active() and imported == [data]
        assert b.output._last_values["plant/counter"] == LastValueEntry(17, None, now)
        assert b.output._sequence_ids == {"plant/counter": 91}
        await a.handover.acknowledge(receipt)
        await a.handover.acknowledge(receipt)
        assert a.handover.inspect_operation("move-1")["phase"] == "completed"
        assert a.runtime._stop_requested and b.runtime.is_active()
    finally:
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_timeout_preserves_same_preparation_and_pending_journal_work(tmp_path):
    storage = journal(tmp_path, True)
    storage.acquire_receiver("receiver")
    storage.admit("unrelated/source", b"first", mid=1)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def handle(record):
        calls.append(record.id)
        entered.set()
        await release.wait()

    a = owner(storage, "source", handler=handle)
    b = owner(journal(tmp_path), "target")
    try:
        await a.handover.start()
        await asyncio.wait_for(entered.wait(), 2)
        offer = b.handover.offer("move-2")
        with pytest.raises(TimeoutError):
            await a.handover.prepare(offer, timeout=0.01)
        assert not a.runtime.is_active() and not b.runtime.is_active()
        storage.admit("unrelated/source", b"second", mid=2)
        with pytest.raises(JournalError):
            await b.handover.start()
        assert calls == [1]
        release.set()
        receipt = await a.handover.prepare(offer)
        await b.handover.activate(receipt)
        async def caught_up():
            while storage.status()["pending"] or storage.status()["running"]:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(caught_up(), 2)
        assert b.data["rows"] == ["second"] and calls == [1]
        await a.handover.acknowledge(receipt)
    finally:
        release.set()
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_partial_import_is_durable_unknown_and_is_never_retried(tmp_path):
    imports = []

    async def broken(value):
        imports.append(value)
        raise RuntimeError("uncertain import")

    a = owner(journal(tmp_path, True), "source")
    b = owner(journal(tmp_path), "target", import_hook=broken)
    try:
        await a.handover.start()
        receipt = await a.handover.prepare(b.handover.offer("move-3"))
        for _ in range(2):
            with pytest.raises(RuntimeError, match="uncertain import"):
                await b.handover.activate(receipt)
        assert len(imports) == 1 and not b.runtime.is_active()
        assert a.handover.inspect_operation("move-3")["phase"] == "applying"
        with pytest.raises(JournalError, match="unconfirmed"):
            await a.handover.acknowledge(receipt)
        await b.runtime.stop()
        reopened = owner(journal(tmp_path), "target")
        try:
            with pytest.raises(JournalError, match="reconcile"):
                await reopened.handover.activate(receipt)
            assert not reopened.runtime.is_active()
        finally:
            await cleanup(reopened)
    finally:
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_operation_id_and_target_launch_cannot_be_replaced(tmp_path):
    a = owner(journal(tmp_path, True), "source")
    b = owner(journal(tmp_path), "target")
    c = owner(journal(tmp_path), "unrelated-launch")
    try:
        await a.handover.start()
        offer = b.handover.offer("move-4")
        receipt = await a.handover.prepare(offer)
        with pytest.raises(JournalError, match="different evidence"):
            await a.handover.prepare({**offer, "target": "unrelated-launch"})
        with pytest.raises(JournalError, match="another launch"):
            await c.handover.activate(receipt)
        with pytest.raises(JournalError):
            await c.handover.start()
        await b.handover.activate(receipt)
        await a.handover.acknowledge(receipt)
    finally:
        await cleanup(a, b, c)


@pytest.mark.asyncio
async def test_prepared_source_sigkill_recovers_same_receipt_and_requires_reviewed_retirement(tmp_path):
    initial = journal(tmp_path, True)
    initial.close()
    target = owner(journal(tmp_path), "target")
    offer_path = tmp_path / "offer.json"
    offer_path.write_text(json.dumps(target.handover.offer("process-move")))
    ready = tmp_path / "prepared"
    code = """
import asyncio, json, signal, sys
from pathlib import Path
from test_journal_handover import journal, owner
async def main():
    source = owner(journal(Path(sys.argv[1])), 'source')
    source.data['total'] = 123
    await source.handover.start()
    receipt = await source.handover.prepare(json.loads(Path(sys.argv[2]).read_text()))
    Path(sys.argv[3]).write_text(json.dumps(receipt))
    signal.pause()
asyncio.run(main())
"""
    package = Path(__file__).resolve().parents[1]
    child = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), str(offer_path), str(ready)],
        env={**os.environ, "PYTHONPATH": os.pathsep.join((str(package / "src"), str(package / "tests")))})
    try:
        async def prepared():
            while not ready.exists():
                assert child.poll() is None
                await asyncio.sleep(0.01)
        await asyncio.wait_for(prepared(), 5)
        receipt = await target.handover.recover_receipt("process-move")
        assert receipt == json.loads(ready.read_text())
        await target.handover.activate(receipt)
        assert target.data["total"] == 123 and target.runtime.is_active()
        with pytest.raises(JournalError, match="owned"):
            await target.handover.reconcile_source_exit(receipt, resolution_id="review-exit")
        child.kill()
        child.wait(timeout=3)
        await target.handover.reconcile_source_exit(receipt, resolution_id="review-exit")
        await target.handover.reconcile_source_exit(receipt, resolution_id="review-exit")
        assert target.handover.inspect_operation("process-move")["phase"] == "completed"
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        await cleanup(target)


@pytest.mark.asyncio
async def test_stop_during_import_waits_for_original_attempt_without_opening_input(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    async def restore(value):
        entered.set()
        await release.wait()
    a = owner(journal(tmp_path, True), "source")
    b = owner(journal(tmp_path), "target", import_hook=restore)
    try:
        await a.handover.start()
        receipt = await a.handover.prepare(b.handover.offer("stop-import"))
        activating = asyncio.create_task(b.handover.activate(receipt))
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(TimeoutError):
            await b.runtime.stop(timeout=0.01)
        b.output.client.close.assert_not_called()
        release.set()
        with pytest.raises(JournalError, match="stopped during"):
            await activating
        await b.runtime.stop()
        assert not b.runtime.is_active() and b.source._task is None
        assert a.handover.inspect_operation("stop-import")["phase"] == "applying"
    finally:
        release.set()
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_return_uses_fresh_owner_and_operation_after_confirmed_source_cleanup(tmp_path):
    a = owner(journal(tmp_path, True), "first", state={"rows": ["sample"], "total": 8})
    b = owner(journal(tmp_path), "second")
    c = owner(journal(tmp_path), "fresh-return")
    try:
        await a.handover.start()
        receipt = await a.handover.prepare(b.handover.offer("forward"))
        with pytest.raises(JournalError, match="unconfirmed"):
            await a.handover.acknowledge(receipt)
        await b.handover.activate(receipt)
        await a.handover.acknowledge(receipt)
        reverse = await b.handover.prepare(c.handover.offer("return"))
        await c.handover.activate(reverse)
        await b.handover.acknowledge(reverse)
        assert c.data == {"rows": ["sample"], "total": 8}
        assert c.runtime.is_active() and not a.runtime.is_active() and not b.runtime.is_active()
        with pytest.raises(JournalError, match="stopping"):
            await a.handover.start()
    finally:
        await cleanup(a, b, c)


@pytest.mark.asyncio
async def test_failed_source_close_can_retry_after_its_consumer_worker_closed(tmp_path):
    from uns_kit import ProcessShutdownError
    a = owner(journal(tmp_path, True), "source")
    b = owner(journal(tmp_path), "target")
    try:
        await a.handover.start()
        receipt = await a.handover.prepare(b.handover.offer("close-retry"))
        await b.handover.activate(receipt)
        a.output.client.close.side_effect = [RuntimeError("unconfirmed close"), None]
        with pytest.raises(ProcessShutdownError):
            await a.handover.acknowledge(receipt)
        assert a.source._closed and a.handover.inspect_operation("close-retry")["phase"] == "activated"
        await a.handover.acknowledge(receipt)
        assert a.output.client.close.await_count == 2
        assert a.handover.inspect_operation("close-retry")["phase"] == "completed"
    finally:
        await cleanup(a, b)


@pytest.mark.asyncio
async def test_process_cron_stays_passive_until_fenced_owner_activates(tmp_path):
    storage = journal(tmp_path, True)
    runtime = UnsProxyProcess("fixture", {"processName": "cron-owner", "managedOwnership": True})
    runtime._client = SimpleNamespace(close=AsyncMock())
    runtime._status_monitor = SimpleNamespace(stop=AsyncMock())
    cron = await runtime.create_cron_proxy("0 0 1 1 *")
    calls = []
    async def scheduled(event):
        calls.append(event)
    async def handle(record):
        pass
    cron.event.on("cronEvent", scheduled)
    source = runtime.create_journal_input(storage, handle, owner_id="cron-launch")
    handover = JournalHandover(runtime, source, proxy_contracts={})
    try:
        await cron._emit_event(event_name=None, cron_expression="0 0 1 1 *")
        assert calls == []
        await handover.start()
        await cron._emit_event(event_name=None, cron_expression="0 0 1 1 *")
        assert len(calls) == 1
        await runtime.stop()
        await cron._emit_event(event_name=None, cron_expression="0 0 1 1 *")
        assert len(calls) == 1
    finally:
        await runtime.stop()
        storage.close()
