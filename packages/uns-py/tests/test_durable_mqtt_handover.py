from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
from pathlib import Path

import aiomqtt
import pytest

from uns_kit import ApplicationHandoverState, AsyncUnsClient, ClientError, DurableInputJournal, DurableMqttReceiver, JournalHandover, UnsProxyProcess
from test_managed_input_broker import broker as broker_fixture


@pytest.fixture
def broker(tmp_path):
    yield from broker_fixture.__wrapped__(tmp_path)


def storage(tmp_path, *, create=False, max_records=20000):
    return DurableInputJournal(tmp_path.resolve() / "durable", journal_id="example-input", contract="transform-v1", create=create, max_records=max_records)


async def until(predicate, timeout=4):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(wait(), timeout)


@pytest.mark.asyncio
async def test_real_capacity_disconnect_does_not_ack_and_resume_recovers_original(broker, tmp_path):
    journal = storage(tmp_path, create=True, max_records=1)
    receiver = DurableMqttReceiver(journal, "127.0.0.1", ["example/capacity"], port=broker.port, client_id="capacity-receiver")
    try:
        await receiver.start()
        async with aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await publisher.publish("example/capacity", b"first", qos=1)
            await until(lambda: journal.status()["pending"] == 1)
            await publisher.publish("example/capacity", b"second", qos=1)
            await until(lambda: receiver.status()["paused"])
        journal.acquire_consumer("consumer")
        first = journal.take("consumer")
        assert first.payload == b"first"
        journal.settle("consumer", first.id, confirmed=True)
        await receiver.resume()
        await until(lambda: journal.status()["pending"] == 1)
        second = journal.take("consumer")
        assert second.payload == b"second" and second.id > first.id
        journal.settle("consumer", second.id, confirmed=True)
        journal.release_consumer("consumer")
    finally:
        await receiver.stop()
        journal.close()


@pytest.mark.asyncio
async def test_real_receiver_crash_after_commit_before_ack_deduplicates_redelivery(broker, tmp_path):
    journal = storage(tmp_path, create=True)
    journal.close()
    ready = tmp_path / "receiver-ready"
    code = """
import asyncio, os, sys
from pathlib import Path
from uns_kit import DurableInputJournal, DurableMqttReceiver
async def main():
    journal = DurableInputJournal(sys.argv[1], journal_id='example-input', contract='transform-v1')
    original = journal.admit
    def crash_after_commit(*args, **kwargs):
        original(*args, **kwargs)
        os._exit(19)
    journal.admit = crash_after_commit
    receiver = DurableMqttReceiver(journal, '127.0.0.1', ['example/crash'], port=int(sys.argv[2]), client_id='crash-receiver')
    await receiver.start()
    Path(sys.argv[3]).write_text('ready')
    await asyncio.Event().wait()
asyncio.run(main())
"""
    child = subprocess.Popen([sys.executable, "-c", code, str(journal.directory), str(broker.port), str(ready)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")})
    receiver = None
    try:
        await until(lambda: ready.exists())
        async with aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await publisher.publish("example/crash", b"committed-original", qos=1)
        await until(lambda: child.poll() is not None)
        assert child.returncode == 19
        journal = storage(tmp_path)
        assert journal.status()["pending"] == 1
        receiver = DurableMqttReceiver(journal, "127.0.0.1", ["example/crash"], port=broker.port, client_id="crash-receiver")
        admissions = []
        admit = journal.admit
        def observe(*args, **kwargs):
            admissions.append(kwargs.get("duplicate"))
            return admit(*args, **kwargs)
        journal.admit = observe
        await receiver.start()
        await until(lambda: bool(admissions))
        assert admissions == [True] and journal.status()["pending"] == 1
        journal.acquire_consumer("consumer")
        record = journal.take("consumer")
        assert record.id == 1 and record.payload == b"committed-original"
        journal.settle("consumer", record.id, confirmed=True)
        journal.release_consumer("consumer")
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=3)
        if receiver is not None:
            await receiver.stop()
        journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("topic,revision", [("example/inspection/frames", "frame-window-v1"), ("warehouse/pump-7/pressure", "pressure-window-v3")])
async def test_actual_hot_handover_retains_continuous_input_http_and_application_state(broker, tmp_path, topic, revision):
    admission = storage(tmp_path, create=True)
    receiver = DurableMqttReceiver(admission, "127.0.0.1", [topic], port=broker.port, client_id="hot-receiver")
    await receiver.start()
    entered, release = asyncio.Event(), asyncio.Event()
    http_entered, http_release = threading.Event(), threading.Event()
    seen = []
    imported = []

    async def make_owner(owner_id):
        journal = storage(tmp_path)
        runtime = UnsProxyProcess("127.0.0.1", {"processName": revision, "port": broker.port, "managedOwnership": True})
        await runtime.start()
        output = await runtime.create_uns_mqtt_proxy("127.0.0.1", "output", uns_parameters={"publishQos": 1, "publishConcurrency": 1})
        data = {"total": 0}
        async def handle(record):
            if owner_id == "source":
                entered.set()
                await release.wait()
            data["total"] += int(record.payload)
            seen.append((owner_id, record.id))
            await output.publish_message(topic + "/derived", str(data["total"]))
        source = runtime.create_journal_input(journal, handle, owner_id=owner_id)
        api = await runtime.create_api_proxy("api")
        async def read(event):
            async def work():
                http_entered.set()
                await asyncio.to_thread(http_release.wait, 3)
            await api.run_read_only_request(event, work)
            event.res.json({"ok": True})
        api.event.on("apiGetEvent", read)
        await api.get("example/service", "asset", "reader", "main", "history")
        def validate(value):
            if type(value) is not dict or set(value) != {"total"} or type(value["total"]) is not int:
                raise ValueError("Invalid accumulator")
        def restore(value):
            imported.append((owner_id, value["total"]))
            data.update(value)
        handover = JournalHandover(runtime, source, proxy_contracts={"output": revision}, application_state=ApplicationHandoverState(revision, lambda: data, validate, restore))
        return runtime, journal, source, api, handover, data

    a = await make_owner("source")
    b = await make_owner("target")
    try:
        await a[4].start()
        async with aiomqtt.Client("127.0.0.1", broker.port) as observer, aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await observer.subscribe(topic + "/derived", qos=1)
            await publisher.publish(topic, b"1", qos=1)
            await asyncio.wait_for(entered.wait(), 2)
            path = "example/service/asset/reader/main/history"
            async with AsyncUnsClient(f"http://127.0.0.1:{a[3]._port}") as client:
                http = asyncio.create_task(client.get(path, authorize=False))
                assert await asyncio.to_thread(http_entered.wait, 2)
                offer = b[4].offer("hot-move-1")
                with pytest.raises(TimeoutError):
                    await a[4].prepare(offer, timeout=0.01)
                async with AsyncUnsClient(f"http://127.0.0.1:{a[3]._port}") as separate_client:
                    with pytest.raises(ClientError) as rejected:
                        await separate_client.get(path, authorize=False)
                assert rejected.value.status_code == 503
                # Independent receiver keeps committing while neither business
                # owner admits new work and the old HTTP request is draining.
                await publisher.publish(topic, b"2", qos=1)
                await publisher.publish(topic, b"3", qos=1)
                await until(lambda: admission.status()["pending"] == 2)
                release.set()
                await asyncio.sleep(0.02)
                assert a[4].inspect_operation("hot-move-1")["phase"] == "preparing"
                http_release.set()
                assert (await http).json() == {"ok": True}
                receipt = await a[4].prepare(offer)
                assert not a[0].is_active() and not b[0].is_active()
                await b[4].activate(receipt)
                await b[4].activate(receipt)
                await until(lambda: b[5]["total"] == 6)
                await b[2]._worker.run(lambda: None)
                received = [bytes((await asyncio.wait_for(observer.messages.__anext__(), 3)).payload) for _ in range(3)]
                assert received == [b"1", b"3", b"6"]
                assert imported == [("target", 1)] and seen == [("source", 1), ("target", 2), ("target", 3)]
                await a[4].acknowledge(receipt)
                assert a[4].inspect_operation("hot-move-1")["phase"] == "completed"
    finally:
        release.set()
        http_release.set()
        for runtime, journal, source, api, handover, data in (a, b):
            await runtime.stop()
            journal.close()
        await receiver.stop()
        admission.close()
