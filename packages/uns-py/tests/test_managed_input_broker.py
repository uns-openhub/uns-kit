from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from types import SimpleNamespace

import aiomqtt
import pytest

from uns_kit import UnsProxyProcess, UnsMqttProxy


@pytest.fixture
def broker(tmp_path):
    executable = shutil.which("mosquitto") or "/usr/local/sbin/mosquitto"
    if not shutil.which(executable):
        pytest.skip("A local mosquitto binary is required for broker acceptance")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = tmp_path / "mosquitto.conf"
    config.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n")
    with (tmp_path / "broker.log").open("w+") as log:
        process = subprocess.Popen([executable, "-c", str(config)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 3
            while True:
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        pytest.fail("Local test broker did not start")
                    time.sleep(0.01)
            def restart():
                nonlocal process
                process.terminate()
                process.wait(timeout=3)
                process = subprocess.Popen([executable, "-c", str(config)], stdout=log, stderr=log)
                deadline = time.monotonic() + 3
                while True:
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                            return
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Local broker restart failed")
                        time.sleep(0.01)
            yield SimpleNamespace(port=port, restart=restart)
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["example/inspection/frames", "warehouse/pump-7/pressure"])
async def test_actual_broker_process_stop_drains_admitted_input_before_output_close(broker, source) -> None:
    process = UnsProxyProcess("127.0.0.1", {"processName": "broker-fixture", "port": broker.port, "managedOwnership": True})
    await process.start()
    output = await process.create_uns_mqtt_proxy("127.0.0.1", "output", uns_parameters={"maxPendingPublishes": 1, "publishConcurrency": 1})
    input = await process.create_uns_mqtt_proxy("127.0.0.1", "input", uns_parameters={"managedInput": True, "mqttSubToTopics": [source], "maxPendingInputs": 4})
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []
    destination = source + "/derived"

    async def handle(message):
        seen.append(bytes(message.payload))
        entered.set()
        await release.wait()
        await output.publish_message(destination, message.payload)

    input.set_input_handler(handle)
    process.set_active(True)
    try:
        await input.start_input()
        assert input.get_input_readiness()
        async with aiomqtt.Client("127.0.0.1", broker.port) as observer, aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await observer.subscribe(destination, qos=1)
            await publisher.publish(source, b"first", qos=1)
            await asyncio.wait_for(entered.wait(), 2)
            await publisher.publish(source, b"second", qos=1)
            async def two_admitted():
                while input._managed_input.pending != 2:
                    await asyncio.sleep(0.001)
            await asyncio.wait_for(two_admitted(), 2)
            stopping = asyncio.create_task(process.stop(timeout=2))
            await asyncio.sleep(0.02)
            assert not stopping.done() and seen == [b"first"]
            assert not input.get_input_readiness()
            release.set()
            received = [bytes((await asyncio.wait_for(observer.messages.__anext__(), 2)).payload) for _ in range(2)]
            await stopping
            assert received == [b"first", b"second"] and seen == received
            # A stopped subscriber does not execute a new broker publication.
            await publisher.publish(source, b"after-stop", qos=1)
            await asyncio.sleep(0.02)
            assert seen == received
        await process.stop()
    finally:
        release.set()
        await process.stop()


@pytest.mark.asyncio
async def test_actual_reconnect_preserves_admitted_work_without_replay(broker) -> None:
    input = UnsMqttProxy("127.0.0.1", port=broker.port, process_name="recovery", instance_name="input", managed_input=True, max_pending_inputs=4, reconnect_interval=0.01)
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []

    async def handle(message):
        entered.set()
        await release.wait()
        effects.append(bytes(message.payload))

    input.set_input_handler(handle)
    await input.connect()
    try:
        await input.start_input("example/recovery")
        async with aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await publisher.publish("example/recovery", b"old-one", qos=1)
            await asyncio.wait_for(entered.wait(), 2)
            await publisher.publish("example/recovery", b"old-two", qos=1)
            while input._managed_input.pending != 2:
                await asyncio.sleep(0.001)
        old_session = input._managed_input._session
        await asyncio.to_thread(broker.restart)
        async def ready_again():
            while not input.get_input_readiness() or input._managed_input._session is old_session:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(ready_again(), 3)
        async with aiomqtt.Client("127.0.0.1", broker.port) as publisher:
            await publisher.publish("example/recovery", b"new-three", qos=1)
            while input._managed_input.pending != 3:
                await asyncio.sleep(0.001)
        release.set()
        await input.close()
        assert effects == [b"old-one", b"old-two", b"new-three"]
    finally:
        release.set()
        await input.close()
