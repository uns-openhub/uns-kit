"""Version-1 same-host Python handover over one shared durable journal.

This deliberately does not advertise the TS MQTT correlated-v2 wire capability
or the Node process-group host protocol. Kernel ownership and durable operation
phases fence both peers; no timeout authorizes a new owner.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping

from .durable_input import JournalInput
from .durable_journal import JournalCapacityError, JournalError, JournalOwnershipError, bounded_json, identifier
from .uns_mqtt_proxy import LastValueEntry


@dataclass(frozen=True)
class ApplicationHandoverState:
    contract: str
    export_state: Callable[[], Any]
    validate_state: Callable[[Any], Any]
    import_state: Callable[[Any], Any]

    def __post_init__(self) -> None:
        identifier(self.contract)
        if not all(callable(callback) for callback in (self.export_state, self.validate_state, self.import_state)):
            raise TypeError("Invalid application handover callbacks")


def create_application_handover_contract(revision: str, resources: Any) -> str:
    identifier(revision)
    if len(revision) > 60 or any(char in revision for char in ":/"):
        raise ValueError("Application revision must be at most 60 safe characters")
    return revision + "/" + hashlib.sha256(bounded_json(resources)).hexdigest()


class JournalHandover:
    """Prepare -> activate -> acknowledge, persisted under the same operation ID.

    One journal input per owner. The independent receiver can keep writing while
    source HTTP/application work drains. A whole-process failure during import
    or an unconfirmed callback requires review, not another activation attempt.
    """

    def __init__(self, runtime: Any, source: JournalInput, *, proxy_contracts: Mapping[str, str],
                 application_state: ApplicationHandoverState | None = None,
                 on_activate: Callable[[], Any] | None = None) -> None:
        if not runtime.process_parameters.managed_ownership or source not in runtime._journal_inputs:
            raise JournalError("Journal handover requires a managed process-owned journal input")
        if len(runtime._journal_inputs) != 1 or getattr(runtime, "_journal_handover", None) is not None:
            raise JournalError("Prepare one journal handover owner before activation")
        if runtime.is_active() or source._task is not None or runtime._stop_requested:
            raise JournalError("Prepare handover contracts while passive, before starting input")
        if any(proxy._managed_input is not None or proxy._input_topics for proxy in runtime._proxies):
            raise JournalError("Journal handover cannot transfer unrelated raw MQTT inputs")
        proxies = {proxy._instance_name: proxy for proxy in runtime._proxies}
        if len(proxies) != len(runtime._proxies) or set(proxies) != set(proxy_contracts):
            raise JournalError("Every output proxy requires one semantic contract")
        if len(proxies) > 64:
            raise JournalError("Handover supports at most 64 output proxies")
        for contract in proxy_contracts.values():
            identifier(contract)
        if any(proxy._publish_qos != 1 or not proxy._strict_publish_drain for proxy in proxies.values()):
            raise JournalError("Handover outputs require strict QoS 1 drain")
        if any(proxy._pending_enqueue_operations or proxy._pending_publish_completions or proxy._publish_failure is not None for proxy in proxies.values()):
            raise JournalError("Prepare empty confirmed outputs before sealing handover contracts")
        if on_activate is not None and not callable(on_activate):
            raise TypeError("Activation callback must be callable")
        self.runtime, self.source, self.journal = runtime, source, source.journal
        self._application, self._on_activate = application_state, on_activate
        self._proxies = proxies
        descriptor = {"protocol": 1, "journal": self.journal.binding,
            "application": application_state.contract if application_state else None,
            "proxies": [{"name": name, "contract": proxy_contracts[name], "host": proxy.client.host,
                "port": proxy.client.port or 1883, "tls": proxy.client.tls}
                for name, proxy in sorted(proxies.items())]}
        self._configuration = hashlib.sha256(bounded_json(descriptor)).hexdigest()
        self._tasks: dict[tuple[str, str], tuple[bytes, asyncio.Task]] = {}
        self._applied = False
        self._storage_tasks: set[asyncio.Task] = set()
        runtime._journal_handover = self
        runtime._handover_passive = True
        for proxy in proxies.values():
            proxy._handover_frozen = True

    async def _storage(self, operation: Callable[[], Any]) -> Any:
        # Control IO must outlive a stopped consumer: source cleanup/final
        # receipts may be retried after its data worker has already closed.
        if len(self._storage_tasks) >= 8:
            raise JournalCapacityError("Journal handover control IO capacity exceeded")
        task = asyncio.create_task(asyncio.to_thread(operation))
        self._storage_tasks.add(task)
        def settled(result: asyncio.Task) -> None:
            self._storage_tasks.discard(result)
            if not result.cancelled():
                result.exception()
        task.add_done_callback(settled)
        return await asyncio.shield(task)

    def _set_active(self, active: bool) -> None:
        self.runtime.active = active
        self.runtime._handover_passive = not active
        for proxy in self._proxies.values():
            proxy._handover_frozen = not active

    async def start(self) -> None:
        """Explicit cold start; never an election or timeout fallback."""
        if self.runtime._stop_requested or self.runtime._handover_released:
            raise JournalError("Owner is stopping or already released")
        await self._operation("start", self.source.owner_id, {}, self._cold_start, 30.0)

    async def _cold_start(self) -> None:
        if self._applied or self.runtime._stop_requested:
            raise JournalError("Owner cannot cold-start after activation or stop")
        await self._storage(lambda: self.journal.acquire_consumer(self.source.owner_id))
        self.source._owns = True
        try:
            await self._activate_application()
            if self.runtime._stop_requested or self.source._releasing:
                raise JournalError("Owner stopped during cold activation")
            self._set_active(True)
            self.source._start_owned()
        except BaseException:
            self._set_active(False)
            raise

    async def _activate_application(self) -> None:
        # Outputs can be used by the explicit activation hook. HTTP and input
        # remain passive until that callback has finished.
        for proxy in self._proxies.values():
            proxy._handover_frozen = False
        if self._on_activate is not None:
            value = self._on_activate()
            if inspect.isawaitable(value):
                await value

    def offer(self, operation_id: str) -> dict[str, Any]:
        identifier(operation_id)
        if self.runtime.is_active() or self.runtime._stop_requested or self.source._task is not None:
            raise JournalError("Only a prepared passive target can offer handover")
        if any(proxy._last_values or proxy._sequence_ids for proxy in self._proxies.values()):
            raise JournalError("Target calculation state must be empty before offering handover")
        return {"protocol": 1, "operationId": operation_id, "journalId": self.journal.journal_id,
            "binding": self.journal.binding, "configuration": self._configuration, "target": self.source.owner_id}

    async def _operation(self, role: str, operation_id: str, value: Any,
                         action: Callable[[], Any], timeout: float | None) -> Any:
        if timeout is not None and (isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Handover timeout must be positive finite seconds or None")
        encoded = bounded_json(value)
        key = (role, operation_id)
        previous = self._tasks.get(key)
        if previous is not None and previous[0] == encoded and role == "acknowledge" and previous[1].done() and not previous[1].cancelled() and previous[1].exception() is not None:
            # Only idempotent source cleanup is retried. Import/application
            # attempts remain latched, including across a caller timeout.
            del self._tasks[key]
            previous = None
        if previous is None:
            if len(self._tasks) >= 256:
                raise JournalError("Too many handover operations")
            task = asyncio.create_task(action())
            task.add_done_callback(lambda result: None if result.cancelled() else result.exception())
            self._tasks[key] = (encoded, task)
            if role in ("start", "prepare", "activate"):
                self.runtime._handover_action_task = task
        else:
            if previous[0] != encoded:
                raise JournalError("Handover operation was reused with different evidence")
            task = previous[1]
        result = await asyncio.wait_for(asyncio.shield(task), timeout)
        return None if result is None else json.loads(bounded_json(result))

    def _validate_offer(self, offer: Any) -> None:
        if type(offer) is not dict or set(offer) != {"protocol", "operationId", "journalId", "binding", "configuration", "target"}:
            raise JournalError("Invalid handover offer")
        identifier(offer["operationId"])
        identifier(offer["target"])
        if offer["protocol"] != 1 or offer["journalId"] != self.journal.journal_id or offer["binding"] != self.journal.binding or offer["configuration"] != self._configuration or offer["target"] == self.source.owner_id:
            raise JournalError("Target journal, endpoints or semantic contracts differ")

    async def prepare(self, offer: dict[str, Any], *, timeout: float | None = 30.0) -> dict[str, Any]:
        offer = json.loads(bounded_json(offer))
        self._validate_offer(offer)
        return await self._operation("prepare", offer["operationId"], offer, lambda: self._prepare(offer), timeout)

    async def _prepare(self, offer: dict[str, Any]) -> dict[str, Any]:
        if not self.runtime.is_active() or not self.source._owns or self.runtime._stop_requested:
            raise JournalOwnershipError("Only the live source can prepare this handover")
        # Read-only validation before release, including application-selected
        # semantic resources. The final snapshot is taken only after drain.
        preliminary = self._snapshot()
        bounded_json(preliminary, max_bytes=1024 * 1024)
        self._validate_snapshot(preliminary, empty=False)

        def reserve() -> None:
            with self.journal._mutex:
                self.journal._lock("handover-source")
                try:
                    with self.journal._transaction() as db:
                        if db.execute("SELECT count(*) FROM transfers").fetchone()[0] >= 256:
                            raise JournalError("Handover history full; review retained storage")
                        if db.execute("SELECT count(*) FROM transfers WHERE phase!='completed'").fetchone()[0]:
                            raise JournalError("An existing handover requires reconciliation")
                        if db.execute("SELECT 1 FROM transfers WHERE id=?", (offer["operationId"],)).fetchone():
                            raise JournalError("Handover operation ID was already used")
                        db.execute("INSERT INTO transfers(id,source,target,offer,phase) VALUES (?,?,?,?,'preparing')",
                            (offer["operationId"], self.source.owner_id, offer["target"], bounded_json(offer).decode()))
                except BaseException:
                    self.journal._unlock("handover-source")
                    raise
        await self._storage(reserve)
        self.runtime._handover_released = True
        self.runtime.active = False
        self.runtime._handover_passive = True
        self.runtime._begin_owned_release()
        await self.runtime._drain_owned_work(timeout=None)
        for proxy in self._proxies.values():
            await proxy.flush()
            proxy._handover_frozen = True
        final = self._snapshot()
        self._validate_snapshot(final, empty=False)
        snapshot = bounded_json(final, max_bytes=1024 * 1024)
        digest = hashlib.sha256(snapshot).hexdigest()

        def persist() -> None:
            with self.journal._transaction() as db:
                changed = db.execute("UPDATE transfers SET phase='prepared',snapshot=?,digest=? WHERE id=? AND source=? AND target=? AND phase='preparing'",
                    (snapshot, digest, offer["operationId"], self.source.owner_id, offer["target"])).rowcount
                if changed != 1:
                    raise JournalError("Handover source reservation changed")
        await self._storage(persist)
        await self.source._release_ownership(transferred=True)
        return {**offer, "source": self.source.owner_id, "stateDigest": digest}

    def _snapshot(self) -> dict[str, Any]:
        application = None
        if self._application is not None:
            application = json.loads(bounded_json(self._application.export_state()))
            value = self._application.validate_state(application)
            if inspect.isawaitable(value):
                if inspect.iscoroutine(value):
                    value.close()
                raise JournalError("Application validation must be synchronous")
        proxies = []
        for name, proxy in sorted(self._proxies.items()):
            proxies.append({"name": name, "values": [{"topic": topic, "value": entry.value,
                "uom": entry.uom, "timestamp": entry.timestamp.isoformat()}
                for topic, entry in proxy._last_values.items()], "sequences": list(map(list, proxy._sequence_ids.items()))})
        return {"protocol": 1, "configuration": self._configuration, "application": application, "proxies": proxies}

    def _validate_snapshot(self, snapshot: Any, *, empty: bool = True) -> None:
        if type(snapshot) is not dict or set(snapshot) != {"protocol", "configuration", "application", "proxies"} or snapshot["protocol"] != 1 or snapshot["configuration"] != self._configuration:
            raise JournalError("Snapshot contract differs")
        entries = snapshot["proxies"]
        if type(entries) is not list or len(entries) != len(self._proxies) or {entry.get("name") for entry in entries} != set(self._proxies):
            raise JournalError("Snapshot proxy set differs")
        for entry in entries:
            proxy = self._proxies[entry["name"]]
            if set(entry) != {"name", "values", "sequences"} or (empty and (proxy._last_values or proxy._sequence_ids)):
                raise JournalError("Target calculation state must be empty")
            if type(entry["values"]) is not list or type(entry["sequences"]) is not list or max(len(entry["values"]), len(entry["sequences"])) > 10000:
                raise JournalError("Invalid calculation state bounds")
            names = set()
            for value in entry["values"]:
                if type(value) is not dict or set(value) != {"topic", "value", "uom", "timestamp"} or not isinstance(value["topic"], str) or not value["topic"] or value["topic"] in names or (value["uom"] is not None and type(value["uom"]) is not str):
                    raise JournalError("Invalid calculation value")
                names.add(value["topic"])
                stamp = datetime.fromisoformat(value["timestamp"])
                if stamp.tzinfo is None:
                    raise JournalError("Calculation timestamps require timezone")
                scalar = value["value"]
                if type(scalar) not in (int, float, str, list) or (type(scalar) is list and (len(scalar) > 4096 or any(type(item) not in (int, float, str) for item in scalar))):
                    raise JournalError("Invalid calculation scalar")
            names = set()
            for sequence in entry["sequences"]:
                if type(sequence) is not list or len(sequence) != 2 or type(sequence[0]) is not str or not sequence[0] or sequence[0] in names or type(sequence[1]) is not int or not 0 <= sequence[1] <= 9007199254740991:
                    raise JournalError("Invalid calculation sequence")
                names.add(sequence[0])
        if self._application is None:
            if snapshot["application"] is not None:
                raise JournalError("Unexpected application state")
        else:
            value = self._application.validate_state(snapshot["application"])
            if inspect.isawaitable(value):
                if inspect.iscoroutine(value):
                    value.close()
                raise JournalError("Application validation must be synchronous")

    def _evidence(self, receipt: Any) -> tuple[dict[str, Any], str, str]:
        if type(receipt) is not dict or set(receipt) != {"protocol", "operationId", "journalId", "binding", "configuration", "target", "source", "stateDigest"}:
            raise JournalError("Invalid handover receipt")
        offer = {key: value for key, value in receipt.items() if key not in ("source", "stateDigest")}
        # Source checks offer as-is; target validates against source identity.
        check = {**offer, "target": receipt["source"]} if receipt["target"] == self.source.owner_id else offer
        self._validate_offer(check)
        identifier(receipt["source"])
        digest = receipt["stateDigest"]
        if type(digest) is not str or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise JournalError("Invalid state digest")
        return offer, receipt["operationId"], digest

    async def activate(self, receipt: dict[str, Any], *, timeout: float | None = 30.0) -> dict[str, Any]:
        receipt = json.loads(bounded_json(receipt))
        self._evidence(receipt)
        if receipt["target"] != self.source.owner_id:
            raise JournalOwnershipError("Receipt targets another launch")
        if ("activate", receipt["operationId"]) not in self._tasks:
            # Invalid/forged evidence cannot reserve a legitimate operation ID.
            encoded = await self._storage(lambda: self._load_receipt(receipt))
            if hashlib.sha256(encoded).hexdigest() != receipt["stateDigest"]:
                raise JournalError("Persisted snapshot digest differs")
            self._validate_snapshot(json.loads(encoded))
        return await self._operation("activate", receipt["operationId"], receipt, lambda: self._activate(receipt), timeout)

    def _load_receipt(self, receipt: dict[str, Any]) -> bytes:
        with self.journal._transaction() as db:
            row = db.execute("SELECT source,target,offer,phase,snapshot,digest FROM transfers WHERE id=? AND length(snapshot)<=1048576", (receipt["operationId"],)).fetchone()
            offer = {key: value for key, value in receipt.items() if key not in ("source", "stateDigest")}
            if row is None or row[:4] != (receipt["source"], self.source.owner_id, bounded_json(offer).decode(), "prepared") or row[5] != receipt["stateDigest"]:
                raise JournalError("Handover is absent, changed or already attempted; reconcile same operation")
            return bytes(row[4])

    async def _activate(self, receipt: dict[str, Any]) -> dict[str, Any]:
        if self.runtime.is_active() or self.runtime._stop_requested or self.source._task is not None or self._applied:
            raise JournalError("Target is not an empty passive owner")

        encoded = await self._storage(lambda: self._load_receipt(receipt))
        if hashlib.sha256(encoded).hexdigest() != receipt["stateDigest"]:
            raise JournalError("Persisted snapshot digest differs")
        snapshot = json.loads(encoded)
        bounded_json(snapshot, max_bytes=1024 * 1024)
        self._validate_snapshot(snapshot)
        await self._storage(lambda: self.journal.acquire_consumer(self.source.owner_id, transfer_id=receipt["operationId"]))
        self.source._owns = True

        def applying() -> None:
            with self.journal._transaction() as db:
                if db.execute("UPDATE transfers SET phase='applying' WHERE id=? AND phase='prepared'", (receipt["operationId"],)).rowcount != 1:
                    raise JournalError("Target application attempt is already owned")
        await self._storage(applying)
        self._applied = True
        try:
            for entry in snapshot["proxies"]:
                proxy = self._proxies[entry["name"]]
                proxy._last_values = {value["topic"]: LastValueEntry(value["value"], value["uom"], datetime.fromisoformat(value["timestamp"])) for value in entry["values"]}
                proxy._sequence_ids = dict(entry["sequences"])
            if self._application is not None:
                result = self._application.import_state(snapshot["application"])
                if inspect.isawaitable(result):
                    await result
            await self._activate_application()
            if self.runtime._stop_requested or self.source._releasing:
                raise JournalError("Target stopped during state import; activation is unconfirmed")
            self._set_active(True)
            self.source._start_owned()
            def activated() -> None:
                with self.journal._transaction() as db:
                    if db.execute("UPDATE transfers SET phase='activated' WHERE id=? AND phase='applying'", (receipt["operationId"],)).rowcount != 1:
                        raise JournalError("Activation reservation changed")
            await self._storage(activated)
            return receipt
        except BaseException:
            self._set_active(False)
            self.runtime._begin_owned_release()
            raise

    async def acknowledge(self, receipt: dict[str, Any], *, timeout: float | None = 30.0) -> dict[str, Any]:
        receipt = json.loads(bounded_json(receipt))
        self._evidence(receipt)
        if receipt["source"] != self.source.owner_id:
            raise JournalOwnershipError("Receipt belongs to another source")
        return await self._operation("acknowledge", receipt["operationId"], receipt, lambda: self._acknowledge(receipt), timeout)

    async def _acknowledge(self, receipt: dict[str, Any]) -> dict[str, Any]:
        if "handover-source" not in self.journal._locks:
            raise JournalOwnershipError("Original source exit requires explicit recovered-source reconciliation")
        def check() -> None:
            with self.journal._transaction() as db:
                row = db.execute("SELECT source,target,digest,phase FROM transfers WHERE id=?", (receipt["operationId"],)).fetchone()
                if row != (self.source.owner_id, receipt["target"], receipt["stateDigest"], "activated"):
                    raise JournalError("Target activation is unconfirmed")
        await self._storage(check)
        await self.runtime.stop()
        # The input worker is now closed by process stop. Control IO remains
        # available until the final receipt is durable.
        def complete() -> None:
            with self.journal._transaction() as db:
                if db.execute("UPDATE transfers SET phase='completed' WHERE id=? AND phase='activated'", (receipt["operationId"],)).rowcount != 1:
                    raise JournalError("Source completion reservation changed")
        await self._storage(complete)
        self.journal._unlock("handover-source")
        return receipt

    async def recover_receipt(self, operation_id: str) -> dict[str, Any]:
        """Read the original prepared evidence after response/process loss."""
        identifier(operation_id)
        def load() -> dict[str, Any]:
            with self.journal._transaction() as db:
                row = db.execute("SELECT offer,source,phase,digest FROM transfers WHERE id=?", (operation_id,)).fetchone()
                if row is None or row[2] not in ("prepared", "activated", "completed") or not row[3]:
                    raise JournalError("No confirmed prepared receipt; reconcile the original operation")
                receipt = {**json.loads(row[0]), "source": row[1], "stateDigest": row[3]}
                self._evidence(receipt)
                if self.source.owner_id not in (receipt["source"], receipt["target"]):
                    raise JournalOwnershipError("Receipt belongs to other launches")
                return receipt
        return await self._storage(load)

    async def reconcile_source_exit(self, receipt: dict[str, Any], *, resolution_id: str) -> None:
        """Explicit reviewed retirement after a prepared source was killed.

        Requires committed target activation and the source's now-free kernel
        lock. This confirms retirement, not that killed source cleanup ran.
        No import, business replay or new ownership operation is performed.
        """
        receipt = json.loads(bounded_json(receipt))
        self._evidence(receipt)
        identifier(resolution_id)
        if receipt["target"] != self.source.owner_id:
            raise JournalOwnershipError("Only the exact target can reconcile source retirement")
        decision = bounded_json({"operation": receipt["operationId"], "digest": receipt["stateDigest"], "decision": "source-retired"}).decode()
        def resolve() -> None:
            with self.journal._mutex:
                self.journal._lock("handover-source")
                try:
                    with self.journal._transaction() as db:
                        old = db.execute("SELECT record_id,decision FROM resolutions WHERE id=?", (resolution_id,)).fetchone()
                        if old:
                            if old != (0, decision):
                                raise JournalError("Resolution ID was reused")
                            return
                        if db.execute("SELECT count(*) FROM resolutions").fetchone()[0] >= 1024:
                            raise JournalError("Resolution history is full")
                        row = db.execute("SELECT source,target,phase,digest FROM transfers WHERE id=?", (receipt["operationId"],)).fetchone()
                        if row != (receipt["source"], self.source.owner_id, "activated", receipt["stateDigest"]):
                            raise JournalError("Target activation is not confirmed")
                        db.execute("INSERT INTO resolutions VALUES (?,0,?)", (resolution_id, decision))
                        db.execute("UPDATE transfers SET phase='completed' WHERE id=?", (receipt["operationId"],))
                finally:
                    self.journal._unlock("handover-source")
        await self._storage(resolve)

    def inspect_operation(self, operation_id: str) -> dict[str, Any] | None:
        identifier(operation_id)
        with self.journal._mutex:
            row = self.journal._db.execute("SELECT source,target,phase,digest FROM transfers WHERE id=?", (operation_id,)).fetchone()
            return None if row is None else {"protocol": 1, "operation_id": operation_id,
                "source": row[0], "target": row[1], "phase": row[2], "state_digest": row[3]}
