# Python durable input and same-host hot handover

Prepared in **uns-kit 0.2.10**. This is an opt-in Python journal protocol **1**,
using one private local POSIX directory shared by an independent receiver and
successive Python business owners. It extends the 0.2.9 input/HTTP drain slice.
Source review used TypeScript 3.0.54 as a behavioral reference.

This is a working same-host handover, not an implementation of TypeScript's
MQTT `correlated-v2` transport or the Node process-group protocol. Do not declare
those capabilities, synthesize host drain receipts or assume Controller support.
Publication, application adoption, host supervision and deployment are separate.

## Independent durable admission

```python
from uns_kit import DurableInputJournal, DurableMqttReceiver

journal = DurableInputJournal(
    "/absolute/private/state/inspection",  # pre-existing private parent
    journal_id="inspection-frames",
    contract="frames-v1",                  # semantic revision, not package version
    create=True,                           # explicit initial provisioning
)
receiver = DurableMqttReceiver(
    journal, "127.0.0.1", ["example/inspection/frames"],
    client_id="inspection-durable-receiver",  # stable, dedicated broker session
    port=1883,
)
await receiver.start()
# Own the receiver's signal/finally path separately from a business publisher.
await receiver.stop()
journal.close()
```

The receiver uses public paho MQTT manual acknowledgement. It accepts **QoS 1
publications** into SQLite WAL with `synchronous=FULL`, commits topic, original
binary payload, receive time and delivery identity, and only then attempts PUBACK.
The broker session uses the same client ID and `clean_session=False` across
restarts. Session state must be retained by the broker; configure broker
persistence separately if broker restart survival is required.

Redelivery of a committed, unacknowledged broker packet reuses its existing record
instead of admitting another one. This is scoped to the same observed broker
session and packet identity. A changed payload on an ambiguous DUP packet fails
closed. Distinct business events, publisher retries and session loss remain
separate; this is not a universal exactly-once guarantee.

Capacity/storage/ACK failures disconnect without acknowledging the failed
delivery. The receiver remains visibly paused. After reviewing/restoring storage
or making room, call `await receiver.resume()`. Resume retries transport and
admission, never a business callback. A stopped receiver requires a new instance.
Cancelling a start/resume/stop waiter does not release its still-running IO or
allow another receiver to claim the same session early.

Defaults bound unfinished records to 20,000, topic/payload bytes to 64 MiB, each
record to 1 MiB and SQLite main-file growth to 256 MiB. The WAL/SHM, SQLite page
overhead and broker/library allocations are additional. Disk pressure is checked
before admission; no pending record is pruned to make room. SQLite may reuse
freed pages rather than shrink its files. Operation history is bounded to 256
handovers and 1,024 explicit resolutions; exhaustion requires storage review,
not silent history deletion or ID reuse.

The directory and database must be owner-only, use absolute paths without
symlinks, and remain on one host with reliable local filesystem locking/fsync.
Windows, network filesystems and cross-host copying of a live WAL are unsupported.
Back up a stopped, closed journal with all its files; this slice does not implement
the Runtime backup-helper or online snapshot contract. Never delete/reinitialize
a journal to bypass unknown work. The journal is caller-owned: stop its consumers
and receiver before closing the handle. Closing a handle is not a business drain.

## One serial business owner

```python
from uns_kit import UnsProxyProcess

runtime = UnsProxyProcess(infra_host, {
    "processName": "example-transformer",
    "managedOwnership": True,
})
await runtime.start()
output = await runtime.create_uns_mqtt_proxy(output_host, "observations", uns_parameters={
    "publishQos": 1, "publishConcurrency": 1,
})

async def handle(record):
    # record.id is stable within this journal; payload is immutable original bytes.
    # Parsing, topics, units, business IDs and durable DB checkpoints belong here.
    await output.publish_message("example/inspection/derived", record.payload)

source = runtime.create_journal_input(journal, handle, owner_id="reviewed-launch-1")
runtime.set_active(True)
await source.start()
# Signal/finally path:
await runtime.stop()
```

Only one kernel-locked consumer can execute a handler. The active record is
committed as `running` **before** invocation. The checkpoint deletes that record
only after the handler settles and all process-owned output flushes complete.
Journal outputs require strict drain and explicit QoS 1. They make one publish
attempt; an uncertain broker result is not automatically replayed. Broker ACK
does not prove downstream database commit.

Release stops taking new disk records and finishes the one already admitted to
the application. Remaining `pending` records stay available to the next confirmed
owner; Stop does not empty the entire durable backlog. An input/drain timeout or
cancelled waiter never cancels or resubmits the owned handler. Runtime Stop drains
HTTP and application work before closing output. Managed cron dispatch remains
passive until ownership is active. Register all outputs before starting input.

A failed handler or uncertain flush stays `unknown`. SIGKILL during a running
record also blocks the next owner; the original payload is retained. The SDK
cannot prove whether an external write committed. After reviewing the original
business operation and fencing its old consumer, explicitly call:

```python
journal.reconcile(record_id, resolution_id="review-unique-id", decision="completed")
# OR decision="retry" only when an operator has established safe replay/idempotency.
```

No live consumer may hold the lock during this decision. Resolutions are persisted
and idempotent under their original ID; changed evidence under that ID is rejected.
Reuse the original stable owner ID when recovering that launch. A different
unretired owner does not gain authority just because its process is absent.
These synchronous storage/inspection methods may block: offload them when called
from latency-sensitive async code. Local file access grants no platform RBAC.

The journal preserves pending/unconfirmed inputs, not arbitrary application RAM
after a crash. If a handler stores un-emitted samples only in RAM and returns,
that is not a durable business checkpoint. Persist those decisions in the handler
before success, using business idempotency and the stable journal/record identity.
Do not infer business-event uniqueness from a broker packet ID or an input hash.

## Hot handover without moving the journal

Prepare both processes with `managedOwnership=True`, one process-owned journal
input and the same journal identity, bounds, output endpoint/semantic contracts
and application contract. Each opens its own journal handle. Keep the receiver
running independently while the two business owners switch. Do not combine this
handover with a raw/manual MQTT reader or another input source.

Declare every proxy/API/cron and lifecycle participant before sealing contracts:

```python
from uns_kit import (
    ApplicationHandoverState, JournalHandover,
    create_application_handover_contract,
)

contract = create_application_handover_contract("window-v1", {
    "windowSeconds": 60, "unit": "count",
    "outputTopic": "example/inspection/derived",
})
handover = JournalHandover(runtime, source,
    proxy_contracts={"observations": contract},
    application_state=ApplicationHandoverState(
        contract=contract,
        export_state=export_window,       # synchronous, pure, bounded JSON
        validate_state=validate_window,   # synchronous; raise on every invalid field
        import_state=restore_window,      # may be async; await all its work
    ),
    on_activate=prepare_owned_timers,      # optional; HTTP/input still closed here
)
```

Select all business resources affecting state meaning: mappings, destination
topics, scaling, units and aggregation/workflow definitions. Secrets and resolved
credentials must not be included. The helper hashes canonical Python JSON; it
does not assert hash/wire interoperability with the TS handover implementation.
State is plain finite JSON, at most 768 KiB for application state, 1 MiB for the
whole snapshot, depth 32 and 50,000 nodes. Invalid/oversized state is rejected,
never truncated. Every output's Python counter baselines and sequence state are
also validated and restored. Output-only proxies have no MQTT input overlap to
guess: the authoritative frontier is the shared journal checkpoint.

Only the initial reviewed owner calls `await handover.start()`. The standby
stays passive indefinitely; neither a heartbeat nor a timeout promotes it.
The caller supplies the same operation evidence between peers:

```python
offer = target_handover.offer("move-unique-id")
receipt = await source_handover.prepare(offer, timeout=30)
await target_handover.activate(receipt, timeout=30)
await source_handover.acknowledge(receipt, timeout=30)
```

1. Compare target journal/configuration/semantic contracts before source release.
2. Persist the exact source, target launch and operation in `preparing`.
3. Close source admission, finish its active record, accepted HTTP/application
   work and broker flush. Receiver continues journaling new inputs.
4. Persist the final application/calculation snapshot and digest as `prepared`,
   then release the source consumer lock. Source output stays fenced.
5. Target validates the entire snapshot and exact evidence, obtains the consumer
   lock, persists `applying`, imports once and prepares owned application resources.
   Only then open target HTTP/input and record `activated`.
6. Source checks the committed target activation, stops its remaining resources,
   and only after successful cleanup records `completed`.

Repeated successful prepare/activate/acknowledge calls return copies of the same
evidence. A timeout/cancelled waiter leaves that same operation owned and running.
Repeated activation never re-imports application state or resets sequences.
Unknown import/activation effects remain `applying`; restarting the target cannot
silently repeat them. A concurrent Stop waits for the original import attempt and
closes admission, instead of closing resources underneath it. A failed source
cleanup can be retried without repeating target import. Return/rollback uses a
fresh passive process and new reviewed reverse handover, never the old RAM owner.

`inspect_operation(id)` returns only phase, peer IDs and digest; it does not expose
the snapshot. `await recover_receipt(id)` retrieves the original prepared evidence
after response loss or source restart. A persisted `prepared` receipt can be
activated by the exact target after a source crash. If that source was killed
after preparation, its ordinary cleanup was not confirmed: after reviewing/fencing
the launch, the exact target can explicitly call
`await reconcile_source_exit(receipt, resolution_id="review-exit-id")`.
This requires committed activation and the source's now-free kernel lock. The
durable resolution records retirement, not successful execution of killed cleanup.
`preparing` without a snapshot and `applying` with unknown effects remain blocked
for business/host reconciliation; no generic reset or force promotion is provided.

Application release/drain hooks must stop timers/pollers, await all accepted work
and propagate unknown effects. They must not synchronously stop/drain the same
owner from inside an accepted handler or its own handover callback. No detached
task, external SQL transaction or HTTP effect is made safe by merely changing a
phase. IDs and digests provide binding/integrity, not signatures or authorization.

## Acceptance and boundaries

The Python suite exercises real localhost Mosquitto admission/ACK loss, full
capacity/resume, a receiver killed after commit before ACK, a consumer SIGKILL,
two input namespaces/contracts, actual HTTP drain and output observation, duplicate
evidence, partial import, concurrent Stop, reverse handover and a separate prepared
source process killed before retirement. The wheel is checked independently.

This provides same-host standalone ownership and input durability. It does not
elect peers, move a database between hosts, orchestrate Controller placement,
supervise the independent receiver, implement Runtime backup/restore, or promise
loss-free capture before durable admission. Broker outages/session expiry, hardware
fsync behavior, business idempotency and platform launch authority remain explicit
application/infrastructure responsibilities.
