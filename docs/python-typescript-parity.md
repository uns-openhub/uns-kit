# Python and TypeScript SDK parity

Source review: 17 November 2026. TypeScript baseline: 3.0.54 (`cbdedb8`).
Python baseline: 0.2.6; the first runtime/discovery slice is prepared in 0.2.7.
The second authority/async IO slice is prepared in 0.2.8 (18 November 2026).
The managed MQTT/HTTP ownership slice is prepared in 0.2.9 (18 November 2026).
Durable input and same-host Python journal handover are prepared in 0.2.10.
These are source capabilities, not evidence of registry publication or deployment.

The goal is compatible reusable SDK behavior and wire contracts, with idiomatic
Python APIs. Applications retain their business parsing, mappings, topics, units
and checkpoint semantics. Controller keeps authentication, authorization,
placement, durable operations and reconciliation. See the
[shared ownership boundary](controller-runtime-boundary.md).

## Current comparison and sequence

| Area | Python source | Next action |
| --- | --- | --- |
| MQTT data/table packets and metadata | Existing builders, parsers and topic discovery | Preserve the existing versioned migration contract; do not change business schemas as part of runtime parity. |
| Config and secrets | Environment/Infisical resolution and generated config schemas exist | Review actual transport options and validation independently; both SDKs have gaps. |
| MQTT input/output | Managed RAM input in 0.2.9; bounded SQLite input, independent manual-ACK receiver and QoS 1 checkpoints in 0.2.10 | Broker persistence, business idempotency and a durable output outbox remain separate. |
| Datahub client and service token rotation | Existing client/token-file provider; bounded `AsyncUnsClient` added in 0.2.8 | Keep configured/catalog protocols separate; add native transport only if its cancellation and resource semantics are equally verified. |
| Database access | Shared PostgreSQL, Oracle and SQLite helpers; thread-affine async facade added in 0.2.8 | Transactions, streaming and pooling remain separate; never replay SQL with an unknown outcome. |
| Application work | `ProcessWork` added in 0.2.7 | Adopt explicit background admission and accepted-input tracking. It is not a durable job journal. |
| Standalone lifecycle | 0.2.7 hooks extended in 0.2.9 with automatic managed input/HTTP drain before application/output cleanup | Application still owns signals and detached work. Integrate host cooperative stop only after its full wire protocol is implemented. |
| Service discovery | TS-compatible retained service metadata added in 0.2.7 | Verify actual discovery with a compatible local Controller and synthetic broker. Metadata does not prove health or grant rights. |
| API ownership and Operator API | Fresh service authority in 0.2.8; local ownership gates, accepted HTTP work and explicit read-only drain in 0.2.9 | Operator delegation/exact launch registration remain absent. Existing data offers are not the caller-mediated catalog protocol. |
| Stateful hot handover | Same-host Python journal protocol 1 in 0.2.10: exact operation/peer binding, application/counter snapshot and fenced activation | TS `correlated-v2`, cross-host transfer and Controller receipts remain absent. Legacy `handover` arguments do not enable the new protocol. |
| Retained stores, Runtime outbox and process groups | Newer TS protocols absent | Later opt-in slices with storage ownership, backup, reconciliation and compatible host/helper support. |

For a normal Python service, the first three slices cover standalone lifecycle/
discovery, ordinary service authorization, asynchronous IO facades and managed
MQTT/HTTP work. Next review generic host cooperative stop and Operator contracts.
Full process-group consolidation is not a prerequisite for those improvements.
Avoid copying TS defects or making every advanced capability mandatory.

## First slice: 0.2.7

### Discovery

`publish_service_metadata()` emits the same camelCase JSON schemaVersion 1 and
process-scoped `service-metadata` topic as TS `publishServiceMetadata()`.
It includes package/process identity, declared capabilities, API/health paths
and optional non-secret `extra` observations. This is separate from HTTP
`register_service()`, API route registration and active status.

Python and TS test the same two independent application fixtures in
`tests/fixtures/service-metadata-v1.json`. A fixture match establishes wire
compatibility, not successful Controller discovery or live service health.

### Accepted application work

`ProcessWork.track(awaitable)` immediately owns work already accepted from a
transport. `run(factory)` gates new background work using a supplied ownership
predicate and pause state. Cancellation/failure remains latched; subsequent
success does not certify earlier unknown effects. Drain and background-caller
cancellation do not cancel the owned work. All methods use one asyncio loop.

This is RAM bookkeeping. It adds no concurrency bound, retry, source replay,
durable checkpoint or exactly-once guarantee. The application must separately
bound the number of admitted operations and make safe decisions after partial
effects. Do not replace bounded admission with an unbounded `gather()`.

### Stop and resource ownership

An application registers `ProcessLifecycleHooks` before starting its inputs.
All synchronous `on_release` callbacks run before any application drain. The
application must close its own MQTT/HTTP admission and pause background work.
Process-owned cron dispatch skips new callbacks after Stop is requested.
Existing cron handlers must participate in the application's accepted-work drain.

Concurrent Stop/shutdown calls await the same cleanup task. Application drains
finish before transport teardown. A failed drain leaves transports open and
does not report a clean stop. Timeout leaves that same application drain running;
a later Stop can await its completion without invoking it again. A permanently
failed drain requires application reconciliation, not blind business replay.

After successful application drain, teardown attempts every independent
API/cron/output/status/client cleanup and aggregates failures in
`ProcessShutdownError.errors` (Python 3.10-compatible). A later Stop retries
failed resource closes while preserving successful cleanup. Successful Stop
is idempotent. A stopped owner cannot restart or create new proxies; create a
new owner after confirmed cleanup instead.

`timeout` is in seconds and bounds application drain plus each MQTT proxy's
existing drain timeout; it is not a total wall-clock deadline for every resource.
`drain=False` retains the existing explicit output-discard option; it never
bypasses registered application drain hooks. No signal handler, process exit,
Controller operation receipt or hot-handover capability is installed implicitly.
The new hooks are an asynchronous-runtime API; the sync facade remains unchanged.

## Verification boundaries

The [0.2.9 guide](python-managed-input-http.md) records managed input, output
backpressure, HTTP disconnect semantics, explicit read-only work and process drain
order. Acceptance includes a real temporary broker/restart and SDK HTTP servers.

The [0.2.10 guide](python-durable-input-handover.md) records durable admission,
same-host peer switching, bounded storage, crash/ACK-loss recovery, explicit
unknown-effect decisions and protocol limits. This is not mixed Python/TS wire
acceptance or an installed Controller/Runtime capability.

The [0.2.8 guide](python-service-access-async.md) records background-only
declaration loading, current service authority, worker affinity/capacity,
cancellation and no-replay recovery limits. It includes two unrelated Python
readers and shared TS/Python authority fixtures. Configured UNS/catalog and
Operator protocols remain distinct future work.

- Run the complete Python suite with its API/cron extras and local loopback support.
- Run the TS service-metadata fixture tests.
- Build the Python wheel and sdist, then inspect/import the wheel independently.
- Exercise actual Controller discovery and process stop separately before relying
  on managed operation behavior. Do not synthesize capability/launch evidence.
- Require a second unrelated service for each new reusable host-facing protocol.

Publication, downstream upgrades, Controller integration and deployment remain
separate steps. This slice changes no application dependencies or host processes.
