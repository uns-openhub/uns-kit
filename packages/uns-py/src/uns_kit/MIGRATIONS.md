# Python SDK migrations

## Crossing <0.2.11 to >=0.2.11

Default API endpoint metadata now publishes retained snapshots on the Controller
registry topic `uns-infra/<package>/<version>/<process>/<instance>/api-endpoints`,
matching TypeScript. Earlier Python versions used the unrecognized `/api` suffix.
Endpoint registration events report the corrected topic too; endpoint removal
publishes the remaining snapshot on that same topic. Custom metadata consumers
must subscribe to `/api-endpoints`. Handler URLs and API authorization are unchanged.

Provision the declared parent UNS topic before exposing tree-backed API endpoints
on a fresh Controller. No ETL business-code changes are required for this fix.

## Crossing <0.2.10 to >=0.2.10

Durable input and same-host journal hot handover are explicit opt-ins. Existing
`managed_input` RAM queues do not become persistent. Legacy `handover` arguments
do not enable `JournalHandover` or acquire TS/Controller protocol capabilities.

Provision a private local POSIX journal with stable semantic identity and bounds.
Run `DurableMqttReceiver` independently with a dedicated stable persistent-session
client ID and QoS 1 sources. Its ACK follows SQLite commit; broker persistence and
session expiry remain infrastructure responsibilities. Failed/full admission
disconnects without ACK; review storage and explicitly resume transport.

`create_journal_input()` owns one serial callback. All process-owned output must
use `publishQos: 1` and strict drain before a record is checkpointed. The new QoS
output mode makes one broker attempt and propagates uncertainty without replay.
Application DB checkpoints/idempotency must be awaited inside the handler.
Stop drains admitted application work while leaving unstarted disk records pending.
Crash/failed effects remain unconfirmed. Review the original durable operation
before an explicit persisted `reconcile(..., resolution_id=..., decision=...)`.

Prepare every proxy/API/cron before sealing `JournalHandover` contracts. Use
managed ownership, one process-owned journal source, exact launch IDs and matching
application/business resources. Only the initial owner cold-starts. A target
activates from its original prepared receipt; timeouts/repeated messages never
authorize a different writer or repeat an import. Unknown `preparing`/`applying`
operations require review. Source retirement after SIGKILL is explicitly recorded
as such and cannot masquerade as successfully executed source cleanup.

The optional state helper uses canonical Python JSON, not a TS wire-compatibility
claim. Do not advertise MQTT correlated-v2, Node process groups or host cooperative
drain receipts. No live state copying, cross-host transfer, universal exactly-once
guarantee or automatic recovery of arbitrary RAM state is added. See the source
guide `docs/python-durable-input-handover.md` for the supported contract and limits.
Managed process-owned cron now respects passive ownership, including before
initial activation; legacy processes without managed ownership keep their behavior.

## Crossing <0.2.9 to >=0.2.9

Managed MQTT input is opt-in (`managed_input` / `managedInput`). Register one
handler, then explicitly await `start_input()` after resources and ownership are
prepared. Do not combine it with the same client's manual message iterators.
The SDK bounds admitted input, preserves failed originals and never retries a
handler automatically. Unsubscribe acknowledgement and accepted-work drain
precede output close. Overflow/uncertain work blocks a clean Stop.

HTTP requests are now tracked by default. Process/API Stop rejects new requests
and awaits accepted application work before closing resources. A disconnected
ordinary request or failed handler can leave an unknown outcome and block Stop.
Explicit `run_read_only_request()` is only for effect-free work; declare once
before the first async step/response completion. GET/POST names prove no safety.
Custom catch-all HTTP routes remain subject to their existing authorization.

Process-owned MQTT output now defaults to a bounded 1000-item queue and 32
capacity waiters, with strict drain. Explicit nonpositive queue limits preserve
legacy unbounded behavior. Strict drain latches asynchronous broker errors even
after counters reach zero; adapt cleanup code that assumed logged publish errors
were a successful flush. Standalone legacy output keeps its default behavior.

Opt-in `managed_ownership` suppresses automatic activation; call `set_active(True)`
only after preparing handlers. It is a local admission flag, not a lease or hot
handover implementation. Setting it inactive begins permanent MQTT input release;
do not reuse that released source on reactivation. HTTP gates can be used
independently. Await the same drain after timeout; never replay an unknown effect
or launch a successor just to clear a failed stop.

Continue using one application signal owner and await process Stop. This slice
does not advertise Controller cooperative-stop, persistent journals or hot
handover. See `docs/python-managed-input-http.md` for limits and acceptance.

## Crossing <0.2.8 to >=0.2.8

Ordinary authenticated API routes now require current Controller authority for
verified machine/service claims. Configure `service_authorization_status_url`
(mapping `serviceAuthorizationStatusUrl`) or a trusted
`UNS_CONTROLLER_PUBLIC_BASE`. The status endpoint must return active protocol 1,
machine principal and `configuredAccessAuthority: 2`. Missing/old/unavailable
authority fails closed; review receiver compatibility before upgrading service
API consumers. User JWT behavior remains unchanged.

Configured UNS service credentials cannot call ordinary service APIs using path
rules: they receive 403 `service_api_permission_required`. Operator delegation
is not supported in Python ordinary endpoints. Machine path rules must be
nonempty and structurally valid. Declarations grant no live authority.

An explicitly configured token provider that returns no token no longer falls
back to interactive human login, in either sync or async clients. Synchronous
credential-bearing provider requests now reject foreign origins/redirects and
bound responses to 16 MiB. `AsyncUnsClient` provides configurable response bounds
and the same endpoint/result contracts, with bounded serialized offload.

PostgreSQL/Oracle failures now propagate without automatically replaying SQL;
their failed connection is discarded, and only a new explicit operation can
reconnect. This affects synchronous clients as well as the new async facade.
Reconcile writes after an uncertain commit instead of blindly retrying them.

`AsyncUnsClient` and `create_async_database_client()` are opt-in. Await all their
operations and close them after accepted work drains. Cancellation/asyncio
timeout stops waiting, not the underlying HTTP/SQL effect. Capacity rejection
means no operation was admitted; do not confuse it with an uncertain outcome.
Per-client workers are bounded and serialized, with SQLite connection affinity.
This release does not add transactions, streaming, pooling or hot handover.

Read the source guide `docs/python-service-access-async.md` and the bundled
README. The background declaration loader supports protocol 1 without catalog
publications and requires safe no-follow directory IO. Configured/catalog
protocols remain separate unsupported opt-ins, not implicit grants.

## Crossing <0.2.7 to >=0.2.7

The async process owner adds optional `ProcessWork`, `ProcessLifecycleHooks`
and TS-compatible retained service metadata. Existing applications do not
automatically adopt application-work tracking, discovery declarations or signals.

`UnsProxyProcess.stop()` now makes the process inactive immediately, coalesces
concurrent calls, attempts every independent transport cleanup and reports
failures as `ProcessShutdownError` with original exceptions in `.errors`.
Adapt code relying on a particular first transport exception. Successful stop
is idempotent; reusing a stopped owner with `start()`/new proxies is rejected.
Construct a new owner after confirmed cleanup when restarting the application.

For opt-in lifecycle hooks, stop admitting new MQTT/HTTP/background work in
synchronous `on_release`, then await accepted work in `drain`. Register hooks
before inputs. Application drain precedes transport close even with `drain=False`.
A timeout does not cancel application effects: retry the same Stop to await the
same drain. A failed business effect remains unconfirmed and needs reconciliation.
Do not replay business effects or mark work successful merely to unblock cleanup.

Continue owning SIGINT/SIGTERM in the application and await `process.stop()`
from its cleanup path. This release installs no signal owner or hot handover
protocol. Existing Python `handover` parameters still do not implement TS handover.
HTTP request admission/drain and managed MQTT input are not added automatically.

`publish_service_metadata()` and its `publishServiceMetadata()` alias use TS
schemaVersion 1 on the process-scoped retained `service-metadata` topic. Use
only non-secret observations. Metadata grants no API/MQTT/database permissions
and establishes neither live health nor Controller acceptance by itself.
