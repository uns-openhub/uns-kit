# Retained stores: opt-in same-host adoption

Retained-store adoption starts in UNS Kit 3.0.45. The 3.0.46 extension adds
explicit copying between local mounts and verification of recovered stores.
Managed activation requires coordinated Controller and Runtime releases. Existing applications do not change behavior.
This is a filesystem capability, separate from the small JSON
handover-state contract and MQTT output persistence. It does not change the
default handover protocol, enable disk queues, or transfer data between hosts.

## Ownership

The SDK owns the versioned manifest, binding, safe path validation and resumable
same-filesystem rename helpers. Controller owns authenticated review, launch
checks, reservations, durable operation state and managed lifecycle fences.
The application owns data schema compatibility, its exclusive writer/lease,
maintenance semantics, replay and database acknowledgements. Runtime/tools own
durable mounts and backup/restore coverage. No Controller package-name branches
or execution of application code are required.

Publish `retained-store.manifest.json` at the installed package root. Give each
opaque resource a stable ID, a relative legacy path, kind and required flag.
Declare a format only when both versions can safely open the same data. An
explicit legacy version list permits sources without a manifest; it is a
compatibility promise by the application, not an automatic schema conversion.
Keep keys, tokens and startup configuration outside resource declarations.

The two example manifests in `examples/retained-store/` deliberately describe
different applications/formats. Whole data directories include SQLite WAL/SHM
sidecars without the Controller reading SQLite or interpreting payloads.
The npm core package includes this guide at `dist/runtime/retained-store-guide.md`
and the same manifests at `dist/runtime/retained-store-examples/`.

```ts
import {
  readRetainedStoreManifest,
  resolveRetainedStoreResources,
  openRetainedStoreLease,
} from '@uns-kit/core/runtime/retained-store.js';

const resolved = resolveRetainedStoreResources(instanceDirectory,
  readRetainedStoreManifest(packageDirectory),
  { rttNode: packageName, version: packageVersion, instanceId });
const lease = resolved.binding ? openRetainedStoreLease(resolved.binding) : undefined;
// Now open resolved payload resources and acquire application database leases.
// Keep the shared lease until writers close; call lease?.close() on shutdown.
// An attached store enters application maintenance before replay.
const dataDirectory = resolved.resources.queue;
```

A binding is private and names an opaque store UUID, generation, operation,
owner, reviewed configuration digest and stable directory. The shared store
metadata must match it exactly. A stale owner or fenced instance fails closed.
The helper does not acquire the application's database lock or authorize an
operator. Do not bypass either check with an application-specific fallback.

## Host procedure

1. Resolve installed package/instance IDs server-side. Require matching portable
   configuration, compatible declarations, exact compiled artifact fingerprints,
   an unoccupied target and one filesystem. Preview never creates storage.
2. Persist the reviewed selection/digest and, before moving, commit operation
   intent. Require the reviewed source launch's clean physical exit according to
   the supervisor; target must also be stopped. Reserve both managed instances.
3. Use `prepareRetainedStoreMove` and `applyRetainedStoreMove(plan, 'adopt')`.
   Persist the complete plan before applying it. Moves are atomic per resource,
   fsynced, inode/device pinned and resumable; there is no copy or overwrite
   fallback. Several resources do not form one filesystem transaction.
4. Commit the binding/current owner and durable outcome before releasing the
   receiving instance fence. Start only the new owner, initially in maintenance.
5. After stopping that owner, capture `prepareRetainedStoreReturn(plan)` and
   persist return intent. Return the *current* resources, including new optional
   or atomically replaced control files. Never restore an old queue snapshot.

Reconcile a timeout by reading/repeating the same operation UUID. A different
request is not a substitute for recovery. Keep the old-side fence after success.
A direct manual launch of an older binary outside Controller can ignore a fence;
this protocol protects managed lifecycle and participating SDK consumers.

## Stopped-owner configuration review

Packages can declare neutral scalar tuning in
`retained-store-configuration.manifest.json` at the installed package root.
Protocol 1 supports activation on the next start after a confirmed stop.
For example, an indexing worker could declare this logging threshold.

```json
{
  "protocol": 1,
  "activation": "stopped",
  "fields": [
    {
      "path": ["diagnostics", "level"],
      "kind": "enum",
      "values": ["warn", "info"],
      "dependencies": [["diagnostics", "sink"]]
    }
  ]
}
```

Each declaration names an existing object field. A rule supports `boolean`,
`enum` with a fixed list of strings, or `integer` with inclusive `minimum` and
`maximum`. The old and new values must both satisfy the same rule. Each rule
lists its dependencies explicitly. These object paths must be present and
unchanged. An empty list declares that there are no required local configuration
dependencies. The package attests that tuning leaves source, destination,
storage format, replay and checkpoint semantics intact. External reference
resolution and package provenance require separate host checks when relevant.

`readRetainedStoreConfigurationManifest()` returns the validated declaration
and its exact-file SHA-256 digest. `reviewRetainedStoreConfigurationChange()`
accepts both complete JSON revisions as UTF-8 bytes or strings. It rejects
duplicate keys, additions, removals, object and array reordering, and changes
outside the declared paths. Its result contains field paths only. Review uses
at most 64 KiB for the declaration and 1 MiB per configuration and rejects
malformed input, excessive depth and unknown declaration versions. The host
enforces authorization, artifact and revision identity, confirmed stop,
maintenance locking and durable application of the reviewed change.

## Queued handover

`runtime/queued-handover.js` provides launch-bound queue observations and a
durable receipt protocol. A package can advertise
`unsDatahub.queuedHandover: { "protocolVersion": 1, "inputBoundary": "none" }`
and call `UnsProxyProcess.enableQueuedHandover()` with an application observer.
The SDK adds observations from configured MQTT outboxes. Observations carry
per-queue units, timestamps, and exact, lower-bound, or unknown quality.
An observer can report queue progress even when a replay boundary is unavailable.

An executable adapter declares `inputBoundary: "durable-replay-v1"`. It
provides its actual queue identity, an independently verified durable input
checkpoint, a successor that can resume from that checkpoint, and a durable
hold for all successor external effects. A bare unsubscribe and resubscribe
has a gap. The bounded RAM overlap used by ordinary correlated MQTT handover
cannot establish durable replay. Use `queuedHandoverRetainedStoreIdentity()`
with the SDK-verified binding for a retained queue.

A package can instead declare `inputBoundary: "mqtt-qos0-gap-v1"` for a
managed MQTT 5 QoS 0 subscription. The reviewed binding must carry literal
`acceptedInputLoss: true` and the adapter must attest the actual boundary using
`closeQueuedHandoverQos0Input()` and `openQueuedHandoverQos0Input()`. The source
fences local delivery before submitting UNSUBSCRIBE and confirms the broker
acknowledgement. The successor starts connected and unsubscribed, and its
local delivery gate opens on the matching successful QoS 0 SUBACK. The receipt
records `closedAt` and `openedAt`. Messages published during that interval can
be lost. The successor suppresses retained replay and still admits live retained
publications. A frozen source remains closed after MQTT reconnect. Gap evidence
is specific to this policy and cannot satisfy durable replay.

A Controller-launched `UnsProxyProcess` with an SDK publish queue automatically
reports the queue over launch-bound IPC. A service with one durable managed MQTT
outbox can call `await owner.enableQueuedHandover({ inputBoundary })` to supply
its replay proof. The SDK derives the queue identity from the opened Runtime
mount and owner/queue namespace, holds successor publications, observes
capacity, and waits for broker acknowledgements. Controller review can use
`queuedHandoverManagedMqttIdentity()` with the standard
`managedMqttOutputBinding()`. It reads mount metadata without opening any
outbox database. An ordinary service with no durable input proof stays in
observation mode.

The authenticated host binds one operation to the reviewed checksum, both
launches, and both queue identities. It launches the successor with
`UNS_QUEUED_HANDOVER_BINDING` and `UNS_QUEUED_HANDOVER_ROLE=target`. The
successor must establish its application effect hold before registering the
adapter. The SDK suppresses ordinary MQTT handover and holds publication
from the target. The host checks the held target before freezing the source.
The target must report fresh, exact zero counts for every known queue before
the SDK opens its input. New work can accumulate after the capture receipt.
The SDK commits receipts in order for source freeze, target capture, source
drain, and target release. Each receipt pins the durable input boundary and
the preceding receipt. The host observes source exit before requesting
target release. The source drains in the background and readers observe
pending work until the application and SDK queues confirm completion. When a
bounded application drain leaves known work, the same operation can resume while
SDK publishes remain available. The runtime seals SDK publishes after confirming
all application lanes empty. If an effect takes longer than an IPC reply, the
runtime keeps its durable intent and records any late proof for reconciliation.

An approved source recovery launch uses `UNS_QUEUED_HANDOVER_RECOVERY` with
the new physical launch ID, same operation ID, and durable frozen receipt
digest. The application reestablishes its closed input boundary through
`recoverSourceInput()` before SDK output resumes. The old source remains
fenced against ordinary startup. A released successor can restart normally
with identical retained queue identity. It can also become the source of a
distinct reviewed operation on the same launch and queue identity. Its exact
release receipt remains retryable until the SDK durably records the new freeze
intent. An unknown freeze, activation, or release outcome keeps its local fence
for reconciliation.

An explicitly prepared local draft can receive a new empty store on its
verified Runtime mount through `prepareEmptyRetainedStore()`. The host persists
the reviewed draft and binding intent before invoking it. The SDK initializes
required directory resources, rejects required file resources, and writes a
fresh-origin receipt before publishing the store and binding. The same draft
identity, binding, and declaration can resume an interrupted preparation. An
existing store or draft with data is preserved. `isFreshRetainedStore()` proves
origin under the application's storage lease. Applications still check their
own queue and replay-control state before lifting any startup hold. The shared
backup storage program includes the receipt when capturing the store.

## Deliberate limits

POSIX same-user, same-filesystem storage only. No cross-host transport, resource
merging, schema conversion, automatic deletion, retention finalization, or
exactly-once destination guarantee. Unknown write acknowledgements keep their
existing application semantics. Launches, configurations and artifacts changed
after review are rejected. An absent target requires the existing installation
workflow first.

Before production release, Runtime and backup tools must explicitly cover the
stable root, including a queue-excluding backup policy and full restore proof.
Moving a directory outside the previous archive layout is not backup coverage.
Consumers must be checked again against published, versioned registry packages;
a local file tarball is development evidence only.

## Runtime storage and backup extension

A bound application must now explicitly declare `storageProtocol: 1` in its
manifest and acquire `openRetainedStoreLease(binding)` before opening payloads.
Keep that handle until final writer shutdown and close it then; the SDK also pins
live leases process-wide, bounded to32. This is a participation contract, not an
inference from an installed SDK version. Older unbound applications keep their
legacy behavior. Bound sources/targets lacking the declaration are rejected by
managed startup/adoption instead of being guessed safe for offline capture.
The storage program has been tested on Node 24 with POSIX local filesystems;
application payload formats remain opaque to it.

The trusted Runtime root is `UNS_RETAINED_STORE_ROOT`; managed adoption requires
it, and an existing binding must match it exactly. No automatic rebasing of old
absolute bindings is performed. `store-resources.json` records the declaration,
including optional per-resource `backup: "payload" | "metadata"` (default payload).
An incomplete store operation or root restore blocks startup. A returned legacy
layout leaves a validated empty-store retirement marker, rather than making old
bindings appear live.

The shared `retained-store-storage.mjs` program supplies leases, offline capture
and retryable restoration without importing application modules. Runtime tools
vendor its exact bytes/limits with provenance. Capture requires all bound owners
stopped; output/input journals retain their existing distinct online protocols.
The full RTT backup includes the extra capsule inside its existing signed,
age-encrypted payload. Queue-excluding capture reports excluded payloads and
cannot be used as a complete queue restore. Restore retains store/owner/generation
identity at the same logical root on another offline volume, and leaves an applying
fence until complete. Known private partial staging files may be re-created only
from the verified snapshot; differing published work is never overwritten.

## Cross-mount transfer and recovery verification in 3.0.46

Same-filesystem rename remains the default.
For initial relocation to a separately mounted Runtime volume, the host may
explicitly select `transfer: "copy-verified"`. Stop the source cleanly **before**
reviewing this transport. Review pins content hashes, aggregate byte/file counts,
resource identities, both installed artifacts, configuration and supervisor epoch.
There is no automatic `EXDEV` fallback or caller-selected filesystem path.

The host uses `prepareRetainedStoreMoveAsync`, `applyRetainedStoreMoveAsync` and
`prepareRetainedStoreReturnAsync`. Bounded worker jobs perform opaque hashing and
copying while the host event loop can serve health and recovery reads. Persist the
complete plan before mutation and hold the existing maintenance/lifecycle fences
through the durable outcome. Jobs have a 128MiB old-generation heap and a 300s
bound; uncertainty always requires reconciliation of the same operation.

Copy stages and fsyncs each declared resource on its receiving mount, verifies
its exact contents and atomically publishes it. Source files remain unchanged and
fenced; allow space for both copies. Known incomplete scratch can be resumed from
the unchanged reviewed source. Unknown entries, links, changed source bytes and
differing published files are preserved and refused. The existing limits remain
32 stores, 100,000 entries, 64GiB aggregate files and 32GiB per file.

Return copies the **current** stopped store back. If current work differs from
the original, the original is first preserved as
`.retained-original-<store UUID>-<resource ID>` beside its legacy path. The retired
stable copy remains inert and is included by full backup; it cannot acquire a
runtime lease. No cleanup, resource merge, conversion or cross-host transfer is
implied. Runtime disk capacity and the application's destination acknowledgement
semantics remain independent operational requirements.

Offline restore now retains a verified local snapshot manifest. The data-only
`verify-restored` / `verify-root` actions rehash exact declared payloads under an
exclusive root lease before Runtime activation. A completed restore retry never
replaces newer application data. Full RTT stage/activation/completion binds the
verified root, signed backup digest and local manifest digest into its protected
receipts. Missing, excluded, unfinished or altered storage cannot enable bound
owners. After application work advances, do not activate an old snapshot: preserve
current storage and use a fresh reviewed owner transition. Historic operation
plans pin original physical files and are not rebased after a full restore.
