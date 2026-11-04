# Retained stores: opt-in same-host adoption

Prepared in UNS Kit 3.0.45; npm publication and coordinated Controller/Runtime
releases are separate steps. Existing applications do not change behavior.
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
