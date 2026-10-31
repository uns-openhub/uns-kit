# Application lifecycle and state handover

The SDK owns ownership, correlation, queue boundaries, HTTP admission and bounded
state transport. Applications own parsing, side effects and the meaning of their
RAM state. Controller orchestrates and reconciles the operation; it does not need
a branch for each application name.

## Prepare one process owner

```ts
import { ProcessWork, UnsProxyProcess } from '@uns-kit/core';
import '@uns-kit/api';

const accepted = new ProcessWork();
const runtime = new UnsProxyProcess(config.infra.host, {
  processName: config.uns.processName,
  handoverProtocol: config.uns.handoverProtocol,
  deferHandoverUntilReady: true,
  handoverShutdown: {
    onRelease: () => stopApplicationTimers(),
    drain: () => accepted.drain(),
    onActivate: async () => { await prepareOwnedApplicationWork(); },
  },
});
// Install MQTT handlers, API routes and cron consumers before declaring readiness.
// Transport-admitted callbacks must finish even after ownership release:
input.event.on('input', event => {
  void accepted.track(publishTransformed(event)).catch(reportUnconfirmedWork);
});
// Gate new background work. Never use this gate to discard accepted MQTT input.
const timer = setInterval(() => {
  void accepted.run(async () => {
    if (runtime.isActive()) await pollAndPublish();
  }).catch(reportUnconfirmedWork);
}, 1000);

// API-only processes explicitly prepare the same owner without a fake data proxy.
await runtime.prepareHandover(config.uns.instanceMode, config.uns.handover);
runtime.enableMigrationDrain();
runtime.installSignalHandlers(); // Use only when the app has no existing signal owner.
```

The readiness declaration starts discovery only after setup. API proxies defer
route/catalog advertisements while passive, reject new requests with a retryable
503, wait for accepted requests and close their HTTP server on shutdown. Cron
tasks do not execute on a stateful standby. Stop application-owned timers at
release and await their accepted work. A failed or uncertain effect stays latched;
reconnection alone cannot make its drain successful.

## Preserve application-owned RAM

```ts
handoverState: {
  contract: createApplicationHandoverContract('window-accumulator-v1', { windowMs, unit }),
  exportState: () => ({ samples, windowStartedAt }),
  validateState: value => windowSchema.parse(value),
  importState: value => {
    const restored = windowSchema.parse(value);
    samples = restored.samples;
    windowStartedAt = restored.windowStartedAt;
  },
}
```

Import `createApplicationHandoverContract` from `@uns-kit/core`. Select only
business resources that define the state: aggregation windows, mappings, units
and workflow definitions. The helper hashes canonical bounded JSON; it does not
transmit configuration or credentials. Changing a resource rejects the old state
before release. Object property order is immaterial; array order is preserved.
Do not include host addresses or secrets; a host migration should remain compatible.

Every proxy still declares its own `handoverStateContract`. The SDK binds the
application contract into proxy compatibility, rejects different contracts before
releasing the source, drains accepted work, validates the complete snapshot, then
restores it before activating successor input. API-only owners can transfer an
empty proxy set through the same protocol. Duplicate completion messages only
replay acknowledgement; they do not import the state again.

State is plain JSON, at most 768 KiB, depth 32 and 50,000 nodes inside the existing
1 MiB envelope. Oversized state is rejected, never truncated. Keep credentials,
sockets, promises and file paths out of snapshots. Use semantic contracts for
meaning/scaling changes, rather than a package version. Business validators must
check every component before import and reject stale definitions.

An aggregation window needs its un-emitted samples and time boundary. A workflow
needs its active session identity, counters, conditions and remaining timeout.
An authoritative database and a rebuildable read cache do not need to be copied
wholesale. Include every cached operand that changes workflow decisions, preserve
its absence and original timestamps, and reconstruct unrelated reads normally.

Disk queues require their separate reviewed drain/transfer protocol. This feature
does not transfer a database, replay arbitrary source events, provide broker
durability, or guarantee exactly-once external effects after an unknown response.
Review the same durable operation after response loss; never force-start another
writer. Return ownership with a fresh reviewed reverse handover.
