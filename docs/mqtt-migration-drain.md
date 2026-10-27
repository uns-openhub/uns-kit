# MQTT output drain and a deployment before the state mount

Available from `@uns-kit/core` 3.0.33. This is a Node/MQTT capability, independent
of an application's name, mappings or topic namespace. It does not transfer an
input journal, migrate a receiver/publisher pair, or grant Controller permissions.

## Declare and prepare the runtime

Declare support in the application's `package.json`:

```json
{ "unsDatahub": { "schemaVersion": 1, "mqttDrain": { "protocolVersion": 1 } } }
```

Register application cleanup before making proxies, and acknowledge readiness only
after input handlers and initial metadata have been prepared:

```ts
import { UnsProxyProcess } from '@uns-kit/core/uns/uns-proxy-process.js';

const accepted = new Set<Promise<void>>();
const runtime = new UnsProxyProcess(infraHost, {
  processName: 'inspection-service',
  handoverShutdown: {
    onRelease: () => clearInterval(refreshTimer),
    drain: async () => { while (accepted.size) await Promise.all([...accepted]); },
    timeoutMs: 30_000,
  },
});
// Create proxies, register handlers and prepare initial metadata here.
runtime.enableMigrationDrain();
// The same SDK mechanism can be called by standalone signal handlers:
// await runtime.drainForMigration();
```

Keep accepting already delivered input while the SDK makes subscribers passive.
The application hook must await accepted asynchronous work and report failures;
logging and swallowing a failed publication must not certify a clean migration.
Do not call `process.exit()` inside hooks. Retain ordinary standalone shutdown
handlers; a Controller heartbeat is not required for cleanup.

Drain stops background admission, makes subscribers passive, waits for accepted
application work, flushes output, seals further publication and closes the runtime.
The host binds its receipt to the original launch/PID and operation, then requires
the original process to exit successfully before starting a successor. Duplicate
requests share the in-flight SDK drain. Timeout, missing receipt, retained output,
or an unknown outcome blocks activation and requires reconciliation of the same
operation. Hosts must not race this handshake with a forced PM2 stop.

## Explicitly disabled persistence before a mount exists

```json
{
  "uns": { "handover": false },
  "output": {
    "publishQueue": {
      "enabled": false,
      "mode": "durable",
      "storage": "runtime",
      "directory": "/var/lib/uns-openhub/mqtt-output-state/inspection-service/main"
    }
  }
}
```

`enabled: false` uses the legacy RAM publisher and creates no output
directory. Keep explicit RAM admission limits in the application. An existing queue must be empty, belong to the same output identity,
and have no live owner. Unknown files or retained pending rows block disabling;
the SDK never deletes them. Omitting `enabled` keeps persistence enabled for an
existing `publishQueue` declaration. Enabling it later requires the reviewed
Runtime mount and its backup contract. A package upgrade does not enable it.

RAM-only mode can lose queued output on a crash. Persistent mode preserves admitted
records for replay, with at-least-once delivery. For QoS 1, MQTT completion confirms
the broker acknowledgement; it does not prove database commits or complete history.
Input received by the broker while this service is stopped depends on the client's
MQTT session/QoS and broker configuration. A drained output queue alone does not
prove uninterrupted source capture.

Existing paired input-journal deployments remain guarded. Drain and explicitly
retire their prior ownership through their supported reviewed procedure before
selecting an independent single-process configuration. Do not drop their journal
configuration to bypass an old reservation. Rollback must drain the successor's
output before returning ownership, and preserve both directories for review.
