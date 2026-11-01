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
import { UnsProxyProcess } from '@uns-kit/core';

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

## Async input and a full RAM publisher (3.0.40)

An event listener that starts async work without awaiting it can empty an input
queue into thousands of concurrent publications. The publisher limit still
protects memory, but rejected work cannot be called a clean migration. Use the
same opt-in SDK mechanism for any source adapter:

A complete SDK-only example is [managed-mqtt-forwarder.mjs](examples/managed-mqtt-forwarder.mjs).
It uses a project's own package identity and does not import any microservice.

```ts
const output = await runtime.createUnsMqttProxy(outputHost, 'observations', mode, true, {
  handoverStateContract: 'inspection-observation-v1',
  defaultPublishOptions: { qos: 1 },
  maxPendingPublishes: 1000,
  publishBackpressure: { maxWaiting: 32 },
});
const input = await runtime.createUnsMqttProxy(inputHost, 'frames', mode, true, {
  handoverStateContract: 'inspection-frame-v1',
  mqttSubToTopics: ['factory/inspection/frames'],
  managedInput: true,
});
input.setInputHandler(async event => {
  for (const observation of decodeFrame(event.message)) {
    await output.publishMqttMessage(observation);
  }
});
runtime.enableMigrationDrain();
runtime.installSignalHandlers({ retainOnFailure: true });
```

One input remains owned by the worker until this callback completes. Output
capacity waiting is FIFO and bounded; it does not retry the callback, increase
the output limit or move its pending data into an unbounded promise backlog.
Wait only on capacity known not to have admitted this particular publication.
Actual publisher/worker failures still block clean migration. A failed handler
retains its input for review; never retry it automatically after a partial effect.

For ordinary stop, the SDK awaits broker unsubscribe acknowledgement and drains
already accepted owned input before flushing output and closing workers. For hot
handover, it takes the normal verified input boundary instead. Stop on a passive
shadow never executes its redundant inputs. A timeout retains work and gives no
clean receipt; after the destination recovers, reconcile/retry the same stop.
The retain-on-failure signal owner coalesces concurrent signals and allows a
later retry. Do not combine it with custom process signal handlers.

`enableMigrationDrain()` also installs the launch-bound SDK cooperative-stop
handler (`uns-kit:mqtt-stop:v1`). Its prepared readiness event advertises
`cooperativeStopProtocol: 1`. A supporting Controller binds that evidence to the
exact PM2 launch and sends the shared request instead of arming PM2's forced-stop
timer. It reports success only after a natural zero exit; timeout leaves the
same owner available for retry. Migration fences continue using their existing
guarded drain, and are never removed by ordinary Stop.

This needs the corresponding generic Controller support. A legacy host's
`pm2.stop` may still send SIGKILL after its deadline; an SDK handler cannot
prevent SIGKILL or preserve RAM after it. Upgrade the host before relying on
retention for a blocked ordinary Stop. The cooperative protocol itself does not
authorize a caller: platform authentication and exact launch ownership remain
Controller responsibilities.

RAM data is still lost if the process is killed. This is neither a durable input
journal nor a promise that a stopped subscriber receives future broker events.
Enabling managed input changes compatibility, so review an initial cold upgrade
before migrating matching managed peers. Existing non-opt-in services are unchanged.

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
