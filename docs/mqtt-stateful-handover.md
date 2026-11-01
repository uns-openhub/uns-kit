# State-preserving MQTT handover

This opt-in SDK protocol transfers the in-memory counter baselines and publication
sequence state owned by `UnsMqttProxy`. It is independent of application names,
profile selectors, paths, and units. It does not migrate arbitrary application
state, disk output queues, or a durable database. Keep raw counter publication
as the preferred recoverable source; an abrupt crash still loses RAM state.

An application declares `unsDatahub.mqttHandover.protocolVersion: 2` and
`unsDatahub.mqttDrain.protocolVersion: 1` in package.json. Reviewed source and
target configuration use `uns.handoverProtocol: "correlated-v2"`. Pass that
configuration field to `new UnsProxyProcess(host, { processName,
handoverProtocol, handoverShutdown: { drain: () => acceptedWork.drain() } })`.
Each MQTT proxy also declares a stable `handoverStateContract`, for example
`meter-counter-kwh-v1`. This is a semantic compatibility revision, not the package
version. Change it for changed scaling, counter meaning, reset policy or parsing
semantics. The SDK binds it to the destination, source subscription and proxy
identity; changing these blocks a live transfer before activation. A new package
version may reuse the contract only when these semantics remain compatible.

Added paths have no prior baseline and removed paths are not published again.
A changed unit/type establishes a new baseline instead of subtracting incompatible
values. This protects arithmetic; it does not authorize a schema/unit migration or
convert existing archived values. Never infer compatibility from matching path
names alone. Review semantic changes through the existing contract migration.

Register `enableMigrationDrain()` only after all input handlers are installed.
The application drain must await every accepted asynchronous publication;
callbacks that silently discard failures do not satisfy this contract. Stop timer,
polling and other background admission through `handoverShutdown.onRelease` before
draining. The transferred baseline must not change after the state boundary.

The host assigns a unique handover ID and fences the old process against restart.
The successor subscribes passively and advertises its compatibility offer. A
different offer is rejected while the source remains active. The source then freezes subscriber admission,
waits for accepted work and broker acknowledgements, and sends a bounded,
versioned snapshot. The successor verifies the exact peer and operation ID,
proxy set, digest, and unique overlap of topic + payload before importing the
baselines and activating input. A replay only resends acknowledgement; it cannot
reset the baseline. The source stays passive until an acknowledgement of that
exact digest permits its clean exit. Missing, ambiguous, oversized or incompatible
state blocks the transition; a successor with a reviewed ID never self-promotes.

The original format requires the last three delivered input packets. The
3.0.38 format3 described below supports a shorter uniquely identifiable shared
suffix. Streams without an identifiable shared overlap cannot use either form.
This is not a claim of universal MQTT exactly-once delivery: duplicates/loss before SDK admission,
broker session limits and crashes remain separate concerns. On an unknown result,
reconcile the same operation; do not force-start another writer.

Legacy correlated-v1 remains unchanged and does not acquire delta continuity.
A return to the original controller must transfer the current owner's state in
another reviewed handover. Restarting the old process from its former RAM state
is not a valid return. Persistent queues and other local runtime state continue
to require their separate drain/transfer protocols.


## Minimal application wiring

```ts
const accepted = new Set<Promise<unknown>>();
let publicationFailed = false;
const drain = async () => {
  while (accepted.size) await Promise.allSettled([...accepted]);
  if (publicationFailed) throw Error("Accepted publication remains unconfirmed");
};
const runtime = new UnsProxyProcess(config.infra.host, {
  processName: config.uns.processName,
  handoverProtocol: config.uns.handoverProtocol,
  handoverShutdown: { drain, timeoutMs: 30000 },
});
const input = await runtime.createUnsMqttProxy(config.input.host, "input",
  config.uns.instanceMode, config.uns.handover, {
    mqttSubToTopics: [config.input.topic],
    handoverStateContract: "meter-parser-v1",
  });
const output = await runtime.createUnsMqttProxy(config.output.host, "output",
  config.uns.instanceMode, config.uns.handover, {
    handoverStateContract: "cumulative-kwh-v1",
    defaultPublishOptions: { qos: 1, retain: false },
  });
// Admission can succeed before the RAM publisher reports a delivery failure.
output.event.on("error", () => { publicationFailed = true; });
input.event.on("input", event => {
  // Application owns parsing, paths, units, scaling and validation.
  const work = output.publishMqttMessage(parseReading(event), MessageMode.Both);
  accepted.add(work);
  void work.catch(() => { publicationFailed = true; })
    .finally(() => accepted.delete(work));
});
runtime.enableMigrationDrain();
```

Keep the application's existing standalone signal cleanup and call
`drainForMigration()` from it; do not swallow a failed drain. If parsing or
scaling changes between application versions, change the semantic contract.
A release number alone neither grants nor prevents compatibility.

The completed SDK handover emits a process-local, synchronous drain receipt with
the operation ID and state digest. The managed host persists it against its
original launch fence before exit. A write failure is not a successful migration.
An unmanaged application can use the SDK protocol, but it does not obtain the
Controller's durable operation history or restart protection automatically.


The control broker must authenticate clients and restrict access to the relevant
handover/status topics. Operation IDs, process IDs and hashes are correlation and
integrity checks, not signatures or a substitute for MQTT authorization.

## Unitless data and background publication (3.0.36)

State can contain strings and numbers without a physical unit. The absent `uom`
remains absent on the wire and after import. Empty string and a named unit remain
distinct. A format discriminator in the compatibility digest prevents an older
required-unit reader from accepting this state after the source has frozen.
Mixed 3.0.35/3.0.36 peers therefore refuse a live transfer before freeze, including
numeric-only applications. Perform a separately reviewed initial cold upgrade;
subsequent moves between updated peers preserve state normally.

A passive successor starts with empty calculation/sequence state. Background
metadata must wait for ownership, even if the transport is connected. For example:

```ts
let acceptingBackground = true;
let nextRefresh = 0;
const timer = setInterval(() => {
  if (!acceptingBackground || !runtime.isActive() || Date.now() < nextRefresh) return;
  nextRefresh = Date.now() + 86400000;
  trackAccepted(publishMetadata());
}, 250);
// In handoverShutdown.onRelease: acceptingBackground = false; clearInterval(timer).
// In handoverShutdown.drain: await every tracked publication; propagate failures.
```

Use the existing accepted-work tracker; do not drop input callbacks or replace
SDK drain with a timer. `isActive()` does not prove delivery or database commit.

A RAM-only publisher can reject or lose unacknowledged packets during a destination
outage. Reconnection does not prove those packets were delivered. Keep an observed
publication failure latched in the application drain and block a clean migration
receipt, including a repeated request after reconnect. Review the uncertain range
before a separate recovery; never clear this latch merely to complete a move.

## HTTP readers and disconnects (3.0.39)

Applications explicitly distinguish read-only work from effects. A request's
method or a caller's header is not evidence that it is safe to abandon. For an
endpoint that only reads, use the API proxy helper before starting asynchronous
work:

```ts
api.event.on('apiGetEvent', async ({ req, res }) => {
  try {
    await api.runReadOnlyRequest(res, async () => {
      // Authenticate and enforce topic/tenant access as usual.
      await authorize(req);
      // The application owns cancellation and awaits all admitted work.
      const rows = await readRows(req, res);
      if (!res.destroyed) res.json({ rows });
    });
  } catch (error) {
    if (!res.destroyed && !res.headersSent) res.status(503).json({ error: 'Read unavailable' });
  }
});
```

Use the wrapper exactly once on the original admitted response. All work must
settle inside it; do not start detached operations. It remains available in
standalone mode. During a hot transfer, new requests stay rejected while the
source is passive. For an admitted read, disconnect or an early response does
not finish its work: drain waits for the callback. A completed or failed read
then permits drain without asserting that the client received the response.
The same rule applies to a read-only POST batch/search endpoint after reviewing
its behavior. Never apply it to a route that can write or publish.

Unmarked requests retain conservative behavior. Disconnect before confirmed
response completion leaves their outcome unknown and blocks handover. A late
declaration cannot clear that latch. Reconcile business effects under the
original operation instead of retrying or discarding the uncertainty.

## Sparse input and late subscribers (3.0.38)

No application-name or topic-name exceptions are needed. The SDK first obtains a
fresh passive-input preview from each worker, after current MQTT subscription
acknowledgements. An output-only proxy has an empty input boundary; it still needs
a connected transport. Its absence of input subscriptions is not an input fault.

Before releasing input, the source checks that its recent processed tail and the
successor's buffer have a uniquely identifiable ordered common suffix. A successor
that missed older source packets can therefore continue after one new common
packet, when that packet is unambiguous. Missing or ambiguous overlap leaves the
source active; the target sends a bounded series of fresh offers. It does not
promote itself on expiry. Quiet sources may need a real new input packet before
a live move can be proven. Do not inject fake business readings to bypass this.

After freezing, the source transfers its actual tail and calculation state. Every
successor input queue validates that frozen boundary against its whole buffer
before any state is imported or any input is consumed. The preview is a hint, not
an activation permission. A late packet can allow another read-only preparation
on a repeated FIN. Once any state import or activation has begun, an error may
have an unknown effect: the SDK blocks further application rather than replaying
the import. A completed duplicate FIN only replays the same receipt.

Fingerprints include topic and exact payload. The implementation does not dedupe
equal measurement values or timestamps. Identical input packets remain separate
queue entries; a repeated boundary that cannot be uniquely located blocks the
move. Source history is bounded to 10 packets and previews to 100 fingerprints;
it does not copy an unbounded queue into the control message.

Format3 is included in the compatibility digest. Mixed format2/format3 stateful
peers refuse before source freeze, even if their business contract is identical.
Plan a reviewed initial stop-before-start SDK upgrade on one host, then test live
moves between matching updated peers. Controller protocol2, standalone cleanup,
legacy readers and persistent-output ownership are separate and unchanged.

### Repeatable application check

Use isolated input/output brokers and synthetic data, keeping the application's
real parser, paths, time conversion and units:

1. Start one active source and publish several acknowledged batches before the
   successor exists. Record every expected output tuple and packet multiplicity.
2. Prepare a fresh stopped target through the host's normal preview and request
   one reviewed live move. Deliberately discard the HTTP response.
3. Once the passive target subscribes, send one new uniquely identifiable packet
   and then leave the stream quiet. Reconcile the original durable operation ID.
4. Compare every output's topic, source time, value and unit against the input;
   check original exit0, the bound SDK drain receipt and one active successor.
5. Return ownership through another reviewed hot move into a fresh stopped
   instance. Do not remove the old fence or restart its stale RAM baseline.
6. Repeat with equal values at different times and legitimate identical packets.
   Verify that replaying a completed operation does not change the target PID.

Also test absent/ambiguous overlap and delayed input. A successful bounded test
proves that case; it does not prove arbitrary broker ordering, replay of missing
QoS0 messages, crash durability, archive commits or universal exactly-once MQTT.
