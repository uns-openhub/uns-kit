# State-preserving MQTT handover (3.0.35)

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

The overlap is based on the last three delivered input packets. Streams without
an identifiable shared overlap cannot use this handover. It is not a claim of
universal MQTT exactly-once delivery: duplicates/loss before SDK admission,
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
