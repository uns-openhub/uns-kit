# Reviewed MQTT output enablement for a process group

Prepared for `@uns-kit/core` 3.0.32; not part of npm 3.0.31.
Source preparation does not confirm npm publication. This extends the existing same-host paired
upgrade API with nested input protocol 3. It does not change historical protocol 1
version reviews, protocol 2 generation reviews, or runtime policies 1–4.

The SDK owns this data-only contract and queue inspection. A host owns authenticated
review, durable intents, exact revisions, reservations, clean exits and one-use
launch/restoration permits. Applications own parsing, profiles, mappings, paths,
units and the output proxy's instance name. The host never imports application JS.

## Application declaration

An application that uses the standard `output.publishQueue` SDK option declares the
exact instance name passed to its `createUnsMqttProxy` call:

```json
{
  "unsDatahub": {
    "processGroupMqttOutputRuntime": {
      "protocolVersion": 1,
      "instanceName": "observations"
    }
  }
}
```

The application must actually pass its selected standard output channel and the
configured queue into that named SDK proxy; the declaration does not wire business
code. For example, after resolving `output` from the existing channel resolver:

```ts
const mqttOutput = await process.createUnsMqttProxy(
  output.host, "observations", config.uns.instanceMode, false,
  { ...mqttChannelParameters(output), defaultPublishOptions: { qos: 1 } },
);
```

Use QoS 1 or 2 for queued publications. Keep checkpoint advancement tied to actual
MQTT confirmation when the application uses a durable input journal.

This is capability metadata, not permission to launch or change configuration. A
power adapter and a document router can use different names and nested group
selectors without changes in the host. The SDK derives the same stable
package/process/proxy/destination identity used by its MQTT worker. Version and
placement IDs are deliberately absent from that existing outbox identity.

## Prepare two stopped successor placements

Keep the original receiver and publisher, configurations and input journal intact.
The successor pair may use the same or a different installed package version.
Retain the exact input journal UUID, directory, generation, profiles, resource
hashes, channels, process names, topic paths, units and other portable settings.
Only the successor publisher adds `output.publishQueue` and explicitly selects
`uns.handover=false` and `uns.instanceMode=force` or `wait`:

```json
{
  "uns": { "handover": false, "instanceMode": "force" },
  "output": {
    "publishQueue": {
      "mode": "durable", "storage": "runtime",
      "directory": "/var/lib/uns-openhub/mqtt-output/telemetry/combined",
      "maxRecords": 10000, "maxBytes": 16777216,
      "maxDiskBytes": 33554432, "memoryRecords": 128
    }
  }
}
```

Use an existing reviewed permanent Linux mount configured by the trusted launcher
as `UNS_MQTT_OUTPUT_STATE_ROOT`. It must be separate from input admission storage,
private and owned by the process user. Its SDK root identity must already be
initialized. An operator may explicitly initialize a new empty mount with
`openManagedOutputRoot(directory, root)` and immediately close the returned lease;
this is a storage preparation action, not part of a read-only preview. Never
initialize over unknown storage or create a second identity to bypass a refusal.
The receiver keeps its original input queue; it must not acquire an output queue.

## Review and execute

Use `reviewInstalledProcessGroupMqttOutput` for shared data-only review, not the
ordinary version or generation reviewer. The existing Controller
`ReviewRttProcessGroupUpgrade` / `ManageRttProcessGroupUpgrade` envelope stays
protocol 1; its `input` is:

```json
{
  "protocolVersion": 3,
  "source": { "publisher": "<placement>", "receiver": "<placement>" },
  "target": { "publisher": "<placement>", "receiver": "<placement>" },
  "acknowledgeInputPause": true,
  "acknowledgePossibleReplay": true,
  "acknowledgePendingOutputBlocksRollback": true
}
```

Each placeholder represents the existing placement object containing `controller`,
`rttNode`, `version` and `instanceId`. All four placements must be distinct, on the
same controller and in the same package. Each side has a paired version. Preview
returns kind `mqtt-output-enablement`, an exact fingerprint, profiles, retained
input journal and the output root/relative-directory/owner/destination identity.
It reads bounded data and does not initialize or modify either journal.

Prepare with a caller-chosen UUID and that fingerprint. Retain the UUID even when
the HTTP response is lost. Fetch that operation and use its current revision for
execute or rollback; never replace an uncertain operation with another UUID.
The host stops publisher then receiver, proves their guarded clean exits, rechecks
empty output storage, replaces the policy by compare-and-swap, then starts receiver
before publisher. Target readiness also requires the declared output database and
an active exclusive owner lease. Input work can remain pending in the same journal.

## Rollback and uncertain outcomes

A rollback first checks accepted output while the target is still alive. If output
is pending, both target processes remain available to finish it against the original
broker. Restore that broker and retry the same reviewed operation after it drains.
Then the host stops the receiver, waits for the retained input journal to empty
while the publisher is still alive, stops the publisher and inspects output again
before restoring the
original policy and starting the original receiver/publisher. It repeats the check
at the launch boundary. No queue is deleted, rebound, relocated or silently skipped.

If new pending output is discovered after the target has stopped, storage cannot
be read, a lease remains held, a root/destination changes, or an exit/launch outcome
is unknown, rollback remains blocked. Preserve the queue and reconcile the exact
operation. A stopped publisher with retained work needs a separately reviewed
recovery decision; this API does not invent a replacement launch or discard work.

Finite capacity can pause admission. Broker session retention remains an external
condition, and an interruption can lose data that never reached durable acceptance.
MQTT acknowledgement proves broker acceptance, not database commit or exactly-once
processing; uncertain delivery can replay. This does not transfer queues between
hosts or convert a previously enabled queue to a different destination.
