# Optional MQTT output persistence

Status: prepared source3.0.30. No registry, Controller,
application or production rollout is implied. Node22.13+ with node:sqlite is
required for this optional path; local acceptance uses Node24 on macOS and Linux.
Windows persistence is not validated by this rehearsal. Npm publication remains
owner-operated; existing applications require no migration.

## Scope and ownership

This capability is in the TypeScript MQTT output worker. It does not change input
subscription QoS or acknowledge incoming MQTT on an application's behalf. It is
not an automatic migration of an application's admission journal, Archiver spool,
SQL writer, Python client, HTTP endpoint or gRPC request contract.

UNS Kit owns queue storage, capacity, retry and output delivery. Applications own
business decoding, topics, units and transformations. The output record contains
the **final serialized payload** and publish options, so replay never regenerates
a timestamp, recalculates a delta or applies changed business configuration.
Controller availability is not required to stop/reopen a standalone queue.

## Select the guarantee

| Selection | Admission completes | Storage | Failure boundary |
| --- | --- | --- | --- |
| absent | Existing worker accepts into RAM | Existing behavior | Existing semantics remain |
| `buffered` | Bounded RAM admission or committed overflow record | RAM normally; SQLite on overflow, observed output failure, and orderly close | Sudden death may lose RAM work before spill |
| `durable` | SQLite WAL/FULL commit completes | SQLite before every admission acknowledgement | Confirmed admissions survive process restart with their storage intact |

Both optional modes currently publish serially with QoS1 or2 (default1). Only `qos`
and `retain` per-message options are supported. QoS0, message-expiry properties,
connection-scoped topic aliases and other publish options are rejected **before
admission**; durable retry must not silently renew expiry or claim a QoS0 ACK.
Broker acknowledgement proves MQTT delivery to the broker, not completion by an
Archiver, another subscriber, a replicated database or a physical machine.

If a connection/ACK is lost after broker receipt, replay may duplicate a message.
No exactly-once end-to-end claim is made. The final topic, payload and unit remain
unchanged. Applications requiring idempotency must carry a suitable business event
identity; a private queue row id is not a globally unique business transaction.

## Configuration

Configure this only under the standard **output** channel. Input and infra/control
schemas reject it, so process status/heartbeat traffic cannot accidentally inherit
the output backlog. An application must use the standard channel resolution and
pass its resulting parameters to the output proxy; changing JSON alone cannot
upgrade an application's hard-coded MQTT construction.

```json
{
  "output": {
    "host": "localhost",
    "port": 1883,
    "publishQueue": {
      "mode": "durable",
      "directory": "/var/lib/my-service/mqtt-output",
      "maxRecords": 10000,
      "maxBytes": 67108864,
      "memoryRecords": 256,
      "memoryBytes": 4194304,
      "maxDiskBytes": 268435456,
      "minFreeBytes": 16777216,
      "retryIntervalMs": 1000,
      "publishTimeoutMs": 10000
    }
  }
}
```

The directory must be absolute, dedicated, private (0700) and owned by the runtime
user. SQLite files are0600; a symlink at the directory/file boundary is rejected.
Use an explicit persistent mount for container replacement. A local `/tmp` directory
is suitable only for disposable tests. Provision restrictive permissions; the SDK
does not take ownership of someone else's directory or silently relocate records.
The SQLite database is only an **outbox**, not a replacement for source history.

Defaults: max10000 records,64MiB outstanding payload/accounting bytes,256MiB SQLite
file/admission budget,16MiB free-space reserve,512KiB per envelope (including topic and
accounting overhead). Byte and record bounds include the in-flight record. Admission also accounts for WAL/SHM and checkpoint growth, refusing further work
when pinned readers prevent reclaiming that budget. Filesystem metadata and
completion/checkpoint overhead still require additional filesystem space. Finite disk/queue
limits reject new admissions; they never evict old accepted work. The message cap
is a local envelope cap, not the MQTT protocol's maximum packet size.

## Application use

```ts
import { ConfigFile, UnsProxyProcess, mqttChannelParameters, resolveMqttChannel } from "@uns-kit/core";

const config = await ConfigFile.loadConfig();
const process = new UnsProxyProcess(config.infra.host!, { processName: config.uns.processName });
const channel = resolveMqttChannel(config.infra, config.output);
// One outbox owner. The old RAM/hash handover protocol cannot transfer it.
const output = await process.createUnsMqttProxy(channel.host, "output", "wait", false,
  mqttChannelParameters(channel));
await output.publishMessage("example/site/line/temperature", JSON.stringify({ value: 42, uom: "°C" }));
// Above: admitted (durable mode: disk committed), not delivered.
await output.flush(30000); // includes recovered rows, not only this process's new admissions
console.log(output.getPublishQueueStatus()); // cached metadata; no payloads or credentials
// Process-owned proxy cleanup normally goes through the owning process:
await process.stop();
```

Two independent examples use different package/process names, topics and directories
in `docs/examples/mqtt-output-persistence.ts`. No package-name or customer branches
are needed. Legacy services without `publishQueue` remain on their issued code path.

## Stop, restart and pending ownership

The stored identity binds the package name, process/proxy name and broker endpoint
(including configured server lists), but excludes credentials and package version.
A changed broker/owner is refused even with an empty existing outbox; deliberately
review a new directory rather than silently redirecting retained data. Credential
rotation is possible with unchanged destination identity. Only one worker/process
can own a directory; an OS-held SQLite lease is released when that process dies.
Before changing a directory or removing this option, drain and review retained work;
removing configuration does not automatically drain an abandoned directory.
Never share a directory between unrelated proxies or start a second writer to
bypass the ownership guard. Retained data is not automatically moved across hosts.

Orderly close persists buffered work before releasing its lease. If that fails,
the worker is retained and stop rejects; repair space/permissions and retry stop.
A drain timeout is an error, never an empty-queue acknowledgement. `stop({drain:
false})` on a standalone proxy preserves pending optional-queue work instead of
purging it. Abrupt termination in buffered mode remains a distinct loss boundary.

A delivery attempt has a finite deadline. On ACK timeout its old MQTT socket is
retired before another attempt so mqtt.js does not retain an unbounded series of
concurrent old publishes. The record remains pending until a later confirmed
attempt. A restart reopens the same file and orders old work before new admissions.
Corrupt records, schema/owner changes and storage errors block unsafe progress.

Legacy `handover=true`/skip-hash recovery cannot be combined with this new outbox;
use `handover=false` and one owning process for this first capability. The existing
separate process-group journals and transitions remain unchanged. This does **not**
add Controller host authorization, automatic migration, shared capsule backup or
cross-host outbox transfer; those need separately reviewed generic integration.
Until then arrange an appropriate consistent backup of this directory. Do not
assume the existing process-group SQLite backup contract recognizes this format.

## Acceptance

Local tests cover both guarantees, capacity, FIFO across RAM/disk, full SQLite
file, private ownership, reopen, corruption, ACK ambiguity and orderly close. The
separate loopback broker rehearsal exercises actual built worker/transport and
record replay. No production broker, credentials, data, queue or service is used.
