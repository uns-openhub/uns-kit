# Runtime-managed MQTT output state (3.0.31)

This optional SDK capability is shared by every TypeScript microservice. No
application name, business profile or destination topic is interpreted by the
Controller or the backup helper. It extends the optional 3.0.30 MQTT outbox;
ordinary applications without `output.publishQueue` keep their existing behavior.

## Configuration and ownership

Use `output.publishQueue.storage: "runtime"` with a literal normalized directory
under the trusted `UNS_MQTT_OUTPUT_STATE_ROOT` mount. Runtime tools use
`/var/lib/uns-openhub/mqtt-output`; the two directory components beneath it are
an application namespace and a stable queue ID. Both use lowercase letters,
digits and hyphens, beginning with a letter, at most 100 characters each.

```json
{
  "output": {
    "host": "mqtt.example.invalid",
    "port": 1883,
    "publishQueue": {
      "mode": "durable",
      "storage": "runtime",
      "directory": "/var/lib/uns-openhub/mqtt-output/meters/energy",
      "maxRecords": 10000,
      "maxBytes": 67108864
    }
  }
}
```

An unrelated vision service uses the same standard setting with
`/var/lib/uns-openhub/mqtt-output/vision/observations`. It needs no Controller
extension, custom descriptor or knowledge of the meters service. The application's
existing standard channel resolution passes the option to the SDK MQTT publisher.

The root must be a private 0700 directory owned by the service UID, an actual
Linux mount, with a supported local filesystem (ext4/xfs/btrfs/zfs). Runtime
provisions a distinct **external volume for every Controller**. Never share a
volume between live Controllers. Provisioning and changing a volume is an
operator action; the SDK never creates or silently substitutes a Runtime mount.
Node 24 with `node:sqlite` is required.

The SDK root guard binds a UUID; each queue binds that root, its relative path,
and the original owner/broker identity. Queue files use private permissions.
Live owners hold shared root leases and exclusive queue-owner leases; restore
requires the exclusive root lease. Another process cannot adopt the same queue
while its owner remains alive. Changing an owner or destination requires separate
review, rather than copying rows into a different identity.

`storage` absent/default `local` keeps the 3.0.30 schema 1 behavior. Controller-managed
queue starts require `runtime`; an older 3.0.30 strict config parser rejects the new
`storage` field, and its store refuses schema 2. Do not remove the option to force
an older binary to accept restored state. Existing schema 1 queues are preserved
and require an explicit migration; this feature does not relabel them.

## Capture and recovery

The SDK ships a data-only `runtime/mqtt-output-state-program.mjs` and fixed
`mqtt-output-state-contract.json`. Native helpers vendor these exact assets with
package/version/digest receipts. They never import or execute application code.
The helper reads only standard `output.publishQueue` configuration to require
coverage of every configured queue; an absent mount/queue or invalid snapshot
fails the backup. Retained queues without an active configuration are included.

Each SQLite online backup produces a standalone, checksum-verified database.
A signed/encrypted full RTT backup carries a bounded `mqtt-output-state-v1.tar`
capsule and matching protected/public coverage. The capsule preserves serialized
MQTT envelopes, IDs, order, values, times, units, QoS and retain flags. Original
queues are not drained, moved, deleted or acknowledged during capture. Snapshots
are consistent per queue, not one atomic snapshot of several application queues.

Fixed aggregate limits: 32 queues, 100,000 records per queue, 512 MiB snapshot database
bytes, 256 KiB manifest and 1 GiB capsule. Application admission bounds remain in force.
A bound, corrupt row, missing configured queue or extra/unsafe file refuses capture
or restore, without declaring success or deleting pending work.

Restore uses a verified signed backup and its exact manifest checksum, the original
age identity, an explicit source-stopped confirmation and an empty private mounted
target. It verifies all bytes before use, stages files, records an applying marker,
then publishes a complete marker. Applications refuse to start on an incomplete
root. Repeating the same operation may reconcile already published exact files;
an inconsistent partially copied staging file remains blocked for manual review.
Unknown target files, another snapshot, changed bytes, a live root owner and
unreviewed replacement of state are refused. Restoring files does not start services
or send MQTT messages. Restart the same reviewed application/owner/configuration
only after the source is stopped and restore is complete.

Use a separate retained source volume for rollback. There is no merge of two
queues, transfer of an active owner, automatic cluster migration or automatic full
RTT activation. Older RTT-only restore deliberately refuses a backup containing
this state. Tools provide a separate reviewed state-restore command.

## Delivery and backup limits

`durable` local admission reaches disk before returning control. Broker completion
is separate; use `flush()` before advancing application input checkpoints.
`buffered` can retain accepted work only in RAM. Gracefully close/quiesce that
application to spill its RAM before a complete pending-work backup. Live capture
covers **disk rows only**, not RAM, in-flight packets or upstream broker queues.
A power loss, unknown MQTT acknowledgement or restore of an older snapshot can
produce replay. This is bounded at-least-once delivery, not exactly-once delivery
or zero-loss input MQTT. Consumers requiring deduplication need their own stable
business IDs. Existing input journals and non-MQTT outputs remain separate.
