# Upgrading an already grouped runtime

Added in UNS Kit 3.0.28. It does not change protocol 1 application behavior
or require business adapters to implement an alternative upgrade protocol.

Read [the ownership rule](controller-runtime-boundary.md) first. The SDK validates
data and installed resources. The authenticated host owns review, durable intent,
membership, launch authority and recovery. Application code continues to own its
input decoder, profile mappings, output paths, units and journal writes.

## Scope

One package on one owning Linux host; an existing grouped publisher and independent
receiver; two stopped target instances on another installed version; the same bound
local MQTT/SQLite journal. This is a paired version upgrade, not another N-to-one
consolidation and not a cross-host queue transfer. Both roles must move together
because the private runtime policy pins one artifact root and exact placements.

The new SDK functions live in `@uns-kit/core/runtime/process-group-upgrade.js`:

- `processGroupUpgradeInputSchema` and `processGroupUpgradeRequestSchema` validate
  bounded, versioned selections. No package or customer names have special meaning.
- `reviewInstalledProcessGroupUpgrade` compares configurations, channel identities,
  installed manifests/mapping digests, artifact hashes, exact policies and the
  existing journal UUID/contract. It reads journal metadata with a read-only SQLite
  connection and never imports application JavaScript.
- `inspectProcessGroupUpgradeJournal` observes exact receiver/profile ownership by
  PID/start identity in the current Linux namespace. Merely starting PM2 is not
  proof that the journal is ready.
- `replaceReviewedProcessGroupPolicy` performs an atomic compare-and-swap of exact
  reviewed bytes. Only the host may decide when proven exits permit that switch.

`validateProcessGroupPolicyCandidate` reviews a proposed generic policy without
issuing runtime authority. Actual startup still reads the active canonical policy
and requires an exact operation reservation and launch identity. Historical policy
readers stay frozen; a compatible new artifact uses schema 3.

## Host procedure

1. Prepare stopped publisher and receiver configurations. Preserve their journal
   directory/UUID and logical process names; keep target placement IDs distinct.
2. Preview both roles and rollback together. Review the fingerprint and unchanged
   configuration/resource identities. A declaration alone grants no permission.
3. Prepare one durable operation using a caller-generated UUID. Retry a lost
   preparation response with that same UUID and fingerprint.
4. Stop the old publisher, proving drain/exit, then the old receiver. Intake can
   accumulate in the bounded journal until the receiver stops. Keep all data.
5. After proving both exits, activate the reviewed target policy. Start the target
   receiver and confirm its exact journal ownership, then start the publisher.
6. On lost response or helper interruption, read the same operation. Reconcile
   existing request/launch identities; never invent another start to bypass an
   unconfirmed outcome. Changed configuration, policy or membership blocks progress.
7. For rollback, stop and fence the target publisher and receiver, prove both exits,
   restore the exact original policy bytes, then restore the original receiver and
   publisher with one-use permits. Reuse the same journal and checkpoints.

The host retains private original/target policy snapshots under the existing
registry's `.upgrades/<operation-id>/` directory. These are not additional active
policies. They must remain available for local operation recovery. This slice does
not add whole-backup restore coverage for in-progress upgrade snapshots.

## Controller API example

The local Controller candidate exposes `ReviewRttProcessGroupUpgrade(input: Json!)`
for administrators/operators and `ManageRttProcessGroupUpgrade(input: Json!)` for
administrators. The owning controller must advertise
`rttProcessGroupUpgradeProtocol: 1`; older peers are explicitly refused.

```json
{
  "protocolVersion": 1,
  "controllerName": "controller-1",
  "action": "preview",
  "input": {
    "protocolVersion": 1,
    "source": {
      "publisher": {"controller":"controller-1","rttNode":"meter-transformer","version":"1.0.0","instanceId":"old-publisher"},
      "receiver": {"controller":"controller-1","rttNode":"meter-transformer","version":"1.0.0","instanceId":"old-receiver"}
    },
    "target": {
      "publisher": {"controller":"controller-1","rttNode":"meter-transformer","version":"1.1.0","instanceId":"new-publisher"},
      "receiver": {"controller":"controller-1","rttNode":"meter-transformer","version":"1.1.0","instanceId":"new-receiver"}
    }
  }
}
```

`prepare` adds `id` and `expectedFingerprint` to that request. `get` uses the same
`id`; `execute`/`rollback` add the latest `expectedRevision` and the unchanged
`expectedFingerprint`. A command response is an acknowledgement of durable intent;
poll `get` for the actual state. Ordinary single-instance migration must explicitly
block a grouped runtime instead of presenting an incomplete green preview.

Outage before MQTT admission can lose source data when the broker does not retain
it. Accepted journal entries survive the paired upgrade/rollback, but output replay
can be at least once. No exactly-once guarantee or automatic resolution of unknown
launch outcomes is implied. Standalone signal/drain behavior remains application-owned.
