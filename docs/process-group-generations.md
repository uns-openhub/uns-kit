# Reviewed process-group journal generations

Status: SDK3.0.29 release preparation; npm publication is pending. Controller integration remains a local candidate. This guide describes the
shared contract; it is not production activation authority or a registry release.
See [configuration resources](process-group-configuration-runtime.md) for declarations,
opaque resources, journal contract 2 and policy 4.

A resource revision must not reinterpret pending work. Even an empty old journal
keeps its original UUID and contract. The generation transition uses two separately
reviewed publisher/receiver pairs and retains both journals and both canonical
policies. It never copies tasks or changes an existing journal's resource binding.

## Shared input and review

`processGroupUpgradeInputSchema` accepts the historical version input (protocol 1)
or `processGroupGenerationInputSchema` (protocol 2). The surrounding management
request remains protocol 1; only its nested `input` selects generation semantics.
The host owns RBAC, persistence, leases, reservations and physical process control.
The SDK supplies data-only review and storage verification. A new adapter must not
require package-name branching in the host.

```ts
import { processGroupGenerationInputSchema }
  from '@uns-kit/core/runtime/process-group-generation.js';

const placement = (instanceId: string) => ({
  controller: 'local-controller', rttNode: 'inspection-adapter',
  version: '1.1.0', instanceId,
});
const input = processGroupGenerationInputSchema.parse({
  protocolVersion: 2,
  source: {
    publisher: placement('current-publisher'),
    receiver: placement('current-receiver'),
  },
  target: {
    publisher: placement('next-publisher'),
    receiver: placement('next-receiver'),
  },
  acknowledgeInputPause: true,
  acknowledgeResourceReview: true,
  acknowledgePossibleReplay: true,
});
```

There must be four distinct placements for one package and owning controller. Each
pair uses one installed version; the two pairs may use the same or different versions.
The source may use registered file resources or configuration resources. The target
must use configuration resources and a distinct canonical directory and UUID.

`reviewInstalledProcessGroupGeneration` reads bounded installed data and private
policies without importing application JavaScript. Its review includes separate
source/target journal references and `resourceChanges`: logical profile, data group,
old digest and new digest. These digests describe identity, not semantic equivalence.
The application owner must review paths, units, interpretation and any intended
business changes independently. `acknowledgeResourceReview` records that obligation.

The logical group, publisher and receiver process names, profile names/data groups,
input topic, resolved broker identities and portable configuration outside declared
resources must match. Journal capacities/headroom must match too. Each side requires
its own complete current canonical policy. Neither declaration nor acknowledgement
creates a policy or grants launch authority.

## Preparation and execution

1. Preserve the old configuration, policy, journal and UUID. Validate the candidate
   resources in the application, including paths and units.
2. Prepare and authorize a separate stopped target pair through the existing
   installation/configuration procedure. Explicitly initialize its receiver once,
   inspect the result and bind its journal UUID. Do not launch the target pair.
   Unknown initialization outcomes need inspection; do not create a substitute journal.
3. Review the four placements. The source pair must be ready and guarded. The target
   must be never-launched and pristine: no tasks/bytes, admitted or completed events,
   or nonzero progress. Missing/partial journals, inconsistent counters and stale
   policies fail review. Mutable source backlog counts are not a plan fingerprint.
4. Prepare with a caller-supplied operation UUID and the exact preview fingerprint.
   Reuse that UUID after a lost response. Execute with its current revision. The host
   revalidates before stopping anything and reserves the same process-name resources
   used by ordinary migrations and consolidation.
5. Stop the old **receiver first**. Leave its publisher running until all admitted
   tasks are confirmed complete. Then stop the publisher and recheck clean exits and
   an empty journal. A pending or inconsistent journal pauses the same operation.
6. Verify both retained policies, then start the new receiver and publisher with
   durable one-use launch intents. Recheck the old journal before each launch.

Stopping input can lose external MQTT messages that were never admitted. Broker
session/QoS/retention guarantees are separate. `acknowledgeInputPause` explicitly
accepts this pause; the operation does not claim lossless input or exactly-once output.
Uncertain output may retain work for replay under its original contract. A stopped
receiver's persistent broker session may continue collecting QoS messages while the
successor runs. On rollback it can deliver values the successor already published.
The preview exposes this delivery limit and `acknowledgePossibleReplay` is mandatory.
Do not infer business deduplication from per-journal MQTT packet receipts. The local
rehearsal explicitly verifies 33 unique readings and 11 such repeated publications,
with their original times/values/units, without deleting broker-session data.

## Interruption, lost response and rollback

Intent is durable before a stop/start is sent. A confirmed effect with a lost reply
is reconciled through that operation. An intent without a confirmed physical outcome
blocks; it is not sent a second time. Creating another operation does not bypass the
resource reservation. An uncertain target launch blocks rollback too.

Rollback stops the new receiver first, lets the new publisher finish its generation,
then stops it. Only after clean exits and an empty successor journal may the exact
old receiver/publisher be restored through existing one-use restoration permits.
Both canonical policies and both journal identities remain. Pending work must never
be moved into the other generation to make rollback appear complete.

The initial candidate verifies actual MQTT/application publication, SQLite state and
PostgreSQL operation persistence with an owned child-process adapter. A later isolated
Linux rehearsal also exercises the normal authenticated Caddy/GraphQL path, real
RttManager/PM2 guards and restoration permits, explicit mounted journal preparation,
Controller restart with pending work and same-operation recovery. It verifies44
unique readings and55 exact publications, including11 broker-session repeats.
These are local candidate checks, not issued-artifact or production activation,
cross-host queue transfer, or a new-generation archiver/QuestDB history proof. A [same-journal configured paired-version upgrade](process-group-version-upgrade.md#configuration-owned-resources)
is also implemented as a later local candidate through the version-only path;
this resource transition deliberately uses a new pristine generation.

Before release, publish the coordinated SDK, update real registry dependency locks,
recheck packed/frozen consumers and helper provenance, then repeat the installed
host rehearsal from those issued artifacts. PR/CI, immutable releases and production rollout remain separate steps.
