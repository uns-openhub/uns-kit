# Configuration-owned process-group resources

Prepared for UNS Kit3.0.29; npm publication is pending. The candidate Controller and backup
helper support the local same-generation workflow described below; this is not a
published or production-activated contract. Protocol 1 and policies
1–3 remain unchanged. Ordinary applications require no migration.

This optional protocol lets an application keep its business resources in reviewed
instance configuration instead of a release-bundled file. A mapping change can
then become a configuration operation; a new decoder still needs a code release.
The SDK does not interpret mapping rows, units, recipes or customer namespaces.

## Shared boundary

The application declares data-only selectors in package JSON under
`unsDatahub.processGroupConfigurationRuntime`. The Controller/host reads that
declaration and configuration without importing application JavaScript. The SDK
validates identities, immutable snapshots and authorization. Only the application
interprets each profile's opaque `resource` and writes its own journal.

The [worked example](examples/configuration-process-group.md) uses an inspection
adapter. SDK tests also use a document router with different selectors and resources.
Neither the SDK nor the host requires an application-name or group-name branch.

## Snapshot and journal contract

`configuration-group-contract.js` exports the descriptor/group/journal schemas,
`selectedConfigurationGroup`, `configurationGroupSnapshot`,
`configurationResourceJson`, `configurationResourceHash`, and
`configurationGroupJournalContract`. All imports use the prefix
`@uns-kit/core/runtime/` and the `.js` suffix.

A snapshot copies and deeply freezes literal finite JSON. It contains the group
digest and each opaque resource's digest. The journal-contract helper requires an
actual SDK-issued snapshot, the exact profile/resource/data-group set, and a
matching group digest. It emits journal contract version 2. Do not relabel an
existing version-1 journal or substitute a caller-constructed proof object.

Canonicalization is versioned: binary object-key ordering, retained array order,
and SHA-256 domain prefix `uns-configuration-json-v1\n`. There is no locale-dependent
sorting. Data is bounded to 1 MiB of canonical UTF-8 JSON, 65,536 visited values,
depth 32, and 2–16 profiles. Accessors, symbols, sparse arrays, cycles, class instances
and nonfinite numbers are rejected. These limits cover the complete group document,
not 1 MiB per profile. Configuration resources must be literal snapshots: compare
the runtime-resolved snapshot against reviewed authority and reject changes caused
by environment interpolation instead of quietly accepting different business rules.

Logical `group.id` and storage `generationId` are separate identities. A generation
uses `<state-root>/<namespace>/admission/<generationId>` and the corresponding
private `authorizations/<generationId>.json`. Group IDs may be reused across reviewed
revisions; journal generations and UUIDs must not be silently reused or replaced.

## Authority and launch

`configuration-group-authorization.js` defines policy schema 4 / protocol 2.
The existing shared authorization entrypoints issue live authority only after
checking the canonical private policy. `validateProcessGroupPolicyCandidate`
reviews bytes without issuing authority. A package declaration, group digest or
successful local pilot grants no permissions.

The policy pins the package/artifact/declaration, controller, exact raw configuration
for each placement, three resolved MQTT channels, group revision, generation and
journal UUID. Roles are one source per member, one combined target and one explicitly
named receiver. Only a reviewed receiver with an unbound journal may initialize.
Normal launches require the bound UUID. Target reservations additionally pin
`configurationGroupSha256` and `generationId`, plus the existing operation/launch IDs.
Retained generation directories continue fencing launches if their policy is missing.
Never log raw configuration or secret-bearing resources in diagnostics.

## Changing a revision safely

1. Validate the candidate application's business resources and preserve the prior
   configuration/policy and all pending work.
2. Let the old generation finish under its old contract, or use a separately reviewed
   transition retaining both generations. An empty old journal still keeps its identity.
3. Review a distinct generation/configuration, initialize only its receiver, inspect
   the outcome and bind its UUID. Unknown initialization outcomes require inspection;
   do not retry by creating another empty journal.
4. The host revalidates the reviewed state immediately before execution and owns the
   durable operation, leases and recovery decision. Applications keep signal/drain
   cleanup and per-profile publication checkpoints.
5. Before rollback, reconcile new-generation pending work and all physical processes.
   Do not restart an old writer after an uncertain stop or while incompatible pending
   work remains. The separate [generation transition candidate](process-group-generations.md) defines this host sequence and its recovery rules.

Local tests prove policy/snapshot fencing and application journal/MQTT recovery.
The local generic host integration selects the declared protocol, reads the exact
SQLite contract and ready receiver, stages stopped unbound configurations, preserves
group/generation fields in reservations, and uses the existing durable consolidation
coordinator. Review identity includes the generation; changes fail before stopping
sources. Existing lost-response/recovery rules apply within that same generation.
Ordinary Node/Python services without declarations retain their existing behavior.

The shared standalone snapshot asset accepts journal contracts 1 and 2. Version 2
stores each `profiles.contract` row as the same `{processName, resourceSha256,
dataGroup}` projection as the outer journal contract. The reader verifies these
bindings and generation before producing a self-contained SQLite snapshot. A local
mounted-Linux proof covers two actual application generations and an unrelated
retained file journal, encrypted/signed capture, independent payload/checkpoint
read-back and reopening after a container SIGKILL. The helper never executes
application code to capture state. Archiver event spools remain excluded.

The later [generation transition candidate](process-group-generations.md) adds a
reviewed separate-generation operation and symmetric pending-work drain/rollback.
A later [configured paired version candidate](process-group-version-upgrade.md#configuration-owned-resources)
adds unchanged-generation version upgrades through the same shared host procedure.
It preserves the existing journal and checkpoints; resource changes still need a
separate generation. The old file upgrade workflow stays unchanged. Current backup limits also
remain unchanged (nine journals total and unique flat snapshot group identifiers
across namespaces); ambiguous layouts fail capture rather than being omitted.
Recovery remains offline review with no automatic replay. Production activation
requires the completed local rehearsals to be repeated with published SDK/helper artifacts,
frozen registry consumer locks and a separately authorized rollout.

Consumers composing exported Zod schemas must align the SDK/application Zod
resolution and run schema generation plus typecheck from their coordinated lockfiles.
Local acceptance uses 4.4.3, matching both existing frozen builds; a fresh unpinned
consumer selected 4.6 and exposed incompatible schema type versions. A source symlink
alone is insufficient packaging evidence. Recheck this boundary with the actual
published SDK and downstream frozen lock before release.

This remains same-host MQTT admission with possible replay after an ambiguous
publication. It does not promise exactly-once output, atomic external side effects,
cross-host queue migration, or recovery of input never admitted by the receiver.
