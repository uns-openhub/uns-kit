# Reviewed process-group runtime protocol 1

Available in `@uns-kit/core` 3.0.27. This is an opt-in SDK contract for applications
and host launchers; ordinary publishers require no migration.

## Roles and ownership

| Owner | Responsibility |
| --- | --- |
| SDK | Versioned declaration and manifest schemas, policy verification, resource/channel hashes, configuration normalization, journal identity, staging and inspection, neutral receiver restart settings, snapshot reader |
| Controller/host | Authentication and RBAC, reviewed plan, durable intent, source guards, launch identity, supervision integration and recovery decision |
| Application | Source payload parsing, business mappings/units/topics, journal writer and profile checkpoints, signal/drain cleanup and publication |
| Operator | Exact private policy, storage mount and backup coverage review |

A host reads package JSON and bounded resources. It never loads application JS to
understand a group. The SDK API is shared by the host and application, so verification
and journal identity cannot drift through copied implementations.

## Data-only package declaration

```json
{
  "name": "example-transformer",
  "unsDatahub": {
    "processGroupAdmission": 1,
    "processGroupAdmissionSupervisor": 1,
    "processGroupPersistentAdmission": 1,
    "processGroupRuntime": {
      "protocolVersion": 1,
      "namespace": "telemetry",
      "publisherPath": ["transform"],
      "journalPath": ["durableInput"],
      "runtimeRolePath": ["runtimeRole"],
      "receiverRole": "receiver",
      "defaultDataGroup": "telemetry",
      "manifests": [{"id": "line-4", "file": "contracts/line-4.json"}]
    }
  }
}
```

The three feature flags advertise implemented durable admission, independent receiver
supervision and permanent storage support. They remain fixed protocol capabilities,
not per-group counters. A private reviewed policy is still required.

Selectors may be nested own-property paths (1–8 safe keys), must be distinct and
cannot overlap. Optional `match: {path, value}` selects the applicable runtime mode.
Manifests are normalized package-relative resources with unique IDs/files (up to64).
Namespace and group identifiers are data, not SDK or Controller enumerations.

Publisher projection uses `mappingFile` and `dataGroup` for an individual source,
or `groupManifest` for the target/receiver. Journal configuration uses the shared
`processGroupJournalSchema` (`directory`, `journalId`, `groupManifest`, bounded
`maxRecords`, `maxBytes`, `minFreeBytes`). Other configuration is preserved. MQTT
`infra/input/output` use the standard SDK channel resolution; no secrets are placed
in policy fields. The manifest identifies exact profile names, resources/digests,
data groups, input topic and target process. Receiver identity is `<target>-admission`.

Import schemas and helpers from `@uns-kit/core/runtime/process-group-contract.js`,
`process-group-authorization.js`, `process-group-control-contract.js`,
`process-group-inspection.js`, `process-group-supervision.js`, and
`process-group-staging.js`. Keep application-specific adapters inside the application.

## Authorization and interruption

Schema3 policies pin package name, descriptor hash, artifact hash, manifest/mapping
hashes, channel identities, controller, every placement/configuration and exact
journal/mount. Private registry: `<stateRoot>/<namespace>/authorizations/<group>.json`;
journal: `<stateRoot>/<namespace>/admission/<group>`. Declaration alone is insufficient.
A target requires its exact durable operation and launch reservation. Missing response
means reconcile that same intent, never blindly execute again. Missing policy for
retained work blocks direct-source launch. Only an explicitly authorized receiver
may initialize an unbound journal; publishers cannot initialize one.

Frozen compatibility readers for historical policies1/2 preserve their exact old
artifact/group restrictions. Future packages/groups must use protocol1/schema3.
Unknown versions fail closed. No application data or journal is rewritten by a
version bump. SIGINT/SIGTERM handling and idempotent drain remain application-owned
and must work without the Controller.

## Scope and limits

This version supports one owning controller, one package, 2–16 source publishers,
one target and one receiver, MQTT input and the existing bounded SQLite journal
format on an explicit local Linux rw mount (ext4/xfs/btrfs/zfs). Application payload
formats remain free. Other transports/storage require a separately versioned shared
adapter; a package cannot redefine protocol1 by declaration. No cross-host transfer
or exactly-once delivery is promised. Gaps before durable admission remain possible.

The snapshot asset `dist/runtime/process-group-snapshot.mjs` is the canonical
standalone reader. Native backup helpers may vendor that exact released asset with
package/version/SHA256 provenance and an equality check. It snapshots all namespaces,
including retained work, without running application code. Version1 backup capsules
retain their old group paths; group IDs must be unique across namespaces on that
node. Ambiguity fails the backup. Current conservative backup bounds remain nine
journals, 512MiB per database and1GiB per capsule; excess state fails visibly.
No live archiver spool is included by this reader.
