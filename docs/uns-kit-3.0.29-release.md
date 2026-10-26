# UNS Kit3.0.29 — configuration-owned process groups

Status: release source prepared; npm publication is a separate owner-operated step.
This does not publish a Controller/helper release or authorize a deployment.

The six TypeScript packages share version3.0.29. CLI-generated dependency pins and
migration guidance use that version. Existing services without process-group
capabilities and existing file-based groups require no migration.

## Added shared capability

- Optional `processGroupConfigurationRuntime` protocol2 for immutable, bounded plain
  JSON resources defined in application configuration. Business parsing, mappings,
  paths and units remain in the application.
- Common descriptor selection, snapshots/hash rules, schema4 private policies,
  journal contract2, inspection, staging, supervision and launch-binding checks.
  Applications and hosts reuse these helpers; package names grant no special rights.
- Reviewed separate-generation input/review with explicit input-pause, resource-review
  and possible-broker-replay acknowledgements. Prior journals, checkpoints and policies
  are retained; old pending work is never reinterpreted.
- Configured same-generation paired version review through the existing version
  request. Resources, logical identities, brokers and journal UUID/contract/generation
  must remain unchanged. Pending records survive; policy byte snapshots support rollback.
- Standalone data-only snapshot reader understands file/configuration journal formats
  and verifies resource/profile/checkpoint bindings. Native helpers vendor this exact
  asset with SDK version and digest provenance.

Hosts still own authentication, operator review, durable intent, locks/reservations,
physical process exits and one-use launch/recovery authority. A shared declaration
alone grants no permission. Unknown outcomes stay blocked pending reconciliation.
Historical policy readers and the existing file workflow remain supported.

## Documentation and compatibility

Start with [ownership](controller-runtime-boundary.md), then the
[configuration guide](process-group-configuration-runtime.md),
[worked unrelated adapter](examples/configuration-process-group.md),
[resource generations](process-group-generations.md) and
[paired version upgrade example](process-group-version-upgrade.md).
The package includes a version-bounded3.0.29 entry in core `MIGRATIONS.md`; CLI create,
bundle and upgrade paths include the new guidance without replacing custom rules.

Configuration mode requires a protocol2-aware host and compatible helper. It does
not force applications to stop supporting their old file configuration. Publish the
SDK before updating consumers' real registry dependencies and frozen lockfiles. Align
Zod resolution during that step; local acceptance used4.4.3 matching the consumers'
frozen source builds. Repeat downstream schema generation and typecheck after locking.

## Verification and limits

Local SDK verification:239 root/core tests and415 workflow tests pass; one existing
external Azure connectivity test is skipped. All six packages build and their package
entrypoints/types are checked. CLI upgrade preserves custom instructions and is
idempotent. CI includes the new configured-group and generated-guidance regressions.

Earlier integration acceptance used the same feature code before version assignment:
real local Linux/Caddy/JWT/GraphQL/RttManager/PM2, Controller restart with admitted work,
lost responses, rollback and44 exact reviewed outputs on a retained same-generation
journal. That was a local unpublished integration candidate, not an installed public
Runtime or production deployment. Registry consumers still require verification
against the actually published3.0.29 packages.

Scope remains same-host MQTT/SQLite, bounded resources and journals. Backup reader
bounds remain9 journals and unique flat snapshot group IDs across namespaces;
ambiguous layouts fail instead of being silently omitted. Archiver spool exclusion
remains. No exactly-once output, recovery of unadmitted input, cross-host queue transfer,
automatic unknown-outcome resolution or complete system-restore claim is added.

## Owner publication

From the reviewed, committed `master` checkout:

```sh
pnpm login
pnpm run ts:publish
```

The repository's publish script builds, checks and publishes the six packages. Read
back every published version before updating downstream locks. SDK source push, npm
publication, downstream PR/CI, immutable host/helper releases and production rollout
are distinct steps.
