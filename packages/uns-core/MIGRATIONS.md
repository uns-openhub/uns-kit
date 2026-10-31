# @uns-kit/core migrations

Use this document when upgrading an existing application. Before changing
`@uns-kit/*` versions, record the installed source version and the intended
target version. Apply every migration whose version boundary is crossed; do not
apply migrations that are outside that range.

Agents must inspect the application's existing ownership and shutdown flow
before editing it. The examples below describe the intended behavior, not a
mechanical search-and-replace operation.

## 3.0.36 - Unitless state and passive background publishers

When crossing `<3.0.36` to `>=3.0.36`, MQTT handover preserves absent units
for strings and unitless numeric data. It never substitutes an empty string or
an invented physical unit. The optional-unit state format is bound into the
compatibility offer: mixed 3.0.35/3.0.36 stateful peers reject before freezing the
source. Review an initial stop-before-start upgrade, then use matching updated
peers for live moves. Do not bypass this refusal by relabeling a contract.

A passive successor must not publish startup metadata or poll results before
state import. Gate background work with `UnsProxyProcess.isActive()` and stop it
in `handoverShutdown.onRelease`; keep tracking all accepted asynchronous work.
This getter is an ownership signal, not broker connectivity or archive readiness.
Input callbacks already delivered by the SDK must still finish during drain.
Legacy configurations and persistent-output ownership rules are unchanged.

## 3.0.35 - Opt-in state-preserving MQTT handover

When crossing `<3.0.35` to `>=3.0.35`, existing applications keep their current
handover behavior. State continuity is opt-in: declare `mqttHandover` protocol2
and `mqttDrain` protocol1, pass `uns.handoverProtocol: "correlated-v2"` to
`UnsProxyProcess`, declare each proxy's semantic `handoverStateContract`, and
register migration drain after installing application input handlers.

Use an updated host that understands this capability and its bound drain/exit
receipt. Both peers must support the protocol; persistent output and other
runtime-owned state remain separate reviewed transfers. No package-name logic
or application callbacks may be executed by the host to inspect compatibility.

The SDK transfers its counter baselines and publication sequence state only after
accepted work and output flush. The target advertises compatibility before the
source freezes. Different destination/subscriptions/proxy sets/semantic revisions
block the handover while the source remains active. New paths start fresh;
removed paths do not erase history; changed unit/type must not reuse arithmetic
baselines. Matching path strings alone are not a compatibility guarantee.

Unknown outcomes must be reconciled under the same operation. Return ownership
with a fresh reverse handover from the current owner, never by restarting stale
RAM. This is not crash durability or universal exactly-once MQTT delivery.
See [example, protocol and limits](https://github.com/uns-openhub/uns-kit/blob/master/docs/mqtt-stateful-handover.md).

## 3.0.34 - Optional paired-input retirement to one direct process

When crossing `<3.0.34` to `>=3.0.34`, ordinary applications require no change.
Packages opting into `unsDatahub.processGroupDirectRuntime` use shared declaration,
resource review and startup/readiness helpers under `runtime/process-group-direct-*`.
The authenticated host must stop receiver first, drain the same input journal,
prove both original exits and persist one launch intent before starting one direct
successor. Preserve business profiles, paths, units, journal UUID and original policy.

Schema5 retirement evidence fences old journal owners and original profiles.
Open recovery pins exact direct configuration/artifact/placement/launch; explicit
closure releases ordinary direct management but retains the old journal and fences.
Unknown outcomes stay blocked in the same operation. Never remove policy/journal
files, rename an owner or force a second launch to bypass review.

`UnsMqttProxy.getInputReadiness()` exposes current SUBACK evidence. Register normal
signal/drain handlers and confirm output before declaring direct readiness; perform
confirmation asynchronously when the configured queue must accept during outages.
No Controller heartbeat is required for normal application cleanup.

Backup snapshots with retirement evidence use internal manifest2 and include exact
retirement/original-policy bytes. Update the transport helper from the same SDK
asset before activation; older transports fail closed. Restore remains offline
review only. See [declaration, API example and limits](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-direct-retirement.md).

## 3.0.33 - Explicit queue disablement and generic clean MQTT migration

When crossing `<3.0.33` to `>=3.0.33`, existing applications require no change.
An existing `publishQueue` remains enabled unless explicitly configured otherwise.
Use `enabled: false` only for an initial RAM-only deployment or a verified empty
old queue: pending state, another identity or a live owner blocks disabling.

Applications opting into ordinary clean MQTT migration declare `unsDatahub.mqttDrain`
protocol1 and call `UnsProxyProcess.enableMigrationDrain()` after preparation.
Provide `handoverShutdown` hooks that stop background admission and await all
accepted application work; report publication failures instead of silently losing
them. Hosts must wait for the bound SDK drain receipt and clean original exit;
PM2 online/stopped alone is insufficient. Unknown results block the successor.
Keep standalone shutdown functional. This does not retire paired input journals
or imply automatic queue transfer. See [the capability and example](https://github.com/uns-openhub/uns-kit/blob/master/docs/mqtt-migration-drain.md).

## 3.0.32 - Reviewed process-group MQTT output enablement

When crossing `<3.0.32` to `>=3.0.32`, ordinary services, input journals and
existing outboxes remain unchanged. Output persistence is never enabled by a
package update. Hosts may explicitly review nested paired transition protocol 3
through `runtime/process-group-upgrade.js`; ordinary version/generation reviews
retain their strict configuration comparison.

Applications opting in declare
`unsDatahub.processGroupMqttOutputRuntime = { protocolVersion: 1, instanceName }`,
matching the actual SDK output proxy. Keep parsing, profiles, resources, paths and
units in the application; do not add a service-name branch to a host.

Use a separate reviewed permanent output mount and retain the exact input journal.
On rollback, stop admission and drain already admitted input before stopping the
publisher. Pending output, changed storage/owner/destination and unknown outcomes
block restoration; preserve both journals and reconcile the same operation. Never
rename an owner, choose another directory or remove data to bypass these checks.
The managed outbox format remains schema2 and the stable identity serialization
is byte compatible with3.0.31. A matching host implementation is required for the
optional transition; applications without the capability require no migration.

See the SDK developer guide
[reviewed MQTT output enablement](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-mqtt-output-transition.md)
for declarations, proxy configuration, review examples and recovery limits.
Source preparation and owner-operated npm publication are separate steps.

## 3.0.31 - Explicit Runtime-managed MQTT outbox storage

For an optional Runtime-managed MQTT outbox set `output.publishQueue.storage` to
`runtime`, bind its literal two-component directory under the trusted
`UNS_MQTT_OUTPUT_STATE_ROOT` mount, and provision private per-Controller external
storage. Matching Controller/tools/helper support is required. Absence remains
`local` and preserves schema 1. Older strict parsers reject the new setting;
older stores reject managed schema 2. Never bypass this guard by removing fields.
Existing queues are preserved; automatic schema 1 adoption is not implemented.

Native backup helpers vendor the exact SDK data-only state program and finite
contract with package/version/SHA receipts. Queue state is a separate signed and
age-encrypted RTT capsule. Restoration requires an empty offline target and stopped
source, retains the original identity and blocks application startup until complete.
The ordinary RTT-only activation path refuses included queues. Buffered RAM is not
included by live disk capture. See `docs/mqtt-output-runtime-state.md` for the two
unrelated consumer examples, recovery sequence and delivery limits.

## 3.0.30 - Optional MQTT output buffering and durable admission

When crossing `<3.0.30` to `>=3.0.30`, output persistence remains optional.
Registry3.0.29 must not be assumed to contain these APIs. Ordinary
applications require no migration and keep the old publisher behavior.

Opt-in TypeScript applications may configure `output.publishQueue` and pass it via
standard MQTT channel resolution. `buffered` uses RAM with spill-on-failure/overflow;
`durable` acknowledges local admission only after a SQLite WAL/FULL commit. Both
require QoS1/2, serial output and an explicit private persistent directory. A publish
call acknowledges admission; `flush()` includes retained records and waits for broker
completion. Unknown delivery outcomes can replay. Input MQTT ACKs are not changed.

Use one owner with `handover=false`. Legacy RAM hash handover cannot transfer the
outbox. Preserve existing application journals/Archiver spools; do not reinterpret,
move or delete them as part of this opt-in. Host migration/automatic backup, Python,
SQL writers and non-MQTT destinations remain outside this capability. See the source
`docs/mqtt-output-persistence.md` and two unrelated examples before enabling it.

### Shared bounded journal backup coverage in 3.0.30

Consume `runtime/process-group-backup-contract.js` for public coverage validation,
and vendor the matching standalone exporter plus contract JSON with source/digest
provenance in native helpers. The fixed contract supports32 journals while keeping
512MiB snapshot and1GiB capsule bounds. Do not take limits from an operation request.
Old two-argument exporter callers keep their nine-journal limit; archives through9
remain compatible. Older readers may reject larger new captures, so coordinate the
host/helper rollout. Preserve all source journals and reject overflow without partial
success. See `docs/process-group-backup-capacity.md`. This follow-up is not
part of source3.0.29; use the coordinated3.0.30 SDK, host and helper before rollout.

## 3.0.29 - Optional configuration resources and reviewed journal transitions

Release preparation: source/packaged local acceptance is complete; npm publication
is a separate owner-operated step. These APIs are not in registry3.0.28.

When crossing `<3.0.29` to `>=3.0.29`, existing applications and file policies need
no migration. Opt-in applications can declare `processGroupConfigurationRuntime`
protocol2 and use `runtime/configuration-group-contract.js`, immutable opaque JSON
resources, journal contract2 and policy schema4. Applications own business validation;
SDK owns common validation/hash/identity rules; hosts own authenticated review,
durable operations and launch control. Never add named-application lifecycle code.

Use `configurationGroupSnapshot` to bind plain JSON resources and retain its SDK-issued
proof. Validate the resolved application resources against the authorized raw snapshot.
Keep runtime group snapshots immutable. No mapping, unit or topic is inferred by the
host. See [the configuration guide](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-configuration-runtime.md)
and [worked unrelated adapter example](https://github.com/uns-openhub/uns-kit/blob/master/docs/examples/configuration-process-group.md).

Keep file protocol1/policy3 and frozen historical policies1/2 unchanged. New journals
store profile bindings as `{processName, resourceSha256, dataGroup}`. Host inspection,
reservations, staging, supervision and standalone snapshots understand both formats;
ordinary undeclared Node/Python services retain their current behavior. Hosts must
pin group/generation in configured launch reservations and share these helpers rather
than reconstruct the validation or execute application JavaScript.

### Resource changes require separate generations

The paired management envelope stays protocol1. Its nested protocol2 input reviews
four placements, separate source/target journals and explicit input-pause,
resource-review and possible-broker-replay acknowledgements. Use the shared
`process-group-generation.js` review; stop receiver first, finish admitted old work,
prove publisher exit, then launch the reviewed new receiver and publisher. Rollback
symmetrically drains the successor generation before restoring the old pair.

Never relabel an old journal or reinterpret pending payloads, even when it is empty.
UUIDs, contracts, checkpoints and canonical policies remain separate and retained.
Unknown initialization/stop/start outcomes require reconciliation of the same durable
operation. Do not initialize a replacement journal or retry an unconfirmed launch.
See [generation preparation and limits](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-generations.md).

### Version changes can retain an unchanged generation

`reviewInstalledProcessGroupUpgrade` supports file groups and configured pairs through
the existing nested protocol1 version input. Configured review adds `retainedJournal`
(contract2); all resources, process/profile/input/broker identities, capacity,
journal directory/UUID and generation must stay unchanged. A resource change belongs
to the separate-generation path. No initializer runs during this version-only step.

The SDK validates staged schema4 target policy data without issuing live authority.
After both prior owners have proven clean exits, the host compares/swaps the canonical
policy and starts receiver before publisher with one-use launch identities. Both old
and new policy byte snapshots remain available for rollback; pending records and
completed checkpoints stay valid in that same journal. Recheck its integrity before
policy replacement and launch. See [paired-version guide and request example](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-version-upgrade.md).

### Packaging and limits

Local Linux acceptance covers pending tasks, interruption, lost response, same-operation
reconciliation, exact outputs and rollback. The shared snapshot reader accepts journal
contracts1/2 and verifies resource/checkpoint bindings; sync native helpers from the
exact installed SDK asset/version/hash. Existing backup bounds remain9 journals total,
unique flat snapshot group identifiers across namespaces, and archiver spool exclusion.

Publish the coordinated SDK before pinning downstream registry versions/lockfiles;
keep Zod resolution compatible and regenerate schemas/typecheck from frozen installs.
Hosts/helpers require their own compatible releases. This does not authorize a
production rollout, cross-host queue transfer, exactly-once output, automatic unknown
outcome resolution or recovery of MQTT input never admitted by the receiver.

## 3.0.28 - Paired process-group version upgrades

When crossing `<3.0.28` to `>=3.0.28`, shared schemas and installed-resource review support a
publisher/receiver version upgrade on the same retained journal. Host launchers
must consume the shared helpers and enforce authenticated durable orchestration;
applications must not copy the upgrade protocol or add Controller-specific business
adapters. Existing protocol 1 applications need no mapping, topic, unit or writer
change. Historical policy readers remain frozen, while the reviewed new artifact
uses generic policy schema 3. See `docs/process-group-version-upgrade.md`.

The neutral receiver supervisor now treats clean exit `0` and explicit contract
refusal `78` as terminal; other failures remain subject to bounded restart.
During planned shutdown the host must park existing supervision as well as verify
the guarded application exit, including for receivers started by older launchers.
Do not infer completed drain from PM2 online/stopped or a sent signal.

A candidate policy validation is review-only. Actual launch still needs the live
canonical policy and exact durable operation/launch reservation. Missing responses
are reconciled on the same operation. SDK publication is a separate manual step;
downstream releases must install the published version and regenerate registry
lockfiles before releasing their host integration.

## 3.0.26 - Broker-acknowledged input subscriptions

When upgrading from `<3.0.26` to `>=3.0.26`, `mqttProxyStatus` includes the additive
numeric event `mqtt-subscriptions-ready`: 1 only when the current connection has
acknowledged every observed subscription and no subscribe/unsubscribe ACK remains
pending; 0 on pending/rejected subscriptions or connection loss. The normal ten
second status heartbeat repeats the observation. No topics, credentials or payloads
are included, and no extra subscriptions or data publications are created.

Applications may observe this event on an `UnsMqttProxy` to distinguish process
Start/connect from prepared broker input. Also verify that application handlers,
registry and admission policy are prepared. Expire old observations and invalidate
them during pause/shutdown/restart. This transport observation does not guarantee
application persistence, a database ACK, replica visibility or exactly-once delivery.
Existing consumers need no change; old SDKs provide no such confirmation.

## 3.0.24 - Generated numeric delta metadata

When upgrading from `<3.0.24` to `>=3.0.24`, numeric scalar publications in
the deprecated `MessageMode.Delta` and `MessageMode.Both` modes declare
`presentationKind: "delta"` and `defaultAggregation: "sum"` on the generated
`-delta` attribute. They no longer inherit cumulative-counter presentation,
last aggregation or a counter reset-policy hint. The original raw attribute
keeps its caller-supplied metadata. Paths, packet values, timestamps, units,
persistence, freshness and the existing difference/reset calculation remain
unchanged. Non-numeric and table publication metadata are unchanged.

Review existing canonical schema definitions for generated delta attributes:
controllers may keep previously stored non-null shape fields after an upgrade.
Correct reviewed definitions to interval/delta semantics and request raw
history with sum aggregation for interval amounts. Do not difference already
calculated deltas. Do not globally label unsuffixed attributes as counters
from their names: other publishers can reuse a name for interval amounts.

These legacy producer delta modes still lose previous-value state on restart.
Preserve consumers while migrating toward raw cumulative counters and history
API delta/rate transforms; this metadata fix does not make delta state durable.

## 3.0.23 - Application drain on handover

Applications upgrading from `<3.0.23` to `>=3.0.23` may register
`handoverShutdown: { onRelease, drain, timeoutMs }` in `UnsProxyProcess`
construction, before creating MQTT proxies. `onRelease` runs synchronously when
an active source accepts a handover: stop admitting new background work, but
continue handling already accepted MQTT input while subscriber queues drain.
`drain` runs after the target's acknowledgement and source proxy stops. Await
accepted application writes, durable spills and replay checkpoints, then close
writers. Do not call `process.exit` inside either hook. Share an idempotent drain
with SIGINT/SIGTERM cleanup; signal cleanup remains application-owned.

The source exits with status 0 only when its remaining proxy stops and drain
finish. A rejected hook or deadline exits with status 1 and a generic diagnostic,
without logging hook error text. The default deadline after acknowledgement is
30 seconds; a registered timeout may be 1 to 300000 ms. Timeout does not cancel
IO or guarantee all writes were saved. Persist recoverable work before relying
on this hook and review pending files after an incomplete drain.

Duplicate acknowledgements share one drain. Unsolicited acknowledgements and
acknowledgements from a different peer do not exit the process. MQTT wire
messages are unchanged, so an old source still uses its old immediate exit:
upgrading only the target cannot retrofit the source hook. `handover_fin` and
`handover_ack` confirm MQTT ownership transfer, not application drain or a safe
legacy-directory import. Wait for source process exit separately. This change
does not add exactly-once semantics or controller fencing.

## 3.0.22 - Safe API request correlation

When upgrading `@uns-kit/api` from `<3.0.22` to `>=3.0.22`, its generic request
log no longer records raw URLs or email claims decoded from an unverified JWT.
It logs a server-generated `requestId`, HTTP method, completion status and
elapsed time. Incoming `x-request-id` values are not reused. The response
`x-request-id` and request property `Symbol.for("uns.http.request-id")` identify
the same server-local request; authenticated application handlers may reuse that
property for downstream query diagnostics. The public `@uns-kit/api/request-log.js`
module exports `SAFE_REQUEST_LOGGING_VERSION = 1` so privacy-sensitive applications
can require the safe middleware before opening HTTP listeners.

If an application's diagnostic logic relied on the old `user` or `endpoint`
log fields, migrate it to operation labels and caller IDs extracted **after**
signature verification. Do not restore URL/topic/body/credential logging or trust
identity headers. API response payloads and authorization behavior are unchanged.

## 3.0.21 - Explicit publisher topic reconciliation

Applications upgrading from `<3.0.21` to `>=3.0.21` can opt into
`UnsProxy.retainProducedTopics(fullTopics)`. Pass the complete set of configured
UNS attribute paths for that proxy. The method removes previously observed topic
metadata outside that set and immediately announces the remaining metadata,
including an empty list when the last observed topic is removed. It returns the
number of removed entries. Configured paths that have never published data are
not registered by this call.

A bridge/runtime must stop accepting events for removed mappings and drain
already accepted publications before reconciliation; a later publication can
register its topic again. Keep configured-but-stopped mappings and shared targets
in the retained set. Connection Stop alone is not a reason to remove metadata.

This API affects only the proxy's publisher metadata cache. It does not delete
UNS definitions, retained MQTT business values, archiver mappings, historical
sources or physical data. Existing publishers need no change; their default
registration and heartbeat behavior remains the same.

## 3.0.19 - Provider-bound Asset identity candidates

Apply this migration when upgrading a publisher from `@uns-kit/core` `<3.0.19`
to `>=3.0.19` only when it should identify an Asset through a reviewed provider
namespace before an external identifier has been mapped to a stable entity.

Use `UnsClient.issueAssetIdentityPublicationEvidenceByExternalIdentity()`.
The result is directly publishable metadata in one of two mutually exclusive
modes:

- an existing provider mapping returns `assetStableEntityId` and
  `assetIdentityProof`, preserving the normal stable-identity transition;
- a new provider identifier returns `assetProviderIdentity` and
  `assetProviderIdentityProof`, allowing the controller to collect signed,
  workload-bound evidence for manual review without auto-merging Assets.

Do not combine the two metadata sets or persist/reuse an expired proof. Legacy
publishers need no change and continue to publish without identity metadata.

## 3.0.15 - Secret references in database configuration

`@uns-kit/database` now accepts the standard environment and Infisical secret
references for database connection values that are resolved before a client is
opened: PostgreSQL/Oracle host, database, user, password, Oracle connect
string/service name/SID, and PostgreSQL TLS material. Database adapters reject
an unresolved reference with a clear startup error rather than passing an
object to the database driver.

Applications using `databasesConfigSchema` should upgrade their direct
`@uns-kit/database` dependency with the aligned `@uns-kit/*` release and
regenerate `config.schema.json`. Applications with their own project schema
must replace `z.string()` with `secretValueSchema` for any configured value
that is intentionally stored as an environment or Infisical reference. Do not
widen ordinary labels, topic names, paths, or other non-secret settings merely
because they are strings.

## 3.0.13 - Optional controller-correlated handovers

`UnsProxyProcess` accepts an optional `handoverId` parameter or
`UNS_HANDOVER_ID` environment variable. When supplied, it adds that opaque id
to handover MQTT payloads and MQTT 5 user properties. A new target includes
the id in its final `handover_ack`, even when its source still runs an older
uns-kit that does not echo the id.

Existing applications need no configuration or source change: without this
value their MQTT messages remain byte-for-byte compatible with the legacy
handover protocol. To opt an RTT target into controller-coordinated hot
migration, set `uns.handoverProtocol: "correlated-v1"` alongside
`uns.instanceMode: "handover"` and `uns.handover: true`. A controller should
inject the value only for a durable, operator-initiated migration and must
still accept an acknowledgement without the id from a legacy target. The value
is correlation metadata, not a secret; do not use it as an authorization or
fencing token.

## 3.0.12 - Expiring retained handover heartbeats

`UnsProxyProcess` now publishes its `active` heartbeat as an MQTT 5 retained
message with a 30-second expiry, refreshed every 10 seconds. A newly started
process can therefore see a live predecessor immediately rather than racing a
single non-retained heartbeat. On normal `UnsProxyProcess.shutdown()`, it
publishes short-lived `active=0` before disconnecting.

The handover manager treats a retained `active=1` as a possibly stale snapshot
and waits for a fresh heartbeat before requesting a handover. If the retained
process has crashed, no fresh heartbeat arrives, so the new process activates
after its normal active timeout; the stale retained message expires shortly
afterwards. Keep the `uns.instanceMode: "handover"` and
`uns.handover: true` settings on both sides of a handover.

Applications that stop an individual process-owned `UnsMqttProxy` rather than
calling `UnsProxyProcess.shutdown()` still rely on the 30-second expiry. Migrate
those applications to the process-level shutdown API when practical.

## 3.0.11 - OpenHub runtime profiles and add-on compatibility

Apply this migration when upgrading from `@uns-kit/cli` or `@uns-kit/core`
`<3.0.11` to `>=3.0.11` for an application that is deployed by an OpenHub 2.x
controller.

New TypeScript and Python projects now use `controllerCompatibility: ">=2 <3"`
and receive three credential-free profiles:

- `config-development-host.json` for a directly started host process;
- `config-development-podman.json` for a process deployed through a local
  Podman controller; and
- `config-production.json` for a production controller instance.

Keep only the selected copy as the untracked `config.json`. Do not commit a
token, user email/password, MQTT password, or customer endpoint in any tracked
profile. Direct host development uses `UNS_SERVICE_TOKEN` and the local
controller identity in `.env`; controller-managed instances receive
`UNS_SERVICE_TOKEN_FILE` and `UNS_CONTROLLER_*` from PM2. Use
`ServiceTokenProvider` for any controller or controller-proxied API call so a
mounted token rotation is used without restarting the service.

Existing applications remain valid, but their old `>=7.1 <8` manifests are not
deployable by an OpenHub 2.x controller. Update that range only when the
application is compatible with the 2.x runtime API, regenerate its
configuration schema, and validate each selected profile before release.

## 3.0.10 - Managed RTT service registration

Apply this migration when upgrading from `@uns-kit/core` `<3.0.10` to
`>=3.0.10` for an RTT service that must prove it started under the controller
that manages its instance.

After the service has successfully started its MQTT/API runtime, construct an
`UnsClient` with `ServiceTokenProvider` and call `registerService`. Use the
controller-injected `RTT_NODE` and `RTT_INSTANCE_ID` only through that helper;
the helper verifies the descriptor id and sends no credentials. A directly
started development process has neither variable, so `registerService` is a
deliberate no-op. Do not synthesize those variables or create an identity in
application configuration.

## 3.0.9 - MQTT channel inheritance

Apply this migration when upgrading from `@uns-kit/core` `<3.0.9` to
`>=3.0.9` if an application supports optional `input` or `output` broker
overrides.

Resolve channels with `resolveMqttChannel(config.infra, config.input)` or
`resolveMqttChannel(config.infra, config.output)`. Pass its `host` to the
proxy constructor and `mqttChannelParameters(channel)` to the connection
options. A partial channel override now inherits the complete `infra` broker
configuration, including credentials, TLS material, reconnect settings,
servers/hosts, and MQTT v5 properties. An explicit channel field still wins.

## 3.0.8 - Controller-managed service tokens

Apply this migration when upgrading from `@uns-kit/core` `<3.0.8` to
`>=3.0.8` for RTT services that call a DataHub controller or a controller-
proxied API.

Use `ServiceTokenProvider` as the `UnsClient` `tokenProvider`. It reads the
controller-mounted `UNS_SERVICE_TOKEN_FILE` first, then `UNS_SERVICE_TOKEN`,
then the resolved `uns.token` configuration value. Keep `AuthClient` only as a
legacy local-development fallback for services that still have
`uns.email`/`uns.password`.

When `UNS_SERVICE_TOKEN_FILE` is set, it is authoritative. A missing or empty
file is an error; do not fall back to a user credential or copy the token into
the service configuration. The provider re-reads the mounted file for each
request, so the controller can rotate credentials atomically without restarting
the RTT process.

## 3.0.7 - Zod 4 configuration contracts

Apply this migration when upgrading from `@uns-kit/core` `<3.0.7` to
`>=3.0.7`.

`@uns-kit/core` and `@uns-kit/database` now use Zod 4. Applications that own a
`src/config/project.config.extension.ts` file must use the same Zod major at the
schema-composition boundary. Update the application's direct `zod` dependency
to `^4.4.3` before regenerating configuration artifacts.

The UNS core and database schemas preserve their previous accepted inputs and
defaults. The migration is nevertheless source-sensitive for applications that
inspect Zod internals or use Zod-3-only type aliases. Replace those seams with
the public Zod 4 API:

- replace `z.AnyZodObject` with `z.ZodObject<z.ZodRawShape>`;
- replace `z.ZodTypeAny` with `z.ZodType` where a broad schema type is needed;
- read object keys through `.shape`, not `_def.shape()`;
- pass both key and value schemas to `z.record(keySchema, valueSchema)`;
- prefer object `.extend()` or shape spreading over deprecated `.merge()`.

After updating the extension schema, run `pnpm run generate-config-schema` and
check both `config.schema.json` and `src/config/app-config.ts`. Then run the
application's typecheck and its existing configuration compatibility tests.

## 3.0.0 - MQTT table columns use named objects

Apply this migration when upgrading from `@uns-kit/core` `<3.0.0` to
`>=3.0.0`.

The MQTT `message.table.columns` wire shape changes from an ordered array with
embedded `name` properties to an object keyed by column name. This makes a
column directly addressable as, for example,
`message.table.columns.power.value`.

Before:

```ts
columns: [{ name: "power", type: "double", value: 42.1, uom: "kW" }];
```

After:

```ts
columns: {
  power: { type: "double", value: 42.1, uom: "kW" },
}
```

`IUnsTable.columns` is now `IUnsTableColumns`, a
`Record<string, IUnsTableColumn>`, and `IUnsTableColumn` no longer has a
`name` property. Structured builders emit MQTT packet version `2.0.0` and
reject array-form outbound columns. Inbound parsing remains transitional:
legacy MQTT packet version `1.x` arrays are accepted and normalized to the
canonical object before being returned to application code.

Column keys emitted by new publishers must match
`^[A-Za-z_][A-Za-z0-9_]{0,62}$` and must not be `__proto__`, `prototype`, or
`constructor`. Each descriptor must contain a valid QuestDB `type` and a
`value`; `uom` remains optional.

For consumers, replace array-only checks, `.length`, `.map()`, and direct
iteration with object entry iteration. The typed helper keeps that boundary
explicit:

```ts
for (const [name, column] of tableColumnEntries(table.columns)) {
  // use name, column.type, column.value, column.uom
}
```

Do not apply this migration to ordered schema metadata such as
`tableColumns`, capture `outputSchema.columns`, or Assistant `TABLE_JSON`;
those are separate formats.

### Rollout requirement

Release compatibility-capable readers before enabling object-form publishers
in production. In particular, deploy and verify the archiver against both
legacy-array and object-form packets first. Publishing the npm package alone
does not clear this rollout gate.

### Upgrade check

- Update all structured table publishers to construct named objects.
- Update all table consumers to iterate canonical object entries.
- Audit raw MQTT publish escape hatches that may bypass `UnsPacket` validation.
- Test legacy packet `1.x` array ingestion and packet `2.0.0` object ingestion.
- Keep schema arrays and UI/API table artifacts unchanged unless separately
  migrated.

## 2.0.71 - MQTT publishing and shutdown lifecycle

Apply this migration when upgrading from `@uns-kit/core` `<2.0.71` to
`>=2.0.71`. No shutdown rewrite is required solely for an upgrade from
`2.0.71` or newer.

### Process-owned MQTT proxies

An MQTT proxy created by `UnsProxyProcess.createUnsMqttProxy()` is owned by that
process. Shut it down through `UnsProxyProcess.shutdown()` only. Do not flush or
stop the same proxy separately during normal process shutdown.

Before:

```ts
await mqttOutput.flush();
await mqttOutput.stop();
await unsProcess.shutdown();
```

After:

```ts
await unsProcess.shutdown();
```

`UnsProxyProcess.shutdown()` closes all process-owned proxies, waits for their
accepted publishes to drain, and attempts every cleanup even if one fails. It
rejects with an `AggregateError` when any cleanup fails, so callers must report
or otherwise handle that rejection.

Long-running applications should initiate this process-level shutdown from both
`SIGINT` and `SIGTERM`. Keep startup-failure cleanup on the same process-level
path as well.

### Standalone MQTT proxies

For an independently constructed `UnsMqttProxy`, call:

```ts
await proxy.stop();
```

`stop()` closes publish admission immediately and drains accepted work by
default. Repeated calls share the same result. Use
`await proxy.stop({ drain: false })` only when intentionally dropping queued
messages is acceptable.

### Publish completion and errors

- `publishMessage()` and `publishMqttMessage()` resolve when the bounded worker
  queue accepts a message, not when the broker confirms the publish.
- Call `await proxy.flush()` when the application needs all previously accepted
  messages to complete while the proxy remains running.
- A full bounded queue rejects the publish instead of creating an unbounded
  main-thread backlog. Decide whether the caller should retry, slow down, or
  fail.
- Asynchronous broker publish failures are emitted on the proxy `error` event.
  Keep an error listener or another explicit error-handling path.

### Upgrade check

For every affected application, verify all of the following:

- Identify whether each proxy is process-owned or standalone.
- Remove duplicate process-owned proxy `flush()` or `stop()` calls from the
  shutdown path.
- Handle rejection from `UnsProxyProcess.shutdown()` and standalone `stop()`.
- Confirm both `SIGINT` and `SIGTERM` use the intended shutdown owner.
- Confirm producers handle queue-full rejection at their required reliability
  level.
- Test that shutdown waits for accepted publishes and does not accept new work.

## 3.0.27 - Shared process-group runtime protocol 1

When crossing `<3.0.27` to `>=3.0.27`, existing applications need no change unless
using reviewed multi-process consolidation. Such applications and host launchers
must consume the shared `runtime/process-group-*.js` modules rather than fork
policy verification, configuration projection, journal identity or supervision.
Declare a data-only `unsDatahub.processGroupRuntime` in the application package.
See `docs/process-group-runtime.md` in the SDK repository for its schema and limits.
A declaration advertises support; it grants no execution or data access rights.

Host launchers still own authenticated commands, durable operation state, process
identity and resource limits. Applications own input parsing, mappings, units,
publishing and the SQLite writer. Both must implement protocol 1 exactly. The
SDK supplies the manifest, authorization, journal contract, staging, inspection,
receiver supervision and standalone snapshot reader. Do not import or execute an
application module inside the Controller. Do not add package/group-specific branches
or release-time group enumerations to a host launcher.

New authorizations use policy schema 3 and pin the declared descriptor, installed
artifact, manifest, mappings, three MQTT channel identities, configurations,
controller, placements, journal and state mount. Policies 1/2 remain frozen readers
for previously issued artifacts; do not relabel them as schema 3 or widen their
scope. Adding a group requires a new reviewed manifest/policy, not a Controller
capability number. Unknown protocols, changed resources, missing retained-state
policies and uncertain operation results remain blocked pending review.

Protocol 1 supports same-package, same-controller MQTT source groups (2–16
publishers), one target and one separately supervised receiver, with the existing
bounded SQLite journal format on a local Linux filesystem mount. It is not an
arbitrary transport, cross-host queue transfer or exactly-once protocol. Durable
admission starts only after the receiver subscription is acknowledged; a broker
or pre-admission gap can still lose input. Configuration-defined secrets remain
in the application; authorization diagnostics and backup outer metadata omit them.
Existing journal payloads and checkpoints are not converted by this upgrade.
