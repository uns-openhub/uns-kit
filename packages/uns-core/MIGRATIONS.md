# @uns-kit/core migrations

Use this document when upgrading an existing application. Before changing
`@uns-kit/*` versions, record the installed source version and the intended
target version. Apply every migration whose version boundary is crossed; do not
apply migrations that are outside that range.

Agents must inspect the application's existing ownership and shutdown flow
before editing it. The examples below describe the intended behavior, not a
mechanical search-and-replace operation.

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
