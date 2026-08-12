# @uns-kit/core migrations

Use this document when upgrading an existing application. Before changing
`@uns-kit/*` versions, record the installed source version and the intended
target version. Apply every migration whose version boundary is crossed; do not
apply migrations that are outside that range.

Agents must inspect the application's existing ownership and shutdown flow
before editing it. The examples below describe the intended behavior, not a
mechanical search-and-replace operation.

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
