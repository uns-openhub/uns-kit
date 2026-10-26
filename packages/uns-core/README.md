# @uns-kit/core

Core utilities and runtime building blocks for Unified Namespace (UNS) applications. The package bundles the process lifecycle manager, MQTT integrations, gRPC gateway helpers, configuration tooling, and shared type definitions that power the UNS ecosystem.

Note: Apps built with uns-kit are intended to be managed by the **UNS OpenHub controller**.

## uns-kit in context

| Package | Description |
| --- | --- |
| [`@uns-kit/core`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-core) | Base runtime (UnsProxyProcess, MQTT helpers, config tooling, gRPC gateway). |
| [`@uns-kit/api`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-api) | Express plugin — HTTP endpoints, JWT/JWKS auth, Swagger, UNS metadata. |
| [`@uns-kit/cron`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-cron) | Cron-driven scheduler that emits UNS events on a fixed cadence. |
| [`@uns-kit/cli`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-cli) | CLI for scaffolding new UNS applications. |

## Installation

```bash
pnpm add @uns-kit/core
# or
npm install @uns-kit/core
```

## Key concepts

- **UnsProxyProcess** — the central runtime class. It manages the MQTT connection, plugin lifecycle, and the instance status topic. Plugins (`@uns-kit/api`, `@uns-kit/cron`) augment it with domain-specific proxy factories.
- **UnsProxy** — base class extended by all plugin proxies. Tracks produced topics, API endpoints, and catch-all mappings; re-publishes them to the controller on a 60-second cadence.
- **ConfigFile** — loads and validates `config.json` at startup. On a real server this file is provided by the UNS OpenHub controller; in development you maintain it yourself.
- **MQTT helpers** — resilient publishers, topic builders, throttled queues, and handover support.
- **gRPC gateway** — infrastructure to bridge Python workers into the UNS message fabric.

## Basic usage

```ts
import UnsProxyProcess from "@uns-kit/core/uns/uns-proxy-process";
import { ConfigFile } from "@uns-kit/core";

const config = await ConfigFile.loadConfig();

// Connect to MQTT broker; processName identifies this service in the controller.
const proc = new UnsProxyProcess(config.infra.host!, { processName: config.uns.processName });
```

Publish retained service metadata when a runtime should be discoverable by the
controller as a core service, addon, or built-in capability:

```ts
await proc.publishServiceMetadata({
  serviceId: "uns-bridge-mqtt",
  kind: "addon",
  addonId: "uns-bridge-mqtt",
  capabilities: ["mqtt-source-browser", "runtime-mappings"],
  apiRoutes: [
    {
      path: "/api/system/bridge/mqtt/runtime/service/bridge/health",
      kind: "health",
    },
  ],
});
```

The metadata is retained on
`uns-infra/<package>/<version>/<processName>/service-metadata`. Controllers
combine this identity/capability payload with live process telemetry and route
health before marking a service healthy.

Extend it with plugins:

```ts
import "@uns-kit/api";
import "@uns-kit/cron";
import { type UnsProxyProcessWithApi } from "@uns-kit/api";
import { type UnsProxyProcessWithCron } from "@uns-kit/cron";

const proc = new UnsProxyProcess(config.infra.host!, { processName: config.uns.processName })
  as UnsProxyProcessWithApi & UnsProxyProcessWithCron;

const api  = await proc.createApiProxy("my-service", { jwtSecret: "CHANGEME" });
const cron = await proc.createCrontabProxy("*/5 * * * *", { event: "tick" });
```

## Datahub client (last value + history)

`UnsClient` provides a minimal REST client for the UNS OpenHub API, including batch last-value, single-topic catch-all history, and batch range endpoints. Prefer a long-lived service token if available; you can pass it directly and skip username/password auth.

```ts
import { UnsClient } from "@uns-kit/core";
import { ConfigFile } from "@uns-kit/core";

const config = await ConfigFile.loadConfig();

const client = new UnsClient("https://datahub.example.com", {
  token: process.env.UNS_SERVICE_TOKEN ?? config.uns.token,
});

const values = await client.lastValue([
  "raw/data/line-1/motor/main/temperature",
  "raw/data/line-1/motor/main/status",
]);
console.log(values);

const data = await client.getAttributeData("sij/acroni/vv/hrm-furnace/equipment/pusher/output-quantity", {
  from: "2026-05-07T11:17:01.157Z",
  to: "2026-05-07T11:22:01.157Z",
  table: "uns_sij_hrm_furnace_data",
  aggregate: "last",
  dedupe: false,
});
console.log(data?.toRecords());

const customData = await client.getData("/projects/project-name/path-to-data/data", {
  fromDate: "20260325",
});
console.log(await customData.json());

const batchHistory = await client.history([
  "sij/acroni/vv/hrm-furnace/equipment/zone-1/temperature",
  "sij/acroni/vv/hrm-furnace/equipment/zone-2/temperature",
], {
  from: "2026-04-09T06:00:00Z",
  to: "2026-04-09T07:00:00Z",
  limit: 500,
});
console.log(batchHistory?.byTopic);
```

## Sub-Asset Publishing

Sub-assets use the same publish fields as normal assets. Set `topic` to the full
parent asset path and set `asset` to the leaf sub-asset:

```ts
await proxy.publishMqttMessage({
  topic: "sij/acroni/jek/pp/",
  asset: "furnace-1",
  objectType: "material",
  objectId: "main",
  attributes: {
    attribute: "daily-production",
    data: {
      time: new Date().toISOString(),
      value: 42,
      uom: "t",
    },
  },
});
await proxy.flush();
```

This publishes
`sij/acroni/jek/pp/furnace-1/material/main/daily-production`. Consumers that
persist the existing QuestDB identity columns should store
`topic = "sij/acroni/jek/pp"` and `asset = "furnace-1"`. Do not use `dataGroup`
for sub-asset hierarchy; it is storage/routing metadata.

## Validity / Liveliness

UNS attributes can declare how the controller decides whether they are live or stale; in most apps this is primarily used to drive UI liveliness/activity indicators. In app-level modeling we use two modes only:

- `interval`: continuously refreshed values (stale after ~2× `expectedIntervalMs`)
- `lifecycle`: event-driven activity that stays active until a defined end value (`lifecycleEndValue`)

```ts
await proxy.publishMqttMessage({
  topic: "raw/data/",
  asset: "line-1",
  objectType: "motor",
  objectId: "main",
  attributes: {
    attribute: "status",
    data: { time: new Date().toISOString(), value: "RUNNING" },
    validityMode: "lifecycle",
    lifecycleEndValue: "STOPPED",
  },
});
await proxy.flush();
```

## Counter Attributes

Publish cumulative counters as raw counter state. Do not use producer-side
delta modes for new code; `MessageMode.Delta`, `MessageMode.Both`, and gRPC
`value_is_cumulative` are deprecated because producer memory is lost across
service restarts. Datahub history APIs should calculate delta/rate from
persisted rows.

For a `Data` attribute, mark the series directly:

```ts
await proxy.publishMqttMessage({
  topic: "raw/data/",
  asset: "line-1",
  objectType: "energy-resource",
  objectId: "main",
  attributes: {
    attribute: "active-energy-total",
    description: "Cumulative active energy counter",
    valueType: "number",
    presentationKind: "counter",
    defaultAggregation: "last",
    counterResetPolicy: "new-value",
    data: {
      time: new Date().toISOString(),
      value: 12345.6,
      uom: "kWh",
      dataGroup: "metering",
    },
  },
});
await proxy.flush();
```

For a `Table` attribute, keep the table as the source row and mark chartable
counter columns with `tableColumns`:

```ts
await proxy.publishMqttMessage({
  topic: "raw/data/",
  asset: "line-1",
  objectType: "energy-resource",
  objectId: "main",
  attributes: {
    attribute: "measurements",
    description: "Metering table",
    tableColumns: [
      {
        name: "active_energy_total",
        valueType: "number",
        presentationKind: "counter",
        defaultAggregation: "last",
        counterResetPolicy: "new-value",
      },
    ],
    table: {
      time: new Date().toISOString(),
      dataGroup: "metering",
      columns: {
        active_energy_total: { type: "double", value: 12345.6, uom: "kWh" },
        power: { type: "double", value: 42.1, uom: "kW" },
      },
    },
  },
});
await proxy.flush();
```

`dataGroup` is a storage/routing hint for consumers such as archivers. It is not
part of the UNS identity path and is not the same as `objectType`. For example,
an archiver may persist a `table` packet with `dataGroup: "metering"` into a
separate physical table family while the UNS path still comes from
`topic/asset/objectType/objectId/attribute`.

## High-throughput publishing

For higher publish rates, configure bounded parallel publishing instead of relying on a single in-flight broker callback:

```ts
const proxy = await proc.createUnsMqttProxy(config.infra.host!, "output", "force", true, {
  publishThrottlingDelay: 0,
  publishConcurrency: 16,
  maxPendingPublishes: 5000,
});
```

`publishMqttMessage()` resolves when a message is accepted into the local bounded publisher queue. Asynchronous broker publish failures are logged and emitted on the proxy `error` event.

If the bounded queue is full, `publishMessage()` / `publishMqttMessage()` reject before another message enters the worker channel. The limit covers requests awaiting worker acceptance plus accepted queued and in-flight publishes, so callers cannot create an unbounded main-thread backlog.

Use `await proxy.flush()` or `await proxy.drainPublishes()` before assuming all accepted messages have reached the broker. `await proxy.stop()` closes publish admission immediately and drains by default with a timeout; repeated `stop()` calls share the same result. Use `await proxy.stop({ drain: false })` only when dropping queued messages is acceptable. When a proxy belongs to `UnsProxyProcess`, call `await process.shutdown()` instead of stopping the proxy separately. Process shutdown attempts every cleanup and rejects with an `AggregateError` if any drain or stop fails.

When upgrading an existing application, read [`MIGRATIONS.md`](./MIGRATIONS.md)
and apply only the migrations whose version boundaries are crossed. The
`3.0.0` migration documents the MQTT table-column object contract, while the
`2.0.71` migration documents MQTT publish and shutdown ownership changes.

## Optional configuration-owned process groups

Version3.0.29 adds optional shared contracts for immutable application-owned JSON
resources, reviewed journal generations and same-generation paired version upgrades.
Existing file groups and ordinary services need no migration. Applications validate
business resources; hosts enforce authenticated durable lifecycle operations.
See the [guide and example](https://github.com/uns-openhub/uns-kit/blob/master/docs/process-group-configuration-runtime.md)
and the3.0.29 entry in `MIGRATIONS.md`. Source preparation is not npm publication or
production activation.

## Sync UNS schema from the controller

`sync-uns-schema` fetches the canonical UNS dictionary and measurements from the controller REST API and refreshes local JSON files and generated TypeScript artifacts.

```bash
# Run inside a generated microservice project:
pnpm run sync-uns-schema

# The controller URL is read from config.json (uns.rest) automatically.
# You will be prompted for the bearer token if not set via env var.
```

Or with explicit options:

```bash
pnpm run sync-uns-schema --controller-url http://localhost:3200 --token <bearer-token>
```

**What it updates (inside a generated project):**
- `uns-dictionary.json`
- `uns-measurements.json`
- `src/uns/uns-dictionary.generated.ts`
- `src/uns/uns-measurements.generated.ts`

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--controller-url` | from `config.json` `uns.rest` or `UNS_CONTROLLER_URL` | Controller base URL |
| `--token` | interactive prompt or `UNS_CONTROLLER_TOKEN` | Admin bearer token |
| `--status` | `active` | Dictionary filter: `active`, `draft`, `deprecated`, `all` |
| `--dry-run` | — | Report changes without writing |
| `--dictionary-only` | — | Skip measurements sync |
| `--measurements-only` | — | Skip dictionary sync |
| `--skip-generate` | — | Skip TS regeneration |
| `--project-root <dir>` | auto-detect | Target project root |

## Config schema generation

Edit `src/config/project.config.extension.ts` inside your project and run:

```bash
pnpm run generate-config-schema
```

This regenerates `config.schema.json` and `src/config/app-config.ts`, keeping editor completions and runtime types in sync with your config extensions.

## Infisical secret resolution

- Looks for `INFISICAL_TOKEN` / `INFISICAL_PERSONAL_TOKEN`, then `/run/secrets/infisical_token`.
- If unavailable, logs a warning and returns `default` (or `undefined` for `optional: true` secrets).
- Required secrets throw with the original error message when Infisical is unreachable.
- Call `resolveInfisicalConfig()` to inspect the resolved token/projectId/siteUrl.

## Development

```bash
pnpm run typecheck   # type-check sources
pnpm run build       # emit JS + declarations to dist/
```

## License

MIT © Aljoša Vister
