# @uns-kit/api

`@uns-kit/api` exposes Express-based HTTP endpoints for UNS deployments. The plugin attaches a `createApiProxy` method to `UnsProxyProcess`, handles JWT/JWKS access control, automatically publishes API metadata back into the Unified Namespace, and serves a Swagger UI for every registered endpoint.

Note: Apps built with uns-kit are intended to be managed by the **UNS OpenHub controller**.
For the MQTT topic registry published by the API plugin, see `../../docs/uns-topics.md`.

## uns-kit in context

| Package                                                                               | Description                                                                 |
| ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| [`@uns-kit/core`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-core) | Base runtime (UnsProxyProcess, MQTT helpers, config tooling, gRPC gateway). |
| [`@uns-kit/api`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-api)   | Express plugin — HTTP endpoints, JWT/JWKS auth, Swagger, UNS metadata.      |
| [`@uns-kit/cron`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-cron) | Cron-driven scheduler that emits UNS events on a fixed cadence.             |
| [`@uns-kit/cli`](https://github.com/uns-openhub/uns-kit/tree/main/packages/uns-cli)   | CLI for scaffolding new UNS applications.                                   |

## Installation

```bash
pnpm add @uns-kit/api
# or
npm install @uns-kit/api
```

`@uns-kit/core` must also be present — the plugin augments its `UnsProxyProcess` type.

## How it works

1. Import `@uns-kit/api` as a side-effect to register the plugin.
2. Call `process.createApiProxy(instanceName, options)` to start an Express server.
3. Register endpoints with `api.get(...)` or `api.post(...)`.
4. Listen to `apiGetEvent` / `apiPostEvent` to handle incoming requests.

Every registered endpoint is:

- Automatically secured with JWT/JWKS or a shared secret.
- Published to the UNS controller as an API metadata record (topic, host, path, method).
- Added to the Swagger spec served at `/<processName>/<instanceName>/swagger.json`.

## GET endpoint example

```ts
import "@uns-kit/api";
import { type UnsProxyProcessWithApi } from "@uns-kit/api";
import type { UnsEvents } from "@uns-kit/core";
import UnsProxyProcess from "@uns-kit/core/uns/uns-proxy-process";

const config = await ConfigFile.loadConfig();
const proc = new UnsProxyProcess(config.infra.host!, { processName: config.uns.processName }) as UnsProxyProcessWithApi;

const api = await proc.createApiProxy("my-service", {
  jwks: { wellKnownJwksUrl: config.uns.jwksWellKnownUrl },
  // or for simple/dev deployments: jwtSecret: "CHANGEME"
});

// Register a GET endpoint
// Signature: api.get(topic, asset, objectType, objectId, attribute, options?)
await api.get("enterprise/site/area/line/", "line-3-furnace", "energy-resource", "main-bus", "current", {
  tags: ["Energy"],
  apiDescription: "Current reading for line-3-furnace main-bus",
  queryParams: [
    { name: "from", type: "string", required: false, description: "Start of time range (ISO 8601)", chatCanonical: "from" },
    { name: "to", type: "string", required: false, description: "End of time range (ISO 8601)", chatCanonical: "to" },
    { name: "limit", type: "number", required: false, description: "Maximum records", chatCanonical: "limit", defaultValue: 100 },
  ],
  chatDefaults: { limit: 100 },
});

// Handle incoming GET requests
api.event.on("apiGetEvent", (event: UnsEvents["apiGetEvent"]) => {
  const { from, to, limit } = event.req.query;
  // fetch your data here
  event.res.json({ status: "ok", data: [] });
});
```

## POST endpoint example

```ts
// Register a POST endpoint
// Signature: api.post(topic, asset, objectType, objectId, attribute, options?)
await api.post("enterprise/site/area/line/", "line-3-furnace", "energy-resource", "main-bus", "setpoint", {
  tags: ["Energy"],
  apiDescription: "Write a new setpoint for line-3-furnace main-bus",
  requestBody: {
    description: "Setpoint payload",
    required: true,
    schema: {
      type: "object",
      required: ["value"],
      properties: {
        value: { type: "number", description: "Target setpoint value" },
        unit: { type: "string", description: "Unit of measurement, e.g. A" },
      },
    },
  },
});

// Handle incoming POST requests — body is pre-parsed JSON
api.event.on("apiPostEvent", (event: UnsEvents["apiPostEvent"]) => {
  const { value, unit } = event.req.body;
  // write/command logic here
  event.res.json({ status: "ok", received: { value, unit } });
});
```

## Data offer example

```ts
import "@uns-kit/api";
import {
  type UnsProxyProcessWithApi,
  defineDataCatalogField,
  defineDataCatalogOfferSource,
  defineDataCatalogQueryParam,
  defineDataCatalogSchema,
  defineServiceApi,
  projectRowsForDataCatalogSchema,
  registerApiCatalog,
} from "@uns-kit/api";
import { ConfigFile, UnsProxyProcess } from "@uns-kit/core";

const config = await ConfigFile.loadConfig();
const proc = new UnsProxyProcess(config.infra.host!, {
  processName: config.uns.processName,
}) as UnsProxyProcessWithApi;

const api = await proc.createApiProxy("catalog", {
  jwks: { wellKnownJwksUrl: config.uns.jwksWellKnownUrl! },
});

const orderRows = [
  { ORDER_ID: "PO-1001", STATUS: "released" },
  { ORDER_ID: "PO-1002", STATUS: "finished" },
];

const productionOrdersSchema = defineDataCatalogSchema({
  id: "production-orders",
  title: "Production Orders",
  contentType: "application/json",
  fields: [
    defineDataCatalogField("count", "number", "Number of returned rows"),
    defineDataCatalogField("data", "array", "Returned rows"),
    defineDataCatalogField("orderId", "string", "Order id", { sourceKey: "ORDER_ID" }),
    defineDataCatalogField("status", "string", "Order status", { sourceKey: "STATUS" }),
  ],
  examplePayloads: [
    {
      count: 1,
      data: [{ orderId: "PO-1001", status: "released" }],
    },
  ],
});

const productionOrderRowSchema = defineDataCatalogSchema({
  id: "production-orders-row",
  title: "Production Order Row",
  contentType: "application/json",
  fields: productionOrdersSchema.fields?.filter((field) => field.name !== "count" && field.name !== "data") ?? [],
});

const serviceApis = {
  status: defineServiceApi({
    attribute: "status",
    method: "GET",
    description: "Service status endpoint",
    tags: ["service"],
    handler: async (event: any) => {
      event.res.json({ status: "ok" });
    },
  }),
  command: defineServiceApi({
    attribute: "command",
    method: "POST",
    description: "Service command endpoint",
    tags: ["service"],
    requestBody: {
      required: true,
      description: "Command payload",
      contentType: "application/json",
      schemas: [
        defineDataCatalogSchema({
          id: "service-command-request",
          title: "Service Command Request",
          contentType: "application/json",
          fields: [
            defineDataCatalogField("command", "string", "Command name", { required: true, example: "refresh-cache" }),
            defineDataCatalogField("target", "string", "Optional target", { example: "orders" }),
          ],
        }),
      ],
    },
    handler: async (event: any) => {
      const { command, target } = event.req.body ?? {};
      if (!command) {
        return event.res.status(400).json({ error: "Missing command" });
      }

      event.res.json({
        status: "accepted",
        received: {
          command,
          ...(target ? { target } : {}),
        },
      });
    },
  }),
};

const dataOfferSources = {
  productionOrders: defineDataCatalogOfferSource({
    offerId: "production-orders",
    topic: "factory/demo/app",
    asset: "orders",
    objectType: "dataset",
    objectId: "production",
    attribute: "list",
    displayName: "Production Orders",
    description: "Browse production orders.",
    method: "GET",
    tags: ["orders"],
    queryParams: [defineDataCatalogQueryParam("status", "Optional status filter")],
    schema: productionOrdersSchema,
    response: {
      statusCode: "200",
      contentType: "application/json",
    },
    handler: async (event: any) => {
      const statusFilter = String(event.req.query.status ?? "")
        .trim()
        .toLowerCase();
      const filteredRows = statusFilter ? orderRows.filter((row) => row.STATUS.toLowerCase() === statusFilter) : orderRows;
      const data = projectRowsForDataCatalogSchema(filteredRows, productionOrderRowSchema);

      event.res.json({
        count: data.length,
        data,
      });
    },
  }),
};

await registerApiCatalog(api, {
  serviceApis,
  dataOfferSources,
  context: undefined,
});
```

## Stream a Data Catalog Parquet export

`writeSchemaRowsToParquetStream` accepts a synchronous or asynchronous row
iterator and writes one Parquet row group at a time. It never collects the whole
source in memory. Pass an already bounded/paged history or database iterator;
the writer cannot reduce memory used by a source that loads every row first.

```ts
import { writeSchemaRowsToParquetStream } from "@uns-kit/api";

const filePath = await writeSchemaRowsToParquetStream({
  schema: rowSchema,
  rows: historyRows(), // AsyncIterable<Record<string, unknown>>
  rowGroupSize: 2_000,
  signal: requestAbortSignal,
});
// Stream filePath to the caller, then remove it when the response closes.
```

The helper creates the output directory when needed, writes to a unique
incomplete file, and renames it only after success. An error or cancellation
removes the incomplete file and rejects the promise. The caller owns the
completed file and its cleanup. The older `writeSchemaRowsToParquet` still
returns `null` on failure for existing applications, but now also accepts a
row iterator and the same row-group options.

## Endpoint signature

```
api.get(topic, asset, objectType, objectId, attribute, options?)
api.post(topic, asset, objectType, objectId, attribute, options?)
```

| Parameter    | Type                                           | Description                                                 |
| ------------ | ---------------------------------------------- | ----------------------------------------------------------- |
| `topic`      | `UnsTopics`                                    | UNS topic path prefix (e.g. `"enterprise/site/area/line/"`) |
| `asset`      | `UnsAsset`                                     | Asset identifier (e.g. `"line-3-furnace"`)                  |
| `objectType` | `UnsObjectType`                                | UNS object type (e.g. `"energy-resource"`)                  |
| `objectId`   | `UnsObjectId`                                  | Object instance id (e.g. `"main-bus"`)                      |
| `attribute`  | `UnsAttribute`                                 | Attribute name (e.g. `"current"`)                           |
| `options`    | `IGetEndpointOptions` / `IPostEndpointOptions` | Tags, description, query params / request body              |

## Auth options

| Option                  | When to use                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| `jwks.wellKnownJwksUrl` | Production — verifies RS256 tokens from the UNS controller JWKS endpoint |
| `jwks.activeKidUrl`     | Optional companion to JWKS — narrows which key ID is active              |
| `jwtSecret`             | Development / simple deployments — symmetric secret                      |

Requests that fail auth return `401 Unauthorized`. Requests whose token `accessRules` do not match the endpoint path return `403 Forbidden`.

## Unregistering an endpoint

```ts
await api.unregister(topic, asset, objectType, objectId, attribute, "GET");
await api.unregister(topic, asset, objectType, objectId, attribute, "POST");
```

This removes the route from Express, the Swagger spec, and the internal UNS endpoint registry.

## registerCatchAll (uns-api-global only)

`api.registerCatchAll()` is reserved for the **uns-api-global** microservice, which acts as a
catch-all gateway for an entire topic namespace. **Regular microservices must not call this** —
use `api.get()` / `api.post()` for per-attribute endpoints instead.

## Scripts

```bash
pnpm run typecheck
pnpm run build
```

`build` emits both JavaScript and type declarations to `dist/`.

## License

MIT © Aljoša Vister
