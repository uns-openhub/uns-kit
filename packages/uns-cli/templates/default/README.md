# __APP_NAME__

Generated with `@uns-kit/cli`.

Requires Node.js 22+ and pnpm (see `packageManager` in package.json).

## Scripts

```bash
pnpm run dev    # start the local development loop
pnpm run build  # emit dist/ output
pnpm run start  # run the compiled entrypoint
pnpm run generate-config-schema  # regenerate config.schema.json and AppConfig augmentations
pnpm run generate-codegen            # regenerate typed GraphQL operations (after configure-codegen)
pnpm run sync-uns-schema -- --controller-url http://localhost:3200 --token <admin-bearer-token>  # pull dictionary + measurements from the controller
pnpm run sync-uns-metadata -- --controller-url http://localhost:3200 --token <metadata-export-token>  # refresh topics/tags/assets for IntelliSense
```

## Configuration

The generator provides three committed, credential-free runtime profiles. Copy
the appropriate profile to the untracked `config.json` before running the app:

| Profile | Use it when | MQTT | Service credential |
|---|---|---|---|
| `config-development-host.json` | Running `pnpm run dev` directly on the host | `localhost` | `.env` → `UNS_SERVICE_TOKEN` |
| `config-development-podman.json` | Deploying through a local Podman OpenHub controller | `mosquitto` | Controller-managed `UNS_SERVICE_TOKEN_FILE` |
| `config-production.json` | Creating a production controller instance | `mosquitto` | Controller-managed token file or approved secret provider |

The profiles contain neither a token nor `uns.email`/`uns.password`. For a
direct host process, copy `.env.example` to `.env`, set a development machine
token, and keep that file untracked. Controller-managed instances receive the
mounted token file and controller identity automatically.

For standalone local development, `pnpm run dev` and `pnpm run start` load a
local `.env` file when it exists. The generated `.gitignore` excludes `.env`
and `.env.*` files so local secrets are not committed. Values already provided
in the process environment take precedence over values from `.env`, and a
missing `.env` file does not prevent the application from starting.

`@uns-kit/core` and `ConfigFile.loadConfig()` continue to read the environment
already prepared for the process; they do not load `.env` themselves. This
local entry-script behavior does not change production, PM2, controller, or
Infisical environment handling.

## Validity / Liveliness

UNS attributes can declare how the controller decides whether they are live or stale; in most apps this is primarily used to drive UI liveliness/activity indicators. In app-level modeling we use two modes only:

- `interval`: continuously refreshed values (stale after ~2× `expectedIntervalMs`)
- `lifecycle`: event-driven activity that stays active until a defined end value (`lifecycleEndValue`)

Example:

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

`publishMqttMessage()` resolves when the message is accepted into the local bounded publisher queue, not when the broker confirms it. Queue-full is reported immediately to the caller. Broker publish failures are emitted later on the proxy `error` event, so use `await proxy.flush()` before shutdown or before assuming all accepted messages were published.

The generated `src/index.ts` is a long-running service. It keeps the UNS process active after the startup publish and calls `UnsProxyProcess.shutdown()` from `SIGINT` and `SIGTERM` handlers. For process-owned MQTT proxies, use that process-level shutdown path instead of stopping the same proxy separately.

## Schema System Metadata

The template includes `src/examples/schema-system-metadata-example.ts`, which
shows how a producer publishes:

- `systemRole: "relationship-evidence"` with `relationshipEvidence`
- `systemRole: "lifecycle-time-source"` with `lifecycle`

These fields are retained in the produced-topics registry and stored by the
UNS OpenHub controller in `attribute_schema.schema_json`.

## Sub-Asset Publishing

The template includes `src/examples/subasset-example.ts`, which shows the V1
sub-asset convention:

- put the full parent asset path in `topic`
- put only the leaf sub-asset name in `asset`

For example, `topic: "enterprise/site/area/line-1/"` and `asset: "pump-1"`
publish `enterprise/site/area/line-1/pump-1/equipment/main/temperature`.
Downstream QuestDB storage keeps the existing columns: `topic` is the parent
asset path and `asset` is the leaf sub-asset.

## Datahub client (last value)

`UnsClient` provides a minimal REST client for the UNS OpenHub API, including the batch last-value endpoint. Prefer the controller-managed token file or a development machine token; do not place it in a tracked config file.

```ts
import { ServiceTokenProvider, UnsClient } from "@uns-kit/core";

const tokenProvider = new ServiceTokenProvider();
const baseUrl = process.env.UNS_CONTROLLER_PUBLIC_BASE;
if (!baseUrl) throw new Error("Set the trusted Controller base URL.");
const client = new UnsClient(baseUrl, {
  tokenProvider,
});

const values = await client.lastValue([
  "raw/data/line-1/motor/main/temperature",
  "raw/data/line-1/motor/main/status",
]);
// Process values without logging credentials or private records.
```

Before a managed background reader starts, ship `service-access.json`, reference
it with `package.json` `unsDatahub.serviceAccess`, retain it in the built package,
and have an administrator apply its exact-release scopes and UNS paths in
**Micro services → instance → Service access**. A declaration requests rights;
it never grants them. A background API reader needs `read:uns` plus its permitted
paths and no human caller. Catalog-mediated reads require the separate reviewed
publication and incoming caller context.

Use the [complete developer walkthrough](https://github.com/uns-openhub/uns-kit/blob/master/docs/catalog-publication-access.md)
and runnable projects shipped in
`node_modules/@uns-kit/core/dist/runtime/service-access-examples/`.
The provider reads `UNS_SERVICE_TOKEN_FILE` for every request; do not capture a
bearer once at startup or fall back after a configured file fails. For direct
host development use an approved machine credential through `UNS_SERVICE_TOKEN`
in the untracked local environment. Do not invent managed RTT launch variables.
MQTT username/password and broker ACLs remain separate from these API rights.

## Next Steps

- Install additional plugins: `pnpm add @uns-kit/api` etc.
- Create MQTT proxies inside `src/index.ts`.
- Extend `src/config/project.config.extension.ts` with project-specific sections and run `pnpm run generate-config-schema` (reload your editor's TS server afterward if completions lag).
- Run `uns-kit configure-devops` to add the Azure DevOps pull-request tooling.
- Run `uns-kit configure-vscode` to copy workspace/launch configuration for VS Code.
- Run `uns-kit configure-codegen` to scaffold GraphQL code generation and UNS refresh scripts.
- Run `pnpm run sync-uns-schema -- --controller-url ... --token ...` to pull `uns-dictionary.json` and `uns-measurements.json` from the controller export into this project, then regenerate the typed files automatically.
- Run `pnpm run sync-uns-metadata -- --controller-url ... --token ...` to regenerate topic, tag, and asset IntelliSense from the controller metadata export. The token must be admin or include `export:uns-reference`.
- Run `uns-kit configure-api` / `configure-cron` to pull in example stubs and install the matching UNS plugins (add `--overwrite` to refresh templates).
- Run `uns-kit configure-python` to copy the Python gateway client template (examples, scripts, proto).
- Commit your new project and start building!

## Agent Onboarding (for AI/code-assist tools)

For agent guidance, see the local [`AGENTS.md`](./AGENTS.md).
