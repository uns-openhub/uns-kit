# Agent Onboarding (template)

Pointers for AI/code-assist tools when working in this generated project.

## What to read locally

- `package.json` for scripts (`sync-uns-*`, `generate-codegen`, etc.).
- `config.schema.json` for the app config shape; `processName` is required.
- `src/examples/*.ts` for idiomatic publishing (multi-attribute) and description registration.
- Installed docs (under `node_modules`):
  - `@uns-kit/core/README.md`
  - `@uns-kit/core/MIGRATIONS.md`
  - `@uns-kit/api/README.md` (if installed)
  - `@uns-kit/cron/README.md` (if installed)
  - `@uns-kit/cli/README.md` (if installed)

## Generators in this project

- `pnpm run generate-codegen` -> GraphQL codegen (after configure-codegen)
- `pnpm run sync-uns-schema -- --controller-url ... --token ...` -> pulls `uns-dictionary.json` + `uns-measurements.json` from the controller and regenerates local TS helpers
- `pnpm run sync-uns-metadata -- --controller-url ... --token ...` -> pulls topics/tags/assets from the controller and regenerates local TS helpers

<!-- uns-kit:migrations:start -->
- When crossing `<3.0.44` to `>=3.0.44`, declared operator APIs get bounded, coalesced JWKS GET recovery in the shared API package. No per-application retry/configuration is needed. Preserve the original key TTL, online revocation checks, queues, pause policies and same-command reconciliation; never retry a business effect or widen permissions. Read the 3.0.44 MIGRATIONS.md entry and use the published coordinated API/core dependency versions.
- When crossing `<3.0.43` to `>=3.0.43`, declared operator APIs register their actual route through a reviewed Controller launch automatically. Preserve the exact launch version/owner, private route-token file and data scopes; do not add application-specific route publishers or broaden identity permissions. Standalone and ordinary endpoints remain unchanged. Read the 3.0.43 MIGRATIONS.md entry before changing launch configuration.
- When crossing `<3.0.41` to `>=3.0.41`, typed null scalar clears require explicit publisher allowNullData and data.valueType string/number. Update readers first; opt-in peers have a distinct handover compatibility hash, ordinary peers remain unchanged. Never replace null with zero, empty text or a missing update.
- When crossing `<3.0.40` to `>=3.0.40`, async MQTT consumers may opt into managedInput/setInputHandler and bounded RAM publishBackpressure. Review an initial cold upgrade for the changed input compatibility; await all work, preserve failed input and use one retryable SDK signal owner. Supporting hosts must use launch-bound cooperative stop; a legacy PM2 deadline can still force termination. Follow MIGRATIONS.md and the MQTT drain example.

## UNS Kit dependency upgrades

- When crossing `<3.0.38` to `>=3.0.38`, correlated-v2 MQTT handover uses format3 on both peers. Review the initial stop-before-start SDK upgrade; never bypass mixed-format refusal. Missing or ambiguous input overlap keeps the source active. Reconcile an unknown result under the same operation, and return ownership through a fresh reverse hot handover. Follow `MIGRATIONS.md` and the SDK example.

- When crossing SDK3.0.37, inspect actual ownership wiring, HTTP/cron cleanup and accepted asynchronous effects. Opt-in applications defer discovery until handlers are prepared, declare readiness explicitly, and transfer bounded versioned application state when unfinished windows/sessions affect results. API-only owners use `prepareHandover`, not a fake MQTT data proxy. Never advertise stateful capability from a dependency version alone; follow `MIGRATIONS.md`.

- When crossing `<3.0.32` to `>=3.0.32`, existing services remain unchanged. Enabling a Runtime MQTT outbox on an authorized process group requires the separate reviewed protocol3 transition and shared capability declaration. Preserve input/output journals, stop admission and drain accepted input before stopping the publisher on rollback, and block restoration on pending output or unknown outcomes. Never add application-specific Controller rules.

- When crossing `<3.0.31` to `>=3.0.31`, ordinary services remain unchanged. Optional Runtime-managed MQTT outboxes require explicit `output.publishQueue.storage: runtime`, a trusted private per-Controller external mount and matching backup support. Preserve schema1 queues; restoring managed state requires a stopped source and an empty offline target. Read the version-bounded migration before enabling it.

- When crossing `<3.0.29` to `>=3.0.29`, configuration-owned process groups opt into shared protocol2/policy4. Keep business resources in the app, bind immutable snapshots and journal generations, use reviewed generation/version transitions, and preserve old journals/policies. Ordinary services and existing file groups need no migration.

- Before changing any `@uns-kit/*` version, record the installed source version and intended target version.
- When crossing SDK3.0.36, preserve absent units and gate background publications with isActive() until ownership. Mixed old/new state formats refuse before freeze; use a reviewed initial cold upgrade, never bypass compatibility.
- When opting into SDK 3.0.35 state-preserving MQTT handover, declare protocol2 and per-proxy semantic contracts. Never infer compatibility from package version or matching paths. Use a reviewed reverse handover for return ownership; a cold restart cannot recover counter baselines from RAM. See the version-bounded migration and SDK example.
- After installing the target version, read `node_modules/@uns-kit/core/MIGRATIONS.md` and apply every migration whose version boundary is crossed. Do not apply unrelated migrations.
- When crossing `<2.0.71` to `>=2.0.71`, inspect MQTT proxy ownership and follow the documented shutdown migration. Process-owned and standalone proxies have different shutdown paths.
- When crossing `<3.0.0` to `>=3.0.0`, migrate MQTT `message.table.columns` publishers from named arrays to named objects and consumers to object-entry iteration. Keep schema `tableColumns` and Assistant/UI table formats unchanged.
- When crossing `<3.0.7` to `>=3.0.7`, update the application's direct `zod` dependency to Zod 4, migrate Zod-3-only project config extension APIs, and regenerate `config.schema.json` plus `src/config/app-config.ts`.
- When crossing `<3.0.11` to `>=3.0.11` for an OpenHub 2.x add-on, replace the old `>=7.1 <8` manifest range with `>=2 <3` only after compatibility validation, then use the three credential-free runtime profiles and `ServiceTokenProvider` guidance in `MIGRATIONS.md`.
- When crossing `<3.0.22` to `>=3.0.22`, API logs use server-generated request IDs rather than URLs or unverified email claims. Reuse `req[Symbol.for("uns.http.request-id")]` in authenticated diagnostics; never restore raw topic/parameter logging or trust caller identity headers.
- When a project configuration stores database connection values as environment or Infisical references, use `secretValueSchema` for those fields and regenerate `config.schema.json`; retain normal string schemas for non-secret settings.

<!-- uns-kit:migrations:end -->
