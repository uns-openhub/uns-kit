# Agent Onboarding (template)

Pointers for AI/code-assist tools when working in this generated project.

## What to read locally

- `package.json` for scripts (`sync-uns-*`, `generate-codegen`, etc.).
- `config.schema.json` for the app config shape; `processName` is required.
- `src/examples/*.ts` for cron-driven publish patterns and description registration.
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
- When crossing `<3.0.45` to `>=3.0.45`, ordinary applications keep their behavior. Opt-in retained-store owners must declare `storageProtocol: 1`, use shared resolved resource paths and hold `openRetainedStoreLease` before opening payload writers until final shutdown. Follow the bundled core guide/examples and version-bounded MIGRATIONS.md; require compatible Controller/Runtime/helper releases, preserve original stores and reconcile unknown outcomes under the same operation ID. Never infer storage safety or enable a queue from an SDK version alone.
- When crossing `<3.0.41` to `>=3.0.41`, typed null scalar clears require explicit publisher allowNullData and data.valueType string/number. Update readers first; opt-in peers have a distinct handover compatibility hash, ordinary peers remain unchanged. Never replace null with zero, empty text or a missing update.
- When crossing `<3.0.40` to `>=3.0.40`, async MQTT consumers may opt into managedInput/setInputHandler and bounded RAM publishBackpressure. Review an initial cold upgrade for the changed input compatibility; await all work, preserve failed input and use one retryable SDK signal owner. Supporting hosts must use launch-bound cooperative stop; a legacy PM2 deadline can still force termination. Follow MIGRATIONS.md and the MQTT drain example.

## UNS Kit dependency upgrades

- When crossing `<3.0.38` to `>=3.0.38`, correlated-v2 MQTT handover uses format3 on both peers. Review the initial stop-before-start SDK upgrade; never bypass mixed-format refusal. Missing or ambiguous input overlap keeps the source active. Reconcile an unknown result under the same operation, and return ownership through a fresh reverse hot handover. Follow `MIGRATIONS.md` and the SDK example.

- When crossing `<3.0.32` to `>=3.0.32`, existing services remain unchanged. Enabling a Runtime MQTT outbox on an authorized process group requires the separate reviewed protocol3 transition and shared capability declaration. Preserve input/output journals, stop admission and drain accepted input before stopping the publisher on rollback, and block restoration on pending output or unknown outcomes. Never add application-specific Controller rules.

- When crossing `<3.0.31` to `>=3.0.31`, ordinary services remain unchanged. Optional Runtime-managed MQTT outboxes require explicit `output.publishQueue.storage: runtime`, a trusted private per-Controller external mount and matching backup support. Preserve schema1 queues; restoring managed state requires a stopped source and an empty offline target. Read the version-bounded migration before enabling it.

- When crossing `<3.0.29` to `>=3.0.29`, configuration-owned process groups opt into shared protocol2/policy4. Keep business resources in the app, bind immutable snapshots and journal generations, use reviewed generation/version transitions, and preserve old journals/policies. Ordinary services and existing file groups need no migration.

- Before changing any `@uns-kit/*` version, record the installed source version and intended target version.
- After installing the target version, read `node_modules/@uns-kit/core/MIGRATIONS.md` and apply every migration whose version boundary is crossed. Do not apply unrelated migrations.
- When a project configuration stores database connection values as environment or Infisical references, use `secretValueSchema` for those fields and regenerate `config.schema.json`; retain normal string schemas for non-secret settings.
- When crossing `<2.0.71` to `>=2.0.71`, inspect MQTT proxy ownership and follow the documented shutdown migration. Process-owned and standalone proxies have different shutdown paths.
- When crossing `<3.0.0` to `>=3.0.0`, migrate MQTT `message.table.columns` publishers from named arrays to named objects and consumers to object-entry iteration. Keep schema `tableColumns` and Assistant/UI table formats unchanged.
- When crossing `<3.0.7` to `>=3.0.7`, update the application's direct `zod` dependency to Zod 4, migrate Zod-3-only project config extension APIs, and regenerate `config.schema.json` plus `src/config/app-config.ts`.

<!-- uns-kit:migrations:end -->
