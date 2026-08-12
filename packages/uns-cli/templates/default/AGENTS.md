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

## UNS Kit dependency upgrades

- Before changing any `@uns-kit/*` version, record the installed source version and intended target version.
- After installing the target version, read `node_modules/@uns-kit/core/MIGRATIONS.md` and apply every migration whose version boundary is crossed. Do not apply unrelated migrations.
- When crossing `<2.0.71` to `>=2.0.71`, inspect MQTT proxy ownership and follow the documented shutdown migration. Process-owned and standalone proxies have different shutdown paths.
- When crossing `<3.0.0` to `>=3.0.0`, migrate MQTT `message.table.columns` publishers from named arrays to named objects and consumers to object-entry iteration. Keep schema `tableColumns` and Assistant/UI table formats unchanged.
- When crossing `<3.0.7` to `>=3.0.7`, update the application's direct `zod` dependency to Zod 4, migrate Zod-3-only project config extension APIs, and regenerate `config.schema.json` plus `src/config/app-config.ts`.
- When crossing `<3.0.11` to `>=3.0.11` for an OpenHub 2.x add-on, replace the old `>=7.1 <8` manifest range with `>=2 <3` only after compatibility validation, then use the three credential-free runtime profiles and `ServiceTokenProvider` guidance in `MIGRATIONS.md`.

<!-- uns-kit:migrations:end -->
