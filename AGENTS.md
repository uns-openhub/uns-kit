<INSTRUCTIONS>
## Repo-specific workflow notes (uns-kit)
- Do not run any pull-request / deploy workflows from Codex for this repo unless explicitly asked.
- Default deliverable for PR-related work here is a clear, ready-to-use git commit message (and the code changes), not opening a PR.
- Hosting: `uns-kit` is on GitHub. Other UNS projects may still live on Azure DevOps, so avoid assuming Azure DevOps PR tooling applies here.
- Shared runtime architecture:
  - UNS Kit owns reusable capability/protocol schemas, common validation, identity rules and runtime helpers. Controller owns generic authenticated orchestration, durable platform state and recovery; applications own business parsing, profiles, mappings, topics and units.
  - New applications or groups using an already supported capability must work through declarations and reviewed configuration, without a package-name branch or business-specific change in Controller. Verify a second unrelated application with different selectors/resources.
  - Put a missing reusable contract/helper in this SDK first, then integrate the generic host and consumers. Do not copy validators between repositories or execute application code to inspect or authorize capabilities.
  - Preserve standalone signal/drain cleanup without Controller presence. Keep historical compatibility readers isolated and frozen; new applications/groups use the shared versioned protocol.
  - The concrete process-group API and its supported limits are documented in `docs/process-group-runtime.md`. Capability declarations do not grant operator permissions; unknown execution outcomes require reconciliation.
- Mainline branch and commits:
  - Work directly on the repository's remote default branch, currently `master` (`origin/HEAD` and CI both target it). The local `main` branch is stale; do not use it as the release branch. Recheck the remote default branch if the repository is renamed or reconfigured.
  - Commit and push scoped, reviewed package changes directly to that branch; no PR is needed. Use clear commit messages and do not include unrelated working-tree changes.
- TypeScript package release preparation:
  - For every publishable `@uns-kit/*` package change, bump the shared package version exactly once with `pnpm run ts:version:patch` before committing. Do not bump the private root package's `0.0.0` version. This script updates all TypeScript package manifests and the CLI's `unsKitPackages` pins; never bump only one package by hand.
  - Run `pnpm run ts:version:check` and `pnpm run ts:build`, plus verification relevant to the changed package. Review the version and generated-file diff, then commit the code and version changes together. A documentation-only `AGENTS.md` edit does not require a package version bump.
  - Publication is a separate manual step after the committed changes are on the mainline branch. The user runs `pnpm login` and then `pnpm run ts:publish` (the repository has no `submit` script); the latter publishes all `@uns-kit/*` packages. Codex does not run authenticated publication unless explicitly asked.
- Python package release preparation:
  - For changes to `packages/uns-py`, use `pnpm run py:version:patch` and `pnpm run py:build`; do not apply the TypeScript version script to a Python-only change. `pnpm run py:publish` is also a separate authenticated publication step.
- Downstream migrations:
  - When a runtime behavior change requires application changes, add a version-bounded entry to `packages/uns-core/MIGRATIONS.md` and keep generated `AGENTS.md` upgrade guidance aligned with it.
  - From `@uns-kit/core` 3.0.19, provider-aware Asset publishers should use `issueAssetIdentityPublicationEvidenceByExternalIdentity()` and publish exactly one returned identity metadata mode; legacy publishers require no change.
- Active cross-repository migration:
  - The canonical plan for changing MQTT `IUnsTable.columns` from a named-column array to an object keyed by column name is `docs/mqtt-table-columns-object-migration-plan.md`. Read and update its status before implementing or reviewing any part of that breaking change.
</INSTRUCTIONS>
