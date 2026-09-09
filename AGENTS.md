<INSTRUCTIONS>
## Repo-specific workflow notes (uns-kit)
- Do not run any pull-request / deploy workflows from Codex for this repo unless explicitly asked.
- Default deliverable for PR-related work here is a clear, ready-to-use git commit message (and the code changes), not opening a PR.
- Hosting: `uns-kit` is on GitHub. Other UNS projects may still live on Azure DevOps, so avoid assuming Azure DevOps PR tooling applies here.
- Workflow scripts:
  - OK to run: `pnpm run ts:version:patch` and `pnpm run ts:build` (non-interactive).
  - Do not run by default: `pnpm run ts:publish` requires interactive/manual handling; user will publish by hand unless explicitly requested otherwise.
- Commits:
  - It’s OK to commit directly to `master` (no PR needed); prioritize good commit messages.
- Downstream migrations:
  - When a runtime behavior change requires application changes, add a version-bounded entry to `packages/uns-core/MIGRATIONS.md` and keep generated `AGENTS.md` upgrade guidance aligned with it.
  - From `@uns-kit/core` 3.0.19, provider-aware Asset publishers should use `issueAssetIdentityPublicationEvidenceByExternalIdentity()` and publish exactly one returned identity metadata mode; legacy publishers require no change.
- Active cross-repository migration:
  - The canonical plan for changing MQTT `IUnsTable.columns` from a named-column array to an object keyed by column name is `docs/mqtt-table-columns-object-migration-plan.md`. Read and update its status before implementing or reviewing any part of that breaking change.
</INSTRUCTIONS>
