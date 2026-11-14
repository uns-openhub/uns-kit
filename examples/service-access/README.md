# Service access examples

- `background-reader/`: no API or catalog; a reviewed workload token reads one UNS history source.
- `catalog-summary/`: a catalog-only caller requests a bounded derived count; the workload has no direct source permission.

Each folder is an independent, credential-free Node.js package. Copy one folder
to a new project, install dependencies and build. Deploy/review/start with a
Controller supporting common Service access (verified with Controller 2.2.95).
The [walkthrough](https://github.com/uns-openhub/uns-kit/blob/master/docs/catalog-publication-access.md)
explains what the developer, administrator, Controller, SDK and caller each own.

The examples are also shipped in `@uns-kit/core/dist/runtime/service-access-examples/`.
No real broker, database, user or production endpoint is supplied. Configure your
own local test resources. These files create no identities, grants or MQTT ACLs.

Maintainers: run `pnpm run ts:build` then `pnpm run test:service-access-examples`.
The verifier packs the SDK, installs it in a temporary consumer, builds/packs both
examples, and exercises their real handlers/token provider against a synthetic
loopback API. This checks imports, shipped files, token rotation, denial and
closed output, not real Controller policy, MQTT connectivity or cluster ownership.
