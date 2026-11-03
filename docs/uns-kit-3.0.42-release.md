# UNS Kit 3.0.42 — registered operator operations and request bodies

This coordinated source release introduces an optional, reusable operator API
contract for services. A service declares bounded GET/POST operations in its
existing API registration. Commands supply a UUID request ID, write domain and
durable application receipt; the application retains its business checks and
effects. Controller orchestration is generic and consumes declarations instead
of service-specific control logic.

Delegated calls use purpose-isolated PS256 credentials, exact operation/request/
launch binding and live parent authorization. Existing endpoints do not gain
operator authority automatically. Normal bearer API authentication and standalone
signal/drain behavior remain separate. Controller 2.2.64 adds the matching MCP
and GraphQL host; consumers without the declarations continue to work normally.

The API publisher also synchronizes Service API body metadata with an explicitly
provided mutation request body. This lets existing clients show their JSON editor
even when the full schema was previously present only in OpenAPI. The correction
applies to POST/PUT/PATCH/DELETE, preserves catalog schemas/examples, and creates
no implicit service registrations or permissions.

See [operator contract](operator-api.md), [durable receipt example](examples/operator-command-receipt.ts),
[body metadata](service-api-request-body.md), and the version-bounded
[migration notes](../packages/uns-core/MIGRATIONS.md).

All six TypeScript packages share 3.0.42. The private root package remains 0.0.0;
the Python package and runtime state/journal formats are unchanged. npm publication
remains owner-operated after the source commit reaches master. A local build or
tarball is not evidence that registry consumers can install this release.

Release preparation passed the six-package version check, sequential builds for
all six packages, 448 root SDK tests, 415 assistant-workflow tests and pack/entry
point checks for every package. The optional example Azure connectivity test is
skipped with no Azure credential supplied; it is not a release integration proof.
The package-consumer checks use a task-local npm cache rather than altering the
owner's existing cache permissions.
