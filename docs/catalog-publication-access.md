# Service access and publications (3.0.47)

This opt-in protocol separates permission to call a catalog operation from
permission to read its source. It is not a sandbox for arbitrary application
code or a substitute for restricted database and MQTT identities.

Package metadata requests authority; it never grants it. Keep existing JWT/JWKS
endpoint guards. Do not forward caller tokens to privileged sources or retry a
denied caller read with stronger credentials.

## Declaration

Ship a bounded JSON file with the installed package, and retain it in the build:

```json
{"unsDatahub":{"serviceAccess":"service-access.json"}}
```

Use `parseServiceAccessDeclaration` from `@uns-kit/core/runtime/service-access.js`:

```json
{
  "protocol": 1,
  "permissions": {
    "scopes": ["read:uns"],
    "accessRules": ["example/background/#"]
  }
}
```

This works without a catalog, HTTP endpoint, or human requester. The declaration
requests a ceiling; an administrator must apply the effective policy under
**Microservices → instance → Service access** before the service can start with
a managed identity. Review the exact installed version and declaration digest.
The administrator can select fewer capabilities and narrower MQTT path filters;
unknown or administrative scopes are rejected. The identity, revocation and
same-request reissue mechanisms are the existing managed workload mechanisms.
UNS resource rules bound direct UNS reads; other recognized runtime scopes retain
their existing authorization semantics. This protocol does not create new
SQL, broker, filesystem or arbitrary external-service permissions.

A combined service adds optional `publications`, containing the manifest parsed
by `parseCatalogPublicationManifest`, to the same envelope. A publication-only
service uses empty `permissions.scopes` and `permissions.accessRules`. Its
managed token then has `catalog:sources:read` and **no raw UNS paths**. Adding
publication sources never adds them to the service's direct `read:uns` rules.
Publication approval is a separate operation/audience grant and never silently
grants other package-requested capabilities. Review both in Service access.
The Catalog contains execution/status and an administrator link to that editor.
There is one writable publication editor.

The earlier unpublished `unsDatahub.catalogAccess` marker remains an isolated
compatibility adapter for the first local fixtures. New consumers use
`serviceAccess`; declaring both markers is rejected. No released applications
are automatically migrated.

The optional publication manifest supports GET JSON,
closed JSON schemas (primitive, object and array types; no references, defaults,
or composition), string query parameters, and exact history source topics.
Source path segments follow the existing managed-identity resource vocabulary:
ASCII letters, digits and hyphens. Each source binds required `from`/`to` date-time parameters, a maximum window
(at most 31 days) and row count (at most 27000). Dates require a UTC offset.
Wildcard topics, arbitrary URLs/SQL/source selectors and undeclared input fields
are rejected. The broker caps source responses at 1 MiB; published responses
have a separately declared cap of at most 1 MiB.

Each operation declares `caller` or `approved-service`. Both require an explicit
administrator publication review and current caller permission on the SDK UNS
resource path (the operation path without `/api/`). In caller mode, source rules
and `read:uns` must also belong to the caller. Service mode never grants these
raw permissions to the caller.

## Runtime

Use `CatalogPublicationClient` with the installed, normalized manifest and the
Controller-provided runtime identity (`RTT_NODE`, `version`, `RTT_INSTANCE_ID`,
`UNS_CONTROLLER_PUBLIC_BASE`). The default `ServiceTokenProvider` reads the
Controller's rotating token file. There is no caller-token fallback.

Use `catalogPublicationHandler(client, operationId, transform)` in an existing
`registerApiCatalog` handler. Call `api.registerCatalogRoute()` to opt into the
existing signed launch route lease. The transform receives `history(sourceId)`
and the already validated input. It owns the derived result, not authorization.
The HTTP helper sets `Cache-Control: no-store` and rejects undeclared result
fields. Each execution rechecks current authority after asynchronous work,
including cached application results. Custom downloads and other handlers must
use the same mediated path; this candidate adds no persistent result cache.

A publication-only workload token has `catalog:sources:read` and no direct
`read:uns` authority. A combined service may separately receive `read:uns` on
its reviewed background paths, without receiving raw access to publication
sources. The Controller broker checks current user policy, active machine
identity/token generation, reviewed identity binding, launch, loaded portable
configuration, publication revision, audience and source bounds. Its internal
history token is exact-topic, lasts 15 seconds, and never leaves the broker.
Neither token bytes nor raw records belong in logs, review receipts or audit.

Two local acceptance consumers (energy intervals and quality sample counts) use
the same helper and Controller path. No application-name branch is needed.

## Adoption boundary

The SDK contract is introduced in 3.0.47. A matching Controller implementation
must also be installed; an SDK upgrade alone does not enable the host capability.
Existing applications do not change mode
implicitly. Install the shared SDK release first, update consumer lockfiles,
then review the matching installed declaration through the Controller.
Controller publication is bound to the installed declaration and portable
configuration; edits require review. A changed loaded configuration requires a
new launch, not token refresh. With the matching Controller candidate, the
signed digest of the same confirmed live launch survives ordinary credential
expiry and Controller restart. Current identity, policy, generation and approval
are still checked; expired bearer tokens remain invalid. Missing, corrupt,
foreign or changed-launch evidence requires explicit reviewed recovery.


## Local common-contract acceptance

An unrelated background history processor, with no API or Catalog offer, was
reviewed, narrowed live, revoked, reissued with the same identity and cold-moved
to another Controller. It retained the same policy and generation across the
move. A combined service read its separate background source directly, while a
catalog-only user consumed a derived result from a different approved source.
Both the user and workload were denied direct access to that publication source.
A publication-only empty raw policy stayed empty in storage and actual tokens.

These are local candidate checks, including clean packed-SDK installs. They are
not production, arbitrary-code isolation or external-resource adapter acceptance.
SDK publication and downstream frozen dependency updates
remain separate steps.
