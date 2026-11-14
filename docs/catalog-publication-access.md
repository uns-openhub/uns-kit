# Service access: from declaration to a running service

For application developers and coding agents using **SDK 3.0.47 or later**.
Common Service access also requires a matching Controller; these workflows were
verified with Controller **2.2.95**. SDK installation alone enables no host rights.

The developer declares a permission ceiling and uses the SDK. An administrator
reviews the installed release in **Micro services → instance → Service access**.
The Controller stores that decision, issues the instance credential and supplies
its file path. The SDK reads the current credential when making a request. The UI
is a policy editor, not a place to copy a bearer token into application code.

## Choose the access model

| Application activity | Credential used | Administrator approves | Mechanism |
| --- | --- | --- | --- |
| Background UNS API read | Service token only | `read:uns` and explicit UNS paths | `ServiceTokenProvider` / API client |
| Approved derived catalog operation | Service token plus the incoming user's token | Service capability, exact publication sources/output and audience; user separately needs operation access | `CatalogPublicationClient` + `catalogPublicationHandler` |
| Direct MQTT subscribe/publish | Broker credentials | Broker ACLs for the relevant topics | MQTT connection settings and broker authorization |
| Direct SQL/external API access | That resource's credential | Database/external-system grants | Application adapter and secret references |

**`read:uns` protects OpenHub API reads. It does not install MQTT ACLs.**
MQTT-style filters in Service access describe API resource paths; their syntax
does not make the HTTP service token a broker password. `infra.host` and the
example's `mqtt.username/password` concern runtime MQTT connectivity. Configure
broker ACLs separately, including the runtime signaling topics your deployment
requires. A direct database connection is governed by its database identity.
This capability is not an arbitrary-code sandbox.

## Runnable examples and package contents

Both examples are independent Node.js 22+ projects with complete source,
credential-free configuration, a declaration, build script and package metadata:

- [Background reader](https://github.com/uns-openhub/uns-kit/tree/master/examples/service-access/background-reader)
- [Catalog summary](https://github.com/uns-openhub/uns-kit/tree/master/examples/service-access/catalog-summary)

The npm core package also ships this guide at
`@uns-kit/core/runtime/service-access-guide.md` and both projects under
`node_modules/@uns-kit/core/dist/runtime/service-access-examples/`. Copy a project
to your own directory before installing/building it; do not edit `node_modules`.
Source code, not repeated prose fragments, is the complete runnable example.

Each `package.json` points at the root declaration and retains it in the artifact:

```json
{
  "files": ["dist", "service-access.json", "config-example.json"],
  "unsDatahub": {
    "schemaVersion": 1,
    "kind": "addon",
    "controllerCompatibility": ">=2 <3",
    "serviceAccess": "service-access.json"
  }
}
```

This is metadata to merge into the application's existing package.json. Keep the
real name/version/dependencies/scripts. The SDK is imported as published packages;
no copied validators, source overlays or application-name Controller branches.
Do not declare the historical `catalogAccess` marker alongside `serviceAccess`.

In either copied example:

```sh
pnpm install
pnpm run build
cp config-example.json config.json
```

Select your local MQTT/Controller endpoints and use environment or secret-provider
references for credentials. Never commit config.json, .env, keys or tokens. Under
a Controller deployment, instance configuration is managed by that Controller.
The examples declare ordinary cold operation, not state-preserving hot handover.

## 1. Background service: no Catalog and no HTTP listener

The background reader asks for one source through the OpenHub history API:

```json
{
  "protocol": 1,
  "permissions": {
    "scopes": ["read:uns"],
    "accessRules": ["example/background/line-1/equipment/main/temperature"]
  }
}
```

There is no `publications` section and no human caller. The complete
`background-reader/src/reader.mjs` imports:

```js
import { ServiceTokenProvider } from '@uns-kit/core/tools/auth/service-token-provider.js';
const tokens = new ServiceTokenProvider();
// Inside each request, not once at application startup:
const token = await tokens.getAccessToken();
```

It sends the token as `Authorization: Bearer …` to
`/api/catchall/batch/range` at `UNS_CONTROLLER_PUBLIC_BASE`, with a fixed declared
source, explicit from/to, a row limit, timeout and no redirects. Missing credentials
or 401/403 fail the request; no retry with a human or more powerful credential.
`src/index.mjs` schedules reads only while the runtime is active. Shutdown stops
admission, aborts the pending read and cleans up the runtime on SIGINT/SIGTERM.

Administrator steps after deployment:

1. Open **Service access** for the exact installed example release and instance.
2. Use **Restore defaults** to populate the draft, or select `read:uns` and the
   exact declared path manually. Defaults do not save or grant anything.
3. Review **Permitted UNS paths**, then **Apply access policy** and confirm.
   Only declared paths or narrower filters are allowed; do not replace them with `#`.
4. **Start** the prepared instance. First managed launch requires reviewed access.
   There is no catalog publication to approve.

The Controller supplies `UNS_SERVICE_TOKEN_FILE` and runtime endpoint/identity
metadata. `ServiceTokenProvider` rereads that file for every request, so atomic
credential rotation is visible without an application restart. When the file is
configured it is authoritative: missing/empty/unreadable means failure, not fallback.
Do not cache the returned token for the lifetime of the process.

Acceptance on your local stack: create some history for the declared example
source. Expect 200 for that source and 403 for another source with the same service
credential. Verify a policy narrowing or revocation takes effect without granting
additional paths. Zero rows is a valid response if the selected window has no data.

## 2. Catalog: a user requests a derived result

The catalog summary has empty direct `permissions.scopes` and `accessRules`. Its
optional `publications` declaration defines `sample-count`, GET JSON, from/to with
a timezone, at most one hour/1000 rows from
`example/private/line-1/equipment/main/temperature`, and a closed result containing
only `{count, hasMore}`. The response cap is 4096 bytes. See its complete
`service-access.json`; the source and response schema are reviewed data.

`catalog-summary/src/index.mjs` reads and validates that declaration with
`parseServiceAccessDeclaration`, then constructs `CatalogPublicationClient` using
the real Controller-injected `RTT_NODE`, `RTT_INSTANCE_ID`, `version` and
`UNS_CONTROLLER_PUBLIC_BASE`. It configures JWT/JWKS authentication, calls
`api.registerCatalogRoute()` for the signed installed-launch route lease, and
registers the offer with `registerApiCatalog`. Do not manufacture these launch
variables to make a directly started process appear Controller-managed.

`src/offer.mjs` wraps its business transform with the actual SDK helper:

```js
catalogPublicationHandler(client, 'sample-count', async ({ history }) => {
  const rows = await history('temperature');
  return { count: rows.data.length, hasMore: rows.truncated };
});
```

`temperature` is the declared source ID, not an arbitrary topic or URL from the
request. The complete source also validates the source response. The helper takes
the caller's bearer from the incoming HTTP request. The client separately reads
the service token through `ServiceTokenProvider`; they are never substituted for
each other. The business transform owns the count, not the authorization policy.

Administrator steps after preparation:

1. Apply only `catalog:sources:read` in **Service access**, with **no direct UNS
   paths**. This capability permits the reviewed catalog workflow, not raw reads.
2. Expand **Published operations → Review access**. Check the exact source,
   maximum interval/rows, **Published fields** and intended audience.
3. **Review publication**, then **Approve publication**. A selected audience role
   does not itself grant every user access to the catalog operation.
4. Give a test reader access to the operation's SDK resource path:
   `example/public/line-1/equipment/dataset/sample-count/json` (without `/api/`).
   Do not grant the private source path just to make Execute work.
5. Start the prepared instance and Execute the operation as that reader, for
   example `from=2026-01-01T00:00:00Z` and `to=2026-01-01T01:00:00Z`.

When moving an existing service from legacy credentials to this declaration,
prepare the new version first. Use **Stop → review Service access/publication →
Start prepared version** on the chosen owner. A still-live older version can
block inspection of the prepared release; the same-instance Apply guard requires
this first-adoption sequence. Retain the old configuration for controlled rollback
until acceptance; do not grant raw access to get past an adoption error.

The request path is:

1. Browser sends the user's token to the service API through the Controller.
2. The SDK sends its service credential and the caller context to the Controller.
3. Controller checks current user operation access, audience, active service
   identity/generation, live launch, loaded configuration, declaration and approval.
4. Controller obtains bounded source data using a short-lived exact-source
   credential internally. It never sends that credential to the service/browser.
5. The service transforms the data; SDK rechecks authority after the asynchronous
   work and validates the closed output schema before returning JSON.

Only SDK-integrated execution gets that output validation. Preserve ordinary
endpoint JWT/JWKS guards; a custom endpoint or custom download must use the same
mediated path. Arbitrary app code with independent DB/MQTT credentials is outside
this enforcement. Revocation cannot erase bytes already delivered.

Acceptance: reader Execute 200/{count,hasMore}; caller and service direct source
reads 403; unapproved/revoked publication 403; invalid window/unknown parameter
400; no raw source fields or credentials in result/logs. The caller needs no
source permission in `approved-service` mode. `caller` mode instead requires the
caller's own source permissions as well; it never retries with stronger authority.

## Contract limits

The publication protocol supports GET JSON, string query parameters, exact history
topics and closed primitive/object/array schemas. Schema references, defaults and
composition, wildcard source topics, arbitrary URLs/SQL and undeclared inputs are
rejected. Source path segments use ASCII letters, digits and hyphens. Each source
binds the required timezone-qualified `from`/`to`, at most 31 days and 27000 rows.
The broker caps source responses at 1 MiB; the separately declared output cap is
also at most 1 MiB. The example intentionally chooses narrower limits.

A combined service can declare background `read:uns` paths and `publications` in
the same envelope. Review both separately. A publication source never becomes a
directly readable background path just because it appears in the manifest.
Unknown or administrative service scopes are rejected.

Ordinary credential renewal and Controller restart preserve a confirmed live
launch binding. They do not approve a different identity, declaration, policy,
configuration or launch. Missing or changed evidence requires reviewed recovery;
an expired bearer remains invalid.

## Troubleshooting and agent checklist

| Symptom | Check |
| --- | --- |
| Start requires access review | Exact installed release and `unsDatahub.serviceAccess`; admin must apply the policy |
| Credential file missing | Real Controller launch/file access; never copy a human token into config |
| Background read 403 | Active workload identity, `read:uns`, matching permitted path, current generation |
| Catalog 403 | Caller operation access, audience, service capability and current publication approval |
| Placement/access changed | Current owner/version/loaded config; stop the legacy owner for first adoption and review the prepared release |
| API works, MQTT denied | Broker credentials/ACLs; API scopes do not create broker access |
| Configuration changed | Review the new configuration/contract and restart as required; token refresh cannot replace loaded-launch evidence |

For an agent generating a service: choose the model above, use the published SDK
imports, ship/validate the declaration, preserve local cleanup, and list the admin
review needed. Do not create identities, modify user rules or broker ACLs, disable
JWT checks, synthesize launch metadata, or broaden paths as an automatic fix.
Keep install/build, review, start, release and production deployment separate.

Maintainers run `pnpm run ts:build` and `pnpm run test:service-access-examples`.
The latter verifies packaged imports/assets, rotating token use, failures and the
real catalog HTTP helper using synthetic loopback endpoints. It does not certify
real MQTT ACLs, Controller RBAC/cluster ownership, or production resource grants.
Additional SQL, filesystem, broker and external-resource permission adapters are
outside this protocol. Follow the version-bounded core MIGRATIONS.md on adoption.
