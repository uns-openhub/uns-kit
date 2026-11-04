# Opt-in operator API contract

The operator declaration/delegation contract was introduced in 3.0.42 and shared
managed-route registration in 3.0.43. This source prepares bounded JWKS recovery
in 3.0.44. npm publication and downstream dependency updates are separate from
source preparation and are required before deployment.
Ordinary endpoints do not become operator operations automatically.

## Managed route registration (3.0.43)

For APIs with explicit `operatorOperations`, `@uns-kit/api` publishes a short-lived
ownership receipt through the owning Controller's fixed
`POST /api/runtime-services/routes` endpoint. No application-specific publisher,
package-name branch or new data permission is required. A second unrelated
application uses the same mechanism. The Controller must first bind an approved,
active service identity to the installed instance and launch it with:

- `UNS_ROUTE_TOKEN_FILE`: private, regular mode-600 file containing a narrow route
  credential; it is reread on every renewal and must not be logged or copied into
  application configuration.
- `UNS_ROUTE_CONTROLLER_NAME`, `RTT_NODE`, `RTT_INSTANCE_ID`, `version` and
  `UNS_ROUTE_LAUNCH_ID`: the exact launch identity. The SDK retains the launch's
  version tag, including an existing `v` prefix; it does not invent one.
- `UNS_CONTROLLER_PUBLIC_BASE`: the owning Controller origin. The existing
  launcher host/port is a fallback when that origin is absent. Caller payloads
  cannot select a registration destination or credential.

The SDK registers the actual listening/published API origin after the first
declared endpoint, renews the 30-second lease every 10 seconds and validates that
the receipt matches this exact owner, process, instance, launch, version and
origin. Registration uses its own purpose-specific credential, not a general
service/data JWT or the operator-delegation JWT. It grants no data scopes and
does not authorize a pause or any other application command.

A missing route-token file environment variable preserves legacy/standalone
startup without automatic registration. A partial opted-in environment is an
explicit error. Unavailable or revoked ownership fails closed for managed
operator dispatch; it does not stop unrelated data processing. Requests are
bounded, redirects refused, stalled transports remain coalesced, and stopping the
API cancels renewal without waiting indefinitely for the Controller. A lost
receipt may be retried for the same launch/origin. The Controller remains the
authority for freshness, unique live ownership, operator grants and command review.

For a custom HTTP integration the reusable client is exported as
`RuntimeRouteRegistration` and `runtimeRouteRegistrationEnvironment` from
`@uns-kit/api/runtime-route-registration.js`. The wire schemas/types and receipt
validator are shared through `@uns-kit/core/runtime/route-registration.js`.
Ordinary `UnsApiProxy` applications only declare their operator endpoints; the
host supplies managed launch authority. Preparing this source release does not
establish npm publication or production deployment.

`@uns-kit/core/runtime/operator-api.js` defines the version-one declaration,
canonical bounded JSON digests and exact delegation validation.
`@uns-kit/api` attaches explicit `operatorOperations` to GET/POST registration
options and publishes them in the OpenAPI `x-uns-operator-operations` extension.

```ts
const operatorOperations = [
  {
    protocol: 1,
    id: "maintenance.preview",
    title: "Preview maintenance",
    kind: "read",
    requestSchema: {
      type: "object",
      properties: { action: { type: "string", const: "preview" } },
      required: ["action"],
      additionalProperties: false,
    },
  },
  {
    protocol: 1,
    id: "maintenance.apply",
    title: "Apply maintenance",
    kind: "command",
    writeDomain: "maintenance-policy",
    replay: "request-id-v1",
    requestSchema: {
      type: "object",
      properties: {
        action: { type: "string", const: "apply" },
        requestId: { type: "string", format: "uuid" },
        expectedRevision: { type: "integer", minimum: 0 },
      },
      required: ["action", "requestId", "expectedRevision"],
      additionalProperties: false,
    },
  },
] satisfies import("@uns-kit/core/runtime/operator-api.js").OperatorOperationDefinition[];
// Supply { operatorOperations } to the application's existing api.post(...)
// registration. Namespace identifiers still come from that application's schema.
```

Use a disjoint `action` constant for reads/commands sharing a POST route. The SDK
rejects ambiguous/invalid payloads, query fields, missing authentication and duplicate
operation IDs. Supported JSON Schema is deliberately bounded and closed: no `$ref`,
executable/complex patterns or arbitrary extensions. Limits are 64 KiB canonical
request JSON, 16 declarations/endpoint, timeout 0.5–10 seconds and response
256 B–256 KiB. Default timeout/response budgets are 5 seconds/64 KiB.
Responses must be JSON, including successful command receipts. A 2xx response
confirms delivery/acceptance; poll application status for an asynchronous business
boundary such as a fully drained pause.

The API SDK verifies native operator/admin JWTs and existing route grants. For a
Controller-delegated workload it additionally verifies a purpose-specific PS256
credential, operation/body digests, exact method and current launch. It checks the
parent identity through the Controller's authenticated token-status endpoint on
every call. Unknown/revoked authority and dependency outages fail closed. Managed
processes derive the public JWKS/status endpoints from `UNS_CONTROLLER_PUBLIC_BASE`;
standalone tests may set `operatorJwksUrl` and `operatorAuthorizationStatusUrl`.
Delegated calls always require online parent status, even in standalone mode.
Do not forward, return or log credentials or accept a client-selected authority URL.

## Bounded JWKS recovery (3.0.44)

The shared `OperatorJwtVerifier` recovers transient failures while fetching public
operator keys. This requires no application-specific code, configuration change,
new Controller endpoint, permission or declaration version. It applies to every
application using the SDK's declared operator endpoints. Ordinary bearer-token
verification and application data/queue processing are unchanged.

Concurrent cache misses share one refresh, including its recovery attempt. A
refresh performs at most two GETs to the same configured JWKS URL, with a one-second
deadline per attempt and 100–199 ms jitter before the single retry. Connection
failures, interrupted bodies, HTTP 408/429 and 5xx can recover this way. Rejected,
malformed or oversized key documents fail closed without a retry. Failed refreshes
have a one-second cooldown to prevent each waiting client from starting a new
request. Redirects are refused and credentials are never sent with these GETs.

The 15-second key-cache freshness limit is unchanged. A failed refresh never
extends it and expired keys cannot authorize a request during an outage. A failed
unknown-key lookup does not discard a different key whose original cache lifetime
has not expired. An authoritative successful response replaces the key set,
including an empty response that removes all keys. A transport ignoring abort stays
the only active transport until it settles; callers remain bounded and its late
response cannot install trust.

Only public-key retrieval is retried. The SDK does not replay an operator POST or
business effect. Signature, PS256, JWT type, audience, expiry, request/launch binding
and the fresh parent-authorization check still apply after recovery. Revocation
returns 403; persistent key-source failure returns `503 operator_jwks_unavailable`
before the application handler runs. This behavior does not introduce offline
operator authority or change durable receipt/reconciliation requirements.

Local regression coverage is in `packages/uns-api/tests/operator-jwt.test.ts` and
`operator-api-http.test.ts`. The latter uses real synthetic HTTP endpoints for two
unrelated applications, including delayed headers/bodies, connection reset, one
handler invocation, revoked parent access and a persistent outage. Preparing this
source release does not establish npm publication or deployment; downstream
consumers must use the separately reviewed published 3.0.44 SDK release before
receiving the fix. See the [version-bounded upgrade guidance](../packages/uns-core/MIGRATIONS.md#3044---bounded-operator-jwks-recovery).

The Controller can orchestrate/review a command; **the application owns its effect
and durable replay receipt**. Store actor, request ID, request digest and result in
the same transaction as a transactional local effect. Repeating an identical
ID/body/actor returns that receipt; mismatches are conflicts. In-memory maps are
insufficient across process restart. See the [SQLite receipt example](examples/operator-command-receipt.ts).
For nontransactional remote effects, preserve uncertainty and reconcile before
retry; this contract does not manufacture exactly-once external effects.

Use `markOperatorNotApplied(res)` from
`@uns-kit/api/operator-api-authority.js` only when a rejection provably made no
effect (for example, pre-effect validation/revision conflict). Leave that marker
absent after a possible commit, database outage or uncertain transport failure.
The host then retains the command/write-domain reservation for the same-request
reconciliation, rather than issuing a different command around a lost response.
The SDK does not implement application pause, resume, data deletion or queue policy.
