# Python service authority and asynchronous IO

Prepared in Python SDK **0.2.8**, against TS SDK source **3.0.54**.
These are source/package verification results, not publication or deployment.
The previous runtime/discovery slice remains documented in the
[parity matrix](python-typescript-parity.md).

## Three separate permissions

- A background UNS API reader uses its own reviewed service identity and explicit
  UNS paths. It needs no human caller token.
- Caller-mediated catalog operations and purpose-bound Operator delegation need
  their own protocols. This slice does not implement those Python protocols.
- MQTT ACLs and external SQL permissions use their own broker/database identities.
  `read:uns`, a package declaration or an HTTP service token does not grant them.

The existing [Service access walkthrough](catalog-publication-access.md) defines
the shared Controller review boundary. No SDK helper creates identities, approvals
or live grants. Do not copy human credentials into a service as a workaround.

## Background declarations

`parse_background_service_access_declaration()` and
`load_background_service_access_declaration(package_file)` implement the
background-only protocol-1 declaration subset. Inline `unsDatahub.serviceAccess`
and safe package-relative JSON references normalize scopes/rules through the
same Python parser. The loader reads regular, bounded files without importing
application code, rejects traversal/links and requires no-follow directory
support. Filesystem platforms without that primitive fail explicitly.

Scope/rule limits follow the TS background contract. Unsupported configured
protocol 2, publications and conflicting `catalogAccess` fields are rejected;
they never become broad direct access. Inline declarations also need a Controller
that advertises inline inspection support. Keep compatible file declarations
where the host requires them. Exact artifact/config/instance review belongs to
the Controller, not to this parser.

Two independent [reference readers](../examples/python-service-access/README.md)
use different packages and sources with the same helpers. TS loads their manifests
through its existing parser; Python tests execute their actual entry functions
against a synthetic local API.

## Async UNS API client

```python
from uns_kit import AsyncUnsClient, ServiceTokenProvider

async with AsyncUnsClient(
    controller_base,
    token_provider=ServiceTokenProvider(),
    max_pending=32,
    timeout=10.0,
) as client:
    history = await client.history(
        "example/production/line-1/equipment/main/temperature",
        from_="2026-01-01T00:00:00Z",
        to="2026-01-01T01:00:00Z",
        limit=100,
    )
```

The awaitable methods are `get`, `get_data`, `post`, `last_value`,
`get_attribute_data`, `history`, `ensure_token`, `set_token` and `close`/`aclose`.
They reuse the synchronous client's URL construction, response parsers, result
classes and error/404 behavior. No second endpoint/query validator is introduced.

Each client serializes IO on its own bounded worker. This is offloaded synchronous
HTTP, not a native async socket implementation. File reads, explicit legacy auth
and HTTP happen outside the asyncio loop. The default credential provider is
`ServiceTokenProvider`, read anew for each authorized request. A missing/empty
provider never triggers implicit user login. A manual token or explicitly supplied
legacy `AuthClient` remains an explicit application choice.

Async requests stay on the configured HTTP(S) origin, refuse redirects, bound
response bytes (default 16 MiB) and do not replay HTTP operations. Error messages
omit upstream bodies, credentials and URLs. Explicit synchronous service-provider
requests use the same transport and origin protection; legacy synchronous
requests without a provider retain their transport behavior.

`timeout` is in seconds for HTTP connection/read operations and body-budget
checks. It does not include time queued behind another accepted operation,
custom token-provider work or a legacy login. OS DNS/connect handling may outlive
caller cancellation. Admission remains bounded while the underlying IO runs.
Use application deadlines for caller responsiveness without treating a timeout
as proof that a POST had no effect.

## Receive-side service authorization

Ordinary authenticated `UnsApiProxy` routes still verify JWT/JWKS and path rules.
JWT/JWKS work is now offloaded through a bounded worker so JWKS IO does not block
the API event loop. After signature verification, any machine/service claim hint
requires current authority from the Controller's `POST /api/auth/token-status`:

```python
api_options = {
    "jwks": {"wellKnownJwksUrl": trusted_jwks_url},
    "serviceAuthorizationStatusUrl": trusted_status_url,
}
```

The dataclass option is `service_authorization_status_url`.
The TS mapping alias `operatorAuthorizationStatusUrl` is accepted as well.
Without an explicit option, `UNS_CONTROLLER_PUBLIC_BASE` selects the fixed
`/api/auth/token-status` path on that origin. Tokens and caller parameters cannot
choose the destination. A missing endpoint yields 503 for service requests.

Every service request needs active protocol 1 status, `principalType: machine`
and `configuredAccessAuthority: 2`. No positive result is cached. Old Controllers,
unavailable/redirecting endpoints, malformed/oversized responses and uncertain
authority cannot authorize a handler. Verification response time is bounded to
one second; at most sixteen checks remain admitted, including work whose callers
timed out. Status responses are limited to 4096 bytes and no bearer is forwarded
through a redirect.

A verified `configuredService` claim receives 403
`service_api_permission_required`: reviewed UNS data access is not permission to
call an ordinary service API. Unsupported Operator delegation also receives 403.
Machine requests require a nonempty valid path-rule set. Existing ordinary user
tokens retain the existing user authorization semantics. Custom catch-all handlers
that bypass SDK authentication remain responsible for their own checks; declaring
metadata does not secure such handlers.

This is ordinary service-API authority parity. It is not Operator API support,
configured UNS/catalog mediation or automatic permission provisioning. A matching
Controller status capability is necessary before upgrading machine API receivers.

## Async database access

```python
from uns_kit import create_async_database_client

async with create_async_database_client(config["databases"]["source"], max_pending=32) as db:
    result = await db.query_file("queries/read-window.sql", {"fromTs": from_time})
```

`create_async_database_client()` returns a lazy `AsyncDatabaseClient` immediately;
driver construction and all `query`, `execute`, `query_file`, `execute_file` and
`close` calls run on the same dedicated thread. Named parameter compilation,
SQL loading and result types reuse existing code. PostgreSQL, Oracle and SQLite
therefore have the same async facade; SQLite keeps its required thread affinity.
Resolved secrets remain application configuration inputs and are not logged.

This adds no connection pool, transaction context or streaming API. Each client
owns one serialized worker/connection. Driver timeouts remain driver settings;
an asyncio timeout does not interrupt SQL or roll back a possible commit.

Both synchronous and async PostgreSQL/Oracle adapters now discard failed
connections and propagate the original failure without replaying any SQL.
The next explicitly requested operation can reconnect. The SQL string might
invoke a function or have committed before a lost response; method names such
as `query`/`execute` do not establish safe retry semantics. Reconcile uncertain
effects using application-owned durable operation identity.

## Capacity, cancellation and cleanup

`max_pending` counts active plus queued operations. `AsyncCapacityError` means
the new call was rejected before admission. `AsyncClientClosedError` rejects new
work after close starts. These are distinct from an admitted operation whose
caller timed out or disconnected.

Cancelling an awaiter never cancels or repeats accepted IO. Its slot remains
occupied until the underlying operation finishes. `close()` first rejects new
work, waits for accepted IO, then closes resources on the owning thread.
Repeated/concurrent close calls share the same result; cancelling a close waiter
does not cancel cleanup. A failed close remains an error, not a clean receipt.
Worker teardown can be awaited from the API server's separate event loop.

For process lifecycle integration, stop source/HTTP admission first, await
`ProcessWork.drain()`, then close API/DB clients in the application's cleanup.
Do not race close against untracked application work or interpret a stopped
awaiter as a completed business operation.

## Verified and unverified boundaries

Python tests use real temporary SQLite, synthetic PostgreSQL/Oracle drivers,
local HTTP servers and real signed JWTs through an SDK API route. They cover
rotation, revocation before effects, denial, redirect refusal, foreign origin,
capacity, cancellation, thread affinity, no ambiguous SQL replay and subsequent
reconnection. Shared fixtures compare authority responses with the actual TS
authority helper. The built wheel is verified separately.

These checks do not establish live PostgreSQL/Oracle, real Controller grants,
Infisical/TLS configuration, installed launch wiring or production readiness.
Publication, downstream lockfile upgrades and deployment remain separate.
