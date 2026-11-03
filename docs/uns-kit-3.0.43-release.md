# UNS Kit 3.0.43 — shared managed operator route registration

This source release adds the shared launch/route wire contract and a reusable
managed-route publisher. An explicitly declared `UnsApiProxy` operator endpoint
registers its actual listening API origin with its owning Controller and renews
the short-lived receipt automatically. Other applications use the same mechanism;
no application-specific publisher or Controller package-name branch is needed.

The host must already bind a reviewed active service identity to the exact
installed instance and supply a private purpose-specific route-token file plus
current launch metadata. The SDK rereads rotated credentials, preserves exact
version tags and verifies the returned owner, process, instance, launch and API
origin. It grants no data scopes or operator-command authority. Controller-side
unique ownership, workload tools and reviewed command execution remain separate.

Registration rejects redirects, invalid origins, untrusted credential file types,
oversized replies and mismatched receipts. Requests have explicit deadlines;
unsettled transports stay coalesced rather than accumulating retries. Stop
cancels renewal without waiting indefinitely for Controller availability. A lost
receipt can be retried for the same launch/origin. Ordinary APIs and legacy or
standalone launches without the route-token environment retain existing behavior.

Applications declaring operators adopt the coordinated core/API version and use
the normal reviewed host launch. A Controller-only hot update cannot add missing
launch environment to a running application. See the
[operator contract](operator-api.md) and
[version-bounded migration](../packages/uns-core/MIGRATIONS.md). Generated project
guidance includes the same migration boundary.

All six TypeScript packages share 3.0.43. The private root remains 0.0.0, while
Python, MQTT data paths, units, journals and queue formats are unchanged. Source
preparation/push, owner-operated npm publication and downstream installation are
separate facts; this document does not claim registry publication or production
deployment.

Release preparation passed the six-package version/pin check, sequential builds
and every package's packed entry checks, 459 root SDK tests and 415 workflow tests.
The optional example Azure connectivity test was skipped with no credential; it
is not counted as release integration evidence. Focused tests cover automatic
registration for two unrelated APIs, ordinary endpoint opt-out, exact versions,
rotated credentials, lost responses, bounded recovery/Stop and invalid authority.
The coordinated host's separate local checks include real MQTT/PostgreSQL route
transactions and actual SDK registration through its credential manager, endpoint
and MCP boundary. These are local tests, not an installed production proof.
