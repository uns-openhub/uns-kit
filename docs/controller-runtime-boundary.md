# Controller, UNS Kit and application responsibilities

This is the shared development rule for applications and hosts using UNS Kit.
The concrete process-group contract is available from `@uns-kit/core` 3.0.27;
other capabilities retain their own version and implementation boundaries.

## Ownership

| Component | Owns |
| --- | --- |
| UNS Kit | Reusable protocol types, schemas, normalization, identity/hash rules, shared validation and lifecycle helpers. |
| Controller or another host | Authentication, permissions, installation and placement, durable reviewed operations, launch reservations, supervision integration, reconciliation and audit. |
| Application | Source decoding, business profiles, mappings, topics, units, publication, journal writing/checkpoints and cleanup of its own resources. |
| Runtime/tools | Deployment, persistent mounts and backup/restore transport using the shared state contract. |

A microservice implements a shared capability. Its package name or customer group
must not select a special lifecycle or permission branch in the Controller.
New profiles using an already supported capability are declarations and reviewed
configuration. A new protocol can require generic host support and a documented
minimum host version.

The host reads bounded package metadata and resources. It must not import an
application entry point, execute its mapping parser or duplicate SDK validators
to discover or authorize capabilities. Platform service-health adapters may
interpret documented service signals; they do not confer launch authority.

For example, a meter transformer and an unrelated machine-status processor can
use process-group protocol 1 with different namespaces, selectors and manifests.
The host needs neither an application-name condition nor a list of business groups.

## Capability, permission and readiness are separate

| Evidence | What it establishes |
| --- | --- |
| Package declaration | The package claims to implement a versioned protocol. |
| Reviewed private policy | The exact artifact, configurations, channels, placements and state have been authorized. |
| Durable operation and launch identity | A particular managed action has been reserved and recorded. |
| PM2/process online | The operating-system process is running. |
| Input subscription/readiness evidence | The applicable input handler and subscriptions are ready. |
| Write acknowledgement | A particular downstream write was acknowledged. |
| Drain acknowledgement and physical exit | Separate evidence about accepted work and process termination. |

Do not infer a later row from an earlier one. An installed or stopped target has
not proved runtime readiness. Do not copy the source's runtime-owned
`uns.handoverProtocol` declaration to make a new target appear hot-capable.
The host evaluates supported capability and reports the selected mode; a reviewed
cold transition can have an input gap.

## Standalone lifecycle

SIGINT/SIGTERM handling, idempotent drain, checkpointing and resource cleanup must
work without a Controller heartbeat or management command. Use the SDK lifecycle
owner where applicable; application work and resources still need application
cleanup. Do not install duplicate competing signal/stop owners.

A Controller can coordinate handover and observe acknowledgements. Its absence
must not disable local stop handling or, by itself, terminate unrelated services.
A capability needing live platform authorization declares that dependency and
rejects unsafe work when authorization is unavailable.

## Persistent state and unknown outcomes

A process move does not imply transfer of an external journal, broker session or
archiver spool. Review storage ownership, mount identity and backup coverage
separately. A journal UUID and checkpoints identify retained work; a version bump
must not silently replace them with an empty journal.

If an execution response is lost, inspect the same durable operation, reservation
and launch identity. Unknown outcome requires reconciliation, not a second
operation, a cleared guard or a competing publisher. Rollback also uses the
reviewed operation and retained checkpoints.

For MQTT, review `clientId`, QoS, clean-session/session-expiry settings and broker
retention together. A local journal protects only input already admitted to it.
It cannot establish delivery for messages that never reached the receiver and
does not provide end-to-end exactly-once delivery.

## Extension checklist

1. Look for an existing SDK contract and host extension point.
2. Keep business parsing, profiles, paths and units in the application.
3. Put missing reusable types/helpers and validation in UNS Kit first. Consumers
   share that implementation instead of maintaining copies.
4. Demonstrate a second unrelated package with different selectors/resources;
   verify services without the capability remain unaffected.
5. Cover interrupted execution, lost response, restart/reconciliation, rollback
   and standalone shutdown in the relevant acceptance scope.
6. Document version bounds and preserve paths, units and retained state. Keep
   historical readers isolated; do not widen old policies for new artifacts.
7. Treat npm publication, downstream dependency updates, host release and production
   deployment as separate steps with separate evidence.

## Further reading

- [Process-group protocol, API and limits](process-group-runtime.md)
- [Worked independent application example](examples/process-group-runtime.md)
- [Version-specific core migration notes](../packages/uns-core/MIGRATIONS.md)
- [Lifecycle and topic reference](uns-topics.md)
