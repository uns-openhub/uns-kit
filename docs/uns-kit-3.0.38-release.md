# UNS Kit3.0.38 — shared MQTT input-boundary handover

Source preparation for all six TypeScript packages. Npm publication is a separate
owner-operated step; this document does not claim registry availability or any
production deployment.

## Result

A passive MQTT successor no longer needs old packets published before it
subscribed. The shared SDK can prove a shorter unique ordered common suffix
against the current source's processed tail. The source stays active until a
fresh compatible offer and input boundary can be checked. Output-only proxies
have an empty input boundary, while real inputs require acknowledged subscriptions.

The actual frozen boundary is checked against every target input buffer before
any state is imported or input consumed. Delayed input permits read-only
preparation to repeat. Repeated successful replies do not reapply calculation
state; an import/activation with an unknown effect cannot be repeated. Topic and
exact payload identify queue entries; equal measurement values are not deduped.

No application-specific Controller rule, business parser, topic, unit or database
schema change is introduced. Controller protocol2 and legacy handover remain
separate; persistent input/output queues retain their existing ownership rules.

## Compatibility and use

Read [the protocol and repeatable example](mqtt-stateful-handover.md) and the
packaged [migration](../packages/uns-core/MIGRATIONS.md#3038---bounded-late-subscriber-input-boundary-format3).
The new format3 is part of the compatibility digest. Mixed format2/format3 peers
refuse before source freeze. Perform a reviewed initial stop-before-start SDK
upgrade, then use matching updated peers for live moves. Preserve semantic
contracts, accepted-work drain and restart fences; do not relabel contracts to
bypass review. A dependency bump alone does not enable stateful migration.

The existing CLI scaffold, bundle and upgrade guidance explain this boundary.

## Verification

- SDK regression tests cover missing/ambiguous overlap, late subscribers,
  subscription evidence, output-only readiness, repeated FIN, delayed input and
  unknown activation effects. Full root Vitest on3.0.38 passed377 tests with one existing skip. All six
  package builds, shared version checks and packed entrypoint checks passed.
- Actual existing batch-transformer application on two issued Controller2.2.54
  members: move and reverse hot move, deliberately unread HTTP replies, retained
  fences/configurations and exact topic/time/value/unit comparison. The cumulative
  observation was240/240 rows (192 during the successful move/return rehearsal),
  zero missing/extra. Scheduled database extraction was not exercised.
- An unrelated MQTT/RAM accumulator with HTTP API also moved and returned:
  179/179 exact rows, retained sum/state, source exit0 and bound drain receipts,
  unchanged successor PID after same-operation reconciliation.
- Registry-installed consumer acceptance remains the next step after owner npm
  publication. Tests used a local SDK candidate overlay, not a published3.0.38.

These bounded synthetic cases do not prove universal MQTT exactly-once, arbitrary
publisher ordering, replay of lost QoS0 messages, abrupt-crash RAM recovery or
archive/database completeness. Quiet sources without a provable common boundary
stay active; successors never self-promote on timeout.

## Owner npm publication

From the clean `master` checkout after source preparation:

```sh
pnpm login
pnpm run ts:publish
```

That builds, checks and publishes all `@uns-kit/*` packages at3.0.38. Verify registry
availability of every package, then align consumers and repeat the actual
application test using installed registry dependencies before production rollout.
