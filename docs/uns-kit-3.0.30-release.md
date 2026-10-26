# UNS Kit 3.0.30 source preparation

Publication of the six npm packages is an owner-operated step. Source, CI,
local archives, actual registry verification and downstream releases are distinct.
Do not publish the older prepared3.0.29 clone if the intent is this combined release.

## Changes

- Optional TypeScript MQTT **output** `publishQueue` with `buffered` RAM/spill and
  `durable` SQLite-WAL/FULL-before-admission modes. Final serialized output is
  retained and broker acknowledgements govern completion. The ordinary publisher
  remains the default; no incoming QoS/ACK, application admission journal, Archiver
  spool, SQL writer or Python behavior is migrated automatically.
- Bounded records/bytes/memory/page budget and free-space reserve; FIFO across
  RAM/disk, exclusive local ownership, retained identity, integrity checks, restart,
  retry and stop/drain. Unsupported QoS0/options and legacy skip-hash handover fail
  explicitly. Delivery uncertainty can duplicate output; no exactly-once claim.
- The previously local shared32-journal backup budget/schema and exact standalone
  snapshot asset, retaining old nine-journal callers and all original byte bounds.
  Coordinated host/helper rollout remains required for larger captures. This does
  not automatically enroll the new MQTT outbox in the old process-group format.

Guides: [MQTT output](mqtt-output-persistence.md),
[two independent examples](examples/mqtt-output-persistence.ts),
[shared backup capacity](process-group-backup-capacity.md), and packaged MIGRATIONS.
The CLI-generated guidance covers the new optional capability boundary.

## Local verification

Run frozen install, version consistency, all six TypeScript builds, root/core and
workflow tests, package-entry checks, then actual archive installation in a separate
consumer. `node scripts/accept-mqtt-output.mjs` needs a local mosquitto binary and
uses only private loopback ports and synthetic records; it stops its own broker and
children and retains its temporary evidence. Set `UNS_MQTT_TEST_BROKER` if needed.
A separate network-disabled Linux rehearsal verifies UID1000/SIGKILL/reopen and
contrasts128 retained durable records with the expected loss of128 RAM-only records.
The MQTT rehearsal suppresses one actual PUBACK, observes replay and drains fully.
These are local functional proofs, not an end-to-end zero-loss or throughput SLA.
Recorded local acceptance:269 root/core tests and415 workflow tests PASS; one
external Azure test skipped. All six builds and packed entry checks PASS; the actual
core archive imports and typechecks in a separate frozen consumer. See the sanitized
[evidence receipt](evidence/mqtt-output-20261026.json).

## Owner publication

From the reviewed clean3.0.30 master checkout:

```sh
pnpm login
pnpm run ts:publish
```

Read back all six registry versions before changing real downstream lockfiles.
Use an actual frozen install and archive/helper read-back before preparing Controller,
Tools or microservice releases. No production action is authorized by this note.
