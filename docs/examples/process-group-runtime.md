# Example: two meter profiles and one grouped publisher

This example describes the data contract in `@uns-kit/core` 3.0.27. It uses a
fictional application and contains no operator policy or credentials. These are
integration fragments, not a runnable application: the application still implements
MQTT ingestion, journal writing/checkpoints, parsing, publication and signal/drain
cleanup. See the [protocol](../process-group-runtime.md) and
[ownership rule](../controller-runtime-boundary.md) for supported scope.

## 1. Package declaration

```json
{
  "name": "example-meter-transformer",
  "version": "1.0.0",
  "unsDatahub": {
    "processGroupAdmission": 1,
    "processGroupAdmissionSupervisor": 1,
    "processGroupPersistentAdmission": 1,
    "processGroupRuntime": {
      "protocolVersion": 1,
      "namespace": "example-meter",
      "publisherPath": ["transform"],
      "journalPath": ["durableInput"],
      "runtimeRolePath": ["runtimeRole"],
      "receiverRole": "receiver",
      "defaultDataGroup": "meter-data",
      "manifests": [{ "id": "cell-1", "file": "contracts/cell-1.json" }]
    }
  }
}
```

`transform`, `durableInput` and `runtimeRole` are this application's keys. Another
application can choose different non-overlapping selectors and namespace without
adding a business adapter to the host. Advertise flags only after implementing
their behavior.

## 2. Exact profile resources

Suppose the application stores its mappings in `contracts/room-a.json` and
`contracts/room-b.json`. The mapping format belongs to the application; the SDK
pins exact bytes. Construct the manifest during packaging:

```ts
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { processGroupManifestSchema } from
  '@uns-kit/core/runtime/process-group-authorization.js';

// Supplied by the packaging caller.
const packageRoot = resolve('/work/example-meter-transformer');
const digest = (file: string) => createHash('sha256')
  .update(readFileSync(resolve(packageRoot, file))).digest('hex');
const manifest = processGroupManifestSchema.parse({
  schemaVersion: 1,
  id: 'cell-1',
  inputTopic: 'example/input/measurements',
  processName: 'example-meter-group',
  profiles: ['room-a', 'room-b'].map(id => ({
    id,
    processName: `example-meter-${id}`,
    mappingFile: `${id}.json`,
    mappingSha256: digest(`contracts/${id}.json`),
    dataGroup: 'meter-data',
  })),
});
```

Save the result as `contracts/cell-1.json` before artifact/policy review.
`inputTopic` is a concrete shared topic, not a wildcard. Manifest member mapping
paths resolve relative to the manifest; an individual source configuration's
`mappingFile` resolves relative to the package root. Preserve existing output
topics, units and timestamp semantics in the mappings.

## 3. Source, target and receiver configurations

The first original uses this application-specific configuration fragment:

```json
{
  "uns": { "processName": "example-meter-room-a" },
  "transform": { "mappingFile": "contracts/room-a.json", "dataGroup": "meter-data" },
  "durableInput": {
    "directory": "/var/lib/uns-openhub/state/example-meter/admission/cell-1",
    "journalId": "9bff72ad-6302-4ef1-b923-0aa6ec4c9a83",
    "groupManifest": "contracts/cell-1.json",
    "maxRecords": 20000,
    "maxBytes": 33554432,
    "minFreeBytes": 67108864
  }
}
```

The second original uses `example-meter-room-b` and `contracts/room-b.json`.
The target replaces the publisher selector and process name:

```json
{
  "uns": { "processName": "example-meter-group" },
  "transform": { "groupManifest": "contracts/cell-1.json" }
}
```

The receiver selects the declared receiver role:

```json
{
  "uns": { "processName": "example-meter-group-admission" },
  "transform": { "groupManifest": "contracts/cell-1.json" },
  "runtimeRole": "receiver"
}
```

The latter fragments also need the exact `durableInput` object above and the
application's remaining normal configuration. The UUID illustrates already bound
state: use the actual UUID returned by the authorized initializer, never copy
this value to initialize or replace a retained journal.

Resolve `infra`, `input` and `output` through standard SDK MQTT channel resolution.
Preserve vault references; resolved credentials do not belong in manifests, policy
examples, logs or documentation. Review session/QoS settings separately.

## 4. Shared verification

The host discovers the descriptor without executing application code:

```ts
import { processGroupDescriptor, normalizeProcessGroupConfiguration } from
  '@uns-kit/core/runtime/process-group-contract.js';

const descriptor = processGroupDescriptor(packageRoot);
if (!descriptor) throw new Error('Package has no process-group capability');
const normalized = normalizeProcessGroupConfiguration(rawConfiguration, descriptor);
if (!normalized.matches) throw new Error('Runtime mode does not use the capability');
```

Discovery/schema validation is not authorization. Application startup uses the
same verifier as the host, with its trusted installed placement and launch context:

```ts
import {
  authorizeConfiguredProcessGroupRuntime,
  assertProcessGroupStateMount,
  assertProcessGroupTargetLaunch,
} from '@uns-kit/core/runtime/process-group-authorization.js';

const authorization = authorizeConfiguredProcessGroupRuntime(
  packageRoot, instanceDirectory, rawConfiguration, owningControllerName,
);
assertProcessGroupStateMount(authorization, stateRoot);
assertProcessGroupTargetLaunch(
  authorization, instanceDirectory, reviewedOperationId, reviewedLaunchId,
);
```

Variables above come from trusted host launch context. The application must not
manufacture operation/launch IDs to bypass a reservation. The host authenticates
the operator and reviews exact schema-3 policy hashes, placements and mount identity
before launch. A declaration or successful schema parse grants no permission.

An unbound journal needs a separately authorized receiver initialization
(`initialize: true` in the shared authorization context). Then bind its actual UUID
and review configuration/policy hashes before publishers use it. Ordinary publisher
authorization above cannot initialize a journal. This example provides no shortcut
for generating or approving a production policy.

## 5. Preparation, cutover and recovery

1. Install the exact artifact and prepare stopped placements required by the
   reviewed transition. A legacy source can stay live while its stopped
   journal-aware replacement is prepared.
2. Review policy, persistent mount and backup coverage. A staged uninitialized
   marker declares configuration; it is neither a queue nor permission to start.
3. Initialize/bind through the authorized receiver path. Confirm its actual ready
   heartbeat and subscription before relying on durable admission.
4. Review exact source/target placements and checksums. Run only the approved durable
   operation and retain its operation and launch IDs.
5. Verify drain/exit, publication, per-profile progress and downstream history.
   PM2 online alone does not prove these facts.

| Interruption | Required response |
| --- | --- |
| Configuration/artifact changed | Refresh review; reject the stale plan. |
| Receiver exits | Supervision can restart it; verify the same journal UUID, pending records and readiness. |
| Publisher exits | Resume from the profile checkpoint; do not initialize an empty journal. |
| Response is lost | Reconcile the same operation and launch, without a competing launch. |
| Database/helper fails with unknown outcome | Keep re-execution blocked until authoritative operation/reservation state is reconciled. |
| Rollback requested | Park the target and restore originals through the reviewed operation with retained checkpoints; confirm actual exits/readiness. |
| Host/container replaced | Reattach reviewed persistent state, preserve identity and review backup/restore separately. |

Application signal handling must drain/checkpoint without the host being present.
MQTT delivery before journal admission depends on broker/session/QoS settings;
this example promises neither lossless switching nor exactly-once history.
Protocol 1 does not transfer queues between hosts.

## Existing implementation examples

The SDK [contract tests](../../tests/runtime/process-group-contract.test.ts) show
unrelated packages/groups, changed-policy/resource rejection, registry selection,
staging and supervision. These are implementation references, not evidence that
a new application already has a journal writer or passed production acceptance.
Demonstrate another unrelated package in your acceptance scope; add no
application-specific branch to the host.
