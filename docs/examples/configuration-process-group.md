# Example: inspection rules in reviewed configuration

Protocol-2 example prepared for SDK3.0.29 (npm publication pending). This is data for a host review, not an operator
permission or a ready-to-run production deployment. See the [contract and limits](../process-group-configuration-runtime.md).

An unrelated `inspection-adapter` can declare these selectors in package JSON:

```json
{
  "unsDatahub": {
    "processGroupConfigurationRuntime": {
      "protocolVersion": 2,
      "namespace": "inspection",
      "publisherPath": ["inspection"],
      "groupPath": ["inspection", "document"],
      "memberPath": ["inspection", "member"],
      "journalPath": ["durability"],
      "runtimeRolePath": ["executionRole"],
      "receiverRole": "ingress"
    }
  }
}
```

The complete instance configuration contains this group at `inspection.document`:

```json
{
  "schemaVersion": 2,
  "id": "cell-4",
  "inputTopic": "example/inspection/input",
  "processName": "inspection-combined",
  "receiverProcessName": "inspection-input",
  "profiles": [
    {
      "id": "vision",
      "processName": "inspection-vision",
      "dataGroup": "inspection",
      "resource": {"schemaVersion": 1, "rules": [{"field": "surface", "maximum": 2}]}
    },
    {
      "id": "weight",
      "processName": "inspection-weight",
      "dataGroup": "inspection",
      "resource": {"schemaVersion": 1, "rules": [{"field": "mass", "minimum": 15}]}
    }
  ]
}
```

Only the inspection adapter validates and executes `rules`. The SDK treats each
resource as bounded plain JSON. A document router may use `routingRules.document`
and entirely different resources with the same contract.

```ts
import { selectedConfigurationGroup, configurationGroupJournalContract }
  from '@uns-kit/core/runtime/configuration-group-contract.js';

const selected = selectedConfigurationGroup(packageRoot, rawConfiguration);
if (!selected) throw Error('Explicit configured group required');
const snapshot = selected.snapshot;
// Application business validation goes here; never run it inside the host.
const journal = {
  directory: '/var/lib/openhub/state/inspection/admission/cell-4-rev-a',
  generationId: 'cell-4-rev-a',
  groupSha256: snapshot.sha256,
  maxRecords: 20000,
  maxBytes: 33554432,
  minFreeBytes: 67108864,
};
// This is a contract string, not authority to initialize or launch anything.
const contract = configurationGroupJournalContract(snapshot, sourceIdentity, journal,
  snapshot.profiles.map(({processName, resourceSha256, dataGroup}) =>
    ({processName, resourceSha256, dataGroup})));
```

Put the journal configuration at `durability`. The source `inspection-vision` selects
`inspection.member: "vision"`; the combined publisher omits the member selector.
The receiver omits it too and selects `executionRole: "ingress"`. Every placement
contains the same complete group document. Its `uns.processName`, raw configuration,
channels and instance ID must match its reviewed placement.

The host stages a schema-4 private policy using the exported schema and common
candidate validator. It then initializes the unbound receiver through its normal
authenticated operation, inspects/binds the resulting UUID, and produces a separate
launch reservation. A hand-written sample policy or a copied proof is insufficient.
Use the same SDK journal/launch checks as protocol 1, with protocol-2 generation and
group fields in the target reservation. Do not remove mount, backup or recovery gates.

For a reviewed change from file resources or an existing configuration revision,
use the [generation transition example and lifecycle](../process-group-generations.md).
Retain the old journal and policy; a new group digest is not permission to reopen
old work with different resources.
