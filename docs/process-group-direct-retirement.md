# Retiring a paired input journal into one direct process

Prepared for UNS Kit 3.0.34. This optional capability is separate from ordinary
paired upgrades. SDK source, npm publication, installed host support and production
activation are separate gates. Ordinary applications need no migration.

The SDK owns the declaration, resource/identity review and runtime verification.
The authenticated host owns process control, durable operation identity, current
membership, name reservations and audit. The application keeps its own decoding,
profiles, paths and units. Nothing depends on a particular package or customer.

## Package declaration

A package supports file resources, configuration resources, or both. Exactly one
resource must be selected in a direct configuration. Nested selectors work too.

```json
{
  "unsDatahub": {
    "processGroupDirectRuntime": {
      "protocolVersion": 1,
      "namespace": "quality",
      "publisherPath": ["transform"],
      "journalPath": ["durability"],
      "runtimeRolePath": ["role"],
      "directRole": "direct",
      "resources": [
        {
          "kind": "file",
          "manifestPath": ["transform", "groupManifest"],
          "manifests": [{ "id": "inspection", "file": "contracts/inspection.json" }]
        },
        { "kind": "configuration", "groupPath": ["transform", "document"] }
      ]
    },
    "mqttDrain": { "protocolVersion": 1 }
  }
}
```

The application also declares the existing file/configuration process-group
capability for its paired source. The host reads bounded package JSON/resources;
it never imports application JavaScript to review a transition.

## Review and execution

`reviewInstalledProcessGroupDirectTransition(operationId, input, configurations,
stateRoot, excludedHostUnsFields)` reviews a current publisher/receiver and a
never-launched direct target on the same owning controller and package. The
source can use generic policy3/file resources or policy4/configuration resources.
Legacy policies1/2 must first use the existing generic paired upgrade.

The target keeps the combined process name, exact logical profiles/resources,
business settings and channel identities. Remove its input-journal selector and
set its declared direct role. Initial output disk persistence must be disabled.
Pending input is allowed at review; queue depth is information, not a changing
configuration fingerprint.

The host must perform these guarded steps:

1. Reserve both original names and the direct target using the same durable UUID.
2. Park the receiver first, prove its physical clean exit, and let the original
   publisher finish all admitted tasks.
3. Verify journal pending records/bytes are zero, drain and stop the publisher,
   and prove its clean physical exit. Recheck the unchanged journal.
4. Atomically replace the exact reviewed policy with schema5 retirement evidence,
   retaining the original policy bytes and journal at the original path/UUID.
5. Save one launch intent, launch one direct target, and verify current PID/launch
   readiness. An attempted but unconfirmed launch cannot be retried as a new one.

`replaceReviewedProcessGroupPolicy()` is a host mechanism, not operator permission.
It is a compare-and-swap that reconciles a lost reply. Configuration files must
not be edited to bypass a failed review, rename an owner or clear its journal.

## Application startup and readiness

See [the executable API example](examples/process-group-direct-retirement.ts).
Before opening input, call `assertProcessGroupDirectLaunch()` with exact raw and
resolved launch configuration, installed package/instance paths, controller name,
operation and launch IDs. Managed policy verification is shared by the host and
application. A declaration alone is never launch authority.

For file-backed groups, load the exact package-bound `manifestFile` returned by
`describeDirectProcessGroup()`, never an instance-first fallback. For configuration
resources, the raw and resolved selected resource must agree. This prevents an
instance-local file or an unexpected substitution from replacing the reviewed data.

After installing input handlers and calling the existing `enableMigrationDrain()`,
confirm initial output publication and report readiness through
`trackProcessGroupDirectReadiness()`. `UnsMqttProxy.getInputReadiness()` exposes
current broker SUBACK evidence and clears on disconnect/worker exit.
`getConnectionReadiness()` also exposes the output transport connection; include it
in the readiness predicate so an output disconnect makes it unready on the next bounded update. The SDK
receipt is PID/configuration/launch-bound with a bounded lifetime. A host uses
`inspectProcessGroupDirectReadiness()`; PM2 `online` is insufficient.

Output confirmation can run asynchronously: an unavailable destination must not
prevent input from using the configured persistence policy. Readiness stays false
until the application confirms its output. Cancel the tracker when shutdown or
migration release starts. Normal signal/drain cleanup works without a Controller
heartbeat. The receipt is an observation, not a new permission or an archive commit.

## Rollback and closure

For rollback, drain the direct target's output first and prove its clean physical
exit. Restore the exact original policy bytes with CAS, then start the original
receiver and publisher against their retained journal. An unknown stop, output or
launch outcome blocks rollback in the same operation. Do not force-kill an owner
or start a competing publisher to resolve uncertainty.

While recovery is open, only the exact reviewed direct placement/configuration/
artifact/launch is allowed. After separately reviewed recovery closure, the
combined direct identity can use normal supported management. The original
profiles, old receiver and all journal-mode starts remain fenced. The journal and
original policy remain retained; closure does not delete them or replay data.

## Backup and limits

The standalone SDK snapshot includes `retirement-policy.json` with exact bytes,
length and SHA256 alongside the original journal/ownership databases. A snapshot
containing retirement evidence uses internal manifest version2. Transport helpers
must explicitly support that version; an older helper rejects it rather than
silently dropping the policy. Existing captures without retirement keep version1.
Restored policies are offline review evidence, never automatic replay authority.

This is a same-host conversion. It does not transfer a broker's persistent session,
move a live journal between machines or guarantee replay of messages published
during the input pause. Output delivery remains at least once, subject to the
configured queue mode. Disabled output disk persistence does not protect RAM
against a process/host crash. Archive commits and history read-back require their
own verification.
