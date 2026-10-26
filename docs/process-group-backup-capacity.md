# Shared backup budget for retained journal generations

Prepared source3.0.30, not an npm release. Owner-operated npm publication and
actual registry/downstream/helper verification remain separate. Source3.0.29 does
not include this follow-up.

`runtime/process-group-backup-contract.json` owns fixed platform transport budgets:
32 journals,256KiB manifest,512MiB source/snapshot data and1GiB encrypted capsule.
The TypeScript `process-group-backup-contract.js` module exports the immutable
`processGroupBackupLimits` and public `processGroupStateBackupSchema`. Coverage
metadata has no profile names, paths, payloads or credentials; those stay encrypted.
The host additionally verifies its own configured root binding.

The native helper vendors exact SDK contract JSON and standalone snapshot asset,
recording both digests. It passes only this embedded fixed contract to the exporter;
no operation request or application config can raise these budgets. The data-only
exporter still refuses symlinks, wrong ownership/mounts, inconsistent state, duplicate
flat generation identifiers and unsafe files. Nothing executes application code.

Old two-argument exporter callers keep the nine-journal bound. Old complete archives
within that bound remain readable. The new managed helper supplies the fixed SDK
contract and can capture active and retained generations through32 journals. A33rd
fails capture rather than omitting any source or publishing a complete manifest.
The capsule layout and schema1 fields are unchanged; older readers may reject a new
archive containing more than9 journals. Coordinate SDK, host metadata validation and
Tools/helper deployment before relying on larger captures.

This is a count-budget extension, not an increase in allowed data bytes. Every
source journal, UUID, checkpoint and task remains under its original contract.
Capture does not replay tasks, modify mappings or drain writers. Native snapshots
are self-contained SQLite outputs; source ownership leases and WAL databases remain.
Namespace/generation path validation and global uniqueness of flat snapshot names
remain unchanged; ambiguous duplicate names still fail closed.

Local Linux tests cover an actual signed/encrypted ten-journal full fixture backup,
32 application journals, independent SQL read-back, preserved pending work/checkpoints,
33rd-journal failure across namespaces without a published full artifact, and reopen
after container SIGKILL. They do not establish a new production, broker-delivery,
whole-system restore or exactly-once guarantee. Archiver spools remain excluded.
