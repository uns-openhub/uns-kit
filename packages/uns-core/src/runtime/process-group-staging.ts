import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { configurationGroupJournalSchema } from "./configuration-group-contract.js";
import { type RuntimeGroupDescriptor, normalizeRuntimeProcessGroup } from "./process-group-runtime-selection.js";

export const stagedJournalFile = "process-group-staged-journal.json";
/** An explicit, config-bound declaration, not permission to initialize or run. */
export function refreshProcessGroupStagedJournal(instanceDir: string, stopped: boolean, descriptor: RuntimeGroupDescriptor | undefined): void {
  const marker = path.join(instanceDir, stagedJournalFile);
  const configFile = path.join(instanceDir, "config.json");
  if (!fs.existsSync(configFile)) return;
  const stat = fs.lstatSync(configFile);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1024 * 1024) throw new Error("Staged configuration must be a bounded regular file.");
  const raw = fs.readFileSync(configFile);
  const config = JSON.parse(raw.toString("utf8")) as Record<string, unknown>;
  const selected = descriptor ? normalizeRuntimeProcessGroup(config, descriptor) : undefined;
  const journal = selected?.journal as { directory?: unknown; journalId?: unknown } | undefined;
  if (!stopped || !journal || journal.journalId !== undefined) {
    fs.rmSync(marker, { force: true });
    return;
  }
  if (selected?.configurationSnapshot && configurationGroupJournalSchema.parse(journal).groupSha256 !== selected.configurationSnapshot.sha256)
    throw Error("Staged journal differs from its configured resource snapshot");
  if (typeof journal.directory !== "string" || !path.isAbsolute(journal.directory) || path.normalize(journal.directory) !== journal.directory)
    throw new Error("Staged journal requires a normalized absolute directory.");
  // Existing/unknown data must never be declared uninitialized, including links.
  for (let dir = journal.directory; dir !== path.dirname(dir); dir = path.dirname(dir)) {
    try {
      const stat = fs.lstatSync(dir);
      if (stat.isSymbolicLink() || dir === journal.directory) throw new Error("Existing journal cannot be marked uninitialized.");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  if (fs.existsSync(path.join(instanceDir, "handover-source-guard.json.launch")))
    throw new Error("A previously launched instance requires journal review.");
  const temporary = marker + "." + randomUUID() + ".tmp";
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(
      fd,
      JSON.stringify({
        schemaVersion: 1,
        kind: "uns-process-group-staged-journal",
        configurationSha256: createHash("sha256").update(raw).digest("hex"),
        directory: journal.directory,
      }) + "\n",
    );
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(temporary, marker);
  const parent = fs.openSync(instanceDir, "r");
  try {
    fs.fsyncSync(parent);
  } finally {
    fs.closeSync(parent);
  }
}
