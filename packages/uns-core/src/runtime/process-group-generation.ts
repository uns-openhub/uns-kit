import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { z } from "zod";

import { configurationGroupJournalContract, configurationGroupJournalSchema } from "./configuration-group-contract.js";
import { authorizeProcessGroupRuntime } from "./process-group-authorization.js";
import { processGroupManifestSchema } from "./process-group-authorization.js";
import {
  processGroupJournalContract,
  processGroupJournalSchema,
  processGroupSourceIdentity,
  resolvedProcessGroupChannels,
} from "./process-group-contract.js";
import { groupPlacementSchema, placementKey } from "./process-group-control-contract.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  readProcessGroupResource as read,
  sha256,
} from "./process-group-resources.js";
import { normalizeRuntimeProcessGroup, selectedProcessGroupDescriptor } from "./process-group-runtime-selection.js";
import type { GroupVersionUpgradeReview, PreparedGroupUpgrade, UpgradeConfiguration, UpgradeConfigurations } from "./process-group-upgrade.js";

const pair = z.strictObject({ publisher: groupPlacementSchema, receiver: groupPlacementSchema });
export const processGroupGenerationInputSchema = z
  .strictObject({
    protocolVersion: z.literal(2),
    source: pair,
    target: pair,
    acknowledgeInputPause: z.literal(true),
    acknowledgeResourceReview: z.literal(true),
    acknowledgePossibleReplay: z.literal(true),
  })
  .superRefine((v, c) => {
    const all = [v.source.publisher, v.source.receiver, v.target.publisher, v.target.receiver];
    if (
      new Set(all.map(placementKey)).size !== 4 ||
      all.some((p) => p.controller !== all[0]!.controller || p.rttNode !== all[0]!.rttNode) ||
      v.source.publisher.version !== v.source.receiver.version ||
      v.target.publisher.version !== v.target.receiver.version
    )
      c.addIssue({ code: "custom", message: "Four distinct paired placements on one package/controller required" });
  });
export type ProcessGroupGenerationInput = z.infer<typeof processGroupGenerationInputSchema>;
export type GenerationJournalReference = {
  directory: string;
  journalId: string;
  journalContractSha256: string;
  generationId: string;
  groupSha256?: string;
  contractVersion: 1 | 2;
  maxRecords: number;
  maxBytes: number;
  minFreeBytes: number;
  profiles: Array<{ processName: string; mappingSha256: string; dataGroup: string }>;
};
export type GroupGenerationReview = Omit<GroupVersionUpgradeReview, "protocolVersion"> & {
  protocolVersion: 2;
  sourcePolicyFile: string;
  sourceJournal: GenerationJournalReference;
  targetJournal: GenerationJournalReference;
  delivery: { inputPausePossible: true; retainedBrokerSessionMayReplay: true; exactlyOnceOutput: false };
  resourceChanges: Array<{ processName: string; beforeSha256: string; afterSha256: string; dataGroup: string }>;
};

/** Bounded read-only storage evidence; no initializer, rewriting or replay. */
export function inspectGenerationJournal(reference: GenerationJournalReference) {
  const file = path.join(reference.directory, "admission.sqlite"),
    stat = fs.lstatSync(file);
  if (
    fs.realpathSync(file) !== file ||
    !stat.isFile() ||
    stat.nlink !== 1 ||
    stat.uid !== process.getuid?.() ||
    (stat.mode & 0o077) !== 0 ||
    stat.size > 512 * 1024 * 1024
  )
    throw Error("Private retained generation journal required");
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000; BEGIN");
    const meta = Object.fromEntries(
      db
        .prepare("SELECT key,value FROM metadata WHERE key IN ('id','contract')")
        .all()
        .map((r) => [r["key"], r["value"]]),
    );
    if (
      meta["id"] !== reference.journalId ||
      typeof meta["contract"] !== "string" ||
      meta["contract"].length > 1048576 ||
      sha256(meta["contract"]) !== reference.journalContractSha256
    )
      throw Error("Reviewed journal generation changed");
    const rows = db.prepare("SELECT name,contract,completed,last_seq FROM profiles ORDER BY name").all();
    if (rows.length !== reference.profiles.length) throw Error("Journal profile set differs");
    for (const profile of reference.profiles) {
      const row = rows.find((r) => r["name"] === profile.processName),
        binding = row ? JSON.parse(String(row["contract"])) : null;
      if (
        !binding ||
        binding.processName !== profile.processName ||
        binding.dataGroup !== profile.dataGroup ||
        binding[reference.contractVersion === 1 ? "mappingSha256" : "resourceSha256"] !== profile.mappingSha256
      )
        throw Error("Journal resource binding differs");
    }
    const usage = db.prepare("SELECT records,bytes,admitted FROM usage WHERE id=1").get();
    const actual = db.prepare("SELECT count(*) records,coalesce(sum(bytes),0) bytes FROM tasks").get();
    const numbers = [usage?.["records"], usage?.["bytes"], usage?.["admitted"], ...rows.flatMap((r) => [r["completed"], r["last_seq"]])];
    const completed = rows.reduce((n, r) => n + Number(r["completed"]), 0);
    if (
      !usage ||
      !actual ||
      numbers.some((n) => typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) ||
      usage["records"] !== actual["records"] ||
      usage["bytes"] !== actual["bytes"] ||
      !Number.isSafeInteger(completed) ||
      Number(usage["admitted"]) !== completed + Number(usage["records"]) ||
      rows.some((r) => (r["completed"] === 0) !== (r["last_seq"] === 0)) ||
      Number(usage["records"]) > reference.maxRecords ||
      Number(usage["bytes"]) > reference.maxBytes
    )
      throw Error("Journal usage is unconfirmed");
    return {
      records: Number(usage["records"]),
      bytes: Number(usage["bytes"]),
      admitted: Number(usage["admitted"]),
      completed,
      lastSequence: rows.reduce((n, r) => Math.max(n, Number(r["last_seq"])), 0),
    };
  } finally {
    try {
      db.exec("ROLLBACK");
    } finally {
      db.close();
    }
  }
}

/** Recheck immediately before the first launch; initialization is never silently reused. */
export function inspectPristineGenerationJournal(reference: GenerationJournalReference) {
  const status = inspectGenerationJournal(reference);
  if (status.records || status.bytes || status.admitted || status.completed || status.lastSequence)
    throw Error("Target generation is not pristine; inspect the existing initialization outcome");
  return status;
}

/** Data-only normalized revision and immutable journal contract shared by reviewed transitions. */
export function describeInstalledProcessGroupGeneration(c: UpgradeConfiguration) {
  const d = selectedProcessGroupDescriptor(c.packageRoot, c.raw);
  if (!d) throw Error("Declared process group required");
  const normalized = normalizeRuntimeProcessGroup(c.raw, d);
  if (!normalized.matches) throw Error("Runtime selector differs");
  const channels = resolvedProcessGroupChannels(c.resolved),
    sourceIdentity = processGroupSourceIdentity(channels[1]!);
  if (d.protocolVersion === 2) {
    const snapshot = normalized.configurationSnapshot!;
    const journal = configurationGroupJournalSchema.parse(normalized.journal);
    const contract = configurationGroupJournalContract(
      snapshot,
      sourceIdentity,
      journal,
      snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, resourceSha256, dataGroup })),
    );
    return {
      d,
      normalized,
      channels,
      id: snapshot.group.id,
      processName: snapshot.group.processName,
      receiverProcessName: snapshot.group.receiverProcessName,
      inputTopic: snapshot.group.inputTopic,
      reference: {
        directory: journal.directory,
        journalId: journal.journalId!,
        generationId: journal.generationId,
        groupSha256: snapshot.sha256,
        contractVersion: 2 as const,
        journalContractSha256: sha256(contract),
        maxRecords: journal.maxRecords,
        maxBytes: journal.maxBytes,
        minFreeBytes: journal.minFreeBytes,
        profiles: snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, mappingSha256: resourceSha256, dataGroup })),
      },
    };
  }
  const journal = processGroupJournalSchema.parse(normalized.journal);
  if (!d.manifests.some((m) => m.file === journal.groupManifest)) throw Error("Registered file group required");
  const group = processGroupManifestSchema.parse(JSON.parse(read(path.resolve(c.packageRoot, journal.groupManifest)).toString()));
  const contract = processGroupJournalContract(sourceIdentity, journal, group.profiles);
  return {
    d,
    normalized,
    channels,
    id: group.id,
    processName: group.processName,
    receiverProcessName: group.processName + "-admission",
    inputTopic: group.inputTopic,
    reference: {
      directory: journal.directory,
      journalId: journal.journalId!,
      generationId: group.id,
      contractVersion: 1 as const,
      journalContractSha256: sha256(contract),
      maxRecords: journal.maxRecords,
      maxBytes: journal.maxBytes,
      minFreeBytes: journal.minFreeBytes,
      profiles: group.profiles.map(({ processName, mappingSha256, dataGroup }) => ({ processName, mappingSha256, dataGroup })),
    },
  };
}

/** Operator reviews opaque before/after digests; business equivalence is application-owned. */
export function reviewInstalledProcessGroupGeneration(
  configs: UpgradeConfigurations,
  controllerName: string,
  stateRoot: string,
  excludedUnsFields: readonly string[] = [],
): PreparedGroupUpgrade {
  const context = (c: UpgradeConfiguration) => ({
    packageRoot: c.packageRoot,
    instanceDirectory: c.instanceDirectory,
    rawConfiguration: c.raw,
    channels: resolvedProcessGroupChannels(c.resolved),
    controllerName,
    stateRoot,
  });
  const a = describeInstalledProcessGroupGeneration(configs.source.publisher),
    b = describeInstalledProcessGroupGeneration(configs.target.publisher);
  if (
    b.d.protocolVersion !== 2 ||
    a.d.namespace !== b.d.namespace ||
    a.id !== b.id ||
    a.processName !== b.processName ||
    a.receiverProcessName !== b.receiverProcessName ||
    a.inputTopic !== b.inputTopic ||
    a.reference.directory === b.reference.directory ||
    !a.reference.journalId ||
    !b.reference.journalId ||
    a.reference.journalId === b.reference.journalId ||
    a.reference.maxRecords !== b.reference.maxRecords ||
    a.reference.maxBytes !== b.reference.maxBytes ||
    a.reference.minFreeBytes !== b.reference.minFreeBytes
  )
    throw Error("Distinct bounded generations with the same process/input identity required");
  const policyFiles = {} as Record<"source" | "target", string>,
    policies = {} as Record<"source" | "target", Buffer>;
  const members: GroupVersionUpgradeReview["members"] = [];
  const portable = (raw: Record<string, unknown>, d: typeof a.d) => {
    const copy = structuredClone(raw),
      uns = { ...(copy["uns"] as Record<string, unknown>) };
    for (const key of excludedUnsFields) delete uns[key];
    copy["uns"] = uns;
    for (const keys of [d.publisherPath, d.journalPath, d.runtimeRolePath]) {
      let node = copy;
      let present = true;
      for (const key of keys.slice(0, -1)) {
        const child = node[key];
        if (child === undefined) {
          present = false;
          break;
        }
        if (!child || typeof child !== "object" || Array.isArray(child)) throw Error("Invalid resource selector parent");
        node = child as Record<string, unknown>;
      }
      if (present) delete node[keys.at(-1)!];
    }
    return hash(copy);
  };
  for (const side of ["source", "target"] as const)
    for (const role of ["publisher", "receiver"] as const) {
      const c = configs[side][role],
        g = describeInstalledProcessGroupGeneration(c),
        expected = side === "source" ? a : b,
        proof = authorizeProcessGroupRuntime(context(c));
      if (
        proof.role !== (role === "publisher" ? "target" : "receiver") ||
        proof.directory !== expected.reference.directory ||
        proof.journalId !== expected.reference.journalId ||
        hash(g.reference) !== hash(expected.reference) ||
        g.normalized.uns["processName"] !== (role === "publisher" ? g.processName : g.receiverProcessName) ||
        (role === "receiver"
          ? g.normalized.runtimeRole !== "admission"
          : g.normalized.runtimeRole !== undefined && g.normalized.runtimeRole !== "publisher")
      )
        throw Error("Exact reviewed generation pair required");
      const pkg = JSON.parse(read(path.join(c.packageRoot, "package.json")).toString());
      if (
        pkg.name !== JSON.parse(read(path.join(configs.source.publisher.packageRoot, "package.json")).toString()).name ||
        portable(c.raw, g.d) !== portable(configs.source[role].raw, a.d) ||
        groupChannelsHash(g.channels) !== groupChannelsHash(a.channels)
      )
        throw Error("Configuration or broker identity changed outside the reviewed resources");
      if (policyFiles[side] && policyFiles[side] !== proof.file) throw Error("Pair policies differ");
      policyFiles[side] = proof.file;
      policies[side] = read(proof.file, true);
      members.push({
        side,
        role,
        processName: String(g.normalized.uns["processName"]),
        configurationSha256: hash(c.raw),
        artifactSha256: groupArtifactHash(c.packageRoot),
      });
    }
  if (policyFiles.source === policyFiles.target) throw Error("Each generation requires its own retained policy");
  const profiles = a.reference.profiles;
  if (
    profiles.length !== b.reference.profiles.length ||
    profiles.some((p) => !b.reference.profiles.some((q) => q.processName === p.processName && q.dataGroup === p.dataGroup))
  )
    throw Error("Logical profile identities or data groups changed");
  inspectGenerationJournal(a.reference);
  inspectPristineGenerationJournal(b.reference);
  if (sha256(read(policyFiles.source, true)) !== sha256(policies.source) || sha256(read(policyFiles.target, true)) !== sha256(policies.target))
    throw Error("Reviewed policies changed");
  const review: GroupGenerationReview = {
    protocolVersion: 2,
    namespace: b.d.namespace,
    groupId: b.id,
    policyFile: policyFiles.target,
    sourcePolicyFile: policyFiles.source,
    sourcePolicySha256: sha256(policies.source),
    targetPolicySha256: sha256(policies.target),
    journalId: b.reference.journalId,
    directory: b.reference.directory,
    journalContractSha256: b.reference.journalContractSha256,
    profiles: b.reference.profiles,
    members,
    sourceJournal: a.reference,
    targetJournal: b.reference,
    delivery: { inputPausePossible: true, retainedBrokerSessionMayReplay: true, exactlyOnceOutput: false },
    resourceChanges: profiles.map((p) => ({
      processName: p.processName,
      beforeSha256: p.mappingSha256,
      afterSha256: b.reference.profiles.find((q) => q.processName === p.processName)!.mappingSha256,
      dataGroup: p.dataGroup,
    })),
  };
  return { review, sourcePolicy: policies.source, targetPolicy: policies.target };
}
