import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { z } from "zod";

import {
  authorizeProcessGroupRuntime,
  processGroupAuthorizationFile,
  processGroupAuthorizationSchema,
  processGroupManifestSchema,
  validateProcessGroupPolicyCandidate,
} from "./process-group-authorization.js";
import {
  normalizeProcessGroupConfiguration,
  processGroupDescriptor,
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

const pair = z.strictObject({ publisher: groupPlacementSchema, receiver: groupPlacementSchema });
export const processGroupUpgradeInputSchema = z.strictObject({ protocolVersion: z.literal(1), source: pair, target: pair }).superRefine((v, c) => {
  const all = [v.source.publisher, v.source.receiver, v.target.publisher, v.target.receiver];
  if (
    new Set(all.map(placementKey)).size !== 4 ||
    all.some((p) => p.controller !== all[0]!.controller || p.rttNode !== all[0]!.rttNode) ||
    v.source.publisher.version !== v.source.receiver.version ||
    v.target.publisher.version !== v.target.receiver.version ||
    v.source.publisher.version === v.target.publisher.version
  )
    c.addIssue({ code: "custom", message: "Four distinct placements, one package/controller and two paired versions required" });
});
export type ProcessGroupUpgradeInput = z.infer<typeof processGroupUpgradeInputSchema>;
export type UpgradeRole = "publisher" | "receiver";
export type UpgradeConfiguration = {
  packageRoot: string;
  instanceDirectory: string;
  raw: Record<string, unknown>;
  resolved: Record<string, unknown>;
};
export type UpgradeConfigurations = Record<"source" | "target", Record<UpgradeRole, UpgradeConfiguration>>;
export type GroupUpgradeReview = {
  protocolVersion: 1;
  namespace: string;
  groupId: string;
  policyFile: string;
  sourcePolicySha256: string;
  targetPolicySha256: string;
  journalId: string;
  directory: string;
  journalContractSha256: string;
  profiles: Array<{ processName: string; mappingSha256: string; dataGroup: string }>;
  members: Array<{ side: "source" | "target"; role: UpgradeRole; processName: string; configurationSha256: string; artifactSha256: string }>;
};
export type PreparedGroupUpgrade = { review: GroupUpgradeReview; sourcePolicy: Buffer; targetPolicy: Buffer };

/** Data-only same-host version review. No application JS, journal writes or launch authority. */
export function reviewInstalledProcessGroupUpgrade(
  configs: UpgradeConfigurations,
  controllerName: string,
  stateRoot: string,
  excludedUnsFields: readonly string[] = [],
): PreparedGroupUpgrade {
  const first = configs.source.publisher;
  const context = (c: UpgradeConfiguration) => ({
    packageRoot: c.packageRoot,
    instanceDirectory: c.instanceDirectory,
    rawConfiguration: c.raw,
    channels: resolvedProcessGroupChannels(c.resolved),
    controllerName,
    stateRoot,
  });
  const old = authorizeProcessGroupRuntime(context(first));
  const receiver = authorizeProcessGroupRuntime(context(configs.source.receiver));
  if (
    old.role !== "target" ||
    receiver.role !== "receiver" ||
    old.file !== receiver.file ||
    old.directory !== receiver.directory ||
    !old.journalId ||
    old.journalId !== receiver.journalId
  )
    throw Error("Exact authorized source publisher/receiver pair required");
  const sourcePolicy = read(old.file, true),
    previous = JSON.parse(sourcePolicy.toString());
  const target = configs.target.publisher,
    descriptor = processGroupDescriptor(target.packageRoot);
  if (!descriptor || descriptor.legacy) throw Error("Target requires the shared process-group protocol declaration");
  const targetConfig = normalizeProcessGroupConfiguration(target.raw, descriptor);
  const journal = processGroupJournalSchema.parse(targetConfig.journal);
  const resource = descriptor.manifests.find((m) => m.file === journal.groupManifest);
  if (!resource) throw Error("Unregistered target manifest");
  const manifestBytes = read(path.join(target.packageRoot, resource.file)),
    manifest = processGroupManifestSchema.parse(JSON.parse(manifestBytes.toString()));
  if (manifest.id !== resource.id || journal.directory !== old.directory || journal.journalId !== old.journalId)
    throw Error("Journal or manifest identity changed");
  const members: GroupUpgradeReview["members"] = [];
  const portable = (raw: Record<string, unknown>) => {
    const copy = structuredClone(raw),
      uns = { ...(copy["uns"] as Record<string, unknown>) };
    for (const k of excludedUnsFields) delete uns[k];
    copy["uns"] = uns;
    return hash(copy);
  };
  for (const role of ["publisher", "receiver"] as const) {
    const a = configs.source[role],
      b = configs.target[role];
    if (
      portable(a.raw) !== portable(b.raw) ||
      groupChannelsHash(resolvedProcessGroupChannels(a.resolved)) !== groupChannelsHash(resolvedProcessGroupChannels(b.resolved))
    )
      throw Error("Configuration, broker identity, paths or units changed");
    for (const side of ["source", "target"] as const) {
      const c = configs[side][role],
        d = processGroupDescriptor(c.packageRoot);
      if (!d) throw Error("Missing shared runtime contract");
      const normalized = normalizeProcessGroupConfiguration(c.raw, d),
        j = processGroupJournalSchema.parse(normalized.journal);
      if (
        !normalized.matches ||
        (role === "receiver"
          ? normalized.runtimeRole !== "admission"
          : normalized.runtimeRole !== undefined && normalized.runtimeRole !== "publisher") ||
        j.directory !== old.directory ||
        j.journalId !== old.journalId ||
        sha256(read(path.join(c.packageRoot, j.groupManifest))) !== sha256(manifestBytes)
      )
        throw Error("Runtime role, manifest or journal differs");
      members.push({
        side,
        role,
        processName: String(normalized.uns["processName"]),
        configurationSha256: hash(c.raw),
        artifactSha256: groupArtifactHash(c.packageRoot),
      });
    }
  }
  if (
    members.find((m) => m.role === "publisher")?.processName !== manifest.processName ||
    members.find((m) => m.role === "receiver")?.processName !== manifest.processName + "-admission"
  )
    throw Error("Manifest process identities differ");
  const pkg = JSON.parse(read(path.join(target.packageRoot, "package.json")).toString());
  const historical = previous.placements.filter((p: { role: string }) => p.role === "source");
  const policy = processGroupAuthorizationSchema.parse({
    schemaVersion: 3,
    kind: "uns-process-group-runtime-authorization",
    protocolVersion: 1,
    packageName: pkg.name,
    namespace: descriptor.namespace,
    groupId: manifest.id,
    descriptorSha256: hash(pkg.unsDatahub.processGroupRuntime),
    controllerName,
    packageRoot: target.packageRoot,
    artifactSha256: groupArtifactHash(target.packageRoot),
    groupManifestSha256: sha256(manifestBytes),
    channelsSha256: groupChannelsHash(resolvedProcessGroupChannels(target.resolved)),
    stateRoot,
    directory: old.directory,
    journalId: old.journalId,
    placements: [
      ...historical,
      ...(["publisher", "receiver"] as const).map((role) => ({
        instanceId: path.basename(configs.target[role].instanceDirectory),
        processName: members.find((m) => m.side === "target" && m.role === role)!.processName,
        role: role === "publisher" ? "target" : "receiver",
        configurationSha256: hash(configs.target[role].raw),
      })),
    ],
  });
  const targetPolicy = Buffer.from(JSON.stringify(policy));
  for (const role of ["publisher", "receiver"] as const) validateProcessGroupPolicyCandidate(context(configs.target[role]), targetPolicy);
  const expected = processGroupJournalContract(
    processGroupSourceIdentity(resolvedProcessGroupChannels(target.resolved)[1]!),
    journal,
    manifest.profiles,
  );
  const file = path.join(old.directory, "admission.sqlite");
  if (fs.realpathSync(file) !== file || !fs.lstatSync(file).isFile()) throw Error("Unsafe journal resource");
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000;");
    const metadata = Object.fromEntries(
      db
        .prepare("SELECT key,value FROM metadata WHERE key IN ('id','contract')")
        .all()
        .map((r) => [r["key"], r["value"]]),
    );
    if (metadata["id"] !== old.journalId || metadata["contract"] !== expected)
      throw Error("Installed target cannot reopen the retained journal contract");
  } finally {
    db.close();
  }
  if (sha256(read(old.file, true)) !== sha256(sourcePolicy)) throw Error("Source policy changed during review");
  return {
    review: {
      protocolVersion: 1,
      namespace: descriptor.namespace,
      groupId: manifest.id,
      policyFile: processGroupAuthorizationFile(context(target)),
      sourcePolicySha256: sha256(sourcePolicy),
      targetPolicySha256: sha256(targetPolicy),
      journalId: old.journalId,
      directory: old.directory,
      journalContractSha256: sha256(expected),
      profiles: manifest.profiles.map(({ processName, mappingSha256, dataGroup }) => ({ processName, mappingSha256, dataGroup })),
      members,
    },
    sourcePolicy,
    targetPolicy,
  };
}

/** Atomic compare-and-swap. The authenticated host must first prove both old owners exited. */
export function replaceReviewedProcessGroupPolicy(file: string, expected: string, bytes: Buffer): void {
  const actual = sha256(read(file, true)),
    next = sha256(bytes);
  if (actual === next) return; // Reconcile an already completed write after a lost reply.
  if (actual !== expected) throw Error("Runtime policy changed; review required");
  const temporary = file + ".upgrade-tmp";
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(fd, bytes);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  try {
    if (sha256(read(file, true)) !== expected) throw Error("Runtime policy changed before replacement");
    fs.renameSync(temporary, file);
    const directory = fs.openSync(path.dirname(file), "r");
    try {
      fs.fsyncSync(directory);
    } finally {
      fs.closeSync(directory);
    }
  } finally {
    if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
  }
}

export const processGroupUpgradeRequestSchema = z
  .strictObject({
    protocolVersion: z.literal(1),
    controllerName: groupPlacementSchema.shape.controller,
    action: z.enum(["preview", "prepare", "get", "execute", "rollback"]),
    input: processGroupUpgradeInputSchema.optional(),
    id: z.string().uuid().optional(),
    expectedFingerprint: z
      .string()
      .regex(/^[a-f0-9]{64}$/)
      .optional(),
    expectedRevision: z.number().int().positive().optional(),
  })
  .superRefine((v, c) => {
    if (
      (["preview", "prepare"].includes(v.action) && !v.input) ||
      (v.action !== "preview" && !v.id) ||
      (["prepare", "execute", "rollback"].includes(v.action) && !v.expectedFingerprint) ||
      (["execute", "rollback"].includes(v.action) && !v.expectedRevision) ||
      (v.input && v.input.source.publisher.controller !== v.controllerName)
    )
      c.addIssue({ code: "custom", message: "Exact upgrade selector and reviewed revision required" });
  });
export type ProcessGroupUpgradeRequest = z.infer<typeof processGroupUpgradeRequestSchema>;
/** Shared journal observations; only PID/start identity and fresh capture prove readiness. */
export function inspectProcessGroupUpgradeJournal(review: GroupUpgradeReview, publisherPid?: number, receiverPid?: number) {
  const file = path.join(review.directory, "admission.sqlite");
  if (fs.realpathSync(file) !== file) throw Error("Unsafe retained journal path");
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000;");
    const meta = Object.fromEntries(
      db
        .prepare("SELECT key,value FROM metadata WHERE key IN ('id','contract','capture','runtime_namespace')")
        .all()
        .map((r) => [r["key"], r["value"]]),
    );
    if (meta["id"] !== review.journalId || sha256(String(meta["contract"])) !== review.journalContractSha256)
      throw Error("Retained journal contract changed");
    const capture = JSON.parse(String(meta["capture"] ?? "null"));
    const identity = (pid: number | undefined, digest: unknown) => {
      if (!pid || !Number.isSafeInteger(pid) || pid <= 0 || typeof digest !== "string" || process.platform !== "linux") return false;
      try {
        const namespace = sha256(fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim() + ":" + fs.readlinkSync("/proc/self/ns/pid"));
        const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8"),
          start = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
        return meta["runtime_namespace"] === namespace && !!start && digest === sha256(namespace + ":" + pid + ":" + start);
      } catch {
        return false;
      }
    };
    const rows = db.prepare("SELECT name,pid,owner,runtime_identity FROM profiles").all();
    return {
      receiverReady:
        capture?.pid === receiverPid &&
        ["ready", "capacity-blocked"].includes(capture?.state) &&
        Number.isSafeInteger(capture?.heartbeat) &&
        capture.heartbeat <= Date.now() + 1000 &&
        Date.now() - capture.heartbeat <= 5000 &&
        identity(receiverPid, capture.runtimeIdentity),
      publisherReady:
        rows.length === review.profiles.length &&
        review.profiles.every((p) =>
          rows.some((r) => r["name"] === p.processName && r["pid"] === publisherPid && !!r["owner"] && identity(publisherPid, r["runtime_identity"])),
        ),
    };
  } finally {
    db.close();
  }
}
