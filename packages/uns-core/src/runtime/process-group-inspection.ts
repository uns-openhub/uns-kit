import { createHash } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { z } from "zod";

import {
  authorizeConfiguredProcessGroupRuntime,
  configurationAuthorizationHash,
  hasProcessGroupAuthorization,
  processGroupManifestSchema as manifest,
} from "./process-group-authorization.js";
import {
  isLocalPilotPath,
  isProcessGroupJournalPath,
  normalizeProcessGroupConfiguration,
  processGroupDescriptor,
  processGroupJournalContract,
  processGroupJournalSchema,
  processGroupSourceIdentity,
  projectProcessGroupConfiguration,
  resolvedProcessGroupInputChannel,
} from "./process-group-contract.js";

export interface ProcessGroupConfigurationEvidence {
  processName: string;
  configurationChecksum: string;
  projectionChecksum: string;
  mappingSha256: string | null;
  dataGroup: string | null;
  contract: { digest: string; processName: string; profiles: { id: string; processName: string; mappingSha256: string; dataGroup: string }[] } | null;
}
/** Bound reads to installed package/instance resources, never arbitrary paths or symlinks. */
function resource(pkg: string, dir: string, file: string, relativeTo?: string): Buffer {
  if (file.length > 512) throw Error("Group resource path too long");
  const roots = [fs.realpathSync(pkg), fs.realpathSync(dir)];
  const candidates = relativeTo
    ? [path.resolve(relativeTo, file)]
    : path.isAbsolute(file)
      ? [file]
      : [path.resolve(dir, file), path.resolve(pkg, file)];
  for (const candidate of candidates) {
    if (!fs.existsSync(candidate)) continue;
    const real = fs.realpathSync(candidate);
    if (real !== candidate || !roots.some((root) => real.startsWith(root + path.sep)))
      throw Error("Group resource must stay inside installed package or instance");
    const fd = fs.openSync(real, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    try {
      const s = fs.fstatSync(fd);
      if (!s.isFile() || s.size > 1048576) throw Error("Invalid group resource");
      const b = fs.readFileSync(fd);
      if (b.length > 1048576) throw Error("Group resource too large");
      return b;
    } finally {
      fs.closeSync(fd);
    }
  }
  throw Error("Installed group resource is missing");
}
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
export function inspectProcessGroupConfiguration(
  pkg: string,
  dir: string,
  raw: string,
  controllerName = process.env["UNS_CONTROLLER_NAME"] ?? "",
  options: { configurationChecksum(raw: string): string | null; excludedUnsFields: readonly string[] },
): ProcessGroupConfigurationEvidence {
  const original = JSON.parse(raw);
  const descriptor = processGroupDescriptor(pkg);
  if (!descriptor) throw Error("Installed package has no supported process-group contract");
  const c = normalizeProcessGroupConfiguration(original, descriptor),
    uns = { ...c.uns },
    publisher = c.publisher;
  const production =
    c.journal && (!isLocalPilotPath(pkg) || hasProcessGroupAuthorization())
      ? authorizeConfiguredProcessGroupRuntime(pkg, dir, original, controllerName)
      : undefined;
  if (!c.matches || (c.runtimeRole !== undefined && c.runtimeRole !== "publisher") || !publisher || typeof uns.processName !== "string")
    throw Error("This runtime has no supported reviewed group configuration");
  if (c.journal !== undefined) {
    // Readiness requires explicit capacities; their limits come from the shared writer schema.
    const admission = processGroupJournalSchema
      .extend({
        minFreeBytes: processGroupJournalSchema.shape.minFreeBytes.unwrap().optional(),
        maxRecords: processGroupJournalSchema.shape.maxRecords.unwrap(),
        maxBytes: processGroupJournalSchema.shape.maxBytes.unwrap(),
      })
      .parse(c.journal);
    if ((!isLocalPilotPath(pkg) && !production) || path.resolve(admission.directory) !== admission.directory)
      throw Error("Durable process-group admission is a same-host local pilot");
    const persistent = !isLocalPilotPath(admission.directory);
    if (production && !persistent) throw Error("Authorized process-group journal requires a permanent mounted state root");
    if (persistent) {
      const stateRoot = process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
      if (!admission.journalId) throw Error("Persistent admission needs its initialized journalId binding");
      if (
        process.platform !== "linux" ||
        !stateRoot ||
        path.resolve(stateRoot) !== stateRoot ||
        !isProcessGroupJournalPath(stateRoot, admission.directory, descriptor.namespace)
      )
        throw Error("Persistent admission needs the reviewed Linux node-local state root");
      if (JSON.parse(resource(pkg, dir, "package.json").toString()).unsDatahub?.processGroupPersistentAdmission !== 1)
        throw Error("Publisher does not support persistent admission ownership");
    }
    const capability = JSON.parse(resource(pkg, dir, "package.json").toString()).unsDatahub?.processGroupAdmission;
    if (capability !== 1) throw Error("Publisher does not support durable process-group admission");
    if (path.isAbsolute(admission.groupManifest) || admission.groupManifest.includes(".."))
      throw Error("Admission manifest must be package-relative");
    const admitted = manifest.parse(JSON.parse(resource(pkg, dir, admission.groupManifest).toString()));
    for (let parent = admission.directory; parent !== "/"; parent = path.dirname(parent))
      if (fs.lstatSync(parent).isSymbolicLink()) throw Error("Admission directory cannot use symbolic links");
    const databaseFile = path.join(admission.directory, "admission.sqlite");
    if (fs.lstatSync(databaseFile).isSymbolicLink()) throw Error("Admission journal cannot use symbolic links");
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
    const journal = new DatabaseSync(databaseFile, { readOnly: true });
    try {
      journal.exec("PRAGMA busy_timeout=1000");
      if (persistent && journal.prepare("SELECT value FROM metadata WHERE key='id'").get()?.["value"] !== admission.journalId)
        throw Error("Persistent admission journalId binding differs");
      const input = resolvedProcessGroupInputChannel(original);
      const sourceIdentity = processGroupSourceIdentity(input);
      const expected = processGroupJournalContract(sourceIdentity, admission, admitted.profiles);
      const contract = journal.prepare("SELECT value FROM metadata WHERE key='contract'").get()?.["value"];
      if (contract !== expected) throw Error("Durable admission journal contract differs");
      const capture = JSON.parse(String(journal.prepare("SELECT value FROM metadata WHERE key='capture'").get()?.["value"] ?? "null"));
      if (
        !capture ||
        !["ready", "capacity-blocked"].includes(capture.state) ||
        !Number.isSafeInteger(capture.pid) ||
        capture.pid <= 0 ||
        !Number.isSafeInteger(capture.heartbeat) ||
        Date.now() - capture.heartbeat > 5000 ||
        capture.heartbeat > Date.now() + 1000
      )
        throw Error("Durable admission receiver is not ready");
      process.kill(capture.pid, 0);
      if (persistent) {
        const namespace = sha(
          Buffer.from(fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim() + ":" + fs.readlinkSync("/proc/self/ns/pid")),
        );
        const stored = journal.prepare("SELECT value FROM metadata WHERE key='runtime_namespace'").get()?.["value"];
        const stat = fs.readFileSync(`/proc/${capture.pid}/stat`, "utf8");
        const start = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
        if (stored !== namespace || !start || capture.runtimeIdentity !== sha(Buffer.from(namespace + ":" + capture.pid + ":" + start)))
          throw Error("Admission capture namespace or process identity differs");
      }
    } finally {
      journal.close();
    }
    if (publisher.groupManifest && publisher.groupManifest !== admission.groupManifest) throw Error("Admission and publisher manifests differ");
    if (!publisher.groupManifest) {
      const member = admitted.profiles.find((p) => p.processName === uns.processName);
      if (
        !member ||
        member.mappingSha256 !== sha(resource(pkg, dir, String(publisher.mappingFile))) ||
        member.dataGroup !== (publisher.dataGroup ?? descriptor.defaultDataGroup)
      )
        throw Error("Original publisher differs from admission contract");
    }
  }
  const checksum = options.configurationChecksum(raw);
  if (!checksum) throw Error("Invalid group configuration");
  const processName = uns.processName;
  for (const k of [...options.excludedUnsFields, "processName", "instanceMode", "handover", "handoverProtocol"]) delete uns[k];
  const projectionChecksum = configurationAuthorizationHash(projectProcessGroupConfiguration(original, descriptor, uns, production?.sha256));
  if (typeof publisher.groupManifest === "string") {
    if (Object.keys(publisher).some((k) => k !== "groupManifest")) throw Error("Group selector cannot override member settings");
    // Installed manifests are package-relative; members resolve relative to that exact file.
    if (path.isAbsolute(publisher.groupManifest) || publisher.groupManifest.includes("..")) throw Error("Group manifest must be package-relative");
    const bytes = resource(pkg, dir, path.resolve(pkg, publisher.groupManifest)),
      m = manifest.parse(JSON.parse(bytes.toString()));
    if (
      m.processName !== processName ||
      new Set(m.profiles.map((p) => p.processName)).size !== m.profiles.length ||
      m.profiles.some((p) => p.processName === processName)
    )
      throw Error("Manifest process identities overlap");
    for (const p of m.profiles)
      if (sha(resource(pkg, dir, p.mappingFile, path.dirname(path.resolve(pkg, publisher.groupManifest)))) !== p.mappingSha256)
        throw Error("Manifest member checksum changed");
    return {
      processName,
      configurationChecksum: checksum,
      projectionChecksum,
      mappingSha256: null,
      dataGroup: null,
      contract: {
        digest: sha(bytes),
        processName,
        profiles: m.profiles.map(({ mappingFile: _, ...p }) => p),
      },
    };
  }
  if (typeof publisher.mappingFile !== "string" || Object.keys(publisher).some((k) => !["mappingFile", "dataGroup"].includes(k)))
    throw Error("Explicit original mapping file required");
  return {
    processName,
    configurationChecksum: checksum,
    projectionChecksum,
    mappingSha256: sha(resource(pkg, dir, String(publisher.mappingFile))),
    dataGroup: z
      .string()
      .min(1)
      .max(100)
      .parse(publisher.dataGroup ?? descriptor.defaultDataGroup),
    contract: null,
  };
}
