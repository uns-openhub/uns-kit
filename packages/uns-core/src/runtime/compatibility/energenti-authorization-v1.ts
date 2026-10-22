/** Frozen compatibility reader for previously issued Energenti policies v1/v2.
 * New runtimes use the generic SDK contract; never extend this list for new groups. */
// Wire-identical to the Controller's runtime verifier. This is trusted node-local
// policy, never an instance-config flag, network credential or execution permit.
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import {
  configurationAuthorizationHash,
  groupArtifactHash,
  groupChannelsHash,
  readProcessGroupResource as read,
  sha256 as sha,
} from "../process-group-resources.js";

export { configurationAuthorizationHash, groupArtifactHash, groupChannelsHash } from "../process-group-resources.js";

const digest = z.string().regex(/^[a-f0-9]{64}$/);
const absolute = z
  .string()
  .max(512)
  .refine((v) => path.isAbsolute(v) && path.resolve(v) === v);
const names = ["uns-energenti-san", "uns-energenti-crno", "uns-energenti-kepware-hv", "uns-energenti-kepware-hv-admission"];
export const hvAuthorizationSchema = z.strictObject({
  schemaVersion: z.literal(1),
  kind: z.literal("energenti-hv-runtime-authorization"),
  controllerName: z.string().min(1).max(100),
  packageRoot: absolute,
  artifactSha256: digest,
  groupManifestSha256: digest,
  channelsSha256: digest,
  stateRoot: absolute,
  directory: absolute,
  journalId: z.string().uuid().nullable(),
  placements: z
    .array(
      z.strictObject({
        instanceId: z.string().regex(/^[a-zA-Z0-9-]{1,100}$/),
        processName: z.enum(names as [string, ...string[]]),
        role: z.enum(["source", "target", "receiver"]),
        configurationSha256: digest,
      }),
    )
    .length(4),
});
const identity = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
export const productionGroupIds = ["hv", "vv", "jek"] as const;
const groupId = z.enum(productionGroupIds);
const placement = z.strictObject({
  instanceId: z.string().regex(/^[a-zA-Z0-9-]{1,100}$/),
  processName: identity,
  role: z.enum(["source", "target", "receiver"]),
  configurationSha256: digest,
});
/** New groups require an independently reviewed policy; v1 HV remains unchanged. */
export const processGroupAuthorizationSchema = z.strictObject({
  schemaVersion: z.literal(2),
  kind: z.literal("energenti-process-group-runtime-authorization"),
  groupId,
  controllerName: z.string().min(1).max(100),
  packageRoot: absolute,
  artifactSha256: digest,
  groupManifestSha256: digest,
  channelsSha256: digest,
  stateRoot: absolute,
  directory: absolute,
  journalId: z.string().uuid().nullable(),
  placements: z.array(placement).min(4).max(18),
});
const policySchema = z.union([hvAuthorizationSchema, processGroupAuthorizationSchema]);
const manifestSchema = z.strictObject({
  schemaVersion: z.literal(1),
  id: groupId,
  inputTopic: z
    .string()
    .min(1)
    .max(512)
    .regex(/^[^#+\0]+$/),
  processName: identity,
  profiles: z
    .array(
      z.strictObject({
        id: identity,
        processName: identity,
        mappingFile: z.string().min(1).max(512),
        mappingSha256: digest,
        dataGroup: identity,
      }),
    )
    .min(2)
    .max(16),
});
export type HvAuthorizationPolicy = z.infer<typeof hvAuthorizationSchema>;
export type ProcessGroupAuthorizationPolicy = z.infer<typeof processGroupAuthorizationSchema>;
export type HvAuthorization = Readonly<{
  sha256: string;
  file: string;
  directory: string;
  journalId: string | null;
  role: "source" | "target" | "receiver";
  groupId: z.infer<typeof groupId>;
  groupProcessName: string;
}>;
const issued = new WeakSet<object>();
export function hasProcessGroupAuthorization(): boolean {
  return !!(process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"] || process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"]);
}
function privateDirectory(directory: string): void {
  // Read checks every ancestor for links even if the leaf is not a policy file.
  for (let p = directory; p !== "/"; p = path.dirname(p)) if (fs.lstatSync(p).isSymbolicLink()) throw Error();
  const s = fs.lstatSync(directory);
  if (!s.isDirectory() || s.uid !== process.getuid?.() || (s.mode & 0o077) !== 0) throw Error();
}
function policyAt(file: string, stateRoot: string) {
  const bytes = read(file, true),
    policy = policySchema.parse(JSON.parse(bytes.toString()));
  const id = policy.schemaVersion === 1 ? "hv" : policy.groupId;
  if (
    policy.stateRoot !== stateRoot ||
    file !== path.join(stateRoot, "energenti/authorizations", id + ".json") ||
    policy.directory !== path.join(stateRoot, "energenti/admission", id)
  )
    throw Error();
  privateDirectory(stateRoot);
  privateDirectory(path.dirname(file));
  return { bytes, policy, id };
}
/** A registry is a private fixed directory, never a scan or an instance-supplied path. */
function registryDirectory(stateRoot: string, directory: string): string {
  if (!path.isAbsolute(stateRoot) || path.resolve(stateRoot) !== stateRoot || directory !== path.join(stateRoot, "energenti/authorizations"))
    throw Error();
  privateDirectory(stateRoot);
  privateDirectory(directory);
  return directory;
}
function selectedFile(raw: Record<string, unknown>, stateRoot: string): string {
  const j = raw["kepwareJournal"] as Record<string, unknown> | undefined;
  const relative = j?.["groupManifest"];
  const id = productionGroupIds.find((id) => relative === `docs/kepware-groups/${id}.json`);
  if (!id) throw Error();
  const directory = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
  if (directory) return path.join(registryDirectory(stateRoot, directory), id + ".json");
  const legacy = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
  if (!legacy) throw Error();
  return legacy;
}
/** Blocks retained direct publishers only for groups actually configured by the operator. */
export function configuredGroupProcessNames(): Set<string> {
  const result = new Set<string>(),
    stateRoot = process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
  if (!hasProcessGroupAuthorization()) return result;
  try {
    if (!stateRoot) throw Error();
    const files = new Set<string>();
    const single = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
    if (single) files.add(single);
    const directory = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
    if (directory) {
      registryDirectory(stateRoot, directory);
      for (const id of productionGroupIds) {
        const file = path.join(directory, id + ".json");
        // lstat (not exists) preserves denial and dangling-link failures.
        try {
          fs.lstatSync(file);
          files.add(file);
        } catch (e) {
          if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
          // Removing a policy must never re-enable direct publishers beside retained work.
          let retained = false;
          try {
            fs.lstatSync(path.join(stateRoot, "energenti/admission", id));
            retained = true;
          } catch (storageError) {
            if ((storageError as NodeJS.ErrnoException).code !== "ENOENT") throw storageError;
          }
          if (retained) throw Error("Retained journal has no reviewed policy");
        }
      }
    }
    for (const file of files) for (const p of policyAt(file, stateRoot).policy.placements) result.add(p.processName);
    return result;
  } catch {
    throw Error("Process-group authorization registry is missing, changed or insecure; review required");
  }
}
export function authorizeHvRuntime(context: {
  packageRoot: string;
  instanceDirectory: string;
  rawConfiguration: Record<string, unknown>;
  channels: unknown[];
  controllerName: string;
  file?: string;
  stateRoot?: string;
  initialize?: boolean;
}): HvAuthorization {
  try {
    const stateRoot = context.stateRoot ?? process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
    if (!stateRoot || !context.controllerName) throw Error();
    const file = context.file ?? selectedFile(context.rawConfiguration, stateRoot);
    const { bytes, policy: p, id } = policyAt(file, stateRoot);
    if (p.controllerName !== context.controllerName || p.packageRoot !== context.packageRoot) throw Error();
    const pkg = JSON.parse(read(path.join(p.packageRoot, "package.json")).toString());
    const capability = pkg.unsDatahub?.processGroupProductionAuthorization;
    const supportedCapability =
      p.schemaVersion === 1 ? [1, 2, 3].includes(capability) : id === "jek" ? capability === 3 : [2, 3].includes(capability);
    if (pkg.name !== "uns-energenti" || !supportedCapability || groupArtifactHash(p.packageRoot) !== p.artifactSha256) throw Error();
    const manifestPath = `docs/kepware-groups/${id}.json`;
    const manifestBytes = read(path.join(p.packageRoot, manifestPath));
    if (sha(manifestBytes) !== p.groupManifestSha256 || groupChannelsHash(context.channels) !== p.channelsSha256) throw Error();
    const m = manifestSchema.parse(JSON.parse(manifestBytes.toString()));
    if (m.id !== id || m.processName !== `uns-energenti-kepware-${id}`) throw Error();
    const receiver = m.processName + "-admission";
    if (
      new Set(m.profiles.map((x) => x.id)).size !== m.profiles.length ||
      new Set(m.profiles.map((x) => x.processName)).size !== m.profiles.length ||
      m.profiles.some((x) => x.processName === m.processName || x.processName === receiver)
    )
      throw Error();
    // Preserve the exact legacy production boundary; only v2 derives membership from its pinned manifest.
    if (p.schemaVersion === 1) {
      const legacy = [
        { name: names[0], sha: "d9047de4eef02de32df115cb12e900ad4923e802bb4434afa6560e1eaaf66481" },
        { name: names[1], sha: "f2a06f15f31e56592ce228c3f12d934a09bd0e24712c50c420062b64f9f73cb5" },
      ];
      if (
        m.profiles.length !== 2 ||
        legacy.some((x) => !m.profiles.some((v) => v.processName === x.name && v.mappingSha256 === x.sha && v.dataGroup === "kepware"))
      )
        throw Error();
    }
    for (const member of m.profiles) {
      const f = path.resolve(p.packageRoot, "docs/kepware-groups", member.mappingFile);
      if (!f.startsWith(p.packageRoot + path.sep) || !f.endsWith(".csv") || sha(read(f)) !== member.mappingSha256) throw Error();
    }
    const expected = new Map([
      ...m.profiles.map((x) => [x.processName, "source"] as const),
      [m.processName, "target"] as const,
      [receiver, "receiver"] as const,
    ]);
    if (
      p.placements.length !== expected.size ||
      new Set(p.placements.map((x) => x.instanceId)).size !== expected.size ||
      new Set(p.placements.map((x) => x.processName)).size !== expected.size ||
      p.placements.some((x) => expected.get(x.processName) !== x.role)
    )
      throw Error();
    const entry = p.placements.find((x) => context.instanceDirectory === path.join(p.packageRoot, "instances", x.instanceId));
    const c = context.rawConfiguration,
      uns = c["uns"] as Record<string, unknown>,
      k = c["kepware"] as Record<string, unknown>,
      j = c["kepwareJournal"] as Record<string, unknown>;
    if (
      !entry ||
      entry.processName !== uns?.["processName"] ||
      entry.configurationSha256 !== configurationAuthorizationHash(c) ||
      c["sourceKind"] !== "kepware" ||
      !j ||
      j["directory"] !== p.directory ||
      (j["journalId"] ?? null) !== p.journalId ||
      j["groupManifest"] !== manifestPath
    )
      throw Error();
    if (context.initialize ? p.journalId !== null || entry.role !== "receiver" : p.journalId === null) throw Error();
    if (
      entry.role === "receiver"
        ? c["kepwareRuntimeRole"] !== "admission"
        : c["kepwareRuntimeRole"] !== undefined && c["kepwareRuntimeRole"] !== "publisher"
    )
      throw Error();
    const mapping = path.resolve(p.packageRoot, String(k?.["mappingFile"]));
    const member = m.profiles.find((x) => x.processName === entry.processName);
    if (
      entry.role !== "source"
        ? k?.["groupManifest"] !== manifestPath
        : k?.["groupManifest"] !== undefined ||
          k?.["dataGroup"] !== member?.dataGroup ||
          !mapping.startsWith(p.packageRoot + path.sep) ||
          sha(read(mapping)) !== member?.mappingSha256
    )
      throw Error();
    const result = Object.freeze({
      sha256: sha(bytes),
      file,
      directory: p.directory,
      journalId: p.journalId,
      role: entry.role,
      groupId: id,
      groupProcessName: m.processName,
    });
    issued.add(result);
    return result;
  } catch {
    throw Error("Process-group runtime authorization is missing, changed or does not match this installed placement; review required");
  }
}
export function assertHvJournalAuthorization(value: HvAuthorization | undefined, directory: string, journalId: string | undefined): void {
  if (
    !value ||
    !issued.has(value) ||
    value.directory !== directory ||
    value.journalId !== (journalId ?? null) ||
    sha(read(value.file, true)) !== value.sha256
  )
    throw Error("Exact reviewed process-group runtime authorization is required for this journal");
}

/** Preview must reject a missing/read-only/nonlocal mount before any source drain. */
export function assertHvStateMount(value: HvAuthorization, stateRoot: string): void {
  assertHvJournalAuthorization(value, value.directory, value.journalId ?? undefined);
  if (process.platform !== "linux" || value.directory !== path.join(stateRoot, "energenti/admission", value.groupId))
    throw Error("Process group requires the reviewed local Linux state mount");
  const mount = fs
    .readFileSync("/proc/self/mountinfo", "utf8")
    .split("\n")
    .map((l) => l.split(" "))
    .filter((f) => f[4] === stateRoot)
    .at(-1);
  if (!mount || !mount[5]?.split(",").includes("rw") || !["xfs", "ext4", "btrfs", "zfs"].includes(mount[mount.indexOf("-") + 1] ?? ""))
    throw Error("Process-group state root must be an explicit writable local Linux filesystem mount");
}

/** A runtime policy is not permission to bypass the reviewed transition. */
export function assertHvTargetLaunch(
  value: HvAuthorization,
  instanceDirectory: string,
  operationId: string | undefined,
  launchId: string | undefined,
): void {
  if (value.role !== "target") return;
  try {
    assertHvJournalAuthorization(value, value.directory, value.journalId ?? undefined);
    const r = JSON.parse(read(path.join(instanceDirectory, "process-group-reservation.json"), true).toString());
    if (
      !z.string().uuid().safeParse(operationId).success ||
      !z.string().uuid().safeParse(launchId).success ||
      r.operationId !== operationId ||
      r.role !== "target" ||
      r.lastLaunch?.launchId !== launchId ||
      !z.string().uuid().safeParse(r.lastLaunch?.requestId).success ||
      !digest.safeParse(r.configurationChecksum).success
    )
      throw Error();
  } catch {
    throw Error(`${value.groupId.toUpperCase()} target requires its exact reviewed group reservation and launch identity`);
  }
}
