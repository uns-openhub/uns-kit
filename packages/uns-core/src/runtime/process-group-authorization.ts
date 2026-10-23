import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import * as legacy from "./compatibility/energenti-authorization-v1.js";
import {
  type ProcessGroupRuntimeDescriptor,
  normalizeProcessGroupConfiguration,
  processGroupDescriptor,
  resolvedProcessGroupChannels,
} from "./process-group-contract.js";
import {
  configurationAuthorizationHash,
  groupArtifactHash,
  groupChannelsHash,
  readProcessGroupResource as read,
  sha256 as sha,
} from "./process-group-resources.js";

export { configurationAuthorizationHash, groupArtifactHash, groupChannelsHash } from "./process-group-resources.js";

export const hasProcessGroupAuthorization = legacy.hasProcessGroupAuthorization;
const digest = z.string().regex(/^[a-f0-9]{64}$/),
  id = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
const absolute = z
  .string()
  .max(512)
  .refine((v) => path.isAbsolute(v) && path.resolve(v) === v);
const placement = z.strictObject({
  instanceId: z.string().regex(/^[a-zA-Z0-9-]{1,100}$/),
  processName: id,
  role: z.enum(["source", "target", "receiver"]),
  configurationSha256: digest,
});
export const processGroupAuthorizationSchema = z.strictObject({
  schemaVersion: z.literal(3),
  kind: z.literal("uns-process-group-runtime-authorization"),
  protocolVersion: z.literal(1),
  packageName: z.string().min(1).max(214),
  namespace: id,
  groupId: id,
  descriptorSha256: digest,
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
export type ProcessGroupAuthorizationPolicy = z.infer<typeof processGroupAuthorizationSchema>;
export const processGroupManifestSchema = z.strictObject({
  schemaVersion: z.literal(1),
  id,
  inputTopic: z
    .string()
    .min(1)
    .max(512)
    .regex(/^[^#+\0]+$/),
  processName: id,
  profiles: z
    .array(z.strictObject({ id, processName: id, mappingFile: z.string().min(1).max(512), mappingSha256: digest, dataGroup: id }))
    .min(2)
    .max(16),
});
interface GenericAuthorization {
  readonly sha256: string;
  readonly file: string;
  readonly directory: string;
  readonly journalId: string | null;
  readonly role: "source" | "target" | "receiver";
  readonly groupId: string;
  readonly groupProcessName: string;
  readonly namespace: string;
}
export type ProcessGroupAuthorization = legacy.HvAuthorization | GenericAuthorization;
export type HvAuthorization = ProcessGroupAuthorization;
export type HvAuthorizationPolicy = legacy.HvAuthorizationPolicy;
export const hvAuthorizationSchema = legacy.hvAuthorizationSchema;
export const productionGroupIds = legacy.productionGroupIds;
const issued = new WeakSet<object>();
function privateDirectory(dir: string): void {
  for (let p = dir; p !== path.dirname(p); p = path.dirname(p)) if (fs.lstatSync(p).isSymbolicLink()) throw Error("Linked policy directory");
  const s = fs.lstatSync(dir);
  if (!s.isDirectory() || s.uid !== process.getuid?.() || (s.mode & 0o077) !== 0) throw Error("Insecure policy directory");
}
function registryRoot(root: string, dir: string): string {
  if (!path.isAbsolute(root) || path.resolve(root) !== root || path.resolve(dir) !== dir) throw Error("Invalid policy registry");
  privateDirectory(root);
  if (dir === root) return "*"; // Explicit collection of independently owned namespaces.
  const relative = path.relative(root, dir);
  if (!path.isAbsolute(root) || path.resolve(root) !== root || !/^([a-z][a-z0-9-]{0,99})\/authorizations$/.test(relative))
    throw Error("Invalid policy registry");
  privateDirectory(root);
  privateDirectory(dir);
  return relative.split("/")[0]!;
}
function selectedFile(pkg: string, raw: Record<string, unknown>, root: string, explicit?: string): string {
  const descriptor = processGroupDescriptor(pkg);
  if (!descriptor) throw Error("Missing runtime contract");
  const c = normalizeProcessGroupConfiguration(raw, descriptor),
    journal = c.journal as Record<string, unknown> | undefined;
  const manifest = descriptor.manifests.find((m) => m.file === journal?.["groupManifest"]);
  if (!manifest) throw Error("Unregistered group manifest");
  const dir = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
  if (explicit) return explicit;
  if (dir) {
    const selected = registryRoot(root, dir);
    if (selected !== "*" && selected !== descriptor.namespace) throw Error("Foreign runtime registry");
    const registry = selected === "*" ? path.join(root, descriptor.namespace, "authorizations") : dir;
    privateDirectory(registry);
    return path.join(registry, manifest.id + ".json");
  }
  const single = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
  if (!single) throw Error("No selected policy");
  return single;
}
/** Bounded private registry enumeration; retained state without its policy fails closed.
 * New groups are operator-owned data and never a release-time enum in the Controller.
 */
export function configuredGroupProcessNames(): Set<string> {
  if (!hasProcessGroupAuthorization()) return new Set();
  try {
    const root = process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
    if (!root) throw Error();
    const files = new Set<string>();
    const single = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
    if (single) files.add(single);
    const dir = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
    if (dir) {
      const selected = registryRoot(root, dir);
      let registries: { namespace: string; directory: string }[];
      if (selected === "*") {
        const namespaces = fs.readdirSync(root, { withFileTypes: true });
        if (namespaces.length > 64 || namespaces.some((e) => e.isSymbolicLink())) throw Error("Registry namespace budget or link violation");
        registries = namespaces
          .filter((e) => e.isDirectory())
          .map((e) => {
            if (!id.safeParse(e.name).success) throw Error("Invalid registry namespace");
            privateDirectory(path.join(root, e.name));
            return { namespace: e.name, directory: path.join(root, e.name, "authorizations") };
          });
      } else registries = [{ namespace: selected, directory: dir }];
      for (const registry of registries) {
        let entries: fs.Dirent[] = [];
        try {
          privateDirectory(registry.directory);
          entries = fs.readdirSync(registry.directory, { withFileTypes: true });
        } catch (e) {
          if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
        }
        if (entries.length > 64) throw Error("Policy registry budget exceeded");
        for (const entry of entries)
          if (entry.name.endsWith(".json")) {
            if (!/^[a-z][a-z0-9-]{0,99}\.json$/.test(entry.name)) throw Error("Invalid registry entry");
            files.add(path.join(registry.directory, entry.name));
            if (files.size > 64) throw Error("Total policy registry budget exceeded");
          }
        const journals = path.join(root, registry.namespace, "admission");
        try {
          // Root/registry permissions already protect visibility; retain old parent modes.
          for (let parent = journals; parent !== path.dirname(parent); parent = path.dirname(parent))
            if (fs.lstatSync(parent).isSymbolicLink()) throw Error("Linked journal registry");
          const retained = fs.readdirSync(journals, { withFileTypes: true });
          if (retained.length > 64) throw Error("Retained journal budget exceeded");
          for (const entry of retained)
            if (
              !id.safeParse(entry.name).success ||
              entry.isSymbolicLink() ||
              !entry.isDirectory() ||
              !files.has(path.join(registry.directory, entry.name + ".json"))
            )
              throw Error("Retained journal has no reviewed policy");
        } catch (e) {
          if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
        }
      }
    }
    const result = new Set<string>();
    for (const file of files) {
      const raw = JSON.parse(read(file, true).toString());
      const p =
        raw.schemaVersion === 3
          ? processGroupAuthorizationSchema.parse(raw)
          : z.union([legacy.hvAuthorizationSchema, legacy.processGroupAuthorizationSchema]).parse(raw);
      const { namespace, group } = p.schemaVersion === 3 ? { namespace: p.namespace, group: p.groupId } : legacy.legacyPolicyIdentity(p);
      if (
        p.stateRoot !== root ||
        file !== path.join(root, namespace, "authorizations", group + ".json") ||
        p.directory !== path.join(root, namespace, "admission", group)
      )
        throw Error();
      privateDirectory(root);
      privateDirectory(path.dirname(file));
      for (const role of p.placements) result.add(role.processName);
    }
    return result;
  } catch {
    throw Error("Process-group authorization registry is missing, changed or insecure; review required");
  }
}
export interface ProcessGroupAuthorizationContext {
  packageRoot: string;
  instanceDirectory: string;
  rawConfiguration: Record<string, unknown>;
  channels: unknown[];
  controllerName: string;
  file?: string;
  stateRoot?: string;
  initialize?: boolean;
}
function verifyRuntimePolicy(context: ProcessGroupAuthorizationContext, candidate?: Buffer): ProcessGroupAuthorization {
  try {
    const root = context.stateRoot ?? process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
    if (!root || !context.controllerName) throw Error();
    const file = selectedFile(context.packageRoot, context.rawConfiguration, root, context.file),
      bytes = candidate ?? read(file, true),
      unparsed = JSON.parse(bytes.toString());
    if (unparsed.schemaVersion !== 3) {
      if (candidate) throw Error("Only generic policies can be staged");
      return legacy.authorizeHvRuntime({ ...context, file, stateRoot: root });
    }
    const p = processGroupAuthorizationSchema.parse(unparsed),
      d = processGroupDescriptor(context.packageRoot);
    if (!d || d.legacy) throw Error("Generic runtime descriptor required");
    const pkg = JSON.parse(read(path.join(context.packageRoot, "package.json")).toString());
    const manifest = d.manifests.find((m) => m.id === p.groupId);
    if (
      !manifest ||
      p.packageName !== pkg.name ||
      p.namespace !== d.namespace ||
      p.descriptorSha256 !== configurationAuthorizationHash(pkg.unsDatahub.processGroupRuntime) ||
      p.packageRoot !== context.packageRoot ||
      p.controllerName !== context.controllerName ||
      p.stateRoot !== root ||
      file !== path.join(root, d.namespace, "authorizations", p.groupId + ".json") ||
      p.directory !== path.join(root, d.namespace, "admission", p.groupId)
    )
      throw Error();
    privateDirectory(root);
    privateDirectory(path.dirname(file));
    if (groupArtifactHash(context.packageRoot) !== p.artifactSha256 || groupChannelsHash(context.channels) !== p.channelsSha256) throw Error();
    const manifestBytes = read(path.join(context.packageRoot, manifest.file));
    if (sha(manifestBytes) !== p.groupManifestSha256) throw Error();
    const m = processGroupManifestSchema.parse(JSON.parse(manifestBytes.toString()));
    if (m.id !== p.groupId) throw Error();
    const receiver = m.processName + "-admission";
    if (
      new Set(m.profiles.map((x) => x.id)).size !== m.profiles.length ||
      new Set(m.profiles.map((x) => x.processName)).size !== m.profiles.length ||
      m.profiles.some((x) => x.processName === m.processName || x.processName === receiver)
    )
      throw Error();
    for (const member of m.profiles) {
      const f = path.resolve(context.packageRoot, path.dirname(manifest.file), member.mappingFile);
      if (!f.startsWith(context.packageRoot + path.sep) || sha(read(f)) !== member.mappingSha256) throw Error();
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
    const entry = p.placements.find((x) => context.instanceDirectory === path.join(context.packageRoot, "instances", x.instanceId));
    const c = normalizeProcessGroupConfiguration(context.rawConfiguration, d),
      j = c.journal as Record<string, unknown> | undefined,
      k = c.publisher;
    if (
      !entry ||
      !c.matches ||
      entry.processName !== c.uns?.["processName"] ||
      entry.configurationSha256 !== configurationAuthorizationHash(context.rawConfiguration) ||
      !j ||
      j["directory"] !== p.directory ||
      (j["journalId"] ?? null) !== p.journalId ||
      j["groupManifest"] !== manifest.file
    )
      throw Error();
    if (context.initialize ? p.journalId !== null || entry.role !== "receiver" : p.journalId === null) throw Error();
    if (entry.role === "receiver" ? c.runtimeRole !== "admission" : c.runtimeRole !== undefined && c.runtimeRole !== "publisher") throw Error();
    const member = m.profiles.find((x) => x.processName === entry.processName),
      mapping = path.resolve(context.packageRoot, String(k["mappingFile"]));
    if (
      entry.role !== "source"
        ? k["groupManifest"] !== manifest.file
        : k["groupManifest"] !== undefined ||
          k["dataGroup"] !== member?.dataGroup ||
          !mapping.startsWith(context.packageRoot + path.sep) ||
          sha(read(mapping)) !== member?.mappingSha256
    )
      throw Error();
    const result = Object.freeze({
      sha256: sha(bytes),
      file,
      directory: p.directory,
      journalId: p.journalId,
      role: entry.role,
      groupId: p.groupId,
      groupProcessName: m.processName,
      namespace: d.namespace,
    });
    if (!candidate) issued.add(result);
    return result;
  } catch {
    throw Error("Process-group runtime authorization is missing, changed or does not match this installed placement; review required");
  }
}
/** Live authority always comes from the current canonical private policy. */
export function authorizeProcessGroupRuntime(context: ProcessGroupAuthorizationContext): ProcessGroupAuthorization {
  return verifyRuntimePolicy(context);
}
/** Review only: never issues an authorization accepted by runtime/launch checks. */
export function validateProcessGroupPolicyCandidate(context: ProcessGroupAuthorizationContext, bytes: Buffer): void {
  verifyRuntimePolicy(context, bytes);
}
export function processGroupAuthorizationFile(context: ProcessGroupAuthorizationContext): string {
  const root = context.stateRoot ?? process.env["UNS_PROCESS_GROUP_STATE_ROOT"];
  if (!root) throw Error("Missing process-group state root");
  return selectedFile(context.packageRoot, context.rawConfiguration, root, context.file);
}
export const authorizeHvRuntime = authorizeProcessGroupRuntime;
export function authorizeConfiguredProcessGroupRuntime(
  pkg: string,
  dir: string,
  raw: Record<string, unknown>,
  controllerName: string,
  initialize = false,
) {
  return authorizeProcessGroupRuntime({
    packageRoot: pkg,
    instanceDirectory: dir,
    rawConfiguration: raw,
    channels: resolvedProcessGroupChannels(raw),
    controllerName,
    initialize,
  });
}
export function assertProcessGroupJournalAuthorization(
  value: ProcessGroupAuthorization | undefined,
  directory: string,
  journalId: string | undefined,
): void {
  if (value && issued.has(value)) {
    if (value.directory !== directory || value.journalId !== (journalId ?? null) || sha(read(value.file, true)) !== value.sha256)
      throw Error("Exact reviewed process-group authorization required");
    return;
  }
  legacy.assertHvJournalAuthorization(value as legacy.HvAuthorization | undefined, directory, journalId);
}
export const assertHvJournalAuthorization = assertProcessGroupJournalAuthorization;
export function assertProcessGroupStateMount(value: ProcessGroupAuthorization, root: string): void {
  if (!issued.has(value)) {
    legacy.assertHvStateMount(value as legacy.HvAuthorization, root);
    return;
  }
  assertProcessGroupJournalAuthorization(value, value.directory, value.journalId ?? undefined);
  if (process.platform !== "linux" || !("namespace" in value) || value.directory !== path.join(root, value.namespace, "admission", value.groupId))
    throw Error("Process group requires its reviewed Linux state mount");
  const mount = fs
    .readFileSync("/proc/self/mountinfo", "utf8")
    .split("\n")
    .map((l) => l.split(" "))
    .filter((f) => f[4] === root)
    .at(-1);
  if (!mount || !mount[5]?.split(",").includes("rw") || !["xfs", "ext4", "btrfs", "zfs"].includes(mount[mount.indexOf("-") + 1] ?? ""))
    throw Error("Process-group state root must be an explicit writable local Linux filesystem mount");
}
export const assertHvStateMount = assertProcessGroupStateMount;
export function assertProcessGroupTargetLaunch(
  value: ProcessGroupAuthorization,
  dir: string,
  operationId: string | undefined,
  launchId: string | undefined,
): void {
  if (!issued.has(value)) {
    legacy.assertHvTargetLaunch(value as legacy.HvAuthorization, dir, operationId, launchId);
    return;
  }
  if (value.role !== "target") return;
  try {
    assertProcessGroupJournalAuthorization(value, value.directory, value.journalId ?? undefined);
    const r = JSON.parse(read(path.join(dir, "process-group-reservation.json"), true).toString());
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
    throw Error("Target requires its exact reviewed group reservation and launch identity");
  }
}
export const assertHvTargetLaunch = assertProcessGroupTargetLaunch;
