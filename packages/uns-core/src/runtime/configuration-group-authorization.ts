import path from "node:path";
import { z } from "zod";

import { configurationGroupJournalSchema, configurationResourceHash, selectedConfigurationGroup } from "./configuration-group-contract.js";
import type { ProcessGroupAuthorizationContext } from "./process-group-authorization.js";
import { readConfigurationSelector } from "./process-group-contract.js";
import {
  assertPrivateProcessGroupDirectory,
  groupArtifactHash,
  groupChannelsHash,
  readProcessGroupResource,
  sha256,
} from "./process-group-resources.js";

const digest = z.string().regex(/^[a-f0-9]{64}$/),
  identity = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
const absolute = z
  .string()
  .max(512)
  .refine((v) => path.isAbsolute(v) && path.resolve(v) === v);
export const configurationGroupAuthorizationSchema = z.strictObject({
  schemaVersion: z.literal(4),
  kind: z.literal("uns-process-group-runtime-authorization"),
  protocolVersion: z.literal(2),
  packageName: z.string().min(1).max(214),
  namespace: identity,
  groupId: identity,
  generationId: identity,
  descriptorSha256: digest,
  controllerName: z.string().min(1).max(100),
  packageRoot: absolute,
  artifactSha256: digest,
  groupSha256: digest,
  channelsSha256: digest,
  stateRoot: absolute,
  directory: absolute,
  journalId: z.string().uuid().nullable(),
  placements: z
    .array(
      z.strictObject({
        instanceId: z.string().regex(/^[a-zA-Z0-9-]{1,100}$/),
        processName: identity,
        role: z.enum(["source", "target", "receiver"]),
        configurationSha256: digest,
      }),
    )
    .min(4)
    .max(18),
});
export type ConfigurationGroupAuthorizationPolicy = z.infer<typeof configurationGroupAuthorizationSchema>;

/** Canonical policy selection uses the reviewed generation, not an app-selected policy path. */
export function configurationGroupPolicyIdentity(
  packageRoot: string,
  raw: Record<string, unknown>,
):
  | {
      namespace: string;
      generationId: string;
    }
  | undefined {
  const selected = selectedConfigurationGroup(packageRoot, raw);
  if (!selected) return undefined;
  const journal = configurationGroupJournalSchema.parse(readConfigurationSelector(raw, selected.descriptor.journalPath));
  if (journal.groupSha256 !== selected.snapshot.sha256) throw Error("Configuration group journal differs from the selected revision");
  return { namespace: selected.descriptor.namespace, generationId: journal.generationId };
}

/** Data-only verifier. Its result alone is not runtime authority; the common issuer owns that boundary. */
export function verifyConfigurationGroupPolicy(context: ProcessGroupAuthorizationContext, bytes: Buffer, root: string, file: string) {
  const p = configurationGroupAuthorizationSchema.parse(JSON.parse(bytes.toString()));
  const selected = selectedConfigurationGroup(context.packageRoot, context.rawConfiguration);
  if (!selected) throw Error("Configuration group capability required");
  const { descriptor: d, snapshot } = selected,
    group = snapshot.group;
  const pkg = JSON.parse(readProcessGroupResource(path.join(context.packageRoot, "package.json")).toString());
  const declaration = pkg.unsDatahub?.processGroupConfigurationRuntime;
  if (
    p.packageName !== pkg.name ||
    p.namespace !== d.namespace ||
    p.descriptorSha256 !== configurationResourceHash(declaration) ||
    p.packageRoot !== context.packageRoot ||
    p.controllerName !== context.controllerName ||
    p.stateRoot !== root ||
    p.groupId !== group.id ||
    p.groupSha256 !== snapshot.sha256 ||
    file !== path.join(root, d.namespace, "authorizations", p.generationId + ".json") ||
    p.directory !== path.join(root, d.namespace, "admission", p.generationId)
  )
    throw Error("Reviewed configuration group placement differs");
  assertPrivateProcessGroupDirectory(root);
  assertPrivateProcessGroupDirectory(path.dirname(file));
  if (groupArtifactHash(context.packageRoot) !== p.artifactSha256 || groupChannelsHash(context.channels) !== p.channelsSha256)
    throw Error("Reviewed artifact or channels differ");
  const expected = new Map([
    ...group.profiles.map((member) => [member.processName, "source"] as const),
    [group.processName, "target"] as const,
    [group.receiverProcessName, "receiver"] as const,
  ]);
  if (
    p.placements.length !== expected.size ||
    new Set(p.placements.map((x) => x.instanceId)).size !== expected.size ||
    new Set(p.placements.map((x) => x.processName)).size !== expected.size ||
    p.placements.some((x) => expected.get(x.processName) !== x.role)
  )
    throw Error("Reviewed process roles differ");
  const entry = p.placements.find((x) => context.instanceDirectory === path.join(context.packageRoot, "instances", x.instanceId));
  const raw = context.rawConfiguration,
    uns = raw["uns"] as Record<string, unknown> | undefined;
  const journal = configurationGroupJournalSchema.parse(readConfigurationSelector(raw, d.journalPath));
  const role = readConfigurationSelector(raw, d.runtimeRolePath),
    memberId = readConfigurationSelector(raw, d.memberPath);
  if (
    !entry ||
    entry.processName !== uns?.["processName"] ||
    entry.configurationSha256 !== configurationResourceHash(raw) ||
    journal.directory !== p.directory ||
    journal.generationId !== p.generationId ||
    journal.groupSha256 !== p.groupSha256 ||
    (journal.journalId ?? null) !== p.journalId
  )
    throw Error("Reviewed configuration or journal differs");
  if (context.initialize ? p.journalId !== null || entry.role !== "receiver" : p.journalId === null)
    throw Error("Only a reviewed unbound receiver can initialize");
  if (entry.role === "receiver" ? role !== d.receiverRole : role !== undefined && role !== "publisher") throw Error("Reviewed runtime role differs");
  if (entry.role === "source" ? !group.profiles.some((x) => x.id === memberId && x.processName === entry.processName) : memberId !== undefined)
    throw Error("Reviewed member selection differs");
  return Object.freeze({
    sha256: sha256(bytes),
    file,
    directory: p.directory,
    journalId: p.journalId,
    role: entry.role,
    groupId: p.groupId,
    groupProcessName: group.processName,
    namespace: d.namespace,
    generationId: p.generationId,
    configurationGroupSha256: p.groupSha256,
  });
}
