import path from "node:path";
import { z } from "zod";

import { processGroupConfigurationSelectorSchema as selector } from "./process-group-contract.js";
import { groupPlacementSchema, placementKey } from "./process-group-control-contract.js";

const identity = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const absolute = z
  .string()
  .min(1)
  .max(512)
  .refine((v) => path.isAbsolute(v) && path.resolve(v) === v);
const relative = z
  .string()
  .min(1)
  .max(512)
  .refine((v) => !path.isAbsolute(v) && path.normalize(v) === v && !v.split(/[\\/]/).some((p) => p === ".." || p === ".") && !v.includes("\0"));

/** Package data, never launch authority. Selectors are reusable across applications. */
const resource = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("file"),
    manifestPath: selector,
    manifests: z
      .array(z.strictObject({ id: identity, file: relative }))
      .min(1)
      .max(64),
  }),
  z.strictObject({ kind: z.literal("configuration"), groupPath: selector }),
]);
export const processGroupDirectDescriptorSchema = z
  .strictObject({
    protocolVersion: z.literal(1),
    namespace: identity,
    publisherPath: selector,
    journalPath: selector,
    runtimeRolePath: selector,
    directRole: identity,
    resources: z.array(resource).min(1).max(2),
    match: z.strictObject({ path: selector, value: z.string().min(1).max(100) }).optional(),
  })
  .superRefine((v, c) => {
    if (new Set(v.resources.map((r) => r.kind)).size !== v.resources.length)
      c.addIssue({ code: "custom", message: "Distinct direct resource kinds required" });
    for (const r of v.resources) {
      const resourcePath = r.kind === "file" ? r.manifestPath : r.groupPath;
      if (resourcePath.length <= v.publisherPath.length || !v.publisherPath.every((key, i) => resourcePath[i] === key))
        c.addIssue({ code: "custom", message: "Direct resource selector must be a publisher leaf" });
      if (
        r.kind === "file" &&
        (new Set(r.manifests.map((m) => m.id)).size !== r.manifests.length || new Set(r.manifests.map((m) => m.file)).size !== r.manifests.length)
      )
        c.addIssue({ code: "custom", message: "Distinct direct manifest resources required" });
    }
    const paths = [v.publisherPath, v.journalPath, v.runtimeRolePath].map((p) => p.join("."));
    if (new Set(paths).size !== paths.length || paths.some((p, i) => paths.some((q, j) => i !== j && q.startsWith(p + "."))))
      c.addIssue({ code: "custom", message: "Distinct lifecycle selectors required" });
  });
export type ProcessGroupDirectDescriptor = z.infer<typeof processGroupDirectDescriptorSchema>;
export const processGroupDirectInputSchema = z
  .strictObject({
    protocolVersion: z.literal(1),
    source: z.strictObject({ publisher: groupPlacementSchema, receiver: groupPlacementSchema }),
    target: groupPlacementSchema,
  })
  .superRefine((v, c) => {
    const all = [v.source.publisher, v.source.receiver, v.target];
    if (
      new Set(all.map(placementKey)).size !== 3 ||
      all.some((p) => p.controller !== all[0]!.controller || p.rttNode !== all[0]!.rttNode) ||
      v.source.publisher.version !== v.source.receiver.version
    )
      c.addIssue({ code: "custom", message: "Three distinct placements with one owning controller/package and one source-pair version required" });
  });
export type ProcessGroupDirectInput = z.infer<typeof processGroupDirectInputSchema>;
export const processGroupDirectRequestSchema = z
  .strictObject({
    protocolVersion: z.literal(1),
    controllerName: groupPlacementSchema.shape.controller,
    id: z.string().uuid(),
    action: z.enum(["preview", "prepare", "get", "execute", "rollback", "close-recovery"]),
    input: processGroupDirectInputSchema.optional(),
    expectedFingerprint: digest.optional(),
    expectedRevision: z.number().int().positive().optional(),
  })
  .superRefine((v, c) => {
    if (
      (["preview", "prepare"].includes(v.action) && !v.input) ||
      (["prepare", "execute", "rollback", "close-recovery"].includes(v.action) && !v.expectedFingerprint) ||
      (["execute", "rollback", "close-recovery"].includes(v.action) && !v.expectedRevision) ||
      (v.input && v.input.target.controller !== v.controllerName)
    )
      c.addIssue({ code: "custom", message: "Exact transition identity, owning member and reviewed revision required" });
  });
export type ProcessGroupDirectRequest = z.infer<typeof processGroupDirectRequestSchema>;
export const retiredJournalReferenceSchema = z
  .strictObject({
    directory: absolute,
    journalId: z.string().uuid(),
    journalContractSha256: digest,
    generationId: identity,
    groupSha256: digest.optional(),
    contractVersion: z.union([z.literal(1), z.literal(2)]),
    maxRecords: z.number().int().min(1).max(100000),
    maxBytes: z
      .number()
      .int()
      .min(1024)
      .max(256 * 1024 * 1024),
    minFreeBytes: z
      .number()
      .int()
      .min(0)
      .max(1024 * 1024 * 1024),
    profiles: z
      .array(z.strictObject({ processName: identity, mappingSha256: digest, dataGroup: identity }))
      .min(2)
      .max(16),
  })
  .superRefine((v, c) => {
    if (v.contractVersion === 2 && !v.groupSha256) c.addIssue({ code: "custom", message: "Configuration generation identity required" });
    if (new Set(v.profiles.map((p) => p.processName)).size !== v.profiles.length)
      c.addIssue({ code: "custom", message: "Distinct retained profiles required" });
  });
export type RetiredJournalReference = z.infer<typeof retiredJournalReferenceSchema>;
const member = z.strictObject({
  placement: groupPlacementSchema,
  processName: identity,
  configurationSha256: digest,
  artifactSha256: digest,
});
export const processGroupRetirementPolicySchema = z
  .strictObject({
    schemaVersion: z.literal(5),
    kind: z.literal("uns-process-group-direct-retirement"),
    protocolVersion: z.literal(1),
    operationId: z.string().uuid(),
    packageName: z.string().min(1).max(214),
    namespace: identity,
    groupId: identity,
    controllerName: groupPlacementSchema.shape.controller,
    stateRoot: absolute,
    recovery: z.enum(["open", "closed"]),
    journal: retiredJournalReferenceSchema,
    sourcePolicyJson: z.string().min(1).max(32768),
    sourcePolicySha256: digest,
    source: z.strictObject({
      publisher: member.extend({ policyConfigurationSha256: digest }),
      receiver: member.extend({ policyConfigurationSha256: digest }),
    }),
    target: member.extend({ packageRoot: absolute, descriptorSha256: digest, resourceSha256: digest, channelsSha256: digest }),
  })
  .superRefine((v, c) => {
    if (
      v.target.processName !== v.source.publisher.processName ||
      v.source.receiver.processName === v.target.processName ||
      v.source.publisher.placement.controller !== v.controllerName ||
      v.source.receiver.placement.controller !== v.controllerName ||
      v.target.placement.controller !== v.controllerName ||
      v.journal.profiles.some((p) => [v.target.processName, v.source.receiver.processName].includes(p.processName))
    )
      c.addIssue({ code: "custom", message: "Exact paired and direct ownership identities required" });
  });
export type ProcessGroupRetirementPolicy = z.infer<typeof processGroupRetirementPolicySchema>;
