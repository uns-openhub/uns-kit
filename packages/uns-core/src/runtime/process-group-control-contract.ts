import { z } from "zod";

const name = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/);
export const groupPlacementSchema = z.strictObject({
  controller: name,
  rttNode: name,
  version: name,
  instanceId: name,
});
export type GroupPlacement = z.infer<typeof groupPlacementSchema>;
export const groupInputSchema = z
  .strictObject({
    sources: z.array(groupPlacementSchema).min(2).max(16),
    target: groupPlacementSchema,
  })
  .superRefine((v, c) => {
    const keys = [...v.sources, v.target].map(placementKey);
    if (new Set(keys).size !== keys.length) c.addIssue({ code: "custom", message: "Distinct placements required" });
    if (v.sources.some((s) => s.controller !== v.target.controller || s.rttNode !== v.target.rttNode))
      c.addIssue({
        code: "custom",
        message: "This group contract requires one package on one owning controller",
      });
  });
export type GroupInput = z.infer<typeof groupInputSchema>;
export const placementKey = (p: GroupPlacement): string => [p.controller, p.rttNode, p.version, p.instanceId].join(":");
export { configurationAuthorizationHash as groupHash } from "./process-group-resources.js";
export type GroupMember = {
  id: string;
  processName: string;
  mappingSha256: string;
  dataGroup: string;
};
export const groupMemberCommandSchema = z.strictObject({
  action: z.enum(["inspect", "reserve", "launch", "park", "restore", "release"]),
  placement: groupPlacementSchema,
  operationId: z.string().uuid(),
  role: z.enum(["source", "target"]),
  expectedConfigurationChecksum: z
    .string()
    .regex(/^[a-f0-9]{64}$/)
    .optional(),
  requestId: z.string().uuid().optional(),
});
export type GroupMemberCommand = z.infer<typeof groupMemberCommandSchema>;
