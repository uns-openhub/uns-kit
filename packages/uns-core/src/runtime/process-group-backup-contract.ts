import { z } from "zod";
import contract from "./process-group-backup-contract.json" with { type: "json" };

/** SDK-owned fixed resource budget; hosts never accept it from an operation request. */
export const processGroupBackupLimits = Object.freeze(z.strictObject({
  schemaVersion: z.literal(1),
  maximumJournals: z.number().int().min(1).max(128),
  maximumManifestBytes: z.literal(262144),
  maximumSnapshotBytes: z.literal(536870912),
  maximumCapsuleBytes: z.literal(1073741824),
}).parse(contract));

/** Public coverage only; identities, checkpoints and payloads remain encrypted. */
export const processGroupStateBackupSchema = z.strictObject({
  schemaVersion: z.literal(1),
  state: z.enum(["included", "empty"]),
  rootSha256: z.string().regex(/^[a-f0-9]{64}$/),
  journals: z.number().int().min(0).max(processGroupBackupLimits.maximumJournals),
  bytes: z.number().int().positive().max(processGroupBackupLimits.maximumCapsuleBytes),
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
}).refine(v => v.state === "included" ? v.journals > 0 : v.journals === 0);
