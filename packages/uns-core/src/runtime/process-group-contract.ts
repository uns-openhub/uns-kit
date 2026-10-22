import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import { type MqttChannelConfig, resolveMqttChannel } from "../uns-config/mqtt-channel.js";
import { legacyDescriptorForPackage, legacyEnergentiDescriptor } from "./compatibility/energenti-descriptor-v1.js";
/** Versioned, data-only contract for same-host MQTT journal process groups.
 * Package declarations describe resources; only private operator policies grant authority.
 */
import { readProcessGroupResource, sha256 } from "./process-group-resources.js";

export { legacyDescriptorForPackage, legacyEnergentiDescriptor } from "./compatibility/energenti-descriptor-v1.js";

export const processGroupProtocolVersion = 1 as const;
const identity = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
const selector = z
  .array(
    z
      .string()
      .regex(/^[a-zA-Z][a-zA-Z0-9_-]{0,99}$/)
      .refine((v) => !["__proto__", "constructor", "prototype"].includes(v)),
  )
  .min(1)
  .max(8);
const relative = z
  .string()
  .min(1)
  .max(512)
  .refine((v) => !path.isAbsolute(v) && path.normalize(v) === v && !v.split(/[\\/]/).some((x) => x === ".." || x === ".") && !v.includes("\0"));
export const processGroupRuntimeDescriptorSchema = z
  .strictObject({
    protocolVersion: z.literal(1),
    namespace: identity,
    publisherPath: selector,
    journalPath: selector,
    runtimeRolePath: selector,
    receiverRole: identity,
    defaultDataGroup: identity.optional(),
    match: z.strictObject({ path: selector, value: z.string().min(1).max(100) }).optional(),
    manifests: z
      .array(z.strictObject({ id: identity, file: relative }))
      .min(1)
      .max(64),
  })
  .superRefine((d, c) => {
    if (new Set(d.manifests.map((v) => v.id)).size !== d.manifests.length || new Set(d.manifests.map((v) => v.file)).size !== d.manifests.length)
      c.addIssue({ code: "custom", message: "Distinct group manifests required" });
    const paths = [d.publisherPath, d.journalPath, d.runtimeRolePath].map((v) => v.join("."));
    if (new Set(paths).size !== paths.length || paths.some((p, i) => paths.some((other, j) => i !== j && other.startsWith(p + "."))))
      c.addIssue({ code: "custom", message: "Distinct configuration selectors required" });
  });
export type ProcessGroupRuntimeDescriptor = z.infer<typeof processGroupRuntimeDescriptorSchema> & { legacy?: true };
export function readConfigurationSelector(raw: unknown, keys: readonly string[]): unknown {
  let value: unknown = raw;
  for (const key of keys) {
    if (!value || typeof value !== "object" || Array.isArray(value) || !Object.hasOwn(value, key)) return undefined;
    value = (value as Record<string, unknown>)[key];
  }
  return value;
}
/** Strictly bounded package metadata, no executable application adapter in the Controller. */
export function processGroupDescriptor(packageRoot: string): ProcessGroupRuntimeDescriptor | undefined {
  const file = path.join(packageRoot, "package.json");
  if (!fs.existsSync(file)) return undefined;
  const pkg = JSON.parse(readProcessGroupResource(file).toString());
  const declared = pkg.unsDatahub?.processGroupRuntime;
  if (declared !== undefined) return processGroupRuntimeDescriptorSchema.parse(declared);
  // Frozen bridge for issued capability1/2/3 artifacts; never add future groups here.
  if ([1, 2, 3].includes(pkg.unsDatahub?.processGroupProductionAuthorization)) return legacyDescriptorForPackage(String(pkg.name));
  return undefined;
}
export function normalizeProcessGroupConfiguration(raw: Record<string, unknown>, d: ProcessGroupRuntimeDescriptor) {
  const publisher = readConfigurationSelector(raw, d.publisherPath) as Record<string, unknown> | undefined;
  const role = readConfigurationSelector(raw, d.runtimeRolePath);
  return {
    ...raw,
    uns: raw["uns"] as Record<string, unknown>,
    publisher: publisher ?? {},
    journal: readConfigurationSelector(raw, d.journalPath),
    runtimeRole: role === d.receiverRole ? "admission" : role,
    matches: !d.match || readConfigurationSelector(raw, d.match.path) === d.match.value,
  };
}
export function projectProcessGroupConfiguration(
  raw: Record<string, unknown>,
  d: ProcessGroupRuntimeDescriptor,
  uns: Record<string, unknown>,
  authorizationSha256?: string,
): Record<string, unknown> {
  const projection = structuredClone(raw);
  projection["uns"] = uns;
  let value = projection;
  for (const key of d.publisherPath.slice(0, -1)) {
    const child = value[key];
    if (!child || typeof child !== "object" || Array.isArray(child)) throw Error("Invalid publisher projection");
    value = child as Record<string, unknown>;
  }
  delete value[d.publisherPath.at(-1)!];
  if (authorizationSha256) projection["authorizationSha256"] = authorizationSha256;
  return projection;
}
export function resolvedProcessGroupChannels(raw: Record<string, unknown>, _d?: ProcessGroupRuntimeDescriptor) {
  const infra = (raw["infra"] ?? {}) as MqttChannelConfig;
  return [
    resolveMqttChannel(infra),
    resolveMqttChannel(infra, (raw["input"] ?? {}) as MqttChannelConfig),
    resolveMqttChannel(infra, (raw["output"] ?? {}) as MqttChannelConfig),
  ];
}
export function isLocalPilotPath(value: string, platform = process.platform): boolean {
  return path.resolve(value) === value && (value.startsWith("/private/tmp/") || (platform === "linux" && value.startsWith("/tmp/")));
}
export function isProcessGroupJournalPath(root: string, directory: string, namespace: string): boolean {
  return (
    path.isAbsolute(root) &&
    path.resolve(root) === root &&
    new RegExp(`^${namespace}/admission/[a-z][a-z0-9-]{0,99}$`).test(path.relative(root, directory))
  );
}
export const processGroupJournalSchema = z.strictObject({
  directory: z.string().startsWith("/").max(512),
  journalId: z.string().uuid().optional(),
  minFreeBytes: z
    .number()
    .int()
    .min(1024 * 1024)
    .max(1024 * 1024 * 1024)
    .default(64 * 1024 * 1024),
  groupManifest: z.string().min(1).max(512),
  maxRecords: z.number().int().min(1).max(100000).default(20000),
  maxBytes: z
    .number()
    .int()
    .min(1024)
    .max(256 * 1024 * 1024)
    .default(32 * 1024 * 1024),
});
export type ProcessGroupJournalConfiguration = z.infer<typeof processGroupJournalSchema>;
export interface ProcessGroupJournalProfile {
  processName: string;
  mappingSha256: string;
  dataGroup: string;
}
export interface ProcessGroupJournalTask {
  profile: string;
  payload: string;
}

export function resolvedProcessGroupInputChannel(raw: Record<string, unknown>) {
  return resolveMqttChannel((raw["infra"] ?? {}) as MqttChannelConfig, (raw["input"] ?? {}) as MqttChannelConfig);
}

export function processGroupSourceIdentity(input: {
  host: string;
  port?: number | undefined;
  protocol?: string | undefined;
  hosts?: string[] | undefined;
  servers?: unknown[] | undefined;
}): string {
  return sha256(
    JSON.stringify({
      host: input.host,
      port: input.port ?? 1883,
      protocol: input.protocol ?? "mqtt",
      hosts: input.hosts ?? [],
      servers: input.servers ?? [],
    }),
  );
}
export function processGroupJournalContract(
  sourceIdentity: string,
  config: Pick<ProcessGroupJournalConfiguration, "maxRecords" | "maxBytes">,
  profiles: ProcessGroupJournalProfile[],
): string {
  return JSON.stringify({
    version: 1,
    sourceIdentity,
    maxRecords: config.maxRecords,
    maxBytes: config.maxBytes,
    profiles: profiles
      .map(({ processName, mappingSha256, dataGroup }) => ({ processName, mappingSha256, dataGroup }))
      .sort((a, b) => a.processName.localeCompare(b.processName)),
  });
}
