import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import { readConfigurationSelector } from "./process-group-contract.js";
import { readProcessGroupResource } from "./process-group-resources.js";

const identity = z.string().regex(/^[a-z][a-z0-9-]{0,99}$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const selector = z
  .array(
    z
      .string()
      .regex(/^[a-zA-Z][a-zA-Z0-9_-]{0,99}$/)
      .refine((v) => !["__proto__", "prototype", "constructor"].includes(v)),
  )
  .min(1)
  .max(8);
const MAX_BYTES = 1024 * 1024;

/** Versioned canonical JSON: binary key order, no locale, executable values or accessors. */
export function configurationResourceJson(value: unknown): string {
  let nodes = 0,
    bytes = 0;
  const ancestors = new Set<object>();
  const charge = (size: number) => {
    bytes += size;
    if (bytes > MAX_BYTES) throw Error("Configuration resource exceeds 1 MiB");
  };
  function visit(v: unknown, depth: number): string {
    if (++nodes > 65536 || depth > 32) throw Error("Configuration resource exceeds its structural budget");
    if (v === null || typeof v === "boolean") {
      charge(v === null ? 4 : v ? 4 : 5);
      return JSON.stringify(v);
    }
    if (typeof v === "string") {
      if (Buffer.byteLength(v) > MAX_BYTES) throw Error("Configuration resource string exceeds 1 MiB");
      const encoded = JSON.stringify(v);
      charge(Buffer.byteLength(encoded));
      return encoded;
    }
    if (typeof v === "number" && Number.isFinite(v)) {
      const encoded = JSON.stringify(v);
      charge(encoded.length);
      return encoded;
    }
    if (!v || typeof v !== "object" || ancestors.has(v)) throw Error("Configuration resource must contain finite plain JSON only");
    if (
      Array.isArray(v)
        ? Object.getPrototypeOf(v) !== Array.prototype
        : Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null
    )
      throw Error("Configuration resource must contain plain JSON objects");
    if (Object.getOwnPropertySymbols(v).length) throw Error("Configuration resource cannot contain symbol properties");
    const descriptors = Object.getOwnPropertyDescriptors(v);
    for (const [key, d] of Object.entries(descriptors)) {
      if (Array.isArray(v) && key === "length") continue;
      if (!d.enumerable || !Object.hasOwn(d, "value")) throw Error("Configuration resource cannot contain hidden properties or accessors");
    }
    ancestors.add(v);
    charge(2); // Container brackets/braces.
    let result: string;
    if (Array.isArray(v)) {
      if (v.length > 65536 || Object.keys(v).length !== v.length || Object.keys(v).some((k, i) => k !== String(i)))
        throw Error("Configuration resource arrays must be dense JSON arrays");
      charge(Math.max(0, v.length - 1));
      result = "[" + Array.from({ length: v.length }, (_, i) => visit(descriptors[String(i)]!.value, depth + 1)).join(",") + "]";
    } else
      result =
        "{" +
        Object.keys(v)
          .sort()
          .map((k, i) => {
            if (Buffer.byteLength(k) > MAX_BYTES) throw Error("Configuration resource key exceeds 1 MiB");
            charge(Buffer.byteLength(JSON.stringify(k)) + 1 + (i ? 1 : 0));
            return JSON.stringify(k) + ":" + visit(descriptors[k]!.value, depth + 1);
          })
          .join(",") +
        "}";
    ancestors.delete(v);
    return result;
  }
  const json = visit(value, 0);
  if (Buffer.byteLength(json) > MAX_BYTES) throw Error("Configuration resource exceeds 1 MiB");
  return json;
}

export function configurationResourceHash(value: unknown): string {
  return createHash("sha256").update("uns-configuration-json-v1\n").update(configurationResourceJson(value)).digest("hex");
}

export const configurationGroupProfileSchema = z.strictObject({
  id: identity,
  processName: identity,
  dataGroup: identity,
  resource: z.unknown(),
});
export const configurationGroupSchema = z.strictObject({
  schemaVersion: z.literal(2),
  id: identity,
  inputTopic: z
    .string()
    .min(1)
    .max(512)
    .regex(/^[^#+\0]+$/),
  processName: identity,
  receiverProcessName: identity,
  profiles: z.array(configurationGroupProfileSchema).min(2).max(16),
});
export const configurationGroupDescriptorSchema = z
  .strictObject({
    protocolVersion: z.literal(2),
    namespace: identity,
    publisherPath: selector,
    groupPath: selector,
    memberPath: selector,
    journalPath: selector,
    runtimeRolePath: selector,
    receiverRole: identity,
    match: z.strictObject({ path: selector, value: z.string().min(1).max(100) }).optional(),
  })
  .superRefine((d, context) => {
    const independent = [d.publisherPath, d.journalPath, d.runtimeRolePath].map((p) => p.join("."));
    if (new Set(independent).size !== independent.length || independent.some((p, i) => independent.some((q, j) => i !== j && q.startsWith(p + "."))))
      context.addIssue({ code: "custom", message: "Independent publisher, journal and role selectors required" });
    const parent = d.publisherPath.join(".") + ".",
      group = d.groupPath.join("."),
      member = d.memberPath.join(".");
    if (
      !group.startsWith(parent) ||
      !member.startsWith(parent) ||
      group === member ||
      group.startsWith(member + ".") ||
      member.startsWith(group + ".")
    )
      context.addIssue({ code: "custom", message: "Distinct group/member selectors within the publisher required" });
  });
export type ConfigurationGroupDescriptor = z.infer<typeof configurationGroupDescriptorSchema>;
export type ConfigurationGroup = z.infer<typeof configurationGroupSchema>;
export interface ConfigurationGroupProfile {
  readonly id: string;
  readonly processName: string;
  readonly dataGroup: string;
  readonly resourceSha256: string;
  readonly resource: unknown;
}
export interface ConfigurationGroupSnapshot {
  readonly group: Readonly<ConfigurationGroup>;
  readonly sha256: string;
  readonly profiles: readonly ConfigurationGroupProfile[];
}
const snapshots = new WeakSet<object>();
function freeze<T>(value: T): T {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
export function configurationGroupSnapshot(value: unknown): ConfigurationGroupSnapshot {
  // Bound and copy before schema traversal; caller mutation cannot change a running revision.
  const group = configurationGroupSchema.parse(JSON.parse(configurationResourceJson(value)));
  const names = [group.processName, group.receiverProcessName, ...group.profiles.map((p) => p.processName)];
  if (new Set(names).size !== names.length || new Set(group.profiles.map((p) => p.id)).size !== group.profiles.length)
    throw Error("Configuration group identities overlap");
  const profiles = group.profiles.map((p) => ({ ...p, resourceSha256: configurationResourceHash(p.resource) }));
  const result = freeze({ group, sha256: configurationResourceHash(group), profiles });
  snapshots.add(result);
  return result;
}
export function assertConfigurationGroupSnapshot(value: ConfigurationGroupSnapshot): void {
  if (!snapshots.has(value)) throw Error("Exact SDK-issued configuration group snapshot required");
}

/** Optional data-only capability; packages without it keep their existing behavior. */
export function configurationGroupDescriptor(packageRoot: string): ConfigurationGroupDescriptor | undefined {
  const file = path.join(packageRoot, "package.json");
  // Ordinary Node/Python deployments do not opt in by existing on the host.
  if (!fs.existsSync(file)) return undefined;
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  let bytes: Buffer;
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > MAX_BYTES) throw Error("Invalid installed runtime metadata");
    bytes = fs.readFileSync(fd);
    if (bytes.length > MAX_BYTES) throw Error("Installed runtime metadata is too large");
  } finally {
    fs.closeSync(fd);
  }
  const pkg = JSON.parse(bytes.toString());
  const value = pkg.unsDatahub?.processGroupConfigurationRuntime;
  if (value !== undefined && !readProcessGroupResource(file).equals(bytes)) throw Error("Installed runtime metadata changed during discovery");
  return value === undefined ? undefined : configurationGroupDescriptorSchema.parse(value);
}
export function selectedConfigurationGroup(
  packageRoot: string,
  raw: Record<string, unknown>,
):
  | {
      descriptor: ConfigurationGroupDescriptor;
      snapshot: ConfigurationGroupSnapshot;
    }
  | undefined {
  const descriptor = configurationGroupDescriptor(packageRoot);
  if (!descriptor) return undefined;
  const value = readConfigurationSelector(raw, descriptor.groupPath);
  if (value === undefined) return undefined;
  if (descriptor.match && readConfigurationSelector(raw, descriptor.match.path) !== descriptor.match.value)
    throw Error("Configuration group capability selector does not match");
  return { descriptor, snapshot: configurationGroupSnapshot(value) };
}

export const configurationGroupJournalSchema = z.strictObject({
  directory: z.string().startsWith("/").max(512),
  journalId: z.string().uuid().optional(),
  generationId: identity,
  groupSha256: digest,
  minFreeBytes: z
    .number()
    .int()
    .min(1024 * 1024)
    .max(1024 * 1024 * 1024)
    .default(64 * 1024 * 1024),
  maxRecords: z.number().int().min(1).max(100000).default(20000),
  maxBytes: z
    .number()
    .int()
    .min(1024)
    .max(256 * 1024 * 1024)
    .default(32 * 1024 * 1024),
});
export type ConfigurationGroupJournal = z.infer<typeof configurationGroupJournalSchema>;
export function configurationGroupJournalContract(
  snapshot: ConfigurationGroupSnapshot,
  sourceIdentity: string,
  config: ConfigurationGroupJournal,
  profiles: readonly { processName: string; resourceSha256: string; dataGroup: string }[],
): string {
  assertConfigurationGroupSnapshot(snapshot);
  const journal = configurationGroupJournalSchema.parse(config);
  if (journal.groupSha256 !== snapshot.sha256 || !digest.safeParse(sourceIdentity).success)
    throw Error("Configuration group journal revision differs");
  const expected = snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, resourceSha256, dataGroup }));
  const normalize = (v: typeof expected) => [...v].sort((a, b) => (a.processName < b.processName ? -1 : a.processName > b.processName ? 1 : 0));
  if (configurationResourceHash(normalize([...profiles])) !== configurationResourceHash(normalize(expected)))
    throw Error("Configuration group journal profiles differ");
  return configurationResourceJson({
    version: 2,
    generationId: journal.generationId,
    groupSha256: snapshot.sha256,
    sourceIdentity,
    maxRecords: journal.maxRecords,
    maxBytes: journal.maxBytes,
    profiles: normalize(expected),
  });
}
