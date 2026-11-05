import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import { available, maintenanceLease, runtimeLease } from "./retained-store-storage.mjs";
import { inspectRetainedCopy, applyRetainedCopy, type RetainedCopyFingerprint } from "./retained-store-copy.mjs";
import { retainedStoreJob } from "./retained-store-job.mjs";
const copyLimits = () => JSON.parse(fs.readFileSync(new URL("./retained-store-storage-contract.json", import.meta.url), "utf8"));

const segment = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/);
const relative = z
  .string()
  .min(1)
  .max(256)
  .refine((value) => !path.isAbsolute(value) && !value.includes("\\") && value.split("/").every((p) => /^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(p)));
export const RETAINED_STORE_MANIFEST = "retained-store.manifest.json";
export const RETAINED_STORE_BINDING = "retained-store-binding.json";
export const RETAINED_STORE_GUARD = "retained-store-guard.json";
export const retainedStoreManifestSchema = z
  .object({
    protocol: z.literal(1),
    storageProtocol: z.literal(1).optional(),
    format: segment,
    resources: z
      .array(
        z
          .object({
            id: segment,
            path: relative,
            kind: z.enum(["file", "directory"]),
            required: z.boolean(),
            backup: z.enum(["payload", "metadata"]).optional(),
          })
          .strict(),
      )
      .min(1)
      .max(16),
    legacyVersions: z.array(segment).max(32).default([]),
  })
  .strict()
  .superRefine((m, ctx) => {
    if (new Set(m.resources.map((r) => r.id)).size !== m.resources.length) ctx.addIssue({ code: "custom", message: "Duplicate resource ID" });
    for (const r of m.resources) {
      if (["store.json", "store-resources.json", "store-operation.json", "store-lease.sqlite", "store-returned.json"].includes(r.id))
        ctx.addIssue({ code: "custom", message: "Reserved store metadata resource" });
      if (["config.json", "controller-state.json", RETAINED_STORE_BINDING, RETAINED_STORE_GUARD].includes(r.path))
        ctx.addIssue({ code: "custom", message: "Reserved instance resource" });
      if (m.resources.some((o) => o !== r && (o.path === r.path || o.path.startsWith(r.path + "/"))))
        ctx.addIssue({ code: "custom", message: "Overlapping resources" });
    }
  });
export type RetainedStoreManifest = z.infer<typeof retainedStoreManifestSchema>;
export type StoreOwner = { rttNode: string; version: string; instanceId: string };
export type StoreBinding = {
  protocol: 1;
  storeId: string;
  generation: string;
  operationId: string;
  directory: string;
  format: string;
  configurationDigest: string;
  owner: StoreOwner;
  maintenance: true;
};
export type ResourceMove = { id: string; relative: string; kind: "file" | "directory"; device: number; inode: number; content?: RetainedCopyFingerprint };
export type StoreMovePlan = {
  storeId: string;
  generation: string;
  format: string;
  source: string;
  directory: string;
  slots: RetainedStoreManifest["resources"];
  moves: ResourceMove[];
  transfer?: "copy-verified";
  originalMoves?: ResourceMove[];
};
const ownerSchema = z.object({ rttNode: segment, version: segment, instanceId: segment }).strict();
const bindingSchema = z
  .object({
    protocol: z.literal(1),
    storeId: z.uuid(),
    generation: z.uuid(),
    operationId: z.uuid(),
    directory: z.string().max(4096),
    format: segment,
    configurationDigest: z.string().regex(/^[a-f0-9]{64}$/),
    owner: ownerSchema,
    maintenance: z.literal(true),
  })
  .strict();
export function retainedStoreDigest(value: unknown): string {
  const stable = (v: any): any =>
    Array.isArray(v)
      ? v.map(stable)
      : v && typeof v === "object"
        ? Object.fromEntries(
            Object.keys(v)
              .sort()
              .map((k) => [k, stable(v[k])]),
          )
        : v;
  return createHash("sha256")
    .update(JSON.stringify(stable(value)))
    .digest("hex");
}
export function retainedStoreEntry(file: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(file);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return;
    throw e;
  }
}
export function assertRetainedStorePath(file: string): void {
  if (!path.isAbsolute(file) || path.normalize(file) !== file) throw Error("Unnormalized retained store path");
  for (let p = file; p !== path.dirname(p); p = path.dirname(p)) {
    const s = retainedStoreEntry(p);
    if (s?.isSymbolicLink()) throw Error("Linked retained store path");
  }
}
export function readRetainedStoreJson(file: string): unknown {
  assertRetainedStorePath(file);
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const s = fs.fstatSync(fd);
    if (!s.isFile() || s.nlink !== 1 || s.size > 128 * 1024 || s.uid !== process.getuid?.()) throw Error("Unsafe retained store metadata");
    return JSON.parse(fs.readFileSync(fd, "utf8"));
  } finally {
    fs.closeSync(fd);
  }
}
export function syncRetainedDirectory(directory: string): void {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
export function writeRetainedStoreJson(file: string, value: unknown): void {
  assertRetainedStorePath(file);
  const temporary = file + "." + randomUUID() + ".tmp";
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > 128 * 1024) throw Error("Retained store metadata too large");
  try {
    const fd = fs.openSync(temporary, "wx", 0o600);
    try {
      fs.writeFileSync(fd, text);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, file);
    syncRetainedDirectory(path.dirname(file));
  } finally {
    if (retainedStoreEntry(temporary)) fs.unlinkSync(temporary);
  }
}
export function readRetainedStoreManifest(packageDirectory: string): RetainedStoreManifest {
  return retainedStoreManifestSchema.parse(readRetainedStoreJson(path.join(packageDirectory, RETAINED_STORE_MANIFEST)));
}
export function prepareRetainedStoreMove(source: string, root: string, storeId: string, manifest: RetainedStoreManifest, transfer?: "copy-verified", deferContent=false): StoreMovePlan {
  z.uuid().parse(storeId);
  retainedStoreManifestSchema.parse(manifest);
  assertRetainedStorePath(source);
  assertRetainedStorePath(root);
  const directory = path.join(root, storeId);
  if (retainedStoreEntry(directory)) throw Error("Retained store destination exists");
  const existingRoot = retainedStoreEntry(root);
  if (existingRoot && (!existingRoot.isDirectory() || existingRoot.uid !== process.getuid?.() || existingRoot.mode & 0o077))
    throw Error("Retained root must be private and owned");
  const rootStat = existingRoot ?? fs.statSync(path.dirname(root)),
    sourceStat = fs.statSync(source);
  if (root === source || root.startsWith(source + path.sep) || source.startsWith(root + path.sep)) throw Error("Nested retained store roots");
  if (transfer !== undefined && transfer !== "copy-verified") throw Error("Unsupported retained transfer");
  if (!transfer && rootStat.dev !== sourceStat.dev) throw Error("Retained store requires same filesystem; review an explicit verified copy");
  const moves: ResourceMove[] = [];
  for (const resource of manifest.resources) {
    const file = path.join(source, resource.path);
    assertRetainedStorePath(file);
    const s = retainedStoreEntry(file);
    if (!s) {
      if (resource.required) throw Error("Required retained resource missing");
      continue;
    }
    if (
      s.uid !== process.getuid?.() ||
      (!transfer && s.dev !== rootStat.dev) ||
      s.isSymbolicLink() ||
      (resource.kind === "file" ? !s.isFile() || s.nlink !== 1 : !s.isDirectory())
    )
      throw Error("Unsafe retained resource");
    moves.push({ id: resource.id, relative: resource.path, kind: resource.kind, device: s.dev, inode: s.ino,
      ...(transfer&&!deferContent ? {content:inspectRetainedCopy(file,resource.kind,copyLimits())} : {}) });
  }
  if(transfer && (moves.reduce((n,m)=>n+(m.content?.bytes??0),0)>copyLimits().maximumBytes || moves.reduce((n,m)=>n+(m.content?.entries??0),0)>copyLimits().maximumEntries))throw Error("Retained copy aggregate limit");
  return { storeId, generation: randomUUID(), format: manifest.format, source, directory, slots: manifest.resources, moves, ...(transfer?{transfer}:{}) };
}
function matches(file: string, move: ResourceMove): boolean {
  assertRetainedStorePath(file);
  const s = retainedStoreEntry(file);
  return (
    !!s &&
    !s.isSymbolicLink() &&
    s.dev === move.device &&
    s.ino === move.inode &&
    s.uid === process.getuid?.() &&
    (move.kind === "file" ? s.isFile() && s.nlink === 1 : s.isDirectory())
  );
}
/** Caller durably records the complete plan and holds its resource reservation first.
 * Each rename is atomic; the multi-resource operation is deliberately resumable. */
export function applyRetainedStoreMove(plan: StoreMovePlan, direction: "adopt" | "return", afterMove?: (id: string) => void, onFile?: (file:string)=>void, validateOnly=false): void {
  const resourceMove = z.object({id:segment,relative,kind:z.enum(["file","directory"]),device:z.number().int().nonnegative(),inode:z.number().int().nonnegative(),content:z.object({digest:z.string().regex(/^[a-f0-9]{64}$/),bytes:z.number().int().nonnegative().max(copyLimits().maximumBytes),entries:z.number().int().positive().max(copyLimits().maximumEntries)}).strict().optional()}).strict();
  z.object({
    storeId: z.uuid(),
    generation: z.uuid(),
    format: segment,
    source: z.string().max(4096),
    directory: z.string().max(4096),
    slots: z
      .array(
        z
          .object({
            id: segment,
            path: relative,
            kind: z.enum(["file", "directory"]),
            required: z.boolean(),
            backup: z.enum(["payload", "metadata"]).optional(),
          })
          .strict(),
      )
      .max(16),
    moves:z.array(resourceMove).max(16),
    transfer:z.literal("copy-verified").optional(),
    originalMoves:z.array(resourceMove).max(16).optional(),
  })
    .strict()
    .parse(plan);
  z.enum(["adopt", "return"]).parse(direction);
  const manifest = retainedStoreManifestSchema.parse({ protocol: 1, format: plan.format, resources: plan.slots });
  if (plan.moves.some((move) => !manifest.resources.some((slot) => slot.id === move.id && slot.path === move.relative && slot.kind === move.kind)))
    throw Error("Retained move differs from declared resources");
  if (
    new Set(plan.moves.map((m) => m.id)).size !== plan.moves.length ||
    plan.moves.some((m) => plan.moves.some((o) => o !== m && (m.relative === o.relative || o.relative.startsWith(m.relative + "/"))))
  )
    throw Error("Overlapping retained moves");
  if (plan.directory === plan.source || plan.directory.startsWith(plan.source + path.sep) || plan.source.startsWith(plan.directory + path.sep))
    throw Error("Nested retained move");
  assertRetainedStorePath(plan.source);
  assertRetainedStorePath(plan.directory);
  if (!retainedStoreEntry(plan.directory)) {
    fs.mkdirSync(plan.directory, { mode: 0o700 });
    syncRetainedDirectory(path.dirname(plan.directory));
  }
  const directoryStat = fs.lstatSync(plan.directory);
  if (!directoryStat.isDirectory() || directoryStat.uid !== process.getuid?.() || directoryStat.mode & 0o077)
    throw Error("Unsafe retained store directory");
  if(plan.transfer){
    if(plan.moves.some(m=>!m.content) || plan.originalMoves?.some(m=>!m.content))throw Error("Verified copy requires reviewed content fingerprints");
    for(const entries of [plan.moves,plan.originalMoves??[]])if(entries.reduce((n,m)=>n+(m.content?.bytes??0),0)>copyLimits().maximumBytes||entries.reduce((n,m)=>n+(m.content?.entries??0),0)>copyLimits().maximumEntries)throw Error("Retained copy aggregate limit");
    if((plan.originalMoves??[]).some(m=>!manifest.resources.some(r=>r.id===m.id&&r.path===m.relative&&r.kind===m.kind)))throw Error("Original copy differs from declared resources");
    if(!validateOnly)applyRetainedCopy(plan,direction,copyLimits(),afterMove,onFile); return;
  }
  if(plan.originalMoves)throw Error("Original copy receipt requires verified transfer");
  for (const move of plan.moves) {
    const legacy = path.join(plan.source, move.relative),
      stable = path.join(plan.directory, move.id);
    const [from, to] = direction === "adopt" ? [legacy, stable] : [stable, legacy];
    if (!retainedStoreEntry(from) && matches(to, move)) continue;
    if (!matches(from, move) || retainedStoreEntry(to)) throw Error("Retained resource changed or destination is occupied");
    fs.renameSync(from, to);
    syncRetainedDirectory(path.dirname(from));
    syncRetainedDirectory(path.dirname(to));
    if (!matches(to, move)) throw Error("Retained resource identity changed");
    afterMove?.(move.id);
  }
}
export function writeRetainedStoreBinding(instanceDirectory: string, binding: StoreBinding): void {
  bindingSchema.parse(binding);
  writeRetainedStoreJson(path.join(instanceDirectory, RETAINED_STORE_BINDING), binding);
}
export function loadRetainedStoreBinding(instanceDirectory: string, operationId?: string): StoreBinding | undefined {
  const file = path.join(instanceDirectory, RETAINED_STORE_BINDING);
  if (!retainedStoreEntry(file)) return;
  const s = fs.statSync(file);
  if (s.mode & 0o077) throw Error("Retained store binding must be private");
  const binding = bindingSchema.parse(readRetainedStoreJson(file));
  assertRetainedStorePath(binding.directory);
  const runtimeRoot = process.env["UNS_RETAINED_STORE_ROOT"]?.trim();
  if (runtimeRoot && path.dirname(binding.directory) !== runtimeRoot) throw Error("Retained binding does not match the configured Runtime root");
  const directoryStat = retainedStoreEntry(binding.directory);
  if (!directoryStat?.isDirectory() || directoryStat.uid !== process.getuid?.() || directoryStat.mode & 0o077)
    throw Error("Unsafe retained store directory");
  available(path.dirname(binding.directory));
  if (
    retainedStoreEntry(path.join(binding.directory, "store-operation.json")) &&
    (readRetainedStoreJson(path.join(binding.directory, "store-operation.json")) as any).operationId !== operationId
  )
    throw Error("Retained store operation is incomplete");
  const meta = bindingSchema.parse(readRetainedStoreJson(path.join(binding.directory, "store.json")));
  if (retainedStoreDigest(meta) !== retainedStoreDigest(binding)) throw Error("Retained store owner or generation changed");
  return binding;
}
/** No application payload/schema inspection is performed by this helper. */
export function resolveRetainedStoreResources(
  instanceDirectory: string,
  manifest: RetainedStoreManifest,
  expected?: Partial<StoreOwner>,
): { binding?: StoreBinding; resources: Record<string, string> } {
  retainedStoreManifestSchema.parse(manifest);
  if (retainedStoreEntry(path.join(instanceDirectory, RETAINED_STORE_GUARD))) throw Error("Retained store operation has fenced this instance");
  const binding = loadRetainedStoreBinding(instanceDirectory);
  if (
    binding &&
    expected &&
    Object.entries(expected).some(([k, v]) =>
      k === "version" ? binding.owner.version.replace(/^v/, "") !== String(v).replace(/^v/, "") : (binding.owner as any)[k] !== v,
    )
  )
    throw Error("Retained binding does not name this runtime");
  if (binding && binding.format !== manifest.format) throw Error("Unsupported retained store format");
  if (binding) {
    if (manifest.storageProtocol !== 1) throw Error("Bound retained stores require the shared storage protocol");
    const declaration = retainedStoreManifestSchema.parse(readRetainedStoreJson(path.join(binding.directory, "store-resources.json")));
    if (
      declaration.storageProtocol !== 1 ||
      declaration.format !== manifest.format ||
      retainedStoreDigest(declaration.resources) !== retainedStoreDigest(manifest.resources)
    )
      throw Error("Retained store resource declaration changed");
    for (const resource of manifest.resources)
      if (resource.required && !retainedStoreEntry(path.join(binding.directory, resource.id))) throw Error("Required retained resource missing");
  }
  return {
    binding,
    resources: Object.fromEntries(
      manifest.resources.map((r) => [r.id, binding ? path.join(binding.directory, r.id) : path.join(instanceDirectory, r.path)]),
    ),
  };
}

export const retainedStoreSelectionSchema = z
  .object({
    requestId: z.uuid(),
    rttNode: segment,
    sourceVersion: segment,
    sourceInstanceId: segment,
    targetVersion: segment,
    targetInstanceId: segment,
    transfer:z.literal("copy-verified").optional(),
  })
  .strict()
  .refine((x) => x.sourceVersion !== x.targetVersion || x.sourceInstanceId !== x.targetInstanceId, "Source and target must differ");
export const retainedStoreRequestSchema = z.discriminatedUnion("action", [
  z.object({ action: z.literal("review"), selection: retainedStoreSelectionSchema }).strict(),
  z.object({ action: z.literal("get"), id: z.uuid() }).strict(),
  z.object({ action: z.enum(["adopt", "return"]), id: z.uuid(), reviewedChecksum: z.string().regex(/^[a-f0-9]{64}$/) }).strict(),
]);
export type RetainedStoreRequest = z.infer<typeof retainedStoreRequestSchema>;

/** Capture mutable control files from the current stopped owner before persisting return intent. */
export function prepareRetainedStoreReturn(plan: StoreMovePlan,deferContent=false): StoreMovePlan {
  const moves: ResourceMove[] = [];
  if(plan.transfer){
    for(const slot of plan.slots){
      const stable=path.join(plan.directory,slot.id),legacy=path.join(plan.source,slot.path),a=retainedStoreEntry(stable),b=retainedStoreEntry(legacy);
      if(a){assertRetainedStorePath(stable);moves.push({id:slot.id,relative:slot.path,kind:slot.kind,device:a.dev,inode:a.ino,...(!deferContent?{content:inspectRetainedCopy(stable,slot.kind,copyLimits())}:{})});}
      else if(b){const original=plan.moves.find(m=>m.id===slot.id);if(!original||!matches(legacy,original)||(!deferContent&&retainedStoreDigest(inspectRetainedCopy(legacy,slot.kind,copyLimits()))!==retainedStoreDigest(original.content)))throw Error("Original retained copy changed");}
      else if(slot.required)throw Error("Current retained resource missing");
    }
    return{...plan,moves,originalMoves:plan.moves};
  }
  for (const slot of plan.slots) {
    const stable = path.join(plan.directory, slot.id),
      legacy = path.join(plan.source, slot.path);
    assertRetainedStorePath(stable);
    assertRetainedStorePath(legacy);
    const a = retainedStoreEntry(stable),
      b = retainedStoreEntry(legacy);
    if (a && b) throw Error("Return destination is occupied");
    if (a) {
      if (a.uid !== process.getuid?.() || a.isSymbolicLink() || (slot.kind === "file" ? !a.isFile() || a.nlink !== 1 : !a.isDirectory()))
        throw Error("Unsafe current retained resource");
      moves.push({ id: slot.id, relative: slot.path, kind: slot.kind, device: a.dev, inode: a.ino });
    } else if (b) {
      const original = plan.moves.find((m) => m.id === slot.id);
      if (!original || !matches(legacy, original)) throw Error("Return source identity changed");
      moves.push(original);
    } else if (slot.required) throw Error("Current retained resource missing");
  }
  return { ...plan, moves };
}

export function retainedStoreRoot(defaultBase: string, configured?: string): string {
  const root = configured?.trim() || path.join(defaultBase, "retained-stores");
  assertRetainedStorePath(root);
  return root;
}
export function openRetainedStoreLease(binding: StoreBinding): { close: () => void } {
  return runtimeLease(binding);
}
export function openRetainedStoreMaintenance(directory: string): () => void {
  return maintenanceLease(directory);
}
export async function prepareRetainedStoreMoveAsync(source:string,root:string,id:string,manifest:RetainedStoreManifest,transfer?:"copy-verified"):Promise<StoreMovePlan>{
 const plan=prepareRetainedStoreMove(source,root,id,manifest,transfer,!!transfer);
 if(transfer)for(const move of plan.moves)move.content=await retainedStoreJob({action:"inspect",path:path.join(source,move.relative),kind:move.kind,limits:copyLimits()}) as RetainedCopyFingerprint;
 return plan;
}
export async function prepareRetainedStoreReturnAsync(plan:StoreMovePlan):Promise<StoreMovePlan>{
 const back=prepareRetainedStoreReturn(plan,!!plan.transfer);
 if(plan.transfer)for(const move of back.moves)move.content=await retainedStoreJob({action:"inspect",path:path.join(plan.directory,move.id),kind:move.kind,limits:copyLimits()}) as RetainedCopyFingerprint;
 return back;
}
export async function applyRetainedStoreMoveAsync(plan:StoreMovePlan,direction:"adopt"|"return",afterMove?:(id:string)=>void):Promise<void>{
 if(!plan.transfer){applyRetainedStoreMove(plan,direction,afterMove);return;}
 applyRetainedStoreMove(plan,direction,undefined,undefined,true);
 await retainedStoreJob({action:"apply",plan,direction,limits:copyLimits()},afterMove);
}
export const retainedStoreBackupSchema = z
  .object({
    schemaVersion: z.literal(1),
    state: z.enum(["included", "excluded", "empty"]),
    rootSha256: z.string().regex(/^[a-f0-9]{64}$/),
    stores: z.number().int().min(0).max(32),
    files: z.number().int().min(0).max(100000),
    bytes: z.number().int().positive().max(68753031168),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
  })
  .strict();
