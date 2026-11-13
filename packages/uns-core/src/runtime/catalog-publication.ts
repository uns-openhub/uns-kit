import { z } from "zod";

import { operatorDigest, operatorJson, operatorRequestSchema } from "./operator-api.js";

export const CATALOG_PUBLICATION_PROTOCOL = 1;
export const CATALOG_SOURCE_SCOPE = "catalog:sources:read";
export const CATALOG_MANIFEST_FIELD = "catalogAccess";
const id = z
  .string()
  .min(1)
  .max(120)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/);
const topic = z
  .string()
  .min(1)
  .max(512)
  .regex(/^[A-Za-z0-9-]+(?:\/[A-Za-z0-9-]+)*$/)
  .refine((value) => !value.split("/").some((part) => part === "." || part === ".."));
const localPath = z
  .string()
  .min(1)
  .max(1024)
  .regex(/^\/api\/[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_.-]+)*$/)
  .refine((value) => !value.split("/").some((part) => part === "." || part === ".."));
export class CatalogAccessError extends Error {
  constructor(
    public readonly code: string,
    public readonly status = 400,
  ) {
    super(code);
  }
}
const historySource = z
  .object({
    id,
    kind: z.literal("history"),
    topic,
    maxWindowMs: z
      .number()
      .int()
      .min(1000)
      .max(31 * 86400000)
      .default(7 * 86400000),
    maxRows: z.number().int().min(1).max(27000).default(1000),
    fromParameter: id.default("from"),
    toParameter: id.default("to"),
  })
  .strict();
const operation = z
  .object({
    id,
    title: z.string().min(1).max(160),
    method: z.literal("GET"),
    path: localPath,
    mode: z.enum(["caller", "approved-service"]),
    requestSchema: z.record(z.string(), z.unknown()),
    responseSchema: z.record(z.string(), z.unknown()),
    sources: z.array(historySource).min(1).max(16),
    timeoutMs: z.number().int().min(500).max(30000).default(10000),
    maxResponseBytes: z.number().int().min(256).max(1048576).default(262144),
  })
  .strict();
const manifest = z.object({ protocol: z.literal(1), operations: z.array(operation).min(1).max(32) }).strict();
export type CatalogOperationInput = z.input<typeof operation>;
export type CatalogOperation = z.output<typeof operation>;
export type CatalogHistorySource = z.output<typeof historySource>;
export type CatalogPublicationManifest = z.output<typeof manifest>;
export const catalogAccessDigest = operatorDigest;
/** Catalog routes use the same UNS resource path as SDK endpoint guards. */
export const catalogOperationResource = (path: string): string => path.replace(/^\/api\//, "");

/** Inspection consumes bounded JSON data, never application JavaScript. */
export function parseCatalogPublicationManifest(input: unknown): CatalogPublicationManifest {
  try {
    operatorJson(input);
    const parsed = manifest.parse(input),
      ids = new Set<string>(),
      paths = new Set<string>();
    for (const op of parsed.operations) {
      if (ids.has(op.id) || paths.has(op.path)) throw Error("duplicate operation");
      ids.add(op.id);
      paths.add(op.path);
      op.requestSchema = catalogSchema(op.requestSchema);
      op.responseSchema = catalogSchema(op.responseSchema);
      if (op.requestSchema["type"] !== "object") throw Error("request must be an object");
      if (Object.values((op.requestSchema["properties"] as Record<string, Record<string, unknown>>) ?? {}).some((p) => p["type"] !== "string"))
        throw Error("GET parameters must be strings");
      const sources = new Set<string>();
      for (const source of op.sources) {
        if (sources.has(source.id) || source.fromParameter === source.toParameter) throw Error("source binding");
        sources.add(source.id);
        const properties = op.requestSchema["properties"] as Record<string, unknown> | undefined;
        for (const key of [source.fromParameter, source.toParameter]) {
          const parameter = properties?.[key] as Record<string, unknown> | undefined;
          if (
            parameter?.["type"] !== "string" ||
            parameter["format"] !== "date-time" ||
            !(op.requestSchema["required"] as string[] | undefined)?.includes(key)
          )
            throw Error("time parameter binding");
        }
      }
    }
    return parsed;
  } catch {
    throw new CatalogAccessError("catalog_manifest_invalid");
  }
}

function catalogSchema(value: unknown): Record<string, unknown> {
  const schema = operatorRequestSchema(value);
  const visit = (node: Record<string, unknown>): void => {
    if (
      !["object", "array", "string", "integer", "number", "boolean", "null"].includes(String(node["type"])) ||
      ["oneOf", "anyOf", "allOf", "default"].some((key) => key in node)
    )
      throw Error("unsupported catalog schema");
    if (node["properties"]) for (const child of Object.values(node["properties"] as Record<string, Record<string, unknown>>)) visit(child);
    if (node["items"]) visit(node["items"] as Record<string, unknown>);
  };
  visit(schema);
  return schema;
}
/** Zod's JSON Schema importer limits date-time to UTC. JSON Schema permits
 * explicit offsets. Check these formats separately without altering input. */
function offsetSchema(schema: Record<string, unknown>, value: unknown): Record<string, unknown> {
  const copy = { ...schema };
  if (copy["format"] === "date-time") {
    if (value !== undefined && !z.iso.datetime({ offset: true }).safeParse(value).success) throw Error("date-time");
    delete copy["format"];
  }
  if (copy["properties"])
    copy["properties"] = Object.fromEntries(
      Object.entries(copy["properties"] as Record<string, Record<string, unknown>>).map(([key, child]) => [
        key,
        offsetSchema(child, value && typeof value === "object" ? (value as Record<string, unknown>)[key] : undefined),
      ]),
    );
  if (copy["items"]) {
    const child = copy["items"] as Record<string, unknown>;
    if (Array.isArray(value)) for (const item of value) offsetSchema(child, item);
    copy["items"] = offsetSchema(child, undefined);
  }
  return copy;
}
function assertSchema(schema: Record<string, unknown>, value: unknown, code: string, maxBytes: number): void {
  try {
    const json = JSON.stringify(value);
    if (json === undefined || Buffer.byteLength(json, "utf8") > maxBytes) throw Error("size");
    // fromJSONSchema strips unknown keys by default, so validate the unchanged
    // value against a strict schema rather than returning parsed/transformed data.
    if (!z.fromJSONSchema(offsetSchema(schema, value) as never).safeParse(value).success) throw Error("schema");
  } catch {
    throw new CatalogAccessError(code, code === "catalog_response_invalid" ? 502 : 400);
  }
}
export function assertCatalogRequest(op: CatalogOperation, input: unknown): void {
  operatorJson(input);
  assertSchema(op.requestSchema, input, "catalog_request_invalid", 65536);
  for (const source of op.sources) catalogHistoryQuery(source, input);
}
export function assertCatalogResponse(op: CatalogOperation, value: unknown): void {
  assertSchema(op.responseSchema, value, "catalog_response_invalid", op.maxResponseBytes);
}
export function catalogHistoryQuery(
  source: CatalogHistorySource,
  input: unknown,
): {
  topics: string[];
  from: string;
  to: string;
  limit: number;
  dedupe: false;
} {
  const values = input as Record<string, unknown> | null;
  const from = values?.[source.fromParameter],
    to = values?.[source.toParameter];
  if (!z.iso.datetime({ offset: true }).safeParse(from).success || !z.iso.datetime({ offset: true }).safeParse(to).success)
    throw new CatalogAccessError("catalog_window_invalid");
  const start = Date.parse(from as string),
    end = Date.parse(to as string);
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || end - start > source.maxWindowMs)
    throw new CatalogAccessError("catalog_window_invalid");
  return { topics: [source.topic], from: new Date(start).toISOString(), to: new Date(end).toISOString(), limit: source.maxRows, dedupe: false };
}

export const catalogInvocationSchema = z
  .object({
    rttNode: id,
    version: id,
    instanceId: id,
    operationId: id,
    manifestDigest: z.string().regex(/^[a-f0-9]{64}$/),
    input: z.record(z.string(), z.unknown()),
    callerToken: z.string().min(1).max(16384),
  })
  .strict();
export type CatalogInvocation = z.infer<typeof catalogInvocationSchema>;
