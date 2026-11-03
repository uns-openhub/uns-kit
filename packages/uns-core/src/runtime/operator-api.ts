import { createHash } from "node:crypto";
import { z } from "zod";

/** Explicit opt-in. OpenAPI paths alone are never operator authority. */
export const OPERATOR_API_EXTENSION = "x-uns-operator-operations";
export const OPERATOR_DELEGATION_CLAIM = "unsOperatorDelegation";
export const OPERATOR_OUTCOME_HEADER = "x-uns-operator-outcome";
export const OPERATOR_AUTH_SCHEME = "UNS-Operator";
export const OPERATOR_JWT_TYPE = "uns-operator-api+jwt";
export const OPERATOR_JWT_AUDIENCE = "uns-operator-api";
export const OPERATOR_JWT_ALGORITHM = "PS256";
export const OPERATOR_JSON_MAX_BYTES = 65536;
const identifier = z
  .string()
  .min(1)
  .max(120)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const resource = z.string().min(1).max(256);
const localPath = z
  .string()
  .min(1)
  .max(2048)
  .refine(
    (value) => /^\/[a-zA-Z0-9_./-]+$/.test(value) && !value.includes("//") && !value.split("/").some((part) => part === "." || part === ".."),
    "Expected an exact local API path",
  );

export class OperatorApiError extends Error {
  constructor(
    public readonly code: string,
    public readonly status = 400,
  ) {
    super(code);
  }
}

/** Canonical bounded JSON is shared by reviews, wire claims and receipts.
 * Never hashes JSON with dropped undefined values or non-finite numbers. */
export function operatorJson(value: unknown): string {
  let nodes = 0;
  const visit = (item: unknown, depth: number): unknown => {
    if (++nodes > 4096 || depth > 20) throw new OperatorApiError("operator_json_budget");
    if (item === null || typeof item === "boolean") return item;
    if (typeof item === "number" && Number.isFinite(item)) return Object.is(item, -0) ? 0 : item;
    if (typeof item === "string" && item.length <= 32768) return item;
    if (Array.isArray(item) && item.length <= 1024) return item.map((entry) => visit(entry, depth + 1));
    if (item && typeof item === "object" && [Object.prototype, null].includes(Object.getPrototypeOf(item))) {
      const entries = Object.entries(item as Record<string, unknown>);
      if (entries.length > 128) throw new OperatorApiError("operator_json_budget");
      return Object.fromEntries(
        entries
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, entry]) => {
            if (key.length > 256 || ["__proto__", "prototype", "constructor"].includes(key)) throw new OperatorApiError("operator_json_key");
            return [key, visit(entry, depth + 1)];
          }),
      );
    }
    throw new OperatorApiError("operator_json_value");
  };
  const json = JSON.stringify(visit(value, 0));
  if (Buffer.byteLength(json, "utf8") > OPERATOR_JSON_MAX_BYTES) throw new OperatorApiError("operator_json_budget");
  return json;
}
export const operatorDigest = (value: unknown): string => createHash("sha256").update(operatorJson(value)).digest("hex");

/** Protocol 1 deliberately excludes references, executable regex and extensions.
 * UUID format already enforces the standard UUID constraint without a pattern. */
export function operatorRequestSchema(value: unknown): Record<string, unknown> {
  operatorJson(value);
  const vocabulary = new Set([
    "$schema",
    "type",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minItems",
    "maxItems",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "format",
    "const",
    "enum",
    "oneOf",
    "anyOf",
    "allOf",
    "description",
    "title",
    "default",
    "examples",
    "pattern",
  ]);
  let count = 0;
  function schema(input: unknown, depth = 0): Record<string, unknown> {
    if (++count > 256 || depth > 12 || !input || typeof input !== "object" || Array.isArray(input))
      throw new OperatorApiError("operator_schema_budget");
    const source = input as Record<string, unknown>;
    for (const key of Object.keys(source)) if (!vocabulary.has(key)) throw new OperatorApiError("operator_schema_keyword");
    const out = { ...source };
    if ("pattern" in out) {
      if (out["format"] !== "uuid") throw new OperatorApiError("operator_schema_pattern");
      delete out["pattern"];
    }
    if (out["format"] && !["uuid", "date-time", "date", "time", "email", "uri"].includes(String(out["format"])))
      throw new OperatorApiError("operator_schema_format");
    if (source["properties"]) {
      if (source["additionalProperties"] !== false || typeof source["properties"] !== "object" || Array.isArray(source["properties"]))
        throw new OperatorApiError("operator_schema_object_must_be_closed");
      out["properties"] = Object.fromEntries(
        Object.entries(source["properties"] as Record<string, unknown>).map(([key, item]) => [key, schema(item, depth + 1)]),
      );
    }
    if (source["type"] === "object" && source["additionalProperties"] !== false) throw new OperatorApiError("operator_schema_object_must_be_closed");
    if (source["additionalProperties"] !== undefined && source["additionalProperties"] !== false)
      throw new OperatorApiError("operator_schema_object_must_be_closed");
    if (source["items"]) out["items"] = schema(source["items"], depth + 1);
    for (const key of ["oneOf", "anyOf", "allOf"])
      if (source[key]) {
        if (!Array.isArray(source[key]) || source[key].length > 16) throw new OperatorApiError("operator_schema_budget");
        out[key] = source[key].map((item) => schema(item, depth + 1));
      }
    return out;
  }
  const normalized = schema(value);
  try {
    z.fromJSONSchema(normalized as never);
  } catch {
    throw new OperatorApiError("operator_schema_unsupported");
  }
  return normalized;
}
const definitionSchema = z
  .object({
    protocol: z.literal(1),
    id: identifier,
    title: z.string().min(1).max(160),
    kind: z.enum(["read", "command"]),
    requestSchema: z.record(z.string(), z.unknown()),
    writeDomain: identifier.optional(),
    replay: z.literal("request-id-v1").optional(),
    timeoutMs: z.number().int().min(500).max(10000).default(5000),
    maxResponseBytes: z.number().int().min(256).max(262144).default(65536),
  })
  .strict()
  .superRefine((d, ctx) => {
    if (d.kind === "command" && (!d.writeDomain || d.replay !== "request-id-v1"))
      ctx.addIssue({ code: "custom", message: "Commands require a write domain and application request-id receipt support" });
    if (d.kind === "read" && (d.writeDomain || d.replay)) ctx.addIssue({ code: "custom", message: "Reads cannot declare command authority" });
  });
export type OperatorOperationDefinition = z.input<typeof definitionSchema>;
export type OperatorOperation = z.output<typeof definitionSchema> & { method: "GET" | "POST"; path: string };
export function operatorOperation(definition: unknown, method: unknown, path: unknown): OperatorOperation {
  const parsed = definitionSchema.parse(definition);
  const checkedMethod = z.enum(["GET", "POST"]).parse(method);
  if (parsed.kind === "command" && checkedMethod !== "POST") throw new OperatorApiError("operator_command_requires_post");
  return { ...parsed, requestSchema: operatorRequestSchema(parsed.requestSchema), method: checkedMethod, path: localPath.parse(path) };
}
export function operatorOperationFromWire(input: unknown): OperatorOperation {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new OperatorApiError("operator_operation_invalid");
  const { method, path, ...definition } = input as Record<string, unknown>;
  return operatorOperation(definition, method, path);
}
export function assertOperatorPayload(operation: OperatorOperation, value: unknown): void {
  operatorJson(value);
  let valid = false;
  try {
    valid = z.fromJSONSchema(operation.requestSchema as never).safeParse(value).success;
  } catch {
    /* fail closed */
  }
  if (!valid) throw new OperatorApiError("operator_request_invalid");
  if (operation.kind === "command" && !z.uuid().safeParse((value as Record<string, unknown>)?.["requestId"]).success)
    throw new OperatorApiError("operator_request_id_required");
}
export const operatorDeploymentSchema = z
  .object({
    controllerName: resource,
    rttNode: resource,
    version: resource,
    instanceId: resource,
    launchId: z.uuid(),
  })
  .strict();
export type OperatorDeployment = z.infer<typeof operatorDeploymentSchema>;
export const operatorDelegationSchema = z
  .object({
    protocol: z.literal(1),
    operationId: identifier,
    operationDigest: digest,
    requestDigest: digest,
    target: operatorDeploymentSchema,
    tool: z.enum(["controller_read_runtime_operator_operation", "controller_execute_runtime_operator_command"]),
  })
  .strict();
export type OperatorDelegation = z.infer<typeof operatorDelegationSchema>;

/** Called only AFTER normal JWT cryptographic, expiry and route-grant checks. */
export function assertOperatorDelegation(
  claims: Record<string, unknown>,
  operation: OperatorOperation,
  payload: unknown,
  deployment: OperatorDeployment,
  nowSeconds = Math.floor(Date.now() / 1000),
): OperatorDelegation {
  const d = operatorDelegationSchema.parse(claims[OPERATOR_DELEGATION_CLAIM]);
  if (
    claims["serviceToken"] !== true ||
    claims["identityType"] !== "machine" ||
    claims["workloadIdentityType"] !== "CONTROLLER_MCP" ||
    claims["sub"] !== claims["machineIdentityId"] ||
    !z.uuid().safeParse(claims["sub"]).success ||
    !Number.isSafeInteger(claims["tokenVersion"]) ||
    Number(claims["tokenVersion"]) < 1 ||
    claims["role"] !== "operator" ||
    !Number.isSafeInteger(claims["iat"]) ||
    !Number.isSafeInteger(claims["exp"]) ||
    Number(claims["exp"]) <= nowSeconds ||
    Number(claims["iat"]) > nowSeconds + 2 ||
    Number(claims["exp"]) - Number(claims["iat"]) > 30 ||
    Number(claims["exp"]) <= Number(claims["iat"]) ||
    !Array.isArray(claims["accessRules"]) ||
    claims["accessRules"].length !== 1 ||
    claims["accessRules"][0] !== operation.path
  ) {
    throw new OperatorApiError("operator_delegation_invalid", 403);
  }
  if (
    d.operationId !== operation.id ||
    d.operationDigest !== operatorDigest(operation) ||
    d.requestDigest !== operatorDigest(payload) ||
    operatorDigest(d.target) !== operatorDigest(operatorDeploymentSchema.parse(deployment)) ||
    d.tool !== (operation.kind === "command" ? "controller_execute_runtime_operator_command" : "controller_read_runtime_operator_operation")
  ) {
    throw new OperatorApiError("operator_delegation_binding_changed", 403);
  }
  assertOperatorPayload(operation, payload);
  return d;
}
