import { z } from "zod";

export const RETAINED_STORE_CONFIGURATION_MANIFEST = "retained-store-configuration.manifest.json";
export const RETAINED_STORE_CONFIGURATION_MANIFEST_MAX_BYTES = 64 * 1024;
const CONFIGURATION_MAX_BYTES = 1024 * 1024;
const MAX_DEPTH = 48;
const MAX_NODES = 100_000;
const integerSource = /^(?:0|-?[1-9][0-9]*)$/;
const unpairedSurrogate = /[\uD800-\uDFFF]/u;

const fieldPath = z
  .array(
    z
      .string()
      .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/)
      .refine((key) => !["constructor", "prototype"].includes(key)),
  )
  .min(1)
  .max(12);
const field = { path: fieldPath, dependencies: z.array(fieldPath).max(16) };
const fieldSchema = z.discriminatedUnion("kind", [
  z.strictObject({ ...field, kind: z.literal("integer"), minimum: z.int(), maximum: z.int() }),
  z.strictObject({ ...field, kind: z.literal("boolean") }),
  z.strictObject({ ...field, kind: z.literal("enum"), values: z.array(z.string().min(1).max(64)).min(2).max(16) }),
]);

function isPrefix(prefix: readonly string[], path: readonly string[]): boolean {
  return prefix.length <= path.length && prefix.every((part, i) => part === path[i]);
}

const manifestSchema = z
  .strictObject({
    protocol: z.literal(1),
    activation: z.literal("stopped"),
    fields: z.array(fieldSchema).min(1).max(32),
  })
  .superRefine((manifest, context) => {
    const paths = manifest.fields.map(({ path }) => path);
    for (const [index, rule] of manifest.fields.entries()) {
      const issue = (message: string) => context.addIssue({ code: "custom", path: ["fields", index], message });
      if (rule.path[0] === "uns") issue("Platform configuration is reserved");
      if (paths.some((other, i) => i !== index && isPrefix(rule.path, other))) issue("Overlapping declared field paths");
      if (rule.kind === "integer" && rule.minimum >= rule.maximum) issue("Invalid integer interval");
      if (rule.kind === "enum" && (new Set(rule.values).size !== rule.values.length || rule.values.some((value) => unpairedSurrogate.test(value))))
        issue("Invalid enum values");
      if (new Set(rule.dependencies.map((dependency) => JSON.stringify(dependency))).size !== rule.dependencies.length) issue("Duplicate dependency");
      if (rule.dependencies.some((dependency) => paths.some((path) => isPrefix(dependency, path) || isPrefix(path, dependency))))
        issue("A declared dependency overlaps an editable field");
    }
  });

function isPlainDeclarationObject(value: unknown): value is Record<string, unknown> {
  // Zod ignores an own __proto__ key even in strict objects.
  return value !== null && typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype && !Object.hasOwn(value, "__proto__");
}

export const retainedStoreConfigurationManifestSchema = z
  .custom<unknown>((value) => {
    if (!isPlainDeclarationObject(value)) return false;
    const fields = value["fields"];
    return Array.isArray(fields) && fields.length <= 32 && fields.every(isPlainDeclarationObject);
  })
  .pipe(manifestSchema);

export type RetainedStoreConfigurationManifest = z.infer<typeof retainedStoreConfigurationManifestSchema>;
type FieldRule = RetainedStoreConfigurationManifest["fields"][number];
type JsonValue = null | boolean | string | JsonNumber | JsonValue[] | Map<string, JsonValue>;

class JsonNumber {
  constructor(readonly source: string) {}
}

function strictJson(input: string | Uint8Array, maximumBytes: number, label: string, integerNumbers = false): JsonValue {
  const invalid = (): never => {
    throw Error(`${label} must be bounded, unambiguous JSON`);
  };
  if (typeof input === "string" && (input.length > maximumBytes || unpairedSurrogate.test(input))) return invalid();
  if ((typeof input === "string" ? Buffer.byteLength(input) : input.byteLength) > maximumBytes) return invalid();
  let source: string;
  try {
    source = typeof input === "string" ? input : new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(input);
  } catch {
    return invalid();
  }
  const whitespace = /[ \t\r\n]*/y;
  const tokens =
    /"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null|[{}\[\]:,]/y;
  let offset = 0;
  let nodes = 0;
  function next(): string | undefined {
    whitespace.lastIndex = offset;
    whitespace.exec(source);
    offset = whitespace.lastIndex;
    if (offset === source.length) return;
    tokens.lastIndex = offset;
    const token = tokens.exec(source);
    if (!token) return invalid();
    offset = tokens.lastIndex;
    return token[0];
  }

  function decodeString(token: string): string {
    const value: string = JSON.parse(token);
    if (unpairedSurrogate.test(value)) return invalid();
    return value;
  }

  function parse(token: string | undefined, depth: number): JsonValue {
    if (!token || depth > MAX_DEPTH || ++nodes > MAX_NODES) return invalid();
    switch (token) {
      case "{": {
        const values = new Map<string, JsonValue>();
        let key = next();
        if (key === "}") return values;
        while (key?.startsWith('"')) {
          const name = decodeString(key);
          if (values.has(name) || next() !== ":") return invalid();
          values.set(name, parse(next(), depth + 1));
          const delimiter = next();
          if (delimiter === "}") return values;
          if (delimiter !== ",") return invalid();
          key = next();
        }
        return invalid();
      }
      case "[": {
        const values: JsonValue[] = [];
        let item = next();
        if (item === "]") return values;
        for (;;) {
          values.push(parse(item, depth + 1));
          const delimiter = next();
          if (delimiter === "]") return values;
          if (delimiter !== ",") return invalid();
          item = next();
        }
      }
      case "true":
        return true;
      case "false":
        return false;
      case "null":
        return null;
      default:
        if (token.startsWith('"')) return decodeString(token);
        if (!/^-?\d/.test(token) || !Number.isFinite(Number(token))) return invalid();
        if (integerNumbers && (!integerSource.test(token) || !Number.isSafeInteger(Number(token)))) return invalid();
        // Exact tokens distinguish values that a native JSON parse rounds to the same double.
        return new JsonNumber(token);
    }
  }

  const value = parse(next(), 0);
  if (next() !== undefined) return invalid();
  return value;
}

function parseManifest(manifest: unknown): RetainedStoreConfigurationManifest {
  const declaration = retainedStoreConfigurationManifestSchema.safeParse(manifest);
  if (!declaration.success) throw Error("Unsupported or invalid retained store configuration declaration");
  return declaration.data;
}

export function parseRetainedStoreConfigurationManifest(bytes: Uint8Array): RetainedStoreConfigurationManifest {
  strictJson(bytes, RETAINED_STORE_CONFIGURATION_MANIFEST_MAX_BYTES, "Retained store configuration declaration", true);
  return parseManifest(JSON.parse(Buffer.from(bytes).toString("utf8")));
}

function accepts(rule: FieldRule, value: JsonValue): boolean {
  switch (rule.kind) {
    case "boolean":
      return typeof value === "boolean";
    case "enum":
      return typeof value === "string" && rule.values.includes(value);
    case "integer": {
      if (!(value instanceof JsonNumber) || !integerSource.test(value.source)) return false;
      const number = Number(value.source);
      return Number.isSafeInteger(number) && number >= rule.minimum && number <= rule.maximum;
    }
  }
}

function hasPath(root: JsonValue, path: readonly string[]): boolean {
  let value: JsonValue | undefined = root;
  for (const key of path) value = value instanceof Map ? value.get(key) : undefined;
  return value !== undefined;
}

/** Protocol 1 describes neutral scalar tuning with all other settings and ordered collections preserved. */
export function reviewRetainedStoreConfigurationChange(
  manifest: RetainedStoreConfigurationManifest,
  before: string | Uint8Array,
  after: string | Uint8Array,
): { changedPaths: string[][] } {
  const declaration = parseManifest(manifest);
  const previous = strictJson(before, CONFIGURATION_MAX_BYTES, "Current configuration");
  const proposed = strictJson(after, CONFIGURATION_MAX_BYTES, "Proposed configuration");
  if (!(previous instanceof Map) || !(proposed instanceof Map)) throw Error("Retained store configurations must be JSON objects");
  const rules = new Map(declaration.fields.map((rule) => [JSON.stringify(rule.path), rule]));
  const changedPaths: string[][] = [];
  const incompatible = (): never => {
    throw Error("Configuration changes a field, structure, or order outside the retained store declaration");
  };

  function compare(current: JsonValue, next: JsonValue, path: string[] | undefined): void {
    if (current === next) return;
    if (current instanceof Map && next instanceof Map) {
      const nextKeys = [...next.keys()];
      if (current.size !== next.size) return incompatible();
      let index = 0;
      for (const [key, value] of current) {
        if (key !== nextKeys[index++]) return incompatible();
        compare(value, next.get(key)!, path && [...path, key]);
      }
      return;
    }
    if (Array.isArray(current) && Array.isArray(next)) {
      if (current.length !== next.length) return incompatible();
      current.forEach((value, index) => compare(value, next[index], undefined));
      return;
    }
    if (current instanceof JsonNumber && next instanceof JsonNumber && current.source === next.source) return;
    const rule = path && rules.get(JSON.stringify(path));
    if (!rule) return incompatible();
    if (!accepts(rule, current) || !accepts(rule, next)) throw Error("A retained store tuning value is outside the declared scalar range");
    if (!rule.dependencies.every((dependency) => hasPath(previous, dependency)))
      throw Error("A declared retained store configuration dependency is absent");
    changedPaths.push(rule.path);
  }

  compare(previous, proposed, []);
  return { changedPaths };
}
