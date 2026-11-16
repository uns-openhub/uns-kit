import { z } from "zod";

import {
  type CatalogPublicationManifest,
  catalogHistorySourceSchema,
  catalogOperationSchema,
  parseCatalogPublicationManifest,
} from "./catalog-publication.js";
import { operatorDigest, operatorJson } from "./operator-api.js";
import { serviceAccessReferencePathSchema, serviceAccessRuleSchema, serviceRuleWithin } from "./service-access.js";

/** Protocol 2 describes requests, never grants. Catalog sources do not grant direct UNS reads. */
const requirement = z
  .object({
    id: z
      .string()
      .min(1)
      .max(80)
      .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/),
    capability: z.literal("read:uns"),
    topicFromConfig: z.string().min(2).max(512),
    maxAccessRules: z.array(serviceAccessRuleSchema).min(1).max(64).optional(),
  })
  .strict();
const configuredHistorySource = catalogHistorySourceSchema
  .omit({ topic: true })
  .extend({ topicFromConfig: z.string().min(2).max(512) })
  .strict();
const configuredOperation = catalogOperationSchema.extend({ sources: z.array(configuredHistorySource).min(1).max(16) }).strict();
const configuredPublications = z.object({ protocol: z.literal(2), operations: z.array(configuredOperation).min(1).max(32) }).strict();
const envelope = z
  .object({ protocol: z.literal(2), requirements: z.array(requirement).max(32), publications: configuredPublications.optional() })
  .strict();
export type ConfiguredCatalogPublicationManifest = z.output<typeof configuredPublications>;
export type ConfiguredServiceAccessDeclaration = Omit<z.output<typeof envelope>, "publications"> & {
  publications?: ConfiguredCatalogPublicationManifest;
};
export type ConfiguredUnsRequest = {
  requirements: { id: string; capability: "read:uns"; topicFromConfig: string; accessRule: string }[];
  policy: { scopes: "read:uns"[]; accessRules: string[] };
};

function pointerSegments(pointer: string): string[] {
  if (!pointer.startsWith("/") || /~(?![01])/.test(pointer)) throw Error("configured_access_pointer_invalid");
  const segments = pointer
    .slice(1)
    .split("/")
    .map((segment) => segment.replace(/~1/g, "/").replace(/~0/g, "~"));
  if (segments.length > 8 || segments.some((segment) => !segment || ["__proto__", "prototype", "constructor"].includes(segment)))
    throw Error("configured_access_pointer_invalid");
  return segments;
}

export function parseConfiguredServiceAccessDeclaration(value: unknown): ConfiguredServiceAccessDeclaration {
  operatorJson(value);
  const parsed = envelope.parse(value);
  if (!parsed.requirements.length && !parsed.publications) throw Error("service_access_capability_required");
  const ids = new Set<string>();
  const requirements = parsed.requirements
    .map((item) => {
      pointerSegments(item.topicFromConfig);
      if (ids.has(item.id)) throw Error("configured_access_duplicate_requirement");
      ids.add(item.id);
      return { ...item, ...(item.maxAccessRules ? { maxAccessRules: [...new Set(item.maxAccessRules)].sort() } : {}) };
    })
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return {
    protocol: 2,
    requirements,
    ...(parsed.publications ? { publications: parseConfiguredCatalogPublicationManifest(parsed.publications) } : {}),
  };
}

/** Reads only the declared, own object properties. It rejects arrays, references and non-strings. */
function valueAt(config: unknown, pointer: string): unknown {
  let current = config;
  for (const segment of pointerSegments(pointer)) {
    if (current === null || typeof current !== "object" || Array.isArray(current)) throw Error("configured_access_value_missing");
    const prototype = Object.getPrototypeOf(current);
    if (prototype !== Object.prototype && prototype !== null) throw Error("configured_access_value_missing");
    const property = Object.getOwnPropertyDescriptor(current, segment);
    if (!property || !("value" in property)) throw Error("configured_access_value_missing");
    current = property.value;
  }
  return current;
}

/** Validate the configured form as data, including all existing catalog bounds
 * and schemas. A placeholder passes through the existing protocol-1 parser;
 * it never appears in the returned declaration or resolved operation. */
export function parseConfiguredCatalogPublicationManifest(value: unknown): ConfiguredCatalogPublicationManifest {
  operatorJson(value);
  const parsed = configuredPublications.parse(value);
  const operations = parsed.operations.map((operation) => ({
    ...operation,
    sources: operation.sources.map((source) => {
      pointerSegments(source.topicFromConfig);
      const { topicFromConfig: _pointer, ...rest } = source;
      return { ...rest, topic: "configured-placeholder" };
    }),
  }));
  const validated = parseCatalogPublicationManifest({ protocol: 1, operations });
  return {
    protocol: 2,
    operations: validated.operations.map((operation, index) => ({
      ...operation,
      sources: operation.sources.map((source, sourceIndex) => {
        const { topic: _topic, ...rest } = source;
        return { ...rest, topicFromConfig: parsed.operations[index]!.sources[sourceIndex]!.topicFromConfig };
      }),
    })),
  };
}

/** Resolve the loaded configuration to the same ordinary manifest used by the
 * client and Controller broker. No direct read:uns authority is produced. */
export function resolveConfiguredCatalogPublicationManifest(value: unknown, configuration: unknown): CatalogPublicationManifest {
  const parsed = parseConfiguredCatalogPublicationManifest(value);
  const operations = parsed.operations.map((operation) => ({
    ...operation,
    sources: operation.sources.map((source) => {
      const { topicFromConfig, ...rest } = source;
      return { ...rest, topic: valueAt(configuration, topicFromConfig) };
    }),
  }));
  return parseCatalogPublicationManifest({ protocol: 1, operations });
}

/** Pure preview; returned policy is a proposal and does not issue credentials or grant access. */
export function deriveConfiguredUnsAccess(declaration: unknown, configuration: unknown): ConfiguredUnsRequest {
  const parsed = parseConfiguredServiceAccessDeclaration(declaration);
  const requirements = parsed.requirements.map((item) => {
    const value = valueAt(configuration, item.topicFromConfig);
    const validated = serviceAccessRuleSchema.safeParse(value);
    if (!validated.success) throw Error("configured_access_rule_invalid");
    if (item.maxAccessRules && !item.maxAccessRules.some((maximum) => serviceRuleWithin(validated.data, maximum)))
      throw Error("configured_access_rule_outside_maximum");
    return { id: item.id, capability: item.capability, topicFromConfig: item.topicFromConfig, accessRule: validated.data };
  });
  return {
    requirements,
    policy: { scopes: requirements.length ? ["read:uns"] : [], accessRules: [...new Set(requirements.map((item) => item.accessRule))].sort() },
  };
}

/** Stable SDK digests do not replace host binding to artifact, identity and config revisions. */
export const configuredServiceAccessDigest = (value: unknown) => operatorDigest(parseConfiguredServiceAccessDeclaration(value));
export const configuredUnsRequestDigest = (declaration: unknown, configuration: unknown) =>
  operatorDigest(deriveConfiguredUnsAccess(declaration, configuration));

/** Resolve inline or safe relative protocol-2 metadata without reading arbitrary files or executing application code. */
export async function resolveConfiguredServiceAccessDeclaration(
  packageJson: unknown,
  readDeclaration: (relativePath: string) => Promise<unknown>,
): Promise<ConfiguredServiceAccessDeclaration | null> {
  const pkg = z
    .object({ unsDatahub: z.object({ serviceAccess: z.unknown().optional(), catalogAccess: z.unknown().optional() }).passthrough().optional() })
    .passthrough()
    .parse(packageJson);
  if (pkg.unsDatahub?.serviceAccess === undefined) return null;
  if (pkg.unsDatahub.catalogAccess !== undefined) throw Error("service_access_declaration_conflict");
  const raw = pkg.unsDatahub.serviceAccess;
  const value = typeof raw === "string" ? await readDeclaration(serviceAccessReferencePathSchema.parse(raw)) : raw;
  return parseConfiguredServiceAccessDeclaration(value);
}
