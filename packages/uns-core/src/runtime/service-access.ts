import { z } from "zod";

import { CATALOG_SOURCE_SCOPE, type CatalogPublicationManifest, parseCatalogPublicationManifest } from "./catalog-publication.js";
import { operatorDigest, operatorJson } from "./operator-api.js";

export const SERVICE_ACCESS_PROTOCOL = 1;
export const SERVICE_ACCESS_MANIFEST_FIELD = "serviceAccess";
// Only existing platform data/runtime capabilities. Administrative authority is
// never a package-requestable scope. UNS rules bound direct UNS reads; other
// runtime capabilities retain their existing platform authorization semantics.
export const SERVICE_RESOURCE_SCOPES = [
  "read:uns",
  "identity:asset:publish",
  "runtime:captures:read",
  "runtime:captures:write",
  "runtime:triggers:read",
] as const;
const rule = z
  .string()
  .min(1)
  .max(512)
  .refine((s) => s.split("/").every((p, i, a) => p === "+" || (p === "#" && i === a.length - 1) || /^[A-Za-z0-9-]+$/.test(p)));
const envelope = z
  .object({
    protocol: z.literal(1),
    permissions: z.object({ scopes: z.array(z.enum(SERVICE_RESOURCE_SCOPES)).max(5), accessRules: z.array(rule).max(64) }).strict(),
    publications: z.unknown().optional(),
  })
  .strict();
export type ServiceAccessDeclaration = {
  protocol: 1;
  permissions: { scopes: string[]; accessRules: string[] };
  publications?: CatalogPublicationManifest;
};
export function parseServiceAccessDeclaration(value: unknown): ServiceAccessDeclaration {
  operatorJson(value);
  const parsed = envelope.parse(value);
  const permissions = { scopes: [...new Set(parsed.permissions.scopes)].sort(), accessRules: [...new Set(parsed.permissions.accessRules)].sort() };
  if (!!permissions.scopes.length !== !!permissions.accessRules.length) throw Error("service_access_resources_required");
  const publications = parsed.publications === undefined ? undefined : parseCatalogPublicationManifest(parsed.publications);
  if (!permissions.scopes.length && !publications) throw Error("service_access_capability_required");
  return { protocol: 1, permissions, ...(publications ? { publications } : {}) };
}

/** Package metadata accepts one inline declaration or one safe relative JSON
 * reference. Both normalize through the same parser; neither grants authority. */
export const serviceAccessPackageReferenceSchema = z.union([
  z.string().min(1).max(512).refine(
    (value) => !value.includes("\\") && !value.startsWith("/") &&
      !/^[A-Za-z]:/.test(value) &&
      value.split("/").every((part) => !!part && part !== "." && part !== ".."),
    "Expected a package-relative declaration path",
  ),
  envelope.transform(parseServiceAccessDeclaration),
]);

export type ServiceAccessPackageReference = string | ServiceAccessDeclaration;

export async function resolveServiceAccessDeclaration(
  packageJson: unknown,
  readDeclaration: (relativePath: string) => Promise<unknown>,
): Promise<ServiceAccessDeclaration | null> {
  const pkg = z.object({
    unsDatahub: z.object({
      serviceAccess: z.unknown().optional(),
      catalogAccess: z.unknown().optional(),
    }).passthrough().optional(),
  }).passthrough().parse(packageJson);
  const metadata = pkg.unsDatahub;
  if (metadata?.serviceAccess === undefined) return null;
  if (metadata.catalogAccess !== undefined) throw Error("service_access_declaration_conflict");
  const reference = serviceAccessPackageReferenceSchema.parse(metadata.serviceAccess);
  return parseServiceAccessDeclaration(
    typeof reference === "string" ? await readDeclaration(reference) : reference,
  );
}
export const serviceAccessDigest = operatorDigest;
/** Publication source rules are deliberately not raw UNS permissions. A service
 * combining a background reader with a catalog must not gain their cross-product. */
export function serviceAccessCeiling(declaration: ServiceAccessDeclaration) {
  return {
    scopes: [...declaration.permissions.scopes, ...(declaration.publications ? [CATALOG_SOURCE_SCOPE] : [])].sort(),
    accessRules: [...declaration.permissions.accessRules],
  };
}
/** Conservative MQTT filter inclusion, not matching a filter as if it were data. */
export function serviceRuleWithin(requested: string, allowed: string): boolean {
  const a = allowed.split("/"),
    r = requested.split("/");
  let i = 0;
  for (; i < a.length; i++) {
    if (a[i] === "#") return true;
    if (r[i] === undefined || r[i] === "#") return false;
    if (a[i] !== r[i] && a[i] !== "+") return false;
  }
  return i === r.length;
}
