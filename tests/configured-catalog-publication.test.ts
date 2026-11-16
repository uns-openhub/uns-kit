import { describe, expect, it, vi } from "vitest";

import { CatalogPublicationClient } from "../packages/uns-core/src/runtime/catalog-publication-client.js";
import {
  assertCatalogRequest,
  assertCatalogResponse,
  catalogAccessDigest,
  catalogHistoryQuery,
} from "../packages/uns-core/src/runtime/catalog-publication.js";
import {
  configuredServiceAccessDigest,
  deriveConfiguredUnsAccess,
  parseConfiguredServiceAccessDeclaration,
  resolveConfiguredCatalogPublicationManifest,
} from "../packages/uns-core/src/runtime/configured-service-access.js";
import { parseServiceAccessDeclaration } from "../packages/uns-core/src/runtime/service-access.js";

const source = { id: "forecast", kind: "history", topicFromConfig: "/source/topic", maxWindowMs: 3600000, maxRows: 100 };
const operation = {
  id: "stoppages",
  title: "Planned stoppages",
  method: "GET",
  path: "/api/plant/stoppages",
  mode: "approved-service",
  requestSchema: {
    type: "object",
    additionalProperties: false,
    required: ["from", "to"],
    properties: { from: { type: "string", format: "date-time" }, to: { type: "string", format: "date-time" } },
  },
  responseSchema: { type: "object", additionalProperties: false, required: ["count"], properties: { count: { type: "integer", minimum: 0 } } },
  sources: [source],
};
const declaration = { protocol: 2, requirements: [], publications: { protocol: 2, operations: [operation] } };
const input = { from: "2026-01-01T00:00:00Z", to: "2026-01-01T01:00:00Z" };
const config = { source: { topic: "plant/forecast" } };

describe("configured catalog bindings", () => {
  it("resolves the same bounded operation without direct UNS authority", () => {
    const parsed = parseConfiguredServiceAccessDeclaration(declaration);
    expect(deriveConfiguredUnsAccess(parsed, config)).toEqual({ requirements: [], policy: { scopes: [], accessRules: [] } });
    const effective = resolveConfiguredCatalogPublicationManifest(parsed.publications, config),
      op = effective.operations[0]!;
    expect(effective.protocol).toBe(1);
    expect(op.sources[0]).toMatchObject({ id: "forecast", topic: "plant/forecast", maxWindowMs: 3600000, maxRows: 100 });
    expect(catalogHistoryQuery(op.sources[0]!, input)).toMatchObject({ topics: ["plant/forecast"], limit: 100 });
    expect(() => parseServiceAccessDeclaration(declaration)).toThrow();
    expect(() => assertCatalogRequest(op, { ...input, topic: "other/private" })).toThrow();
    expect(() => assertCatalogRequest(op, { ...input, to: "2026-01-01T02:00:00Z" })).toThrow();
    expect(() => assertCatalogResponse(op, { count: 1, raw: "private" })).toThrow();
  });
  it("keeps configured declaration and resolved-source digests distinct", () => {
    const d1 = configuredServiceAccessDigest(declaration);
    const first = resolveConfiguredCatalogPublicationManifest(declaration.publications, config);
    const second = resolveConfiguredCatalogPublicationManifest(declaration.publications, { source: { topic: "other/forecast" } });
    expect(configuredServiceAccessDigest(declaration)).toBe(d1);
    expect(d1).toBe("3f5b20bd199539239e426c643ba62fcdc24da7db99e41cd666f7cfd96e8eb2ad");
    expect(catalogAccessDigest(first)).toBe("d89f8cfee1bbf5c394e4e7329eeb0333bb7d2e880f64b0949ff0878d5d28c739");
    expect(catalogAccessDigest(first)).not.toBe(catalogAccessDigest(second));
    expect(catalogAccessDigest(first)).toBe(catalogAccessDigest(JSON.parse(JSON.stringify(first))));
  });
  it("keeps direct background permissions separate from the catalog source", () => {
    const mixed = { ...declaration, requirements: [{ id: "background", capability: "read:uns", topicFromConfig: "/background/topic" }] };
    const configuration = { ...config, background: { topic: "plant/background" } };
    expect(deriveConfiguredUnsAccess(mixed, configuration).policy).toEqual({ scopes: ["read:uns"], accessRules: ["plant/background"] });
    expect(resolveConfiguredCatalogPublicationManifest(mixed.publications, configuration).operations[0]!.sources[0]!.topic).toBe("plant/forecast");
  });
  it.each([undefined, "plant/#", "plant/+", "https://example.invalid", "${SECRET}", "../hidden", { provider: "env", key: "SECRET" }])(
    "rejects an absent, nonexact or secret source: %j",
    (topic) => {
      expect(() => resolveConfiguredCatalogPublicationManifest(declaration.publications, { source: { topic } })).toThrow();
    },
  );
  it.each(["/source/~2", "source/topic", "/source//topic", "/__proto__/topic", "/a/b/c/d/e/f/g/h/i"])("rejects invalid pointer %s", (pointer) => {
    expect(() =>
      parseConfiguredServiceAccessDeclaration({
        ...declaration,
        publications: { protocol: 2, operations: [{ ...operation, sources: [{ ...source, topicFromConfig: pointer }] }] },
      }),
    ).toThrow();
  });
  it("rejects both a literal and pointer, duplicate source IDs and extra declaration fields", () => {
    for (const sources of [[{ ...source, topic: "other/private" }], [source, source], [{ ...source, extra: true }]])
      expect(() =>
        parseConfiguredServiceAccessDeclaration({ ...declaration, publications: { protocol: 2, operations: [{ ...operation, sources }] } }),
      ).toThrow();
  });
  it("supports an unrelated source layout using the same contract", () => {
    const publications = {
      protocol: 2,
      operations: [
        { ...operation, id: "quality", path: "/api/quality/count", sources: [{ ...source, id: "samples", topicFromConfig: "/input/samples" }] },
      ],
    };
    const manifest = resolveConfiguredCatalogPublicationManifest(publications, { input: { samples: "warehouse/line-2/samples" } });
    expect(manifest.operations[0]!.sources[0]).toMatchObject({ id: "samples", topic: "warehouse/line-2/samples" });
  });
  it("rechecks the resolved digest at a simulated broker and never sends a topic supplied by the caller", async () => {
    const manifest = resolveConfiguredCatalogPublicationManifest(declaration.publications, config),
      digest = catalogAccessDigest(manifest);
    const calls: Record<string, unknown>[] = [];
    const client = new CatalogPublicationClient({
      controllerBase: "http://localhost:3200",
      rttNode: "worker",
      version: "1.0.0",
      instanceId: "one",
      manifest,
      tokenProvider: { getAccessToken: async () => "service" },
      fetch: async (_url, init) => {
        const request = JSON.parse(String(init?.body));
        calls.push(request);
        return request.manifestDigest === digest && request.callerToken === "reader"
          ? new Response(JSON.stringify({ data: [] }))
          : new Response("{}", { status: 403 });
      },
    });
    await expect(
      client.execute("stoppages", input, "reader", async ({ history }) => {
        await history("forecast");
        return { count: 0 };
      }),
    ).resolves.toEqual({ count: 0 });
    expect(calls).toHaveLength(3);
    expect(calls[1]).toMatchObject({ sourceId: "forecast", manifestDigest: digest });
    expect(calls.every((value) => !("topic" in value))).toBe(true);
    await expect(client.execute("stoppages", { ...input, topic: "other/private" }, "reader", vi.fn())).rejects.toThrow();
    await expect(
      client.execute("stoppages", input, "reader", async ({ history }) => {
        await history("other");
        return { count: 0 };
      }),
    ).rejects.toThrow("source not declared");
    const newer = resolveConfiguredCatalogPublicationManifest(declaration.publications, { source: { topic: "other/forecast" } });
    const stale = new CatalogPublicationClient({
      controllerBase: "http://localhost:3200",
      rttNode: "worker",
      version: "1.0.0",
      instanceId: "one",
      manifest: newer,
      tokenProvider: { getAccessToken: async () => "service" },
      fetch: async (_url, init) => new Response("{}", { status: JSON.parse(String(init?.body)).manifestDigest === digest ? 200 : 403 }),
    });
    await expect(stale.execute("stoppages", input, "reader", async () => ({ count: 0 }))).rejects.toThrow("catalog_access_denied");
  });
});
