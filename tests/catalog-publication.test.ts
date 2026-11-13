import { describe, expect, it, vi } from "vitest";

import { CatalogPublicationClient } from "../packages/uns-core/src/runtime/catalog-publication-client.js";
import {
  assertCatalogRequest,
  assertCatalogResponse,
  catalogAccessDigest,
  catalogHistoryQuery,
  parseCatalogPublicationManifest,
} from "../packages/uns-core/src/runtime/catalog-publication.js";

const requestSchema = {
  type: "object",
  additionalProperties: false,
  required: ["from", "to"],
  properties: { from: { type: "string", format: "date-time" }, to: { type: "string", format: "date-time" } },
};
export const manifestInput = {
  protocol: 1,
  operations: [
    {
      id: "stoppages",
      title: "Planned stoppages",
      method: "GET",
      path: "/api/plant/stoppages",
      mode: "approved-service",
      requestSchema,
      responseSchema: { type: "object", additionalProperties: false, required: ["count"], properties: { count: { type: "integer", minimum: 0 } } },
      sources: [{ id: "forecast", kind: "history", topic: "plant/forecast", maxWindowMs: 86400000, maxRows: 100 }],
    },
  ],
};
const input = { from: "2026-01-01T00:00:00+01:00", to: "2026-01-02T00:00:00+01:00" };
describe("catalog declaration", () => {
  it("normalizes and digests bounded contracts for unrelated services", () => {
    const a = parseCatalogPublicationManifest(manifestInput);
    const b = parseCatalogPublicationManifest({
      ...manifestInput,
      operations: [
        {
          ...manifestInput.operations[0],
          id: "quality",
          path: "/api/quality/summary",
          sources: [{ id: "samples", kind: "history", topic: "quality/samples" }],
        },
      ],
    });
    expect(a.operations[0]!.timeoutMs).toBe(10000);
    expect(catalogAccessDigest(a)).not.toBe(catalogAccessDigest(b));
    expect(catalogAccessDigest(a)).toBe(catalogAccessDigest(JSON.parse(JSON.stringify(a))));
  });
  it.each(["plant/#", "plant/+", "/plant/source", "plant/../source"])("rejects nonexact source %s", (topic) => {
    expect(() =>
      parseCatalogPublicationManifest({
        ...manifestInput,
        operations: [{ ...manifestInput.operations[0], sources: [{ id: "source", kind: "history", topic }] }],
      }),
    ).toThrow();
  });
  it("rejects injected source parameters, excess windows and undeclared output", () => {
    const op = parseCatalogPublicationManifest(manifestInput).operations[0]!;
    expect(() => assertCatalogRequest(op, { ...input, topic: "foreign/source" })).toThrow();
    expect(() => assertCatalogRequest(op, { ...input, to: "2026-01-03T00:00:00+01:00" })).toThrow();
    expect(() => assertCatalogResponse(op, { count: 1, raw: 123 })).toThrow();
    expect(() => assertCatalogResponse(op, { count: "1" })).toThrow();
    expect(catalogHistoryQuery(op.sources[0]!, input)).toEqual({
      topics: ["plant/forecast"],
      from: "2025-12-31T23:00:00.000Z",
      to: "2026-01-01T23:00:00.000Z",
      limit: 100,
      dedupe: false,
    });
  });
  it("rejects executable schema features, open outputs and duplicate operations", () => {
    for (const responseSchema of [{ $ref: "https://evil.invalid" }, { type: "object", additionalProperties: true }])
      expect(() => parseCatalogPublicationManifest({ ...manifestInput, operations: [{ ...manifestInput.operations[0], responseSchema }] })).toThrow();
    expect(() =>
      parseCatalogPublicationManifest({ ...manifestInput, operations: [manifestInput.operations[0], manifestInput.operations[0]] }),
    ).toThrow();
  });
  it("rechecks every execution and uses rotated service tokens separately from caller", async () => {
    let service = "service-a";
    const calls: any[] = [];
    const client = new CatalogPublicationClient({
      controllerBase: "http://localhost:3200",
      rttNode: "some-service",
      version: "1.0.0",
      instanceId: "worker",
      manifest: parseCatalogPublicationManifest(manifestInput),
      tokenProvider: { getAccessToken: async () => service },
      fetch: async (url, init) => {
        calls.push({ url: String(url), headers: init?.headers, body: JSON.parse(String(init?.body)) });
        return new Response(JSON.stringify({ data: [] }));
      },
    });
    const transform = vi.fn(async ({ history }: any) => {
      await history("forecast");
      service = "service-b";
      return { count: 0 };
    });
    expect(await client.execute("stoppages", input, "caller", transform)).toEqual({ count: 0 });
    expect(calls).toHaveLength(3);
    expect(calls[0].headers.Authorization).toBe("Bearer service-a");
    expect(calls[2].headers.Authorization).toBe("Bearer service-b");
    expect(calls[1].body.callerToken).toBe("caller");
    expect(calls[1].body.sourceId).toBe("forecast");
  });
  it("bounds a stalled read-only transform and supplies its abort signal", async () => {
    const manifest = parseCatalogPublicationManifest({ ...manifestInput, operations: [{ ...manifestInput.operations[0], timeoutMs: 500 }] });
    let signal: AbortSignal | undefined;
    const client = new CatalogPublicationClient({
      controllerBase: "http://localhost:3200",
      rttNode: "worker",
      version: "1.0.0",
      instanceId: "worker",
      manifest,
      tokenProvider: { getAccessToken: async () => "service" },
      fetch: async () => new Response("{}"),
    });
    await expect(
      client.execute("stoppages", input, "caller", async (ctx) => {
        signal = ctx.signal;
        return new Promise<never>(() => {});
      }),
    ).rejects.toThrow("catalog_execution_timeout");
    expect(signal?.aborted).toBe(true);
  });
  it("does not fall back after denial, or pass unreviewed output", async () => {
    const transform = vi.fn(async () => ({ count: 0 }));
    const client = new CatalogPublicationClient({
      controllerBase: "http://localhost:3200",
      rttNode: "worker",
      version: "1.0.0",
      instanceId: "worker",
      manifest: parseCatalogPublicationManifest(manifestInput),
      tokenProvider: { getAccessToken: async () => "" },
      fetch: vi.fn(),
    });
    await expect(client.execute("stoppages", input, "caller", transform)).rejects.toThrow();
    expect(transform).not.toHaveBeenCalled();
  });
});
