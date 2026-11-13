import { CatalogPublicationClient } from "@uns-kit/core/runtime/catalog-publication-client.js";
import { parseCatalogPublicationManifest } from "@uns-kit/core/runtime/catalog-publication.js";
import express from "express";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it } from "vitest";

import { catalogPublicationHandler } from "../src/catalog-publication-handler.js";

const manifest = parseCatalogPublicationManifest({
  protocol: 1,
  operations: [
    {
      id: "summary",
      title: "Quality summary",
      method: "GET",
      path: "/api/quality/summary",
      mode: "approved-service",
      requestSchema: {
        type: "object",
        additionalProperties: false,
        required: ["from", "to"],
        properties: { from: { type: "string", format: "date-time" }, to: { type: "string", format: "date-time" } },
      },
      responseSchema: { type: "object", additionalProperties: false, required: ["count"], properties: { count: { type: "integer" } } },
      sources: [{ id: "samples", kind: "history", topic: "quality/private" }],
    },
  ],
});
describe("catalog HTTP helper", () => {
  it("requires a caller, validates closed output, and disables caching", async () => {
    let leak = false,
      revoked = false,
      reads = 0;
    const client = new CatalogPublicationClient({
      controllerBase: "http://local.invalid",
      rttNode: "quality-service",
      instanceId: "quality",
      version: "1.0.0",
      manifest,
      tokenProvider: { getAccessToken: async () => "service" },
      fetch: async (_url, init) => {
        const b = JSON.parse(String(init?.body));
        if (revoked || b.callerToken !== "catalog-reader") return new Response("{}", { status: 403 });
        reads++;
        return new Response("{}");
      },
    });
    const app = express();
    app.get(
      "/api/quality/summary",
      catalogPublicationHandler(client, "summary", async () => (leak ? { count: 1, raw: "secret" } : { count: 1 })),
    );
    const server = createServer(app);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/quality/summary?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z`;
    try {
      expect((await fetch(url)).status).toBe(401);
      expect(reads).toBe(0);
      const ok = await fetch(url, { headers: { Authorization: "Bearer catalog-reader" } });
      expect(ok.status).toBe(200);
      expect(ok.headers.get("cache-control")).toBe("no-store");
      expect(await ok.json()).toEqual({ count: 1 });
      leak = true;
      const bad = await fetch(url, { headers: { Authorization: "Bearer catalog-reader" } });
      expect(bad.status).toBe(502);
      expect(await bad.text()).not.toContain("secret");
      revoked = true;
      expect((await fetch(url, { headers: { Authorization: "Bearer catalog-reader" } })).status).toBe(403);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    }
  });
});
