import { afterEach, describe, expect, it } from "vitest";
import http, { type IncomingMessage, type ServerResponse } from "node:http";
import { UnsClient } from "../packages/uns-core/src/index.js";

const readJsonBody = async (req: IncomingMessage): Promise<Record<string, unknown>> => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf-8"));
};

describe("UnsClient datahub endpoints", () => {
  const servers: http.Server[] = [];

  afterEach(async () => {
    while (servers.length > 0) {
      const server = servers.pop();
      if (!server) continue;
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    }
  });

  it("uses the catchall batch last endpoint", async () => {
    const receivedPaths: string[] = [];
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      receivedPaths.push(req.url || "");
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        results: [
          {
            topic: "plant/line/asset/type/id/current",
            value: 42,
            values: { value: 42 },
            source: "cache",
          },
        ],
      }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}/api`, { token: "access-token" });

    const response = await client.lastValue("plant/line/asset/type/id/current");

    expect(receivedPaths).toEqual(["/api/catchall/batch/last"]);
    expect(response?.["plant/line/asset/type/id/current"].value).toBe(42);
  });

  it("fetches single-topic getAttributeData with encoded topic path and query params", async () => {
    let receivedUrl = "";
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      receivedUrl = req.url || "";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        data: [
          ["2026-05-01T06:36:39.690000Z", 1, ""],
        ],
        stats: {
          table: "uns_sij_hrm_furnace_data",
          raw: {
            columns: [
              { name: "timestamp", type: "TIMESTAMP" },
              { name: "value", type: "DOUBLE" },
              { name: "uom", type: "VARCHAR" },
            ],
          },
        },
      }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}`, { token: "access-token" });

    const response = await client.getAttributeData("sij/acroni/vv/hrm-furnace/equipment/pusher/output-quantity", {
      from: "2026-05-07T11:17:01.157Z",
      to: "2026-05-07T11:22:01.157Z",
      table: "uns_sij_hrm_furnace_data",
      aggregate: "last",
      summaryOnly: false,
      dedupe: false,
    });

    expect(receivedUrl).toContain("/api/catchall/sij%2Facroni%2Fvv%2Fhrm-furnace%2Fequipment%2Fpusher%2Foutput-quantity");
    expect(receivedUrl).toContain("table=uns_sij_hrm_furnace_data");
    expect(receivedUrl).toContain("aggregate=last");
    expect(response?.stats?.table).toBe("uns_sij_hrm_furnace_data");
    expect(response?.toRecords()[0].value).toBe(1);
  });

  it("fetches generic custom data with getData", async () => {
    let receivedUrl = "";
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      receivedUrl = req.url || "";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        items: [{ id: "pdo-1", grade: "A" }],
      }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}/api`, { token: "access-token" });

    const response = await client.getData("/projects/project-name/path-to-data/data", {
      fromDate: "20260325",
    });
    const payload = await response.json();

    expect(receivedUrl).toBe("/api/projects/project-name/path-to-data/data?fromDate=20260325");
    expect(payload.items).toEqual([{ id: "pdo-1", grade: "A" }]);
  });

  it("fetches history results with request metadata", async () => {
    let receivedPath = "";
    let receivedBody: Record<string, unknown> = {};
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      receivedPath = req.url || "";
      receivedBody = await readJsonBody(req);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        results: [
          {
            topic: "plant/line/asset/type/id/current",
            data: [["2026-05-01T06:36:39.690000Z", 42, "A"]],
            stats: {
              table: "uns_line_data",
              raw: {
                columns: [
                  { name: "timestamp" },
                  { name: "value" },
                  { name: "uom" },
                ],
              },
            },
          },
        ],
        stats: { requested: 1, succeeded: 1, failed: 0 },
      }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}/api`, { token: "access-token" });

    const response = await client.history(["plant/line/asset/type/id/current"], {
      from: "2026-05-07T11:17:01.157Z",
      to: "2026-05-07T11:22:01.157Z",
      limit: 500,
      dedupe: false,
    });

    expect(receivedPath).toBe("/api/catchall/batch/range");
    expect(receivedBody.topics).toEqual(["plant/line/asset/type/id/current"]);
    expect(receivedBody.limit).toBe(500);
    expect(response?.results[0].data?.[0][1]).toBe(42);
    expect(response?.byTopic["plant/line/asset/type/id/current"].stats?.table).toBe("uns_line_data");
  });

  it("resolves a reviewed provider identity into publish-ready Asset metadata", async () => {
    let receivedPath = "";
    let receivedAuth = "";
    let receivedBody: Record<string, unknown> = {};
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      receivedPath = req.url || "";
      receivedAuth = req.headers.authorization || "";
      receivedBody = await readJsonBody(req);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        data: {
          IssueAssetIdentityPublicationProofByExternalIdentity: {
            proof: "signed-proof",
            stableEntityId: "11111111-1111-4111-8111-111111111111",
            candidateAssetPath: "enterprise/new/RESS-14",
            expiresAt: "2026-09-08T21:30:00.000Z",
          },
        },
      }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}/api`, { token: "workload-token" });

    const metadata = await client.issueAssetIdentityPublicationProofByExternalIdentity({
      providerId: "sap.connector",
      externalSystem: "sap.s4hana",
      externalType: "equipment.number",
      externalId: "RESS-14",
    }, "enterprise/new/RESS-14");

    expect(receivedPath).toBe("/graphql");
    expect(receivedAuth).toBe("Bearer workload-token");
    expect(receivedBody.variables).toEqual({
      input: {
        providerId: "sap.connector",
        externalSystem: "sap.s4hana",
        externalType: "equipment.number",
        externalId: "RESS-14",
        candidateAssetPath: "enterprise/new/RESS-14",
      },
    });
    expect(metadata).toEqual({
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetIdentityProof: "signed-proof",
      candidateAssetPath: "enterprise/new/RESS-14",
      expiresAt: "2026-09-08T21:30:00.000Z",
    });
  });

  it("surfaces GraphQL proof errors without returning partial identity metadata", async () => {
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      await readJsonBody(req);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ errors: [{ message: "The reviewed provider identity is unavailable or inactive." }] }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);

    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}`, { token: "workload-token" });

    await expect(client.issueAssetIdentityPublicationProof(
      "11111111-1111-4111-8111-111111111111",
      "enterprise/new/RESS-14",
    )).rejects.toThrow("reviewed provider identity is unavailable or inactive");
  });

  it("returns publish-ready provider candidate evidence without manufacturing a stable ID", async () => {
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      await readJsonBody(req);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: {
        IssueAssetIdentityPublicationEvidenceByExternalIdentity: {
          mode: "provider-candidate",
          proof: "signed-provider-proof",
          stableEntityId: null,
          providerId: "sap.connector",
          externalSystem: "sap.s4hana",
          externalType: "equipment.number",
          externalId: "RESS-14",
          candidateAssetPath: "enterprise/new/RESS-14",
          expiresAt: "2026-09-09T08:05:00.000Z",
        },
      } }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}`, { token: "workload-token" });

    await expect(client.issueAssetIdentityPublicationEvidenceByExternalIdentity({
      providerId: "sap.connector",
      externalSystem: "sap.s4hana",
      externalType: "equipment.number",
      externalId: "RESS-14",
    }, "enterprise/new/RESS-14")).resolves.toEqual({
      assetProviderIdentity: {
        providerId: "sap.connector",
        externalSystem: "sap.s4hana",
        externalType: "equipment.number",
        externalId: "RESS-14",
      },
      assetProviderIdentityProof: "signed-provider-proof",
      candidateAssetPath: "enterprise/new/RESS-14",
      expiresAt: "2026-09-09T08:05:00.000Z",
    });
  });

  it("keeps the same evidence helper stable when a provider ID is already mapped", async () => {
    const server = http.createServer(async (req: IncomingMessage, res: ServerResponse) => {
      await readJsonBody(req);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ data: {
        IssueAssetIdentityPublicationEvidenceByExternalIdentity: {
          mode: "stable",
          proof: "signed-stable-proof",
          stableEntityId: "11111111-1111-4111-8111-111111111111",
          providerId: "sap.connector",
          externalSystem: "sap.s4hana",
          externalType: "equipment.number",
          externalId: "RESS-14",
          candidateAssetPath: "enterprise/new/RESS-14",
          expiresAt: "2026-09-09T08:05:00.000Z",
        },
      } }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    servers.push(server);
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const client = new UnsClient(`http://127.0.0.1:${port}`, { token: "workload-token" });

    await expect(client.issueAssetIdentityPublicationEvidenceByExternalIdentity({
      providerId: "sap.connector",
      externalSystem: "sap.s4hana",
      externalType: "equipment.number",
      externalId: "RESS-14",
    }, "enterprise/new/RESS-14")).resolves.toEqual({
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetIdentityProof: "signed-stable-proof",
      candidateAssetPath: "enterprise/new/RESS-14",
      expiresAt: "2026-09-09T08:05:00.000Z",
    });
  });
});
