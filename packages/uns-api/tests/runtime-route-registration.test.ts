import { randomUUID } from "node:crypto";
import { mkdtemp, writeFile, chmod, symlink, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RuntimeRouteRegistration, runtimeRouteRegistrationEnvironment } from "../src/runtime-route-registration.js";
import { readRuntimeRouteReceipt } from "../../uns-core/src/runtime/route-registration.js";
import UnsApiProxy from "../src/uns-api-proxy.js";

const deployment = { controllerName: "controller-b", rttNode: "unrelated-fixture", version: "1.2.3", instanceId: "one", launchId: randomUUID() };
const cleanup: Array<() => Promise<unknown> | void> = [];
afterEach(async () => { for (const f of cleanup.splice(0).reverse()) await f(); vi.unstubAllEnvs(); });
async function credential() {
  const dir = await mkdtemp(join(tmpdir(), "uns-route-sdk-"));
  cleanup.push(() => rm(dir, { recursive: true, force: true }));
  const tokenFile = join(dir, "route.jwt"); await writeFile(tokenFile, "local-purpose-token", { mode: 0o600 }); return tokenFile;
}
const receipt = (options: any, patch = {}) => ({ protocolVersion: 1, expiresInMs: 30000, binding: {
  processName: options.processName, controllerName: options.controllerName, version: options.version,
  instanceId: options.instanceId, apiHost: options.apiHost, controllerPublicBase: options.controllerBase,
  deploymentId: options.launchId + ":123:456", ...patch,
} });
async function listen(server: Server) {
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  cleanup.push(() => new Promise<void>(resolve => { server.close(() => resolve()); server.closeAllConnections(); }));
  return `http://127.0.0.1:${(server.address() as any).port}`;
}
async function options(extra: Record<string, any> = {}) {
  return { ...deployment, processName: "fixture-a", apiHost: "http://127.0.0.1:40001", controllerBase: "http://127.0.0.1:40002",
    tokenFile: await credential(), timeoutMs: 200, ...extra };
}
function client(options: any) { const r = new RuntimeRouteRegistration(options); cleanup.push(() => r.stop()); return r; }

describe("shared launch route registration", () => {
  it.each(["generic-pump", "independent-report"])("automatically registers declared %s operations from the actual API launch", async processName => {
    const tokenFile = await credential(); const calls: any[] = []; let expected: any;
    const controllerBase = await listen(createServer(async (req, res) => {
      const parts = []; for await (const part of req) parts.push(part);
      const body = JSON.parse(Buffer.concat(parts).toString()); calls.push({ path: req.url, body, authorization: req.headers.authorization });
      res.writeHead(201, { "content-type": "application/json" }); res.end(JSON.stringify(receipt({ ...expected, apiHost: body.apiHost })));
    }));
    for (const [key, value] of Object.entries({ UNS_ROUTE_TOKEN_FILE: tokenFile, UNS_CONTROLLER_PUBLIC_BASE: controllerBase,
      UNS_ROUTE_CONTROLLER_NAME: deployment.controllerName, RTT_NODE: processName, version: deployment.version,
      RTT_INSTANCE_ID: deployment.instanceId, UNS_ROUTE_LAUNCH_ID: deployment.launchId })) vi.stubEnv(key, value);
    expected = { ...deployment, processName, controllerBase };
    const proxy: any = new UnsApiProxy(processName, "operator", { jwtSecret: "synthetic-test-secret", publishedApiHost: "127.0.0.1" });
    cleanup.push(() => proxy.stop());
    const empty = { type: "object", properties: {}, additionalProperties: false };
    await proxy.get("example" as any, "asset" as any, "service" as any, "main" as any, "state" as any, {
      operatorOperations: [{ protocol: 1, id: "state", title: "Read state", kind: "read", requestSchema: empty }],
    });
    for (let n = 0; !proxy.operatorRouteRegistration.ready && n < 100; n++) await new Promise(resolve => setTimeout(resolve, 5));
    expect(proxy.operatorRouteRegistration.ready).toBe(true);
    expect(calls).toEqual([{ path: "/api/runtime-services/routes", body: { apiHost: `http://127.0.0.1:${proxy.app.server.address().port}` },
      authorization: "Bearer local-purpose-token" }]);
    await proxy.stop(); expect(proxy.operatorRouteRegistration.ready).toBe(false);
  });
  it("does not activate registration for ordinary endpoints even with managed launch metadata", async () => {
    vi.stubEnv("UNS_ROUTE_TOKEN_FILE", "/missing/not-consumed-by-ordinary-api");
    const proxy: any = new UnsApiProxy("legacy-api", "ordinary", { jwtSecret: "synthetic-test-secret" });
    cleanup.push(() => proxy.stop());
    await proxy.get("example" as any, "asset" as any, "service" as any, "main" as any, "state" as any);
    expect(proxy.operatorRouteRegistration).toBeUndefined();
  });
  it("keeps standalone/legacy startup opt-out and validates an opted-in launch", () => {
    expect(runtimeRouteRegistrationEnvironment({ RTT_NODE: "legacy" })).toBeUndefined();
    expect(() => runtimeRouteRegistrationEnvironment({ UNS_ROUTE_TOKEN_FILE: "/tmp/token" })).toThrow("managed runtime route environment");
    const env = { UNS_ROUTE_TOKEN_FILE: "/tmp/token", UNS_CONTROLLER_PUBLIC_BASE: "https://owner.example", UNS_ROUTE_CONTROLLER_NAME: deployment.controllerName,
      RTT_NODE: deployment.rttNode, version: deployment.version, RTT_INSTANCE_ID: deployment.instanceId, UNS_ROUTE_LAUNCH_ID: deployment.launchId };
    expect(runtimeRouteRegistrationEnvironment(env)).toMatchObject({ ...deployment, controllerBase: "https://owner.example" });
    for (const base of ["https://user:password@owner.example", "https://owner.example/other", "file:///tmp/token", "https://owner.example/?token=private"]) {
      expect(() => runtimeRouteRegistrationEnvironment({ ...env, UNS_CONTROLLER_PUBLIC_BASE: base })).toThrow("managed runtime route environment");
    }
  });
  it.each(["1.2.3", "v1.2.3"])("retains the exact launch version %s and rejects a different launch/origin", async version => {
    const o = await options({ version });
    expect(readRuntimeRouteReceipt(receipt(o), o).binding.version).toBe(version);
    for (const mismatch of [{ version: version.startsWith("v") ? version.slice(1) : "v" + version }, { instanceId: "other" },
      { processName: "other" }, { apiHost: "http://elsewhere:40001" }, { controllerName: "other" }, { deploymentId: randomUUID() + ":123:456" }]) {
      expect(() => readRuntimeRouteReceipt(receipt(o, mismatch), o)).toThrow();
    }
  });
  it("rereads rotated credentials, renews after a lost response and sends no application grants", async () => {
    const calls: Array<{ url: string; authorization?: string; body: any }> = [];
    let o: any;
    const controllerBase = await listen(createServer(async (req, res) => {
      const parts = []; for await (const part of req) parts.push(part);
      calls.push({ url: req.url!, authorization: req.headers.authorization, body: JSON.parse(Buffer.concat(parts).toString()) });
      if (calls.length === 1) { req.socket.destroy(); return; }
      res.writeHead(201, { "content-type": "application/json" }); res.end(JSON.stringify(receipt(o)));
    }));
    o = await options({ controllerBase }); const r = client(o);
    expect(await r.renew()).toBe(false);
    await writeFile(o.tokenFile, "rotated-purpose-token");
    expect(await r.renew()).toBe(true); expect(r.ready).toBe(true);
    expect(calls).toEqual([
      { url: "/api/runtime-services/routes", authorization: "Bearer local-purpose-token", body: { apiHost: o.apiHost } },
      { url: "/api/runtime-services/routes", authorization: "Bearer rotated-purpose-token", body: { apiHost: o.apiHost } },
    ]);
  });
  it("refuses symlink, oversized and publicly readable credentials without sending them", async () => {
    const fetch = vi.fn(); const o = await options({ fetch });
    await chmod(o.tokenFile, 0o644); expect(await client(o).renew()).toBe(false);
    await chmod(o.tokenFile, 0o600); await writeFile(o.tokenFile, "x".repeat(16385)); expect(await client(o).renew()).toBe(false);
    const link = o.tokenFile + ".link"; await symlink(o.tokenFile, link);
    expect(await client({ ...o, tokenFile: link }).renew()).toBe(false); expect(fetch).not.toHaveBeenCalled();
  });
  it("bounds a transport that ignores abort and coalesces all later renewals", async () => {
    const fetch = vi.fn(() => new Promise<Response>(() => {})); const r = client(await options({ fetch, timeoutMs: 30 }));
    const first = r.renew(); expect(r.renew()).toBe(first); expect(await first).toBe(false);
    for (let i = 0; i < 100; i++) expect(await r.renew()).toBe(false);
    expect(fetch).toHaveBeenCalledTimes(1); r.stop(); expect(await r.renew()).toBe(false);
  });
  it("bounds a stalled response body and Stop cancels pending work without waiting for the Controller", async () => {
    let cancelled = false;
    const fetch = vi.fn(async () => new Response(new ReadableStream({ cancel() { cancelled = true; } }), { status: 201 }));
    const r = client(await options({ fetch, timeoutMs: 30 })); expect(await r.renew()).toBe(false); expect(cancelled).toBe(true);
    const pendingFetch = vi.fn(() => new Promise<Response>(() => {}));
    const stopping = client(await options({ fetch: pendingFetch, timeoutMs: 10000 })); const pending = stopping.renew(); stopping.stop();
    expect(await pending).toBe(false); expect(stopping.ready).toBe(false);
  });
  it("rejects mismatched receipts, oversized bodies and redirects while allowing a subsequent healthy renewal", async () => {
    const o = await options();
    const fetch = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify(receipt(o, { instanceId: "wrong" })), { status: 201 }))
      .mockResolvedValueOnce(new Response("x".repeat(9000), { status: 201 }))
      .mockResolvedValueOnce(new Response(null, { status: 302, headers: { location: "http://elsewhere" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(receipt(o)), { status: 201 }));
    const r = client({ ...o, fetch });
    expect(await r.renew()).toBe(false); expect(await r.renew()).toBe(false); expect(await r.renew()).toBe(false);
    expect(await r.renew()).toBe(true); expect(fetch.mock.calls[0][1].redirect).toBe("error");
  });
});
