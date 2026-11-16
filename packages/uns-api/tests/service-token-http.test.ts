import jwt from "jsonwebtoken";
import { generateKeyPairSync, randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import UnsApiProxy from "../src/uns-api-proxy.js";

const keys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const jwk = { ...keys.publicKey.export({ format: "jwk" }), kid: "service-http-fixture", alg: "RS256", use: "sig" };
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

async function listen(server: Server): Promise<string> {
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
    server.closeAllConnections();
  }));
  return `http://127.0.0.1:${(server.address() as {port:number}).port}`;
}

async function fixture(withStatus = true) {
  let mode: "active" | "revoked" | "unavailable" | "old-controller" | "wrong-principal" = "active";
  let checks = 0, effects = 0;
  const authority = await listen(createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/jwks") { res.end(JSON.stringify({ keys: [jwk] })); return; }
    if (req.method !== "POST" || req.url !== "/status") { res.writeHead(404).end("{}"); return; }
    checks++;
    if (mode === "unavailable") { res.writeHead(503).end("{}"); return; }
    if (mode === "revoked") { res.writeHead(403).end("{}"); return; }
    res.end(JSON.stringify({ active:true, protocolVersion:1,
      ...(mode === "old-controller" ? {} : {configuredAccessAuthority:2}),
      principalType: mode === "wrong-principal" ? "user" : "machine" }));
  }));
  const proxy: any = new UnsApiProxy("service-fixture", "ordinary", {
    jwks: {wellKnownJwksUrl: authority + "/jwks"},
    // Empty explicitly disables inherited controller configuration for this case.
    operatorAuthorizationStatusUrl: withStatus ? authority + "/status" : "",
  });
  cleanups.push(async () => {
    await proxy.stop();
    if (proxy.app.server.listening) await new Promise<void>(resolve => proxy.app.server.close(resolve));
  });
  while (!proxy.app.server.listening) await new Promise(resolve => setTimeout(resolve, 5));
  for (const method of ["get", "post", "put", "patch", "delete"] as const)
    await proxy[method]("example" as any, "plant" as any, "equipment" as any, "main" as any, "value" as any);
  for (const event of ["apiGetEvent", "apiPostEvent", "apiPutEvent", "apiPatchEvent", "apiDeleteEvent"])
    proxy.event.on(event, ({res}: any) => { effects++; res.json({ok:true}); });
  return {
    url: `http://127.0.0.1:${proxy.app.server.address().port}/api/example/plant/equipment/main/value`,
    setMode: (value: typeof mode) => {mode=value;}, checks: () => checks, effects: () => effects,
  };
}
function token(claims: Record<string, unknown>) {
  return jwt.sign({sub:randomUUID(),accessRules:["#"],...claims}, keys.privateKey,
    {algorithm:"RS256",keyid:jwk.kid,expiresIn:60});
}
const request = (url:string, method:string, bearer:string) => fetch(url, {method,
  headers:{authorization:`Bearer ${bearer}`,"content-type":"application/json"},
  ...(method === "GET" ? {} : {body:"{}"})});

describe("managed service tokens on ordinary SDK HTTP endpoints", () => {
  it("rechecks every method and rejects a token immediately after its authority changes", async () => {
    const f=await fixture(), bearer=token({serviceToken:true,machineIdentityId:randomUUID(),
      configuredService:{protocol:2,operationId:randomUUID()}});
    for (const method of ["GET","POST","PUT","PATCH","DELETE"]) {
      expect((await request(f.url,method,bearer)).status).toBe(200);
      f.setMode("revoked");
      expect((await request(f.url,method,bearer)).status).toBe(403);
      f.setMode("active");
    }
    expect(f.checks()).toBe(10);
    expect(f.effects()).toBe(5);
  });

  it("fails closed when authority is unavailable, old or identifies a different principal", async () => {
    const f=await fixture(), bearer=token({machineIdentityId:randomUUID()});
    for (const mode of ["unavailable","old-controller","wrong-principal"] as const) {
      f.setMode(mode);
      expect((await request(f.url,"GET",bearer)).status).toBe(503);
    }
    expect(f.effects()).toBe(0);
  });

  it("accepts a small concurrent burst when the authority is healthy", async () => {
    const f=await fixture(), bearer=token({machineIdentityId:randomUUID()});
    const responses=await Promise.all(Array.from({length: 8}, () => request(f.url,"GET",bearer)));
    expect(responses.map(response => response.status)).toEqual(Array(8).fill(200));
    expect(f.checks()).toBe(8);
    expect(f.effects()).toBe(8);
  });

  it("protects configured-only and legacy machine claims but leaves ordinary user routes unchanged", async () => {
    const f=await fixture();
    for (const claims of [{configuredService:{protocol:2}}, {serviceToken:true}, {machineIdentityId:randomUUID()}]) {
      f.setMode("revoked");
      expect((await request(f.url,"GET",token(claims))).status).toBe(403);
    }
    f.setMode("unavailable");
    expect((await request(f.url,"GET",token({roles:["user"]}))).status).toBe(200);
    expect(f.checks()).toBe(3);
  });

  it("does not serve a managed service token if no status endpoint is configured", async () => {
    const f=await fixture(false);
    expect((await request(f.url,"GET",token({configuredService:{protocol:2}}))).status).toBe(503);
    expect((await request(f.url,"GET",token({roles:["user"]}))).status).toBe(200);
    expect(f.effects()).toBe(1);
  });
});
