import jwt from "jsonwebtoken";
import { generateKeyPairSync, randomUUID } from "node:crypto";
import { type Server, createServer } from "node:http";
import { afterEach, describe, expect, it } from "vitest";

import {
  OPERATOR_API_EXTENSION,
  OPERATOR_AUTH_SCHEME,
  OPERATOR_DELEGATION_CLAIM,
  OPERATOR_JWT_AUDIENCE,
  OPERATOR_JWT_TYPE,
  operatorDigest,
} from "../../uns-core/src/runtime/operator-api.js";
import UnsApiProxy from "../src/uns-api-proxy.js";

const empty = { type: "object", properties: {}, additionalProperties: false };
const schema = (action: string, command = false) => ({
  type: "object",
  additionalProperties: false,
  properties: { action: { type: "string", const: action }, ...(command ? { requestId: { type: "string", format: "uuid" } } : {}) },
  required: ["action", ...(command ? ["requestId"] : [])],
});
const owner = { controllerName: "controller-b", rttNode: "fixture-a", version: "v1.2.3", instanceId: "target-a", launchId: randomUUID() };
const principalId = randomUUID();
const keys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const publicJwk = { ...keys.publicKey.export({ format: "jwk" }), kid: "operator-http-fixture", alg: "RS256", use: "sig" };
const resources: Array<() => Promise<void>> = [];
let active = true,
  statusUnavailable = false,
  statusChecks = 0;
const restore: Record<string, string | undefined> = {};
afterEach(async () => {
  for (const cleanup of resources.splice(0).reverse()) await cleanup();
  for (const [key, value] of Object.entries(restore)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
    delete restore[key];
  }
  active = true;
  statusUnavailable = false;
  statusChecks = 0;
});
function env(key: string, value: string) {
  if (!(key in restore)) restore[key] = process.env[key];
  process.env[key] = value;
}
async function listen(server: Server): Promise<string> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  resources.push(
    () =>
      new Promise<void>((resolve, reject) => {
        server.close((e) => (e ? reject(e) : resolve()));
        server.closeAllConnections();
      }),
  );
  return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
}
function native(role = "operator") {
  return jwt.sign({ sub: principalId, role, accessRules: ["#"] }, keys.privateKey, { algorithm: "RS256", keyid: publicJwk.kid, expiresIn: 60 });
}
function delegated(operation: any, payload: unknown, change: Record<string, unknown> = {}) {
  const now = Math.floor(Date.now() / 1000);
  return jwt.sign(
    {
      sub: principalId,
      role: "operator",
      serviceToken: true,
      identityType: "machine",
      workloadIdentityType: "CONTROLLER_MCP",
      machineIdentityId: principalId,
      tokenVersion: 1,
      iat: now,
      exp: now + 30,
      accessRules: [operation.path],
      [OPERATOR_DELEGATION_CLAIM]: {
        protocol: 1,
        target: owner,
        operationId: operation.id,
        operationDigest: operatorDigest(operation),
        requestDigest: operatorDigest(payload),
        tool: operation.kind === "command" ? "controller_execute_runtime_operator_command" : "controller_read_runtime_operator_operation",
      },
      ...change,
    },
    keys.privateKey,
    { algorithm: "PS256", keyid: publicJwk.kid, header: { alg: "PS256", typ: OPERATOR_JWT_TYPE }, audience: OPERATOR_JWT_AUDIENCE },
  );
}
type JwksFault = "busy" | "reset" | "slow-headers" | "slow-body" | "outage";
async function fixture(service = "fixture-a", action = "pause", jwksFault?: JwksFault) {
  let jwksRequests = 0;
  const auth = await listen(
    createServer((req, res) => {
      res.setHeader("content-type", "application/json");
      if (req.url === "/jwks") {
        res.end(JSON.stringify({ keys: [publicJwk] }));
        return;
      }
      if (req.url === "/operator-jwks") {
        jwksRequests++;
        const body = JSON.stringify({ keys: [{ ...publicJwk, alg: "PS256" }] });
        if (jwksFault && (jwksRequests === 1 || jwksFault === "outage")) {
          if (jwksFault === "busy" || jwksFault === "outage") {
            res.writeHead(503).end("{}");
            return;
          }
          if (jwksFault === "reset") {
            req.socket.destroy();
            return;
          }
          if (jwksFault === "slow-body") {
            res.writeHead(200);
            res.flushHeaders();
          }
          const timer = setTimeout(() => {
            if (!res.destroyed) res.end(body);
          }, 1_500);
          resources.push(async () => {
            clearTimeout(timer);
          });
          return;
        }
        res.end(body);
        return;
      }
      if (req.method !== "POST") {
        res.writeHead(405);
        res.end("{}");
        return;
      }
      statusChecks++;
      if (statusUnavailable) {
        res.writeHead(503);
        res.end("{}");
        return;
      }
      try {
        jwt.verify(req.headers.authorization?.replace(/^Bearer /, "") ?? "", keys.publicKey, { algorithms: ["RS256", "PS256"] });
      } catch {
        res.writeHead(401);
        res.end("{}");
        return;
      }
      res.writeHead(active ? 200 : 403);
      res.end(JSON.stringify({ protocolVersion: 1, active }));
    }),
  );
  env("RTT_NODE", owner.rttNode);
  env("RTT_INSTANCE_ID", owner.instanceId);
  env("UNS_ROUTE_CONTROLLER_NAME", owner.controllerName);
  env("UNS_ROUTE_LAUNCH_ID", owner.launchId);
  env("version", owner.version);
  const proxy: any = new UnsApiProxy(service, "operatorApi", {
    jwks: { wellKnownJwksUrl: auth + "/jwks" },
    operatorAuthorizationStatusUrl: auth + "/status",
    operatorJwksUrl: auth + "/operator-jwks",
  });
  resources.push(async () => {
    await proxy.stop();
    if (proxy.app.server.listening) await new Promise<void>((resolve) => proxy.app.server.close(resolve));
  });
  while (!proxy.app.server.listening) await new Promise((r) => setTimeout(r, 5));
  await proxy.get("example" as any, service as any, "service" as any, "main" as any, "status" as any, {
    operatorOperations: [{ protocol: 1, id: "status", title: "Operator state", kind: "read", requestSchema: empty }],
  });
  await proxy.post("example" as any, service as any, "service" as any, "main" as any, "control" as any, {
    operatorOperations: [
      { protocol: 1, id: "preview", title: "Preview", kind: "read", requestSchema: schema("preview") },
      {
        protocol: 1,
        id: action,
        title: "Reviewed command",
        kind: "command",
        requestSchema: schema(action, true),
        writeDomain: "policy",
        replay: "request-id-v1",
      },
    ],
  });
  await proxy.post("example" as any, service as any, "service" as any, "main" as any, "legacy" as any);
  let effects = 0;
  proxy.event.on("apiGetEvent", ({ res }: any) => res.json({ state: "ready" }));
  proxy.event.on("apiPostEvent", ({ req, res }: any) => {
    if (req.body.action !== "preview") effects++;
    res.json({ action: req.body.action, effects });
  });
  const publicPath = `/api/example/${service}/service/main/control`;
  const descriptors = proxy.app.swaggerSpec.paths[publicPath].post[OPERATOR_API_EXTENSION];
  return {
    proxy,
    publicPath,
    descriptors,
    url: `http://127.0.0.1:${proxy.app.server.address().port}`,
    effects: () => effects,
    jwksRequests: () => jwksRequests,
  };
}
function post(f: Awaited<ReturnType<typeof fixture>>, payload: unknown, token?: string, path = f.publicPath, forcedScheme?: string) {
  return fetch(f.url + path, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token
        ? {
            authorization: `${forcedScheme ?? (jwt.decode(token, { complete: true })?.header.alg === "PS256" ? OPERATOR_AUTH_SCHEME : "Bearer")} ${token}`,
          }
        : {}),
    },
    body: JSON.stringify(payload),
  });
}

describe("actual SDK HTTP/JWKS operator endpoints", () => {
  it("advertises only explicit operator declarations, never every registered API", async () => {
    const f = await fixture();
    expect(f.descriptors.map((d: any) => d.id)).toEqual(["preview", "pause"]);
    expect(f.proxy.app.swaggerSpec.paths[f.publicPath.replace(/control$/, "legacy")].post[OPERATOR_API_EXTENSION]).toBeUndefined();
  });
  it("requires authenticated operator authority, preserving ordinary legacy APIs", async () => {
    const f = await fixture();
    const body = { action: "pause", requestId: randomUUID() };
    expect((await post(f, body)).status).toBe(401);
    expect((await post(f, body, native("user"))).status).toBe(403);
    expect((await post(f, body, native())).status).toBe(200);
    expect((await post(f, body, native("user"), f.publicPath.replace(/control$/, "legacy"))).status).toBe(200);
    expect(f.effects()).toBe(2);
  });
  it("accepts real scoped delegated calls for unrelated services through the same SDK", async () => {
    for (const [service, action] of [
      ["fixture-a", "pause"],
      ["different-service", "hold"],
    ]) {
      const f = await fixture(service, action);
      const body = { action, requestId: randomUUID() };
      expect((await post(f, body, delegated(f.descriptors[1], body))).status).toBe(200);
      expect(f.effects()).toBe(1);
    }
    expect(statusChecks).toBe(2);
  });
  it.each(["busy", "reset", "slow-headers", "slow-body"] as const)(
    "recovers real JWKS %s before dispatch, without retrying the command",
    async (fault) => {
      const f = await fixture("fixture-a", "pause", fault);
      const body = { action: "pause", requestId: randomUUID() };
      const response = await post(f, body, delegated(f.descriptors[1], body));
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ effects: 1 });
      expect(f.jwksRequests()).toBe(2);
      expect(statusChecks).toBe(1);
      expect(f.effects()).toBe(1);
    },
  );
  it("uses the same recovery for unrelated applications and still checks revocation on cached keys", async () => {
    for (const [service, action] of [
      ["fixture-a", "pause"],
      ["different-service", "hold"],
    ]) {
      active = true;
      const f = await fixture(service, action, "busy");
      const body = { action, requestId: randomUUID() };
      const token = delegated(f.descriptors[1], body);
      expect((await post(f, body, token)).status).toBe(200);
      active = false;
      expect((await post(f, body, token)).status).toBe(403);
      expect(f.effects()).toBe(1);
      expect(f.jwksRequests()).toBe(2);
    }
    expect(statusChecks).toBe(4);
  });
  it("does not use a recovered key as authority for a revoked parent", async () => {
    const f = await fixture("fixture-a", "pause", "busy");
    const body = { action: "pause", requestId: randomUUID() };
    active = false;
    expect((await post(f, body, delegated(f.descriptors[1], body))).status).toBe(403);
    expect(f.jwksRequests()).toBe(2);
    expect(statusChecks).toBe(1);
    expect(f.effects()).toBe(0);
  });
  it("returns a bounded 503 on persistent JWKS outage without running a handler", async () => {
    const f = await fixture("fixture-a", "pause", "outage");
    const body = { action: "pause", requestId: randomUUID() };
    const token = delegated(f.descriptors[1], body);
    const response = await post(f, body, token);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "operator_jwks_unavailable" });
    expect((await post(f, body, token)).status).toBe(503);
    expect(f.jwksRequests()).toBe(2);
    expect(statusChecks).toBe(0);
    expect(f.effects()).toBe(0);
  });
  it("prevents a read delegation from invoking a command at the same POST endpoint", async () => {
    const f = await fixture();
    const preview = { action: "preview" };
    const token = delegated(f.descriptors[0], preview);
    expect((await post(f, preview, token)).status).toBe(200);
    expect((await post(f, { action: "pause", requestId: randomUUID() }, token)).status).toBe(403);
    expect(f.effects()).toBe(0);
  });
  it("rejects altered payload, expired/wrong-launch delegation and unsigned JWTs", async () => {
    const f = await fixture();
    const body = { action: "pause", requestId: randomUUID() };
    const op = f.descriptors[1];
    expect((await post(f, { ...body, requestId: randomUUID() }, delegated(op, body))).status).toBe(403);
    const now = Math.floor(Date.now() / 1000);
    expect((await post(f, body, delegated(op, body, { iat: now - 50, exp: now - 20 }))).status).toBe(401);
    const claim = (jwt.decode(delegated(op, body)) as any)[OPERATOR_DELEGATION_CLAIM];
    expect(
      (await post(f, body, delegated(op, body, { [OPERATOR_DELEGATION_CLAIM]: { ...claim, target: { ...owner, launchId: randomUUID() } } }))).status,
    ).toBe(403);
    expect((await post(f, body, jwt.sign({ role: "operator", sub: principalId, accessRules: ["#"] }, "untrusted", { expiresIn: 30 }))).status).toBe(
      401,
    );
    expect(f.effects()).toBe(0);
  });
  it("checks current authorization and fails closed on revocation or authority outage", async () => {
    const f = await fixture();
    const body = { action: "pause", requestId: randomUUID() };
    const token = delegated(f.descriptors[1], body);
    active = false;
    expect((await post(f, body, token)).status).toBe(403);
    active = true;
    statusUnavailable = true;
    expect((await post(f, body, token)).status).toBe(503);
    statusUnavailable = false;
    expect((await post(f, body, token)).status).toBe(200);
    expect(f.effects()).toBe(1);
  });
  it("rejects the operator credential through ordinary bearer or legacy RSA paths", async () => {
    const f = await fixture();
    const body = { action: "pause", requestId: randomUUID() };
    const token = delegated(f.descriptors[1], body);
    expect((await post(f, body, token, f.publicPath, "Bearer")).status).toBe(401);
    expect((await post(f, body, native(), f.publicPath, OPERATOR_AUTH_SCHEME)).status).toBe(401);
    const claims = jwt.decode(token) as any;
    const wrongPurpose = jwt.sign(claims, keys.privateKey, { algorithm: "RS256", keyid: publicJwk.kid });
    expect((await post(f, body, wrongPurpose)).status).toBe(403);
    expect(f.effects()).toBe(0);
  });
  it("never accepts delegated authority on an undeclared legacy endpoint", async () => {
    const f = await fixture();
    const body = { action: "pause", requestId: randomUUID() };
    const op = { ...f.descriptors[1], path: f.descriptors[1].path.replace(/control$/, "legacy") };
    expect((await post(f, body, delegated(op, body), f.publicPath.replace(/control$/, "legacy"))).status).toBe(403);
    expect(f.effects()).toBe(0);
  });
});
