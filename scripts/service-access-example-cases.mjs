import { buildApiInteractionPath, buildApiInteractionsFromDataOfferSources } from "@uns-kit/api";
import { CatalogPublicationClient } from "@uns-kit/core/runtime/catalog-publication-client.js";
import { loadServiceAccessDeclaration } from "@uns-kit/core/runtime/service-access-package.js";
import { ServiceTokenProvider } from "@uns-kit/core/tools/auth/service-token-provider.js";
import express from "express";
import assert from "node:assert/strict";
import { mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";

import { readHistory, sourceTopic } from "./examples/service-access/background-reader/src/reader.mjs";
import { readHistory as readConfiguredHistory, sourceTopic as configuredSourceTopic } from "./examples/service-access/configured-reader/src/reader.mjs";
import { createSummaryOffer } from "./examples/service-access/catalog-summary/src/offer.mjs";

async function token(t) {
  const dir = await mkdtemp(path.join(tmpdir(), "example-credential-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, "workload");
  await writeFile(file, "synthetic-service-one", { mode: 0o600 });
  return { file, provider: new ServiceTokenProvider({ tokenFile: file }) };
}
async function listen(t, handler) {
  const server = createServer(handler);
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(
    () =>
      new Promise((r) => {
        server.close(r);
        server.closeAllConnections();
      }),
  );
  return `http://127.0.0.1:${server.address().port}`;
}
async function body(req) {
  const parts = [];
  for await (const part of req) parts.push(part);
  return JSON.parse(Buffer.concat(parts).toString());
}
const from = "2026-01-01T00:00:00Z",
  to = "2026-01-01T01:00:00Z";

test("configured reader uses its configured path and rejects a path outside its declaration", async (t) => {
  const { provider } = await token(t);
  const seen = [];
  const base = await listen(t, async (req, res) => {
    seen.push(await body(req)); res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({results:[{data:[[1]]}]}));
  });
  const config = {source:{topic:"example/configured/line-1/temperature"}};
  assert.equal(configuredSourceTopic(config), config.source.topic);
  await readConfiguredHistory(base, from, to, {config,tokenProvider:provider});
  assert.deepEqual(seen[0].topics, [config.source.topic]);
  const other = {source:{topic:"example/configured/line-2/pressure"}};
  await readConfiguredHistory(base, from, to, {config:other,tokenProvider:provider});
  assert.deepEqual(seen[1].topics, [other.source.topic]);
  await assert.rejects(readConfiguredHistory(base, from, to, {config:{source:{topic:"private/elsewhere"}},tokenProvider:provider}));
  assert.equal(seen.length, 2);
});

test("background example uses the current file, bounded request and no caller credential", async (t) => {
  const { file, provider } = await token(t);
  const seen = [];
  const base = await listen(t, async (req, res) => {
    const value = await body(req);
    seen.push({ auth: req.headers.authorization, path: req.url, value });
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ results: [{ data: [[1], [2]] }] }));
  });
  const declaration = await loadServiceAccessDeclaration("./examples/service-access/background-reader/package.json");
  assert.deepEqual(declaration.permissions, { scopes: ["read:uns"], accessRules: [sourceTopic] });
  assert.equal(declaration.publications, undefined);
  assert.equal((await readHistory(base, from, to, { tokenProvider: provider })).length, 2);
  await writeFile(file + ".next", "synthetic-service-two", { mode: 0o600 });
  await rename(file + ".next", file);
  await readHistory(base, from, to, { tokenProvider: provider });
  assert.deepEqual(
    seen.map((x) => x.auth),
    ["Bearer synthetic-service-one", "Bearer synthetic-service-two"],
  );
  assert.equal(seen[0].path, "/api/catchall/batch/range");
  assert.deepEqual(seen[0].value, { topics: [sourceTopic], from, to, limit: 100 });
  assert.equal("callerToken" in seen[0].value, false);
});

test("background missing authoritative file and denied API never fall back", async (t) => {
  const { file, provider } = await token(t);
  let calls = 0;
  const base = await listen(t, (_req, res) => {
    calls++;
    res.writeHead(403);
    res.end("{}");
  });
  await assert.rejects(readHistory(base, from, to, { tokenProvider: provider }), /HTTP 403/);
  assert.equal(calls, 1);
  await rm(file);
  await assert.rejects(readHistory(base, from, to, { tokenProvider: provider }), /token file is unavailable/);
  assert.equal(calls, 1);
});

test("background read propagates local shutdown cancellation", async (t) => {
  const { provider } = await token(t);
  const stop = new AbortController();
  let ready;
  const started = new Promise((r) => (ready = r));
  const pending = readHistory("http://127.0.0.1:1", from, to, {
    tokenProvider: provider,
    signal: stop.signal,
    fetch: (_url, options) =>
      new Promise((_r, reject) => {
        ready();
        options.signal.addEventListener("abort", () => reject(new Error("cancelled")), { once: true });
      }),
  });
  await started;
  stop.abort();
  await assert.rejects(pending, /cancelled/);
});

async function catalog(t) {
  const { provider, file } = await token(t);
  const seen = [];
  let deny = false,
    invalid = false;
  const declaration = await loadServiceAccessDeclaration("./examples/service-access/catalog-summary/package.json");
  const base = await listen(t, async (req, res) => {
    const value = await body(req);
    seen.push({ auth: req.headers.authorization, path: req.url, value });
    res.setHeader("Content-Type", "application/json");
    if (deny || value.callerToken !== "synthetic-reader") {
      res.writeHead(403);
      res.end("{}");
      return;
    }
    if (req.url.endsWith("/history"))
      res.end(
        JSON.stringify(
          invalid ? { secret: "must-not-leak" } : { data: [["private-value-one"], ["private-value-two"]], columns: ["value"], truncated: false },
        ),
      );
    else res.end(JSON.stringify({ revision: 1 }));
  });
  const client = new CatalogPublicationClient({
    controllerBase: base,
    rttNode: "example-catalog-summary",
    version: "1.0.0",
    instanceId: "summary",
    manifest: declaration.publications,
    tokenProvider: provider,
  });
  const offer = createSummaryOffer(client);
  const routes = buildApiInteractionsFromDataOfferSources({ summary: offer });
  assert.equal(buildApiInteractionPath(routes.summary), declaration.publications.operations[0].path);
  assert.deepEqual(declaration.permissions, { scopes: [], accessRules: [] });
  const app = express();
  app.get(declaration.publications.operations[0].path, (req, res) => offer.handler({ req, res }));
  const service = await listen(t, app);
  return { url: service + declaration.publications.operations[0].path, seen, file, deny: () => (deny = true), invalid: () => (invalid = true) };
}

test("catalog example keeps caller/service credentials separate and emits only declared output", async (t) => {
  const c = await catalog(t);
  const req = () => fetch(c.url + `?from=${from}&to=${to}`, { headers: { Authorization: "Bearer synthetic-reader" } });
  const response = await req();
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.deepEqual(await response.json(), { count: 2, hasMore: false });
  assert.deepEqual(
    c.seen.map((x) => x.path),
    ["/api/catalog-publications/authorize", "/api/catalog-publications/history", "/api/catalog-publications/authorize"],
  );
  assert.ok(c.seen.every((x) => x.auth === "Bearer synthetic-service-one" && x.value.callerToken === "synthetic-reader"));
  assert.equal(c.seen[1].value.sourceId, "temperature");
  await writeFile(c.file, "synthetic-service-two");
  await req();
  assert.equal(c.seen.at(-1).auth, "Bearer synthetic-service-two");
});

test("catalog example refuses missing caller, unknown parameters, revocation and malformed source", async (t) => {
  const c = await catalog(t),
    headers = { Authorization: "Bearer synthetic-reader" };
  assert.equal((await fetch(c.url + `?from=${from}&to=${to}`)).status, 401);
  assert.equal(c.seen.length, 0);
  assert.equal((await fetch(c.url + `?from=${from}&to=${to}&topic=elsewhere`, { headers })).status, 400);
  assert.equal(c.seen.length, 0);
  c.invalid();
  const invalid = await fetch(c.url + `?from=${from}&to=${to}`, { headers });
  assert.equal(invalid.status, 503);
  assert.equal((await invalid.text()).includes("must-not-leak"), false);
  c.deny();
  assert.equal((await fetch(c.url + `?from=${from}&to=${to}`, { headers })).status, 403);
});
