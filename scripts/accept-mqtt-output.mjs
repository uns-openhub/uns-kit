/** Local-only built-artifact rehearsal. Requires a loopback mosquitto binary. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requireCore = createRequire(path.join(repo, "packages/uns-core/package.json"));
const mqtt = requireCore("mqtt");
const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
const { default: UnsMqttProxy } = await import(pathToFileURL(path.join(repo, "packages/uns-core/dist/uns-mqtt/uns-mqtt-proxy.js")));
const root = await mkdtemp(path.join(tmpdir(), "uns-mqtt-output-acceptance-"));
const binary = process.env.UNS_MQTT_TEST_BROKER ?? "mosquitto";
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(check, timeout = 15000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const result = await check();
    if (result) return result;
    await wait(20);
  }
  throw Error("Local acceptance deadline exceeded");
}
async function port() {
  const s = net.createServer();
  s.listen(0, "127.0.0.1");
  await once(s, "listening");
  const p = s.address().port;
  await new Promise((r) => s.close(r));
  return p;
}
const brokerPort = await port();
const brokerConfig = path.join(root, "mosquitto.conf");
await writeFile(brokerConfig, `listener ${brokerPort} 127.0.0.1\nallow_anonymous true\npersistence false\n`);
let broker;
const children = new Set();
const proxies = new Set();
const sockets = new Set();
let ackServer;
let observer;
const events = [];
const results = { schemaVersion: 1, root, broker: binary, production: false, tests: [] };
async function startBroker() {
  broker = spawn(binary, ["-c", brokerConfig], { stdio: ["ignore", "ignore", "pipe"] });
  let error;
  broker.on("error", (e) => {
    error = e;
  });
  broker.stderr.on("data", () => {});
  await until(async () => {
    if (error) throw error;
    return new Promise((resolve) => {
      const s = net.connect(brokerPort, "127.0.0.1");
      s.on("connect", () => {
        s.destroy();
        resolve(true);
      });
      s.on("error", () => resolve(false));
    });
  });
}
async function stopBroker() {
  const p = broker;
  if (!p?.pid || p.exitCode !== null) return;
  const ended = once(p, "exit");
  p.kill("SIGTERM");
  await ended;
  broker = undefined;
}
function count(dir) {
  const db = new DatabaseSync(path.join(dir, "mqtt-outbox.sqlite"), { readOnly: true });
  try {
    return Number(db.prepare("SELECT COUNT(*) AS n FROM pending").get().n);
  } finally {
    db.close();
  }
}
async function stopChild(child) {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
  const ended = once(child, "exit");
  child.kill("SIGKILL");
  await ended;
}
const childScript = path.join(root, "publisher.mjs");
await writeFile(
  childScript,
  `
import UnsMqttProxy from ${JSON.stringify(pathToFileURL(path.join(repo, "packages/uns-core/dist/uns-mqtt/uns-mqtt-proxy.js")).href)};
const [role, port, directory, mode, processName, prefix] = process.argv.slice(2);
const p = new UnsMqttProxy('127.0.0.1', processName, 'output', {
  port: Number(port), reconnectPeriod: 50,
  publishQueue: { mode, directory, minFreeBytes: 0, retryIntervalMs: 50, publishTimeoutMs: 500 }
}, true, false);
if (role === 'seed') {
  for (let i=0;i<7;i++) await p.publishMessage(prefix + '/reading', JSON.stringify({id: prefix+'-'+i,time:'2026-10-26T12:00:00.000Z',value:i,uom:'kWh'}));
  console.log('ACCEPTED'); setInterval(()=>{},1000);
} else {
  await p.flush(15000); console.log('RESULT '+JSON.stringify(p.getPublishQueueStatus()));
  await p.stop();
}
`,
);
async function child(role, dir, mode, name, prefix) {
  const app = path.join(root, "app");
  await mkdir(app, { recursive: true });
  await writeFile(path.join(app, "package.json"), JSON.stringify({ name: "local-independent-publisher", version: "1.0.0", type: "module" }));
  const p = spawn(process.execPath, [childScript, role, String(brokerPort), dir, mode, name, prefix], {
    cwd: app,
    env: { ...process.env, UNS_BASE_PATH: app },
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.add(p);
  let output = "",
    errors = "";
  p.stdout.on("data", (b) => {
    output += b;
  });
  p.stderr.on("data", (b) => {
    errors += b;
  });
  const exited = new Promise((resolve) => p.once("exit", (code, signal) => resolve({ code, signal })));
  return { p, exited, text: () => output, errors: () => errors };
}
try {
  await startBroker();
  observer = await mqtt.connectAsync(`mqtt://127.0.0.1:${brokerPort}`, { protocolVersion: 5, reconnectPeriod: 50 });
  observer.on("error", () => {});
  observer.on("message", (topic, data) => {
    try {
      events.push({ topic, ...JSON.parse(data.toString()) });
    } catch {}
  });
  await observer.subscribeAsync("acceptance/#", { qos: 1 });

  // Real public output API -> built worker -> broker -> independent subscriber.
  const healthy = new UnsMqttProxy(
    "127.0.0.1",
    "ordinary-meter",
    "output",
    {
      port: brokerPort,
      publishQueue: { mode: "durable", directory: path.join(root, "healthy"), minFreeBytes: 0 },
    },
    true,
    false,
  );
  proxies.add(healthy);
  for (let i = 0; i < 20; i++)
    await healthy.publishMessage("acceptance/healthy/reading", JSON.stringify({ id: "healthy-" + i, value: i, uom: "kWh" }));
  await healthy.flush(10000);
  await until(() => events.filter((e) => e.topic === "acceptance/healthy/reading").length === 20);
  assert.equal(count(path.join(root, "healthy")), 0);
  await healthy.stop();
  proxies.delete(healthy);
  results.tests.push({ name: "actual-built-proxy-worker", published: 20, observed: 20, pending: 0 });

  // No broker: accept into durable local storage, abruptly kill the entire owner,
  // then reopen the same identity with no new publishes and drain retained work.
  await stopBroker();
  const durableDir = path.join(root, "crashed-durable");
  const seed = await child("seed", durableDir, "durable", "meter-runtime", "acceptance/restart");
  await until(() => seed.text().includes("ACCEPTED"));
  assert.equal(count(durableDir), 7);
  await stopChild(seed.p);
  assert.equal(count(durableDir), 7);
  await startBroker();
  await until(() => observer.connected);
  await observer.subscribeAsync("acceptance/#", { qos: 1 });
  const restored = await child("recover", durableDir, "durable", "meter-runtime", "acceptance/restart");
  const restoredExit = await restored.exited;
  assert.equal(restoredExit.code, 0, restored.errors());
  await until(() => new Set(events.filter((e) => e.topic === "acceptance/restart/reading").map((e) => e.id)).size === 7);
  assert.equal(count(durableDir), 0);
  const readings = events.filter((e) => e.topic === "acceptance/restart/reading");
  for (let i = 0; i < 7; i++) assert(readings.some((e) => e.value === i && e.uom === "kWh" && e.time === "2026-10-26T12:00:00.000Z"));
  results.tests.push({ name: "broker-outage-owner-SIGKILL-retained-restart", committed: 7, retainedAfterKill: 7, distinctObserved: 7, pending: 0 });

  // Buffered failure -> disk -> restart, with another independent application.
  await stopBroker();
  const bufferedDir = path.join(root, "buffered-vision");
  const buffered = await child("seed", bufferedDir, "buffered", "vision-runtime", "acceptance/vision");
  await until(() => buffered.text().includes("ACCEPTED"));
  await until(() => count(bufferedDir) === 7);
  await stopChild(buffered.p);
  await startBroker();
  await until(() => observer.connected);
  await observer.subscribeAsync("acceptance/#", { qos: 1 });
  const replay = await child("recover", bufferedDir, "buffered", "vision-runtime", "acceptance/vision");
  const replayExit = await replay.exited;
  assert.equal(replayExit.code, 0, replay.errors());
  await until(() => new Set(events.filter((e) => e.topic === "acceptance/vision/reading").map((e) => e.id)).size === 7);
  assert.equal(count(bufferedDir), 0);
  results.tests.push({ name: "buffered-failure-spill-independent-owner-restart", retained: 7, distinctObserved: 7, pending: 0 });

  // Transparent MQTT frame proxy drops exactly one PUBACK, not the PUBLISH.
  let dropped = 0;
  ackServer = net.createServer((downstream) => {
    const upstream = net.connect(brokerPort, "127.0.0.1");
    for (const s of [downstream, upstream]) {
      sockets.add(s);
      s.on("error", () => s.destroy());
      s.on("close", () => sockets.delete(s));
    }
    downstream.pipe(upstream);
    let pending = Buffer.alloc(0);
    upstream.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 2) {
        let size = 0,
          multiplier = 1,
          cursor = 1,
          b;
        do {
          if (cursor >= pending.length) return;
          b = pending[cursor++];
          size += (b & 127) * multiplier;
          multiplier *= 128;
        } while (b & 128);
        if (pending.length < cursor + size) return;
        const frame = pending.subarray(0, cursor + size);
        pending = pending.subarray(cursor + size);
        if (frame[0] >> 4 === 4 && dropped === 0) {
          dropped++;
          continue;
        }
        downstream.write(frame);
      }
    });
    upstream.on("close", () => downstream.destroy());
    downstream.on("close", () => upstream.destroy());
  });
  ackServer.listen(0, "127.0.0.1");
  await once(ackServer, "listening");
  const ambiguous = new UnsMqttProxy(
    "127.0.0.1",
    "ack-ambiguity",
    "output",
    {
      port: ackServer.address().port,
      reconnectPeriod: 50,
      publishQueue: { mode: "durable", directory: path.join(root, "ack-lost"), minFreeBytes: 0, publishTimeoutMs: 300, retryIntervalMs: 50 },
    },
    true,
    false,
  );
  proxies.add(ambiguous);
  await ambiguous.publishMessage("acceptance/ack/reading", JSON.stringify({ id: "same-event", value: 99, uom: "kWh" }));
  await ambiguous.flush(15000);
  await until(() => events.filter((e) => e.topic === "acceptance/ack/reading").length >= 2);
  assert.equal(dropped, 1);
  assert(ambiguous.getPublishQueueStatus().retries >= 1);
  assert.equal(count(path.join(root, "ack-lost")), 0);
  results.tests.push({
    name: "actual-lost-PUBACK",
    droppedPubacks: dropped,
    observedDuplicates: events.filter((e) => e.topic === "acceptance/ack/reading").length,
    pending: 0,
  });
  await ambiguous.stop();
  proxies.delete(ambiguous);
  results.pass = true;
} finally {
  for (const p of children) await stopChild(p);
  for (const p of proxies) await p.stop({ drain: false, timeoutMs: 2000 }).catch(() => {});
  if (observer) await observer.endAsync(true);
  for (const s of sockets) s.destroy();
  if (ackServer) await new Promise((r) => ackServer.close(r));
  await stopBroker();
  results.cleanup = { ownedProcessesStopped: true, productionTouched: false, syntheticOutboxesRetained: true };
  await writeFile(path.join(root, "observed.json"), JSON.stringify(events, null, 2));
  await writeFile(path.join(root, "result.json"), JSON.stringify(results, null, 2));
  console.log("ACCEPTANCE_RESULT " + JSON.stringify(results));
}
