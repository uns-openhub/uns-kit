import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";

import { managedMqttOutputBinding } from "../packages/uns-core/src/runtime/mqtt-output-state-contract.js";
import { outputStateLimits, validateOutputRow, validateOutputSnapshot } from "../packages/uns-core/src/runtime/mqtt-output-state-program.mjs";
import { mqttPublishQueueSchema } from "../packages/uns-core/src/uns-mqtt/publish-queue-contract.js";

const limits = {
  schemaVersion: 1,
  maximumQueues: 32,
  maximumRecordsPerQueue: 100000,
  maximumManifestBytes: 262144,
  maximumSnapshotBytes: 536870912,
  maximumCapsuleBytes: 1073741824,
};
const roots: string[] = [];
const sha = (b: string | Buffer) => crypto.createHash("sha256").update(b).digest("hex");
afterEach(() => {
  for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true });
});
function fixture() {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "sdk-mqtt-snapshot-"));
  roots.push(root);
  fs.chmodSync(root, 0o700);
  const rootId = crypto.randomUUID(),
    queues: any[] = [];
  for (const relative of ["meters/periods", "vision/observations"]) {
    const dir = path.join(root, relative);
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const file = path.join(dir, "mqtt-outbox.sqlite");
    const db = new DatabaseSync(file);
    db.exec(
      "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE pending(seq INTEGER PRIMARY KEY,id TEXT,record TEXT,bytes INTEGER,sha256 TEXT)",
    );
    const identity = "a".repeat(64);
    for (const [k, v] of Object.entries({ schema: "2", rootId, relative, identity })) db.prepare("INSERT INTO metadata VALUES(?,?)").run(k, v);
    const topic = relative + "/value",
      message = JSON.stringify({ time: "2026-10-27T10:00:00Z", value: relative.startsWith("meters") ? 1.5 : "OK", uom: "kWh" }),
      r = {
        seq: 1,
        id: crypto.randomUUID(),
        topic,
        message,
        options: { qos: 1, retain: false },
        bytes: Buffer.byteLength(topic) + Buffer.byteLength(message) + 256,
      },
      text = JSON.stringify(r);
    db.prepare("INSERT INTO pending VALUES(?,?,?,?,?)").run(1, r.id, text, r.bytes, sha(text));
    db.close();
    fs.chmodSync(file, 0o600);
    queues.push({
      path: relative,
      file: relative + "/mqtt-outbox.sqlite",
      size: fs.statSync(file).size,
      sha256: sha(fs.readFileSync(file)),
      identity,
      records: 1,
      bytes: r.bytes,
      lastSeq: 1,
    });
  }
  const manifest = {
    schemaVersion: 1,
    kind: "uns-mqtt-output-state-snapshot",
    rootId,
    capturedAt: new Date().toISOString(),
    restorePolicy: "offline-source-stop-confirmation-required-no-replay",
    queues,
  };
  fs.writeFileSync(path.join(root, "manifest.json"), JSON.stringify(manifest), { mode: 0o600 });
  return { root, manifest };
}
describe("generic MQTT state data contract", () => {
  it("validates unrelated owners without importing application code", () => {
    const f = fixture();
    expect(validateOutputSnapshot(f.root, limits).queues).toHaveLength(2);
  });
  it("refuses tampered payload, duplicate/traversal paths and undeclared files", () => {
    for (const kind of ["payload", "duplicate", "traversal", "extra"]) {
      const f = fixture();
      if (kind === "payload") fs.appendFileSync(path.join(f.root, f.manifest.queues[0].file), "tamper");
      if (kind === "duplicate") f.manifest.queues.push(f.manifest.queues[0]);
      if (kind === "traversal") f.manifest.queues[0].path = "../outside";
      if (kind === "extra") fs.writeFileSync(path.join(f.root, "extra"), "x", { mode: 0o600 });
      fs.writeFileSync(path.join(f.root, "manifest.json"), JSON.stringify(f.manifest));
      expect(() => validateOutputSnapshot(f.root, limits)).toThrow();
    }
  });
  it("refuses lifted bounds and malformed queue rows", () => {
    expect(() => outputStateLimits({ ...limits, maximumQueues: 33 })).toThrow();
    expect(() => validateOutputRow({ record: "{}", sha256: "x" })).toThrow(/integrity/);
  });
});

describe("managed standard SDK output binding", () => {
  it("keeps absent/local configuration compatible and requires explicit runtime ownership", () => {
    expect(managedMqttOutputBinding({ anyBusinessConfiguration: true }, undefined)).toBeUndefined();
    expect(mqttPublishQueueSchema.parse({ mode: "durable", directory: "/private/local" }).storage).toBe("local");
    for (const publishQueue of [
      { mode: "durable", directory: "/var/lib/uns-openhub/mqtt-output/meters/energy" },
      { mode: "durable", storage: "runtime", directory: "/outside/energy" },
    ])
      expect(() => managedMqttOutputBinding({ output: { publishQueue } }, "/var/lib/uns-openhub/mqtt-output")).toThrow();
  });
  it("accepts two unrelated consumers without application metadata", () => {
    for (const relative of ["meters/energy", "vision/observations"]) {
      const directory = "/var/lib/uns-openhub/mqtt-output/" + relative;
      expect(
        managedMqttOutputBinding(
          { output: { publishQueue: { mode: "durable", storage: "runtime", directory } } },
          "/var/lib/uns-openhub/mqtt-output",
        ),
      ).toEqual({ directory, relative });
    }
  });
});

it("has no CLI side effects when imported by an ordinary stdin consumer", () => {
  const file = path.resolve("packages/uns-core/src/runtime/mqtt-output-state-program.mjs");
  const run = spawnSync(process.execPath, ["--input-type=module", "-"], {
    input: `await import(${JSON.stringify("file://" + file)});process.stdout.write('consumer-ok')`,
    encoding: "utf8",
  });
  expect(run.status, run.stderr).toBe(0);
  expect(run.stdout).toBe("consumer-ok");
});
