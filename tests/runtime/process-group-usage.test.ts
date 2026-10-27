import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, expect, it } from "vitest";

import { sha256 } from "../../packages/uns-core/src/runtime/process-group-resources.js";
import { inspectProcessGroupUpgradeUsage } from "../../packages/uns-core/src/runtime/process-group-upgrade.js";
import { mqttPublishQueueIdentity } from "../../packages/uns-core/src/uns-mqtt/publish-queue-contract.js";

const roots: string[] = [];
afterEach(() => {
  for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true });
});
function fixture() {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "sdk-retained-input-"));
  roots.push(directory);
  const file = path.join(directory, "admission.sqlite"),
    db = new DatabaseSync(file),
    journalId = randomUUID(),
    contract = JSON.stringify({ version: 1, profiles: ["first", "second"] });
  db.exec(
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE usage(id INTEGER PRIMARY KEY,records INTEGER,bytes INTEGER); CREATE TABLE tasks(seq INTEGER PRIMARY KEY,bytes INTEGER,payload TEXT)",
  );
  db.prepare("INSERT INTO metadata VALUES('id',?)").run(journalId);
  db.prepare("INSERT INTO metadata VALUES('contract',?)").run(contract);
  db.exec("INSERT INTO usage VALUES(1,2,8);INSERT INTO tasks VALUES(1,3,'one'),(2,5,'other')");
  db.close();
  fs.chmodSync(file, 0o600);
  return { file, ref: { directory, journalId, journalContractSha256: sha256(contract) } };
}
it("inspects retained work without changing input bytes or checkpoints", () => {
  const f = fixture(),
    before = fs.readFileSync(f.file);
  expect(inspectProcessGroupUpgradeUsage(f.ref)).toEqual({ records: 2, bytes: 8 });
  expect(fs.readFileSync(f.file)).toEqual(before);
});
it("does not treat a different UUID or contract as empty", () => {
  const f = fixture();
  expect(() => inspectProcessGroupUpgradeUsage({ ...f.ref, journalId: randomUUID() })).toThrow(/identity/);
  expect(() => inspectProcessGroupUpgradeUsage({ ...f.ref, journalContractSha256: "a".repeat(64) })).toThrow(/identity/);
});
it("refuses inaccurate usage and unsafe permissions", () => {
  const f = fixture(),
    db = new DatabaseSync(f.file);
  db.exec("UPDATE usage SET records=0");
  db.close();
  expect(() => inspectProcessGroupUpgradeUsage(f.ref)).toThrow(/usage/);
  fs.chmodSync(f.file, 0o644);
  expect(() => inspectProcessGroupUpgradeUsage(f.ref)).toThrow(/Private/);
});
it("keeps the issued MQTT identity serialization byte compatible across placements", () => {
  expect(mqttPublishQueueIdentity("example-adapter", "combined-observations", { host: "broker", mqttSSL: true })).toBe(
    JSON.stringify({
      package: "example-adapter",
      owner: "combined-observations",
      host: "broker",
      port: 8883,
      protocol: "mqtts",
      hosts: null,
      servers: null,
    }),
  );
});
