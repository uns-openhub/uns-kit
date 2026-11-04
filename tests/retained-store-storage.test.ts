import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";

import { available, captureRetained, restoreRetained, runtimeLease } from "../packages/uns-core/src/runtime/retained-store-storage.mjs";

const limits = {
  schemaVersion: 1,
  maximumStores: 32,
  maximumEntries: 100000,
  maximumBytes: 68719476736,
  maximumManifestBytes: 33554432,
  maximumFileBytes: 34359738368,
  maximumDepth: 32,
};
const roots: string[] = [];
afterEach(() => roots.splice(0).forEach((p) => fs.rmSync(p, { recursive: true, force: true })));
function fixture() {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), "retained-storage-")));
  roots.push(base);
  const root = path.join(base, "state"),
    snapshot = path.join(base, "snapshot");
  for (const p of [root, snapshot]) fs.mkdirSync(p, { mode: 0o700 });
  const id = randomUUID(),
    dir = path.join(root, id);
  fs.mkdirSync(dir, { mode: 0o700 });
  fs.mkdirSync(path.join(dir, "queue"), { mode: 0o700 });
  const b = {
    protocol: 1,
    storeId: id,
    generation: randomUUID(),
    operationId: randomUUID(),
    directory: dir,
    format: "unrelated-worker-v1",
    configurationDigest: "a".repeat(64),
    owner: { rttNode: "independent-worker", version: "1.0.0", instanceId: "one" },
    maintenance: true,
  };
  const declaration = {
    protocol: 1,
    storageProtocol: 1,
    format: b.format,
    resources: [
      { id: "queue", path: "event_storage", kind: "directory", required: true, backup: "payload" },
      { id: "control", path: "pause.json", kind: "file", required: false, backup: "metadata" },
    ],
    legacyVersions: [],
  };
  for (const [file, data] of [
    ["store.json", b],
    ["store-resources.json", declaration],
    ["control", { paused: true, revision: 5 }],
  ] as const)
    fs.writeFileSync(path.join(dir, file), JSON.stringify(data), { mode: 0o600 });
  const db = new DatabaseSync(path.join(dir, "queue", "work.sqlite"));
  db.exec("CREATE TABLE work(id INTEGER PRIMARY KEY,value TEXT);INSERT INTO work VALUES(1,'original'),(2,'pending')");
  db.close();
  return { base, root, snapshot, id, dir, b };
}
describe("shared opaque retained-store offline capture and restore", () => {
  it("refuses capture while a runtime owns the store, without modifying work", () => {
    const f = fixture(),
      before = fs.readFileSync(path.join(f.dir, "queue", "work.sqlite"));
    const owner = runtimeLease(f.b);
    try {
      expect(() => captureRetained(f.root, f.snapshot, "include", limits)).toThrow("owned");
    } finally {
      owner.close();
    }
    expect(fs.readFileSync(path.join(f.dir, "queue", "work.sqlite"))).toEqual(before);
    expect(captureRetained(f.root, f.snapshot, "include", limits).state).toBe("included");
  });
  it("resumes interrupted publication, preserves original volume and never replaces advanced restored work", () => {
    const f = fixture();
    captureRetained(f.root, f.snapshot, "include", limits);
    fs.renameSync(f.root, f.root + ".source");
    fs.mkdirSync(f.root, { mode: 0o700 });
    const digest = createHash("sha256").update("reviewed signed manifest").digest("hex");
    expect(() =>
      restoreRetained(f.snapshot, f.root, digest, limits, () => {
        throw Error("interrupted publication");
      }),
    ).toThrow("interrupted");
    expect(() => available(f.root)).toThrow("incomplete");
    expect(() => restoreRetained(f.snapshot, f.root, "b".repeat(64), limits)).toThrow("Another");
    // Reviewed abort before activation: preserve partial target, return original storage.
    fs.renameSync(f.root, f.root + ".partial");
    fs.renameSync(f.root + ".source", f.root);
    let db = new DatabaseSync(path.join(f.dir, "queue", "work.sqlite"));
    expect(db.prepare("SELECT count(*) AS n FROM work").get()?.n).toBe(2);
    db.close();
    fs.renameSync(f.root, f.root + ".source");
    fs.renameSync(f.root + ".partial", f.root);
    expect(restoreRetained(f.snapshot, f.root, digest, limits).state).toBe("complete");
    available(f.root);
    const owner = runtimeLease(f.b);
    db = new DatabaseSync(path.join(f.dir, "queue", "work.sqlite"));
    db.exec("INSERT INTO work VALUES(3,'after-restore')");
    db.close();
    owner.close();
    expect(restoreRetained(f.snapshot, f.root, digest, limits).state).toBe("complete");
    const again = runtimeLease(f.b);
    db = new DatabaseSync(path.join(f.dir, "queue", "work.sqlite"));
    expect(db.prepare("SELECT count(*) AS n FROM work").get()?.n).toBe(3);
    db.close();
    again.close();
    const original = new DatabaseSync(path.join(f.root + ".source", f.id, "queue", "work.sqlite"), { readOnly: true });
    expect(original.prepare("SELECT count(*) AS n FROM work").get()?.n).toBe(2);
    original.close();
  });
  it("excludes payloads explicitly and refuses incomplete or linked snapshots", () => {
    const f = fixture();
    const c = captureRetained(f.root, f.snapshot, "exclude", limits);
    expect(c.state).toBe("excluded");
    expect(fs.existsSync(path.join(f.snapshot, f.id, "queue"))).toBe(false);
    expect(() => restoreRetained(f.snapshot, f.root, "a".repeat(64), limits)).toThrow("excluded");
    const g = fixture();
    fs.symlinkSync(path.join(g.dir, "control"), path.join(g.dir, "queue", "unsafe"));
    expect(() => captureRetained(g.root, g.snapshot, "include", limits)).toThrow("Linked");
  });
});

it("replaces only a known unpublished partial file from the verified snapshot", () => {
  const f = fixture();
  captureRetained(f.root, f.snapshot, "include", limits);
  fs.renameSync(f.root, f.root + ".source");
  fs.mkdirSync(f.root, { mode: 0o700 });
  const digest = "c".repeat(64),
    manifestDigest = createHash("sha256")
      .update(fs.readFileSync(path.join(f.snapshot, "manifest.json")))
      .digest("hex");
  fs.writeFileSync(
    path.join(f.root, "retained-store-restore.json"),
    JSON.stringify({ protocol: 1, state: "applying", backupDigest: digest, manifestDigest }),
    { mode: 0o600 },
  );
  const partial = path.join(f.root, "retained-restore-stage", f.id, "queue");
  fs.mkdirSync(partial, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(partial, "work.sqlite"), "interrupted partial bytes", { mode: 0o600 });
  expect(restoreRetained(f.snapshot, f.root, digest, limits).state).toBe("complete");
  const db = new DatabaseSync(path.join(f.dir, "queue", "work.sqlite"), { readOnly: true });
  expect(db.prepare("SELECT count(*) AS n FROM work").get()?.n).toBe(2);
  db.close();
});

it("preserves retired owner receipts without resurrecting the returned payload", () => {
  const f = fixture();
  fs.renameSync(path.join(f.dir, "queue"), path.join(f.base, "returned-queue"));
  fs.renameSync(path.join(f.dir, "control"), path.join(f.base, "returned-control"));
  fs.writeFileSync(path.join(f.dir, "store-returned.json"), JSON.stringify({ operationId: f.b.operationId, generation: f.b.generation }), {
    mode: 0o600,
  });
  expect(captureRetained(f.root, f.snapshot, "include", limits).stores).toBe(1);
  expect(fs.existsSync(path.join(f.snapshot, f.id, "queue"))).toBe(false);
  fs.renameSync(f.root, f.root + ".source");
  fs.mkdirSync(f.root, { mode: 0o700 });
  expect(restoreRetained(f.snapshot, f.root, "e".repeat(64), limits).state).toBe("complete");
  expect(fs.existsSync(path.join(f.dir, "store-returned.json"))).toBe(true);
  expect(() => runtimeLease(f.b)).toThrow("retired");
  expect(fs.existsSync(path.join(f.base, "returned-queue", "work.sqlite"))).toBe(true);
});
