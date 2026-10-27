// SDK-owned data-only runtime/snapshot/restore contract. Never imports an application.
import crypto from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(import.meta.url),
  sha = (x) => crypto.createHash("sha256").update(x).digest("hex");
const guardName = ".mqtt-output-root.sqlite",
  markerName = ".mqtt-output-restore.json",
  stageName = ".mqtt-output-restore-stage";
const safeName = /^[a-z][a-z0-9-]{0,99}$/,
  hex = /^[a-f0-9]{64}$/,
  uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-5][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const exactKeys = (value, keys) =>
  value &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  Object.keys(value).length === keys.length &&
  keys.every((k) => Object.hasOwn(value, k));
const fail = () => {
  throw Error("MQTT output state validation failed; preserve storage and review the operation");
};
const sqlite = () => require("node:sqlite");
function hashFile(file) {
  const hash = crypto.createHash("sha256"),
    buffer = Buffer.allocUnsafe(65536),
    fd = fs.openSync(file, "r");
  try {
    let n;
    while ((n = fs.readSync(fd, buffer, 0, buffer.length, null)) > 0) hash.update(buffer.subarray(0, n));
    return hash.digest("hex");
  } finally {
    fs.closeSync(fd);
  }
}
export function outputStateLimits(value) {
  const expected = {
    schemaVersion: 1,
    maximumQueues: 32,
    maximumRecordsPerQueue: 100000,
    maximumManifestBytes: 262144,
    maximumSnapshotBytes: 536870912,
    maximumCapsuleBytes: 1073741824,
  };
  if (!value || Object.keys(value).length !== Object.keys(expected).length || Object.entries(expected).some(([k, v]) => value[k] !== v)) fail();
  return Object.freeze({ ...expected });
}
function check(p, directory = false, optional = false) {
  let s;
  try {
    s = fs.lstatSync(p);
  } catch (e) {
    if (optional && e.code === "ENOENT") return;
    throw e;
  }
  if (
    s.isSymbolicLink() ||
    s.uid !== process.getuid?.() ||
    s.mode & 0o077 ||
    (!directory && (!s.isFile() || s.nlink !== 1)) ||
    (directory && !s.isDirectory())
  )
    fail();
  return s;
}
function noLinks(p) {
  for (let q = p; q !== "/"; q = path.dirname(q)) {
    try {
      if (fs.lstatSync(q).isSymbolicLink()) fail();
    } catch (e) {
      if (e.code !== "ENOENT") throw e;
    }
  }
}
function privateJson(p) {
  const s = check(p);
  if (s.size > 262144) fail();
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function atomicJson(p, value) {
  const temp = p + ".writing-" + crypto.randomUUID();
  let fd;
  try {
    fd = fs.openSync(temp, "wx", 0o600);
    fs.writeFileSync(fd, JSON.stringify(value) + "\n");
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;
    fs.renameSync(temp, p);
    sync(path.dirname(p));
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    if (fs.existsSync(temp)) fs.unlinkSync(temp);
  }
}
function sync(directory) {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
export function assertOutputStateRoot(root) {
  if (process.platform !== "linux" || typeof root !== "string" || root !== path.resolve(root) || !root.startsWith("/var/lib/")) fail();
  noLinks(root);
  check(root, true);
  const mount = fs
    .readFileSync("/proc/self/mountinfo", "utf8")
    .split("\n")
    .map((l) => l.split(" "))
    .find((f) => f[4] === root);
  if (!mount || !["ext4", "xfs", "btrfs", "zfs"].includes(mount[mount.indexOf("-") + 1])) fail();
}
export function outputStateRelative(root, directory) {
  if (directory !== path.resolve(directory)) fail();
  const rel = path.relative(root, directory).split(path.sep);
  if (rel.length !== 2 || rel.some((x) => !safeName.test(x))) fail();
  return rel.join("/");
}
function rootLease(root, exclusive = false) {
  assertOutputStateRoot(root);
  const file = path.join(root, guardName);
  try {
    fs.closeSync(fs.openSync(file, "wx", 0o600));
  } catch (e) {
    if (e.code !== "EEXIST") throw e;
  }
  check(file);
  check(file + "-journal", false, true);
  const { DatabaseSync } = sqlite(),
    db = new DatabaseSync(file);
  try {
    db.exec("PRAGMA busy_timeout=3000; PRAGMA synchronous=FULL");
    if (db.prepare("PRAGMA journal_mode").get().journal_mode !== "delete") fail();
    // Concurrent first owners serialize initialization, then retain shared read leases.
    if (!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'").get()) {
      db.exec("PRAGMA busy_timeout=3000; BEGIN IMMEDIATE");
      if (!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'").get()) {
        if (db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().length) fail();
        db.exec("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)");
        db.prepare("INSERT INTO metadata VALUES(?,?)").run("schema", "mqtt-output-root-1");
        db.prepare("INSERT INTO metadata VALUES(?,?)").run("id", crypto.randomUUID());
        db.exec("COMMIT");
        sync(root);
      } else db.exec("ROLLBACK");
    }
    db.exec(exclusive ? "BEGIN EXCLUSIVE" : "BEGIN");
    const meta = Object.fromEntries(
      db
        .prepare("SELECT key,value FROM metadata")
        .all()
        .map((r) => [r.key, r.value]),
    );
    if (!exactKeys(meta, ["schema", "id"]) || meta.schema !== "mqtt-output-root-1" || !uuid.test(meta.id)) fail();
    return { db, rootId: meta.id, close: () => db.close() };
  } catch (e) {
    db.close();
    throw e;
  }
}
export function openManagedOutputRoot(directory, root) {
  if (!root) return undefined;
  assertOutputStateRoot(root);
  const relative = outputStateRelative(root, directory);
  noLinks(directory);
  for (let p = path.dirname(directory); p !== root; p = path.dirname(p)) check(p, true, true);
  const lease = rootLease(root);
  try {
    const marker = path.join(root, markerName);
    if (fs.existsSync(marker)) {
      const m = privateJson(marker);
      if (
        !exactKeys(m, ["schemaVersion", "state", "sourceSha", "rootId"]) ||
        m.schemaVersion !== 1 ||
        m.state !== "complete" ||
        !hex.test(m.sourceSha) ||
        m.rootId !== lease.rootId
      )
        throw Error("MQTT output restoration is incomplete; reconcile the same restore before starting");
    }
    return { rootId: lease.rootId, relative, schema: "2", close: lease.close };
  } catch (e) {
    lease.close();
    throw e;
  }
}
export function validateOutputRow(row) {
  if (typeof row.record !== "string" || sha(row.record) !== row.sha256) throw Error("MQTT outbox integrity check failed; pending work preserved");
  const r = JSON.parse(row.record);
  if (
    !exactKeys(r, ["seq", "id", "topic", "message", "options", "bytes"]) ||
    r.seq !== Number(row.seq) ||
    r.id !== row.id ||
    r.bytes !== Number(row.bytes) ||
    !Number.isSafeInteger(r.seq) ||
    r.seq < 1 ||
    typeof r.id !== "string" ||
    !r.id ||
    r.id.length > 200 ||
    typeof r.topic !== "string" ||
    !r.topic ||
    /[\0+#]/.test(r.topic) ||
    Buffer.byteLength(r.topic) > 65535 ||
    typeof r.message !== "string" ||
    !r.options ||
    ![1, 2].includes(r.options.qos) ||
    typeof r.options.retain !== "boolean" ||
    Object.keys(r.options).some((k) => !["qos", "retain"].includes(k)) ||
    r.bytes !== Buffer.byteLength(r.topic) + Buffer.byteLength(r.message) + 256
  )
    fail();
  return r;
}
function inspectDatabase(file, binding, limits) {
  check(file);
  const { DatabaseSync } = sqlite(),
    db = new DatabaseSync(file, { readOnly: true });
  try {
    if (db.prepare("PRAGMA quick_check").get().quick_check !== "ok") fail();
    const meta = Object.fromEntries(
      db
        .prepare("SELECT key,value FROM metadata")
        .all()
        .map((r) => [r.key, r.value]),
    );
    if (
      meta.schema !== "2" ||
      meta.rootId !== binding.rootId ||
      meta.relative !== binding.relative ||
      !hex.test(meta.identity) ||
      Object.keys(meta).some((k) => !["schema", "rootId", "relative", "identity"].includes(k))
    )
      fail();
    let records = 0,
      bytes = 0,
      lastSeq = 0;
    for (const row of db.prepare("SELECT seq,id,record,bytes,sha256 FROM pending ORDER BY seq").iterate()) {
      const r = validateOutputRow(row);
      records++;
      bytes += r.bytes;
      lastSeq = r.seq;
      if (records > limits.maximumRecordsPerQueue || bytes > limits.maximumSnapshotBytes) fail();
    }
    return { identity: meta.identity, records, bytes, lastSeq };
  } finally {
    db.close();
  }
}
function manifestBytes(directory) {
  const file = path.join(directory, "manifest.json");
  if (check(file).size > 262144) fail();
  return fs.readFileSync(file);
}
export function validateOutputSnapshot(directory, suppliedLimits) {
  const limits = outputStateLimits(suppliedLimits);
  noLinks(directory);
  check(directory, true);
  const data = manifestBytes(directory);
  if (data.length > limits.maximumManifestBytes) fail();
  const manifest = JSON.parse(data);
  if (
    !exactKeys(manifest, ["schemaVersion", "kind", "rootId", "capturedAt", "restorePolicy", "queues"]) ||
    manifest.schemaVersion !== 1 ||
    manifest.kind !== "uns-mqtt-output-state-snapshot" ||
    !uuid.test(manifest.rootId) ||
    typeof manifest.capturedAt !== "string" ||
    !Number.isFinite(Date.parse(manifest.capturedAt)) ||
    manifest.restorePolicy !== "offline-source-stop-confirmation-required-no-replay" ||
    !Array.isArray(manifest.queues) ||
    manifest.queues.length > limits.maximumQueues
  )
    fail();
  const expected = new Set(["manifest.json"]),
    directories = new Set();
  let size = 0;
  const seen = new Set();
  for (const q of manifest.queues) {
    if (
      !exactKeys(q, ["path", "file", "size", "sha256", "identity", "records", "bytes", "lastSeq"]) ||
      q.path !== outputStateRelative("/", "/" + q.path) ||
      seen.has(q.path) ||
      q.file !== q.path + "/mqtt-outbox.sqlite" ||
      !hex.test(q.sha256) ||
      !hex.test(q.identity)
    )
      fail();
    seen.add(q.path);
    directories.add(q.path.split("/")[0]);
    directories.add(q.path);
    const file = path.join(directory, q.file);
    noLinks(file);
    const s = check(file);
    size += s.size;
    if (s.size !== q.size || size > limits.maximumSnapshotBytes || hashFile(file) !== q.sha256) fail();
    const actual = inspectDatabase(file, { rootId: manifest.rootId, relative: q.path }, limits);
    if (["identity", "records", "bytes", "lastSeq"].some((k) => actual[k] !== q[k])) fail();
    expected.add(q.file);
  }
  // No undeclared files, symlinks or SQLite sidecars enter a restore.
  function walk(dir, rel = "") {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const n = rel ? rel + "/" + e.name : e.name;
      if (e.isSymbolicLink()) fail();
      if (e.isDirectory()) {
        if (!directories.has(n)) fail();
        check(path.join(dir, e.name), true);
        walk(path.join(dir, e.name), n);
      } else if (!expected.has(n)) fail();
    }
  }
  walk(directory);
  return manifest;
}
async function capture(root, out, limits) {
  check(out, true);
  if (fs.readdirSync(out).length) fail();
  const lease = rootLease(root);
  const manifest = {
    schemaVersion: 1,
    kind: "uns-mqtt-output-state-snapshot",
    rootId: lease.rootId,
    capturedAt: new Date().toISOString(),
    restorePolicy: "offline-source-stop-confirmation-required-no-replay",
    queues: [],
  };
  let total = 0;
  try {
    const marker = path.join(root, markerName);
    if (fs.existsSync(marker) && privateJson(marker).state !== "complete") fail();
    for (const e of fs.readdirSync(root, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      if ([guardName, guardName + "-journal", markerName].includes(e.name)) {
        check(path.join(root, e.name));
        continue;
      }
      if (!e.isDirectory() || !safeName.test(e.name)) fail();
      const ns = path.join(root, e.name);
      check(ns, true);
      for (const id of fs.readdirSync(ns).sort()) {
        if (!safeName.test(id) || manifest.queues.length >= limits.maximumQueues) fail();
        const relative = e.name + "/" + id,
          source = path.join(ns, id);
        noLinks(source);
        check(source, true);
        const allowed = new Set([
          "mqtt-outbox.sqlite",
          "mqtt-outbox.sqlite-wal",
          "mqtt-outbox.sqlite-shm",
          "mqtt-outbox-owner.sqlite",
          "mqtt-outbox-owner.sqlite-journal",
        ]);
        for (const f of fs.readdirSync(source)) {
          if (!allowed.has(f)) fail();
          const s = check(path.join(source, f));
          if (s.size > limits.maximumSnapshotBytes) fail();
        }
        const main = path.join(source, "mqtt-outbox.sqlite"),
          before = check(main);
        const targetDir = path.join(out, relative);
        fs.mkdirSync(targetDir, { recursive: true, mode: 0o700 });
        const dest = path.join(targetDir, "mqtt-outbox.sqlite");
        const { DatabaseSync, backup } = sqlite(),
          db = new DatabaseSync(main, { readOnly: true });
        try {
          await backup(db, dest, {
            rate: 128,
            progress: ({ totalPages }) => {
              if (totalPages * 4096 > limits.maximumSnapshotBytes) fail();
            },
          });
        } finally {
          db.close();
        }
        fs.chmodSync(dest, 0o600);
        const frozen = new (sqlite().DatabaseSync)(dest);
        try {
          frozen.exec("PRAGMA journal_mode=DELETE");
        } finally {
          frozen.close();
        }
        const after = check(main);
        if (before.ino !== after.ino || before.dev !== after.dev) fail();
        const s = check(dest);
        total += s.size;
        if (total > limits.maximumSnapshotBytes) fail();
        const data = inspectDatabase(dest, { rootId: lease.rootId, relative }, limits);
        manifest.queues.push({ path: relative, file: relative + "/mqtt-outbox.sqlite", size: s.size, sha256: hashFile(dest), ...data });
      }
    }
    const encoded = JSON.stringify(manifest) + "\n";
    if (Buffer.byteLength(encoded) > limits.maximumManifestBytes) fail();
    fs.writeFileSync(path.join(out, "manifest.json"), encoded, { mode: 0o600 });
    sync(out);
    return manifest;
  } finally {
    lease.close();
  }
}
async function restore(source, root, limits, sourceStopped) {
  if (sourceStopped !== "source-stopped") throw Error("Explicit confirmation of the stopped source is required");
  const manifest = validateOutputSnapshot(source, limits),
    sourceSha = sha(manifestBytes(source));
  assertOutputStateRoot(root);
  // Empty or same-operation target only. An exclusive root lease excludes live SDK writers.
  for (const name of fs.readdirSync(root))
    if (![guardName, guardName + "-journal"].includes(name) && !fs.existsSync(path.join(root, markerName)))
      throw Error("MQTT restore target is not empty; existing state is preserved");
  const lease = rootLease(root, true),
    marker = path.join(root, markerName),
    stage = path.join(root, stageName);
  try {
    let prior;
    if (fs.existsSync(marker)) {
      prior = privateJson(marker);
      if (
        !exactKeys(prior, ["schemaVersion", "state", "sourceSha", "rootId"]) ||
        prior.schemaVersion !== 1 ||
        prior.sourceSha !== sourceSha ||
        !["applying", "complete"].includes(prior.state)
      )
        fail();
    }
    const allowed = new Set([guardName, guardName + "-journal", markerName, stageName, ...manifest.queues.map((q) => q.path.split("/")[0])]);
    for (const e of fs.readdirSync(root)) {
      if (!allowed.has(e) || (!prior && ![guardName, guardName + "-journal"].includes(e)))
        throw Error("MQTT restore target is not empty; existing state is preserved");
    }
    // A resumed restore never adopts undeclared children inside a known namespace.
    for (const ns of new Set(manifest.queues.map((q) => q.path.split("/")[0]))) {
      const dir = path.join(root, ns);
      if (fs.existsSync(dir)) {
        noLinks(dir);
        check(dir, true);
        for (const id of fs.readdirSync(dir)) if (!manifest.queues.some((q) => q.path === ns + "/" + id)) fail();
      }
    }
    if (!prior) {
      atomicJson(marker, { schemaVersion: 1, state: "applying", sourceSha, rootId: manifest.rootId });
      fs.mkdirSync(stage, { mode: 0o700 });
    } else if (prior.rootId !== manifest.rootId) fail();
    lease.db.prepare("UPDATE metadata SET value=? WHERE key='id'").run(manifest.rootId);
    for (const q of manifest.queues) {
      const target = path.join(root, q.path),
        file = path.join(target, "mqtt-outbox.sqlite");
      noLinks(target);
      if (fs.existsSync(target)) {
        check(target, true);
        if (fs.readdirSync(target).some((x) => x !== "mqtt-outbox.sqlite") || hashFile(file) !== q.sha256) fail();
        continue;
      }
      if (prior?.state === "complete") fail();
      const staged = path.join(stage, q.path);
      fs.mkdirSync(staged, { recursive: true, mode: 0o700 });
      const copy = path.join(staged, "mqtt-outbox.sqlite");
      if (fs.existsSync(copy)) {
        check(copy);
        if (hashFile(copy) !== q.sha256) fail();
      } else {
        fs.copyFileSync(path.join(source, q.file), copy, fs.constants.COPYFILE_EXCL);
        fs.chmodSync(copy, 0o600);
        const fd = fs.openSync(copy, "r");
        fs.fsyncSync(fd);
        fs.closeSync(fd);
      }
      fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
      fs.renameSync(staged, target);
      sync(path.dirname(target));
    }
    // Every restored file is rechecked before the root is declared usable.
    for (const q of manifest.queues) if (hashFile(path.join(root, q.file)) !== q.sha256) fail();
    lease.db.exec("COMMIT");
    if (fs.existsSync(stage)) {
      for (const ns of fs.readdirSync(stage)) {
        const dir = path.join(stage, ns);
        check(dir, true);
        if (fs.readdirSync(dir).length) fail();
        fs.rmdirSync(dir);
      }
      fs.rmdirSync(stage);
    }
    atomicJson(marker, { schemaVersion: 1, state: "complete", sourceSha, rootId: manifest.rootId });
    return {
      schemaVersion: 1,
      state: "complete",
      sourceSha,
      queues: manifest.queues.length,
      records: manifest.queues.reduce((n, q) => n + q.records, 0),
    };
  } finally {
    lease.close();
  }
}
export async function runOutputStateProgram(args) {
  const [action, root, out, fixed, confirmation] = args,
    limits = outputStateLimits(JSON.parse(fixed));
  if (action === "capture") return capture(root, out, limits);
  if (action === "validate") return validateOutputSnapshot(root, limits);
  if (action === "restore") return restore(root, out, limits, confirmation);
  fail();
}
export function outputStateMain(args = process.argv.slice(2)) {
  return runOutputStateProgram(args)
    .then((r) => process.stdout.write(JSON.stringify(r) + "\n"))
    .catch(() => {
      process.stderr.write("MQTT output state operation refused; preserve source and target for review\n");
      process.exitCode = 1;
    });
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await outputStateMain();
