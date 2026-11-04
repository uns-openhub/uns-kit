import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

export const RESTORE_FILE = "retained-store-restore.json";
export const DECLARATION = "store-resources.json";
export const OPERATION = "store-operation.json";
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const safe = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/;
const hash = /^[a-f0-9]{64}$/;
export function entry(p) {
  try {
    return fs.lstatSync(p);
  } catch (e) {
    if (e.code === "ENOENT") return;
    throw e;
  }
}
export function safePath(p) {
  if (!path.isAbsolute(p) || path.normalize(p) !== p) throw Error("Unsafe retained path");
  for (let a = p; a !== path.dirname(a); a = path.dirname(a)) if (entry(a)?.isSymbolicLink()) throw Error("Linked retained path");
}
function owned(p, directory = false) {
  safePath(p);
  const s = entry(p);
  if (!s || s.uid !== process.getuid?.() || s.isSymbolicLink() || (directory ? !s.isDirectory() : !s.isFile() || s.nlink !== 1))
    throw Error("Unsafe retained entry");
  return s;
}
function privateRoot(p) {
  const s = owned(p, true);
  if (s.mode & 0o077) throw Error("Retained root must be private");
  return s;
}
export function readJSON(p, limit = 131072) {
  const s = owned(p);
  if (s.size > limit) throw Error("Retained metadata limit");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function sync(p) {
  const fd = fs.openSync(p, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
export function atomicJSON(p, value) {
  safePath(p);
  const temp = p + "." + randomUUID() + ".tmp";
  try {
    const fd = fs.openSync(temp, "wx", 0o600);
    try {
      fs.writeFileSync(fd, JSON.stringify(value));
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temp, p);
    sync(path.dirname(p));
  } finally {
    if (entry(temp)) fs.unlinkSync(temp);
  }
}
export function available(root) {
  privateRoot(root);
  if (entry(path.join(root, RESTORE_FILE)) && readJSON(path.join(root, RESTORE_FILE)).state !== "complete")
    throw Error("Retained restore is incomplete");
}
function lock(file, exclusive) {
  safePath(file);
  if (entry(file)) owned(file);
  const db = new DatabaseSync(file);
  try {
    fs.chmodSync(file, 0o600);
    db.exec("PRAGMA busy_timeout=0; PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS lease(id INTEGER PRIMARY KEY);");
    db.exec(exclusive ? "BEGIN EXCLUSIVE" : "BEGIN; SELECT count(*) FROM lease;");
  } catch (e) {
    db.close();
    throw Error("Retained storage is owned or unavailable", { cause: e });
  }
  return () => {
    try {
      db.exec("ROLLBACK");
    } finally {
      db.close();
    }
  };
}
export function rootLease(root, exclusive = false, restoring = false) {
  if (restoring) privateRoot(root);
  else available(root);
  return lock(path.join(root, "retained-root-lease.sqlite"), exclusive);
}
export function maintenanceLease(directory) {
  const releaseRoot = rootLease(path.dirname(directory));
  try {
    privateRoot(directory);
    const releaseStore = lock(path.join(directory, "store-lease.sqlite"), true);
    return () => {
      try {
        releaseStore();
      } finally {
        releaseRoot();
      }
    };
  } catch (e) {
    releaseRoot();
    throw e;
  }
}
function stable(v) {
  return JSON.stringify(v, (_, x) =>
    x && typeof x === "object" && !Array.isArray(x)
      ? Object.fromEntries(
          Object.keys(x)
            .sort()
            .map((k) => [k, x[k]]),
        )
      : x,
  );
}
const activeLeases = (globalThis[Symbol.for("uns-kit.retained-store.leases.v1")] ??= new Map());
export function runtimeLease(binding) {
  if (activeLeases.has(binding.directory)) throw Error("Retained store already owned by this process");
  const release = maintenanceLease(binding.directory);
  try {
    if (entry(path.join(binding.directory, OPERATION))) throw Error("Retained operation is incomplete");
    if (entry(path.join(binding.directory, "store-returned.json"))) throw Error("Retained store is retired");
    const current = readJSON(path.join(binding.directory, "store.json"));
    declaration(readJSON(path.join(binding.directory, DECLARATION)), binding.format);
    if (stable(current) !== stable(binding)) throw Error("Retained owner changed");
    let closed = false;
    const held = {
      close() {
        if (closed) return;
        closed = true;
        activeLeases.delete(binding.directory);
        release();
      },
    };
    activeLeases.set(binding.directory, held);
    return held;
  } catch (e) {
    release();
    throw e;
  }
}
function sha(file) {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  const h = createHash("sha256"),
    buffer = Buffer.alloc(65536);
  try {
    let n;
    while ((n = fs.readSync(fd, buffer)) > 0) h.update(buffer.subarray(0, n));
    return h.digest("hex");
  } finally {
    fs.closeSync(fd);
  }
}
function relative(p) {
  return (
    typeof p === "string" &&
    p.length <= 4096 &&
    !path.isAbsolute(p) &&
    p.split("/").every((x) => x !== "." && x !== ".." && /^[a-zA-Z0-9._-]{1,255}$/.test(x))
  );
}
function binding(value, root, id) {
  if (
    !value ||
    value.protocol !== 1 ||
    value.storeId !== id ||
    !uuid.test(id) ||
    !uuid.test(value.generation) ||
    !uuid.test(value.operationId) ||
    value.directory !== path.join(root, id) ||
    !safe.test(value.format) ||
    !hash.test(value.configurationDigest) ||
    value.maintenance !== true ||
    !value.owner ||
    !["rttNode", "version", "instanceId"].every((k) => safe.test(value.owner[k]))
  )
    throw Error("Invalid retained binding");
  return value;
}
function declaration(value, format) {
  if (
    !value ||
    value.protocol !== 1 ||
    value.storageProtocol !== 1 ||
    value.format !== format ||
    !Array.isArray(value.resources) ||
    value.resources.length < 1 ||
    value.resources.length > 16
  )
    throw Error("Invalid retained resources");
  const ids = new Set();
  for (const r of value.resources) {
    if (
      !safe.test(r.id) ||
      ["store.json", DECLARATION, OPERATION, "store-lease.sqlite", "store-returned.json"].includes(r.id) ||
      ids.has(r.id) ||
      !relative(r.path) ||
      !["file", "directory"].includes(r.kind) ||
      typeof r.required !== "boolean" ||
      !["payload", "metadata"].includes(r.backup ?? "payload")
    )
      throw Error("Invalid retained resource");
    ids.add(r.id);
  }
  return value;
}
function checkLimit(limits) {
  if (
    limits.schemaVersion !== 1 ||
    limits.maximumStores !== 32 ||
    limits.maximumEntries !== 100000 ||
    limits.maximumBytes !== 68719476736 ||
    limits.maximumManifestBytes !== 33554432 ||
    limits.maximumFileBytes !== 34359738368 ||
    limits.maximumDepth !== 32
  )
    throw Error("Unsupported retained bounds");
}
export function captureRetained(root, destination, mode, limits) {
  checkLimit(limits);
  if (!["include", "exclude"].includes(mode)) throw Error("Invalid retained policy");
  privateRoot(destination);
  const release = rootLease(root, true),
    releases = [];
  try {
    const manifest = { schemaVersion: 1, kind: "uns-retained-store-snapshot", sourceRoot: root, mode, stores: [], entries: [] };
    let total = 0;
    function copy(source, rel, depth = 0) {
      if (depth > limits.maximumDepth || !relative(rel)) throw Error("Retained depth/path limit");
      const s = owned(source, safeDirectory(source));
      if (manifest.entries.length >= limits.maximumEntries) throw Error("Retained entry limit");
      const out = path.join(destination, rel);
      if (s.isDirectory()) {
        fs.mkdirSync(out, { recursive: true, mode: 0o700 });
        manifest.entries.push({ path: rel, type: "directory" });
        for (const n of fs.readdirSync(source).sort()) copy(path.join(source, n), rel + "/" + n, depth + 1);
      } else {
        total += s.size;
        if (total > limits.maximumBytes || s.size > limits.maximumFileBytes) throw Error("Retained byte limit");
        fs.mkdirSync(path.dirname(out), { recursive: true, mode: 0o700 });
        const before = sha(source);
        fs.copyFileSync(source, out, fs.constants.COPYFILE_EXCL);
        fs.chmodSync(out, 0o600);
        const after = owned(source),
          result = sha(out);
        if (before !== result || before !== sha(source) || after.ino !== s.ino || after.size !== s.size || after.mtimeMs !== s.mtimeMs)
          throw Error("Retained source changed");
        manifest.entries.push({ path: rel, type: "file", size: s.size, sha256: result });
      }
    }
    const names = fs.readdirSync(root).sort();
    for (const id of names) {
      if (["retained-root-lease.sqlite", RESTORE_FILE].includes(id)) continue;
      if (!uuid.test(id) || manifest.stores.length >= limits.maximumStores) throw Error("Unexpected retained root entry");
      const dir = path.join(root, id);
      privateRoot(dir);
      const unlock = lock(path.join(dir, "store-lease.sqlite"), true);
      releases.push(unlock);
      if (entry(path.join(dir, OPERATION))) throw Error("Retained operation is incomplete");
      const b = binding(readJSON(path.join(dir, "store.json")), root, id);
      const d = declaration(readJSON(path.join(dir, DECLARATION)), b.format);
      if (entry(path.join(dir, "store-returned.json"))) {
        if (fs.readdirSync(dir).some((n) => !["store.json", DECLARATION, "store-lease.sqlite", "store-returned.json"].includes(n)))
          throw Error("Undeclared returned-store entry");
        const r = readJSON(path.join(dir, "store-returned.json"));
        if (r.operationId !== b.operationId || r.generation !== b.generation || d.resources.some((x) => entry(path.join(dir, x.id))))
          throw Error("Uncertain returned store");
        fs.mkdirSync(path.join(destination, id), { mode: 0o700 });
        manifest.entries.push({ path: id, type: "directory" });
        for (const name of ["store.json", DECLARATION, "store-returned.json"]) copy(path.join(dir, name), id + "/" + name);
        manifest.stores.push({ binding: b, storeId: id, generation: b.generation, owner: b.owner, format: b.format, resources: [], retired: true });
        continue;
      }
      const allowed = new Set(["store.json", DECLARATION, "store-lease.sqlite", ...d.resources.map((x) => x.id)]);
      if (fs.readdirSync(dir).some((n) => !allowed.has(n))) throw Error("Undeclared retained entry");
      fs.mkdirSync(path.join(destination, id), { mode: 0o700 });
      manifest.entries.push({ path: id, type: "directory" });
      copy(path.join(dir, "store.json"), id + "/store.json");
      copy(path.join(dir, DECLARATION), id + "/" + DECLARATION);
      const resources = [];
      for (const r of d.resources) {
        const p = path.join(dir, r.id),
          s = entry(p);
        if (!s) {
          if (r.required) throw Error("Required retained resource missing");
          continue;
        }
        if ((r.kind === "directory") !== s.isDirectory()) throw Error("Retained resource kind mismatch");
        const included = mode === "include" || r.backup === "metadata";
        resources.push({ id: r.id, backup: r.backup ?? "payload", included });
        if (included) copy(p, id + "/" + r.id);
      }
      manifest.stores.push({ binding: b, storeId: id, generation: b.generation, owner: b.owner, format: b.format, resources });
    }
    const raw = JSON.stringify(manifest);
    if (Buffer.byteLength(raw) > limits.maximumManifestBytes) throw Error("Retained manifest limit");
    atomicJSON(path.join(destination, "manifest.json"), manifest);
    return {
      state: manifest.stores.length ? (mode === "include" ? "included" : "excluded") : "empty",
      stores: manifest.stores.length,
      files: manifest.entries.filter((e) => e.type === "file").length,
      payloadBytes: total,
    };
  } finally {
    for (const f of releases.reverse()) f();
    release();
  }
}
function safeDirectory(p) {
  return entry(p)?.isDirectory() === true;
}
function exactTree(directory, expected) {
  const actual = new Set();
  function walk(dir, prefix = "") {
    for (const n of fs.readdirSync(dir)) {
      const rel = prefix ? prefix + "/" + n : n,
        p = path.join(dir, n);
      const st = owned(p, safeDirectory(p));
      actual.add(rel);
      if (st.isDirectory()) walk(p, rel);
    }
  }
  walk(directory);
  if (actual.size !== expected.size || [...actual].some((p) => !expected.has(p))) throw Error("Undeclared snapshot or staging entry");
}
function verifySnapshot(source, limits) {
  const m = readJSON(path.join(source, "manifest.json"), limits.maximumManifestBytes);
  if (
    m.schemaVersion !== 1 ||
    m.kind !== "uns-retained-store-snapshot" ||
    !["include", "exclude"].includes(m.mode) ||
    !Array.isArray(m.stores) ||
    m.stores.length > limits.maximumStores ||
    !Array.isArray(m.entries) ||
    m.entries.length > limits.maximumEntries
  )
    throw Error("Invalid retained snapshot");
  let total = 0;
  const seen = new Set();
  for (const e of m.entries) {
    if (!relative(e.path) || !uuid.test(e.path.split("/")[0]) || seen.has(e.path) || !["file", "directory"].includes(e.type))
      throw Error("Invalid retained snapshot path");
    seen.add(e.path);
    const p = path.join(source, e.path),
      s = owned(p, e.type === "directory");
    if (e.type === "file") {
      total += s.size;
      if (!hash.test(e.sha256) || s.size !== e.size || s.size > limits.maximumFileBytes || total > limits.maximumBytes || sha(p) !== e.sha256)
        throw Error("Retained snapshot changed");
    }
  }
  const allowed = new Set(["manifest.json", ...m.entries.map((e) => e.path)]);
  exactTree(source, allowed);
  const stores = new Map(m.stores.map((v) => [v.storeId, v]));
  if (stores.size !== m.stores.length || m.entries.some((e) => !stores.has(e.path.split("/")[0]))) throw Error("Undeclared snapshot store");
  for (const st of m.stores) {
    const b = binding(readJSON(path.join(source, st.storeId, "store.json")), m.sourceRoot, st.storeId);
    declaration(readJSON(path.join(source, st.storeId, DECLARATION)), b.format);
    if (st.retired) {
      const r = readJSON(path.join(source, st.storeId, "store-returned.json"));
      if (
        r.operationId !== b.operationId ||
        r.generation !== b.generation ||
        m.entries.some(
          (e) =>
            e.path.startsWith(st.storeId + "/") && !["store.json", DECLARATION, "store-returned.json"].includes(e.path.slice(st.storeId.length + 1)),
        )
      )
        throw Error("Invalid retired-store receipt");
    }
    if (st.generation !== b.generation || stable(st.binding) !== stable(b)) throw Error("Snapshot owner differs");
  }
  return m;
}
export function restoreRetained(source, root, backupDigest, limits, afterPublish) {
  checkLimit(limits);
  if (!hash.test(backupDigest)) throw Error("Invalid reviewed backup digest");
  const m = verifySnapshot(source, limits);
  if (m.mode !== "include") throw Error("Payloads excluded; reuse preserved storage");
  if (m.sourceRoot !== root) throw Error("Retained restore requires the same logical Runtime root");
  const release = rootLease(root, true, true);
  try {
    const markerFile = path.join(root, RESTORE_FILE),
      manifestDigest = sha(path.join(source, "manifest.json"));
    let marker = entry(markerFile) ? readJSON(markerFile) : null;
    if (marker && (marker.backupDigest !== backupDigest || marker.manifestDigest !== manifestDigest))
      throw Error("Another retained restore owns this target");
    const ids = m.stores.map((s) => s.storeId);
    if (new Set(ids).size !== ids.length || ids.some((id) => !uuid.test(id))) throw Error("Invalid snapshot store identities");
    if (marker?.state === "complete") {
      for (const id of ids) binding(readJSON(path.join(root, id, "store.json")), root, id);
      return { state: "complete", stores: ids.length };
    }
    if (!marker) {
      if (fs.readdirSync(root).some((n) => n !== "retained-root-lease.sqlite")) throw Error("Retained target is not empty");
      marker = { protocol: 1, state: "applying", backupDigest, manifestDigest };
      atomicJSON(markerFile, marker);
    }
    const stage = path.join(root, "retained-restore-stage");
    if (!entry(stage)) fs.mkdirSync(stage, { mode: 0o700 });
    privateRoot(stage);
    const allowed = new Set(["retained-root-lease.sqlite", RESTORE_FILE, "retained-restore-stage", ...ids]);
    if (fs.readdirSync(root).some((n) => !allowed.has(n))) throw Error("Unexpected restore target entry");
    for (const e of m.entries) {
      const id = e.path.split("/")[0],
        published = entry(path.join(root, id)),
        out = path.join(published ? root : stage, e.path),
        src = path.join(source, e.path);
      if (e.type === "directory") {
        if (!entry(out)) fs.mkdirSync(out, { recursive: true, mode: 0o700 });
        privateRoot(out);
      } else {
        const current = entry(out);
        if (current) {
          const stat = owned(out);
          if (stat.size === e.size && sha(out) === e.sha256) continue;
          if (published) throw Error("Published retained bytes differ; preserve them for review");
        }
        if (published) throw Error("Published retained store is incomplete");
        fs.mkdirSync(path.dirname(out), { recursive: true, mode: 0o700 });
        const part = path.join(stage, "retained-file-" + createHash("sha256").update(e.path).digest("hex") + ".part");
        if (entry(part)) owned(part);
        fs.copyFileSync(src, part);
        fs.chmodSync(part, 0o600);
        const fd = fs.openSync(part, "r");
        try {
          fs.fsyncSync(fd);
        } finally {
          fs.closeSync(fd);
        }
        if (sha(part) !== e.sha256) throw Error("Restored retained bytes differ");
        fs.renameSync(part, out);
        sync(path.dirname(out));
      }
    }
    const knownParts = new Set(
      m.entries.filter((e) => e.type === "file").map((e) => "retained-file-" + createHash("sha256").update(e.path).digest("hex") + ".part"),
    );
    for (const name of fs.readdirSync(stage)) {
      if (knownParts.has(name)) {
        owned(path.join(stage, name));
        fs.unlinkSync(path.join(stage, name));
      } else if (!ids.includes(name)) throw Error("Unexpected restore staging entry");
    }

    for (const id of ids) {
      const dir = entry(path.join(root, id)) ? path.join(root, id) : path.join(stage, id),
        prefix = id + "/";
      exactTree(dir, new Set(m.entries.filter((e) => e.path.startsWith(prefix)).map((e) => e.path.slice(prefix.length))));
    }
    for (const e of [...m.entries].filter((e) => e.type === "directory").sort((a, b) => b.path.length - a.path.length)) {
      const base = entry(path.join(root, e.path.split("/")[0])) ? root : stage;
      sync(path.join(base, e.path));
    }
    for (const id of ids) {
      const to = path.join(root, id);
      if (!entry(to)) {
        binding(readJSON(path.join(stage, id, "store.json")), root, id);
        fs.renameSync(path.join(stage, id), to);
        sync(stage);
        sync(root);
        afterPublish?.(id);
      }
    }
    if (fs.readdirSync(stage).length) throw Error("Unknown restore staging files");
    fs.rmdirSync(stage);
    atomicJSON(markerFile, { ...marker, state: "complete" });
    return { state: "complete", stores: ids.length };
  } finally {
    release();
  }
}
export function retainedStorageMain() {
  const [action, root, out, raw, modeOrDigest] = process.argv.slice(2),
    limits = JSON.parse(raw);
  const result =
    action === "capture"
      ? captureRetained(root, out, modeOrDigest, limits)
      : action === "restore"
        ? restoreRetained(root, out, modeOrDigest, limits)
        : (() => {
            throw Error("Unknown retained operation");
          })();
  process.stdout.write(JSON.stringify(result));
}
