import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { safePath, owned, sha, sync, entry, relative, checkLimit } from "./retained-store-storage.mjs";

function inventory(root, kind, limits) {
  checkLimit(limits);
  const entries = [];
  let bytes = 0;
  function visit(file, rel, depth) {
    if (depth > limits.maximumDepth || (rel && !relative(rel)) || entries.length >= limits.maximumEntries) throw Error("Retained copy path/count limit");
    const stat = entry(file);
    if (!stat) throw Error("Retained copy source is missing");
    if (stat.isDirectory()) {
      owned(file, true);
      entries.push({ path: rel, type: "directory" });
      for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name), rel ? rel + "/" + name : name, depth + 1);
    } else {
      const value = owned(file);
      bytes += value.size;
      if (bytes > limits.maximumBytes || value.size > limits.maximumFileBytes) throw Error("Retained copy byte limit");
      entries.push({ path: rel, type: "file", size: value.size, sha256: sha(file) });
    }
  }
  safePath(root);
  if ((kind === "directory") !== !!entry(root)?.isDirectory()) throw Error("Retained copy resource kind differs");
  visit(root, "", 0);
  return { entries, fingerprint: { digest: createHash("sha256").update(JSON.stringify(entries)).digest("hex"), bytes, entries: entries.length } };
}
export function inspectRetainedCopy(root, kind, limits) { return inventory(root, kind, limits).fingerprint; }
function same(a, b) { return a?.digest === b?.digest && a?.bytes === b?.bytes && a?.entries === b?.entries; }
function assertFingerprint(file, move, limits) {
  const stat = entry(file);
  if (!stat || stat.dev !== move.device || stat.ino !== move.inode || !same(inspectRetainedCopy(file, move.kind, limits), move.content)) throw Error("Retained copy source changed after review");
}
function privateStage(stage) {
  if (!entry(stage)) { fs.mkdirSync(stage, { mode: 0o700 }); sync(path.dirname(stage)); }
  if (owned(stage, true).mode & 0o077) throw Error("Retained copy staging must be private");
}
function checkStage(stage, entries) {
  const allowed = new Set(["payload", ...entries.map(e => e.path ? "payload/" + e.path : "payload")]);
  function visit(dir, prefix) {
    for (const name of fs.readdirSync(dir)) {
      const rel = prefix ? prefix + "/" + name : name, file = path.join(dir, name);
      if (!allowed.has(rel)) throw Error("Unexpected retained copy staging entry");
      const stat = entry(file);
      owned(file, !!stat?.isDirectory());
      if (stat.isDirectory()) visit(file, rel);
    }
  }
  visit(stage, "");
}
function copyFile(source, destination, expected) {
  if (entry(destination)) owned(destination);
  const input = fs.openSync(source, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  let output;
  try {
    output = fs.openSync(destination, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW, 0o600);
    const h = createHash("sha256"), buffer = Buffer.alloc(65536);
    let count = 0, size;
    while ((size = fs.readSync(input, buffer, 0, buffer.length, null))) {
      count += size;
      if (count > expected.size) throw Error("Retained copy source grew");
      h.update(buffer.subarray(0, size));
      let offset = 0;
      while (offset < size) offset += fs.writeSync(output, buffer, offset, size - offset);
    }
    fs.fchmodSync(output, 0o600); fs.fsyncSync(output);
    if (count !== expected.size || h.digest("hex") !== expected.sha256) throw Error("Retained copy source changed while reading");
  } finally { fs.closeSync(input); if(output!==undefined)fs.closeSync(output); }
}
function copyClosed(source, destination, stage, move, limits, onFile) {
  assertFingerprint(source, move, limits);
  const snapshot = inventory(source, move.kind, limits);
  if (entry(destination)) {
    if (!same(inspectRetainedCopy(destination, move.kind, limits), move.content)) throw Error("Published retained copy differs; preserve for review");
    if (entry(stage)) { checkStage(stage, snapshot.entries); fs.rmSync(stage, { recursive: true }); sync(path.dirname(stage)); }
    return;
  }
  privateStage(stage); checkStage(stage, snapshot.entries);
  const payload = path.join(stage, "payload");
  for (const item of snapshot.entries) {
    const from = item.path ? path.join(source, item.path) : source, to = item.path ? path.join(payload, item.path) : payload;
    if (item.type === "directory") {
      if (!entry(to)) fs.mkdirSync(to, { mode: 0o700 });
      if (owned(to, true).mode & 0o077) throw Error("Retained stage directory must be private");
    } else {
      if (entry(to) && owned(to).size === item.size && sha(to) === item.sha256) continue;
      copyFile(from, to, item); onFile?.(item.path);
    }
  }
  assertFingerprint(source, move, limits);
  if (!same(inspectRetainedCopy(payload, move.kind, limits), move.content)) throw Error("Retained staged copy differs");
  for (const item of [...snapshot.entries].reverse()) if (item.type === "directory") sync(item.path ? path.join(payload, item.path) : payload);
  fs.renameSync(payload, destination); sync(stage); sync(path.dirname(destination));
  fs.rmdirSync(stage); sync(path.dirname(stage));
}
export function applyRetainedCopy(plan, direction, limits, afterMove, onFile) {
  checkLimit(limits); safePath(plan.source); safePath(plan.directory);
  for (const move of plan.moves) {
    const legacy = path.join(plan.source, move.relative), stable = path.join(plan.directory, move.id);
    const stage = direction === "adopt" ? path.join(plan.directory, ".retained-copy-" + move.id) : path.join(plan.source, ".retained-return-" + plan.storeId + "-" + move.id);
    if (direction === "adopt") copyClosed(legacy, stable, stage, move, limits, onFile);
    else {
      assertFingerprint(stable, move, limits);
      const original = plan.originalMoves?.find(m => m.id === move.id), saved = path.join(plan.source, ".retained-original-" + plan.storeId + "-" + move.id);
      if (entry(legacy) && !same(inspectRetainedCopy(legacy, move.kind, limits), move.content)) {
        if (!original || entry(saved)) throw Error("Return would overwrite unreviewed legacy work");
        assertFingerprint(legacy, original, limits);
        fs.renameSync(legacy, saved); sync(path.dirname(legacy));
      }
      if (entry(saved)) {
        if (!original) throw Error("Unreviewed original retained copy");
        assertFingerprint(saved, original, limits);
      }
      copyClosed(stable, legacy, stage, move, limits, onFile);
    }
    afterMove?.(move.id);
  }
  if (direction === "return") {
    for (const original of plan.originalMoves ?? []) {
      const stage = path.join(plan.directory, ".retained-copy-" + original.id);
      if (!entry(stage)) continue;
      const legacy = path.join(plan.source, original.relative);
      if (!plan.moves.some(m => m.id === original.id)) assertFingerprint(legacy, original, limits);
      const reference = plan.moves.some(m => m.id === original.id) ? path.join(plan.directory, original.id) : legacy;
      const snapshot = inventory(reference, original.kind, limits);
      checkStage(stage, snapshot.entries);
      // Only incomplete scratch is discarded; all original bytes remain proven.
      fs.rmSync(stage, { recursive: true }); sync(plan.directory);
    }
  }
}
