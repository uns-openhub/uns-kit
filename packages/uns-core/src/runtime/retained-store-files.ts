import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export function retainedStoreDigest(value: unknown): string {
  const stable = (v: any): any =>
    Array.isArray(v)
      ? v.map(stable)
      : v && typeof v === "object"
        ? Object.fromEntries(
            Object.keys(v)
              .sort()
              .map((k) => [k, stable(v[k])]),
          )
        : v;
  return createHash("sha256")
    .update(JSON.stringify(stable(value)))
    .digest("hex");
}
export function retainedStoreEntry(file: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(file);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return;
    throw e;
  }
}
export function assertRetainedStorePath(file: string): void {
  if (!path.isAbsolute(file) || path.normalize(file) !== file) throw Error("Unnormalized retained store path");
  for (let p = file; p !== path.dirname(p); p = path.dirname(p)) {
    const s = retainedStoreEntry(p);
    if (s?.isSymbolicLink()) throw Error("Linked retained store path");
  }
}
export function readRetainedStoreJson(file: string): unknown {
  assertRetainedStorePath(file);
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const s = fs.fstatSync(fd);
    if (!s.isFile() || s.nlink !== 1 || s.size > 128 * 1024 || s.uid !== process.getuid?.()) throw Error("Unsafe retained store metadata");
    return JSON.parse(fs.readFileSync(fd, "utf8"));
  } finally {
    fs.closeSync(fd);
  }
}

export function syncRetainedDirectory(directory: string): void {
  const fd = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}
export function writeRetainedStoreJson(file: string, value: unknown): void {
  assertRetainedStorePath(file);
  const temporary = file + "." + randomUUID() + ".tmp";
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > 128 * 1024) throw Error("Retained store metadata too large");
  try {
    const fd = fs.openSync(temporary, "wx", 0o600);
    try {
      fs.writeFileSync(fd, text);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, file);
    syncRetainedDirectory(path.dirname(file));
  } finally {
    if (retainedStoreEntry(temporary)) fs.unlinkSync(temporary);
  }
}
