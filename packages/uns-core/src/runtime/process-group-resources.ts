import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const sha256 = (b: string | Buffer) => createHash("sha256").update(b).digest("hex");
function canonical(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(canonical);
  return v && typeof v === "object"
    ? Object.fromEntries(
        Object.entries(v)
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([k, x]) => [k, canonical(x)]),
      )
    : v;
}
export const configurationAuthorizationHash = (v: unknown): string => sha256(JSON.stringify(canonical(v)));
export function readProcessGroupResource(file: string, privateFile = false): Buffer {
  if (file.length > 512 || path.resolve(file) !== file) throw Error("Process-group authorization resource path is not normalized");
  for (let p = file; p !== "/"; p = path.dirname(p))
    if (fs.lstatSync(p).isSymbolicLink()) throw Error("Process-group authorization resource cannot contain links");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const s = fs.fstatSync(fd);
    if (!s.isFile() || s.size > 1048576 || s.nlink !== 1 || (privateFile && (s.uid !== process.getuid?.() || (s.mode & 0o077) !== 0)))
      throw Error("Process-group authorization resource is not a bounded private regular file");
    const b = fs.readFileSync(fd);
    if (b.length > 1048576) throw Error("Process-group authorization resource too large");
    return b;
  } finally {
    fs.closeSync(fd);
  }
}
/** Pins metadata and compiled JS/JSON modules, not an installed dependency tree. */
export function groupArtifactHash(root: string): string {
  const files = ["package.json"];
  let total = 0,
    entries = 0;
  const visit = (dir: string, depth = 0) => {
    if (depth > 32) throw Error("Process-group artifact directory depth exceeded");
    const directory = fs.opendirSync(dir);
    try {
      let e;
      while ((e = directory.readSync())) {
        if (++entries > 4096) throw Error("Process-group artifact directory budget exceeded");
        const f = path.join(dir, e.name);
        if (e.isSymbolicLink()) throw Error("Process-group artifact cannot contain links");
        if (e.isDirectory()) visit(f, depth + 1);
        else if (e.isFile() && /\.(?:[cm]?js|json)$/.test(e.name)) files.push(path.relative(root, f));
        if (files.length > 2048) throw Error("Process-group artifact file budget exceeded");
      }
    } finally {
      directory.closeSync();
    }
  };
  visit(path.join(root, "dist"));
  if (files.length < 2) throw Error("Process-group compiled artifact is missing");
  return configurationAuthorizationHash(
    files.sort().map((f) => {
      const b = readProcessGroupResource(path.join(root, f));
      total += b.length;
      if (total > 32 * 1024 * 1024) throw Error("Process-group artifact byte budget exceeded");
      return [f, sha256(b)];
    }),
  );
}
/** Endpoint/TLS identity only; credentials never become a policy field or diagnostic. */
export function groupChannelsHash(channels: unknown[]): string {
  if (channels.length !== 3) throw Error("Process-group requires infra, input and output channel identities");
  return configurationAuthorizationHash(
    channels.map((value) => {
      const c = value as Record<string, unknown>;
      if (typeof c["host"] !== "string" || !c["host"]) throw Error("Process-group broker host must be resolved");
      const protocol = c["protocol"] ?? "mqtt";
      return {
        host: c["host"],
        port: c["port"] ?? (protocol === "mqtts" ? 8883 : 1883),
        protocol,
        hosts: c["hosts"] ?? [],
        servers: c["servers"] ?? [],
        servername: c["servername"] ?? null,
        rejectUnauthorized: c["rejectUnauthorized"] ?? true,
        ca: c["ca"] === undefined ? null : sha256(String(c["ca"])),
        cert: c["cert"] === undefined ? null : sha256(String(c["cert"])),
      };
    }),
  );
}
