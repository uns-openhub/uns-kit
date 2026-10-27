import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { lstatSync, readdirSync } from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import type { MqttPublishQueueOptions } from "./publish-queue-contract.js";

/** Disabling persistence is not permission to hide retained output or a live owner. */
export function assertDisabledPublishQueueEmpty(config: MqttPublishQueueOptions, identity: string): void {
  if (config.enabled !== false) return;
  assertRetainedPublishQueueEmpty(config, identity);
}

/** Host preflight may prove emptiness; the launching SDK still verifies its exact identity. */
export function assertRetainedPublishQueueEmpty(config: MqttPublishQueueOptions, identity?: string): void {
  if (!path.isAbsolute(config.directory) || path.parse(config.directory).root === config.directory)
    throw Error("Disabled MQTT queue still requires a dedicated absolute directory");
  for (let p = config.directory; p !== path.dirname(p); p = path.dirname(p)) {
    try { if (lstatSync(p).isSymbolicLink()) throw Error("Unsafe disabled MQTT queue path"); }
    catch (e) { if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e; }
  }
  let entries: string[];
  try {
    const s = lstatSync(config.directory);
    if (!s.isDirectory() || s.mode & 0o077 || (process.getuid && s.uid !== process.getuid()))
      throw Error("Retained disabled MQTT queue directory is unsafe");
    entries = readdirSync(config.directory);
  } catch (e) { if ((e as NodeJS.ErrnoException).code === "ENOENT") return; throw e; }
  if (!entries.length) return;
  const allowed = new Set(["mqtt-outbox.sqlite", "mqtt-outbox.sqlite-wal", "mqtt-outbox.sqlite-shm", "mqtt-outbox-owner.sqlite", "mqtt-outbox-owner.sqlite-journal"]);
  if (entries.some(name => !allowed.has(name)) || !entries.includes("mqtt-outbox.sqlite") || !entries.includes("mqtt-outbox-owner.sqlite"))
    throw Error("Unknown retained MQTT state; disabling is blocked");
  for (const name of entries) {
    const s = lstatSync(path.join(config.directory, name));
    if (!s.isFile() || s.isSymbolicLink() || s.nlink !== 1 || s.mode & 0o077 || (process.getuid && s.uid !== process.getuid()))
      throw Error("Unsafe retained MQTT output file");
  }
  const { DatabaseSync: Sqlite } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  let lease: DatabaseSync | undefined, db: DatabaseSync | undefined;
  try {
    lease = new Sqlite(path.join(config.directory, "mqtt-outbox-owner.sqlite"), { readOnly: true });
    lease.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
    db = new Sqlite(path.join(config.directory, "mqtt-outbox.sqlite"), { readOnly: true });
    const metadata = db.prepare("SELECT value FROM metadata WHERE key=?");
    if (!["1", "2"].includes(String(metadata.get("schema")?.value)) || (identity !== undefined ? metadata.get("identity")?.value !== createHash("sha256").update(identity).digest("hex") : !/^[a-f0-9]{64}$/.test(String(metadata.get("identity")?.value))))
      throw Error("Retained MQTT output identity differs; disabling is blocked");
    if (Number(db.prepare("SELECT COUNT(*) AS n FROM pending").get()?.n) !== 0)
      throw Error("Retained MQTT output is not empty; drain it with its original owner before disabling");
  } finally { db?.close(); lease?.close(); }
}
