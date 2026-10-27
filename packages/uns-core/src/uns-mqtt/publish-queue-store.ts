import { createHash } from "node:crypto";
import { closeSync, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, realpathSync, statfsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";

import { openManagedOutputRoot, validateOutputRow } from "../runtime/mqtt-output-state-program.mjs";
import type { ResolvedMqttPublishQueueOptions } from "./publish-queue-contract.js";

export type PublishRecord = { seq: number; id: string; topic: string; message: string; options: { qos: 1 | 2; retain: boolean }; bytes: number };
const digest = (value: string) => createHash("sha256").update(value).digest("hex");

/** Private, exclusively owned output outbox. It never opens application journals. */
export class MqttPublishQueueStore {
  private db: DatabaseSync;
  private lease: DatabaseSync;
  private directory: string;
  private managed?: ReturnType<typeof openManagedOutputRoot>;
  private finalSummary?: { records: number; bytes: number; lastSeq: number };
  constructor(
    private config: ResolvedMqttPublishQueueOptions,
    identity: string,
  ) {
    if (!path.isAbsolute(config.directory) || path.parse(config.directory).root === config.directory)
      throw Error("MQTT publish queue requires a dedicated absolute directory");
    if (config.storage === "runtime" && !process.env["UNS_MQTT_OUTPUT_STATE_ROOT"])
      throw Error("Runtime MQTT output storage requires a reviewed permanent mount");
    this.managed = config.storage === "runtime" ? openManagedOutputRoot(config.directory, process.env["UNS_MQTT_OUTPUT_STATE_ROOT"]) : undefined;
    try {
      mkdirSync(config.directory, { recursive: true, mode: 0o700 });
      const dir = lstatSync(config.directory);
      if (!dir.isDirectory() || dir.isSymbolicLink() || dir.mode & 0o077 || (process.getuid && dir.uid !== process.getuid()))
        throw Error("MQTT publish queue directory must be private and owned by this user");
      this.directory = realpathSync(config.directory);
      const file = path.join(this.directory, "mqtt-outbox.sqlite");
      const leaseFile = path.join(this.directory, "mqtt-outbox-owner.sqlite");
      for (const name of [leaseFile, file, file + "-wal", file + "-shm", leaseFile + "-journal"])
        if (existsSync(name)) {
          const stat = lstatSync(name);
          if (!stat.isFile() || stat.isSymbolicLink() || stat.mode & 0o077 || (process.getuid && stat.uid !== process.getuid()))
            throw Error("Unsafe MQTT publish queue file; preserve and inspect it");
        }
      // node:sqlite is loaded only for the opt-in path; ordinary publishers are unchanged.
      const { DatabaseSync: Sqlite } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
      const openPrivate = (name: string) => {
        if (!existsSync(name)) closeSync(openSync(name, "wx", 0o600));
        return new Sqlite(name);
      };
      this.lease = openPrivate(leaseFile);
      this.lease.exec("PRAGMA busy_timeout=0; PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE");
      const existed = existsSync(file);
      if (existed) {
        const reader = new Sqlite(file, { readOnly: true });
        try {
          if (reader.prepare("PRAGMA page_size").get().page_size !== 4096)
            throw Error("Unsupported MQTT outbox page size; preserve and review storage");
          const meta = reader.prepare("SELECT value FROM metadata WHERE key=?");
          reader.prepare("SELECT seq,id,record,bytes,sha256 FROM pending LIMIT 1").get();
          if (
            meta.get("schema")?.value !== (this.managed ? "2" : "1") ||
            (this.managed && (meta.get("rootId")?.value !== this.managed.rootId || meta.get("relative")?.value !== this.managed.relative)) ||
            meta.get("identity")?.value !== digest(identity)
          )
            throw Error("MQTT outbox identity or schema differs; preserve pending work and use its original destination");
        } finally {
          reader.close();
        }
      }
      this.db = openPrivate(file);
      this.db.exec("PRAGMA busy_timeout=1000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=64");
      const pageLimit = Math.floor(config.maxDiskBytes / 4096);
      const selectedPages = Number(this.db.prepare(`PRAGMA max_page_count=${pageLimit}`).get().max_page_count);
      if (selectedPages > pageLimit) throw Error("Retained MQTT outbox exceeds maxDiskBytes; preserve and review storage");
      this.db.exec(`CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pending(seq INTEGER PRIMARY KEY,id TEXT UNIQUE NOT NULL,record TEXT NOT NULL,bytes INTEGER NOT NULL,sha256 TEXT NOT NULL);`);
      if (!existed)
        this.transaction(() => {
          this.db.prepare("INSERT INTO metadata VALUES(?,?)").run("schema", this.managed ? "2" : "1");
          if (this.managed) {
            this.db.prepare("INSERT INTO metadata VALUES(?,?)").run("rootId", this.managed.rootId);
            this.db.prepare("INSERT INTO metadata VALUES(?,?)").run("relative", this.managed.relative);
          }
          this.db.prepare("INSERT INTO metadata VALUES(?,?)").run("identity", digest(identity));
        });
      const fd = openSync(this.directory, "r");
      try {
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
    } catch (error) {
      this.db?.close();
      this.lease?.close();
      this.managed?.close();
      throw error;
    }
  }
  private transaction<T>(run: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = run();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  summary(): { records: number; bytes: number; lastSeq: number } {
    if (this.finalSummary) return { ...this.finalSummary };
    const row = this.db.prepare("SELECT COUNT(*) AS records,COALESCE(SUM(bytes),0) AS bytes,COALESCE(MAX(seq),0) AS lastSeq FROM pending").get();
    return { records: Number(row.records), bytes: Number(row.bytes), lastSeq: Number(row.lastSeq) };
  }
  head(): PublishRecord | undefined {
    const row = this.db.prepare("SELECT seq,id,record,bytes,sha256 FROM pending ORDER BY seq LIMIT 1").get();
    if (!row) return undefined;
    return validateOutputRow(row);
  }

  private physicalBytes(): number {
    return [
      "mqtt-outbox.sqlite",
      "mqtt-outbox.sqlite-wal",
      "mqtt-outbox.sqlite-shm",
      "mqtt-outbox-owner.sqlite",
      "mqtt-outbox-owner.sqlite-journal",
    ].reduce((total, name) => {
      const file = path.join(this.directory, name);
      try {
        return total + lstatSync(file).size;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return total;
        throw error;
      }
    }, 0);
  }

  put(record: PublishRecord): void {
    const space = statfsSync(this.directory, { bigint: true });
    if (space.bavail * space.bsize < BigInt(this.config.minFreeBytes)) throw Error("MQTT outbox free-space reserve exhausted");
    const serialized = JSON.stringify(record);
    // Account for WAL plus database growth, not just logical pending payloads.
    // Long-lived external readers may prevent truncation: refuse new admission
    // instead of letting their pinned WAL consume the filesystem indefinitely.
    const reserve = 2 * Buffer.byteLength(serialized) + 65536;
    if (this.physicalBytes() + reserve > this.config.maxDiskBytes) {
      this.db.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get();
      if (this.physicalBytes() + reserve > this.config.maxDiskBytes) throw Error("MQTT physical disk budget reached; admission rejected");
    }
    this.transaction(() => {
      const total = this.summary();
      if (total.records >= this.config.maxRecords || total.bytes + record.bytes > this.config.maxBytes)
        throw Error("MQTT disk queue capacity reached; admission rejected");
      this.db.prepare("INSERT INTO pending VALUES(?,?,?,?,?)").run(record.seq, record.id, serialized, record.bytes, digest(serialized));
    });
  }
  complete(record: PublishRecord): void {
    const result = this.db.prepare("DELETE FROM pending WHERE seq=? AND id=?").run(record.seq, record.id);
    if (Number(result.changes) !== 1) throw Error("MQTT outbox checkpoint differs; completion not confirmed");
  }
  close(): void {
    if (this.finalSummary) return;
    this.finalSummary = this.summary();
    try {
      this.db.close();
    } finally {
      this.lease.close();
      this.managed?.close();
    }
  }
}
