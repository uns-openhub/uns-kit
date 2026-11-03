/** Local transactional receipt example; the authenticated SDK supplies actor.
 * This deliberately does not claim exactly-once remote/network side effects.
 * Node 22+ with node:sqlite. Put the database in application-owned durable state. */
import { operatorDigest } from "@uns-kit/core/runtime/operator-api.js";
import { DatabaseSync } from "node:sqlite";

export function maintenanceReceiptStore(databaseFile: string) {
  const db = new DatabaseSync(databaseFile);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
    CREATE TABLE IF NOT EXISTS maintenance_policy(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL);
    INSERT OR IGNORE INTO maintenance_policy VALUES(1,0);
    CREATE TABLE IF NOT EXISTS maintenance_receipt(request_id TEXT PRIMARY KEY, actor TEXT NOT NULL, digest TEXT NOT NULL, result TEXT NOT NULL);`);
  return {
    apply(request: { action: "apply"; requestId: string; expectedRevision: number }, actor: string) {
      // Call only after the SDK validates the declared schema and authority.
      const digest = operatorDigest(request);
      db.exec("BEGIN IMMEDIATE");
      try {
        const old = db.prepare("SELECT * FROM maintenance_receipt WHERE request_id=?").get(request.requestId);
        if (old) {
          if (old.actor !== actor || old.digest !== digest) throw new Error("request_id_conflict");
          db.exec("COMMIT");
          return JSON.parse(String(old.result));
        }
        const changed = db.prepare("UPDATE maintenance_policy SET revision=revision+1 WHERE id=1 AND revision=?").run(request.expectedRevision);
        if (changed.changes !== 1) throw new Error("revision_conflict");
        const result = { requestId: request.requestId, revision: request.expectedRevision + 1 };
        db.prepare("INSERT INTO maintenance_receipt VALUES(?,?,?,?)").run(request.requestId, actor, digest, JSON.stringify(result));
        db.exec("COMMIT");
        return result;
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
    close() {
      db.close();
    },
  };
}
