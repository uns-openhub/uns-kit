// Fixed helper-owned program. No request-supplied paths, code, SQL or package imports.
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { DatabaseSync, backup } from "node:sqlite";

const [root, output] = process.argv.slice(2);
const hash = (b) => crypto.createHash("sha256").update(b).digest("hex");
const fileHash = (p) => {
  const h = crypto.createHash("sha256"),
    buffer = Buffer.alloc(64 * 1024),
    fd = fs.openSync(p, "r");
  try {
    let n;
    while ((n = fs.readSync(fd, buffer, 0, buffer.length, null))) h.update(buffer.subarray(0, n));
    return h.digest("hex");
  } finally {
    fs.closeSync(fd);
  }
};
const fail = () => {
  throw Error("Persistent admission state is not safe to snapshot; preserve it and review configuration");
};
const check = (p, directory = false, optional = false) => {
  let s;
  try {
    s = fs.lstatSync(p);
  } catch (e) {
    if (optional && e.code === "ENOENT") return;
    throw e;
  }
  if (s.isSymbolicLink() || (directory ? !s.isDirectory() : !s.isFile() || s.nlink !== 1) || s.uid !== process.getuid() || (s.mode & 0o077) !== 0)
    fail();
  if (!directory && s.size > 512 * 1024 * 1024) fail();
};
if (
  process.platform !== "linux" ||
  !root ||
  root !== path.resolve(root) ||
  !root.startsWith("/var/lib/") ||
  !output ||
  output !== path.resolve(output)
)
  fail();
for (let p = root; p !== "/"; p = path.dirname(p)) if (fs.lstatSync(p).isSymbolicLink()) fail();
check(root, true);
check(output, true);
const mount = fs
  .readFileSync("/proc/self/mountinfo", "utf8")
  .split("\n")
  .map((s) => s.split(" "))
  .find((f) => f[4] === root);
if (!mount || !["ext4", "xfs", "btrfs", "zfs"].includes(mount[mount.indexOf("-") + 1])) fail();

let capturedBytes = 0,
  snapshotBytes = 0;
const manifest = {
  schemaVersion: 1,
  kind: "uns-process-group-journal-snapshot",
  capturedAt: new Date().toISOString(),
  groups: [],
  restorePolicy: "offline-review-required-no-automatic-replay",
};
// A configured empty volume is explicit empty coverage, never confused with absent configuration.
const rootEntries = fs.readdirSync(root, { withFileTypes: true });
if (rootEntries.length > 128 || rootEntries.some((e) => e.isSymbolicLink())) fail();
const namespaces = rootEntries
  .filter((e) => e.isDirectory())
  .map((e) => e.name)
  .sort();
if (namespaces.length > 64) fail();
const capturedGroups = new Set();
for (const namespace of namespaces) {
  if (!/^[a-z][a-z0-9-]{0,99}$/.test(namespace)) fail();
  const namespaceRoot = path.join(root, namespace),
    admittedRoot = path.join(namespaceRoot, "admission");
  check(namespaceRoot, true);
  try {
    fs.lstatSync(admittedRoot);
  } catch (e) {
    if (e.code === "ENOENT") continue;
    throw e;
  }
  check(admittedRoot, true);
  const groups = fs.readdirSync(admittedRoot).sort();
  if (groups.length > 9) fail();
  for (const group of groups) {
    if (!/^[a-z][a-z0-9-]{0,99}$/.test(group) || capturedGroups.has(group) || capturedGroups.size >= 9) fail();
    capturedGroups.add(group);
    const source = path.join(admittedRoot, group);
    check(source, true);
    for (const name of ["admission.sqlite", "ownership.sqlite"]) {
      check(path.join(source, name));
      for (const suffix of ["-wal", "-shm", "-journal"]) check(path.join(source, name + suffix), false, true);
    }
    const sourceSize = ["admission.sqlite", "admission.sqlite-wal", "ownership.sqlite", "ownership.sqlite-journal"].reduce((total, name) => {
      try {
        return total + fs.statSync(path.join(source, name)).size;
      } catch (e) {
        if (e.code === "ENOENT") return total;
        throw e;
      }
    }, 0);
    capturedBytes += sourceSize;
    if (capturedBytes > 512 * 1024 * 1024) fail();
    const space = fs.statfsSync(output, { bigint: true });
    if (space.bavail * space.bsize < BigInt(sourceSize * 2 + 64 * 1024 * 1024)) fail();
    const target = path.join(output, group);
    fs.mkdirSync(target, { mode: 0o700 });
    const owner = new DatabaseSync(path.join(source, "ownership.sqlite"), { readOnly: true });
    let db;
    try {
      owner.exec("PRAGMA busy_timeout=1000; BEGIN");
      if (!owner.prepare("SELECT value FROM namespace WHERE id=1").get()?.value) fail(); // Holds namespace replacement out for both snapshots.
      db = new DatabaseSync(path.join(source, "admission.sqlite"), { readOnly: true });
      db.exec("PRAGMA busy_timeout=3000");
      const pageSize = Number(db.prepare("PRAGMA page_size").get().page_size);
      if (!Number.isSafeInteger(pageSize) || pageSize < 512 || pageSize > 65536) fail();
      await backup(db, path.join(target, "admission.sqlite"), {
        rate: 128,
        progress: ({ totalPages }) => {
          const bytes = totalPages * pageSize,
            space = fs.statfsSync(output, { bigint: true });
          if (
            !Number.isSafeInteger(bytes) ||
            bytes > 512 * 1024 * 1024 - snapshotBytes ||
            space.bavail * space.bsize < BigInt(bytes + 64 * 1024 * 1024)
          )
            fail();
        },
      });
      await backup(owner, path.join(target, "ownership.sqlite"), { rate: 128 });
    } finally {
      db?.close();
      owner.close();
    }
    for (const name of ["admission.sqlite", "ownership.sqlite"]) fs.chmodSync(path.join(target, name), 0o600);
    const snap = new DatabaseSync(path.join(target, "admission.sqlite"));
    let info;
    try {
      if (snap.prepare("PRAGMA quick_check").get().quick_check !== "ok") fail();
      const meta = Object.fromEntries(
        snap
          .prepare("SELECT key,value FROM metadata")
          .all()
          .map((v) => [v.key, v.value]),
      );
      if (!/^[0-9a-f-]{36}$/.test(meta.id ?? "")) fail();
      const contract = JSON.parse(meta.contract);
      const profiles = snap.prepare("SELECT name,contract,completed,last_seq FROM profiles ORDER BY name").all();
      if (
        profiles.length < 1 ||
        profiles.length > 19 ||
        ![1, 2].includes(contract.version) ||
        !Array.isArray(contract.profiles) ||
        contract.profiles.length !== profiles.length
      )
        fail();
      if (contract.version === 2) {
        if (
          contract.generationId !== group ||
          !/^[a-f0-9]{64}$/.test(contract.groupSha256 ?? "") ||
          !/^[a-f0-9]{64}$/.test(contract.sourceIdentity ?? "") ||
          profiles.length < 2 ||
          profiles.length > 16 ||
          new Set(contract.profiles.map((p) => p.processName)).size !== profiles.length ||
          !Number.isSafeInteger(contract.maxRecords) ||
          contract.maxRecords < 1 ||
          contract.maxRecords > 100000 ||
          !Number.isSafeInteger(contract.maxBytes) ||
          contract.maxBytes < 1024 ||
          contract.maxBytes > 256 * 1024 * 1024
        )
          fail();
        for (const row of profiles) {
          const binding = JSON.parse(row.contract),
            expected = contract.profiles.find((p) => p.processName === row.name);
          if (
            !expected ||
            !/^[a-z][a-z0-9-]{0,99}$/.test(row.name) ||
            !/^[a-f0-9]{64}$/.test(expected.resourceSha256 ?? "") ||
            !/^[a-z][a-z0-9-]{0,99}$/.test(expected.dataGroup ?? "") ||
            binding.processName !== row.name ||
            binding.resourceSha256 !== expected.resourceSha256 ||
            binding.dataGroup !== expected.dataGroup
          )
            fail();
        }
      }
      const usage = snap.prepare("SELECT records,bytes,admitted FROM usage WHERE id=1").get();
      const actual = snap.prepare("SELECT count(*) records,coalesce(sum(bytes),0) bytes FROM tasks").get();
      if (
        !usage ||
        usage.records !== actual.records ||
        usage.bytes !== actual.bytes ||
        usage.records > contract.maxRecords ||
        usage.bytes > contract.maxBytes
      )
        fail();
      info = {
        group,
        namespace,
        ...(contract.version === 2
          ? { journalContractVersion: 2, generationId: contract.generationId, configurationGroupSha256: contract.groupSha256 }
          : {}),
        journalId: meta.id,
        clientId: namespace + "-admission-" + meta.id,
        contractSha256: hash(meta.contract),
        usage,
        profiles: profiles.map((p) => ({ name: p.name, contractSha256: hash(p.contract), completed: p.completed, lastSeq: p.last_seq })),
      };
    } finally {
      snap.close();
    }
    // Snapshot may have WAL mode persisted; force a self-contained DB and no sidecars.
    for (const name of ["admission.sqlite", "ownership.sqlite"]) {
      const p = path.join(target, name);
      const d = new DatabaseSync(p);
      try {
        d.exec("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE");
      } finally {
        d.close();
      }
      const fd = fs.openSync(p, "r");
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      snapshotBytes += fs.statSync(p).size;
      if (snapshotBytes > 512 * 1024 * 1024) fail();
      info[name === "admission.sqlite" ? "admissionSha256" : "ownershipSha256"] = fileHash(p);
    }
    manifest.groups.push(info);
  }
}
const remaining = fs.statfsSync(output, { bigint: true });
if (remaining.bavail * remaining.bsize < BigInt(snapshotBytes + 64 * 1024 * 1024)) fail();
fs.writeFileSync(path.join(output, "manifest.json"), JSON.stringify(manifest) + "\n", { mode: 0o600, flag: "wx" });
// No source payloads, credentials, paths or process metadata on stdout/stderr.
