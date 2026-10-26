import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { describe, expect, it } from "vitest";

import {
  configurationGroupJournalContract,
  configurationGroupSnapshot,
  configurationResourceHash,
} from "../../packages/uns-core/src/runtime/configuration-group-contract.js";
import { processGroupSourceIdentity } from "../../packages/uns-core/src/runtime/process-group-contract.js";
import { inspectProcessGroupConfiguration } from "../../packages/uns-core/src/runtime/process-group-inspection.js";
import { selectedProcessGroupDescriptor } from "../../packages/uns-core/src/runtime/process-group-runtime-selection.js";
import { refreshProcessGroupStagedJournal } from "../../packages/uns-core/src/runtime/process-group-staging.js";
import { processGroupReceiverSupervision } from "../../packages/uns-core/src/runtime/process-group-supervision.js";

export function inspectionFixture(name = "inspection-adapter", selector = "inspection") {
  const root = fs.mkdtempSync((process.platform === "linux" ? "/tmp/" : "/private/tmp/") + "configured-inspection-");
  const pkg = path.join(root, "pkg"),
    dir = path.join(pkg, "instances", "target");
  fs.mkdirSync(dir, { recursive: true });
  const descriptor = {
    protocolVersion: 2,
    namespace: "inspection",
    publisherPath: [selector],
    groupPath: [selector, "document"],
    memberPath: [selector, "member"],
    journalPath: ["durability"],
    runtimeRolePath: ["executionRole"],
    receiverRole: "ingress",
  };
  const group = {
    schemaVersion: 2,
    id: "cell",
    inputTopic: "example/input",
    processName: name + "-combined",
    receiverProcessName: name + "-receiver",
    profiles: ["a", "b"].map((id) => ({ id, processName: name + "-" + id, dataGroup: "inspection", resource: { rules: [{ equals: id }] } })),
  };
  const snapshot = configurationGroupSnapshot(group),
    journalId = randomUUID(),
    directory = path.join(root, "journal");
  fs.mkdirSync(directory, { mode: 0o700 });
  fs.writeFileSync(
    path.join(pkg, "package.json"),
    JSON.stringify({
      name,
      unsDatahub: { processGroupAdmission: 1, processGroupAdmissionSupervisor: 1, processGroupConfigurationRuntime: descriptor },
    }),
  );
  const journal = { directory, journalId, generationId: "cell-rev-a", groupSha256: snapshot.sha256, maxRecords: 100, maxBytes: 1048576 };
  const raw = {
    uns: { processName: group.processName },
    infra: { host: "127.0.0.1", port: 1883 },
    [selector]: { document: group },
    durability: journal,
  };
  const expected = configurationGroupJournalContract(
    snapshot,
    processGroupSourceIdentity(raw.infra),
    { ...journal, minFreeBytes: 1048576 },
    snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, resourceSha256, dataGroup })),
  );
  const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");
  const db = new DatabaseSync(path.join(directory, "admission.sqlite"));
  db.exec("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)");
  for (const [key, value] of Object.entries({
    id: journalId,
    contract: expected,
    capture: JSON.stringify({ state: "ready", pid: process.pid, heartbeat: Date.now() }),
  }))
    db.prepare("INSERT INTO metadata VALUES(?,?)").run(key, value);
  return {
    root,
    pkg,
    dir,
    descriptor,
    group,
    snapshot,
    raw,
    selector,
    db,
    journal,
    inspect: (value: unknown) =>
      inspectProcessGroupConfiguration(pkg, dir, JSON.stringify(value), "local", {
        configurationChecksum: (raw) => configurationResourceHash(JSON.parse(raw)),
        excludedUnsFields: [],
      }),
    close() {
      db.close();
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
}
describe("configuration-owned host inspection", () => {
  it("does not impose capability metadata on ordinary Node or Python deployments", () => {
    const root = fs.mkdtempSync((process.platform === "linux" ? "/tmp/" : "/private/tmp/") + "ordinary-runtime-");
    try {
      expect(selectedProcessGroupDescriptor(root, {})).toBeUndefined();
      fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name: "ordinary-service", version: "1.0.0" }));
      expect(selectedProcessGroupDescriptor(root, { settings: { anything: true } })).toBeUndefined();
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it.each([
    ["inspection-adapter", "inspection"],
    ["document-router", "routingRules"],
  ])("reviews %s using only data and exact disk bindings", (name, selector) => {
    const f = inspectionFixture(name, selector);
    try {
      const target = f.inspect(f.raw);
      expect(target.contract?.digest).toBe(f.snapshot.sha256);
      expect(target.generationId).toBe("cell-rev-a");
      for (const member of f.group.profiles) {
        const source = f.inspect({ ...f.raw, uns: { processName: member.processName }, [selector]: { document: f.group, member: member.id } });
        expect(source.contract).toBeNull();
        expect(source.mappingSha256).toBe(f.snapshot.profiles.find((p) => p.id === member.id)?.resourceSha256);
        expect(source.projectionChecksum).toBe(target.projectionChecksum);
      }
      const altered = structuredClone(f.raw);
      altered.durability.generationId = "cell-rev-b";
      expect(() => f.inspect(altered)).toThrow(/contract differs/);
      const changed = structuredClone(f.raw);
      (changed[selector] as { document: typeof f.group }).document.profiles[0]!.resource.rules[0]!.equals = "changed";
      expect(() => f.inspect(changed)).toThrow();
      f.db
        .prepare("UPDATE metadata SET value=? WHERE key='capture'")
        .run(JSON.stringify({ state: "stopped", pid: process.pid, heartbeat: Date.now() }));
      expect(() => f.inspect(f.raw)).toThrow(/not ready/);
    } finally {
      f.close();
    }
  });
  it("stages only stopped absent state; refuses existing/unknown initialization outcomes", () => {
    const f = inspectionFixture();
    try {
      const raw = { ...f.raw, durability: { ...f.journal, directory: path.join(f.root, "not-created"), journalId: undefined } };
      fs.writeFileSync(path.join(f.dir, "config.json"), JSON.stringify(raw));
      const d = selectedProcessGroupDescriptor(f.pkg, raw)!;
      refreshProcessGroupStagedJournal(f.dir, true, d);
      expect(fs.existsSync(path.join(f.dir, "process-group-staged-journal.json"))).toBe(true);
      fs.mkdirSync(raw.durability.directory);
      expect(() => refreshProcessGroupStagedJournal(f.dir, true, d)).toThrow(/Existing journal/);
      refreshProcessGroupStagedJournal(f.dir, false, d);
      expect(fs.existsSync(path.join(f.dir, "process-group-staged-journal.json"))).toBe(false);
    } finally {
      f.close();
    }
  });
  it("uses declared receiver roles for bounded supervision without package names", () => {
    const f = inspectionFixture();
    try {
      const raw = { ...f.raw, executionRole: "ingress" };
      const d = selectedProcessGroupDescriptor(f.pkg, raw)!;
      const caps = { supervised: true, persistent: false, authorized: false },
        supervisor = { enabled: true, restartOnExit: true, restartOnUnhealthy: false };
      expect(processGroupReceiverSupervision(raw, d, f.dir, caps, supervisor).stop_exit_codes).toEqual([0, 78]);
      expect(() => processGroupReceiverSupervision(raw, d, f.dir, { ...caps, supervised: false }, supervisor)).toThrow(/does not support/);
    } finally {
      f.close();
    }
  });
});
