import fs from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

import { authorizeProcessGroupRuntime } from "../../packages/uns-core/src/runtime/process-group-authorization.js";
import { resolvedProcessGroupChannels } from "../../packages/uns-core/src/runtime/process-group-contract.js";
import { inspectGenerationJournal } from "../../packages/uns-core/src/runtime/process-group-generation.js";
import {
  replaceReviewedProcessGroupPolicy,
  reviewInstalledProcessGroupMqttOutput,
  reviewInstalledProcessGroupUpgrade,
} from "../../packages/uns-core/src/runtime/process-group-upgrade.js";
import { generationFixture } from "../helpers/process-group-generation-fixture.js";

vi.mock("../../packages/uns-core/src/runtime/mqtt-output-state-program.mjs", async (original) => ({
  ...(await original<object>()),
  inspectManagedOutputQueue: (_r: string, _d: string, identity: string) => ({
    rootId: "11111111-1111-4111-8111-111111111111",
    relative: "documents/combined",
    identity,
    records: 0,
    bytes: 0,
    exists: false,
    ownerActive: false,
  }),
}));

function fixture(name = "quality-adapter", nested = false) {
  const f = generationFixture(name, nested, true);
  const oldEnvironment = { ...process.env };
  process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = f.registry;
  delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
  for (const role of ["publisher", "receiver"] as const) {
    f.configs.target[role].raw = structuredClone(f.configs.source[role].raw);
    f.configs.target[role].resolved = f.configs.target[role].raw;
  }
  const context = (side: "source" | "target", role: "publisher" | "receiver") => {
    const c = f.configs[side][role];
    return {
      packageRoot: c.packageRoot,
      instanceDirectory: c.instanceDirectory,
      rawConfiguration: c.raw,
      channels: resolvedProcessGroupChannels(c.resolved),
      controllerName: "local",
      stateRoot: f.stateRoot,
    };
  };
  return {
    ...f,
    close() {
      try {
        f.close();
      } finally {
        process.env = oldEnvironment;
      }
    },
    context,
    upgrade: () => reviewInstalledProcessGroupUpgrade(f.configs, "local", f.stateRoot),
  };
}

describe("configured same-journal paired version review", () => {
  it.each([
    ["quality-adapter", false],
    ["document-router", true],
  ] as const)("retains %s's pending journal through policy switch/rollback", (name, nested) => {
    const f = fixture(name, nested);
    try {
      const file = path.join(f.journals.source.directory, "admission.sqlite"),
        before = fs.readFileSync(file),
        p = f.upgrade();
      if (p.review.protocolVersion !== 1 || !p.review.retainedJournal) throw Error("Wrong review");
      expect(p.review.namespace).toBe(nested ? "documents" : "quality");
      const ref = p.review.retainedJournal;
      expect(ref.contractVersion).toBe(2);
      expect(ref.generationId).toBe("cell-rev-a");
      expect(ref.journalId).toBe(f.journals.source.journalId);
      expect(inspectGenerationJournal(ref)).toMatchObject({ records: 1, admitted: 1, completed: 0 });
      expect(() => authorizeProcessGroupRuntime(f.context("target", "receiver"))).toThrow();
      replaceReviewedProcessGroupPolicy(p.review.policyFile, p.review.sourcePolicySha256, p.targetPolicy);
      replaceReviewedProcessGroupPolicy(p.review.policyFile, p.review.sourcePolicySha256, p.targetPolicy); // lost write reply
      for (const role of ["publisher", "receiver"] as const) {
        expect(authorizeProcessGroupRuntime(f.context("target", role)).configurationGroupSha256).toBe(ref.groupSha256);
        expect(() => authorizeProcessGroupRuntime(f.context("source", role))).toThrow();
      }
      replaceReviewedProcessGroupPolicy(p.review.policyFile, p.review.targetPolicySha256, p.sourcePolicy);
      replaceReviewedProcessGroupPolicy(p.review.policyFile, p.review.targetPolicySha256, p.sourcePolicy);
      expect(authorizeProcessGroupRuntime(f.context("source", "publisher")).journalId).toBe(ref.journalId);
      expect(fs.readFileSync(file)).toEqual(before);
      expect(fs.readFileSync(p.review.policyFile)).toEqual(p.sourcePolicy);
    } finally {
      f.close();
    }
  });
  it.each([
    "resource",
    "input",
    "profile",
    "generation",
    "group-digest",
    "directory",
    "uuid",
    "capacity",
    "other-config",
    "broker",
    "role",
    "package",
    "same-version",
    "selector",
    "policy",
    "journal",
    "lost-work",
  ])("refuses %s instead of reinterpreting retained work", (change) => {
    const f = fixture();
    try {
      const c = f.configs.target.publisher.raw as any,
        pkg = path.join(f.configs.target.publisher.packageRoot, "package.json"),
        originalPolicy = fs.readFileSync(path.join(f.registry, "cell-rev-a.json"));
      if (change === "resource") c.transform.document.profiles[0].resource.rows[0].unit = "MWh";
      if (change === "input") c.transform.document.inputTopic = "different/input";
      if (change === "profile") c.transform.document.profiles.reverse();
      if (change === "generation") c.durability.generationId = "other-revision";
      if (change === "group-digest") c.durability.groupSha256 = "f".repeat(64);
      if (change === "directory") c.durability.directory += "-other";
      if (change === "uuid") c.durability.journalId = f.journals.target.journalId;
      if (change === "capacity") c.durability.maxRecords++;
      if (change === "other-config") c.unit = "MWh";
      if (change === "broker") c.infra.host = "other.invalid";
      if (change === "role") c.role = "input";
      if (["package", "same-version", "selector"].includes(change)) {
        const p = JSON.parse(fs.readFileSync(pkg, "utf8"));
        if (change === "package") p.name = "other-adapter";
        if (change === "same-version") p.version = "1.0.0";
        if (change === "selector") p.unsDatahub.processGroupConfigurationRuntime.receiverRole = "different";
        fs.writeFileSync(pkg, JSON.stringify(p));
      }
      if (change === "policy") fs.appendFileSync(path.join(f.registry, "cell-rev-a.json"), "invalid");
      if (change === "journal") f.databases.source.exec("UPDATE metadata SET value='{}' WHERE key='contract'");
      if (change === "lost-work") f.databases.source.exec("UPDATE usage SET records=0,bytes=0");
      expect(f.upgrade).toThrow();
      expect(f.databases.source.prepare("SELECT count(*) n FROM tasks").get()?.["n"]).toBe(1);
      if (change !== "policy") expect(fs.readFileSync(path.join(f.registry, "cell-rev-a.json"))).toEqual(originalPolicy);
    } finally {
      f.close();
    }
  });
  it("rejects a file-to-configuration change through the version-only path", () => {
    const f = generationFixture();
    try {
      expect(() => reviewInstalledProcessGroupUpgrade(f.configs, "local", f.stateRoot)).toThrow();
    } finally {
      f.close();
    }
  });
});

describe("configuration-owned MQTT output enablement", () => {
  it.each([
    ["quality-adapter", false],
    ["document-router", true],
  ] as const)("%s retains its opaque resource and pending tasks", (name, nested) => {
    const f = fixture(name, nested);
    try {
      process.env["UNS_MQTT_OUTPUT_STATE_ROOT"] = "/var/lib/output";
      const p = path.join(f.configs.target.publisher.packageRoot, "package.json"),
        pkg = JSON.parse(fs.readFileSync(p, "utf8"));
      pkg.unsDatahub.processGroupMqttOutputRuntime = { protocolVersion: 1, instanceName: "observations" };
      fs.writeFileSync(p, JSON.stringify(pkg));
      const raw = f.configs.target.publisher.raw as any;
      raw.output = { ...(raw.output ?? {}), publishQueue: { mode: "durable", storage: "runtime", directory: "/var/lib/output/documents/combined" } };
      raw.uns.handover = false;
      raw.uns.instanceMode = "force";
      const prepared = reviewInstalledProcessGroupMqttOutput(f.configs, "local", f.stateRoot);
      if (prepared.review.protocolVersion !== 3) throw Error("Wrong protocol");
      expect(prepared.review.retainedJournal?.journalId).toBe(f.journals.source.journalId);
      expect(inspectGenerationJournal(prepared.review.retainedJournal!)).toMatchObject({ records: 1 });
      replaceReviewedProcessGroupPolicy(prepared.review.policyFile, prepared.review.sourcePolicySha256, prepared.targetPolicy);
      expect(authorizeProcessGroupRuntime(f.context("target", "publisher")).journalId).toBe(f.journals.source.journalId);
      replaceReviewedProcessGroupPolicy(prepared.review.policyFile, prepared.review.targetPolicySha256, prepared.sourcePolicy);
    } finally {
      f.close();
    }
  });
});
