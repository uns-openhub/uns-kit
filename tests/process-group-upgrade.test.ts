import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";

import { authorizeProcessGroupRuntime } from "../packages/uns-core/src/runtime/process-group-authorization.js";
import { processGroupJournalContract, processGroupSourceIdentity } from "../packages/uns-core/src/runtime/process-group-contract.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  sha256,
} from "../packages/uns-core/src/runtime/process-group-resources.js";
import { processGroupUpgradeRequestSchema } from "../packages/uns-core/src/runtime/process-group-upgrade.js";
import {
  type UpgradeConfigurations,
  processGroupUpgradeInputSchema,
  replaceReviewedProcessGroupPolicy,
  reviewInstalledProcessGroupMqttOutput,
  reviewInstalledProcessGroupUpgrade,
} from "../packages/uns-core/src/runtime/process-group-upgrade.js";

const outputInspection = vi.hoisted(() => ({ records: 0, bytes: 0, rootId: "11111111-1111-4111-8111-111111111111" }));
vi.mock("../packages/uns-core/src/runtime/mqtt-output-state-program.mjs", async (original) => ({
  ...(await original<object>()),
  inspectManagedOutputQueue: (_root: string, _dir: string, identity: string) => ({
    ...outputInspection,
    relative: "observations/combined",
    identity,
    exists: false,
  }),
}));

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true });
});
function fixture(nested = false) {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(tmpdir()), "sdk-pair-upgrade-")),
    stateRoot = path.join(root, "state"),
    directory = path.join(stateRoot, "telemetry", "admission", "line-x"),
    policyFile = path.join(stateRoot, "telemetry", "authorizations", "line-x.json");
  roots.push(root);
  for (const dir of [stateRoot, directory, path.dirname(policyFile)]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const descriptor = {
    protocolVersion: 1,
    namespace: "telemetry",
    publisherPath: nested ? ["behavior", "rules"] : ["transform"],
    journalPath: nested ? ["storage", "input"] : ["durable"],
    runtimeRolePath: ["role"],
    receiverRole: "receiver",
    manifests: [{ id: "line-x", file: "contracts/line-x.json" }],
  };
  const profiles = ["east", "west"].map((id) => ({
    id,
    processName: id,
    mappingFile: id + ".csv",
    mappingSha256: sha256("signal,path,unit\n" + id + ",factory/line-x/" + id + ",kWh\n"),
    dataGroup: "observations",
  }));
  const manifest = { schemaVersion: 1, id: "line-x", inputTopic: "fixture/input", processName: "line-x-group", profiles },
    journalId = randomUUID(),
    broker = { host: "fixture.invalid", port: 1883 },
    journal = { directory, journalId, groupManifest: "contracts/line-x.json", maxRecords: 20000, maxBytes: 33554432 };
  const configs = { source: {}, target: {} } as UpgradeConfigurations;
  for (const side of ["source", "target"] as const) {
    const packageRoot = path.join(root, side);
    fs.mkdirSync(path.join(packageRoot, "dist"), { recursive: true });
    fs.mkdirSync(path.join(packageRoot, "contracts"));
    fs.writeFileSync(path.join(packageRoot, "dist", "index.js"), "export const protocol=1;\n");
    fs.writeFileSync(
      path.join(packageRoot, "package.json"),
      JSON.stringify({
        name: nested ? "unrelated-pump-adapter" : "unrelated-power-adapter",
        version: side === "source" ? "1.0.0" : "1.1.0",
        unsDatahub: { processGroupRuntime: descriptor },
      }),
    );
    fs.writeFileSync(path.join(packageRoot, "contracts", "line-x.json"), JSON.stringify(manifest));
    for (const p of profiles)
      fs.writeFileSync(path.join(packageRoot, "contracts", p.mappingFile), "signal,path,unit\n" + p.id + ",factory/line-x/" + p.id + ",kWh\n");
    for (const role of ["publisher", "receiver"] as const) {
      const instanceDirectory = path.join(packageRoot, "instances", role);
      fs.mkdirSync(instanceDirectory, { recursive: true });
      const raw = {
        uns: { processName: "line-x-group" + (role === "receiver" ? "-admission" : "") },
        infra: broker,
        ...(nested
          ? { behavior: { rules: { groupManifest: "contracts/line-x.json" } }, storage: { input: journal } }
          : { transform: { groupManifest: "contracts/line-x.json" }, durable: journal }),
        ...(role === "receiver" ? { role: "receiver" } : {}),
      };
      configs[side][role] = { packageRoot, instanceDirectory, raw, resolved: raw };
    }
  }
  const source = configs.source.publisher.packageRoot,
    oldPolicy = {
      schemaVersion: 3,
      kind: "uns-process-group-runtime-authorization",
      protocolVersion: 1,
      packageName: nested ? "unrelated-pump-adapter" : "unrelated-power-adapter",
      namespace: "telemetry",
      groupId: "line-x",
      descriptorSha256: hash(descriptor),
      controllerName: "local",
      packageRoot: source,
      artifactSha256: groupArtifactHash(source),
      groupManifestSha256: sha256(JSON.stringify(manifest)),
      channelsSha256: groupChannelsHash([broker, broker, broker]),
      stateRoot,
      directory,
      journalId,
      placements: [
        ...profiles.map((p) => ({ instanceId: p.id, processName: p.processName, role: "source", configurationSha256: "a".repeat(64) })),
        ...(["publisher", "receiver"] as const).map((role) => ({
          instanceId: role,
          processName: configs.source[role].raw["uns"] && (configs.source[role].raw["uns"] as any).processName,
          role: role === "publisher" ? "target" : "receiver",
          configurationSha256: hash(configs.source[role].raw),
        })),
      ],
    };
  fs.writeFileSync(policyFile, JSON.stringify(oldPolicy), { mode: 0o600 });
  const db = new DatabaseSync(path.join(directory, "admission.sqlite"));
  db.exec("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)");
  for (const [k, v] of Object.entries({
    id: journalId,
    contract: processGroupJournalContract(processGroupSourceIdentity(broker), journal, profiles),
  }))
    db.prepare("INSERT INTO metadata VALUES(?,?)").run(k, v);
  db.close();
  const context = (side: "source" | "target") => ({
    packageRoot: configs[side].publisher.packageRoot,
    instanceDirectory: configs[side].publisher.instanceDirectory,
    rawConfiguration: configs[side].publisher.raw,
    channels: [broker, broker, broker],
    controllerName: "local",
    stateRoot,
    file: policyFile,
  });
  // Explicit file selection remains canonical and is not a runtime bypass.
  const review = () => {
    const prior = process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE;
    process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE = policyFile;
    try {
      return reviewInstalledProcessGroupUpgrade(configs, "local", stateRoot);
    } finally {
      if (prior === undefined) delete process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE;
      else process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE = prior;
    }
  };
  return { configs, policyFile, review, context, directory };
}
describe("shared installed process-group version review", () => {
  it.each([false, true])("supports an unrelated package and nested selectors=%s, with exact policy rollback", (nested) => {
    const f = fixture(nested),
      before = fs.readFileSync(f.policyFile),
      prepared = f.review();
    expect(fs.readFileSync(f.policyFile)).toEqual(before);
    expect(prepared.review.profiles).toHaveLength(2);
    expect(() => authorizeProcessGroupRuntime(f.context("target"))).toThrow();
    replaceReviewedProcessGroupPolicy(f.policyFile, prepared.review.sourcePolicySha256, prepared.targetPolicy);
    replaceReviewedProcessGroupPolicy(f.policyFile, prepared.review.sourcePolicySha256, prepared.targetPolicy); // lost response
    expect(authorizeProcessGroupRuntime(f.context("target")).role).toBe("target");
    expect(() => authorizeProcessGroupRuntime(f.context("source"))).toThrow();
    replaceReviewedProcessGroupPolicy(f.policyFile, prepared.review.targetPolicySha256, prepared.sourcePolicy);
    expect(fs.readFileSync(f.policyFile)).toEqual(before);
    expect(authorizeProcessGroupRuntime(f.context("source")).role).toBe("target");
  });
  it("rejects changed application configuration and preserves original policy", () => {
    const f = fixture(),
      before = fs.readFileSync(f.policyFile);
    f.configs.target.publisher.raw["unit"] = "MWh";
    expect(f.review).toThrow();
    expect(fs.readFileSync(f.policyFile)).toEqual(before);
  });
  it("rejects changed retained journal contract without rewriting it", () => {
    const f = fixture();
    const db = new DatabaseSync(path.join(f.directory, "admission.sqlite"));
    db.prepare("UPDATE metadata SET value='changed' WHERE key='contract'").run();
    db.close();
    expect(f.review).toThrow(/journal contract/);
  });
  it("rejects a concurrent policy replacement instead of overwriting it", () => {
    const f = fixture(),
      p = f.review();
    fs.writeFileSync(f.policyFile, "{}", { mode: 0o600 });
    expect(() => replaceReviewedProcessGroupPolicy(f.policyFile, p.review.sourcePolicySha256, p.targetPolicy)).toThrow(/changed/);
    expect(fs.readFileSync(f.policyFile, "utf8")).toBe("{}");
  });
  it("keeps paired upgrade same-host and distinct from consolidation", () => {
    const p = (v: string, id: string) => ({ controller: "local", rttNode: "generic", version: v, instanceId: id });
    expect(
      processGroupUpgradeInputSchema.safeParse({
        protocolVersion: 1,
        source: { publisher: p("1", "a"), receiver: p("1", "b") },
        target: { publisher: p("2", "c"), receiver: { ...p("2", "d"), controller: "remote" } },
      }).success,
    ).toBe(false);
  });
});

it("keeps the documented request example valid for the shared upgrade schema", () => {
  const guide = fs.readFileSync(new URL("../docs/process-group-version-upgrade.md", import.meta.url), "utf8");
  const example = guide.match(/```json\n([\s\S]*?)\n```/);
  expect(example).not.toBeNull();
  expect(processGroupUpgradeRequestSchema.parse(JSON.parse(example![1]!)).action).toBe("preview");
});

describe("explicit generic MQTT output enablement", () => {
  function outputFixture(nested = false) {
    const f = fixture(nested);
    const pkg = path.join(f.configs.target.publisher.packageRoot, "package.json");
    const data = JSON.parse(fs.readFileSync(pkg, "utf8"));
    data.unsDatahub.processGroupMqttOutputRuntime = { protocolVersion: 1, instanceName: "observations" };
    fs.writeFileSync(pkg, JSON.stringify(data));
    Object.assign(f.configs.target.publisher.raw["uns"] as object, { handover: false, instanceMode: "force" });
    f.configs.target.publisher.raw["output"] = {
      publishQueue: { mode: "durable", storage: "runtime", directory: "/var/lib/output/observations/combined" },
    };
    const review = () => {
      const prior = process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE;
      process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE = f.policyFile;
      try {
        return reviewInstalledProcessGroupMqttOutput(
          f.configs,
          "local",
          path.dirname(path.dirname(path.dirname(f.directory))),
          [],
          "/var/lib/output",
        );
      } finally {
        if (prior === undefined) delete process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE;
        else process.env.UNS_PROCESS_GROUP_AUTHORIZATION_FILE = prior;
      }
    };
    outputInspection.records = 0;
    outputInspection.bytes = 0;
    return { ...f, review };
  }
  it.each([false, true])("keeps profiles, destination and journal for unrelated selectors=%s", (nested) => {
    const f = outputFixture(nested),
      before = fs.readFileSync(f.policyFile),
      prepared = f.review();
    expect(prepared.review.protocolVersion).toBe(3);
    expect(prepared.review.directory).toBe(f.directory);
    expect(prepared.review.profiles.map((p) => p.processName)).toEqual(["east", "west"]);
    expect(fs.readFileSync(f.policyFile)).toEqual(before);
    replaceReviewedProcessGroupPolicy(f.policyFile, prepared.review.sourcePolicySha256, prepared.targetPolicy);
    expect(authorizeProcessGroupRuntime(f.context("target")).role).toBe("target");
    replaceReviewedProcessGroupPolicy(f.policyFile, prepared.review.targetPolicySha256, prepared.sourcePolicy);
    expect(fs.readFileSync(f.policyFile)).toEqual(before);
  });
  it.each(["unit", "broker", "receiver", "input", "queueMode", "handover", "declaration", "pending"])(
    "rejects %s with old authority preserved",
    (kind) => {
      const f = outputFixture(),
        before = fs.readFileSync(f.policyFile),
        raw = f.configs.target.publisher.raw as any;
      if (kind === "unit") raw.unit = "MWh";
      if (kind === "broker") raw.output.host = "elsewhere.invalid";
      if (kind === "receiver") f.configs.target.receiver.raw["output"] = raw.output;
      if (kind === "input") raw.durable.journalId = randomUUID();
      if (kind === "queueMode") raw.output.publishQueue.mode = "buffered";
      if (kind === "handover") raw.uns.handover = true;
      if (kind === "pending") outputInspection.records = 1;
      if (kind === "declaration") {
        const p = path.join(f.configs.target.publisher.packageRoot, "package.json"),
          pkg = JSON.parse(fs.readFileSync(p, "utf8"));
        delete pkg.unsDatahub.processGroupMqttOutputRuntime;
        fs.writeFileSync(p, JSON.stringify(pkg));
      }
      expect(f.review).toThrow();
      expect(fs.readFileSync(f.policyFile)).toEqual(before);
    },
  );
  it("requires explicit acknowledgement and refuses source/target aliasing", () => {
    const p = (id: string) => ({ controller: "local", rttNode: "unrelated", version: "1.0.0", instanceId: id });
    const input = {
      protocolVersion: 3,
      source: { publisher: p("a"), receiver: p("b") },
      target: { publisher: p("c"), receiver: p("d") },
      acknowledgeInputPause: true,
      acknowledgePossibleReplay: true,
      acknowledgePendingOutputBlocksRollback: true,
    };
    expect(processGroupUpgradeInputSchema.parse(input).protocolVersion).toBe(3);
    expect(() => processGroupUpgradeInputSchema.parse({ ...input, acknowledgePendingOutputBlocksRollback: false })).toThrow();
    expect(() => processGroupUpgradeInputSchema.parse({ ...input, target: input.source })).toThrow();
  });
});
