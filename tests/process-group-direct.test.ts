import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  authorizeProcessGroupRuntime,
  configuredGroupProcessNames,
  parseProcessGroupRetirementPolicy,
} from "../packages/uns-core/src/runtime/process-group-authorization.js";
import { resolvedProcessGroupChannels } from "../packages/uns-core/src/runtime/process-group-contract.js";
import { processGroupDirectDescriptorSchema, processGroupDirectInputSchema } from "../packages/uns-core/src/runtime/process-group-direct-contract.js";
import {
  describeDirectProcessGroup,
  reviewInstalledProcessGroupDirectTransition,
} from "../packages/uns-core/src/runtime/process-group-direct-review.js";
import {
  assertProcessGroupDirectLaunch,
  inspectProcessGroupDirectReadiness,
  trackProcessGroupDirectReadiness,
} from "../packages/uns-core/src/runtime/process-group-direct-runtime.js";
import { sha256 } from "../packages/uns-core/src/runtime/process-group-resources.js";
import { replaceReviewedProcessGroupPolicy } from "../packages/uns-core/src/runtime/process-group-upgrade.js";
import { generationFixture } from "./helpers/process-group-generation-fixture.js";

const initialEnv = { ...process.env };
const cleanup: Array<() => void> = [];
afterEach(() => {
  for (const f of cleanup.splice(0).reverse()) f();
  process.env = { ...initialEnv };
});
function fixture(configured = false, nested = false) {
  const f = generationFixture(nested ? "document-router" : "quality-adapter", nested, configured);
  cleanup.push(() => f.close());
  process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = f.registry;
  process.env["UNS_PROCESS_GROUP_STATE_ROOT"] = f.stateRoot;
  delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
  const source = f.configs.source.publisher,
    target = f.configs.target.publisher;
  const pkg = JSON.parse(fs.readFileSync(path.join(source.packageRoot, "package.json"), "utf8"));
  const d = pkg.unsDatahub[configured ? "processGroupConfigurationRuntime" : "processGroupRuntime"];
  const descriptor = {
    protocolVersion: 1,
    namespace: d.namespace,
    publisherPath: d.publisherPath,
    journalPath: d.journalPath,
    runtimeRolePath: d.runtimeRolePath,
    directRole: "direct",
    resources: configured
      ? [{ kind: "configuration", groupPath: d.groupPath }]
      : [{ kind: "file", manifestPath: [...d.publisherPath, "groupManifest"], manifests: d.manifests }],
  };
  const targetPkg = JSON.parse(fs.readFileSync(path.join(target.packageRoot, "package.json"), "utf8"));
  targetPkg.unsDatahub = { ...pkg.unsDatahub, processGroupDirectRuntime: descriptor };
  fs.writeFileSync(path.join(target.packageRoot, "package.json"), JSON.stringify(targetPkg));
  target.raw = structuredClone(source.raw);
  target.resolved = target.raw;
  let parent = target.raw;
  for (const key of d.journalPath.slice(0, -1)) parent = parent[key] as Record<string, unknown>;
  delete parent[d.journalPath.at(-1)];
  // Remove empty parent as the application would when preparing a direct configuration.
  if (nested) delete target.raw["storage"];
  target.raw["role"] = "direct";
  fs.writeFileSync(path.join(target.instanceDirectory, "config.json"), JSON.stringify(target.raw));
  const placement = (side: "source" | "target", role: "publisher" | "receiver") => ({
    controller: "local",
    rttNode: pkg.name,
    version: side === "source" ? "1.0.0" : "1.1.0",
    instanceId: role,
  });
  const input = {
    protocolVersion: 1,
    source: { publisher: placement("source", "publisher"), receiver: placement("source", "receiver") },
    target: placement("target", "publisher"),
  };
  const operationId = randomUUID(),
    configs = { source: f.configs.source, target };
  const review = () => reviewInstalledProcessGroupDirectTransition(operationId, input, configs, f.stateRoot);
  const context = () => ({ ...target, controllerName: "local", operationId, launchId });
  const launchId = randomUUID();
  const reserve = () =>
    fs.writeFileSync(
      path.join(target.instanceDirectory, "process-group-reservation.json"),
      JSON.stringify({
        operationId,
        role: "target",
        configurationChecksum: JSON.parse(reviewed!.targetPolicy.toString()).target.configurationSha256,
        lastLaunch: { requestId: randomUUID(), launchId },
      }),
      { mode: 0o600 },
    );
  let reviewed: ReturnType<typeof review> | undefined;
  const prepare = () => (reviewed = review());
  const drain = () => {
    f.databases.source.exec(
      "DELETE FROM tasks;UPDATE usage SET records=0,bytes=0;UPDATE profiles SET completed=1,last_seq=1 WHERE name='" +
        f.profiles[0]!.processName +
        "'",
    );
  };
  const activate = () => {
    const r = prepare();
    drain();
    replaceReviewedProcessGroupPolicy(r.review.policyFile, r.review.sourcePolicySha256, r.targetPolicy);
    reserve();
    return r;
  };
  return { ...f, target, descriptor, input, configs, operationId, review, context, reserve, prepare, drain, activate };
}
describe("generic paired input retirement", () => {
  for (const configured of [false, true])
    for (const nested of [false, true])
      it(`reviews opaque ${configured ? "configured" : "file"} resources with ${nested ? "nested" : "flat"} selectors`, () => {
        const f = fixture(configured, nested),
          r = f.prepare();
        expect(r.review.usage.records).toBe(1);
        expect(r.review.source.publisher.processName).toBe(r.review.target.processName);
        expect(r.review.journal.directory).toBe(f.journals.source.directory);
        expect(fs.readFileSync(r.review.policyFile)).toEqual(r.sourcePolicy);
        expect(r.review.delivery).toEqual({ inputPausePossible: true, exactlyOnceOutput: false, brokerReplayNotTransferred: true });
        expect(parseProcessGroupRetirementPolicy(JSON.parse(r.targetPolicy.toString())).sourcePolicyJson).toBe(r.sourcePolicy.toString());
      });
  it("keeps the target fenced until the exact reviewed policy, launch and empty journal exist", () => {
    const f = fixture(),
      r = f.prepare();
    expect(() => assertProcessGroupDirectLaunch(f.context())).toThrow(/authority/);
    replaceReviewedProcessGroupPolicy(r.review.policyFile, r.review.sourcePolicySha256, r.targetPolicy);
    f.reserve();
    expect(() => assertProcessGroupDirectLaunch(f.context())).toThrow(/not empty/);
    f.drain();
    expect(() => assertProcessGroupDirectLaunch(f.context())).not.toThrow();
    expect(() => assertProcessGroupDirectLaunch({ ...f.context(), launchId: randomUUID() })).toThrow(/launch reservation/);
  });
  it("reconciles a lost policy-switch reply and restores the identical original policy", () => {
    const f = fixture(true, true),
      r = f.activate();
    replaceReviewedProcessGroupPolicy(r.review.policyFile, r.review.sourcePolicySha256, r.targetPolicy);
    expect(() => assertProcessGroupDirectLaunch(f.context())).not.toThrow();
    replaceReviewedProcessGroupPolicy(r.review.policyFile, r.review.targetPolicySha256, r.sourcePolicy);
    expect(fs.readFileSync(r.review.policyFile)).toEqual(r.sourcePolicy);
    expect(() => assertProcessGroupDirectLaunch(f.context())).toThrow();
    expect(
      authorizeProcessGroupRuntime({
        packageRoot: f.configs.source.publisher.packageRoot,
        instanceDirectory: f.configs.source.publisher.instanceDirectory,
        rawConfiguration: f.configs.source.publisher.raw,
        channels: resolvedProcessGroupChannels(f.configs.source.publisher.resolved),
        controllerName: "local",
        stateRoot: f.stateRoot,
      }).role,
    ).toBe("target");
  });
  it("blocks old journal owners, original profiles, changed targets and unreviewed output enablement", () => {
    const f = fixture(),
      r = f.activate();
    expect(configuredGroupProcessNames().has(f.profiles[0]!.processName)).toBe(true);
    expect(() =>
      authorizeProcessGroupRuntime({
        packageRoot: f.configs.source.publisher.packageRoot,
        instanceDirectory: f.configs.source.publisher.instanceDirectory,
        rawConfiguration: f.configs.source.publisher.raw,
        channels: resolvedProcessGroupChannels(f.configs.source.publisher.resolved),
        controllerName: "local",
        stateRoot: f.stateRoot,
      }),
    ).toThrow();
    const original = structuredClone(f.target.raw);
    f.target.raw["output"] = { ...(f.target.raw["infra"] as object), publishQueue: { enabled: true } };
    expect(() => assertProcessGroupDirectLaunch(f.context())).toThrow();
    f.target.raw = original;
    f.target.resolved = original;
    expect(() => assertProcessGroupDirectLaunch({ ...f.context(), instanceDirectory: f.configs.target.receiver.instanceDirectory })).toThrow();
    const p = JSON.parse(r.targetPolicy.toString());
    p.sourcePolicyJson += " ";
    expect(() => parseProcessGroupRetirementPolicy(p)).toThrow(/bytes changed/);
  });
  for (const mutation of ["channels", "business", "profile", "placement", "journal", "output"] as const)
    it(`rejects changed ${mutation} before stopping anything`, () => {
      const f = fixture();
      if (mutation === "channels") f.target.resolved = { ...f.target.resolved, input: { host: "other.example" } };
      if (mutation === "business") f.target.raw["unrelatedBusinessOption"] = true;
      if (mutation === "profile") fs.appendFileSync(path.join(f.target.packageRoot, "contracts/a.csv"), "different\n");
      if (mutation === "placement") f.input.target.instanceId = "receiver";
      if (mutation === "journal") f.target.raw["durability"] = f.journals.source;
      if (mutation === "output") f.target.resolved = { ...f.target.resolved, output: { host: "127.0.0.1", publishQueue: { enabled: true } } };
      expect(() => f.review()).toThrow();
      expect(f.databases.source.prepare("SELECT records FROM usage").get()?.records).toBe(1);
    });
  it("rejects policy replacement, private-mode weakening and linked resource files", () => {
    const f = fixture();
    const policy = path.join(f.registry, "cell.json"),
      original = fs.readFileSync(policy);
    fs.chmodSync(policy, 0o644);
    expect(() => f.review()).toThrow();
    fs.chmodSync(policy, 0o600);
    const r = f.prepare();
    fs.writeFileSync(policy, Buffer.concat([original, Buffer.from(" ")]));
    expect(() => replaceReviewedProcessGroupPolicy(policy, r.review.sourcePolicySha256, r.targetPolicy)).toThrow();
    fs.writeFileSync(policy, original);
    const mapping = path.join(f.target.packageRoot, "contracts/a.csv");
    fs.renameSync(mapping, mapping + ".original");
    fs.symlinkSync(mapping + ".original", mapping);
    expect(() => f.review()).toThrow();
  });
  it("rejects overlapping selectors and distinct-placement violations", () => {
    const f = fixture();
    expect(processGroupDirectDescriptorSchema.safeParse({ ...f.descriptor, journalPath: f.descriptor.publisherPath }).success).toBe(false);
    expect(processGroupDirectInputSchema.safeParse({ ...f.input, target: f.input.source.publisher }).success).toBe(false);
  });
  it("expires readiness for the exact launch and clears it on standalone cleanup", () => {
    const f = fixture();
    f.activate();
    const context = f.context(),
      stop = trackProcessGroupDirectReadiness(context, () => true);
    cleanup.push(stop);
    const expected = {
      pid: process.pid,
      launchId: context.launchId,
      configurationSha256: JSON.parse(fs.readFileSync(path.join(f.target.instanceDirectory, "process-group-reservation.json"), "utf8"))
        .configurationChecksum,
    };
    expect(inspectProcessGroupDirectReadiness(f.target.instanceDirectory, expected)).toBe(true);
    expect(inspectProcessGroupDirectReadiness(f.target.instanceDirectory, { ...expected, pid: process.pid + 1 })).toBe(false);
    expect(inspectProcessGroupDirectReadiness(f.target.instanceDirectory, expected, Date.now() + 6000)).toBe(false);
    stop();
    expect(inspectProcessGroupDirectReadiness(f.target.instanceDirectory, expected)).toBe(false);
  });
  it("keeps original identities fenced after explicit closure while allowing the combined direct identity", () => {
    const f = fixture(true, true),
      r = f.activate(),
      closed = Buffer.from(JSON.stringify({ ...JSON.parse(r.targetPolicy.toString()), recovery: "closed" }));
    replaceReviewedProcessGroupPolicy(r.review.policyFile, r.review.targetPolicySha256, closed);
    expect(() => assertProcessGroupDirectLaunch({ ...f.context(), operationId: undefined, launchId: undefined })).not.toThrow();
    for (const processName of [f.profiles[0]!.processName, JSON.parse(closed.toString()).source.receiver.processName]) {
      const context = f.context();
      context.raw = structuredClone(context.raw);
      (context.raw["uns"] as Record<string, unknown>)["processName"] = processName;
      expect(() => assertProcessGroupDirectLaunch(context)).toThrow();
    }
  });
  it("returns the package-bound manifest instead of an instance shadow and rejects resolved resource drift", () => {
    const f = fixture();
    const shadow = path.join(f.target.instanceDirectory, "contracts", "cell.json");
    fs.mkdirSync(path.dirname(shadow), { recursive: true });
    fs.writeFileSync(shadow, '{"wrong":true}');
    expect(describeDirectProcessGroup(f.target).manifestFile).toBe(path.join(f.target.packageRoot, "contracts", "cell.json"));
    f.target.resolved = structuredClone(f.target.raw);
    (f.target.resolved["transform"] as Record<string, unknown>)["groupManifest"] = "contracts/other.json";
    expect(() => f.review()).toThrow(/Resolved/);
  });
});
