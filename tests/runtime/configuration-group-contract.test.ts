import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

import { configurationGroupAuthorizationSchema } from "../../packages/uns-core/src/runtime/configuration-group-authorization.js";
import {
  configurationGroupDescriptorSchema,
  configurationGroupJournalContract,
  configurationGroupSnapshot,
  configurationResourceHash,
  configurationResourceJson,
  selectedConfigurationGroup,
} from "../../packages/uns-core/src/runtime/configuration-group-contract.js";
import * as api from "../../packages/uns-core/src/runtime/process-group-authorization.js";

function fixture(name = "quality-adapter", namespace = "quality", selector = "inspection") {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "uns-configured-group-")));
  fs.chmodSync(root, 0o700);
  const pkg = path.join(root, "package"),
    state = path.join(root, "state"),
    registry = path.join(state, namespace, "authorizations");
  fs.mkdirSync(path.join(pkg, "dist"), { recursive: true });
  fs.mkdirSync(registry, { recursive: true, mode: 0o700 });
  fs.chmodSync(state, 0o700);
  const descriptor = {
    protocolVersion: 2,
    namespace,
    publisherPath: [selector],
    groupPath: [selector, "document"],
    memberPath: [selector, "member"],
    journalPath: ["durability"],
    runtimeRolePath: ["executionRole"],
    receiverRole: "ingress",
  };
  const metadata = { name, version: "1.0.0", unsDatahub: { processGroupConfigurationRuntime: descriptor } };
  const write = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
  write(path.join(pkg, "package.json"), metadata);
  fs.writeFileSync(path.join(pkg, "dist/index.js"), "export const runtime = true;");
  const group = {
    schemaVersion: 2,
    id: "dynamic-cell",
    inputTopic: "canary/quality/input",
    processName: name + "-combined",
    receiverProcessName: name + "-receiver",
    profiles: ["a", "b"].map((id) => ({
      id,
      processName: name + "-" + id,
      dataGroup: "quality",
      resource: { schema: 7, acceptedQuality: id, limits: [1, 2, 3] },
    })),
  };
  const snapshot = configurationGroupSnapshot(group),
    generationId = "generation-a",
    journalId = randomUUID();
  const directory = path.join(state, namespace, "admission", generationId);
  const configurations = [
    ...group.profiles.map((p) => ({ id: p.id, processName: p.processName, role: "source", member: p.id })),
    { id: "target", processName: group.processName, role: "target", member: undefined },
    { id: "receiver", processName: group.receiverProcessName, role: "receiver", member: undefined },
  ].map((p) => {
    const dir = path.join(pkg, "instances", p.id);
    fs.mkdirSync(dir, { recursive: true });
    const raw = {
      uns: { processName: p.processName },
      [selector]: { document: structuredClone(group), ...(p.member ? { member: p.member } : {}) },
      durability: { directory, journalId, generationId, groupSha256: snapshot.sha256, maxRecords: 10, maxBytes: 4096 },
      ...(p.role === "receiver" ? { executionRole: "ingress" } : {}),
    };
    write(path.join(dir, "config.json"), raw);
    return { ...p, raw, dir };
  });
  const channels = Array.from({ length: 3 }, () => ({ host: "127.0.0.1", port: 1883 }));
  const policy = configurationGroupAuthorizationSchema.parse({
    schemaVersion: 4,
    kind: "uns-process-group-runtime-authorization",
    protocolVersion: 2,
    packageName: name,
    namespace,
    groupId: group.id,
    generationId,
    descriptorSha256: configurationResourceHash(descriptor),
    controllerName: "local",
    packageRoot: pkg,
    artifactSha256: api.groupArtifactHash(pkg),
    groupSha256: snapshot.sha256,
    channelsSha256: api.groupChannelsHash(channels),
    stateRoot: state,
    directory,
    journalId,
    placements: configurations.map((c) => ({
      instanceId: c.id,
      processName: c.processName,
      role: c.role,
      configurationSha256: configurationResourceHash(c.raw),
    })),
  });
  const file = path.join(registry, generationId + ".json"),
    save = () => write(file, policy);
  save();
  return {
    root,
    pkg,
    state,
    registry,
    descriptor,
    metadata,
    group,
    snapshot,
    configurations,
    policy,
    file,
    write,
    save,
    context: (i: number) => ({
      packageRoot: pkg,
      instanceDirectory: configurations[i]!.dir,
      rawConfiguration: configurations[i]!.raw,
      channels,
      controllerName: "local",
      file,
      stateRoot: state,
    }),
    close: () => fs.rmSync(root, { recursive: true, force: true }),
  };
}

describe("generic configuration resources", () => {
  it("uses binary canonical JSON without locale or executable values", () => {
    expect(configurationResourceJson({ z: 1, a: { q: 2, b: false } })).toBe('{"a":{"b":false,"q":2},"z":1}');
    expect(configurationResourceHash({ b: 1, a: 2 })).toBe(configurationResourceHash({ a: 2, b: 1 }));
    expect(configurationResourceJson({ "2": "second", "10": "tenth" })).toBe('{"10":"tenth","2":"second"}');
    expect(configurationResourceHash([1, 2])).not.toBe(configurationResourceHash([2, 1]));
    let invoked = false;
    const object = Object.defineProperty({}, "value", {
      enumerable: true,
      get: () => {
        invoked = true;
        return 1;
      },
    });
    expect(() => configurationResourceJson(object)).toThrow(/accessor/);
    expect(invoked).toBe(false);
    const cyclic: Record<string, unknown> = {};
    cyclic["self"] = cyclic;
    for (const value of [cyclic, undefined, NaN, Infinity, new Date(), () => 1, [undefined], new Array(2)])
      expect(() => configurationResourceJson(value)).toThrow();
  });
  it("bounds structure/bytes and never accepts a mutable or forged journal snapshot", () => {
    expect(() => configurationResourceHash({ data: "x".repeat(1024 * 1024 + 1) })).toThrow(/1 MiB/);
    expect(() => configurationResourceHash(Array.from({ length: 20 }, () => "x".repeat(65536)))).toThrow(/1 MiB/);
    expect(() => configurationResourceHash({ data: "\u0001".repeat(200000) })).toThrow(/1 MiB/);
    let nested: unknown = null;
    for (let i = 0; i < 34; i++) nested = { child: nested };
    expect(() => configurationResourceHash(nested)).toThrow(/budget/);
    const f = fixture();
    try {
      const old = f.snapshot.profiles[0]!.resource;
      f.group.profiles[0]!.resource.limits[0] = 999;
      expect((old as { limits: number[] }).limits[0]).toBe(1);
      expect(Object.isFrozen(old)).toBe(true);
      const journal = { ...f.configurations[0]!.raw.durability, minFreeBytes: 1048576 };
      const profiles = f.snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, resourceSha256, dataGroup }));
      const contract = configurationGroupJournalContract(f.snapshot, "a".repeat(64), journal, profiles);
      expect(JSON.parse(contract).version).toBe(2);
      expect(() => configurationGroupJournalContract({ ...f.snapshot }, "a".repeat(64), journal, profiles)).toThrow(/SDK-issued/);
      expect(() => configurationGroupJournalContract(f.snapshot, "a".repeat(64), { ...journal, groupSha256: "b".repeat(64) }, profiles)).toThrow(
        /revision/,
      );
      expect(() => configurationGroupJournalContract(f.snapshot, "a".repeat(64), journal, profiles.slice(1))).toThrow(/profiles/);
    } finally {
      f.close();
    }
  });
  it("rejects overlapping identities, unsafe selectors and unknown versions", () => {
    const f = fixture();
    try {
      expect(() => configurationGroupDescriptorSchema.parse({ ...f.descriptor, protocolVersion: 99 })).toThrow();
      expect(() => configurationGroupDescriptorSchema.parse({ ...f.descriptor, memberPath: ["__proto__", "member"] })).toThrow();
      expect(() => configurationGroupSnapshot({ ...f.group, receiverProcessName: f.group.processName })).toThrow(/identities/);
      expect(() => configurationGroupSnapshot({ ...f.group, profiles: [f.group.profiles[0], f.group.profiles[0]] })).toThrow(/identities/);
    } finally {
      f.close();
    }
  });
});

describe("configuration-backed group authority", () => {
  it.each([
    ["quality-adapter", "quality", "inspection"],
    ["document-router", "documents", "routingRules"],
  ])("authorizes %s with unrelated selectors and opaque resources", (name, namespace, selector) => {
    const f = fixture(name, namespace, selector);
    try {
      for (let i = 0; i < 4; i++) {
        const proof = api.authorizeProcessGroupRuntime(f.context(i));
        expect(proof.groupId).toBe("dynamic-cell");
        api.assertProcessGroupJournalAuthorization(proof, proof.directory, proof.journalId ?? undefined);
      }
      expect(selectedConfigurationGroup(f.pkg, f.configurations[0]!.raw)!.snapshot.sha256).toBe(f.snapshot.sha256);
    } finally {
      f.close();
    }
  });
  it.each([
    "resource",
    "journal-revision",
    "generation",
    "member",
    "role",
    "configuration",
    "artifact",
    "descriptor",
    "controller",
    "policy-path",
    "channels",
    "permissions",
    "protocol",
  ])("refuses changed %s", (kind) => {
    const f = fixture();
    try {
      const context = f.context(0);
      if (kind === "resource")
        (context.rawConfiguration["inspection"] as { document: typeof f.group }).document.profiles[0]!.resource.limits[0] = 999;
      if (kind === "journal-revision") context.rawConfiguration.durability.groupSha256 = "a".repeat(64);
      if (kind === "generation") context.rawConfiguration.durability.generationId = "other-generation";
      if (kind === "member") (context.rawConfiguration["inspection"] as { member: string }).member = "b";
      if (kind === "role") f.policy.placements[0]!.role = "target";
      if (kind === "configuration") (context.rawConfiguration as Record<string, unknown>)["extra"] = true;
      if (kind === "artifact") fs.appendFileSync(path.join(f.pkg, "dist/index.js"), "changed");
      if (kind === "descriptor") f.policy.descriptorSha256 = "a".repeat(64);
      if (kind === "controller") context.controllerName = "foreign";
      if (kind === "policy-path") context.file = path.join(f.registry, "foreign.json");
      if (kind === "channels") context.channels[0]!.host = "foreign-broker";
      if (kind === "protocol") (f.policy as Record<string, unknown>)["protocolVersion"] = 99;
      f.save();
      if (kind === "permissions") fs.chmodSync(f.file, 0o644);
      expect(() => api.authorizeProcessGroupRuntime(context)).toThrow(/authorization/);
    } finally {
      f.close();
    }
  });
  it("candidate validation never grants authority; target launch pins group and generation", () => {
    const f = fixture();
    try {
      const context = f.context(2),
        operationId = randomUUID(),
        launchId = randomUUID();
      api.validateProcessGroupPolicyCandidate(context, fs.readFileSync(f.file));
      const proof = api.authorizeProcessGroupRuntime(context);
      expect(() => api.assertProcessGroupJournalAuthorization({ ...proof }, proof.directory, proof.journalId ?? undefined)).toThrow();
      const reservation = { operationId, role: "target", configurationChecksum: "a".repeat(64), lastLaunch: { launchId, requestId: randomUUID() } };
      f.write(path.join(context.instanceDirectory, "process-group-reservation.json"), reservation);
      expect(() => api.assertProcessGroupTargetLaunch(proof, context.instanceDirectory, operationId, launchId)).toThrow();
      f.write(path.join(context.instanceDirectory, "process-group-reservation.json"), {
        ...reservation,
        configurationGroupSha256: f.snapshot.sha256,
        generationId: f.policy.generationId,
      });
      api.assertProcessGroupTargetLaunch(proof, context.instanceDirectory, operationId, launchId);
      expect(() => api.assertProcessGroupTargetLaunch(proof, context.instanceDirectory, operationId, randomUUID())).toThrow();
      fs.appendFileSync(f.file, "\n");
      expect(() => api.assertProcessGroupJournalAuthorization(proof, proof.directory, proof.journalId ?? undefined)).toThrow();
    } finally {
      f.close();
    }
  });
  it("only the unbound reviewed receiver can initialize a new generation", () => {
    const f = fixture();
    try {
      f.policy.journalId = null;
      for (let i = 0; i < f.configurations.length; i++) {
        delete (f.configurations[i]!.raw.durability as { journalId?: string }).journalId;
        f.policy.placements[i]!.configurationSha256 = configurationResourceHash(f.configurations[i]!.raw);
      }
      f.save();
      expect(api.authorizeProcessGroupRuntime({ ...f.context(3), initialize: true }).journalId).toBeNull();
      for (let i = 0; i < 3; i++) expect(() => api.authorizeProcessGroupRuntime({ ...f.context(i), initialize: true })).toThrow();
      expect(() => api.authorizeProcessGroupRuntime(f.context(3))).toThrow();
    } finally {
      f.close();
    }
  });
  it("retained generations stay fenced when their policy is removed", () => {
    const f = fixture(),
      env = { ...process.env };
    try {
      process.env["UNS_PROCESS_GROUP_STATE_ROOT"] = f.state;
      process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = f.registry;
      delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
      fs.mkdirSync(path.dirname(f.policy.directory), { recursive: true });
      fs.mkdirSync(f.policy.directory);
      expect(api.configuredGroupProcessNames().size).toBe(4);
      fs.unlinkSync(f.file);
      expect(() => api.configuredGroupProcessNames()).toThrow(/review required/);
    } finally {
      process.env = env;
      f.close();
    }
  });
});
