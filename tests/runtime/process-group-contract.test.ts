import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

import * as api from "../../packages/uns-core/src/runtime/process-group-authorization.js";
import {
  isLocalPilotPath,
  normalizeProcessGroupConfiguration,
  processGroupDescriptor,
  processGroupRuntimeDescriptorSchema,
} from "../../packages/uns-core/src/runtime/process-group-contract.js";
import { inspectProcessGroupConfiguration } from "../../packages/uns-core/src/runtime/process-group-inspection.js";
import { refreshProcessGroupStagedJournal, stagedJournalFile } from "../../packages/uns-core/src/runtime/process-group-staging.js";
import { processGroupReceiverSupervision } from "../../packages/uns-core/src/runtime/process-group-supervision.js";

function fixture(group = "cell-42", packageName = "example-transformer") {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "uns-shared-contract-")));
  fs.chmodSync(root, 0o700);
  const pkg = path.join(root, "package"),
    state = path.join(root, "state"),
    registry = path.join(state, "runtime/authorizations");
  fs.mkdirSync(path.join(pkg, "dist"), { recursive: true });
  fs.mkdirSync(path.join(pkg, "contracts"));
  fs.mkdirSync(registry, { recursive: true, mode: 0o700 });
  fs.chmodSync(state, 0o700);
  const descriptor = {
    protocolVersion: 1,
    namespace: "runtime",
    publisherPath: ["transform"],
    journalPath: ["durableInput"],
    runtimeRolePath: ["runtimeRole"],
    receiverRole: "receiver",
    defaultDataGroup: "telemetry",
    manifests: [{ id: group, file: `contracts/${group}.json` }],
  };
  const metadata = {
    name: packageName,
    version: "1.0.0",
    unsDatahub: { processGroupRuntime: descriptor, processGroupAdmission: 1, processGroupPersistentAdmission: 1, processGroupAdmissionSupervisor: 1 },
  };
  const write = (file: string, data: unknown) => fs.writeFileSync(file, JSON.stringify(data), { mode: 0o600 });
  write(path.join(pkg, "package.json"), metadata);
  fs.writeFileSync(path.join(pkg, "dist/index.js"), 'export const application = "fixture";');
  const hash = (b: string | Buffer) => createHash("sha256").update(b).digest("hex");
  const resources = ["a", "b"].map((id) => {
    const name = `${packageName}-${id}`,
      file = `contracts/${id}.json`,
      bytes = JSON.stringify({ operation: id });
    fs.writeFileSync(path.join(pkg, file), bytes);
    return { id, processName: name, mappingFile: `${id}.json`, mappingSha256: hash(bytes), dataGroup: "telemetry" };
  });
  const manifest = { schemaVersion: 1, id: group, inputTopic: `canary/${group}`, processName: `${packageName}-group`, profiles: resources };
  const manifestFile = path.join(pkg, `contracts/${group}.json`);
  write(manifestFile, manifest);
  const directory = path.join(state, "runtime/admission", group),
    journalId = randomUUID();
  const configurations = [
    ...resources.map((v) => ({
      id: v.id,
      processName: v.processName,
      role: "source",
      publisher: { mappingFile: `contracts/${v.id}.json`, dataGroup: "telemetry" },
    })),
    { id: "target", processName: manifest.processName, role: "target", publisher: { groupManifest: `contracts/${group}.json` } },
    { id: "receiver", processName: manifest.processName + "-admission", role: "receiver", publisher: { groupManifest: `contracts/${group}.json` } },
  ].map((v) => {
    const dir = path.join(pkg, "instances", v.id);
    fs.mkdirSync(dir, { recursive: true });
    const raw = {
      uns: { processName: v.processName },
      transform: v.publisher,
      durableInput: { directory, journalId, groupManifest: `contracts/${group}.json`, maxRecords: 10, maxBytes: 4096 },
      ...(v.role === "receiver" ? { runtimeRole: "receiver" } : {}),
    };
    write(path.join(dir, "config.json"), raw);
    return { ...v, dir, raw };
  });
  const channels = Array.from({ length: 3 }, () => ({ host: "127.0.0.1", port: 1883 }));
  const policy = {
    schemaVersion: 3,
    kind: "uns-process-group-runtime-authorization",
    protocolVersion: 1,
    packageName,
    namespace: "runtime",
    groupId: group,
    descriptorSha256: api.configurationAuthorizationHash(descriptor),
    controllerName: "local",
    packageRoot: pkg,
    artifactSha256: api.groupArtifactHash(pkg),
    groupManifestSha256: hash(fs.readFileSync(manifestFile)),
    channelsSha256: api.groupChannelsHash(channels),
    stateRoot: state,
    directory,
    journalId,
    placements: configurations.map((c) => ({
      instanceId: c.id,
      processName: c.processName,
      role: c.role,
      configurationSha256: api.configurationAuthorizationHash(c.raw),
    })),
  };
  const file = path.join(registry, group + ".json");
  const save = () => write(file, policy);
  save();
  return {
    root,
    pkg,
    state,
    registry,
    descriptor,
    metadata,
    policy,
    file,
    configurations,
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
describe("shared process-group contract", () => {
  it.each([
    ["cell-42", "example-transformer"],
    ["next-group", "independent-adapter"],
  ])("accepts reviewed %s in %s without application/group code", (group, name) => {
    const f = fixture(group, name);
    try {
      for (let i = 0; i < 4; i++) {
        const a = api.authorizeProcessGroupRuntime(f.context(i));
        expect(a.groupId).toBe(group);
        api.assertProcessGroupJournalAuthorization(a, a.directory, a.journalId ?? undefined);
      }
      expect(normalizeProcessGroupConfiguration(f.configurations[3]!.raw, processGroupDescriptor(f.pkg)!).runtimeRole).toBe("admission");
    } finally {
      f.close();
    }
  });
  it.each([
    "package",
    "descriptor",
    "artifact",
    "mapping",
    "controller",
    "role",
    "config",
    "journal",
    "foreign-file",
    "symlink",
    "permission",
    "unknown-protocol",
    "channels",
    "declared-protocol",
  ])("denies changed %s before launch", (kind) => {
    const f = fixture();
    try {
      const ctx = f.context(0);
      if (kind === "package") f.policy.packageName = "another-package";
      if (kind === "descriptor") f.policy.descriptorSha256 = "a".repeat(64);
      if (kind === "artifact") fs.appendFileSync(path.join(f.pkg, "dist/index.js"), " changed");
      if (kind === "mapping") fs.appendFileSync(path.join(f.pkg, "contracts/a.json"), " changed");
      if (kind === "controller") ctx.controllerName = "foreign";
      if (kind === "channels") ctx.channels[0]!.host = "foreign-broker";
      if (kind === "declared-protocol") {
        f.metadata.unsDatahub.processGroupRuntime.protocolVersion = 2;
        f.write(path.join(f.pkg, "package.json"), f.metadata);
        f.policy.artifactSha256 = api.groupArtifactHash(f.pkg);
      }
      if (kind === "role") f.policy.placements[0]!.role = "target";
      if (kind === "config") ctx.rawConfiguration = { ...ctx.rawConfiguration, extra: true };
      if (kind === "journal") f.policy.directory = path.join(f.state, "runtime/admission/another");
      if (kind === "foreign-file") ctx.file = path.join(f.registry, "foreign.json");
      if (kind === "unknown-protocol") (f.policy as Record<string, unknown>)["protocolVersion"] = 2;
      f.save();
      if (kind === "permission") fs.chmodSync(f.file, 0o644);
      if (kind === "symlink") {
        fs.renameSync(f.file, f.file + ".retained");
        fs.symlinkSync(f.file + ".retained", f.file);
      }
      expect(() => api.authorizeProcessGroupRuntime(ctx)).toThrow(/authorization/);
    } finally {
      f.close();
    }
  });
  it("keeps policy resources live-bound, rejects forged values and requires exact launch identity", () => {
    const f = fixture();
    try {
      const a = api.authorizeProcessGroupRuntime(f.context(2)),
        dir = f.configurations[2]!.dir,
        operationId = randomUUID(),
        launchId = randomUUID();
      expect(() => api.assertProcessGroupTargetLaunch(a, dir, operationId, launchId)).toThrow(/exact reviewed/);
      f.write(path.join(dir, "process-group-reservation.json"), {
        operationId,
        role: "target",
        configurationChecksum: "a".repeat(64),
        lastLaunch: { requestId: randomUUID(), launchId },
      });
      api.assertProcessGroupTargetLaunch(a, dir, operationId, launchId);
      expect(() => api.assertProcessGroupTargetLaunch(a, dir, operationId, randomUUID())).toThrow();
      expect(() => api.assertProcessGroupJournalAuthorization({ ...a }, a.directory, a.journalId ?? undefined)).toThrow();
      fs.appendFileSync(f.file, "\n");
      expect(() => api.assertProcessGroupJournalAuthorization(a, a.directory, a.journalId ?? undefined)).toThrow();
    } finally {
      f.close();
    }
  });
  it("initializes only an explicitly unbound receiver and never a publisher", () => {
    const f = fixture();
    try {
      (f.policy as Record<string, unknown>)["journalId"] = null;
      for (let i = 0; i < 4; i++) {
        delete (f.configurations[i]!.raw.durableInput as { journalId?: string }).journalId;
        f.policy.placements[i]!.configurationSha256 = api.configurationAuthorizationHash(f.configurations[i]!.raw);
      }
      f.save();
      expect(api.authorizeProcessGroupRuntime({ ...f.context(3), initialize: true }).journalId).toBeNull();
      for (let i = 0; i < 3; i++) expect(() => api.authorizeProcessGroupRuntime({ ...f.context(i), initialize: true })).toThrow();
      expect(() => api.authorizeProcessGroupRuntime(f.context(3))).toThrow();
    } finally {
      f.close();
    }
  });
  it("selects only reviewed private registry data and keeps retained journals fenced after deletion", () => {
    const f = fixture(),
      old = { ...process.env };
    try {
      process.env["UNS_PROCESS_GROUP_STATE_ROOT"] = f.state;
      process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = f.registry;
      delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
      expect(api.configuredGroupProcessNames().size).toBe(4);
      const ctx = f.context(0);
      delete (ctx as { file?: string }).file;
      expect(api.authorizeProcessGroupRuntime(ctx).groupId).toBe("cell-42");
      fs.mkdirSync(f.policy.directory, { recursive: true, mode: 0o700 });
      fs.unlinkSync(f.file);
      expect(() => api.configuredGroupProcessNames()).toThrow(/registry/);
      expect(() => api.authorizeProcessGroupRuntime(ctx)).toThrow();
    } finally {
      process.env = old;
      f.close();
    }
  });
  it("shares one reviewed registry across unrelated applications without a package branch", () => {
    const a = fixture("temperature-cell", "temperature-pipeline"),
      b = fixture("quality-cell", "quality-pipeline"),
      old = { ...process.env };
    try {
      const directory = path.join(a.state, "runtime/admission/quality-cell");
      b.policy.stateRoot = a.state;
      b.policy.directory = directory;
      for (let i = 0; i < 4; i++) {
        b.configurations[i]!.raw.durableInput.directory = directory;
        b.policy.placements[i]!.configurationSha256 = api.configurationAuthorizationHash(b.configurations[i]!.raw);
      }
      const file = path.join(a.registry, "quality-cell.json");
      b.write(file, b.policy);
      process.env["UNS_PROCESS_GROUP_STATE_ROOT"] = a.state;
      process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = a.registry;
      delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
      expect(api.configuredGroupProcessNames().size).toBe(8);
      for (const fixture of [a, b]) {
        const ctx = { ...fixture.context(0), stateRoot: a.state } as api.ProcessGroupAuthorizationContext;
        delete ctx.file;
        expect(api.authorizeProcessGroupRuntime(ctx).groupId).toBe(fixture.policy.groupId);
      }
    } finally {
      process.env = old;
      b.close();
      a.close();
    }
  });
  it("supervises and stages a second application using descriptor data", () => {
    const f = fixture();
    try {
      const d = processGroupDescriptor(f.pkg)!;
      expect(
        processGroupReceiverSupervision(
          f.configurations[3]!.raw,
          d,
          f.configurations[3]!.dir,
          { supervised: true, persistent: true, authorized: true },
          { enabled: true, restartOnExit: true, restartOnUnhealthy: false },
        ),
      ).toMatchObject({ autorestart: true, stop_exit_codes: [78] });
      const raw = f.configurations[3]!.raw;
      delete (raw.durableInput as { journalId?: string }).journalId;
      f.write(path.join(f.configurations[3]!.dir, "config.json"), raw);
      refreshProcessGroupStagedJournal(f.configurations[3]!.dir, true, d);
      expect(fs.existsSync(path.join(f.configurations[3]!.dir, stagedJournalFile))).toBe(true);
      fs.mkdirSync(f.policy.directory, { recursive: true, mode: 0o700 });
      expect(() => refreshProcessGroupStagedJournal(f.configurations[3]!.dir, true, d)).toThrow(/Existing/);
    } finally {
      f.close();
    }
  });
  it("inspects a generic stopped original without running application code", () => {
    const f = fixture();
    try {
      const raw = { ...f.configurations[0]!.raw };
      delete (raw as { durableInput?: unknown }).durableInput;
      const observed = inspectProcessGroupConfiguration(f.pkg, f.configurations[0]!.dir, JSON.stringify(raw), "local", {
        configurationChecksum: api.configurationAuthorizationHash,
        excludedUnsFields: [],
      });
      expect(observed.processName).toBe("example-transformer-a");
      expect(observed.dataGroup).toBe("telemetry");
      expect(observed.contract).toBeNull();
    } finally {
      f.close();
    }
  });
  it("keeps ordinary packages independent of journal path rules while rejecting linked group resources", () => {
    const f = fixture();
    try {
      const alias = path.join(f.root, "linked-package");
      fs.symlinkSync(f.pkg, alias);
      expect(() => processGroupDescriptor(alias)).toThrow(/links/);
      f.write(path.join(f.pkg, "package.json"), { name: "ordinary-python-runtime", version: "1.0.0" });
      expect(processGroupDescriptor(alias)).toBeUndefined();
    } finally {
      f.close();
    }
  });
  it("requires the reviewed state mount during inspection before opening journal data", () => {
    const f = fixture(),
      old = { ...process.env };
    try {
      process.env["UNS_PROCESS_GROUP_STATE_ROOT"] = f.state;
      process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"] = f.file;
      delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
      const c = f.configurations[2]!;
      Object.assign(c.raw, { infra: { host: "127.0.0.1", port: 1883 } });
      f.policy.placements[2]!.configurationSha256 = api.configurationAuthorizationHash(c.raw);
      f.save();
      expect(() =>
        inspectProcessGroupConfiguration(f.pkg, c.dir, JSON.stringify(c.raw), "local", {
          configurationChecksum: api.configurationAuthorizationHash,
          excludedUnsFields: [],
        }),
      ).toThrow(/state mount|filesystem mount/);
    } finally {
      process.env = old;
      f.close();
    }
  });
  it("rejects unknown protocol, dangerous/overlapping selectors and unsafe pilot paths", () => {
    const f = fixture();
    try {
      for (const bad of [
        { ...f.descriptor, protocolVersion: 2 },
        { ...f.descriptor, publisherPath: ["__proto__"] },
        { ...f.descriptor, journalPath: ["transform", "journal"] },
      ])
        expect(processGroupRuntimeDescriptorSchema.safeParse(bad).success).toBe(false);
      expect(isLocalPilotPath("/tmp/x/../../production", "linux")).toBe(false);
    } finally {
      f.close();
    }
  });
});
