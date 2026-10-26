import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  configurationGroupJournalContract,
  configurationGroupSnapshot,
  configurationResourceHash,
} from "../../packages/uns-core/src/runtime/configuration-group-contract.js";
import { processGroupJournalContract, processGroupSourceIdentity } from "../../packages/uns-core/src/runtime/process-group-contract.js";
import { reviewInstalledProcessGroupGeneration } from "../../packages/uns-core/src/runtime/process-group-generation.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  sha256,
} from "../../packages/uns-core/src/runtime/process-group-resources.js";
import type { UpgradeConfigurations } from "../../packages/uns-core/src/runtime/process-group-upgrade.js";

export function generationFixture(name = "quality-adapter", nested = false, configurationSource = false) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "sdk-generation-"))),
    stateRoot = path.join(root, "state"),
    namespace = nested ? "documents" : "quality";
  const write = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
  const selector = nested ? ["behavior", "rules"] : ["transform"],
    journalPath = nested ? ["storage", "journal"] : ["durability"];
  const descriptor = {
    protocolVersion: 1,
    namespace,
    publisherPath: selector,
    journalPath,
    runtimeRolePath: ["role"],
    receiverRole: "input",
    manifests: [{ id: "cell", file: "contracts/cell.json" }],
  };
  const configured = {
    protocolVersion: 2,
    namespace,
    publisherPath: selector,
    groupPath: [...selector, "document"],
    memberPath: [...selector, "member"],
    journalPath,
    runtimeRolePath: ["role"],
    receiverRole: "input",
  };
  const profiles = ["a", "b"].map((id) => ({
    id,
    processName: name + "-" + id,
    dataGroup: "observations",
    mappingFile: id + ".csv",
    mappingSha256: sha256("source,path,unit\n" + id + ",example/" + id + ",kWh\n"),
  }));
  const manifest = { schemaVersion: 1, id: "cell", inputTopic: "example/input", processName: name + "-combined", profiles };
  const group = {
    schemaVersion: 2,
    id: "cell",
    inputTopic: manifest.inputTopic,
    processName: manifest.processName,
    receiverProcessName: manifest.processName + "-admission",
    profiles: profiles.map((p) => ({
      id: p.id,
      processName: p.processName,
      dataGroup: p.dataGroup,
      resource: { schema: 1, rows: [{ source: p.id, path: "example/" + p.id, unit: "kWh" }] },
    })),
  };
  const snapshot = configurationGroupSnapshot(group),
    broker = { host: "127.0.0.1", port: 1883 },
    channels = [broker, broker, broker];
  const configs = { source: {}, target: {} } as UpgradeConfigurations;
  const registry = path.join(stateRoot, namespace, "authorizations");
  fs.mkdirSync(registry, { recursive: true, mode: 0o700 });
  fs.chmodSync(stateRoot, 0o700);
  const journals = {
    source: {
      directory: path.join(stateRoot, namespace, "admission", "cell"),
      journalId: randomUUID(),
      groupManifest: "contracts/cell.json",
      maxRecords: 100,
      maxBytes: 1048576,
    },
    target: {
      directory: path.join(stateRoot, namespace, "admission", "cell-rev-b"),
      journalId: randomUUID(),
      generationId: "cell-rev-b",
      groupSha256: snapshot.sha256,
      maxRecords: 100,
      maxBytes: 1048576,
    },
  };
  if (configurationSource) delete (journals.source as Partial<typeof journals.source>).groupManifest;
  if (configurationSource)
    Object.assign(journals.source, {
      generationId: "cell-rev-a",
      groupSha256: snapshot.sha256,
      directory: path.join(stateRoot, namespace, "admission", "cell-rev-a"),
    });
  for (const side of ["source", "target"] as const) {
    const legacy = side === "source" && !configurationSource;
    const pkg = path.join(root, side);
    fs.mkdirSync(path.join(pkg, "dist"), { recursive: true });
    fs.mkdirSync(path.join(pkg, "contracts"));
    write(path.join(pkg, "package.json"), {
      name,
      version: side === "source" ? "1.0.0" : "1.1.0",
      unsDatahub: legacy ? { processGroupRuntime: descriptor } : { processGroupConfigurationRuntime: configured },
    });
    fs.writeFileSync(path.join(pkg, "dist/index.js"), 'throw Error("Inspection must not execute application JS");');
    write(path.join(pkg, "contracts/cell.json"), manifest);
    for (const p of profiles)
      fs.writeFileSync(path.join(pkg, "contracts", p.mappingFile), "source,path,unit\n" + p.id + ",example/" + p.id + ",kWh\n");
    for (const role of ["publisher", "receiver"] as const) {
      const instanceDirectory = path.join(pkg, "instances", role);
      fs.mkdirSync(instanceDirectory, { recursive: true });
      const publisher = legacy ? { groupManifest: "contracts/cell.json" } : { document: structuredClone(group) };
      const raw = {
        uns: { processName: role === "publisher" ? group.processName : group.receiverProcessName },
        infra: broker,
        ...(nested ? { behavior: { rules: publisher }, storage: { journal: journals[side] } } : { transform: publisher, durability: journals[side] }),
        ...(role === "receiver" ? { role: "input" } : {}),
      };
      configs[side][role] = { packageRoot: pkg, instanceDirectory, raw, resolved: raw };
      write(path.join(instanceDirectory, "config.json"), raw);
    }
    const c = configs[side].publisher,
      policy = legacy
        ? {
            schemaVersion: 3,
            kind: "uns-process-group-runtime-authorization",
            protocolVersion: 1,
            packageName: name,
            namespace,
            groupId: "cell",
            descriptorSha256: hash(descriptor),
            controllerName: "local",
            packageRoot: pkg,
            artifactSha256: groupArtifactHash(pkg),
            groupManifestSha256: sha256(JSON.stringify(manifest)),
            channelsSha256: groupChannelsHash(channels),
            stateRoot,
            directory: journals.source.directory,
            journalId: journals.source.journalId,
            placements: [
              ...profiles.map((p) => ({ instanceId: p.id, processName: p.processName, role: "source", configurationSha256: "a".repeat(64) })),
              ...(["publisher", "receiver"] as const).map((role) => ({
                instanceId: role,
                processName: (configs[side][role].raw["uns"] as { processName: string }).processName,
                role: role === "publisher" ? "target" : "receiver",
                configurationSha256: hash(configs[side][role].raw),
              })),
            ],
          }
        : {
            schemaVersion: 4,
            kind: "uns-process-group-runtime-authorization",
            protocolVersion: 2,
            packageName: name,
            namespace,
            groupId: "cell",
            generationId: side === "source" ? "cell-rev-a" : "cell-rev-b",
            descriptorSha256: configurationResourceHash(configured),
            controllerName: "local",
            packageRoot: pkg,
            artifactSha256: groupArtifactHash(pkg),
            groupSha256: snapshot.sha256,
            channelsSha256: groupChannelsHash(channels),
            stateRoot,
            directory: journals[side].directory,
            journalId: journals[side].journalId,
            placements: [
              ...profiles.map((p) => ({ instanceId: p.id, processName: p.processName, role: "source", configurationSha256: "a".repeat(64) })),
              ...(["publisher", "receiver"] as const).map((role) => ({
                instanceId: role,
                processName: (configs[side][role].raw["uns"] as { processName: string }).processName,
                role: role === "publisher" ? "target" : "receiver",
                configurationSha256: configurationResourceHash(configs[side][role].raw),
              })),
            ],
          };
    write(path.join(registry, (side === "source" ? (configurationSource ? "cell-rev-a" : "cell") : "cell-rev-b") + ".json"), policy);
  }
  const databases = {} as Record<"source" | "target", DatabaseSync>;
  for (const side of ["source", "target"] as const) {
    fs.mkdirSync(journals[side].directory, { recursive: true, mode: 0o700 });
    const file = path.join(journals[side].directory, "admission.sqlite"),
      db = new DatabaseSync(file);
    fs.chmodSync(file, 0o600);
    databases[side] = db;
    db.exec(
      "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE profiles(name TEXT PRIMARY KEY,contract TEXT,completed INTEGER,last_seq INTEGER);CREATE TABLE tasks(seq INTEGER PRIMARY KEY,profile TEXT,payload TEXT,bytes INTEGER);CREATE TABLE usage(id INTEGER PRIMARY KEY,records INTEGER,bytes INTEGER,admitted INTEGER)",
    );
    const legacy = side === "source" && !configurationSource;
    const contract = legacy
      ? processGroupJournalContract(processGroupSourceIdentity(broker), journals.source, profiles)
      : configurationGroupJournalContract(
          snapshot,
          processGroupSourceIdentity(broker),
          { ...journals[side], generationId: side === "source" ? "cell-rev-a" : "cell-rev-b", groupSha256: snapshot.sha256, minFreeBytes: 67108864 },
          snapshot.profiles.map(({ processName, resourceSha256, dataGroup }) => ({ processName, resourceSha256, dataGroup })),
        );
    db.prepare("INSERT INTO metadata VALUES(?,?)").run("id", journals[side].journalId);
    db.prepare("INSERT INTO metadata VALUES(?,?)").run("contract", contract);
    for (const [i, p] of profiles.entries())
      db.prepare("INSERT INTO profiles VALUES(?,?,0,0)").run(
        p.processName,
        JSON.stringify(
          legacy
            ? { processName: p.processName, mappingSha256: p.mappingSha256, dataGroup: p.dataGroup }
            : { processName: p.processName, resourceSha256: snapshot.profiles[i]!.resourceSha256, dataGroup: p.dataGroup },
        ),
      );
    db.exec("INSERT INTO usage VALUES(1,0,0,0)");
  }
  const append = (side: "source" | "target") => {
    const db = databases[side];
    db.prepare("INSERT INTO tasks VALUES(1,?,'retained',8)").run(profiles[0]!.processName);
    db.exec("UPDATE usage SET records=1,bytes=8,admitted=1");
  };
  append("source");
  const review = () => {
    const old = { ...process.env };
    process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] = registry;
    delete process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
    try {
      return reviewInstalledProcessGroupGeneration(configs, "local", stateRoot);
    } finally {
      process.env = old;
    }
  };
  return {
    root,
    stateRoot,
    registry,
    configs,
    profiles,
    group,
    journals,
    databases,
    review,
    append,
    close() {
      for (const db of Object.values(databases)) db.close();
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
}
