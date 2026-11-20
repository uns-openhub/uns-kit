import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { captureRetained, restoreRetained } from "../packages/uns-core/src/runtime/retained-store-storage.mjs";
import {
  type StoreBinding,
  applyRetainedStoreMove,
  canPrepareEmptyRetainedStore,
  isFreshRetainedStore,
  openRetainedStoreLease,
  prepareEmptyRetainedStore,
  prepareRetainedStoreMove,
  prepareRetainedStoreReturn,
  resolveRetainedStoreResources,
  retainedStoreDigest,
  retainedStoreManifestSchema,
  writeRetainedStoreBinding,
  writeRetainedStoreJson,
} from "../packages/uns-core/src/runtime/retained-store.js";

const roots: string[] = [];
afterEach(() => {
  for (const r of roots.splice(0)) fs.rmSync(r, { recursive: true, force: true });
});
function fixture() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), "kit-retained-")));
  roots.push(root);
  const source = path.join(root, "source"),
    stores = path.join(root, "stores");
  fs.mkdirSync(source);
  fs.mkdirSync(stores, { mode: 0o700 });
  return { source, stores };
}
describe("generic same-filesystem retained store contract", () => {
  it("provisions exact fresh empty resources and preserves their provenance through backup and restore", () => {
    const { source, stores } = fixture();
    const input = preparation(source, stores);
    expect(canPrepareEmptyRetainedStore(input.manifest)).toBe(true);
    expect(isFreshRetainedStore(input.binding, input.manifest)).toBe(false);
    prepareEmptyRetainedStore(input);
    prepareEmptyRetainedStore(input);
    const resolved = resolveRetainedStoreResources(source, input.manifest);
    expect(resolved.binding).toEqual(input.binding);
    expect(fs.readdirSync(resolved.resources["queue"]!)).toEqual([]);
    expect(fs.existsSync(resolved.resources["pause"]!)).toBe(false);
    const active = openRetainedStoreLease(input.binding);
    try {
      expect(isFreshRetainedStore(input.binding, input.manifest)).toBe(true);
      expect(isFreshRetainedStore({ ...input.binding, configurationDigest: "b".repeat(64) }, input.manifest)).toBe(false);
      expect(() => prepareEmptyRetainedStore(input)).toThrow("owned");
    } finally {
      active.close();
    }
    const snapshot = path.join(path.dirname(source), "snapshot");
    fs.mkdirSync(snapshot, { mode: 0o700 });
    const limits = JSON.parse(
      fs.readFileSync(new URL("../packages/uns-core/src/runtime/retained-store-storage-contract.json", import.meta.url), "utf8"),
    );
    expect(captureRetained(stores, snapshot, "include", limits)).toMatchObject({ stores: 1 });
    expect(fs.existsSync(path.join(snapshot, input.binding.storeId, "store-fresh.json"))).toBe(true);
    fs.renameSync(stores, stores + "-original");
    fs.mkdirSync(stores, { mode: 0o700 });
    expect(restoreRetained(snapshot, stores, "a".repeat(64), limits)).toMatchObject({ state: "complete", stores: 1 });
    const restored = openRetainedStoreLease(input.binding);
    try {
      expect(isFreshRetainedStore(input.binding, input.manifest)).toBe(true);
    } finally {
      restored.close();
    }
  });

  it("resumes exact staging and binding publication after interruptions without touching advanced data", () => {
    const { source, stores } = fixture();
    const input = preparation(source, stores);
    const { binding, manifest } = input;
    prepareEmptyRetainedStore(input);
    const receipt = JSON.parse(fs.readFileSync(path.join(binding.directory, "store-fresh.json"), "utf8"));
    const stage = path.join(stores, `retained-prepare-${binding.storeId}-${retainedStoreDigest(receipt)}`);
    fs.unlinkSync(path.join(source, "retained-store-binding.json"));
    fs.renameSync(binding.directory, stage);
    fs.rmdirSync(path.join(stage, "queue"));
    fs.unlinkSync(path.join(stage, "store-resources.json"));
    fs.writeFileSync(path.join(stage, `store-resources.json.${randomUUID()}.tmp`), "incomplete", { mode: 0o600 });
    expect(() => prepareEmptyRetainedStore({ ...input, binding: { ...binding, generation: randomUUID() } })).toThrow(
      "different retained preparation",
    );
    prepareEmptyRetainedStore(input);
    expect(fs.existsSync(stage)).toBe(false);
    const bindingPath = path.join(source, "retained-store-binding.json");
    const bindingTemporary = path.join(source, ".retained-store-binding.json.empty-retained-preparation");
    const bindingBytes = fs.readFileSync(bindingPath);
    fs.unlinkSync(bindingPath);
    fs.writeFileSync(bindingTemporary, "foreign intent", { mode: 0o600 });
    expect(() => prepareEmptyRetainedStore(input)).toThrow("staging differs");
    expect(fs.readFileSync(bindingTemporary, "utf8")).toBe("foreign intent");
    fs.writeFileSync(bindingTemporary, bindingBytes.subarray(0, Math.floor(bindingBytes.length / 2)));
    prepareEmptyRetainedStore(input);
    expect(fs.existsSync(bindingTemporary)).toBe(false);
    expect(resolveRetainedStoreResources(source, manifest).binding).toEqual(binding);
    const anotherDraft = path.join(path.dirname(source), "another-draft");
    fs.mkdirSync(anotherDraft);
    expect(() => prepareEmptyRetainedStore({ ...input, instanceDirectory: anotherDraft })).toThrow("differs from preparation");
    const work = path.join(binding.directory, "queue", "pending");
    fs.writeFileSync(work, "processed by a runtime");
    expect(() => prepareEmptyRetainedStore(input)).toThrow("not empty");
    expect(fs.readFileSync(work, "utf8")).toBe("processed by a runtime");
  });

  it("rejects required files, other mounts, existing legacy data and historical ownership before preparing", () => {
    const { source, stores } = fixture();
    const input = preparation(source, stores);
    const requiredFile = { ...input.manifest, resources: input.manifest.resources.map((resource) => ({ ...resource, required: true })) };
    expect(canPrepareEmptyRetainedStore(requiredFile)).toBe(false);
    expect(() => prepareEmptyRetainedStore({ ...input, manifest: requiredFile })).toThrow("cannot initialize");
    expect(() => prepareEmptyRetainedStore({ ...input, retainedRoot: source })).toThrow("exact Runtime root");
    fs.writeFileSync(path.join(source, "pause.json"), "operator-paused");
    expect(() => prepareEmptyRetainedStore(input)).toThrow("already has retained resource data");
    expect(fs.readFileSync(path.join(source, "pause.json"), "utf8")).toBe("operator-paused");
    fs.unlinkSync(path.join(source, "pause.json"));
    fs.mkdirSync(input.binding.directory, { mode: 0o700 });
    const historical = path.join(input.binding.directory, "store.json");
    writeRetainedStoreJson(historical, input.binding);
    expect(isFreshRetainedStore(input.binding, input.manifest)).toBe(false);
    expect(() => prepareEmptyRetainedStore(input)).toThrow("not this empty retained preparation");
    expect(fs.readdirSync(input.binding.directory)).toEqual(["store.json"]);
  });
  it.each(["database", "unrelated-text-index"])("recovers interrupted moves and returns current advanced %s data", (format) => {
    const { source, stores } = fixture();
    fs.mkdirSync(path.join(source, "data"));
    fs.writeFileSync(path.join(source, "data", "pending"), "before");
    fs.writeFileSync(path.join(source, "pause.json"), "paused");
    const manifest = retainedStoreManifestSchema.parse({
      protocol: 1,
      format,
      resources: [
        { id: "work", path: "data", kind: "directory", required: true },
        { id: "control", path: "pause.json", kind: "file", required: true },
      ],
    });
    const plan = prepareRetainedStoreMove(source, stores, randomUUID(), manifest);
    expect(() =>
      applyRetainedStoreMove(plan, "adopt", () => {
        throw Error("reply lost");
      }),
    ).toThrow("reply lost");
    applyRetainedStoreMove(plan, "adopt");
    applyRetainedStoreMove(plan, "adopt");
    fs.writeFileSync(path.join(plan.directory, "work", "pending"), "advanced");
    applyRetainedStoreMove(plan, "return");
    applyRetainedStoreMove(plan, "return");
    expect(fs.readFileSync(path.join(source, "data", "pending"), "utf8")).toBe("advanced");
  });
  it("rejects path escape, overlaps, symlinks and overwrites", () => {
    const { source, stores } = fixture();
    for (const relative of ["../outside", "/absolute", "data/../queue"]) {
      expect(() =>
        retainedStoreManifestSchema.parse({
          protocol: 1,
          storageProtocol: 1,
          format: "x",
          resources: [{ id: "work", path: relative, kind: "directory", required: true }],
        }),
      ).toThrow();
    }
    fs.symlinkSync(stores, path.join(source, "data"));
    const m = retainedStoreManifestSchema.parse({
      protocol: 1,
      storageProtocol: 1,
      format: "x",
      resources: [{ id: "work", path: "data", kind: "directory", required: true }],
    });
    expect(() => prepareRetainedStoreMove(source, stores, randomUUID(), m)).toThrow();
    fs.unlinkSync(path.join(source, "data"));
    fs.mkdirSync(path.join(source, "data"));
    const plan = prepareRetainedStoreMove(source, stores, randomUUID(), m);
    fs.mkdirSync(plan.directory, { mode: 0o700 });
    fs.mkdirSync(path.join(plan.directory, "work"));
    expect(() => applyRetainedStoreMove(plan, "adopt")).toThrow("occupied");
  });
});

function preparation(instanceDirectory: string, retainedRoot: string) {
  const storeId = randomUUID();
  const manifest = retainedStoreManifestSchema.parse({
    protocol: 1,
    storageProtocol: 1,
    format: "independent-fixture-v1",
    resources: [
      { id: "queue", path: "data", kind: "directory", required: true },
      { id: "pause", path: "pause.json", kind: "file", required: false, backup: "metadata" },
    ],
  });
  const binding = {
    protocol: 1,
    storeId,
    generation: randomUUID(),
    operationId: randomUUID(),
    directory: path.join(retainedRoot, storeId),
    format: manifest.format,
    configurationDigest: "a".repeat(64),
    owner: { rttNode: "independent-worker", version: "1.0.0", instanceId: "draft" },
    maintenance: true,
  } satisfies StoreBinding;
  return { instanceDirectory, retainedRoot, binding, manifest };
}

it("binds only the current owner and returns new or atomically replaced control files", () => {
  const { source, stores } = fixture();
  fs.mkdirSync(path.join(source, "data"));
  fs.writeFileSync(path.join(source, "pause.json"), "old");
  const manifest = retainedStoreManifestSchema.parse({
    protocol: 1,
    storageProtocol: 1,
    format: "any-service-v1",
    resources: [
      { id: "queue", path: "data", kind: "directory", required: true },
      { id: "pause", path: "pause.json", kind: "file", required: false },
      { id: "new-control", path: "new.json", kind: "file", required: false },
    ],
  });
  const plan = prepareRetainedStoreMove(source, stores, randomUUID(), manifest);
  applyRetainedStoreMove(plan, "adopt");
  const owner = { rttNode: "independent-worker", version: "1.0.0", instanceId: "one" };
  const binding = {
    protocol: 1 as const,
    storeId: plan.storeId,
    generation: plan.generation,
    operationId: randomUUID(),
    directory: plan.directory,
    format: plan.format,
    configurationDigest: "a".repeat(64),
    owner,
    maintenance: true as const,
  };
  writeRetainedStoreBinding(source, binding);
  writeRetainedStoreJson(path.join(plan.directory, "store.json"), binding);
  writeRetainedStoreJson(path.join(plan.directory, "store-resources.json"), manifest);
  expect(resolveRetainedStoreResources(source, manifest, { ...owner, version: "v1.0.0" }).binding?.storeId).toBe(plan.storeId);
  expect(() => resolveRetainedStoreResources(source, manifest, { instanceId: "other" })).toThrow("runtime");
  writeRetainedStoreJson(path.join(plan.directory, "pause"), { revision: 2 });
  writeRetainedStoreJson(path.join(plan.directory, "new-control"), { revision: 3 });
  applyRetainedStoreMove(prepareRetainedStoreReturn(plan), "return");
  expect(JSON.parse(fs.readFileSync(path.join(source, "pause.json"), "utf8"))).toEqual({ revision: 2 });
  expect(JSON.parse(fs.readFileSync(path.join(source, "new.json"), "utf8"))).toEqual({ revision: 3 });
  writeRetainedStoreJson(path.join(plan.directory, "store.json"), { ...binding, owner: { ...owner, instanceId: "other" } });
  expect(() => resolveRetainedStoreResources(source, manifest)).toThrow("owner");
  expect(() =>
    retainedStoreManifestSchema.parse({
      protocol: 1,
      storageProtocol: 1,
      format: "x",
      resources: [{ id: "store.json", path: "data", kind: "directory", required: true }],
    }),
  ).toThrow("metadata");
});
