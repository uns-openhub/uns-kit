import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  applyRetainedStoreMove,
  prepareRetainedStoreMove,
  prepareRetainedStoreReturn,
  resolveRetainedStoreResources,
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
