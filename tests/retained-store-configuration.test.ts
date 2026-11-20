import { createHash } from "node:crypto";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

import {
  RETAINED_STORE_CONFIGURATION_MANIFEST,
  type RetainedStoreConfigurationManifest,
  readRetainedStoreConfigurationManifest,
  retainedStoreConfigurationManifestSchema,
  retainedStoreManifestSchema,
  reviewRetainedStoreConfigurationChange,
} from "../packages/uns-core/src/runtime/retained-store.js";

const declaration: RetainedStoreConfigurationManifest = {
  protocol: 1,
  activation: "stopped",
  fields: [{ path: ["replay", "batchSize"], kind: "integer", minimum: 1, maximum: 1_000, dependencies: [["destinations"]] }],
};
const current = '{"replay":{"batchSize":50},"destinations":["alpha","beta"],"maxId":9007199254740992,"uns":{"processName":"worker"}}';

describe("retained store configuration declaration protocol 1", () => {
  it("reviews existing scalar tuning fields and returns paths without configuration values", () => {
    expect(reviewRetainedStoreConfigurationChange(declaration, Buffer.from(current), current.replace(":50", ":125"))).toEqual({
      changedPaths: [["replay", "batchSize"]],
    });
    expect(reviewRetainedStoreConfigurationChange(declaration, current, `\n${current}\t`)).toEqual({ changedPaths: [] });
  });

  it("accepts independent text indexing diagnostics and protects source and target settings", () => {
    const indexer: RetainedStoreConfigurationManifest = {
      protocol: 1,
      activation: "stopped",
      fields: [
        { path: ["diagnostics", "level"], kind: "enum", values: ["info", "debug"], dependencies: [["targetIndex"]] },
        { path: ["diagnostics", "showTimings"], kind: "boolean", dependencies: [] },
      ],
    };
    const before = '{"targetIndex":"reports","queueFormat":"text-v1","source":"staging","diagnostics":{"level":"info","showTimings":false}}';
    const after = before.replace('"info"', '"debug"').replace("false", "true");
    expect(reviewRetainedStoreConfigurationChange(indexer, before, after)).toEqual({
      changedPaths: [
        ["diagnostics", "level"],
        ["diagnostics", "showTimings"],
      ],
    });
    for (const [existing, replacement] of [
      ["reports", "audit"],
      ["text-v1", "doc-v2"],
      ["staging", "production"],
    ]) {
      expect(() => reviewRetainedStoreConfigurationChange(indexer, before, after.replace(existing, replacement))).toThrow(
        /outside the retained store declaration/,
      );
    }
    expect(() => reviewRetainedStoreConfigurationChange(indexer, before, after.replace("debug", "trace"))).toThrow(/scalar range/);
  });

  it.each([
    current.replace('"alpha","beta"', '"beta","alpha"'),
    current.replace('"alpha","beta"', '"alpha","alpha"'),
    current.replace('"alpha","beta"', '"alpha"'),
    current.replace('"alpha","beta"', '"alpha","beta","alpha"'),
    current.replace('"replay":{"batchSize":50},"destinations":["alpha","beta"]', '"destinations":["alpha","beta"],"replay":{"batchSize":50}'),
    current.replace('"batchSize":50', ""),
    current.replace('"batchSize":50', '"batchSize":50,"other":1'),
    current.replace("9007199254740992", "9007199254740993"),
    current.replace('"processName":"worker"', '"processName":"other-worker"'),
  ])("rejects undeclared changes, including structural changes and numeric aliases (%s)", (proposed) => {
    expect(() => reviewRetainedStoreConfigurationChange(declaration, current, proposed)).toThrow(/outside the retained store declaration/);
  });

  it.each(["-0", "0", "1001", "51.0", "5.1e1", "51.000000000000000001", '"51"', "true", "null", "{}", "[]"])(
    "rejects changed values outside the exact canonical scalar domain (%s)",
    (value) => {
      expect(() => reviewRetainedStoreConfigurationChange(declaration, current, current.replace(":50", `:${value}`))).toThrow(/scalar range/);
    },
  );

  it("checks the old value and the presence of declared dependencies", () => {
    expect(() => reviewRetainedStoreConfigurationChange(declaration, current.replace(":50", ":0"), current)).toThrow(/scalar range/);
    const withoutDependency = current.replace(',"destinations":["alpha","beta"]', "");
    expect(() => reviewRetainedStoreConfigurationChange(declaration, withoutDependency, withoutDependency.replace(":50", ":125"))).toThrow(
      /dependency is absent/,
    );
  });

  it.each([
    ["literal duplicates", '{"unrelated":{"key":1,"key":2}}'],
    ["escaped duplicates", '{"unrelated":{"key":1,"\\u006bey":2}}'],
    ["unpaired surrogate escape", '{"unrelated":"\\ud800"}'],
    ["an invalid UTF-8 byte", Buffer.from([0xff])],
    ["a byte order mark", `\ufeff${current}`],
    ["a trailing comma", '{"unrelated":[1,]}'],
    ["an invalid number", '{"unrelated":01}'],
    ["nonfinite number conversion", '{"unrelated":1e999}'],
    ["trailing content", `${current}{}`],
    ["excessively deep input", `{"a":${"[".repeat(51)}0${"]".repeat(51)}}`],
    ["excessively wide input", `{"a":[${"0,".repeat(100_000)}0]}`],
    ["excessive byte length", " ".repeat(1024 * 1024 + 1)],
  ])("fails closed on %s in either revision", (_, invalid) => {
    expect(() => reviewRetainedStoreConfigurationChange(declaration, invalid, current)).toThrow(/unambiguous JSON/);
    expect(() => reviewRetainedStoreConfigurationChange(declaration, current, invalid)).toThrow(/unambiguous JSON/);
  });

  it("rejects unsupported and ambiguous declaration forms", () => {
    const [field] = declaration.fields;
    for (const invalid of [
      { ...declaration, protocol: 2 },
      { ...declaration, activation: "live" },
      { ...declaration, fields: [field, field] },
      { ...declaration, fields: [field, { ...field, path: ["replay"] }] },
      { ...declaration, fields: [{ ...field, dependencies: [["replay"]] }] },
      { ...declaration, fields: [{ ...field, dependencies: [["data"], ["data"]] }] },
      { ...declaration, fields: [{ ...field, path: ["uns", "broker"] }] },
      { ...declaration, fields: [{ ...field, path: ["__proto__", "batchSize"] }] },
      { ...declaration, fields: [{ ...field, minimum: 3000 }] },
      { ...declaration, fields: [{ ...field, kind: "enum", values: ["a", "a"] }] },
      { ...declaration, fields: [{ ...field, dependencies: undefined }] },
      { ...declaration, fields: [{ ...field, extra: "accept" }] },
    ]) {
      expect(retainedStoreConfigurationManifestSchema.safeParse(invalid).success).toBe(false);
    }
    expect(retainedStoreManifestSchema.safeParse(declaration).success).toBe(false);
  });

  it("reads a bounded package declaration and binds its exact bytes without following links", () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(), "retained-configuration-test-")));
    try {
      const file = path.join(fixture, RETAINED_STORE_CONFIGURATION_MANIFEST);
      const json = JSON.stringify(declaration);
      const bytes = Buffer.from(`${json}\n`);
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/ENOENT/);
      fs.writeFileSync(file, bytes);
      expect(readRetainedStoreConfigurationManifest(fixture)).toEqual({
        manifest: declaration,
        digest: createHash("sha256").update(bytes).digest("hex"),
      });
      fs.writeFileSync(file, json.replace('"protocol":1', '"protocol":1,"pr\\u006ftocol":1'));
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/unambiguous JSON/);
      fs.writeFileSync(file, json.replace('"protocol":1', '"protocol":1.000000000000000001'));
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/unambiguous JSON/);
      fs.writeFileSync(file, json.replace('"protocol":1', '"protocol":1,"__proto__":true'));
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/invalid retained store configuration declaration/);
      fs.writeFileSync(file, json.replace('"kind":"integer"', '"kind":"integer","__proto__":true'));
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/invalid retained store configuration declaration/);
      fs.writeFileSync(file, Buffer.alloc(64 * 1024 + 1));
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/Unsafe/);
      const linkedFile = path.join(fixture, "other");
      fs.renameSync(file, linkedFile);
      fs.writeFileSync(linkedFile, bytes);
      fs.linkSync(linkedFile, file);
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/Unsafe/);
      fs.unlinkSync(file);
      fs.symlinkSync(linkedFile, file);
      expect(() => readRetainedStoreConfigurationManifest(fixture)).toThrow(/Linked/);
    } finally {
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });
});
