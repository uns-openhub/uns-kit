import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

import {
  configuredServiceAccessDigest,
  configuredUnsRequestDigest,
  deriveConfiguredUnsAccess,
  parseConfiguredServiceAccessDeclaration,
  resolveConfiguredServiceAccessDeclaration,
} from "./configured-service-access.js";
import { loadConfiguredServiceAccessDeclaration, loadServiceAccessDeclaration } from "./service-access-package.js";
import { parseServiceAccessDeclaration } from "./service-access.js";

const declaration = {
  protocol: 2,
  requirements: [{ id: "temperature", capability: "read:uns", topicFromConfig: "/source/topic", maxAccessRules: ["factory/line-1/#"] }],
};
const configuration = { source: { topic: "factory/line-1/temperature" } };

describe("configured service access protocol 2", () => {
  it("derives a review proposal without granting access", () => {
    expect(deriveConfiguredUnsAccess(declaration, configuration)).toEqual({
      requirements: [{ id: "temperature", capability: "read:uns", topicFromConfig: "/source/topic", accessRule: "factory/line-1/temperature" }],
      policy: { scopes: ["read:uns"], accessRules: ["factory/line-1/temperature"] },
    });
    expect(() => parseServiceAccessDeclaration(declaration)).toThrow();
    expect(() =>
      parseConfiguredServiceAccessDeclaration({ protocol: 1, permissions: { scopes: ["read:uns"], accessRules: ["factory/#"] } }),
    ).toThrow();
  });
  it("supports an unrelated service and configuration layout through the same contract", () => {
    const different = {
      protocol: 2,
      requirements: [{ id: "pressure", capability: "read:uns", topicFromConfig: "/input/pressure", maxAccessRules: ["warehouse/pumps/#"] }],
    };
    expect(deriveConfiguredUnsAccess(different, { input: { pressure: "warehouse/pumps/pump-7" } }).policy).toEqual({
      scopes: ["read:uns"],
      accessRules: ["warehouse/pumps/pump-7"],
    });
  });
  it("pins canonical digest vectors for other SDK and Controller implementations", () => {
    expect(configuredServiceAccessDigest(declaration)).toBe("0495cf877378c7235ac8f03cd1f61cca6be727e041835d506fe54145bf0ac5f7");
    expect(configuredUnsRequestDigest(declaration, configuration)).toBe("34484075b7769fec9c501ebd9f8b63608a6f27af74f6b218c58203ca43f49bb5");
  });
  it.each(["/source/~2", "/source/~", "source/topic", "/source//topic", "/__proto__/topic", "/constructor/topic", "/a/b/c/d/e/f/g/h/i"])(
    "rejects invalid pointer %s",
    (topicFromConfig) => {
      expect(() =>
        parseConfiguredServiceAccessDeclaration({ ...declaration, requirements: [{ ...declaration.requirements[0], topicFromConfig }] }),
      ).toThrow();
    },
  );
  it("handles RFC 6901 escaped names without falling through prototypes or getters", () => {
    const d = { protocol: 2, requirements: [{ id: "x", capability: "read:uns", topicFromConfig: "/a~1b/t~0p" }] };
    expect(deriveConfiguredUnsAccess(d, { "a/b": { "t~p": "plant/main" } }).policy.accessRules).toEqual(["plant/main"]);
    expect(() => deriveConfiguredUnsAccess(declaration, Object.create({ source: configuration.source }))).toThrow();
    const getter = vi.fn(() => "factory/line-1/temperature");
    expect(() =>
      deriveConfiguredUnsAccess(declaration, {
        source: {
          get topic() {
            return getter();
          },
        },
      }),
    ).toThrow();
    expect(getter).not.toHaveBeenCalled();
  });
  it.each([
    "/uns/token",
    "/mqtt/PASSWORD",
    "/nested/client_secret/topic",
    "/auth/access-token",
    "/service/API_KEY",
    "/security/private-key-file",
    "/data/credentials/url",
    "/a/secret_access_key",
    "/a/dbPasswordRef",
    "/bearer",
    "/a/connection_string",
    "/configs~1SECRET/source",
    "/auth.api-key/value",
    "/infra/ACCESS_KEY_ID",
    "/encryption-key",
  ])("rejects a known credential field at any pointer depth: %s", (topicFromConfig) => {
    const d = { ...declaration, requirements: [{ ...declaration.requirements[0], topicFromConfig }] };
    expect(() => parseConfiguredServiceAccessDeclaration(d)).toThrow("configured_access_pointer_sensitive");
    expect(() => deriveConfiguredUnsAccess(d, { source: { topic: "factory/line-1/temperature" } })).toThrow("configured_access_pointer_sensitive");
  });
  it("allows ordinary configuration names that merely resemble credential names", () => {
    for (const key of ["tokenizer", "tokenRate", "secretary", "sourceTopic"]) {
      const d = { protocol: 2, requirements: [{ id: "x", capability: "read:uns", topicFromConfig: `/sources/${key}` }] };
      expect(deriveConfiguredUnsAccess(d, { sources: { [key]: "plant/line" } }).policy.accessRules).toEqual(["plant/line"]);
    }
  });
  it("rejects a credential pointer before reading its value", () => {
    const getter = vi.fn(() => "secret-looking-like-topic");
    const config = { uns: { get token() { return getter(); } } };
    const d = { protocol: 2, requirements: [{ id: "x", capability: "read:uns", topicFromConfig: "/uns/token" }] };
    expect(() => deriveConfiguredUnsAccess(d, config)).toThrow("configured_access_pointer_sensitive");
    expect(getter).not.toHaveBeenCalled();
  });
  it.each([
    {},
    { source: [] },
    { source: { topic: null } },
    { source: { topic: { provider: "env", key: "SECRET" } } },
    { source: { topic: "${SECRET}" } },
    { source: { topic: "https://elsewhere" } },
    { source: { topic: "factory/line-2/temp" } },
    { source: { topic: "factory/#" } },
  ])("rejects missing, references, arrays, invalid and overbroad source %j", (value) => {
    expect(() => deriveConfiguredUnsAccess(declaration, value)).toThrow();
  });
  it("handles explicit wildcard requests only within declared bounds and has no implicit wildcard", () => {
    expect(deriveConfiguredUnsAccess(declaration, { source: { topic: "factory/line-1/+" } }).policy.accessRules).toEqual(["factory/line-1/+"]);
    const withoutMaximum = { protocol: 2, requirements: [{ id: "x", capability: "read:uns", topicFromConfig: "/source/topic" }] };
    expect(deriveConfiguredUnsAccess(withoutMaximum, { source: { topic: "factory/other" } }).policy.accessRules).toEqual(["factory/other"]);
    expect(deriveConfiguredUnsAccess(withoutMaximum, { source: { topic: "#" } }).policy.accessRules).toEqual(["#"]);
  });
  it("keeps rejected values out of errors and bounds rule and pointer sizes", () => {
    const privateValue = "secret-value-with-${sensitive}";
    try {
      deriveConfiguredUnsAccess(declaration, { source: { topic: privateValue } });
      throw Error("expected rejection");
    } catch (error) {
      expect(String(error)).not.toContain(privateValue);
      expect(String(error)).toContain("configured_access_rule_invalid");
    }
    expect(() => deriveConfiguredUnsAccess(declaration, { source: { topic: "a".repeat(513) } })).toThrow();
    expect(() =>
      parseConfiguredServiceAccessDeclaration({
        protocol: 2,
        requirements: [{ id: "x", capability: "read:uns", topicFromConfig: "/" + "a".repeat(512) }],
      }),
    ).toThrow();
    expect(() =>
      parseConfiguredServiceAccessDeclaration({
        protocol: 2,
        requirements: [{ id: "x", capability: "read:uns", topicFromConfig: "/a", maxAccessRules: Array(65).fill("plant/#") }],
      }),
    ).toThrow();
  });
  it.each([
    { protocol: 3, requirements: [] },
    { protocol: 2, requirements: [] },
    { protocol: 2, requirements: Array.from({ length: 33 }, (_, i) => ({ id: String(i), capability: "read:uns", topicFromConfig: "/a" })) },
    {
      protocol: 2,
      requirements: [
        { id: "x", capability: "read:uns", topicFromConfig: "/a" },
        { id: "x", capability: "read:uns", topicFromConfig: "/b" },
      ],
    },
    { protocol: 2, requirements: [{ id: "x", capability: "admin:all", topicFromConfig: "/a" }] },
    { ...declaration, publications: {} },
  ])("fails closed on unsupported contracts %j", (value) => expect(() => parseConfiguredServiceAccessDeclaration(value)).toThrow());
  it("canonicalizes requirement order, maximum filters and derived path order for stable digests", () => {
    const a = {
      protocol: 2,
      requirements: [
        { id: "z", capability: "read:uns", topicFromConfig: "/z", maxAccessRules: ["plant/#", "plant/#"] },
        { id: "a", capability: "read:uns", topicFromConfig: "/a" },
      ],
    };
    const b = { protocol: 2, requirements: [a.requirements[1], { ...a.requirements[0], maxAccessRules: ["plant/#"] }] };
    expect(configuredServiceAccessDigest(a)).toBe(configuredServiceAccessDigest(b));
    expect(deriveConfiguredUnsAccess(a, { z: "plant/z", a: "plant/a" }).policy.accessRules).toEqual(["plant/a", "plant/z"]);
    expect(configuredUnsRequestDigest(a, { z: "plant/z", a: "plant/a" })).not.toBe(configuredUnsRequestDigest(a, { z: "plant/z", a: "plant/b" }));
  });
  it("loads only inline or safe relative metadata and rejects legacy collisions", async () => {
    const read = vi.fn(async () => declaration);
    expect(await resolveConfiguredServiceAccessDeclaration({ unsDatahub: { serviceAccess: declaration } }, read)).toEqual(
      parseConfiguredServiceAccessDeclaration(declaration),
    );
    expect(await resolveConfiguredServiceAccessDeclaration({ unsDatahub: { serviceAccess: "access.json" } }, read)).toEqual(
      parseConfiguredServiceAccessDeclaration(declaration),
    );
    expect(read).toHaveBeenCalledWith("access.json");
    await expect(resolveConfiguredServiceAccessDeclaration({ unsDatahub: { serviceAccess: "../private.json" } }, read)).rejects.toThrow();
    await expect(
      resolveConfiguredServiceAccessDeclaration({ unsDatahub: { serviceAccess: declaration, catalogAccess: "old.json" } }, read),
    ).rejects.toThrow();
    expect(await resolveConfiguredServiceAccessDeclaration({}, read)).toBeNull();
  });
  it("loads v2 package files safely and keeps the protocol-1 loader fail-closed", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "configured-access-"));
    const file = path.join(root, "package.json");
    try {
      await mkdir(path.join(root, "metadata"));
      await writeFile(path.join(root, "metadata/access.json"), JSON.stringify(declaration));
      await writeFile(file, JSON.stringify({ unsDatahub: { serviceAccess: "metadata/access.json" } }));
      expect(await loadConfiguredServiceAccessDeclaration(file)).toEqual(parseConfiguredServiceAccessDeclaration(declaration));
      await expect(loadServiceAccessDeclaration(file)).rejects.toThrow();
      await symlink(path.join(root, "metadata"), path.join(root, "link"));
      await writeFile(file, JSON.stringify({ unsDatahub: { serviceAccess: "link/access.json" } }));
      await expect(loadConfiguredServiceAccessDeclaration(file)).rejects.toThrow();
      await writeFile(path.join(root, "metadata/large.json"), " ".repeat(65537));
      await writeFile(file, JSON.stringify({ unsDatahub: { serviceAccess: "metadata/large.json" } }));
      await expect(loadConfiguredServiceAccessDeclaration(file)).rejects.toThrow("file_invalid");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
