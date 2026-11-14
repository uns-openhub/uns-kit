import { expect, it, vi } from "vitest";
import { mkdtemp, mkdir, writeFile, symlink, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { loadServiceAccessDeclaration } from "./service-access-package.js";
import { resolveServiceAccessDeclaration, serviceAccessDigest } from "./service-access.js";

const declaration = { protocol: 1, permissions: { scopes: ["read:uns"], accessRules: ["plant/sensor/#"] } };
const pkg = (serviceAccess: unknown) => ({ unsDatahub: { serviceAccess } });

it("normalizes inline/file representations identically without reading a file for inline data", async () => {
  const read = vi.fn(async () => declaration);
  const inline = await resolveServiceAccessDeclaration(pkg(declaration), read);
  expect(read).not.toHaveBeenCalled();
  const file = await resolveServiceAccessDeclaration(pkg("access.json"), read);
  expect(read).toHaveBeenCalledExactlyOnceWith("access.json");
  expect(inline).toEqual(file);
  expect(serviceAccessDigest(inline)).toBe(serviceAccessDigest(file));
});

it.each([null, 42, [], {}, { ...declaration, protocol: 2 }, { ...declaration, approved: true },
  "../access.json", "/access.json", "C:/access.json", "a\\access.json", "a//access.json", "./access.json"])
("fails closed on an invalid package reference %j", async (reference) => {
  const read = vi.fn();
  await expect(resolveServiceAccessDeclaration(pkg(reference), read)).rejects.toThrow();
  expect(read).not.toHaveBeenCalled();
});

it("keeps absence optional and rejects ambiguous legacy metadata before reading", async () => {
  const read = vi.fn();
  expect(await resolveServiceAccessDeclaration({ name: "ordinary-service" }, read)).toBeNull();
  expect(await resolveServiceAccessDeclaration({ unsDatahub: { catalogAccess: "legacy.json" } }, read)).toBeNull();
  await expect(resolveServiceAccessDeclaration({ unsDatahub: { serviceAccess: declaration, catalogAccess: "old.json" } }, read)).rejects.toThrow("conflict");
  expect(read).not.toHaveBeenCalled();
});

it("loads package data and compatible files, refusing symlinks, oversized data and missing declarations", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "service-access-package-"));
  const file = path.join(root, "package.json");
  try {
    const save = (reference: unknown) => writeFile(file, JSON.stringify(pkg(reference)));
    await save(declaration);
    expect(await loadServiceAccessDeclaration(pathToFileURL(file))).toEqual(declaration);
    await mkdir(path.join(root, "metadata"));
    await writeFile(path.join(root, "metadata/access.json"), JSON.stringify(declaration));
    await save("metadata/access.json");
    expect(await loadServiceAccessDeclaration(file)).toEqual(declaration);
    await symlink(path.join(root, "metadata"), path.join(root, "linked"));
    await save("linked/access.json");
    await expect(loadServiceAccessDeclaration(file)).rejects.toThrow();
    await save("missing.json");
    await expect(loadServiceAccessDeclaration(file)).rejects.toThrow();
    await writeFile(path.join(root, "huge.json"), " ".repeat(65537));
    await save("huge.json");
    await expect(loadServiceAccessDeclaration(file)).rejects.toThrow("file_invalid");
    await writeFile(file, JSON.stringify({ ...pkg(declaration), description: "x".repeat(65536) }));
    await expect(loadServiceAccessDeclaration(file)).rejects.toThrow("file_invalid");
    await rm(file);
    await symlink(path.join(root, "metadata/access.json"), file);
    await expect(loadServiceAccessDeclaration(file)).rejects.toThrow();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
