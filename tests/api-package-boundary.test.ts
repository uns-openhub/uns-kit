import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, it } from "vitest";

it("rebuilds incomplete API output before packing and imports the real archive as a consumer", () => {
  const apiRoot = path.resolve("packages/uns-api");
  const temporary = mkdtempSync(path.join(tmpdir(), "uns-api-package-"));
  try {
    // Reproduce the incomplete 3.0.24 archive: the main entry points are absent.
    rmSync(path.join(apiRoot, "dist/index.js"), { force: true });
    rmSync(path.join(apiRoot, "dist/index.d.ts"), { force: true });
    execFileSync("pnpm", ["pack", "--pack-destination", temporary], {
      cwd: apiRoot,
      stdio: "pipe",
      timeout: 60_000,
    });
    const archive = path.join(
      temporary,
      readdirSync(temporary).find((file) => file.endsWith(".tgz"))!,
    );
    const contents = execFileSync("tar", ["-tzf", archive], { encoding: "utf8" }).split("\n");
    for (const module of ["index", "uns-api-plugin", "uns-api-proxy", "api-interfaces", "api-event-routing", "parquet", "request-log"]) {
      expect(contents).toContain(`package/dist/${module}.js`);
      expect(contents).toContain(`package/dist/${module}.d.ts`);
    }
    execFileSync("tar", ["-xzf", archive, "-C", temporary]);
    const extracted = path.join(temporary, "package");
    // Use installed dependencies, but load API code exclusively from the packed archive.
    symlinkSync(path.join(apiRoot, "node_modules"), path.join(extracted, "node_modules"), "dir");
    const consumer = path.join(temporary, "consumer");
    const scope = path.join(consumer, "node_modules/@uns-kit");
    mkdirSync(scope, { recursive: true });
    writeFileSync(path.join(consumer, "package.json"), JSON.stringify({ name: "api-consumer", version: "1.0.0", type: "module" }));
    symlinkSync(extracted, path.join(scope, "api"), "dir");
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
      import assert from 'node:assert/strict';
      const api = await import('@uns-kit/api');
      assert.equal(typeof api.UnsApiProxy, 'function');
      assert.equal(typeof api.registerApiCatalog, 'function');
      assert.equal(typeof api.defineDataCatalogOfferSource, 'function');
      assert.equal(typeof api.catalogPublicationHandler, 'function');
    `,
      ],
      { cwd: consumer, stdio: ["ignore", "pipe", "inherit"], timeout: 15_000 },
    );
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}, 90_000);

it("rejects an archive whose manifest points to missing runtime or type entry points", () => {
  const temporary = mkdtempSync(path.join(tmpdir(), "uns-package-missing-"));
  try {
    writeFileSync(
      path.join(temporary, "package.json"),
      JSON.stringify({
        name: "@uns-kit/package-fixture",
        version: "1.0.0",
        files: ["dist"],
        main: "dist/index.js",
        types: "dist/index.d.ts",
      }),
    );
    expect(() =>
      execFileSync(process.execPath, [path.resolve("scripts/check-package-artifacts.cjs"), temporary], {
        stdio: "pipe",
        encoding: "utf8",
        timeout: 15_000,
      }),
    ).toThrow(/archive is missing: dist\/index\.js, dist\/index\.d\.ts/);
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}, 20_000);

it("includes common service access, publication types and bundled adoption guidance in the archives", () => {
  const temporary = mkdtempSync(path.join(tmpdir(), "uns-service-access-package-"));
  try {
    for (const name of ["core", "api"]) {
      execFileSync("pnpm", ["pack", "--pack-destination", temporary], {
        cwd: path.resolve(`packages/uns-${name}`), stdio: "pipe", timeout: 60_000,
      });
      const archive = path.join(temporary, readdirSync(temporary).find(file => file.startsWith(`uns-kit-${name}-`) && file.endsWith(".tgz"))!);
      const contents = execFileSync("tar", ["-tzf", archive], {encoding: "utf8"}).split("\n");
      const modules = name === "core" ? ["runtime/service-access", "runtime/service-access-package", "runtime/catalog-publication", "runtime/catalog-publication-client"] : ["catalog-publication-handler"];
      for (const module of modules) {
        expect(contents).toContain(`package/dist/${module}.js`);
        expect(contents).toContain(`package/dist/${module}.d.ts`);
      }
      if (name === "core") {
        expect(contents).toContain("package/dist/runtime/service-access-guide.md");
        const guide = execFileSync("tar", ["-xOzf", archive, "package/dist/runtime/service-access-guide.md"], {encoding: "utf8"});
        expect(guide).toContain("unsDatahub");
        expect(guide).toContain("loadServiceAccessDeclaration");
        expect(guide).toContain("resolveServiceAccessDeclaration");
      }
    }
  } finally { rmSync(temporary, {recursive: true, force: true}); }
}, 120_000);
