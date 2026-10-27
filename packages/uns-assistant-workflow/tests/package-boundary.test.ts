import { describe, expect, it } from "vitest";
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import * as ts from "typescript";

const require = createRequire(import.meta.url);
const PACKAGE_ROOT = findPackageRoot();
const CORE_SOURCE_ROOT = join(PACKAGE_ROOT, "src");
const PACKAGE_MANIFEST = JSON.parse(readFileSync(join(PACKAGE_ROOT, "package.json"), "utf8")) as {
  name: string;
  version: string;
};
let packageBuildReady = false;

describe("assistant workflow package boundary", () => {
  it("keeps source imports inside the core module", () => {
    const violations = listTypeScriptFiles(CORE_SOURCE_ROOT).flatMap((filePath) =>
      listModuleSpecifiers(filePath)
        .filter((specifier) => specifier.startsWith("..") || !specifier.startsWith("."))
        .map((specifier) => `${relative(process.cwd(), filePath)} imports ${specifier}`),
    );

    expect(violations).toEqual([]);
  });

  it("imports the built package entrypoint as a package consumer", async () => {
    ensurePackageBuild();

    const builtPackage = await import(pathToFileURL(join(PACKAGE_ROOT, "dist/index.js")).href) as {
      defineAssistantWorkflow: (definition: unknown) => unknown;
      buildAssistantWorkflowDefinitionPackage: (definitions: readonly unknown[]) => unknown;
    };

    expect(typeof builtPackage.defineAssistantWorkflow).toBe("function");
    expect(typeof builtPackage.buildAssistantWorkflowDefinitionPackage).toBe("function");
  }, 20_000);

  it("imports built package subpaths as package consumers", async () => {
    ensurePackageBuild();

    const builtDefinition = await import(pathToFileURL(join(PACKAGE_ROOT, "dist/definition.js")).href) as {
      defineAssistantWorkflow: (definition: unknown) => unknown;
    };
    const builtRun = await import(pathToFileURL(join(PACKAGE_ROOT, "dist/run.js")).href) as {
      buildAssistantWorkflowRun: (input: unknown) => unknown;
    };
    const builtToolHandoffs = await import(pathToFileURL(join(PACKAGE_ROOT, "dist/tool-handoffs.js")).href) as {
      selectAssistantWorkflowApprovedToolInvocation: (input: unknown) => unknown;
    };
    const builtToolEvidence = await import(pathToFileURL(join(PACKAGE_ROOT, "dist/tool-evidence.js")).href) as {
      parseAssistantWorkflowToolEvidence: (value: unknown, options: unknown) => unknown;
    };

    expect(typeof builtDefinition.defineAssistantWorkflow).toBe("function");
    expect(typeof builtRun.buildAssistantWorkflowRun).toBe("function");
    expect(typeof builtToolHandoffs.selectAssistantWorkflowApprovedToolInvocation).toBe("function");
    expect(typeof builtToolEvidence.parseAssistantWorkflowToolEvidence).toBe("function");
  }, 20_000);

  it("packs only the assistant workflow package artifact", () => {
    ensurePackageBuild();

    const packOutput = execNpm(["pack", "--dry-run", "--json"], {
      cwd: PACKAGE_ROOT,
    });
    const artifact = parsePackArtifact(packOutput) as {
      name: string;
      version: string;
      filename: string;
      files: { path: string }[];
    };

    expect(artifact.name).toBe(PACKAGE_MANIFEST.name);
    expect(artifact.version).toBe(PACKAGE_MANIFEST.version);
    expect(artifact.filename).toBe(`uns-kit-assistant-workflow-${PACKAGE_MANIFEST.version}.tgz`);
    expect(artifact.files.length).toBeGreaterThan(0);

    const packedPaths = artifact.files.map((file) => file.path).sort();
    expect(packedPaths).toContain("API_STABILITY.md");
    expect(packedPaths).toContain("README.md");
    expect(packedPaths).toContain("package.json");
    expect(packedPaths).toContain("dist/index.js");
    expect(packedPaths).toContain("dist/index.d.ts");
    expect(packedPaths).toContain("dist/tool-handoffs.js");
    expect(packedPaths).toContain("dist/tool-handoffs.d.ts");
    expect(packedPaths).toContain("dist/tool-evidence.js");
    expect(packedPaths).toContain("dist/tool-evidence.d.ts");
    expect(
      packedPaths.every((path) =>
        path === "API_STABILITY.md" || path === "README.md" || path === "package.json" || path.startsWith("dist/"),
      ),
    ).toBe(true);
    expect(packedPaths).not.toContain("src/index.ts");
  }, 20_000);

  it("installs the packed tarball in an isolated ESM consumer", () => {
    ensurePackageBuild();

    const consumerDir = mkdtempSync(join(tmpdir(), "assistant-workflow-consumer-"));
    let tarballPath: string | null = null;
    try {
      const packOutput = execNpm(["pack", "--json"], { cwd: PACKAGE_ROOT });
      const artifact = parsePackArtifact(packOutput) as { filename: string };
      tarballPath = join(PACKAGE_ROOT, artifact.filename);

      writeFileSync(join(consumerDir, "package.json"), JSON.stringify({ type: "module" }));
      // Keep pnpm's local-tarball cache key independent of the checkout depth.
      copyFileSync(tarballPath, join(consumerDir, "package.tgz"));
      execPnpm([
        "add",
        "--ignore-scripts",
        "--no-lockfile",
        "./package.tgz",
      ], { cwd: consumerDir });

      const output = execFileSync(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          [
            'import { defineAssistantWorkflow } from "@uns-kit/assistant-workflow";',
            'import { buildAssistantWorkflowRun } from "@uns-kit/assistant-workflow/run";',
            'import { selectAssistantWorkflowApprovedToolInvocation } from "@uns-kit/assistant-workflow/tool-handoffs";',
            'import { parseAssistantWorkflowToolEvidence } from "@uns-kit/assistant-workflow/tool-evidence";',
            'if (typeof defineAssistantWorkflow !== "function" || typeof buildAssistantWorkflowRun !== "function" || typeof selectAssistantWorkflowApprovedToolInvocation !== "function" || typeof parseAssistantWorkflowToolEvidence !== "function") process.exit(1);',
            'process.stdout.write("consumer-imports-ok");',
          ].join("\n"),
        ],
        {
          cwd: consumerDir,
          encoding: "utf8",
          stdio: ["ignore", "pipe", "pipe"],
        },
      );

      expect(output).toBe("consumer-imports-ok");
    } finally {
      if (tarballPath && existsSync(tarballPath)) rmSync(tarballPath, { force: true });
      rmSync(consumerDir, { recursive: true, force: true });
    }
  }, 30_000);
});

function ensurePackageBuild(): void {
  if (packageBuildReady) return;
  const tscBin = require.resolve("typescript/bin/tsc");
  execFileSync(process.execPath, [tscBin, "-p", "tsconfig.build.json"], {
    cwd: PACKAGE_ROOT,
    stdio: ["ignore", "pipe", "pipe"],
  });
  packageBuildReady = true;
}

function execPnpm(
  args: readonly string[],
  options: { cwd: string },
): string {
  return execFileSync("pnpm", args, {
    cwd: options.cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function execNpm(
  args: readonly string[],
  options: { cwd: string },
): string {
  return execFileSync("npm", args, {
    cwd: options.cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function parsePackArtifact(output: string): unknown {
  const parsed = JSON.parse(output) as unknown;
  if (Array.isArray(parsed)) {
    const artifact = parsed[0];
    if (artifact) return artifact;
    throw new Error("npm pack did not return an artifact.");
  }
  return parsed;
}

function findPackageRoot(): string {
  const cwd = process.cwd();
  const cwdPackageJsonPath = join(cwd, "package.json");
  if (existsSync(cwdPackageJsonPath)) {
    const cwdPackageJson = JSON.parse(readFileSync(cwdPackageJsonPath, "utf8")) as { name?: string };
    if (cwdPackageJson.name === "@uns-kit/assistant-workflow") return cwd;
  }
  throw new Error("Package boundary tests must run from the assistant workflow package directory.");
}

function listTypeScriptFiles(dirPath: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dirPath)) {
    const entryPath = join(dirPath, entry);
    const stat = statSync(entryPath);
    if (stat.isDirectory()) {
      out.push(...listTypeScriptFiles(entryPath));
    } else if (entry.endsWith(".ts")) {
      out.push(entryPath);
    }
  }
  return out;
}

function listModuleSpecifiers(filePath: string): string[] {
  const source = readFileSync(filePath, "utf8");
  const sourceFile = ts.createSourceFile(filePath, source, ts.ScriptTarget.ESNext, true);
  const specifiers: string[] = [];

  sourceFile.forEachChild((node) => {
    if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier) {
      const specifier = node.moduleSpecifier;
      if (ts.isStringLiteral(specifier)) {
        specifiers.push(specifier.text);
      }
    }
  });

  return specifiers;
}
