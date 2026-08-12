import { afterEach, describe, expect, it } from "vitest";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const tsxCliPath = require.resolve("tsx/cli");
const cliEntry = path.resolve(process.cwd(), "packages/uns-cli/src/index.ts");
const tempDirs: string[] = [];

describe("uns-kit upgrade", () => {
  afterEach(async () => {
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("preserves custom AGENTS instructions and updates migration guidance idempotently", async () => {
    const targetDir = await makeProject({
      agents: "# Project instructions\n\nKeep this project-specific rule.\n",
    });

    const firstResult = runTsCli(targetDir, ["upgrade"]);
    expect(firstResult.status).toBe(0);

    const firstAgents = await readFile(path.join(targetDir, "AGENTS.md"), "utf8");
    expect(firstAgents).toContain("Keep this project-specific rule.");
    expect(firstAgents).toContain("node_modules/@uns-kit/core/MIGRATIONS.md");
    expect(firstAgents).toContain("When crossing `<2.0.71` to `>=2.0.71`");
    expect(firstAgents).toContain("When crossing `<3.0.0` to `>=3.0.0`");
    expect(firstAgents).toContain("When crossing `<3.0.7` to `>=3.0.7`");
    expect(countOccurrences(firstAgents, "<!-- uns-kit:migrations:start -->")).toBe(1);
    expect(countOccurrences(firstAgents, "<!-- uns-kit:migrations:end -->")).toBe(1);
    const firstPackage = JSON.parse(await readFile(path.join(targetDir, "package.json"), "utf8"));
    expect(firstPackage.unsDatahub).toEqual({
      schemaVersion: 1,
      kind: "addon",
      controllerCompatibility: ">=2 <3",
    });

    const secondResult = runTsCli(targetDir, ["upgrade"]);
    expect(secondResult.status).toBe(0);

    const secondAgents = await readFile(path.join(targetDir, "AGENTS.md"), "utf8");
    expect(secondAgents).toBe(firstAgents);
    const secondPackage = JSON.parse(await readFile(path.join(targetDir, "package.json"), "utf8"));
    expect(secondPackage).toEqual(firstPackage);
  });

  it("creates AGENTS.md when an existing project does not have one", async () => {
    const targetDir = await makeProject({});

    const result = runTsCli(targetDir, ["upgrade"]);
    expect(result.status).toBe(0);

    const agents = await readFile(path.join(targetDir, "AGENTS.md"), "utf8");
    expect(agents).toMatch(/^# AGENTS/);
    expect(agents).toContain("node_modules/@uns-kit/core/MIGRATIONS.md");
    expect(agents).toContain("Process-owned and standalone proxies have different shutdown paths.");
    expect(agents).toContain("migrate MQTT `message.table.columns` publishers");
  });

  it("preserves existing unsDatahub metadata", async () => {
    const existingMetadata = { schemaVersion: 2, kind: "custom", owner: "platform-team" };
    const targetDir = await makeProject({ unsDatahub: existingMetadata });

    const result = runTsCli(targetDir, ["upgrade"]);
    expect(result.status).toBe(0);

    const pkg = JSON.parse(await readFile(path.join(targetDir, "package.json"), "utf8"));
    expect(pkg.unsDatahub).toEqual(existingMetadata);
  });
});

function runTsCli(cwd: string, args: string[]) {
  return spawnSync(process.execPath, [tsxCliPath, cliEntry, ...args], {
    cwd,
    encoding: "utf8",
    env: { ...process.env, NO_COLOR: "1" },
  });
}

async function makeProject(options: { agents?: string; unsDatahub?: Record<string, unknown> }): Promise<string> {
  const targetDir = await mkdtemp(path.join(tmpdir(), "uns-cli-upgrade-"));
  tempDirs.push(targetDir);
  await mkdir(targetDir, { recursive: true });
  await writeFile(
    path.join(targetDir, "package.json"),
    JSON.stringify(
      {
        name: "upgrade-test-project",
        dependencies: { "@uns-kit/core": "2.0.70" },
        scripts: {},
        ...(options.unsDatahub ? { unsDatahub: options.unsDatahub } : {}),
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  if (options.agents !== undefined) {
    await writeFile(path.join(targetDir, "AGENTS.md"), options.agents, "utf8");
  }
  return targetDir;
}

function countOccurrences(value: string, search: string): number {
  return value.split(search).length - 1;
}
