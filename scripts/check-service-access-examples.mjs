import { execFileSync } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const scratch = await mkdtemp(path.join(tmpdir(), "uns-service-access-examples-"));
const run = (command, args, cwd, extra = {}) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    timeout: 180000,
    env: { ...process.env, ...extra },
  });
try {
  const archives = path.join(scratch, "archives");
  await mkdir(archives);
  const dependencies = {};
  for (const name of ["core", "api"]) {
    const dir = path.join(root, "packages", "uns-" + name);
    const pkg = JSON.parse(await readFile(path.join(dir, "package.json")));
    // pnpm rewrites workspace:* in the archive. Build was run explicitly first.
    run("pnpm", ["pack", "--pack-destination", archives], dir, { npm_config_ignore_scripts: "true" });
    dependencies[pkg.name] = `file:${path.join(archives, `uns-kit-${name}-${pkg.version}.tgz`)}`;
  }
  const consumer = path.join(scratch, "consumer");
  await mkdir(consumer);
  await writeFile(
    path.join(consumer, "package.json"),
    JSON.stringify({ name: "service-access-example-check", version: "1.0.0", private: true, type: "module", dependencies }),
  );
  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false"], consumer);
  // Test the files from the actual core archive, not the monorepo source tree.
  const bundled = path.join(consumer, "node_modules/@uns-kit/core/dist/runtime/service-access-examples");
  const examples = path.join(consumer, "examples/service-access");
  await cp(bundled, examples, { recursive: true });
  const { loadServiceAccessDeclaration, loadConfiguredServiceAccessDeclaration } = await import(path.join(consumer, "node_modules/@uns-kit/core/dist/runtime/service-access-package.js"));
  for (const [name, module] of [
    ["background-reader", "reader.mjs"],
    ["configured-reader", "reader.mjs"],
    ["catalog-summary", "offer.mjs"],
  ]) {
    const dir = path.join(examples, name);
    const declaration = await (name === "configured-reader" ? loadConfiguredServiceAccessDeclaration : loadServiceAccessDeclaration)(path.join(dir, "package.json"));
    if (!declaration) throw new Error(`${name}: missing declaration`);
    for (const file of ["index.mjs", module]) run("node", ["--check", path.join(dir, "src", file)], consumer);
    run("node", ["build.mjs"], dir);
    const [packed] = JSON.parse(run("npm", ["pack", "--dry-run", "--ignore-scripts", "--json"], dir));
    const names = new Set(packed.files.map((f) => f.path));
    for (const file of ["dist/index.js", `dist/${module}`, "package.json", "config-example.json"]) {
      if (!names.has(file)) throw new Error(`${name}: missing ${file}`);
    }
    if (names.has("config.json") || names.has(".env") || [...names].some((n) => n.startsWith("node_modules/")))
      throw new Error("Private files entered an example artifact.");
    console.info(`Verified ${name}: built packaged source, declaration and credential-free assets.`);
  }
  await cp(path.join(root, "scripts/service-access-example-cases.mjs"), path.join(consumer, "examples.test.mjs"));
  process.stdout.write(run("node", ["--test", "examples.test.mjs"], consumer));
} catch (error) {
  // No live environment or tokens are used by these tests. Show captured build diagnostics.
  if (error.stdout) process.stderr.write(error.stdout);
  if (error.stderr) process.stderr.write(error.stderr);
  throw error;
} finally {
  await rm(scratch, { recursive: true, force: true });
}
