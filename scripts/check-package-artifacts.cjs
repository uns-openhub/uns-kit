#!/usr/bin/env node
const { execFileSync } = require("node:child_process");
const { readFileSync, readdirSync, existsSync } = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const packageRoots =
  process.argv.length > 2
    ? process.argv.slice(2).map((directory) => path.resolve(directory))
    : readdirSync(path.join(root, "packages")).map((directory) => path.join(root, "packages", directory));

function targets(value) {
  if (typeof value === "string") return value.includes("*") ? [] : [value.replace(/^\.\//, "")];
  if (!value || typeof value !== "object") return [];
  return Object.values(value).flatMap(targets);
}

for (const packageRoot of packageRoots) {
  const manifestPath = path.join(packageRoot, "package.json");
  if (!existsSync(manifestPath)) continue;
  const pkg = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (pkg.private || !pkg.name?.startsWith("@uns-kit/")) continue;
  // Ignore lifecycle scripts to inspect the artifact without recursively running prepack.
  const output = execFileSync("npm", ["pack", "--dry-run", "--ignore-scripts", "--json"], {
    cwd: packageRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  const [artifact] = JSON.parse(output);
  const files = new Set(artifact.files.map((file) => file.path));
  const required = targets([pkg.main, pkg.module, pkg.types, pkg.bin, pkg.exports]);
  if (!required.length) throw new Error(`${pkg.name} does not declare any package entry points.`);
  const missing = [...new Set(required)].filter((file) => !files.has(file));
  if (missing.length) throw new Error(`${pkg.name}@${pkg.version} archive is missing: ${missing.join(", ")}`);
  console.log(`Verified ${pkg.name}@${pkg.version}: ${files.size} packed files and all declared entry points.`);
}
