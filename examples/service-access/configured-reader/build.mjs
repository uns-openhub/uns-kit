import { copyFile, mkdir, rm } from "node:fs/promises";

await rm("dist", { recursive: true, force: true });
await mkdir("dist");
await copyFile("src/index.mjs", "dist/index.js");
await copyFile("src/reader.mjs", "dist/reader.mjs");
// Inline metadata is included by npm in the package's own package.json.
