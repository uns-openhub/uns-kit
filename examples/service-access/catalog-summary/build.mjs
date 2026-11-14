import { copyFile, mkdir, rm } from "node:fs/promises";

await rm("dist", { recursive: true, force: true });
await mkdir("dist");
await copyFile("src/index.mjs", "dist/index.js");
await copyFile("src/offer.mjs", "dist/offer.mjs");
// service-access.json stays at the package root, as declared in package.json.
