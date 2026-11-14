import { constants } from "node:fs";
import { lstat, open, realpath } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { resolveServiceAccessDeclaration, type ServiceAccessDeclaration } from "./service-access.js";

const MAX_BYTES = 65536;

async function readJson(file: string): Promise<unknown> {
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > MAX_BYTES) throw Error("service_access_file_invalid");
    const buffer = Buffer.alloc(MAX_BYTES + 1);
    let size = 0;
    while (size < buffer.length) {
      const { bytesRead } = await handle.read(buffer, size, buffer.length - size, size);
      if (!bytesRead) break;
      size += bytesRead;
    }
    if (size > MAX_BYTES) throw Error("service_access_file_invalid");
    return JSON.parse(buffer.subarray(0, size).toString("utf8"));
  } finally {
    await handle.close();
  }
}

/** Read the application's package data without importing application code.
 * Pass new URL('../package.json', import.meta.url) from a dist entry point.
 * Referenced declarations stay inside that package and cannot traverse symlinks. */
export async function loadServiceAccessDeclaration(packageFile: string | URL): Promise<ServiceAccessDeclaration | null> {
  const file = path.resolve(packageFile instanceof URL ? fileURLToPath(packageFile) : packageFile);
  const root = await realpath(path.dirname(file));
  return resolveServiceAccessDeclaration(await readJson(path.join(root, path.basename(file))), async (relative) => {
    let current = root;
    for (const segment of relative.split("/")) {
      current = path.join(current, segment);
      if ((await lstat(current)).isSymbolicLink()) throw Error("service_access_file_invalid");
    }
    return readJson(current);
  });
}
