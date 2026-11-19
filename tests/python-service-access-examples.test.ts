import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { loadServiceAccessDeclaration } from "../packages/uns-core/src/runtime/service-access-package.js";

describe("Python background declarations use the existing TS contract", () => {
  for (const [name, source] of [
    ["temperature-reader", "example/production/line-1/equipment/main/temperature"],
    ["warehouse-reader", "warehouse/cold-store/pump-7/equipment/main/pressure"],
  ]) {
    it(name, async () => {
      const manifest = fileURLToPath(new URL(`../examples/python-service-access/${name}/package.json`, import.meta.url));
      expect(await loadServiceAccessDeclaration(manifest)).toEqual({ protocol: 1, permissions: { scopes: ["read:uns"], accessRules: [source] } });
    });
  }
});
