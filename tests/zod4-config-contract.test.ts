import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";

import { baseSchema } from "../packages/uns-core/src/uns-config/config-schema.js";
import { composeConfigSchema } from "../packages/uns-core/src/uns-config/schema-tools.js";
import { unsCoreSchema } from "../packages/uns-core/src/uns-config/uns-core-schema.js";
import { databasesConfigSchema } from "../packages/uns-database/src/schema.js";

const require = createRequire(import.meta.url);
const tsxCliPath = require.resolve("tsx/cli");
const repoRoot = process.cwd();
const generatorEntry = path.resolve(repoRoot, "packages/uns-core/src/tools/generate-config-schema.ts");
const zod3JsonContractDigest = "7c54af9ab1bbbd6bbf08356c32fd078c44951c9c55267aa6428b2e4a9cec9c3c";
const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Zod 4 config contracts", () => {
  it("preserves the Zod 3 core config inputs and defaults", async () => {
    const templateConfig = JSON.parse(await readFile(path.resolve(repoRoot, "packages/uns-cli/templates/default/config.json"), "utf8"));

    expect(baseSchema.parse(templateConfig)).toMatchObject(templateConfig);
    expect(
      baseSchema.parse({
        uns: {
          graphql: "http://localhost:3200/graphql",
          rest: "http://localhost:3200/api",
          processName: "legacy-service",
        },
        infra: { host: "localhost" },
      }),
    ).toMatchObject({
      uns: {
        env: "dev",
        handover: true,
        instanceMode: "wait",
      },
      infra: { host: "localhost" },
    });

    const unsafeInteger = Number.MAX_SAFE_INTEGER + 1;
    expect(
      baseSchema.safeParse({
        ...templateConfig,
        infra: { host: "localhost", port: unsafeInteger },
      }).success,
    ).toBe(true);
    expect(
      baseSchema.safeParse({
        ...templateConfig,
        infra: { host: "localhost", port: 1883.5 },
      }).success,
    ).toBe(false);
    expect(baseSchema.safeParse({ ...templateConfig, unexpected: true }).success).toBe(false);
  });

  it("composes Zod 4 project and database schemas without key overlap", () => {
    const extras = z.object({ databases: databasesConfigSchema });
    const composed = composeConfigSchema(unsCoreSchema, extras).strict();
    const parsed = composed.parse({
      uns: {
        graphql: "http://localhost:3200/graphql",
        rest: "http://localhost:3200/api",
        processName: "database-service",
      },
      infra: { host: "localhost" },
      databases: {
        main: { dialect: "sqlite", filename: ":memory:" },
      },
    });

    expect(parsed.databases.main.dialect).toBe("sqlite");
    expect(() => composeConfigSchema(unsCoreSchema, z.object({ uns: z.object({}) }))).toThrow("Project extras overlap UNS core keys: uns");
  });

  it("keeps generated core JSON Schema and TypeScript output at the Zod 3 contract", async () => {
    const workdir = await makeGeneratorProject("@uns-kit/core");
    const result = runGenerator(workdir);
    expect(result.status, result.stderr).toBe(0);

    const generatedJson = JSON.parse(await readFile(path.join(workdir, "config.schema.json"), "utf8")) as JsonSchema;
    const generatedTypes = await readFile(path.join(workdir, "src/config/app-config.ts"), "utf8");
    const trackedTypes = await readFile(path.resolve(repoRoot, "packages/uns-core/src/config/app-config.ts"), "utf8");

    expect(missingLocalRefs(generatedJson)).toEqual([]);
    expect(contractDigest(generatedJson)).toBe(zod3JsonContractDigest);
    expect(generatedTypes).toBe(trackedTypes);
  });

  it("loads a Zod 4 TypeScript project extension and emits augmentation types", async () => {
    const workdir = await makeGeneratorProject("zod4-generator-fixture");
    const configDir = path.join(workdir, "src/config");
    await mkdir(configDir, { recursive: true });
    await writeFile(
      path.join(configDir, "project.config.extension.ts"),
      `import { z } from "zod";\n\nexport const projectExtrasSchema = z.object({\n  feature: z.object({ enabled: z.boolean().default(true) }).strict(),\n});\n`,
      "utf8",
    );

    const result = runGenerator(workdir);
    expect(result.status, result.stderr).toBe(0);

    const generatedJson = JSON.parse(await readFile(path.join(workdir, "config.schema.json"), "utf8")) as JsonSchema;
    const generatedTypes = await readFile(path.join(workdir, "src/config/app-config.ts"), "utf8");
    const appConfig = resolveLocalRef(generatedJson, generatedJson.$ref);

    expect(missingLocalRefs(generatedJson)).toEqual([]);
    expect(appConfig.properties?.feature).toMatchObject({
      type: "object",
      additionalProperties: false,
    });
    expect(generatedTypes).toContain("feature: {");
    expect(generatedTypes).toContain("enabled?: boolean;");
    expect(generatedTypes).toContain('declare module "@uns-kit/core/config/app-config.js"');
  });
});

type JsonSchema = Record<string, unknown> & {
  $ref?: string;
  properties?: Record<string, JsonSchema>;
};

async function makeGeneratorProject(name: string): Promise<string> {
  const workdir = await mkdtemp(path.join(tmpdir(), "uns-kit-zod4-generator-"));
  tempDirs.push(workdir);
  await symlink(path.resolve(repoRoot, "node_modules"), path.join(workdir, "node_modules"), process.platform === "win32" ? "junction" : "dir");
  await writeFile(path.join(workdir, "package.json"), JSON.stringify({ name, private: true, type: "module" }, null, 2), "utf8");
  return workdir;
}

function runGenerator(cwd: string) {
  return spawnSync(process.execPath, [tsxCliPath, generatorEntry], {
    cwd,
    encoding: "utf8",
    env: { ...process.env, NO_COLOR: "1" },
  });
}

function resolveLocalRef(document: JsonSchema, ref: unknown): JsonSchema {
  if (typeof ref !== "string" || !ref.startsWith("#/")) {
    throw new Error(`Expected a local JSON Schema ref, received ${String(ref)}`);
  }
  return ref
    .slice(2)
    .split("/")
    .reduce<JsonSchema>((value, segment) => value[segment] as JsonSchema, document);
}

function missingLocalRefs(document: JsonSchema): string[] {
  const missing: string[] = [];
  const visit = (value: unknown, location: string): void => {
    if (!value || typeof value !== "object") {
      return;
    }
    const schema = value as JsonSchema;
    if (typeof schema.$ref === "string" && schema.$ref.startsWith("#/")) {
      try {
        resolveLocalRef(document, schema.$ref);
      } catch {
        missing.push(`${location}: ${schema.$ref}`);
      }
    }
    Object.entries(schema).forEach(([key, child]) => visit(child, `${location}.${key}`));
  };
  visit(document, "$ ");
  return missing;
}

function contractDigest(document: JsonSchema): string {
  const canonical = JSON.stringify(canonicalContract(document, document));
  return createHash("sha256").update(canonical).digest("hex");
}

function canonicalContract(input: JsonSchema, document: JsonSchema, stack = new Set<JsonSchema>()): JsonSchema {
  const schema = input.$ref ? resolveLocalRef(document, input.$ref) : input;
  if (stack.has(schema)) {
    return { recursive: true };
  }
  const nextStack = new Set(stack).add(schema);
  const result: JsonSchema = {};
  const scalarKeys = [
    "type",
    "format",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minItems",
    "maxItems",
    "minProperties",
    "maxProperties",
    "pattern",
    "default",
    "const",
  ];
  scalarKeys.forEach((key) => {
    if (schema[key] !== undefined) {
      result[key] = schema[key];
    }
  });
  if (Array.isArray(schema.enum)) {
    result.enum = schema.enum;
  }
  if (Array.isArray(schema.required)) {
    result.required = [...schema.required].sort();
  }
  if (schema.properties && typeof schema.properties === "object") {
    result.properties = Object.fromEntries(
      Object.entries(schema.properties)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, value]) => [key, canonicalContract(value as JsonSchema, document, nextStack)]),
    );
  }
  if (schema.items && typeof schema.items === "object") {
    result.items = canonicalContract(schema.items as JsonSchema, document, nextStack);
  }
  if (schema.additionalProperties !== undefined) {
    result.additionalProperties =
      typeof schema.additionalProperties === "object"
        ? canonicalContract(schema.additionalProperties as JsonSchema, document, nextStack)
        : schema.additionalProperties;
  }

  const alternatives = [...((schema.anyOf as JsonSchema[] | undefined) ?? []), ...((schema.oneOf as JsonSchema[] | undefined) ?? [])].flatMap(
    (option) => {
      const resolved = option.$ref ? resolveLocalRef(document, option.$ref) : option;
      const nested = [...((resolved.anyOf as JsonSchema[] | undefined) ?? []), ...((resolved.oneOf as JsonSchema[] | undefined) ?? [])];
      return nested.length > 0 ? nested : [option];
    },
  );
  if (alternatives.length > 0) {
    result.alternatives = alternatives
      .map((option) => canonicalContract(option, document, nextStack))
      .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  }
  if (Array.isArray(schema.allOf)) {
    result.allOf = (schema.allOf as JsonSchema[])
      .map((option) => canonicalContract(option, document, nextStack))
      .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  }
  return result;
}
