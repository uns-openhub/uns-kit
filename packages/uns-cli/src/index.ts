#!/usr/bin/env node
import { execFile } from "node:child_process";
import { access, mkdir, readFile, readdir, stat, writeFile, copyFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import process from "node:process";
import readline from "node:readline/promises";
import { promisify } from "node:util";
import * as azdev from "azure-devops-node-api";
import type { IGitApi } from "azure-devops-node-api/GitApi.js";
import type { GitRepository } from "azure-devops-node-api/interfaces/GitInterfaces.js";
import {
  generateAgentsMarkdown,
  generateServiceSpecMarkdown,
  getServiceBundleOutputPublisherContracts,
  readAndValidateServiceBundle,
  type ServiceBundle,
  type ServiceBundlePublisherContract,
} from "./service-bundle.js";

const currentFilename = fileURLToPath(import.meta.url);
const currentDirname = path.dirname(currentFilename);
const require = createRequire(import.meta.url);
const cliVersion = resolveCliVersion();
const coreVersion = resolveCoreVersion();
const execFileAsync = promisify(execFile);
const AZURE_DEVOPS_PROVIDER = "azure-devops" as const;

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!command || command === "help" || command === "--help" || command === "-h") {
    printHelp();
    return;
  }

  if (command === "configure-config") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureConfigFiles(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure") {
    const configureArgs = args.slice(1);
    try {
      await runConfigureCommand(configureArgs);
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-templates") {
    const configureArgs = args.slice(1);
    try {
      await runConfigureTemplatesCommand(configureArgs);
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-devops") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureDevops(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-vscode") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureVscode(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-codegen") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureCodegen(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-api") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureApi(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-cron") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureCron(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-python") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configurePython(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "configure-uns-reference") {
    const { targetPath, overwrite } = parseTemplateCommandArgs(args.slice(1));
    try {
      await configureUnsReference(targetPath, { overwrite });
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "upgrade") {
    const targetPath = args[1] && !args[1].startsWith("--") ? args[1] : undefined;
    try {
      await upgradeProject(targetPath);
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  if (command === "create") {
    try {
      const createOptions = parseCreateArgs(args.slice(1));
      if (createOptions.bundlePath) {
        await createProjectFromBundle(createOptions);
      } else if (createOptions.projectName) {
        await createProject(createOptions.projectName);
      } else {
        throw new Error("Missing project name. Example: uns-kit create my-app");
      }
    } catch (error) {
      console.error((error as Error).message);
      process.exitCode = 1;
    }
    return;
  }

  console.error(`Unknown command: ${command}`);
  printHelp();
  process.exitCode = 1;
}

function printHelp(): void {
  console.log(
    `\nuns-kit v${cliVersion}\n` +
    "\nUsage: uns-kit <command> [options]\n" +
    "\nCommands:\n" +
    "  create <name>           Scaffold a new UNS application\n" +
    "  create --bundle <path>  Scaffold a new UNS application from service.bundle.json\n" +
    "  configure [dir] [features...] Configure multiple templates (--all, --overwrite)\n" +
    "  configure-templates [dir] [templates...] Copy any template directory (--all, --overwrite)\n" +
    "  configure-devops [dir]  Configure Azure DevOps tooling in an existing project\n" +
    "  configure-vscode [dir]  Add VS Code workspace configuration files\n" +
    "  configure-codegen [dir] Copy GraphQL codegen template and dependencies\n" +
    "  configure-api [dir]     Copy UNS API examples and add @uns-kit/api\n" +
    "  configure-cron [dir]    Copy UNS cron examples and add @uns-kit/cron\n" +
    "  configure-python [dir]   Copy Python gateway client scaffolding\n" +
    "  configure-uns-reference [dir]  Copy UNS dictionaries (objects/attributes/measurements)\n" +
    "  upgrade [dir]           Remove obsolete scripts and update to current conventions\n" +
    "  help                    Show this message\n",
  );
}

type CreateCommandOptions = {
  projectName?: string;
  bundlePath?: string;
  destPath?: string;
  allowExisting: boolean;
};

function parseCreateArgs(args: string[]): CreateCommandOptions {
  let projectName: string | undefined;
  let bundlePath: string | undefined;
  let destPath: string | undefined;
  let allowExisting = false;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === "--bundle") {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) {
        throw new Error("Missing value for --bundle.");
      }
      bundlePath = next;
      index += 1;
      continue;
    }

    if (arg === "--dest") {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) {
        throw new Error("Missing value for --dest.");
      }
      destPath = next;
      index += 1;
      continue;
    }

    if (arg === "--allow-existing") {
      allowExisting = true;
      continue;
    }

    if (arg.startsWith("--")) {
      throw new Error(`Unknown option ${arg}.`);
    }

    if (!projectName) {
      projectName = arg;
      continue;
    }

    throw new Error(`Unexpected argument ${arg}.`);
  }

  if (bundlePath && projectName) {
    throw new Error("Do not pass a positional project name with --bundle. Use --dest to override the target directory.");
  }

  if (!bundlePath && destPath) {
    throw new Error("--dest can only be used with --bundle.");
  }

  if (!bundlePath && allowExisting) {
    throw new Error("--allow-existing can only be used with --bundle.");
  }

  return { projectName, bundlePath, destPath, allowExisting };
}

async function createProject(projectName: string): Promise<void> {
  const targetDir = path.resolve(process.cwd(), projectName);
  const result = await scaffoldTsProject(projectName, targetDir);
  printTsCreateSuccess(targetDir, result.packageName, initializedGitNextSteps(result.initializedGit, "pnpm run dev"));
}

async function createProjectFromBundle(options: CreateCommandOptions): Promise<void> {
  if (!options.bundlePath) {
    throw new Error("Missing --bundle path.");
  }

  const { bundle, raw } = await readAndValidateServiceBundle(options.bundlePath, {
    expectedStack: "ts",
    cliName: "uns-kit",
    counterpartCliName: "uns-kit-py",
  });

  const targetDir = path.resolve(process.cwd(), options.destPath ?? bundle.metadata.name);
  const result = await scaffoldTsProject(bundle.metadata.name, targetDir, {
    allowExisting: options.allowExisting,
    templateName: bundle.scaffold.template,
  });

  await writeBundlePublisherExample(targetDir, bundle);
  await applyTsBundleFeatures(targetDir, bundle);
  await writeServiceBundleArtifacts(targetDir, bundle, raw);

  printTsCreateSuccess(targetDir, result.packageName, initializedGitNextSteps(result.initializedGit, "pnpm run dev"));
}

async function scaffoldTsProject(
  projectName: string,
  targetDir: string,
  options: { allowExisting?: boolean; templateName?: string } = {},
): Promise<{ packageName: string; initializedGit: boolean }> {
  await ensureTargetDir(targetDir, { allowExisting: options.allowExisting });

  const templateDir = path.resolve(currentDirname, `../templates/${options.templateName ?? DEFAULT_TEMPLATE_NAME}`);
  try {
    await access(templateDir);
  } catch (error) {
    throw new Error(`Template directory is missing: ${templateDir}`);
  }

  await copyTemplateDirectory(templateDir, targetDir, targetDir);
  await configureConfigFiles(targetDir, { overwrite: false });

  const packageName = normalizePackageName(projectName);
  await patchPackageJson(targetDir, packageName);
  await patchConfigJson(targetDir, packageName);
  await replacePlaceholders(targetDir, packageName);
  const initializedGit = await initGitRepository(targetDir);

  return { packageName, initializedGit };
}

async function applyTsBundleFeatures(targetDir: string, bundle: ServiceBundle): Promise<void> {
  const appliedFeatures = new Set<ConfigureFeatureName>();
  for (const featureName of bundle.scaffold.features) {
    const resolvedFeature = resolveBundleConfigureFeatureName(featureName);
    if (!resolvedFeature || appliedFeatures.has(resolvedFeature)) {
      continue;
    }
    appliedFeatures.add(resolvedFeature);
    if (resolvedFeature === "devops") {
      await configureDevopsFromBundle(targetDir, bundle);
      continue;
    }

    const handler = configureFeatureHandlers[resolvedFeature];
    await handler(targetDir, { overwrite: false });
  }
}

async function configureDevopsFromBundle(targetDir: string, bundle: ServiceBundle): Promise<void> {
  const provider = bundle.repository?.provider?.trim() || AZURE_DEVOPS_PROVIDER;
  if (provider !== AZURE_DEVOPS_PROVIDER) {
    throw new Error(
      `Bundle feature "devops" only supports repository.provider="${AZURE_DEVOPS_PROVIDER}" in this MVP. Received "${provider}".`,
    );
  }

  const organization = bundle.repository?.organization?.trim();
  const project = bundle.repository?.project?.trim();
  if (!organization || !project) {
    throw new Error(
      'Bundle feature "devops" requires repository.organization and repository.project for non-interactive scaffolding.',
    );
  }

  const result = await applyAzureDevopsConfig(targetDir, {
    organization,
    project,
    overwrite: false,
    ensureRemote: false,
  });

  logDevopsResult(result, { includeRemoteDetails: false });
}

async function writeBundlePublisherExample(targetDir: string, bundle: ServiceBundle): Promise<void> {
  const contracts = getServiceBundleOutputPublisherContracts(bundle);
  if (!contracts.length) {
    return;
  }
  await writeFile(path.join(targetDir, "src", "index.ts"), renderBundlePublisherExample(bundle, contracts), "utf8");
}

function renderBundlePublisherExample(
  bundle: ServiceBundle,
  contracts: ServiceBundlePublisherContract[],
): string {
  const outputContracts = contracts.map((contract) => ({
    fullPath: contract.fullPath,
    topicPrefix: contract.pathParts.topicPrefix,
    asset: contract.pathParts.asset,
    objectType: contract.pathParts.objectType,
    objectId: contract.pathParts.objectId,
    attribute: contract.pathParts.attribute,
    expectedIntervalMs: contract.expectedIntervalMs,
  }));
  const processName = JSON.stringify(bundle.metadata.name);
  const contractJson = JSON.stringify(outputContracts, null, 2)
    .split("\n")
    .map((line) => `  ${line}`)
    .join("\n")
    .trimStart();

  return `import { ConfigFile, UnsProxyProcess } from "@uns-kit/core";
import type { ISO8601 } from "@uns-kit/core/uns/uns-interfaces.js";

type OutputContract = {
  fullPath: string;
  topicPrefix: string;
  asset: string;
  objectType: string;
  objectId: string;
  attribute: string;
  expectedIntervalMs: number;
};

const outputContracts = ${contractJson} satisfies readonly OutputContract[];

function topicPrefixForPublish(topicPrefix: string): string {
  return topicPrefix.endsWith("/") ? topicPrefix : \`\${topicPrefix}/\`;
}

let activeProcess: UnsProxyProcess | undefined;

async function main(): Promise<void> {
  const config = await ConfigFile.loadConfig();
  const processName = config.uns.processName ?? ${processName};
  const unsProcess = new UnsProxyProcess(config.infra.host ?? "localhost", {
    processName,
  });
  activeProcess = unsProcess;
  const shutdown = (signal: NodeJS.Signals): void => {
    console.log("Received " + signal + "; shutting down UNS process '" + processName + "'.");
    void unsProcess.shutdown().catch((error: unknown) => {
      const reason = error instanceof Error ? error : new Error(String(error));
      console.error("UNS process '" + processName + "' shutdown failed: " + reason.message);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));

  const mqttOutput = await unsProcess.createUnsMqttProxy(
    config.output?.host ?? "localhost",
    "defaultOutput",
    config.uns.instanceMode ?? "wait",
    config.uns.handover ?? true,
  );

  const time = new Date().toISOString() as ISO8601;

  for (const output of outputContracts) {
    await mqttOutput.publishMqttMessage({
      topic: topicPrefixForPublish(output.topicPrefix),
      asset: output.asset,
      objectType: output.objectType,
      objectId: output.objectId,
      attributes: {
        attribute: output.attribute,
        description: \`Generated example publisher for \${output.fullPath}\`,
        data: {
          time,
          value: 0,
          dataGroup: "service-bundle-output",
        },
        validityMode: "interval",
        expectedIntervalMs: output.expectedIntervalMs,
      },
    });
  }

  await mqttOutput.flush();

  console.log(\`UNS process '\${processName}' published \${outputContracts.length} interval example output(s).\`);
}

void main().catch(async (error: unknown) => {
  const reason = error instanceof Error ? error : new Error(String(error));
  try {
    await activeProcess?.shutdown();
  } catch (shutdownError) {
    const shutdownReason = shutdownError instanceof Error ? shutdownError : new Error(String(shutdownError));
    console.error("UNS process cleanup failed: " + shutdownReason.message);
  }
  console.error("UNS process startup failed: " + reason.message);
  process.exitCode = 1;
});
`;
}

async function writeServiceBundleArtifacts(targetDir: string, bundle: ServiceBundle, rawBundle: string): Promise<void> {
  await writeFile(path.join(targetDir, "service.bundle.json"), rawBundle, "utf8");
  await writeFile(path.join(targetDir, "SERVICE_SPEC.md"), generateServiceSpecMarkdown(bundle), "utf8");
  await writeFile(path.join(targetDir, "AGENTS.md"), generateAgentsMarkdown(bundle), "utf8");
}

function initializedGitNextSteps(initializedGit: boolean, runCommand: string): string[] {
  const steps = ["pnpm install", runCommand];
  if (initializedGit) {
    steps.push("git status  # verify the new repository");
  }
  return steps;
}

function printTsCreateSuccess(targetDir: string, packageName: string, nextSteps: string[]): void {
  const relativeTarget = path.relative(process.cwd(), targetDir) || ".";
  console.log(`\nCreated ${packageName} in ${relativeTarget}`);
  console.log("Next steps:");
  if (relativeTarget !== ".") {
    console.log(`  cd ${relativeTarget}`);
  }
  for (const step of nextSteps) {
    console.log(`  ${step}`);
  }
}

async function initGitRepository(targetDir: string): Promise<boolean> {
  try {
    const { stdout } = await execFileAsync("git", ["rev-parse", "--is-inside-work-tree"], {
      cwd: targetDir,
      encoding: "utf8",
    });
    if (stdout.trim() === "true") {
      return false;
    }
  } catch (error) {
    if (isGitCommandNotFoundError(error)) {
      console.log("Git not found on PATH. Skipping repository initialization.");
      return false;
    }

    const execError = error as ExecFileError;
    const stderr = typeof execError.stderr === "string" ? execError.stderr : "";
    if (stderr && !stderr.includes("not a git repository")) {
      console.warn("Unable to determine git repository status:", stderr.trim());
      return false;
    }
  }

  try {
    await execFileAsync("git", ["init"], {
      cwd: targetDir,
      encoding: "utf8",
    });
    console.log("Initialized empty Git repository.");
    return true;
  } catch (error) {
    if (isGitCommandNotFoundError(error)) {
      console.log("Git not found on PATH. Skipping repository initialization.");
      return false;
    }

    const execError = error as ExecFileError;
    const stderr = typeof execError.stderr === "string" ? execError.stderr.trim() : "";
    if (stderr) {
      console.warn(`Failed to initialize git repository: ${stderr}`);
    } else {
      console.warn(`Failed to initialize git repository: ${(error as Error).message}`);
    }
    return false;
  }
}

type PackageJson = {
  name?: string;
  devDependencies?: Record<string, string>;
  dependencies?: Record<string, string>;
  scripts?: Record<string, string>;
  [key: string]: unknown;
};

const DEFAULT_UNS_DATAHUB_ADDON_METADATA = {
  schemaVersion: 1,
  kind: "addon",
  controllerCompatibility: ">=2 <3",
} as const;

function ensureUnsDatahubAddonMetadata(pkg: PackageJson): boolean {
  if (pkg.unsDatahub !== undefined) {
    return false;
  }
  pkg.unsDatahub = { ...DEFAULT_UNS_DATAHUB_ADDON_METADATA };
  return true;
}

type DevopsConfig = {
  provider?: string;
  organization?: string;
  project?: string;
  [key: string]: unknown;
};

type ConfigJson = {
  devops?: DevopsConfig;
  [key: string]: unknown;
};

type AzureRemoteInfo = {
  organization?: string;
  project?: string;
  repository?: string;
};

type ConfigureTemplateOptions = {
  overwrite?: boolean;
};

type CopyTemplateResult = {
  copied: string[];
  skipped: string[];
  overwritten: string[];
};

type ApplyAzureDevopsConfigOptions = {
  organization: string;
  project: string;
  overwrite?: boolean;
  ensureRemote?: boolean;
  repositoryName?: string;
};

type ApplyAzureDevopsConfigResult = {
  provider: string;
  organization: string;
  project: string;
  pkgChanged: boolean;
  pipelineMessage: string;
  gitRemoteMessage?: string;
  repositoryUrlMessage?: string;
};

function setPackageScript(scripts: Record<string, string>, name: string, command: string): boolean {
  if (scripts[name] === command) {
    return false;
  }

  scripts[name] = command;
  return true;
}

function ensureUnsReferenceScripts(scripts: Record<string, string>): boolean {
  const schemaChanged = setPackageScript(
    scripts,
    "sync-uns-schema",
    "node ./node_modules/@uns-kit/core/dist/tools/sync-uns-schema.js",
  );
  const metadataChanged = setPackageScript(
    scripts,
    "sync-uns-metadata",
    "node ./node_modules/@uns-kit/core/dist/tools/sync-uns-metadata.js",
  );
  return schemaChanged || metadataChanged;
}

const OBSOLETE_SCRIPTS = [
  "generate-uns-dictionary",
  "generate-uns-measurements",
  "generate-uns-reference",
  "generate-uns-metadata",
] as const;

const AGENT_MIGRATION_START = "<!-- uns-kit:migrations:start -->";
const AGENT_MIGRATION_END = "<!-- uns-kit:migrations:end -->";
const AGENT_MIGRATION_BLOCK = [
  AGENT_MIGRATION_START,
  "## UNS Kit dependency upgrades",
  "",
  "- Before changing any `@uns-kit/*` version, record the installed source version and intended target version.",
  "- After installing the target version, read `node_modules/@uns-kit/core/MIGRATIONS.md` and apply every migration whose version boundary is crossed. Do not apply unrelated migrations.",
  "- When crossing `<2.0.71` to `>=2.0.71`, inspect MQTT proxy ownership and follow the documented shutdown migration. Process-owned and standalone proxies have different shutdown paths.",
  "- When crossing `<3.0.0` to `>=3.0.0`, migrate MQTT `message.table.columns` publishers from named arrays to named objects and consumers to object-entry iteration. Keep schema `tableColumns` and Assistant/UI table formats unchanged.",
  "- When crossing `<3.0.7` to `>=3.0.7`, update the application's direct `zod` dependency to Zod 4, migrate Zod-3-only project config extension APIs, and regenerate `config.schema.json` plus `src/config/app-config.ts`.",
  "- When crossing `<3.0.11` to `>=3.0.11` for an OpenHub 2.x add-on, replace the old `>=7.1 <8` manifest range with `>=2 <3` only after compatibility validation, then use the three credential-free runtime profiles and `ServiceTokenProvider` guidance in `MIGRATIONS.md`.",
  AGENT_MIGRATION_END,
].join("\n");

type AgentMigrationGuidanceResult = "created" | "updated" | "unchanged";

async function ensureAgentMigrationGuidance(targetDir: string): Promise<AgentMigrationGuidanceResult> {
  const agentsPath = path.join(targetDir, "AGENTS.md");
  let existing: string;

  try {
    existing = await readFile(agentsPath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }

    await writeFile(agentsPath, `# AGENTS\n\n${AGENT_MIGRATION_BLOCK}\n`, "utf8");
    return "created";
  }

  const startIndex = existing.indexOf(AGENT_MIGRATION_START);
  const endIndex = existing.indexOf(AGENT_MIGRATION_END);
  const hasStart = startIndex >= 0;
  const hasEnd = endIndex >= 0;

  if (hasStart !== hasEnd || (hasStart && endIndex < startIndex)) {
    throw new Error(
      `Malformed UNS Kit migration block in ${agentsPath}. Remove the unmatched migration marker and run upgrade again.`,
    );
  }

  let next: string;
  if (hasStart) {
    const afterEnd = endIndex + AGENT_MIGRATION_END.length;
    next = existing.slice(0, startIndex) + AGENT_MIGRATION_BLOCK + existing.slice(afterEnd);
  } else {
    const separator = existing.endsWith("\n\n") ? "" : existing.endsWith("\n") ? "\n" : "\n\n";
    next = `${existing}${separator}${AGENT_MIGRATION_BLOCK}\n`;
  }

  if (next === existing) {
    return "unchanged";
  }

  await writeFile(agentsPath, next, "utf8");
  return "updated";
}

async function configureDevops(targetPath?: string, options?: ConfigureTemplateOptions): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  await ensureGitRepository(targetDir);

  const packagePath = path.join(targetDir, "package.json");
  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;
  const configPath = path.join(targetDir, "config.json");
  let configRaw: string;
  try {
    configRaw = await readFile(configPath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find config.json in ${targetDir}`);
    }
    throw error;
  }

  const config = JSON.parse(configRaw) as ConfigJson;
  const remoteUrl = await getGitRemoteUrl(targetDir, "origin");
  const remoteInfo = remoteUrl ? parseAzureRemote(remoteUrl) : undefined;

  const repositoryName = inferRepositoryNameFromPackage(pkg.name) || inferRepositoryNameFromPackage(path.basename(targetDir));

  const defaultOrganization = config.devops?.organization?.trim() || remoteInfo?.organization || "example-org";
  const organization = await promptWithDefault(
    defaultOrganization ? `Azure DevOps organization [${defaultOrganization}]: ` : "Azure DevOps organization: ",
    defaultOrganization,
    "Azure DevOps organization is required.",
  );

  const defaultProject = config.devops?.project?.trim() || remoteInfo?.project || "";
  const project = await promptWithDefault(
    defaultProject ? `Azure DevOps project [${defaultProject}]: ` : "Azure DevOps project: ",
    defaultProject,
    "Azure DevOps project is required.",
  );

  const result = await applyAzureDevopsConfig(targetDir, {
    organization,
    project,
    overwrite: options?.overwrite,
    ensureRemote: true,
    repositoryName: remoteInfo?.repository ?? repositoryName,
  });
  logDevopsResult(result, { includeRemoteDetails: true });
}

async function applyAzureDevopsConfig(
  targetDir: string,
  options: ApplyAzureDevopsConfigOptions,
): Promise<ApplyAzureDevopsConfigResult> {
  const packagePath = path.join(targetDir, "package.json");
  const configPath = path.join(targetDir, "config.json");

  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  let configRaw: string;
  try {
    configRaw = await readFile(configPath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find config.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;
  const config = JSON.parse(configRaw) as ConfigJson;

  if (!config.devops || typeof config.devops !== "object") {
    config.devops = {};
  }
  const devopsConfig = config.devops as DevopsConfig;
  devopsConfig.provider = AZURE_DEVOPS_PROVIDER;
  devopsConfig.organization = options.organization;
  devopsConfig.project = options.project;

  let gitRemoteMessage: string | undefined;
  let repositoryUrlMessage: string | undefined;

  if (options.ensureRemote) {
    const repositoryName =
      options.repositoryName
      ?? inferRepositoryNameFromPackage(pkg.name)
      ?? inferRepositoryNameFromPackage(path.basename(targetDir));
    const remoteUrl = await getGitRemoteUrl(targetDir, "origin");
    const remoteInfo = remoteUrl ? parseAzureRemote(remoteUrl) : undefined;

    if (!remoteUrl) {
      const { gitApi } = await resolveAzureGitApi(options.organization);
      const repositoryDetails = await ensureAzureRepositoryExists(gitApi, {
        organization: options.organization,
        project: options.project,
        repository: repositoryName,
      });

      const ensuredRemoteUrl =
        repositoryDetails.remoteUrl
        ?? buildAzureGitRemoteUrl(options.organization, options.project, repositoryName);
      await addGitRemote(targetDir, "origin", ensuredRemoteUrl);
      gitRemoteMessage = `  Added git remote origin -> ${ensuredRemoteUrl}`;
      const friendlyUrl =
        repositoryDetails.webUrl
        ?? buildAzureRepositoryUrl(options.organization, options.project, repositoryName);
      if (friendlyUrl) {
        repositoryUrlMessage = `  Repository URL: ${friendlyUrl}`;
      }
    } else {
      gitRemoteMessage = `  Git remote origin detected -> ${remoteUrl}`;
      const friendlyUrl = buildAzureRepositoryUrl(
        remoteInfo?.organization ?? options.organization,
        remoteInfo?.project ?? options.project,
        remoteInfo?.repository ?? repositoryName,
      );
      if (friendlyUrl) {
        repositoryUrlMessage = `  Repository URL: ${friendlyUrl}`;
      }
    }
  }

  const requiredDevDeps: Record<string, string> = {
    "azure-devops-node-api": "^15.1.0",
    "simple-git": "^3.27.0",
    "chalk": "^5.4.1",
    "prettier": "^3.5.3",
  };

  let pkgChanged = false;
  const devDeps = (pkg.devDependencies ??= {});
  const deps = pkg.dependencies ?? {};

  for (const [name, version] of Object.entries(requiredDevDeps)) {
    if (!devDeps[name] && !deps[name]) {
      devDeps[name] = version;
      pkgChanged = true;
    }
  }

  const scripts = (pkg.scripts ??= {});
  if (!scripts["pull-request"]) {
    scripts["pull-request"] = "node ./node_modules/@uns-kit/core/dist/tools/pull-request.js";
    pkgChanged = true;
  }

  const azurePipelineTemplatePath = path.resolve(currentDirname, "../templates/azure-pipelines.yml");
  try {
    await access(azurePipelineTemplatePath);
  } catch (error) {
    throw new Error("Azure Pipelines template is missing. Please ensure templates/azure-pipelines.yml exists.");
  }

  const pipelineTargetPath = path.join(targetDir, "azure-pipelines.yml");
  let pipelineMessage = "";
  if (await fileExists(pipelineTargetPath)) {
    if (options.overwrite) {
      await copyFile(azurePipelineTemplatePath, pipelineTargetPath);
      pipelineMessage = "  Overwrote azure-pipelines.yml pipeline definition.";
    } else {
      pipelineMessage = "  azure-pipelines.yml already exists (skipped).";
    }
  } else {
    await copyFile(azurePipelineTemplatePath, pipelineTargetPath);
    pipelineMessage = "  Added azure-pipelines.yml pipeline definition.";
  }

  await writeFile(configPath, JSON.stringify(config, null, 2) + "\n", "utf8");
  if (pkgChanged) {
    await writeFile(packagePath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
  }

  return {
    provider: AZURE_DEVOPS_PROVIDER,
    organization: options.organization,
    project: options.project,
    pkgChanged,
    pipelineMessage,
    gitRemoteMessage,
    repositoryUrlMessage,
  };
}

function logDevopsResult(
  result: ApplyAzureDevopsConfigResult,
  options: { includeRemoteDetails: boolean },
): void {
  console.log(`\nDevOps tooling configured.`);
  console.log(`  DevOps provider: ${result.provider}`);
  console.log(`  Azure organization: ${result.organization}`);
  console.log(`  Azure project: ${result.project}`);
  if (options.includeRemoteDetails && result.repositoryUrlMessage) {
    console.log(result.repositoryUrlMessage);
  }
  if (options.includeRemoteDetails && result.gitRemoteMessage) {
    console.log(result.gitRemoteMessage);
  }
  if (result.pipelineMessage) {
    console.log(result.pipelineMessage);
  }
  if (result.pkgChanged) {
    console.log("  Updated package.json scripts/devDependencies. Run pnpm install to fetch new packages.");
  } else {
    console.log("  Existing package.json already contained required entries.");
  }
}

async function configureVscode(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const templateDir = path.resolve(currentDirname, "../templates/vscode");

  try {
    await access(templateDir);
  } catch (error) {
    throw new Error("VS Code template directory is missing. Please ensure templates/vscode is available.");
  }

  const { copied, skipped, overwritten } = await copyTemplateDirectory(templateDir, targetDir, targetDir, options);

  console.log("\nVS Code configuration files processed.");
  if (copied.length) {
    console.log("  Added:");
    for (const file of copied) {
      console.log(`    ${file}`);
    }
  }
  if (overwritten.length) {
    console.log("  Overwritten:");
    for (const file of overwritten) {
      console.log(`    ${file}`);
    }
  }
  if (skipped.length) {
    console.log("  Skipped (already exists):");
    for (const file of skipped) {
      console.log(`    ${file}`);
    }
  }
  if (!copied.length && !skipped.length) {
    console.log("  No files were found in the VS Code template directory.");
  }
}

async function configureConfigFiles(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const templateDir = path.resolve(currentDirname, "../templates/config-files");

  try {
    await access(templateDir);
  } catch (error) {
    throw new Error("Configuration template directory is missing. Please ensure templates/config-files is available.");
  }

  const { copied, skipped, overwritten } = await copyTemplateDirectory(templateDir, targetDir, targetDir, options);

  const configFilesToAdjust = [...copied, ...overwritten].filter((file) => {
    const filename = path.basename(file);
    return filename.toLowerCase().startsWith("config") && filename.toLowerCase().endsWith(".json");
  });
  if (configFilesToAdjust.length) {
    await applyConfigTemplatePlaceholders(targetDir, configFilesToAdjust);
  }

  console.log("\nConfiguration example files processed.");
  if (copied.length) {
    console.log("  Added:");
    for (const file of copied) {
      console.log(`    ${file}`);
    }
  }
  if (overwritten.length) {
    console.log("  Overwritten:");
    for (const file of overwritten) {
      console.log(`    ${file}`);
    }
  }
  if (skipped.length) {
    console.log("  Skipped (already exists):");
    for (const file of skipped) {
      console.log(`    ${file}`);
    }
  }
  if (!copied.length && !skipped.length) {
    console.log("  No configuration files were found in templates/config-files.");
  }
}

async function applyConfigTemplatePlaceholders(targetDir: string, relativePaths: string[]): Promise<void> {
  const packageName = await resolvePackageNameFromTarget(targetDir);
  for (const relativePath of relativePaths) {
    const absolutePath = path.join(targetDir, relativePath);
    const replacements: Record<string, string> = {
      __APP_CONFIG__: deriveConfigIdentifier(absolutePath),
    };
    if (packageName) {
      replacements.__APP_NAME__ = packageName;
    }
    await replaceConfigTemplatePlaceholders(absolutePath, replacements, packageName);
  }
}

async function configureCodegen(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const templateDir = path.resolve(currentDirname, "../templates/codegen");
  const packagePath = path.join(targetDir, "package.json");

  try {
    await access(templateDir);
  } catch (error) {
    throw new Error("GraphQL codegen template directory is missing. Please ensure templates/codegen is available.");
  }

  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;

  const { copied, skipped, overwritten } = await copyTemplateDirectory(templateDir, targetDir, targetDir, options);

  const devDeps = (pkg.devDependencies ??= {});
  const deps = pkg.dependencies ?? {};

  const requiredDevDeps: Record<string, string> = {
    "@graphql-codegen/cli": "^5.0.7",
    "@graphql-codegen/typescript": "^4.1.6",
    "@graphql-codegen/typescript-operations": "^4.6.1",
    "@graphql-codegen/typescript-resolvers": "^4.3.1",
    "graphql": "^16.11.0",
    "graphql-request": "^7.2.0"
  };

  let pkgChanged = false;
  for (const [name, version] of Object.entries(requiredDevDeps)) {
    if (!devDeps[name] && !deps[name]) {
      devDeps[name] = version;
      pkgChanged = true;
    }
  }

  const scripts = (pkg.scripts ??= {});
  if (!scripts["generate-codegen"]) {
    scripts["generate-codegen"] = "graphql-code-generator --config codegen.ts";
    pkgChanged = true;
  }

  if (pkgChanged) {
    await writeFile(packagePath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
  }

  console.log("\nGraphQL code generation setup complete.");
  if (copied.length) {
    console.log("  Added files:");
    for (const file of copied) {
      console.log(`    ${file}`);
    }
  }
  if (overwritten.length) {
    console.log("  Overwritten files:");
    for (const file of overwritten) {
      console.log(`    ${file}`);
    }
  }
  if (skipped.length) {
    console.log("  Skipped existing files:");
    for (const file of skipped) {
      console.log(`    ${file}`);
    }
  }
  if (!copied.length && !skipped.length) {
    console.log("  No template files were copied.");
  }

  if (pkgChanged) {
    console.log("  Updated package.json scripts/devDependencies. Run pnpm install to fetch new packages.");
  } else {
    console.log("  Existing package.json already contained required scripts and dependencies.");
  }
}

async function configureApi(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  await configurePlugin({
    targetPath,
    templateName: "api",
    dependencyName: "@uns-kit/api",
    dependencySpecifier: resolveUnsPackageSpecifier("@uns-kit/api", "../../uns-api/package.json"),
    label: "UNS API",
    overwrite: options.overwrite,
  });
}

async function configureCron(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  await configurePlugin({
    targetPath,
    templateName: "cron",
    dependencyName: "@uns-kit/cron",
    dependencySpecifier: resolveUnsPackageSpecifier("@uns-kit/cron", "../../uns-cron/package.json"),
    label: "UNS cron",
    overwrite: options.overwrite,
  });
}

async function configurePython(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  await configurePlugin({
    targetPath,
    templateName: "python",
    label: "UNS Python client",
    overwrite: options.overwrite,
  });
}

async function configureUnsReference(targetPath?: string, options: ConfigureTemplateOptions = {}): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");

  await configurePlugin({
    targetPath,
    templateName: "uns-dictionary",
    label: "UNS dictionary (object/attribute metadata)",
    overwrite: options.overwrite,
  });
  await configurePlugin({
    targetPath,
    templateName: "uns-measurements",
    label: "UNS measurements (units)",
    overwrite: options.overwrite,
  });

  const packagePath = path.join(targetDir, "package.json");
  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;
  const scripts = (pkg.scripts ??= {});
  const pkgChanged = ensureUnsReferenceScripts(scripts);

  if (pkgChanged) {
    await writeFile(packagePath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
    console.log("  Updated package.json schema scripts.");
  } else {
    console.log("  Existing package.json already contained schema scripts.");
  }
}

async function upgradeProject(targetPath?: string): Promise<void> {
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const packagePath = path.join(targetDir, "package.json");

  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;
  const scripts = (pkg.scripts ??= {});
  let changed = false;
  const addonMetadataChanged = ensureUnsDatahubAddonMetadata(pkg);
  changed = addonMetadataChanged || changed;

  const removed: string[] = [];
  for (const name of OBSOLETE_SCRIPTS) {
    if (scripts[name] !== undefined) {
      delete scripts[name];
      removed.push(name);
      changed = true;
    }
  }

  const syncScriptsChanged = ensureUnsReferenceScripts(scripts);
  changed = syncScriptsChanged || changed;

  const agentGuidanceResult = await ensureAgentMigrationGuidance(targetDir);
  changed = agentGuidanceResult !== "unchanged" || changed;

  if (removed.length > 0 || syncScriptsChanged || addonMetadataChanged) {
    await writeFile(packagePath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
  }

  console.log(`\nUpgrade complete for ${targetDir}`);
  if (removed.length > 0) {
    console.log("  Removed obsolete scripts:");
    for (const name of removed) {
      console.log(`    - ${name}`);
    }
  }
  if (syncScriptsChanged) {
    console.log("  Added/updated: sync-uns-schema, sync-uns-metadata");
  } else {
    console.log("  Ensured: sync-uns-schema, sync-uns-metadata");
  }
  if (addonMetadataChanged) {
    console.log("  Added: package.json unsDatahub add-on metadata");
  } else {
    console.log("  Preserved: existing package.json unsDatahub metadata");
  }
  if (agentGuidanceResult === "created") {
    console.log("  Created AGENTS.md with version-bounded migration guidance.");
  } else if (agentGuidanceResult === "updated") {
    console.log("  Added/updated version-bounded migration guidance in AGENTS.md.");
  } else {
    console.log("  Ensured version-bounded migration guidance in AGENTS.md.");
  }
  if (!changed) {
    console.log("  Already up to date.");
  }
}

type ConfigureFeatureHandler = (targetPath?: string, options?: ConfigureTemplateOptions) => Promise<void>;

const configureFeatureHandlers = {
  devops: configureDevops,
  vscode: configureVscode,
  codegen: configureCodegen,
  api: configureApi,
  cron: configureCron,
  python: configurePython,
  "uns-reference": configureUnsReference,
} as const satisfies Record<string, ConfigureFeatureHandler>;

type ConfigureFeatureName = keyof typeof configureFeatureHandlers;

const AVAILABLE_CONFIGURE_FEATURES = Object.keys(configureFeatureHandlers) as ConfigureFeatureName[];

const configureFeatureLabels: Record<ConfigureFeatureName, string> = {
  devops: "Azure DevOps tooling",
  vscode: "VS Code workspace",
  codegen: "GraphQL codegen tooling",
  api: "UNS API resources",
  cron: "UNS cron resources",
  python: "Python client scaffolding",
  "uns-reference": "UNS dictionaries (objects/attributes/measurements)",
};

type ConfigureCommandOptions = {
  targetPath?: string;
  features: ConfigureFeatureName[];
  overwrite: boolean;
};

async function runConfigureCommand(args: string[]): Promise<void> {
  const { targetPath, features, overwrite } = parseConfigureArgs(args);
  if (!features.length) {
    throw new Error("No features specified. Provide feature names or pass --all.");
  }

  const location = targetPath ?? ".";
  const featureSummary = features.map((feature) => configureFeatureLabels[feature]).join(", ");
  console.log(`Configuring ${featureSummary} in ${location}`);
  for (const feature of features) {
    const handler = configureFeatureHandlers[feature];
    await handler(targetPath, { overwrite });
  }
}

const DEFAULT_TEMPLATE_NAME = "default";

async function listTemplateDirectories(templateRoot: string, options?: { includeDefault?: boolean }): Promise<string[]> {
  const entries = await readdir(templateRoot, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => options?.includeDefault ? true : name !== DEFAULT_TEMPLATE_NAME)
    .sort();
}

type ConfigureTemplatesCommandOptions = {
  targetPath?: string;
  overwrite: boolean;
  includeAll: boolean;
  templateNames: string[];
};

function parseConfigureTemplatesArgs(
  args: string[],
  availableTemplates: string[],
): ConfigureTemplatesCommandOptions {
  const templateMap = new Map(availableTemplates.map((name) => [name.toLowerCase(), name]));
  const templateSet = new Set(templateMap.keys());
  const templateNames: string[] = [];
  const unknownTemplates: string[] = [];
  let targetPath: string | undefined;
  let overwrite = false;
  let includeAll = false;
  let expectDir = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (expectDir) {
      targetPath = arg;
      expectDir = false;
      continue;
    }
    if (arg === "--dir") {
      expectDir = true;
      continue;
    }
    if (arg === "--all") {
      includeAll = true;
      continue;
    }
    if (arg === "--overwrite" || arg === "--force") {
      overwrite = true;
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`Unknown option ${arg}.`);
    }

    const normalized = arg.trim().toLowerCase();
    const resolved = templateMap.get(normalized);
    if (resolved) {
      templateNames.push(resolved);
      continue;
    }

    if (!targetPath && (includeAll || i < args.length - 1) && !templateSet.has(normalized)) {
      targetPath = arg;
      continue;
    }

    unknownTemplates.push(arg);
  }

  if (expectDir) {
    throw new Error("Missing value for --dir.");
  }

  if (unknownTemplates.length) {
    const available = availableTemplates.join(", ");
    throw new Error(
      `Unknown template(s): ${unknownTemplates.join(", ")}. Available templates: ${available || "none"}.`,
    );
  }

  return { targetPath, overwrite, includeAll, templateNames };
}

async function runConfigureTemplatesCommand(args: string[]): Promise<void> {
  const templateRoot = path.resolve(currentDirname, "../templates");
  const availableTemplates = await listTemplateDirectories(templateRoot, { includeDefault: true });
  const allTemplates = await listTemplateDirectories(templateRoot, { includeDefault: false });
  const { targetPath, overwrite, includeAll, templateNames } = parseConfigureTemplatesArgs(args, availableTemplates);

  const selectedTemplates = new Set<string>();
  if (includeAll) {
    allTemplates.forEach((name) => selectedTemplates.add(name));
  }
  for (const name of templateNames) {
    selectedTemplates.add(name);
  }

  if (!selectedTemplates.size) {
    throw new Error("No templates specified. Provide template names or pass --all.");
  }

  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const sortedTemplates = Array.from(selectedTemplates).sort();

  for (const templateName of sortedTemplates) {
    const templateDir = path.resolve(templateRoot, templateName);
    const { copied, skipped, overwritten } = await copyTemplateDirectory(
      templateDir,
      targetDir,
      targetDir,
      { overwrite },
    );

    console.log(`\nTemplate "${templateName}" processed.`);
    if (copied.length) {
      console.log("  Added:");
      for (const file of copied) {
        console.log(`    ${file}`);
      }
    }
    if (overwritten.length) {
      console.log("  Overwritten:");
      for (const file of overwritten) {
        console.log(`    ${file}`);
      }
    }
    if (skipped.length) {
      console.log("  Skipped (already exists):");
      for (const file of skipped) {
        console.log(`    ${file}`);
      }
    }
    if (!copied.length && !overwritten.length && !skipped.length) {
      console.log("  No template files were copied.");
    }
  }
}

function parseConfigureArgs(args: string[]): ConfigureCommandOptions {
  let targetPath: string | undefined;
  let includeAll = false;
  let overwrite = false;
  const featureInputs: string[] = [];

  for (const arg of args) {
    if (arg === "--all") {
      includeAll = true;
      continue;
    }
    if (arg === "--overwrite" || arg === "--force") {
      overwrite = true;
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`Unknown option ${arg}.`);
    }

    const normalized = arg.trim().toLowerCase();
    if (configureFeatureAliases[normalized]) {
      featureInputs.push(arg);
      continue;
    }

    if (!targetPath) {
      targetPath = arg;
      continue;
    }

    featureInputs.push(arg);
  }

  const featureOrder: ConfigureFeatureName[] = [];
  const featureSet = new Set<ConfigureFeatureName>();

  const addFeature = (feature: ConfigureFeatureName): void => {
    if (!featureSet.has(feature)) {
      featureSet.add(feature);
      featureOrder.push(feature);
    }
  };

  if (includeAll) {
    for (const feature of AVAILABLE_CONFIGURE_FEATURES) {
      addFeature(feature);
    }
  }

  for (const input of featureInputs) {
    addFeature(resolveConfigureFeatureName(input));
  }

  return { targetPath, features: featureOrder, overwrite };
}

function parseTemplateCommandArgs(args: string[]): { targetPath?: string; overwrite: boolean } {
  let targetPath: string | undefined;
  let overwrite = false;
  for (const arg of args) {
    if (arg === "--overwrite" || arg === "--force") {
      overwrite = true;
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`Unknown option ${arg}.`);
    }
    if (!targetPath) {
      targetPath = arg;
      continue;
    }
    throw new Error(`Unexpected argument ${arg}.`);
  }
  return { targetPath, overwrite };
}

const configureFeatureAliases: Record<string, ConfigureFeatureName> = {
  devops: "devops",
  "configure-devops": "devops",
  vscode: "vscode",
  "configure-vscode": "vscode",
  codegen: "codegen",
  "configure-codegen": "codegen",
  api: "api",
  "configure-api": "api",
  cron: "cron",
  "configure-cron": "cron",
  python: "python",
  "configure-python": "python",
  "uns-reference": "uns-reference",
  "configure-uns-reference": "uns-reference",
};

function resolveConfigureFeatureName(input: unknown): ConfigureFeatureName {
  if (typeof input !== "string") {
    throw new Error(
      `Invalid feature value ${JSON.stringify(input)}. Expected a string from: ${AVAILABLE_CONFIGURE_FEATURES.join(", ")}.`,
    );
  }

  const normalized = input.trim().toLowerCase();
  const feature = configureFeatureAliases[normalized];
  if (!feature) {
    throw new Error(
      `Unknown feature "${input}". Available features: ${AVAILABLE_CONFIGURE_FEATURES.join(", ")}.`,
    );
  }

  return feature;
}

function resolveBundleConfigureFeatureName(input: unknown): ConfigureFeatureName | null {
  if (typeof input === "string") {
    const normalized = input.trim().toLowerCase();
    if (normalized === "workspace" || normalized === "configure-workspace") {
      return "vscode";
    }
  }
  return resolveConfigureFeatureName(input);
}

async function ensureGitRepository(dir: string): Promise<void> {
  try {
    const { stdout } = await execFileAsync("git", ["rev-parse", "--is-inside-work-tree"], {
      cwd: dir,
      encoding: "utf8",
    });
    if (stdout.trim() !== "true") {
      throw new Error(`Directory ${dir} is not a git repository.`);
    }
  } catch (error) {
    if (isGitCommandNotFoundError(error)) {
      throw new Error("Git is required to run configure-devops but was not found in PATH.");
    }

    const execError = error as ExecFileError;
    const stderr = typeof execError.stderr === "string" ? execError.stderr : "";
    if (stderr.includes("not a git repository")) {
      throw new Error(`Directory ${dir} is not a git repository.`);
    }

    throw error;
  }
}

async function getGitRemoteUrl(dir: string, remoteName: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync("git", ["remote", "get-url", remoteName], {
      cwd: dir,
      encoding: "utf8",
    });
    return stdout.trim();
  } catch (error) {
    if (isGitCommandNotFoundError(error)) {
      throw new Error("Git is required to run configure-devops but was not found in PATH.");
    }

    if (isGitRemoteMissingError(error)) {
      return null;
    }

    throw error;
  }
}

async function addGitRemote(dir: string, remoteName: string, remoteUrl: string): Promise<void> {
  try {
    await execFileAsync("git", ["remote", "add", remoteName, remoteUrl], {
      cwd: dir,
      encoding: "utf8",
    });
  } catch (error) {
    if (isGitCommandNotFoundError(error)) {
      throw new Error("Git is required to run configure-devops but was not found in PATH.");
    }

    const execError = error as ExecFileError;
    const stderr = typeof execError.stderr === "string" ? execError.stderr.trim() : "";
    if (stderr) {
      throw new Error(`Failed to add git remote origin: ${stderr}`);
    }

    throw new Error(`Failed to add git remote origin: ${(error as Error).message}`);
  }
}

async function copyTemplateDirectory(
  sourceDir: string,
  targetDir: string,
  targetRoot: string,
  options: ConfigureTemplateOptions = {},
): Promise<CopyTemplateResult> {
  const entries = await readdir(sourceDir, { withFileTypes: true });
  const copied: string[] = [];
  const skipped: string[] = [];
  const overwritten: string[] = [];

  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const destinationName = entry.isFile() ? normalizeTemplateFilename(entry.name) : entry.name;
    const destinationPath = path.join(targetDir, destinationName);
    const relativePath = path.relative(targetRoot, destinationPath) || destinationName;

    if (entry.isDirectory()) {
      await mkdir(destinationPath, { recursive: true });
      const result = await copyTemplateDirectory(sourcePath, destinationPath, targetRoot, options);
      copied.push(...result.copied);
      skipped.push(...result.skipped);
      overwritten.push(...result.overwritten);
      continue;
    }

    if (entry.isFile()) {
      await mkdir(path.dirname(destinationPath), { recursive: true });
      if (await fileExists(destinationPath)) {
        if (options.overwrite) {
          await copyFile(sourcePath, destinationPath);
          overwritten.push(relativePath);
        } else {
          skipped.push(relativePath);
        }
        continue;
      }
      await copyFile(sourcePath, destinationPath);
      copied.push(relativePath);
    }
  }

  return { copied, skipped, overwritten };
}

function normalizeTemplateFilename(filename: string): string {
  if (filename === "gitignore" || filename === ".npmignore") {
    return ".gitignore";
  }
  return filename;
}

async function configurePlugin(options: {
  targetPath?: string;
  templateName: string;
  dependencyName?: string;
  dependencySpecifier?: string;
  label: string;
  overwrite?: boolean;
}): Promise<void> {
  const { targetPath, templateName, dependencyName, dependencySpecifier, label, overwrite } = options;
  const targetDir = path.resolve(process.cwd(), targetPath ?? ".");
  const templateDir = path.resolve(currentDirname, `../templates/${templateName}`);
  const packagePath = path.join(targetDir, "package.json");

  try {
    await access(templateDir);
  } catch (error) {
    throw new Error(`${label} template directory is missing. Please ensure templates/${templateName} exists.`);
  }

  let pkgRaw: string;
  try {
    pkgRaw = await readFile(packagePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`Could not find package.json in ${targetDir}`);
    }
    throw error;
  }

  const pkg = JSON.parse(pkgRaw) as PackageJson;

  const { copied, skipped, overwritten } = await copyTemplateDirectory(templateDir, targetDir, targetDir, { overwrite });

  let pkgChanged = false;
  if (dependencyName && dependencySpecifier) {
    const deps = (pkg.dependencies ??= {});
    if (deps[dependencyName] !== dependencySpecifier) {
      deps[dependencyName] = dependencySpecifier;
      pkgChanged = true;
    }
  }

  if (pkgChanged) {
    await writeFile(packagePath, JSON.stringify(pkg, null, 2) + "\n", "utf8");
  }

  console.log(`\n${label} assets processed.`);
  if (copied.length) {
    console.log("  Added files:");
    for (const file of copied) {
      console.log(`    ${file}`);
    }
  }
  if (overwritten.length) {
    console.log("  Overwritten files:");
    for (const file of overwritten) {
      console.log(`    ${file}`);
    }
  }
  if (skipped.length) {
    console.log("  Skipped existing files:");
    for (const file of skipped) {
      console.log(`    ${file}`);
    }
  }
  if (!copied.length && !overwritten.length && !skipped.length) {
    console.log("  No template files were copied.");
  }

  if (dependencyName && dependencySpecifier) {
    if (pkgChanged) {
      console.log(`  Added dependency ${dependencyName}@${dependencySpecifier}. Run pnpm install to fetch it.`);
    } else {
      console.log("  Existing package.json already contained the required dependency.");
    }
  }
}

async function resolveAzureGitApi(organization: string): Promise<{ gitApi: IGitApi }> {
  const tokensUrl = `https://dev.azure.com/${organization}/_usersSettings/tokens`;
  const envPat = process.env.AZURE_PAT?.trim();

  if (envPat) {
    try {
      const gitApi = await createAzureGitApi(organization, envPat);
      console.log("Using PAT from AZURE_PAT environment variable.");
      return { gitApi };
    } catch (error) {
      console.log("The AZURE_PAT environment variable is invalid or expired. Please provide a new PAT.");
    }
  }

  while (true) {
    const input = (await promptQuestion(
      `Azure DevOps Personal Access Token (create at ${tokensUrl}): `,
    )).trim();

    if (!input) {
      console.log("A Personal Access Token is required to create the repository.");
      continue;
    }

    try {
      const gitApi = await createAzureGitApi(organization, input);
      return { gitApi };
    } catch (error) {
      console.log("The provided PAT is invalid or expired. Please try again.");
    }
  }
}

async function createAzureGitApi(organization: string, personalAccessToken: string): Promise<IGitApi> {
  const authHandler = azdev.getPersonalAccessTokenHandler(personalAccessToken);
  const connection = new azdev.WebApi(`https://dev.azure.com/${organization}`, authHandler);
  await connection.connect();
  return connection.getGitApi();
}

async function ensureAzureRepositoryExists(
  gitApi: IGitApi,
  params: { organization: string; project: string; repository: string },
): Promise<{ remoteUrl?: string; webUrl?: string }> {
  const repositoryName = params.repository.trim();
  if (!repositoryName) {
    throw new Error("Repository name is required.");
  }

  let existingRemoteUrl: string | undefined;
  let existingWebUrl: string | undefined;

  try {
    const repositories = await gitApi.getRepositories(params.project);
    const existing = repositories?.find(
      (repo: GitRepository) => repo.name?.toLowerCase() === repositoryName.toLowerCase(),
    );
    if (existing) {
      existingRemoteUrl = existing.remoteUrl ?? undefined;
      existingWebUrl = existing.webUrl ?? existingRemoteUrl;
      return { remoteUrl: existingRemoteUrl, webUrl: existingWebUrl };
    }
  } catch (error) {
    // Fallback to attempting creation even if listing failed (e.g., limited permissions)
  }

  try {
    const created = await gitApi.createRepository({ name: repositoryName }, params.project);
    const remoteUrl = created?.remoteUrl ?? buildAzureGitRemoteUrl(params.organization, params.project, repositoryName);
    const webUrl = created?.webUrl ?? created?.remoteUrl ?? buildAzureRepositoryUrl(params.organization, params.project, repositoryName);
    console.log(`  Created Azure DevOps repository "${repositoryName}" in project "${params.project}".`);
    return { remoteUrl, webUrl };
  } catch (error) {
    try {
      const repository = await gitApi.getRepository(repositoryName, params.project);
      if (repository?.remoteUrl) {
        return {
          remoteUrl: repository.remoteUrl,
          webUrl: repository.webUrl ?? repository.remoteUrl,
        };
      }
    } catch (lookupError) {
      // Ignore lookup failure; we'll rethrow original error below.
    }

    const message = (error as Error).message || String(error);
    throw new Error(
      `Failed to create Azure DevOps repository "${repositoryName}" in project "${params.project}": ${message}`,
    );
  }
}

function parseAzureRemote(remoteUrl: string): AzureRemoteInfo | undefined {
  try {
    const url = new URL(remoteUrl);
    const hostname = url.hostname.toLowerCase();
    const segments = url.pathname.split("/").filter(Boolean);

    if (hostname === "dev.azure.com") {
      const organization = segments[0] || url.username || undefined;
      const project = segments[1];
      const repository = extractRepositoryFromSegments(segments.slice(2));
      return {
        organization: organization ? decodeURIComponent(organization) : undefined,
        project: project ? decodeURIComponent(project) : undefined,
        repository,
      };
    }

    if (hostname.endsWith(".visualstudio.com")) {
      const organization = hostname.replace(/\.visualstudio\.com$/, "");
      const project = segments[0];
      const repository = extractRepositoryFromSegments(segments.slice(1));
      return {
        organization,
        project: project ? decodeURIComponent(project) : undefined,
        repository,
      };
    }
  } catch (error) {
    // Non-HTTP remote (e.g., SSH)
  }

  if (remoteUrl.startsWith("git@ssh.dev.azure.com:")) {
    const [, pathPart] = remoteUrl.split(":", 2);
    if (pathPart) {
      const segments = pathPart.split("/").filter(Boolean);
      // Format: v3/{organization}/{project}/{repo}
      if (segments[0]?.toLowerCase() === "v3") {
        return {
          organization: segments[1] ? decodeURIComponent(segments[1]) : undefined,
          project: segments[2] ? decodeURIComponent(segments[2]) : undefined,
          repository: stripGitExtension(segments.slice(3).join("/")),
        };
      }
    }
  }

  return undefined;
}

function extractRepositoryFromSegments(segments: string[]): string | undefined {
  if (!segments.length) {
    return undefined;
  }

  if (segments[0] === "_git") {
    return stripGitExtension(segments.slice(1).join("/"));
  }

  if (segments.length >= 2 && segments[1] === "_git") {
    return stripGitExtension(segments.slice(2).join("/"));
  }

  return stripGitExtension(segments.join("/"));
}

function stripGitExtension(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  return value.endsWith(".git") ? value.slice(0, -4) : value;
}

function encodeAzureSegment(segment: string): string {
  return encodeURIComponent(segment.trim());
}

function buildAzureGitRemoteUrl(organization: string, project: string, repository: string): string {
  const segments = [organization, project, "_git", repository].map(encodeAzureSegment);
  return `https://dev.azure.com/${segments.join("/")}`;
}

function buildAzureRepositoryUrl(organization: string, project: string, repository: string): string | undefined {
  if (!organization || !project || !repository) {
    return undefined;
  }
  return buildAzureGitRemoteUrl(organization, project, repository);
}

function inferRepositoryNameFromPackage(pkgName: unknown): string {
  if (typeof pkgName !== "string" || !pkgName.trim()) {
    return "uns-app";
  }

  const trimmed = pkgName.trim();
  const baseName = trimmed.startsWith("@") ? trimmed.split("/").pop() ?? trimmed : trimmed;
  return baseName.replace(/[^A-Za-z0-9._-]+/g, "-");
}

function isGitRemoteMissingError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const execError = error as ExecFileError;
  if (typeof execError.stderr === "string") {
    return execError.stderr.includes("No such remote");
  }

  return false;
}

function isGitCommandNotFoundError(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && (error as NodeJS.ErrnoException).code === "ENOENT");
}

type ExecFileError = NodeJS.ErrnoException & {
  code?: number | string;
  stdout?: string;
  stderr?: string;
};

async function promptWithDefault(message: string, defaultValue: string, requiredMessage: string): Promise<string> {
  while (true) {
    const answer = (await promptQuestion(message)).trim();
    if (answer) {
      return answer;
    }
    if (defaultValue) {
      return defaultValue;
    }
    console.log(requiredMessage);
  }
}

async function promptQuestion(message: string): Promise<string> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  try {
    return await rl.question(message);
  } finally {
    rl.close();
  }
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

async function ensureTargetDir(dir: string, options: { allowExisting?: boolean } = {}): Promise<void> {
  try {
    const stats = await stat(dir);
    if (!stats.isDirectory()) {
      throw new Error(`Path ${dir} exists and is not a directory.`);
    }
    const entries = await readdir(dir);
    if (entries.length > 0 && !options.allowExisting) {
      throw new Error(`Directory ${dir} is not empty.`);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      await mkdir(dir, { recursive: true });
      return;
    }
    throw error;
  }
}

async function patchPackageJson(targetDir: string, packageName: string): Promise<void> {
  const pkgFile = path.join(targetDir, "package.json");
  const raw = await readFile(pkgFile, "utf8");
  const pkg = JSON.parse(raw);
  pkg.name = packageName;

  const dependencies = (pkg.dependencies ??= {});
  if (dependencies["@uns-kit/core"]) {
    dependencies["@uns-kit/core"] = resolveUnsPackageSpecifier(
      "@uns-kit/core",
      "../../uns-core/package.json",
    );
  } else {
    dependencies["@uns-kit/core"] = `^${coreVersion}`;
  }

  await writeFile(pkgFile, JSON.stringify(pkg, null, 2) + "\n", "utf8");
}

async function patchConfigJson(targetDir: string, packageName: string): Promise<void> {
  const configFile = path.join(targetDir, "config.json");

  try {
    const raw = await readFile(configFile, "utf8");
    const config = JSON.parse(raw);
    if (config.uns && typeof config.uns === "object") {
      config.uns.processName = packageName;
    }
    await writeFile(configFile, JSON.stringify(config, null, 2) + "\n", "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return;
    }
    throw error;
  }
}

async function replacePlaceholders(targetDir: string, packageName: string): Promise<void> {
  const configFilePath = path.join(targetDir, "config.json");
  const replacements: Record<string, string> = {
    __APP_NAME__: packageName,
    __APP_CONFIG__: deriveConfigIdentifier(configFilePath),
  };

  const profileFiles = [
    configFilePath,
    path.join(targetDir, "config-development-host.json"),
    path.join(targetDir, "config-development-podman.json"),
    path.join(targetDir, "config-production.json"),
  ];

  const filesToUpdate = [
    path.join(targetDir, "README.md"),
    path.join(targetDir, "src/index.ts"),
    ...profileFiles,
  ];

  for (const file of filesToUpdate) {
    try {
      await access(file);
      if (profileFiles.some(profileFile => path.resolve(file) === path.resolve(profileFile))) {
        await replaceConfigTemplatePlaceholders(file, replacements, packageName);
        continue;
      }

      const original = await readFile(file, "utf8");
      const updated = replaceInString(original, replacements);
      if (updated !== original) {
        await writeFile(file, updated, "utf8");
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
    }
  }
}

async function resolvePackageNameFromTarget(targetDir: string): Promise<string | undefined> {
  const packagePath = path.join(targetDir, "package.json");
  try {
    const raw = await readFile(packagePath, "utf8");
    const pkg = JSON.parse(raw) as PackageJson;
    const name = typeof pkg.name === "string" ? pkg.name.trim() : "";
    return name || undefined;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

function deriveConfigIdentifier(filePath: string): string {
  const baseName = path.basename(filePath, path.extname(filePath));
  if (baseName.startsWith("config-")) {
    const suffix = baseName.slice("config-".length);
    return suffix || "config";
  }
  return baseName || "config";
}

async function replaceConfigTemplatePlaceholders(
  filePath: string,
  replacements: Record<string, string>,
  packageName?: string,
): Promise<void> {
  let raw: string;
  try {
    raw = await readFile(filePath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return;
    }
    throw error;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as Record<string, unknown>;
  } catch (error) {
    const updated = replaceInString(raw, replacements);
    if (updated !== raw) {
      await writeFile(filePath, updated, "utf8");
    }
    return;
  }

  let modified = applyJsonTemplateReplacements(parsed, replacements);

  if (packageName && parsed && typeof parsed === "object") {
    const root = parsed as Record<string, unknown>;
    const unsSection = root["uns"];
    if (unsSection && typeof unsSection === "object") {
      const unsRecord = unsSection as Record<string, unknown>;
      if (unsRecord["processName"] !== packageName) {
        unsRecord["processName"] = packageName;
        modified = true;
      }
    }
  }

  if (!modified) {
    return;
  }

  const formatted = JSON.stringify(parsed, null, 2) + "\n";
  await writeFile(filePath, formatted, "utf8");
}

function applyJsonTemplateReplacements(target: unknown, replacements: Record<string, string>): boolean {
  let modified = false;

  const visit = (value: unknown): unknown => {
    if (typeof value === "string") {
      const replaced = replaceInString(value, replacements);
      if (replaced !== value) {
        modified = true;
      }
      return replaced;
    }

    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        const next = visit(value[index]);
        if (next !== value[index]) {
          value[index] = next;
        }
      }
      return value;
    }

    if (value && typeof value === "object") {
      const record = value as Record<string, unknown>;
      for (const [key, child] of Object.entries(record)) {
        const next = visit(child);
        if (next !== child) {
          record[key] = next;
        }
      }
      return record;
    }

    return value;
  };

  visit(target);
  return modified;
}

function replaceInString(input: string, replacements: Record<string, string>): string {
  let result = input;
  for (const [needle, replacement] of Object.entries(replacements)) {
    if (needle && result.includes(needle)) {
      result = result.split(needle).join(replacement);
    }
  }
  return result;
}

function normalizePackageName(input: string): string {
  const trimmed = input.trim();
  if (trimmed.startsWith("@")) {
    return trimmed;
  }
  return trimmed
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    || "uns-app";
}

function resolveUnsPackageSpecifier(packageName: string, relativeLocalPath: string): string {
  const localPath = path.resolve(currentDirname, relativeLocalPath);
  if (existsSync(localPath)) {
    const pkg = require(localPath) as { version?: string };
    const version = pkg?.version ?? "0.0.1";
    return `workspace:^${version}`;
  }

  const version = resolveUnsPackageVersion(packageName, relativeLocalPath);
  return version === "latest" ? "latest" : `^${version}`;
}

function resolveUnsPackageVersion(packageName: string, relativeLocalPath: string): string {
  const attempt = (factory: () => string | undefined): string | undefined => {
    try {
      return factory();
    } catch (error) {
      return undefined;
    }
  };

  const directVersion = attempt(() => {
    const pkg = require(`${packageName}/package.json`) as { version?: string };
    return pkg?.version;
  });
  if (directVersion) {
    return directVersion;
  }

  const workspaceVersion = attempt(() => {
    const pkgPath = path.resolve(currentDirname, relativeLocalPath);
    const pkg = require(pkgPath) as { version?: string };
    return pkg?.version;
  });
  if (workspaceVersion) {
    return workspaceVersion;
  }

  const cliDependencyVersion = attempt(() => {
    const cliPkg = require("../package.json") as {
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
      unsKitPackages?: Record<string, string>;
    };
    const pinned = cliPkg.unsKitPackages?.[packageName];
    if (typeof pinned === "string" && pinned.trim()) {
      return pinned.trim();
    }

    const ranges = [cliPkg.dependencies?.[packageName], cliPkg.devDependencies?.[packageName]].filter(
      (range): range is string => typeof range === "string",
    );

    for (const range of ranges) {
      const match = range.match(/\d+\.\d+\.\d+/);
      if (match?.[0]) {
        return match[0];
      }
    }
    return undefined;
  });
  if (cliDependencyVersion) {
    return cliDependencyVersion;
  }

  return "latest";
}

function resolveCliVersion(): string {
  const attempt = (factory: () => string | undefined): string | undefined => {
    try {
      return factory();
    } catch (error) {
      return undefined;
    }
  };

  const packageVersion = attempt(() => {
    const pkg = require("../package.json") as { version?: string };
    return pkg?.version;
  });
  if (packageVersion) {
    return packageVersion;
  }

  const envVersion = attempt(() => process.env.npm_package_version?.trim());
  if (envVersion) {
    return envVersion;
  }

  return "0.0.0";
}

function resolveCoreVersion(): string {
  const attempt = (factory: () => string | undefined): string | undefined => {
    try {
      return factory();
    } catch (error) {
      return undefined;
    }
  };

  const directVersion = attempt(() => {
    const pkg = require("@uns-kit/core/package.json") as { version?: string };
    return pkg?.version;
  });
  if (directVersion) {
    return directVersion;
  }

  const workspaceVersion = attempt(() => {
    const localPath = path.resolve(currentDirname, "../../uns-core/package.json");
    const pkg = require(localPath) as { version?: string };
    return pkg?.version;
  });
  if (workspaceVersion) {
    return workspaceVersion;
  }

  const dependencyVersion = attempt(() => {
    const cliPkg = require("../package.json") as {
      dependencies?: Record<string, string>;
    };
    const range = cliPkg.dependencies?.["@uns-kit/core"];
    if (typeof range === "string") {
      const match = range.match(/\d+\.\d+\.\d+/);
      return match?.[0];
    }
    return undefined;
  });
  if (dependencyVersion) {
    return dependencyVersion;
  }

  return "0.0.1";
}

void main();
