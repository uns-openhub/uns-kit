import fs from "node:fs";
import path from "node:path";

import { configurationGroupSnapshot, configurationResourceHash } from "./configuration-group-contract.js";
import { authorizeProcessGroupRuntime, processGroupManifestSchema } from "./process-group-authorization.js";
import {
  processGroupJournalContract,
  processGroupJournalSchema,
  processGroupSourceIdentity,
  readConfigurationSelector,
  resolvedProcessGroupChannels,
} from "./process-group-contract.js";
import {
  type ProcessGroupDirectDescriptor,
  type ProcessGroupDirectInput,
  processGroupDirectDescriptorSchema,
  processGroupDirectInputSchema,
  processGroupRetirementPolicySchema,
  retiredJournalReferenceSchema,
} from "./process-group-direct-contract.js";
import { describeInstalledProcessGroupGeneration, inspectGenerationJournal } from "./process-group-generation.js";
import type { GenerationJournalReference } from "./process-group-generation.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  readProcessGroupResource as read,
  sha256,
} from "./process-group-resources.js";
import { normalizeRuntimeProcessGroup, selectedProcessGroupDescriptor } from "./process-group-runtime-selection.js";
import type { UpgradeConfiguration } from "./process-group-upgrade.js";

type Config = UpgradeConfiguration;
export type DirectTransitionConfigurations = { source: { publisher: Config; receiver: Config }; target: Config };
const inside = (root: string, file: string) => {
  const resolved = path.resolve(root, file);
  if (!resolved.startsWith(path.resolve(root) + path.sep)) throw Error("Direct resource escapes installed package");
  return read(resolved);
};
export function processGroupDirectDescriptor(packageRoot: string): ProcessGroupDirectDescriptor | undefined {
  const pkg = JSON.parse(read(path.join(packageRoot, "package.json")).toString());
  return pkg.unsDatahub?.processGroupDirectRuntime === undefined
    ? undefined
    : processGroupDirectDescriptorSchema.parse(pkg.unsDatahub.processGroupDirectRuntime);
}
export function describeDirectProcessGroup(c: Config) {
  const descriptor = processGroupDirectDescriptor(c.packageRoot);
  if (
    !descriptor ||
    (descriptor.match && readConfigurationSelector(c.raw, descriptor.match.path) !== descriptor.match.value) ||
    readConfigurationSelector(c.raw, descriptor.runtimeRolePath) !== descriptor.directRole ||
    readConfigurationSelector(c.raw, descriptor.journalPath) !== undefined
  )
    throw Error("Exact declared direct mode without an input journal required");
  const publisher = readConfigurationSelector(c.raw, descriptor.publisherPath);
  if (!publisher || typeof publisher !== "object" || Array.isArray(publisher)) throw Error("Explicit direct publisher required");
  const selectedResources = descriptor.resources.filter(
    (r) => readConfigurationSelector(c.raw, r.kind === "file" ? r.manifestPath : r.groupPath) !== undefined,
  );
  if (selectedResources.length !== 1) throw Error("Select exactly one declared direct resource");
  const resource = selectedResources[0]!;
  if (resource.kind === "configuration") {
    const snapshot = configurationGroupSnapshot(readConfigurationSelector(c.raw, resource.groupPath));
    if (
      configurationResourceHash(readConfigurationSelector(c.resolved, resource.groupPath)) !==
      configurationResourceHash(readConfigurationSelector(c.raw, resource.groupPath))
    )
      throw Error("Resolved direct business resource differs from its reviewed configuration");
    return {
      descriptor,
      resource,
      groupId: snapshot.group.id,
      processName: snapshot.group.processName,
      resourceSha256: snapshot.sha256,
      profiles: snapshot.profiles.map((p) => ({ processName: p.processName, mappingSha256: p.resourceSha256, dataGroup: p.dataGroup })),
      snapshot,
    };
  }
  const manifestPath = readConfigurationSelector(c.raw, resource.manifestPath);
  const selected = resource.manifests.find((m) => m.file === manifestPath);
  if (!selected) throw Error("Unregistered direct manifest");
  if (readConfigurationSelector(c.resolved, resource.manifestPath) !== selected.file) throw Error("Resolved direct manifest selection changed");
  const bytes = inside(c.packageRoot, selected.file),
    manifest = processGroupManifestSchema.parse(JSON.parse(bytes.toString()));
  if (manifest.id !== selected.id) throw Error("Direct manifest identity differs");
  for (const p of manifest.profiles)
    if (sha256(inside(c.packageRoot, path.join(path.dirname(selected.file), p.mappingFile))) !== p.mappingSha256)
      throw Error("Direct profile resource changed");
  return {
    descriptor,
    resource,
    groupId: manifest.id,
    processName: manifest.processName,
    resourceSha256: sha256(bytes),
    profiles: manifest.profiles.map(({ processName, mappingSha256, dataGroup }) => ({ processName, mappingSha256, dataGroup })),
    manifest,
    manifestPath: selected.file,
    manifestFile: path.resolve(c.packageRoot, selected.file),
  };
}
function remove(raw: Record<string, unknown>, keys: readonly string[]): void {
  let current = raw;
  const parents: Array<[Record<string, unknown>, string]> = [];
  for (const key of keys.slice(0, -1)) {
    const next = current[key];
    if (!next || typeof next !== "object" || Array.isArray(next)) return;
    parents.push([current, key]);
    current = next as Record<string, unknown>;
  }
  delete current[keys.at(-1)!];
  for (const [parent, key] of parents.reverse()) {
    const value = parent[key];
    if (value && typeof value === "object" && !Object.keys(value).length) delete parent[key];
    else break;
  }
}
function projection(raw: Record<string, unknown>, d: ProcessGroupDirectDescriptor, excluded: readonly string[]) {
  const copy = structuredClone(raw);
  remove(copy, d.journalPath);
  remove(copy, d.runtimeRolePath);
  for (const r of d.resources) if (r.kind === "file") remove(copy, r.manifestPath);
  const uns = copy["uns"];
  if (uns && typeof uns === "object" && !Array.isArray(uns)) for (const key of excluded) delete (uns as Record<string, unknown>)[key];
  // Removing a lifecycle leaf must not turn an otherwise empty publisher into a difference.
  const publisher = readConfigurationSelector(copy, d.publisherPath);
  if (publisher && typeof publisher === "object" && !Object.keys(publisher).length) remove(copy, d.publisherPath);
  return hash(copy);
}
/** Data-only installed-resource review. Does not stop, initialize, rewrite or grant launch authority. */
export function reviewInstalledProcessGroupDirectTransition(
  operationId: string,
  rawInput: unknown,
  configs: DirectTransitionConfigurations,
  stateRoot: string,
  excludedUnsFields: readonly string[] = [],
) {
  const input = processGroupDirectInputSchema.parse(rawInput),
    owner = input.target.controller,
    first = configs.source.publisher,
    target = configs.target;
  const pkg = (c: Config) => JSON.parse(read(path.join(c.packageRoot, "package.json")).toString());
  for (const [p, c] of [
    [input.source.publisher, first],
    [input.source.receiver, configs.source.receiver],
    [input.target, target],
  ] as const) {
    if (
      path.resolve(c.packageRoot, "instances", p.instanceId) !== c.instanceDirectory ||
      fs.realpathSync(c.instanceDirectory) !== c.instanceDirectory ||
      pkg(c).name !== p.rttNode ||
      pkg(c).version !== p.version
    )
      throw Error("Exact installed package and instance placement required");
  }
  const context = (c: Config) => ({
    packageRoot: c.packageRoot,
    instanceDirectory: c.instanceDirectory,
    rawConfiguration: c.raw,
    channels: resolvedProcessGroupChannels(c.resolved),
    controllerName: owner,
    stateRoot,
  });
  const old = authorizeProcessGroupRuntime(context(first)),
    receiver = authorizeProcessGroupRuntime(context(configs.source.receiver));
  if (
    old.role !== "target" ||
    receiver.role !== "receiver" ||
    old.file !== receiver.file ||
    old.sha256 !== receiver.sha256 ||
    old.directory !== receiver.directory ||
    !old.journalId ||
    old.journalId !== receiver.journalId
  )
    throw Error("Exact authorized publisher/receiver owners required");
  const sourcePolicy = read(old.file, true),
    sourceDescriptor = selectedProcessGroupDescriptor(first.packageRoot, first.raw);
  if (!sourceDescriptor) throw Error("Missing source shared contract");
  const normalized = normalizeRuntimeProcessGroup(first.raw, sourceDescriptor),
    direct = describeDirectProcessGroup(target),
    d = direct.descriptor;
  const namespace = sourceDescriptor.namespace;
  if (
    d.namespace !== namespace ||
    hash(d.publisherPath) !== hash(sourceDescriptor.publisherPath) ||
    hash(d.journalPath) !== hash(sourceDescriptor.journalPath) ||
    hash(d.runtimeRolePath) !== hash(sourceDescriptor.runtimeRolePath) ||
    direct.groupId !== old.groupId ||
    direct.processName !== old.groupProcessName ||
    (target.raw["uns"] as Record<string, unknown>)?.["processName"] !== old.groupProcessName ||
    projection(first.raw, d, excludedUnsFields) !== projection(target.raw, d, excludedUnsFields)
  )
    throw Error("Direct business configuration, selector or group identity changed");
  const channels = groupChannelsHash(context(first).channels);
  if (channels !== groupChannelsHash(context(configs.source.receiver).channels) || channels !== groupChannelsHash(context(target).channels))
    throw Error("Direct broker/channel identity changed");
  for (const c of [first, configs.source.receiver, target])
    if ((resolvedProcessGroupChannels(c.resolved)[2] as { publishQueue?: { enabled?: boolean } }).publishQueue?.enabled === true)
      throw Error("Initial direct retirement requires disabled disk MQTT output");
  let reference, sourceResourceSha256: string;
  if (sourceDescriptor.protocolVersion === 2) {
    if (direct.resource.kind !== "configuration" || !direct.snapshot) throw Error("Resource kind must remain configuration based");
    const generation = describeInstalledProcessGroupGeneration(first);
    reference = generation.reference;
    sourceResourceSha256 = configurationGroupSnapshot(readConfigurationSelector(first.raw, sourceDescriptor.groupPath)).sha256;
  } else {
    if (direct.resource.kind !== "file" || !direct.manifest) throw Error("Resource kind must remain file based");
    const journal = processGroupJournalSchema.parse(normalized.journal);
    const declared = sourceDescriptor.manifests.find((m) => m.file === journal.groupManifest);
    if (!declared) throw Error("Unregistered original manifest");
    const bytes = inside(first.packageRoot, declared.file),
      manifest = processGroupManifestSchema.parse(JSON.parse(bytes.toString()));
    sourceResourceSha256 = sha256(bytes);
    const contract = processGroupJournalContract(processGroupSourceIdentity(context(first).channels[1] as never), journal, manifest.profiles);
    reference = {
      directory: old.directory,
      journalId: old.journalId,
      journalContractSha256: sha256(contract),
      generationId: manifest.id,
      contractVersion: 1 as const,
      maxRecords: journal.maxRecords,
      maxBytes: journal.maxBytes,
      minFreeBytes: journal.minFreeBytes,
      profiles: manifest.profiles.map(({ processName, mappingSha256, dataGroup }) => ({ processName, mappingSha256, dataGroup })),
    };
  }
  const journal = retiredJournalReferenceSchema.parse(reference) as GenerationJournalReference;
  if (sourceResourceSha256 !== direct.resourceSha256 || hash(journal.profiles) !== hash(direct.profiles))
    throw Error("Direct profile resources, paths or units changed");
  const usage = inspectGenerationJournal(journal); // Pending work is allowed at preview, never at policy switch.
  if (!Buffer.from(sourcePolicy.toString(), "utf8").equals(sourcePolicy)) throw Error("Original policy encoding cannot be retained exactly");
  const member = (placement: typeof input.target, c: Config) => ({
    placement,
    processName: String((c.raw["uns"] as Record<string, unknown>)["processName"]),
    configurationSha256: hash(c.raw),
    artifactSha256: groupArtifactHash(c.packageRoot),
  });
  const policy = processGroupRetirementPolicySchema.parse({
    schemaVersion: 5,
    kind: "uns-process-group-direct-retirement",
    protocolVersion: 1,
    operationId,
    packageName: pkg(first).name,
    namespace,
    groupId: old.groupId,
    controllerName: owner,
    stateRoot,
    recovery: "open",
    journal,
    sourcePolicyJson: sourcePolicy.toString(),
    sourcePolicySha256: sha256(sourcePolicy),
    source: {
      publisher: {
        ...member(input.source.publisher, first),
        policyConfigurationSha256: sourceDescriptor.protocolVersion === 2 ? configurationResourceHash(first.raw) : hash(first.raw),
      },
      receiver: {
        ...member(input.source.receiver, configs.source.receiver),
        policyConfigurationSha256:
          sourceDescriptor.protocolVersion === 2 ? configurationResourceHash(configs.source.receiver.raw) : hash(configs.source.receiver.raw),
      },
    },
    target: {
      ...member(input.target, target),
      packageRoot: target.packageRoot,
      descriptorSha256: hash(d),
      resourceSha256: direct.resourceSha256,
      channelsSha256: channels,
    },
  });
  if (sha256(read(old.file, true)) !== policy.sourcePolicySha256) throw Error("Original policy changed during direct review");
  const targetPolicy = Buffer.from(JSON.stringify(policy));
  if (targetPolicy.length > 65536) throw Error("Retirement policy byte budget exceeded");
  return {
    review: {
      protocolVersion: 1 as const,
      kind: "paired-to-direct" as const,
      input,
      operationId,
      policyFile: old.file,
      sourcePolicySha256: sha256(sourcePolicy),
      targetPolicySha256: sha256(targetPolicy),
      journal,
      source: policy.source,
      target: policy.target,
      namespace,
      groupId: old.groupId,
      delivery: { inputPausePossible: true, exactlyOnceOutput: false, brokerReplayNotTransferred: true },
      usage,
    },
    sourcePolicy,
    targetPolicy,
  };
}
