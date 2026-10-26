import path from "node:path";

import { configurationGroupAuthorizationSchema } from "./configuration-group-authorization.js";
import { configurationResourceHash } from "./configuration-group-contract.js";
import { authorizeProcessGroupRuntime, processGroupAuthorizationFile, validateProcessGroupPolicyCandidate } from "./process-group-authorization.js";
import { resolvedProcessGroupChannels } from "./process-group-contract.js";
import { describeInstalledProcessGroupGeneration, inspectGenerationJournal } from "./process-group-generation.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  readProcessGroupResource as read,
  sha256,
} from "./process-group-resources.js";
import type { GroupVersionUpgradeReview, PreparedGroupUpgrade, UpgradeConfiguration, UpgradeConfigurations } from "./process-group-upgrade.js";

/** Same generation, different paired artifacts. Never initializes or rewrites the journal. */
export function reviewInstalledConfigurationGroupUpgrade(
  configs: UpgradeConfigurations,
  controllerName: string,
  stateRoot: string,
  excludedUnsFields: readonly string[] = [],
): PreparedGroupUpgrade {
  const context = (c: UpgradeConfiguration) => ({
    packageRoot: c.packageRoot,
    instanceDirectory: c.instanceDirectory,
    rawConfiguration: c.raw,
    channels: resolvedProcessGroupChannels(c.resolved),
    controllerName,
    stateRoot,
  });
  const source = configs.source.publisher,
    target = configs.target.publisher,
    a = describeInstalledProcessGroupGeneration(source),
    b = describeInstalledProcessGroupGeneration(target);
  if (a.d.protocolVersion !== 2 || b.d.protocolVersion !== 2 || a.d.namespace !== b.d.namespace || hash(a.reference) !== hash(b.reference))
    throw Error("Unchanged configuration generation required; review resource changes as a separate generation");
  const pkg = (c: UpgradeConfiguration) => JSON.parse(read(path.join(c.packageRoot, "package.json")).toString()),
    sourcePackage = pkg(source),
    targetPackage = pkg(target);
  if (!sourcePackage.version || !targetPackage.version || sourcePackage.version === targetPackage.version)
    throw Error("Two different installed paired versions required");
  const old = authorizeProcessGroupRuntime(context(source));
  const sourcePolicy = read(old.file, true),
    previous = configurationGroupAuthorizationSchema.parse(JSON.parse(sourcePolicy.toString()));
  const portable = (raw: Record<string, unknown>) => {
    const copy = structuredClone(raw),
      uns = { ...(copy["uns"] as Record<string, unknown>) };
    for (const key of excludedUnsFields) delete uns[key];
    copy["uns"] = uns;
    return hash(copy);
  };
  const members: GroupVersionUpgradeReview["members"] = [];
  const directories = new Set<string>();
  for (const side of ["source", "target"] as const)
    for (const role of ["publisher", "receiver"] as const) {
      const c = configs[side][role],
        g = describeInstalledProcessGroupGeneration(c),
        installed = pkg(c);
      if (
        g.d.protocolVersion !== 2 ||
        g.d.namespace !== a.d.namespace ||
        installed.name !== sourcePackage.name ||
        installed.version !== (side === "source" ? sourcePackage.version : targetPackage.version) ||
        c.packageRoot !== configs[side].publisher.packageRoot ||
        directories.has(c.instanceDirectory) ||
        hash(g.reference) !== hash(a.reference) ||
        g.normalized.uns["processName"] !== (role === "publisher" ? a.processName : a.receiverProcessName) ||
        (role === "receiver"
          ? g.normalized.runtimeRole !== "admission"
          : g.normalized.runtimeRole !== undefined && g.normalized.runtimeRole !== "publisher") ||
        portable(c.raw) !== portable(configs.source[role].raw) ||
        groupChannelsHash(g.channels) !== groupChannelsHash(a.channels) ||
        processGroupAuthorizationFile(context(c)) !== old.file
      )
        throw Error("Exact paired configuration, resources, broker identity and retained journal required");
      directories.add(c.instanceDirectory);
      if (side === "source") {
        const proof = authorizeProcessGroupRuntime(context(c));
        if (proof.role !== (role === "publisher" ? "target" : "receiver") || proof.sha256 !== sha256(sourcePolicy))
          throw Error("Exact authorized source pair required");
      }
      members.push({
        side,
        role,
        processName: String(g.normalized.uns["processName"]),
        configurationSha256: hash(c.raw),
        artifactSha256: groupArtifactHash(c.packageRoot),
      });
    }
  const policy = configurationGroupAuthorizationSchema.parse({
    ...previous,
    packageRoot: target.packageRoot,
    descriptorSha256: configurationResourceHash(targetPackage.unsDatahub.processGroupConfigurationRuntime),
    artifactSha256: groupArtifactHash(target.packageRoot),
    placements: [
      ...previous.placements.filter((p) => p.role === "source"),
      ...(["publisher", "receiver"] as const).map((role) => ({
        instanceId: path.basename(configs.target[role].instanceDirectory),
        processName: members.find((m) => m.side === "target" && m.role === role)!.processName,
        role: role === "publisher" ? "target" : "receiver",
        configurationSha256: configurationResourceHash(configs.target[role].raw),
      })),
    ],
  });
  const targetPolicy = Buffer.from(JSON.stringify(policy));
  for (const role of ["publisher", "receiver"] as const) validateProcessGroupPolicyCandidate(context(configs.target[role]), targetPolicy);
  inspectGenerationJournal(a.reference); // Pending, previously completed work is valid and must remain bound.
  if (sha256(read(old.file, true)) !== sha256(sourcePolicy)) throw Error("Source policy changed during review");
  return {
    review: {
      protocolVersion: 1,
      namespace: a.d.namespace,
      groupId: a.id,
      policyFile: old.file,
      sourcePolicySha256: sha256(sourcePolicy),
      targetPolicySha256: sha256(targetPolicy),
      journalId: a.reference.journalId,
      directory: a.reference.directory,
      journalContractSha256: a.reference.journalContractSha256,
      profiles: a.reference.profiles,
      retainedJournal: a.reference,
      members,
    },
    sourcePolicy,
    targetPolicy,
  };
}
