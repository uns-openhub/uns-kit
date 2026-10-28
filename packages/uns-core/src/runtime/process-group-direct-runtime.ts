import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import { configuredGroupProcessNames, hasProcessGroupAuthorization, parseProcessGroupRetirementPolicy } from "./process-group-authorization.js";
import { resolvedProcessGroupChannels } from "./process-group-contract.js";
import { describeDirectProcessGroup, processGroupDirectDescriptor } from "./process-group-direct-review.js";
import { type GenerationJournalReference, inspectGenerationJournal } from "./process-group-generation.js";
import {
  groupArtifactHash,
  groupChannelsHash,
  configurationAuthorizationHash as hash,
  assertPrivateProcessGroupDirectory as privateDirectory,
  readProcessGroupResource as read,
} from "./process-group-resources.js";
import type { UpgradeConfiguration } from "./process-group-upgrade.js";

export type DirectRuntimeContext = UpgradeConfiguration & {
  controllerName: string;
  operationId?: string;
  launchId?: string;
};
/** Startup guard, shared by host and application. An installed declaration never grants authority. */
export function assertProcessGroupDirectLaunch(context: DirectRuntimeContext): void {
  if (!hasProcessGroupAuthorization()) return; // Standalone deployments without a managed journal remain supported.
  const names = configuredGroupProcessNames(),
    name = String((context.raw["uns"] as Record<string, unknown>)?.["processName"]);
  if (!names.has(name)) return;
  const d = processGroupDirectDescriptor(context.packageRoot);
  if (!d) throw Error("Retained group identity requires a reviewed direct runtime");
  const root = process.env["UNS_PROCESS_GROUP_STATE_ROOT"]!;
  const directory = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"];
  const single = process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"];
  const registry = path.join(root, d.namespace, "authorizations");
  privateDirectory(root);
  privateDirectory(registry);
  if (directory && directory !== root && directory !== registry) throw Error("Foreign direct policy registry");
  const files = new Set<string>();
  if (single) files.add(single);
  if (directory) {
    const entries = fs.readdirSync(registry, { withFileTypes: true });
    if (entries.length > 64) throw Error("Direct policy budget exceeded");
    for (const e of entries) if (e.name.endsWith(".json")) files.add(path.join(registry, e.name));
  }
  const policies = [...files]
    .map((file) => ({ file, raw: JSON.parse(read(file, true).toString()) }))
    .filter((p) => p.raw.schemaVersion === 5 && p.raw.target?.processName === name);
  if (policies.length !== 1) throw Error("Group input owners remain active or retired; reviewed direct authority required");
  const { file, raw } = policies[0]!,
    p = parseProcessGroupRetirementPolicy(raw),
    actual = describeDirectProcessGroup(context),
    channels = resolvedProcessGroupChannels(context.resolved),
    pkg = JSON.parse(read(path.join(context.packageRoot, "package.json")).toString());
  if (
    p.stateRoot !== root ||
    p.namespace !== d.namespace ||
    file !== path.join(registry, p.journal.generationId + ".json") ||
    context.controllerName !== p.controllerName ||
    pkg.name !== p.packageName ||
    actual.groupId !== p.groupId ||
    actual.processName !== name ||
    actual.resourceSha256 !== p.target.resourceSha256 ||
    hash(actual.profiles) !== hash(p.journal.profiles)
  )
    throw Error("Retained direct group resource or identity changed");
  if (
    path.resolve(context.packageRoot) !== context.packageRoot ||
    fs.realpathSync(context.packageRoot) !== context.packageRoot ||
    path.dirname(context.instanceDirectory) !== path.join(context.packageRoot, "instances") ||
    fs.realpathSync(context.instanceDirectory) !== context.instanceDirectory ||
    !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(path.basename(context.instanceDirectory))
  )
    throw Error("Canonical installed direct instance required");
  const usage = inspectGenerationJournal(p.journal as GenerationJournalReference);
  if (usage.records || usage.bytes) throw Error("Retained input journal is not empty; direct launch blocked");
  if (p.recovery === "closed") return; // Original/profile and journal-mode launches remain fenced by schema5.
  if (
    context.packageRoot !== p.target.packageRoot ||
    pkg.version !== p.target.placement.version ||
    context.instanceDirectory !== path.join(context.packageRoot, "instances", p.target.placement.instanceId) ||
    fs.realpathSync(context.instanceDirectory) !== context.instanceDirectory ||
    hash(context.raw) !== p.target.configurationSha256 ||
    groupArtifactHash(context.packageRoot) !== p.target.artifactSha256 ||
    hash(d) !== p.target.descriptorSha256 ||
    groupChannelsHash(channels) !== p.target.channelsSha256 ||
    (channels[2] as { publishQueue?: { enabled?: boolean } }).publishQueue?.enabled === true
  )
    throw Error("Exact reviewed direct placement, configuration and artifact required");
  const r = JSON.parse(read(path.join(context.instanceDirectory, "process-group-reservation.json"), true).toString());
  if (
    !z.string().uuid().safeParse(context.launchId).success ||
    context.operationId !== p.operationId ||
    r.operationId !== p.operationId ||
    r.role !== "target" ||
    r.configurationChecksum !== p.target.configurationSha256 ||
    r.lastLaunch?.launchId !== context.launchId ||
    !z.string().uuid().safeParse(r.lastLaunch?.requestId).success
  )
    throw Error("Direct target requires its exact reviewed launch reservation");
}

/** PID/launch-bound observation, never authority. Start after output confirmation and input handler setup. */
export function trackProcessGroupDirectReadiness(context: DirectRuntimeContext, ready: () => boolean): () => void {
  assertProcessGroupDirectLaunch(context);
  if (!context.launchId) return () => {};
  const file = path.join(context.instanceDirectory, "process-group-direct-readiness.json");
  let active = true;
  const publish = () => {
    const bytes = JSON.stringify({
      protocolVersion: 1,
      pid: process.pid,
      launchId: context.launchId,
      configurationSha256: hash(context.raw),
      ready: active && ready(),
      at: Date.now(),
    });
    const temp = file + "." + randomUUID() + ".tmp",
      fd = fs.openSync(temp, "wx", 0o600);
    try {
      fs.writeFileSync(fd, bytes);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temp, file);
  };
  publish();
  const timer = setInterval(publish, 1000);
  timer.unref();
  return () => {
    active = false;
    clearInterval(timer);
    publish();
  };
}
export function inspectProcessGroupDirectReadiness(
  directory: string,
  expected: { pid: number; launchId: string; configurationSha256: string },
  now = Date.now(),
): boolean {
  try {
    const r = z
      .strictObject({
        protocolVersion: z.literal(1),
        pid: z.number().int().positive(),
        launchId: z.string().uuid(),
        configurationSha256: z.string().regex(/^[a-f0-9]{64}$/),
        ready: z.boolean(),
        at: z.number().int().positive(),
      })
      .parse(JSON.parse(read(path.join(directory, "process-group-direct-readiness.json"), true).toString()));
    return (
      r.ready &&
      r.pid === expected.pid &&
      r.launchId === expected.launchId &&
      r.configurationSha256 === expected.configurationSha256 &&
      r.at <= now + 1000 &&
      now - r.at <= 5000
    );
  } catch {
    return false;
  }
}

/** Preserve the managed registry in a direct process; never inherit a journal-mode launch permit. */
export function processGroupDirectLaunchEnvironment(operationId: string | undefined): Record<string, string> {
  return {
    UNS_PROCESS_GROUP_LOCAL_PILOT: "0",
    UNS_PROCESS_GROUP_AUTHORIZATION_FILE: process.env["UNS_PROCESS_GROUP_AUTHORIZATION_FILE"] ?? "",
    UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY: process.env["UNS_PROCESS_GROUP_AUTHORIZATION_DIRECTORY"] ?? "",
    UNS_PROCESS_GROUP_STATE_ROOT: process.env["UNS_PROCESS_GROUP_STATE_ROOT"] ?? "",
    UNS_PROCESS_GROUP_OPERATION_ID: operationId ?? "",
  };
}
