import path from "node:path";
import { z } from "zod";

import { mqttPublishQueueIdentity, mqttPublishQueueSchema } from "../uns-mqtt/publish-queue-contract.js";
import { managedMqttOutputBinding, mqttOutputStateLimits } from "./mqtt-output-state-contract.js";
import { inspectManagedOutputQueue } from "./mqtt-output-state-program.mjs";
import { resolvedProcessGroupChannels } from "./process-group-contract.js";
import { groupPlacementSchema, placementKey } from "./process-group-control-contract.js";
import { configurationAuthorizationHash as hash, readProcessGroupResource as read, sha256 } from "./process-group-resources.js";
import type { UpgradeConfigurations } from "./process-group-upgrade.js";

export const processGroupMqttOutputRuntimeSchema = z.strictObject({
  protocolVersion: z.literal(1),
  instanceName: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/),
});
const pair = z.strictObject({ publisher: groupPlacementSchema, receiver: groupPlacementSchema });
/** An explicit output enablement, never an incidental version/configuration change. */
export const processGroupMqttOutputInputSchema = z
  .strictObject({
    protocolVersion: z.literal(3),
    source: pair,
    target: pair,
    acknowledgeInputPause: z.literal(true),
    acknowledgePossibleReplay: z.literal(true),
    acknowledgePendingOutputBlocksRollback: z.literal(true),
  })
  .superRefine((v, c) => {
    const all = [v.source.publisher, v.source.receiver, v.target.publisher, v.target.receiver];
    if (
      new Set(all.map(placementKey)).size !== 4 ||
      all.some((p) => p.controller !== all[0]!.controller || p.rttNode !== all[0]!.rttNode) ||
      v.source.publisher.version !== v.source.receiver.version ||
      v.target.publisher.version !== v.target.receiver.version
    )
      c.addIssue({ code: "custom", message: "Four distinct same-host paired placements required" });
  });
export type GroupMqttOutputReference = { root: string; directory: string; rootId: string; relative: string; identity: string; optionsSha256: string };
/** Only these explicitly reviewed output fields can differ; all other portable data remains exact. */
export function mqttOutputPortableConfiguration(raw: Record<string, unknown>, role: "publisher" | "receiver") {
  const copy = structuredClone(raw);
  if (role === "publisher") {
    const output = copy["output"] as Record<string, unknown> | undefined;
    if (output) {
      delete output["publishQueue"];
      if (!Object.keys(output).length) delete copy["output"];
    }
    const uns = copy["uns"] as Record<string, unknown> | undefined;
    if (uns) {
      delete uns["handover"];
      delete uns["instanceMode"];
    }
  }
  return copy;
}
export function reviewProcessGroupMqttOutput(configs: UpgradeConfigurations, root: string | undefined, stateRoot: string): GroupMqttOutputReference {
  const source = configs.source.publisher,
    target = configs.target.publisher;
  for (const role of ["publisher", "receiver"] as const) {
    const raw = configs.source[role].raw as { output?: { publishQueue?: unknown } };
    if (raw.output?.publishQueue !== undefined) throw Error("Output enablement requires a source without an MQTT queue");
  }
  if ((configs.target.receiver.raw["output"] as { publishQueue?: unknown } | undefined)?.publishQueue !== undefined)
    throw Error("Admission receiver cannot own the publisher output queue");
  const queue = mqttPublishQueueSchema.parse((target.raw["output"] as { publishQueue?: unknown } | undefined)?.publishQueue);
  if (queue.mode !== "durable" || queue.storage !== "runtime") throw Error("Reviewed output enablement requires durable Runtime storage");
  const uns = target.raw["uns"] as Record<string, unknown>;
  if (uns["handover"] !== false || !["force", "wait"].includes(String(uns["instanceMode"])))
    throw Error("MQTT output enablement requires explicit stop-before-start configuration");
  if (!root || path.resolve(root) === path.resolve(stateRoot) || queue.directory.startsWith(path.resolve(stateRoot) + path.sep))
    throw Error("A separate permanent MQTT output mount is required");
  const binding = managedMqttOutputBinding(target.raw, root)!;
  const pkg = JSON.parse(read(path.join(target.packageRoot, "package.json")).toString());
  const sourcePackage = JSON.parse(read(path.join(source.packageRoot, "package.json")).toString());
  if (pkg.name !== sourcePackage.name) throw Error("Publisher package identity changed");
  const declaration = processGroupMqttOutputRuntimeSchema.parse(pkg.unsDatahub?.processGroupMqttOutputRuntime);
  const processName = String((target.raw["uns"] as Record<string, unknown>)["processName"]);
  if (processName !== String((source.raw["uns"] as Record<string, unknown>)["processName"])) throw Error("Publisher identity changed");
  const identity = sha256(
    mqttPublishQueueIdentity(pkg.name, processName + "-" + declaration.instanceName, resolvedProcessGroupChannels(target.resolved)[2]!),
  );
  const status = inspectManagedOutputQueue(root, binding.directory, identity, mqttOutputStateLimits);
  if (status.records || status.bytes) throw Error("Output enablement requires an empty original-destination queue; preserve pending work");
  return { root, directory: binding.directory, rootId: status.rootId, relative: binding.relative, identity, optionsSha256: hash(queue) };
}
/** Recheck after proven exit and immediately before policy/start. Unknown state is never empty. */
export function inspectReviewedProcessGroupMqttOutput(reference: GroupMqttOutputReference, requireStopped = true) {
  const status = inspectManagedOutputQueue(reference.root, reference.directory, reference.identity, mqttOutputStateLimits, requireStopped);
  if (status.rootId !== reference.rootId || status.relative !== reference.relative) throw Error("Reviewed MQTT output storage identity changed");
  return status;
}
