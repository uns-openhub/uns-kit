import { z } from "zod";

import { mqttPublishQueueSchema } from "../uns-mqtt/publish-queue-contract.js";
import fixed from "./mqtt-output-state-contract.json" with { type: "json" };
import { outputStateLimits } from "./mqtt-output-state-program.mjs";
import { outputStateRelative } from "./mqtt-output-state-program.mjs";

export const mqttOutputStateLimits = outputStateLimits(fixed);
export const mqttOutputStateBackupSchema = z
  .strictObject({
    schemaVersion: z.literal(1),
    state: z.enum(["included", "empty"]),
    rootSha256: z.string().regex(/^[a-f0-9]{64}$/),
    queues: z.number().int().min(0).max(mqttOutputStateLimits.maximumQueues),
    records: z
      .number()
      .int()
      .min(0)
      .max(mqttOutputStateLimits.maximumQueues * mqttOutputStateLimits.maximumRecordsPerQueue),
    bytes: z.number().int().positive().max(mqttOutputStateLimits.maximumCapsuleBytes),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
  })
  .refine((v) => (v.state === "included" ? v.queues > 0 : v.queues === 0 && v.records === 0));

/** Controller inspection of standard SDK configuration, without application code. */
export function managedMqttOutputBinding(config: unknown, root: string | undefined): { directory: string; relative: string } | undefined {
  const value = config as { output?: { publishQueue?: unknown } };
  if (value?.output?.publishQueue === undefined) return undefined;
  const queue = mqttPublishQueueSchema.parse(value.output.publishQueue);
  if (queue.storage !== "runtime" || !root)
    throw Error("Controller-managed MQTT output requires storage=runtime and a reviewed persistent Runtime mount");
  return { directory: queue.directory, relative: outputStateRelative(root, queue.directory) };
}
