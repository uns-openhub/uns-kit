import { z } from "zod";
import fs from "node:fs";
import path from "node:path";
import type { MqttPublishQueueStatus } from "../uns-mqtt/publish-queue-contract.js";

/** Generic process-local handshake. The host supplies identity and durable receipts. */
export const MQTT_DRAIN_HANDLER = Symbol.for("uns-kit.mqtt-drain.v1");
export const MQTT_DRAIN_READY_EVENT = "uns-kit:mqtt-drain-ready";
export const mqttDrainReceiptSchema = z.strictObject({
  protocol: z.literal(1),
  state: z.literal("drained"),
  pendingRecords: z.literal(0),
  outputs: z.number().int().min(0).max(64),
});
export type MqttDrainReceipt = z.infer<typeof mqttDrainReceiptSchema>;
export const mqttDrainCapabilitySchema = z.strictObject({ protocolVersion: z.literal(1) });
export function readMqttDrainCapability(packageRoot: string): 1 | undefined {
  const file = path.join(packageRoot, "package.json");
  let stat: fs.Stats;
  try { stat = fs.lstatSync(file); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1048576) throw Error("Unsafe installed MQTT drain descriptor");
  const value = JSON.parse(fs.readFileSync(file, "utf8"))?.unsDatahub?.mqttDrain;
  if (value === undefined) return undefined;
  mqttDrainCapabilitySchema.parse(value);
  return 1;
}
export interface MqttDrainOutput {
  flush(timeoutMs: number): Promise<void>;
  sealPublishes(): void;
  getPublishQueueStatus(): MqttPublishQueueStatus | undefined;
}

/** Quiesce before flushing. A timeout is a blocked attempt, never a clean exit. */
export async function drainMqttForMigration(options: {
  quiesce(): Promise<void>;
  finishAccepted(): Promise<void>;
  outputs: readonly MqttDrainOutput[];
  timeoutMs?: number;
}): Promise<MqttDrainReceipt> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300_000 || options.outputs.length > 64)
    throw Error("Invalid bounded MQTT migration drain");
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = Date.now() + timeoutMs;
  let expired = false;
  const work = (async () => {
    await options.quiesce();
    await options.finishAccepted();
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw Error("MQTT migration drain timed out; retain the original owner");
    await Promise.all(options.outputs.map(output => output.flush(remaining)));
    for (const output of options.outputs) {
      const status = output.getPublishQueueStatus();
      if (status && (status.pendingRecords !== 0 || status.memoryRecords !== 0 || status.diskRecords !== 0 || status.inFlight || status.state === "blocked"))
        throw Error("MQTT output is not confirmed empty; migration remains blocked");
    }
    if (expired || Date.now() > deadline) throw Error("MQTT migration drain expired; outcome must be reconciled");
    // No await between the final checks and admission fences.
    for (const output of options.outputs) output.sealPublishes();
    return mqttDrainReceiptSchema.parse({ protocol: 1, state: "drained", pendingRecords: 0, outputs: options.outputs.length });
  })();
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => { expired = true; reject(Error("MQTT migration drain timed out; pending work retained")); }, timeoutMs);
    })]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
