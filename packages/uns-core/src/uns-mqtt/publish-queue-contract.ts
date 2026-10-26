import { z } from "zod";

/** Opt-in output buffering; absence preserves the issued MQTT publisher. */
export const mqttPublishQueueSchema = z
  .object({
    mode: z.enum(["buffered", "durable"]),
    directory: z.string().min(1),
    maxRecords: z.number().int().min(1).max(100000).default(10000),
    maxBytes: z.number().int().min(1).max(536870912).default(67108864),
    memoryRecords: z.number().int().min(0).max(10000).default(256),
    memoryBytes: z.number().int().min(0).max(67108864).default(4194304),
    maxMessageBytes: z.number().int().min(1).max(1048576).default(524288),
    maxDiskBytes: z.number().int().min(1048576).max(1073741824).default(268435456),
    minFreeBytes: z.number().int().min(0).max(1073741824).default(16777216),
    retryIntervalMs: z.number().int().min(10).max(60000).default(1000),
    publishTimeoutMs: z.number().int().min(10).max(60000).default(10000),
  })
  .strict();
export type MqttPublishQueueOptions = z.input<typeof mqttPublishQueueSchema>;
export type ResolvedMqttPublishQueueOptions = z.output<typeof mqttPublishQueueSchema>;
export type MqttPublishQueueStatus = {
  mode: "buffered" | "durable";
  state: "idle" | "pending" | "retrying" | "blocked" | "closed";
  pendingRecords: number;
  pendingBytes: number;
  memoryRecords: number;
  diskRecords: number;
  inFlight: boolean;
  delivered: number;
  retries: number;
  lastError: string | null;
};
