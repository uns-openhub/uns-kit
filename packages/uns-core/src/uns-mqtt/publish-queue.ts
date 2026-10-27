import type { IClientPublishOptions } from "mqtt";

import { type MqttPublishQueueOptions, type MqttPublishQueueStatus, mqttPublishQueueSchema } from "./publish-queue-contract.js";
import { MqttPublishQueueStore, type PublishRecord } from "./publish-queue-store.js";

/** MQTT output queue only. Input acknowledgement and business transforms remain caller-owned. */
export class MqttPublishQueue {
  private config;
  private store: MqttPublishQueueStore;
  private memory: PublishRecord[] = [];
  private completions = new Map<string, { resolve: () => void; reject: (error: Error) => void }>();
  private waiters = new Set<{ resolve: () => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private seq: number;
  private active: boolean;
  private closed = false;
  private processing: Promise<void> | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private delivered = 0;
  private retries = 0;
  private lastError: string | null = null;
  private blocked: Error | undefined;

  constructor(
    options: MqttPublishQueueOptions,
    identity: string,
    private send: (record: PublishRecord) => Promise<void>,
    active = true,
    private changed: (status: MqttPublishQueueStatus) => void = () => {},
  ) {
    this.config = mqttPublishQueueSchema.parse(options);
    if (!this.config.enabled) throw Error("Disabled MQTT persistence must be resolved before constructing an outbox");
    this.store = new MqttPublishQueueStore(this.config, identity);
    const saved = this.store.summary();
    if (saved.records > this.config.maxRecords || saved.bytes > this.config.maxBytes) {
      this.store.close();
      throw Error("Retained MQTT outbox exceeds configured limits; preserve and review it");
    }
    this.seq = saved.lastSeq;
    this.active = active;
    this.schedule();
  }

  /** Returns only after RAM admission, or (durable mode) after a FULL SQLite commit. */
  enqueueOrThrow(topic: string, message: string, id: string, options?: IClientPublishOptions): Promise<void> {
    if (this.closed || this.blocked) throw this.blocked ?? Error("MQTT publish queue is closed");
    if (this.completions.has(id)) throw Error("Duplicate MQTT queue admission id");
    if (
      !topic ||
      topic.includes("\0") ||
      /[+#]/.test(topic) ||
      Buffer.byteLength(topic) > 65535 ||
      typeof message !== "string" ||
      !id ||
      id.length > 200
    )
      throw Error("Invalid MQTT publish envelope");
    if (options && Object.keys(options).some((key) => !["qos", "retain"].includes(key)))
      throw Error("Persistent MQTT output currently supports only qos and retain publish options");
    const qos = options?.qos ?? 1;
    if (qos !== 1 && qos !== 2) throw Error("Persistent MQTT output requires QoS 1 or 2 broker acknowledgement");
    const bytes = Buffer.byteLength(topic) + Buffer.byteLength(message) + 256;
    if (bytes > this.config.maxMessageBytes) throw Error("MQTT publish exceeds maxMessageBytes");
    const total = this.snapshot();
    if (total.pendingRecords >= this.config.maxRecords || total.pendingBytes + bytes > this.config.maxBytes)
      throw Error("MQTT publish queue capacity reached; admission rejected");
    const record: PublishRecord = { seq: ++this.seq, id, topic, message, options: { qos, retain: options?.retain ?? false }, bytes };
    if (
      this.config.mode === "durable" ||
      this.memory.length >= this.config.memoryRecords ||
      this.memory.reduce((sum, r) => sum + r.bytes, 0) + bytes > this.config.memoryBytes
    )
      this.store.put(record);
    else this.memory.push(record);
    const delivered = new Promise<void>((resolve, reject) => this.completions.set(id, { resolve, reject }));
    this.emit();
    this.schedule();
    return delivered;
  }
  snapshot(): MqttPublishQueueStatus {
    const disk = this.store.summary();
    return {
      mode: this.config.mode,
      state: this.closed
        ? "closed"
        : this.blocked
          ? "blocked"
          : this.retryTimer
            ? "retrying"
            : disk.records + this.memory.length
              ? "pending"
              : "idle",
      pendingRecords: disk.records + this.memory.length,
      pendingBytes: disk.bytes + this.memory.reduce((sum, r) => sum + r.bytes, 0),
      memoryRecords: this.memory.length,
      diskRecords: disk.records,
      inFlight: !!this.processing,
      delivered: this.delivered,
      retries: this.retries,
      lastError: this.lastError,
    };
  }
  getState(): boolean {
    return this.active;
  }
  becomeActive(snapshot?: { batchSize?: number; referenceHash?: string }): void {
    if (snapshot?.batchSize) throw Error("Persistent MQTT output cannot discard work using legacy handover hashes");
    this.active = true;
    this.schedule();
  }
  async becomePassive(_n?: number): Promise<{ batchSize: number; referenceHash: string }> {
    this.active = false;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
    await this.processing;
    this.emit();
    return { batchSize: 0, referenceHash: "" };
  }
  waitForIdle(timeoutMs: number): Promise<void> {
    if (this.blocked) return Promise.reject(this.blocked);
    if (!this.snapshot().pendingRecords && !this.processing) return Promise.resolve();
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return Promise.reject(Error("MQTT drain timeout must be positive and finite"));
    return new Promise((resolve, reject) => {
      const waiter = {
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters.delete(waiter);
          reject(Error("MQTT publish queue drain timed out; pending work retained"));
        }, timeoutMs),
      };
      this.waiters.add(waiter);
    });
  }
  /** Caller must stop its MQTT transport first to release any in-flight publish. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.active = false;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
    await this.processing;
    // Persist all buffered work before releasing the storage lease. Failure leaves it open.
    while (this.memory.length) {
      this.store.put(this.memory[0]);
      this.memory.shift();
    }
    this.closed = true;
    const stopped = Error("MQTT output stopped; undelivered work retained on disk");
    for (const completion of this.completions.values()) completion.reject(stopped);
    this.completions.clear();
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(stopped);
    }
    this.waiters.clear();
    this.emit();
    this.store.close();
  }
  private schedule(): void {
    if (!this.active || this.closed || this.blocked || this.processing || this.retryTimer) return;
    // Defer so enqueue acceptance is visible before the delivery callback.
    this.processing = Promise.resolve()
      .then(() => this.run())
      .catch((error) => this.fail(error))
      .finally(() => {
        this.processing = undefined;
        if (!this.closed) {
          this.emit();
          if (this.snapshot().pendingRecords) this.schedule();
        }
      });
  }
  private async run(): Promise<void> {
    while (this.active && !this.closed && !this.blocked) {
      const disk = this.store.head();
      const memory = this.memory[0];
      const record = !memory || (disk && disk.seq < memory.seq) ? disk : memory;
      if (!record) return;
      try {
        await this.send(record);
      } catch {
        // A lost acknowledgement remains uncertain; persist and replay, never mark success.
        // After an observed outage, move all already accepted RAM work to disk.
        while (this.memory.length) {
          this.store.put(this.memory[0]);
          this.memory.shift();
        }
        this.retries++;
        this.lastError = "MQTT delivery unconfirmed; retained for retry";
        if (this.active)
          this.retryTimer = setTimeout(() => {
            this.retryTimer = undefined;
            this.schedule();
          }, this.config.retryIntervalMs);
        return;
      }
      if (record === memory) this.memory.shift();
      else this.store.complete(record);
      this.delivered++;
      this.lastError = null;
      this.completions.get(record.id)?.resolve();
      this.completions.delete(record.id);
      this.emit();
    }
  }
  private fail(reason: unknown): void {
    this.blocked = reason instanceof Error ? reason : Error("MQTT outbox storage failed");
    this.lastError = "MQTT outbox blocked; preserve storage and inspect local error";
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(this.blocked);
    }
    this.waiters.clear();
    this.emit();
  }
  private emit(): void {
    this.changed(this.snapshot());
    if (!this.snapshot().pendingRecords && !this.processing) {
      for (const waiter of this.waiters) {
        clearTimeout(waiter.timer);
        waiter.resolve();
      }
      this.waiters.clear();
    }
  }
}
